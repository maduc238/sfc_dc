"""Prepare and run a CPU-only, matched-topology distributed operating study."""

import argparse
from copy import deepcopy
import csv
import hashlib
import json
import math
from pathlib import Path
import uuid

import yaml

from virne.distributed.config import _merge, load_config, validate_config
from virne.distributed.hybrid.benchmark import PAIRED_FIELDS, run_benchmark, validate_jobs
from virne.distributed.model import Substrate


DEFAULT_STUDY = Path(__file__).resolve().parents[2] / 'configs/experiments/distributed_study.yaml'
REPORT_METRICS = ('window_acceptance_ratio', 'window_accepted_requests',
                  'window_energy_kwh', 'window_energy_kwh_per_accepted_request',
                  'window_energy_kwh_per_served_vnf_hour', 'window_mean_service_latency_ms',
                  'window_mean_placement_time_ms', 'window_p95_admission_delay_ms',
                  'window_mean_cpu_utilization', 'window_mean_wan_utilization',
                  'window_migrations_completed', 'window_migration_normalized_cost',
                  'window_migration_downtime_s', 'qaoa_sim_calls', 'decision_budget_exhaustions', 'runtime_s',
                  'window_served_vnf_hours', 'migration_planning_budget_exhaustions',
                  'window_mean_solver_ms_per_started_request', 'window_started_decisions',
                  'window_queue_expirations', 'window_rejected_deadline', 'window_rejected_stale')


def load_study(path):
    with Path(path).open(encoding='utf-8') as stream:
        study = yaml.safe_load(stream)
    keys = {'schema_version', 'baselines', 'default_methods', 'offered_cpu_ratios',
            'daily_shape', 'stages', 'common'}
    if (not isinstance(study, dict) or not keys <= set(study)
            or set(study) - keys - {'execution', 'method_groups', 'profiles', 'simulation', 'evaluation'}
            or study['schema_version'] != 1):
        raise ValueError('Study YAML requires schema_version: 1 and the documented study fields')
    execution = study.setdefault('execution', {'jobs': 1})
    if not isinstance(execution, dict) or set(execution) != {'jobs'}:
        raise ValueError('Study execution must contain jobs')
    validate_jobs(execution['jobs'])
    baselines, defaults = study['baselines'], study['default_methods']
    if (not isinstance(baselines, dict) or not baselines
            or any(not isinstance(k, str) or not isinstance(v, str) or not v.strip()
                   for k, v in baselines.items())):
        raise ValueError('Study baselines must map method names to baseline YAML paths')
    if (not isinstance(defaults, list) or not defaults or not all(isinstance(v, str) for v in defaults)
            or len(set(defaults)) != len(defaults) or set(defaults) - baselines.keys()):
        raise ValueError('default_methods must select distinct entries from baselines')
    groups = study.get('method_groups', {})
    if not isinstance(groups, dict) or any(
        not isinstance(name, str) or not isinstance(values, list) or not values
        or any(not isinstance(v, str) for v in values) or len(set(values)) != len(values)
        or set(values) - baselines.keys() for name, values in groups.items()
    ):
        raise ValueError('method_groups must map group names to distinct supported baseline names')
    ratios = study['offered_cpu_ratios']
    if not isinstance(ratios, list) or not ratios:
        raise ValueError('offered_cpu_ratios must be a nonempty list')
    if any(type(v) not in (int, float) for v in ratios):
        raise ValueError('offered_cpu_ratios must be numeric')
    labels = [rho_label(v) for v in ratios]
    if len(set(labels)) != len(labels):
        raise ValueError('offered_cpu_ratios must be distinct')
    shape = study['daily_shape']
    if (not isinstance(shape, list) or len(shape) != 24
            or any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in shape)
            or sum(shape) <= 0):
        raise ValueError('daily_shape requires 24 nonnegative finite hourly weights with positive sum')
    if not isinstance(study['common'], dict) or not isinstance(study['stages'], dict) or not study['stages']:
        raise ValueError('Study common and stages must be mappings')
    profiles = study.get('profiles', {})
    if not isinstance(profiles, dict) or any(
        not isinstance(name, str) or name == 'standard' or not isinstance(overrides, dict)
        or not overrides or set(overrides) - {'topology', 'workload', 'control_plane'}
        for name, overrides in profiles.items()
    ):
        raise ValueError('Profiles may override topology, workload and control_plane for every method')
    for name, stage in study['stages'].items():
        if not isinstance(stage, dict) or set(stage) != {'duration_hours', 'warmup_hours', 'seeds'}:
            raise ValueError(f'Invalid stage settings: {name}')
        duration, warmup, seeds = stage['duration_hours'], stage['warmup_hours'], stage['seeds']
        if (any(type(v) not in (int, float) or not math.isfinite(v) for v in (duration, warmup))
                or not 0 <= warmup < duration):
            raise ValueError(f'Stage {name} requires 0 <= warmup_hours < duration_hours')
        if (not isinstance(seeds, list) or not seeds or any(type(s) is not int or s < 0 for s in seeds)
                or len(set(seeds)) != len(seeds)):
            raise ValueError(f'Stage {name} requires distinct nonnegative integer seeds')
    return study


def rho_label(rho):
    if not math.isfinite(rho) or rho <= 0 or not math.isclose(rho * 100, round(rho * 100)):
        raise ValueError('CPU load ratios must be positive multiples of 0.01')
    return f'rho{round(100 * rho):03d}'


def integrated_hours(factors, start, end):
    total = 0.0
    while start < end:
        hour = math.floor(start)
        stop = min(end, hour + 1)
        total += (stop - start) * factors[hour % 24]
        start = stop
    return total


def study_configs(root, rhos, methods, duration_hours, warmup_hours, study_path=DEFAULT_STUDY,
                  detail=None, profile='standard'):
    """Every method receives the same physical network, workload and system costs."""
    study_path = Path(study_path).resolve()
    study = load_study(study_path)
    if profile != 'standard' and profile not in study.get('profiles', {}):
        raise ValueError(f'Unknown study profile: {profile}')
    if (not rhos or not methods or len(set(methods)) != len(methods)
            or set(methods) - study['baselines'].keys()):
        raise ValueError('Select distinct, supported methods and at least one load')
    labels = [rho_label(r) for r in rhos]
    if len(set(labels)) != len(labels):
        raise ValueError('CPU load ratios must be distinct')
    sources, templates = {}, {}
    factors = [v * 24 / sum(study['daily_shape']) for v in study['daily_shape']]
    for method in methods:
        source = (study_path.parent / study['baselines'][method]).resolve()
        sources[method] = str(source)
        c = _merge(load_config(source), study['common'])
        if profile != 'standard':
            c = _merge(c, study['profiles'][profile])
        if detail is not None:
            c['output']['detail'] = detail
        c['workload'].update(duration_hours=duration_hours, hourly_multipliers=factors)
        c['measurement']['warmup_hours'] = warmup_hours
        c['experiment']['output_dir'] = str(Path(root).resolve() / 'single-runs')
        validate_config(c)
        if len(c['topology']['data_centers']) < 2 or c['hybrid']['qa']['enabled']:
            raise ValueError('This study requires distributed DCs and CPU-only quantum simulation')
        templates[method] = c
    base = next(iter(templates.values()))
    cpu = sum(dc['num_servers'] * dc.get('cpu', base['topology']['server']['cpu'])
              for dc in base['topology']['data_centers'])
    mean_request_cpu = sum(base['workload']['vnfs_per_request']) / 2 * sum(base['workload']['cpu']) / 2
    configs, loads = {}, []
    for rho, label in zip(rhos, labels):
        rate = rho * cpu / (base['workload']['mean_lifetime_hours'] * mean_request_cpu)
        offered = rate * integrated_hours(factors, 0, duration_hours)
        measured = rate * integrated_hours(factors, warmup_hours, duration_hours)
        loads.append({'load': label, 'offered_cpu_ratio': rho, 'arrival_rate_per_hour': rate,
                      'expected_requests': offered, 'expected_window_requests': measured})
        for method in methods:
            c = deepcopy(templates[method])
            c['experiment']['name'] = f'distributed_{method}_{label}'
            c['workload'].update(arrival_rate_per_hour=rate, num_requests=max(1, round(offered)))
            # num_requests is descriptive here; duration_hours determines when arrivals stop.
            validate_config(c)
            configs[(label, method)] = c
        first = configs[(label, methods[0])]
        for method in methods[1:]:
            c = configs[(label, method)]
            for key in ('topology', 'workload', 'control_plane', 'migration', 'power', 'measurement'):
                if c[key] != first[key]:
                    raise ValueError(f'Study baselines must share {key} settings: {method}')
            for key in ('scope', 'allow_colocation', 'k_paths'):
                if c['solver'][key] != first['solver'][key]:
                    raise ValueError(f'Study baselines must share solver.{key}: {method}')
    topology_hash = hashlib.sha256(json.dumps(base['topology'], sort_keys=True).encode()).hexdigest()
    dcs = base['topology']['data_centers']
    sizes = {dc['num_servers'] for dc in dcs}
    design = {'schema_version': 1, 'architecture': 'distributed_only',
              'source_study': str(study_path), 'baseline_sources': sources,
              'num_data_centers': len(dcs), 'servers_per_dc': next(iter(sizes)) if len(sizes) == 1 else None,
              'num_servers': sum(dc['num_servers'] for dc in dcs),
              'total_cpu': cpu, 'total_memory_gb': sum(
                  dc['num_servers'] * dc.get('memory_gb', base['topology']['server']['memory_gb']) for dc in dcs),
              'topology_sha256': topology_hash, 'topology': base['topology'],
              'duration_hours': duration_hours, 'warmup_hours': warmup_hours,
              'measurement_hours': duration_hours - warmup_hours, 'hourly_multipliers': factors,
              'mean_request_cpu': mean_request_cpu, 'methods': list(methods), 'loads': loads,
              'controller_latency_mode': base['control_plane']['latency_mode'],
              'quantum_execution': 'CPU_statevector_only',
              'assumptions': ['Synthetic daily Poisson arrivals; not a production trace',
                              f"Exponential service lifetime, mean {base['workload']['mean_lifetime_hours']:g} hours",
                              f"Embedding scope: {base['solver']['scope']}; {len(dcs)} distributed DCs",
                              'Identical serial intra-DC migration and costs for every method',
                              'Admission cohort uses arrival time; power excludes warm-up and drain']}
    if profile != 'standard':
        design['profile'] = profile
    return configs, design


def _csv(path, rows):
    if rows:
        with path.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def prepare(root, rhos, methods, duration_hours, warmup_hours, seeds, stage, resume=False,
            study_path=DEFAULT_STUDY, detail=None, profile='standard'):
    if not seeds or len(set(seeds)) != len(seeds) or any(s < 0 for s in seeds):
        raise ValueError('Seeds must be distinct nonnegative integers')
    configs, design = study_configs(root, rhos, methods, duration_hours, warmup_hours, study_path, detail, profile)
    if detail is not None:
        design['output_detail'] = detail
    Substrate(next(iter(configs.values())))  # Validate the actual physical graph before any runs.
    design.update(stage=stage, seeds=list(seeds), runs=len(configs) * len(seeds),
                  expected_total_requests=sum(x['expected_requests'] for x in design['loads'])
                  * len(methods) * len(seeds))
    root = Path(root).resolve()
    if root.exists():
        if not resume:
            raise ValueError('Output already exists; use --resume or choose a new output directory')
        saved = json.loads((root / 'design.json').read_text(encoding='utf-8'))
        if saved != design:
            raise ValueError('Study design changed; choose a new output directory')
    else:
        root.mkdir(parents=True)
        (root / 'design.json').write_text(json.dumps(design, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    paths = {}
    for (load, method), c in configs.items():
        path = root / 'configs' / load / f'{method}.yaml'
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if yaml.safe_load(path.read_text(encoding='utf-8')) != c:
                raise ValueError(f'Prepared config changed: {path}')
        else:
            path.write_text('# Duration controls arrivals; num_requests is the expected count.\n'
                            + yaml.safe_dump(c, sort_keys=False), encoding='utf-8')
        paths.setdefault(load, []).append(path)
    return paths, design


def summarize(root, design):
    rows, pairs = [], []
    for load in design['loads']:
        folder = root / f"eval-{load['load']}"
        source = json.loads((folder / 'aggregate.json').read_text(encoding='utf-8'))
        by_experiment = {}
        for item in source:
            if item['metric'] not in REPORT_METRICS:
                continue
            row = by_experiment.setdefault(item['experiment'], {
                'load': load['load'], 'offered_cpu_ratio': load['offered_cpu_ratio'],
                'experiment': item['experiment'], 'runs': item['runs']})
            row[item['metric']] = item['mean']
        rows.extend(by_experiment.values())
        with (folder / 'paired.csv').open(newline='', encoding='utf-8') as stream:
            pairs.extend({'load': load['load'], **r} for r in csv.DictReader(stream))
    _csv(root / 'summary.csv', rows)
    with (root / 'paired.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=['load', *PAIRED_FIELDS])
        writer.writeheader()
        writer.writerows(pairs)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--study', type=Path, default=DEFAULT_STUDY,
                        help='Study YAML listing source baseline YAMLs, common settings and stages')
    parser.add_argument('--stage', default='pilot', help='Stage name from the study YAML; default: pilot')
    parser.add_argument('--output', type=Path, help='Study directory, reused only with --resume')
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument('--methods', nargs='+', help='Override default_methods from the study YAML')
    selection.add_argument('--group', help='Select a method_groups entry from the study YAML')
    parser.add_argument('--profile', default='standard', help='Apply one shared scenario profile from the study YAML')
    parser.add_argument('--rhos', type=float, nargs='+', help='Override offered_cpu_ratios from the study YAML')
    parser.add_argument('--seeds', type=int, nargs='+', help='Override stage seeds from the study YAML')
    parser.add_argument('--duration-hours', type=float, help='Override stage duration_hours')
    parser.add_argument('--warmup-hours', type=float, help='Override stage warmup_hours')
    parser.add_argument('--prepare-only', action='store_true', help='Write validated YAMLs/design without simulation')
    parser.add_argument('--detail', choices=('full', 'compact', 'summary'),
                        help='Override artifact detail; summary omits raw request/event/QUBO files')
    parser.add_argument('--resume', action='store_true', help='Verify config/source and reuse completed runs')
    parser.add_argument('--jobs', type=int, help='Override execution.jobs from the study YAML')
    args = parser.parse_args(argv)
    root = (args.output or Path('results/distributed') / f'{args.stage}-{uuid.uuid4().hex[:12]}').resolve()
    try:
        study = load_study(args.study)
        if args.stage not in study['stages']:
            raise ValueError(f'Unknown study stage: {args.stage}')
        stage = study['stages'][args.stage]
        duration = args.duration_hours if args.duration_hours is not None else stage['duration_hours']
        warmup = args.warmup_hours if args.warmup_hours is not None else stage['warmup_hours']
        seeds = args.seeds if args.seeds is not None else stage['seeds']
        rhos = args.rhos if args.rhos is not None else study['offered_cpu_ratios']
        if args.group is not None and args.group not in study.get('method_groups', {}):
            raise ValueError(f'Unknown method group: {args.group}')
        methods = (study['method_groups'][args.group] if args.group is not None else
                   args.methods if args.methods is not None else study['default_methods'])
        jobs = validate_jobs(args.jobs if args.jobs is not None else study['execution']['jobs'])
        paths, design = prepare(root, rhos, methods, duration, warmup, seeds, args.stage, args.resume,
                                args.study, args.detail, args.profile)
        print(f"Study YAML: {args.study.resolve()}\nStudy: {root}\n"
              f"{design['num_servers']} servers / {design['num_data_centers']} DCs; {design['runs']} runs; "
              f"up to {jobs} concurrent runs; "
              f"{design['measurement_hours']:g} measurement hours/run", flush=True)
        for load in design['loads']:
            print(f"{load['load']}: lambda={load['arrival_rate_per_hour']:.4f}/h, "
                  f"expected arrivals={load['expected_requests']:.0f}, "
                  f"window={load['expected_window_requests']:.0f}", flush=True)
        if args.prepare_only:
            print('Prepared only. Use identical arguments with --resume to run this study.', flush=True)
            return
        for load, configs in paths.items():
            run_benchmark(configs, seeds, root / f'eval-{load}', resume=args.resume, jobs=jobs)
        summarize(root, design)
    except (OSError, ValueError, yaml.YAMLError) as error:
        parser.error(str(error))
    print(f"Summary: {root / 'summary.csv'}\nPaired seed intervals: {root / 'paired.csv'}", flush=True)


if __name__ == '__main__':
    main()
