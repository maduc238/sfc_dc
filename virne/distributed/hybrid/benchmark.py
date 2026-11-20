"""Matched-workload baseline runs with per-seed outputs and bootstrap intervals."""

import argparse
from contextlib import contextmanager
import csv
from copy import deepcopy
import json
import math
import multiprocessing
import os
from pathlib import Path
import random
import signal
from statistics import mean
import yaml

from virne.distributed.config import load_config
from virne.distributed.hybrid.pace import PACE_COUNTERS
from virne.distributed.runner import run, source_fingerprint


METRICS = ('acceptance_ratio', 'accepted_requests', 'energy_kwh', 'server_energy_kwh',
           'network_energy_kwh', 'energy_kwh_per_accepted_request',
           'time_weighted_service_latency_ms', 'p95_latency_ms',
           'mean_decision_delay_ms', 'mean_queue_delay_ms',
           'p95_admission_delay_ms', 'p99_admission_delay_ms', 'mean_placement_time_ms',
           'runtime_s', 'rejected_deadline', 'rejected_stale', 'decision_budget_exhaustions',
           'mean_cpu_utilization', 'mean_wan_utilization',
           'backend_calls', 'qaoa_sim_calls', 'backend_wall_time_ms', 'admitted_rescues',
           'samples', 'placement_feasible_samples', 'routing_feasible_samples',
           'qpu_dispatch_attempts', 'estimated_qpu_cost',
           'migration_attempts', 'migration_completed', 'migration_aborted',
           'migration_data_mbit_hops', 'migration_downtime_s', 'migration_p95_downtime_s',
           'migration_normalized_cost', 'migration_planning_wall_time_ms')
METRICS += ('window_acceptance_ratio', 'window_accepted_requests', 'window_offered_requests',
            'window_energy_kwh', 'window_energy_kwh_per_accepted_request', 'window_mean_power_w',
            'window_served_vnf_hours', 'window_energy_kwh_per_served_vnf_hour',
            'window_mean_cpu_utilization', 'window_mean_memory_utilization', 'window_mean_wan_utilization',
            'window_mean_active_servers', 'window_mean_service_latency_ms',
            'window_mean_placement_time_ms', 'window_p95_admission_delay_ms',
            'window_migrations_completed', 'window_migrations_aborted',
            'window_migration_downtime_s', 'window_migration_data_mbit_hops',
            'window_migration_normalized_cost', 'window_migration_planning_wall_ms')
METRICS += ('validation_attempts', 'validation_truncated_calls') + tuple(
    'phase_' + phase + '_wall_time_ms' for phase in ('incumbent', 'candidate', 'qubo', 'sampling', 'validation', 'other'))
METRICS += ('guided_utility_stops', 'guided_validation_batches', 'guided_feedback_updates',
            'guided_feedback_repairs', 'guided_duplicate_skips', 'guided_explorations')
METRICS += ('guided_validation_limit_stops', 'guided_mean_proposal_compatibility_loss')
METRICS += ('migration_planning_budget_exhaustions',) + tuple('pace_' + key for key in PACE_COUNTERS)
METRICS += ('window_mean_solver_ms_per_started_request', 'window_started_decisions',
            'window_queue_expirations', 'window_rejected_deadline', 'window_rejected_stale')
METRICS += ('rejected_compute', 'rejected_bandwidth', 'rejected_latency', 'rejected_budget',
            'window_rejected_compute', 'window_rejected_bandwidth', 'window_rejected_latency',
            'window_rejected_budget', 'window_mean_peak_fabric_link_utilization', 'window_migration_route_changes',
            'window_mean_busy_workers', 'window_mean_queued_requests', 'classical_backend_calls',
            'exact_calls', 'sa_calls', 'qaoa_sim_sampled_calls', 'qaoa_sim_size_limit_calls',
            'qaoa_statevector_evaluations') + tuple(
    backend + suffix for backend in ('exact', 'sa', 'qaoa_sim')
    for suffix in ('_completed_calls', '_validated_samples', '_routing_feasible_samples', '_wall_time_ms'))

PAIRED_METRICS = ('window_acceptance_ratio', 'window_energy_kwh',
                  'window_energy_kwh_per_served_vnf_hour', 'window_mean_service_latency_ms',
                  'window_mean_placement_time_ms', 'window_p95_admission_delay_ms',
                  'window_migration_normalized_cost', 'window_migration_downtime_s',
                  'window_served_vnf_hours', 'window_mean_solver_ms_per_started_request',
                  'window_rejected_deadline')
PAIRED_FIELDS = ('reference', 'experiment', 'metric', 'paired_seeds', 'mean_delta',
                 'bootstrap_95_low', 'bootstrap_95_high')
THREAD_LIMITS = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS')


def validate_jobs(jobs):
    if type(jobs) is not int or jobs < 1:
        raise ValueError('jobs must be a positive integer')
    return jobs


@contextmanager
def _single_thread_children():
    previous = {name: os.environ.get(name) for name in THREAD_LIMITS}
    try:
        for name in THREAD_LIMITS:
            os.environ[name] = '1'
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _initialize_worker():
    # The parent sets these before spawn, including imports by the child main module.
    for name in THREAD_LIMITS:
        os.environ[name] = '1'
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def execute_jobs(worker, pending, jobs):
    """Release each simulation's event history when its worker exits."""
    validate_jobs(jobs)
    if pending and jobs > 1:
        with _single_thread_children(), multiprocessing.get_context('spawn').Pool(
                min(jobs, len(pending)), initializer=_initialize_worker, maxtasksperchild=1) as pool:
            yield from pool.imap_unordered(worker, pending, chunksize=1)
    else:
        yield from map(worker, pending)


def _run_job(job):
    config, path, execution = job
    folder, summary = run(config, source=path, execution=execution)
    # Keep full graphs and event histories in the worker; return only the summary.
    return {**summary, 'output_dir': str(folder), 'source_config': str(Path(path).resolve())}


def _checkpoint(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False), encoding='utf-8')
    temporary.replace(path)


def aggregate(rows):
    rng = random.Random(1701)
    grouped = {}
    for row in rows:
        grouped.setdefault(row['experiment'], []).append(row)
    result = []
    for name, group in sorted(grouped.items()):
        for metric in METRICS:
            # An undefined latency/utilization is missing data, never a zero sample.
            values = [row[metric] for row in group if row.get(metric) is not None]
            if any(not isinstance(v, (int, float)) or not math.isfinite(v) for v in values):
                raise ValueError(f'Invalid numeric benchmark metric: {metric}')
            lower = upper = None
            if len(values) >= 2:
                estimates = sorted(mean(rng.choices(values, k=len(values))) for _ in range(2000))
                lower, upper = estimates[49], estimates[1949]
            result.append({'experiment': name, 'metric': metric, 'runs': len(group),
                           'valid_runs': len(values), 'mean': mean(values) if values else None,
                           'bootstrap_95_low': lower, 'bootstrap_95_high': upper})
    return result


def paired_intervals(rows):
    """Differences on shared seeds; hours within a seed are not independent repeats."""
    rng = random.Random(1702)
    groups, references = {}, set()
    for row in rows:
        groups.setdefault(row['experiment'], {})[row['seed']] = row
        if row.get('solver') in ('hre', 'first_fit_decreasing'):
            references.add(row['experiment'])
    result = []
    for reference in sorted(references):
        for name, runs in sorted(groups.items()):
            if name == reference:
                continue
            for metric in PAIRED_METRICS:
                values = []
                for seed in sorted(runs.keys() & groups[reference].keys()):
                    a, b = runs[seed].get(metric), groups[reference][seed].get(metric)
                    if a is not None and b is not None:
                        values.append(a - b)
                if not values:
                    continue
                low = high = None
                if len(values) >= 2:
                    estimates = sorted(mean(rng.choices(values, k=len(values))) for _ in range(2000))
                    low, high = estimates[49], estimates[1949]
                result.append(dict(zip(PAIRED_FIELDS, (reference, name, metric, len(values),
                                                       mean(values), low, high))))
    return result


def run_benchmark(paths, seeds, output, resume=False, allow_topology_differences=False, jobs=1):
    validate_jobs(jobs)
    if not paths or not seeds:
        raise ValueError('At least one config and seed are required')
    configs = [load_config(path) for path in paths]
    if len(set(seeds)) != len(seeds):
        raise ValueError('Benchmark seeds must be distinct')
    if len({c['experiment']['name'] for c in configs}) != len(configs):
        raise ValueError('Benchmark experiment names must be distinct')
    # Workload and controller resource/timing assumptions must match before spending compute.
    for c in configs[1:]:
        if c['workload'] != configs[0]['workload'] or c['control_plane'] != configs[0]['control_plane']:
            raise ValueError('Matched benchmark requires identical workload and control_plane settings')
        if c['migration'] != configs[0]['migration']:
            raise ValueError('Matched benchmark requires identical migration settings and costs')
        if c['measurement'] != configs[0]['measurement'] or c['power'] != configs[0]['power']:
            raise ValueError('Matched benchmark requires identical measurement windows and power settings')
        if not allow_topology_differences and c['topology'] != configs[0]['topology']:
            raise ValueError('Matched benchmark requires identical distributed topology/server settings')
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=resume)
    rows = []
    if resume and (output / 'comparison.json').exists():
        rows = json.loads((output / 'comparison.json').read_text(encoding='utf-8'))
    execution = {'schema_version': 1, 'jobs': jobs, 'start_method': 'spawn' if jobs > 1 else 'direct',
                 'thread_limits': {k: '1' if jobs > 1 else os.environ.get(k) for k in THREAD_LIMITS}}
    if rows and (output / 'execution.json').exists():
        saved_execution = json.loads((output / 'execution.json').read_text(encoding='utf-8'))
        if saved_execution != execution:
            raise ValueError('Execution settings changed; resume with the same --jobs and thread limits '
                             'or choose a new output directory')
    completed = {}
    configured = {c['experiment']['name']: c for c in configs}
    hashes = {}

    def check_workload(row):
        seed = row['seed']
        offered = row['workload_sha256'], row['migration_profiles_sha256']
        expected = hashes.setdefault(seed, offered)
        if offered[0] != expected[0]:
            raise ValueError('Offered workload mismatch')
        if offered[1] != expected[1]:
            raise ValueError('Migration profiles mismatch')

    for row in rows:
        key = row['experiment'], row['seed']
        if key in completed or key[0] not in configured or key[1] not in seeds:
            raise ValueError('Resume checkpoint has duplicate or unexpected runs')
        folder = Path(row['output_dir']).resolve()
        if not folder.is_relative_to(output):
            raise ValueError('Resume artifacts must remain inside the benchmark output directory')
        saved = yaml.safe_load((folder / 'config.yaml').read_text(encoding='utf-8'))
        expected = deepcopy(configured[key[0]])
        expected['experiment'].update(seed=key[1], output_dir=str(output / 'runs'))
        manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
        if saved != expected or manifest['source_sha256'] != source_fingerprint():
            raise ValueError('Source/config changed since checkpoint; start a new output directory')
        if manifest.get('execution', {}).get('jobs', 1) != jobs:
            raise ValueError('Cannot mix sequential and parallel execution settings in a resumed benchmark')
        if expected['hybrid']['policy']['name'] == 'learned':
            old_model = json.loads((folder / 'policy_model.json').read_text(encoding='utf-8'))
            new_model = json.loads(Path(expected['hybrid']['policy']['model_path']).read_text(encoding='utf-8'))
            if old_model != new_model:
                raise ValueError('Learned policy changed since checkpoint')
        completed[key] = row
        check_workload(row)
    pending = []
    for seed in seeds:
        for path, original in zip(paths, configs):
            c = deepcopy(original)
            c['experiment'].update(seed=seed, output_dir=str(output / 'runs'))
            key = c['experiment']['name'], seed
            if key in completed:
                print(f"Reuse {key[0]} seed={seed}", flush=True)
            else:
                pending.append((c, str(path), execution))
    _checkpoint(output / 'execution.json', execution)
    order = {(c['experiment']['name'], seed): i for i, (seed, c) in enumerate(
        (seed, c) for seed in seeds for c in configs)}

    def record(row):
        check_workload(row)
        rows.append(row)
        # Completion order must not change bootstrap inputs or checkpoint ordering.
        rows.sort(key=lambda r: order[(r['experiment'], r['seed'])])
        _checkpoint(output / 'comparison.json', rows)
        print(f"[{len(rows)}/{len(order)}] {row['experiment']} seed={row['seed']}: "
              f"{row['accepted_requests']}/{row['num_requests']} accepted; calls={row['backend_calls']}", flush=True)

    if pending and jobs > 1:
        workers = min(jobs, len(pending))
        print(f'Parallel execution: {workers} processes; one BLAS/OpenMP thread per process', flush=True)
        print('Solver wall times and wall-clock budgets include host contention. '
              'Use --jobs 1 for isolated timing measurements.', flush=True)
    for row in execute_jobs(_run_job, pending, jobs):
        record(row)
    rows.sort(key=lambda r: order[(r['experiment'], r['seed'])])
    aggregated = aggregate(rows)
    for name, data in [('comparison', rows), ('aggregate', aggregated)]:
        with (output / f'{name}.csv').open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(dict.fromkeys(k for row in data for k in row)))
            writer.writeheader()
            writer.writerows(data)
    (output / 'aggregate.json').write_text(json.dumps(aggregated, indent=2, allow_nan=False), encoding='utf-8')
    with (output / 'paired.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=PAIRED_FIELDS)
        writer.writeheader()
        writer.writerows(paired_intervals(rows))
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description='Compare baseline YAMLs under matched workload/controller budgets.')
    parser.add_argument('--configs', required=True, nargs='+', type=Path)
    parser.add_argument('--seeds', nargs='+', type=int, default=[11, 22, 33])
    parser.add_argument('--output', required=True, type=Path, help='New output directory; existing runs are never overwritten')
    parser.add_argument('--resume', action='store_true', help='Reuse verified completed runs in this output directory')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Independent simulation processes; default: 1 for isolated timing measurements')
    parser.add_argument('--allow-topology-differences', action='store_true',
                        help='Only for explicit architecture comparisons, not algorithm comparisons')
    args = parser.parse_args(argv)
    try:
        run_benchmark(args.configs, args.seeds, args.output, args.resume,
                      args.allow_topology_differences, args.jobs)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
