"""Run matched distributed scenarios and publish one portable directory per method."""

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from tempfile import NamedTemporaryFile, TemporaryDirectory
import uuid

import yaml

from virne.distributed.experiments import load_study, study_configs
from virne.distributed.hybrid.benchmark import execute_jobs, validate_jobs
from virne.distributed.model import Substrate
from virne.distributed.runner import run, source_fingerprint


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def _yaml(path):
    return yaml.safe_load(path.read_text(encoding='utf-8'))


def _labels(values, kind):
    if (not isinstance(values, list) or not values
            or any(not isinstance(v, str) or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]*', v) for v in values)
            or len({v.casefold() for v in values}) != len(values)):
        raise ValueError(f'Expected distinct {kind} names containing letters, numbers, underscores or hyphens')
    return values


def environment_hash(config):
    matched = {key: config[key] for key in ('topology', 'workload', 'migration', 'control_plane', 'power', 'measurement')}
    matched['placement'] = {key: config['solver'][key] for key in ('scope', 'allow_colocation', 'k_paths')}
    return digest(matched)


def _scenario(study_path, study, path):
    spec = _yaml(path)
    required = {'schema_version', 'simulation', 'name', 'stage', 'study', 'rhos', 'references'}
    if (not isinstance(spec, dict) or not required <= spec.keys()
            or spec.keys() - required - {'group', 'methods', 'profile', 'jobs'}
            or spec['simulation'] != 'distributed_scenario' or spec['schema_version'] != 1
            or not isinstance(spec['study'], str) or (path.parent / spec['study']).resolve() != study_path):
        raise ValueError(f'Invalid scenario or parent study in {path}')
    name, = _labels([spec['name']], 'scenario')
    if ('group' in spec) == ('methods' in spec):
        raise ValueError(f'{name} requires exactly one of group or methods')
    groups = study.get('method_groups', {})
    if 'group' in spec and (not isinstance(spec['group'], str) or spec['group'] not in groups):
        raise ValueError(f'Unknown method group in {name}')
    methods = _labels(spec['methods'] if 'methods' in spec else groups[spec['group']], 'method')
    references = _labels(spec['references'], 'reference')
    if set(references) - set(methods):
        raise ValueError(f'All references must run in scenario {name}')
    rhos = spec['rhos']
    if not isinstance(rhos, list) or not rhos or any(type(r) not in (int, float) for r in rhos):
        raise ValueError(f'Invalid load ratios in {name}')
    if not isinstance(spec['stage'], str) or spec['stage'] not in study['stages']:
        raise ValueError(f'Unknown stage in {name}')
    stage = study['stages'][spec['stage']]
    jobs = validate_jobs(spec.get('jobs', study['execution']['jobs']))
    profile = spec.get('profile', 'standard')
    if not isinstance(profile, str):
        raise ValueError(f'Invalid profile in {name}')
    configs, design = study_configs('.', rhos, methods, stage['duration_hours'], stage['warmup_hours'],
                                    study_path, detail='summary', profile=profile)
    Substrate(next(iter(configs.values())))
    for config in configs.values():
        config['experiment']['output_dir'] = 'results/distributed/standalone'
        if not config['migration']['enabled']:
            raise ValueError(f'{name} requires the shared migration model')
        if config['hybrid']['policy']['name'] == 'learned':
            from virne.distributed.hybrid.policy import SelectionPolicy
            SelectionPolicy(config['hybrid'])
    metadata = {k: design[k] for k in ('num_servers', 'num_data_centers', 'topology_sha256', 'duration_hours',
                                        'warmup_hours', 'measurement_hours', 'loads')}
    metadata.update(name=name, stage=spec['stage'], profile=profile, seeds=stage['seeds'], jobs=jobs,
                    methods=methods, references=references, runs=len(configs) * len(stage['seeds']))
    return metadata, configs


def evaluation_design(entry):
    entry = Path(entry).resolve()
    raw = _yaml(entry)
    if not isinstance(raw, dict):
        raise ValueError('Expected an evaluation or scenario YAML')
    single = raw.get('simulation') == 'distributed_scenario'
    if single and not isinstance(raw.get('study'), str):
        raise ValueError('A scenario must name its parent study YAML')
    study_path = (entry.parent / raw['study']).resolve() if single else entry
    study = load_study(study_path)
    options = study.get('evaluation')
    if (study.get('simulation') != 'distributed_evaluation' or not isinstance(options, dict)
            or set(options) != {'output_dir', 'scenarios'}
            or not isinstance(options['output_dir'], str) or not options['output_dir'].strip()
            or not isinstance(options['scenarios'], list) or not options['scenarios']
            or any(not isinstance(v, str) or not v.strip() for v in options['scenarios'])):
        raise ValueError(f'Invalid distributed evaluation settings in {study_path}')
    files = [entry] if single else [(study_path.parent / p).resolve() for p in options['scenarios']]
    plans = [_scenario(study_path, study, path) for path in files]
    _labels([meta['name'] for meta, _ in plans], 'scenario')
    return Path(options['output_dir']).expanduser().resolve(), plans


def _document(path, value, *, as_yaml=False):
    """An interrupted write must never leave a half-written index or config."""
    if path.exists():
        if (_yaml(path) if as_yaml else read_json(path)) != value:
            raise ValueError(f'Configuration or source changed at {path}. Start a new campaign.')
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    body = yaml.safe_dump(value, sort_keys=False) if as_yaml else json.dumps(value, indent=2, allow_nan=False) + '\n'
    with NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as stream:
        stream.write(body)
        temporary = Path(stream.name)
    temporary.replace(path)


def trial_identity(method, load):
    return {'campaign_id': method['campaign_id'], 'scenario': method['scenario'], 'method': method['method'],
            'load': load, 'config_sha256': method['configs'][load]['sha256'],
            'environment_sha256': method['configs'][load]['environment_sha256']}


def checked_result(folder, method, load, seed):
    """Verify a published trial using only files carried by the method directory."""
    manifest = read_json(folder / 'manifest.json')
    summary_path = folder / 'summary.json'
    if hashlib.sha256(summary_path.read_bytes()).hexdigest() != manifest.get('summary_sha256'):
        raise ValueError(f'Summary checksum mismatch at {folder}')
    for filename in ('config.yaml', 'per_dc.csv', 'timeseries.csv', 'summary.csv'):
        if not (folder / filename).is_file():
            raise ValueError(f'Incomplete trial: missing {folder / filename}')
    if hashlib.sha256((folder / 'config.yaml').read_bytes()).hexdigest() != manifest.get('resolved_config_sha256'):
        raise ValueError(f'Resolved config checksum mismatch at {folder}')
    model_hash = method['configs'][load].get('policy_sha256')
    if model_hash and digest(read_json(folder / 'policy_model.json')) != model_hash:
        raise ValueError(f'Learned policy mismatch at {folder}')
    row = read_json(summary_path)
    expected = {**trial_identity(method, load), 'seed': seed, 'source_sha256': method['source_sha256'],
                'experiment': method['configs'][load]['experiment'], 'execution_jobs': method['jobs'],
                'num_servers': method['num_servers'], 'num_data_centers': method['num_data_centers']}
    if (any(row.get(k) != v for k, v in expected.items()) or row.get('resources_released') is not True
            or row.get('migration_enabled') is not True or row.get('qpu_dispatch_attempts') != 0
            or manifest.get('source_sha256') != expected['source_sha256'] or manifest.get('qpu_enabled') is not False
            or manifest.get('output_detail') != 'summary'
            or any(not row.get(k) or row[k] != manifest.get(k) for k in ('workload_sha256', 'migration_profiles_sha256'))):
        raise ValueError(f'Invalid trial identity, resources or workload at {folder}')
    return row


def match_workload(seen, row):
    key = row['scenario'], row['load'], row['seed']
    offered = tuple(row[k] for k in ('environment_sha256', 'workload_sha256', 'migration_profiles_sha256',
                                     'num_requests', 'window_offered_requests'))
    if seen.setdefault(key, offered) != offered:
        raise ValueError(f'Mismatched workload, migration profiles or environment for {key}')


def _run_trial(task):
    load_dir, method, load, seed, template = task
    target = load_dir / f'seed-{seed}'
    config = deepcopy(template)
    config['experiment']['seed'] = seed
    execution = {'jobs': method['jobs'], 'start_method': 'spawn' if method['jobs'] > 1 else 'direct',
                 'trial': trial_identity(method, load)}
    with TemporaryDirectory(prefix='.pending-', dir=load_dir) as work:
        staged, _ = run(config, source=load_dir / 'config.yaml', execution=execution, output_folder=Path(work) / 'result')
        manifest_path = staged / 'manifest.json'
        manifest = read_json(manifest_path)
        manifest['summary_sha256'] = hashlib.sha256((staged / 'summary.json').read_bytes()).hexdigest()
        manifest['resolved_config_sha256'] = hashlib.sha256((staged / 'config.yaml').read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        checked_result(staged, method, load, seed)
        try:
            staged.rename(target)
        except OSError:
            if not target.is_dir():
                raise
    return checked_result(target, method, load, seed)


def _method_index(common, scenario, configs, method):
    settings = {}
    for (load, name), config in configs.items():
        if name == method:
            settings[load] = {'sha256': digest(config), 'environment_sha256': environment_hash(config),
                              'experiment': config['experiment']['name']}
            if config['hybrid']['policy']['name'] == 'learned':
                settings[load]['policy_sha256'] = digest(read_json(Path(config['hybrid']['policy']['model_path'])))
    return {**common, **{k: scenario[k] for k in ('stage', 'profile', 'seeds', 'references', 'jobs', 'loads',
                                                  'num_servers', 'num_data_centers', 'topology_sha256',
                                                  'duration_hours', 'warmup_hours', 'measurement_hours')},
            'scenario': scenario['name'], 'method': method, 'configs': settings}


def evaluate(entry, *, resume=None, validate_only=False):
    base, plans = evaluation_design(entry)
    for scenario, _ in plans:
        print(f"{scenario['name']}: {scenario['runs']} trials, {scenario['num_servers']} servers, "
              f"{scenario['jobs']} concurrent simulations", flush=True)
    print(f"Total: {sum(s['runs'] for s, _ in plans)} trials", flush=True)
    if validate_only:
        return
    if resume is None:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        campaign_id = f'{stamp}-{uuid.uuid4().hex[:8]}'
        root = base / campaign_id
        root.mkdir(parents=True, exist_ok=False)
    else:
        root = Path(resume).resolve()
        campaign_id = read_json(root / 'campaign.json')['campaign_id']
    print(f'Campaign: {root}', flush=True)
    common = {'schema_version': 1, 'campaign_id': campaign_id, 'source_sha256': source_fingerprint()}
    scenarios, methods = {}, {}
    for scenario, configs in plans:
        name = scenario['name']
        indices = {m: _method_index(common, scenario, configs, m) for m in scenario['methods']}
        methods[name] = indices
        scenarios[name] = {**common, **scenario, 'method_hashes': {m: digest(v) for m, v in indices.items()}}
    _document(root / 'campaign.json', {**common, 'scenarios': {n: digest(s) for n, s in scenarios.items()}})
    for scenario, configs in plans:
        name = scenario['name']
        _document(root / name / 'scenario.json', scenarios[name])
        for method, index in methods[name].items():
            _document(root / name / method / 'method.json', index)
            for load in index['configs']:
                _document(root / name / method / load / 'config.yaml', configs[load, method], as_yaml=True)
    for scenario, configs in plans:
        name = scenario['name']
        pending, seen = [], {}
        for seed in scenario['seeds']:
            for (load, method), config in configs.items():
                load_dir = root / name / method / load
                target = load_dir / f'seed-{seed}'
                index = methods[name][method]
                if target.exists():
                    match_workload(seen, checked_result(target, index, load, seed))
                else:
                    pending.append((load_dir, index, load, seed, config))
        total = scenario['runs']
        done = total - len(pending)
        print(f'{name}: {done}/{total} already completed', flush=True)
        for row in execute_jobs(_run_trial, pending, scenario['jobs']):
            match_workload(seen, row)
            done += 1
            print(f"[{name} {done}/{total}] {row['method']} {row['load']} seed={row['seed']} "
                  f"{row['accepted_requests']}/{row['num_requests']} accepted", flush=True)
    print(f'Complete. Summarize with: python summarize_results.py --dir "{root}"', flush=True)
    return root
