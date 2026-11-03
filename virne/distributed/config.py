"""Strict, standalone YAML configuration for the distributed DC simulator."""

from copy import deepcopy
from math import isfinite
from pathlib import Path

import yaml


DEFAULTS = {
    'schema_version': 1,
    'simulation': 'distributed_dc',
    'experiment': {'name': 'distributed_hre', 'seed': 42, 'output_dir': 'results/distributed'},
    'topology': {
        'wan': {'num_nodes': 5, 'edges': [[0, 1], [1, 2], [2, 3], [3, 0],
                                        [0, 4], [1, 4], [2, 4], [3, 4]],
                'bandwidth_mbps': 100.0, 'latency_ms': 5.0},
        'fabric': {'type': 'leaf_spine', 'servers_per_rack': 4, 'num_spines': 2,
                   'fat_tree_k': 4, 'bandwidth_mbps': 1000.0, 'latency_ms': 0.05},
        'server': {'cpu': 8.0, 'memory_gb': 64.0},
        'data_centers': [{'id': f'dc{i}', 'wan_node': i, 'num_servers': 16} for i in range(4)],
    },
    'workload': {
        'num_requests': 200, 'arrival_rate_per_hour': 40.0, 'mean_lifetime_hours': 2.0,
        'ingress_nodes': [0, 1, 2, 3], 'vnfs_per_request': [4, 12],
        'cpu': [1.2, 2.4], 'memory_gb': [2.0, 8.0], 'bandwidth_mbps': [10.0, 90.0],
        'max_latency_ms': [30.0, 120.0], 'processing_delay_ms': 0.25,
        'return_to_ingress': False,
    },
    'solver': {'name': 'hre', 'scope': 'single_dc', 'allow_colocation': False,
               'k_paths': 4},
    'hybrid': {
        'incumbent_solver': 'hre', 'max_dcs': 2, 'neighborhood_size': 4,
        'candidates_per_vnf': 3, 'max_neighborhoods': 2, 'max_variables': 32,
        'max_backend_calls': 2, 'max_validations': 256, 'budget_ms': 500.0,
        'optimize_existing': True,
        'objective': {'power_weight': 1.0, 'latency_weight': 1.0,
                      'power_reference_w': 1000.0, 'latency_reference_ms': 100.0},
        'policy': {'name': 'rule', 'static_backend': 'sa', 'quantum_backend': 'qaoa_sim',
                   'exact_max_assignments': 256, 'exact_ms_per_assignment': 0.25,
                   'minimum_remaining_ms': 2.0, 'quantum_expected_gain': 0.0,
                   'quantum_minimum_gain': 0.05, 'quantum_expected_ms': 100.0,
                   'model_path': ''},
        'exact': {'max_assignments': 4096},
        'sa': {'reads': 24, 'sweeps': 80, 'beta_start': 0.1, 'beta_end': 5.0},
        'qaoa': {'max_variables': 16, 'layers': 1, 'evaluations': 12, 'shots': 128,
                 'readout_flip_probability': 0.0},
        'qa': {'enabled': False, 'solver': '', 'num_reads': 100, 'annealing_time_us': 20.0,
               'max_calls_per_run': 0, 'estimated_cost_per_call': 0.0,
               'max_cost_per_run': 0.0, 'timeout_ms': 20000.0},
    },
    'control_plane': {
        'enabled': False, 'workers': 1, 'decision_timeout_ms': 1000.0,
        'latency_mode': 'modeled', 'base_latency_ms': 1.0, 'validation_latency_ms': 0.1,
        'backend_latency_ms': {'exact': 2.0, 'sa': 5.0, 'qaoa_sim': 25.0, 'qa': 100.0},
        'trace_ms': [],
    },
    'power': {'server_idle_w': 100.0, 'server_peak_w': 200.0, 'server_off_w': 0.0,
              'switch_idle_w': 39.0, 'switch_port_w': 0.9, 'switch_off_w': 0.0,
              'wan_always_on': True},
}


def _merge(defaults, supplied, path='config'):
    if not isinstance(supplied, dict):
        raise ValueError(f'{path} must be a mapping')
    unknown = supplied.keys() - defaults.keys()
    if unknown:
        raise ValueError(f'{path}: unknown keys: {", ".join(sorted(map(str, unknown)))}')
    result = deepcopy(defaults)
    for key, value in supplied.items():
        result[key] = _merge(defaults[key], value, f'{path}.{key}') if isinstance(
            defaults[key], dict) else deepcopy(value)
    return result


def _number(value, label, minimum=0, integer=False, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise ValueError(f'{label} must be a finite number')
    if value < minimum or (positive and value == minimum) or (integer and not isinstance(value, int)):
        raise ValueError(f'{label} has an invalid value: {value}')


def validate_config(config):
    if config['schema_version'] != 1 or config['simulation'] != 'distributed_dc':
        raise ValueError('Expected schema_version: 1 and simulation: distributed_dc')
    exp, topo, work, solver = (config[k] for k in ('experiment', 'topology', 'workload', 'solver'))
    if not isinstance(exp['name'], str) or not exp['name'] or any(
        c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in exp['name']
    ):
        raise ValueError('experiment.name must contain only letters, digits, underscores or hyphens')
    if not isinstance(exp['output_dir'], str) or not exp['output_dir'].strip():
        raise ValueError('experiment.output_dir must be a nonempty path')
    _number(exp['seed'], 'experiment.seed', integer=True)
    wan, fabric = topo['wan'], topo['fabric']
    _number(wan['num_nodes'], 'wan.num_nodes', integer=True, positive=True)
    for kind, settings in [('wan', wan), ('fabric', fabric)]:
        # null denotes an unconstrained capacity; never use floating-point infinity in outputs.
        if settings['bandwidth_mbps'] is not None:
            _number(settings['bandwidth_mbps'], f'{kind}.bandwidth_mbps', positive=True)
        _number(settings['latency_ms'], f'{kind}.latency_ms')
    if not isinstance(wan['edges'], list):
        raise ValueError('wan.edges must be a list of undirected node pairs')
    seen_edges = set()
    for edge in wan['edges']:
        if not isinstance(edge, list) or len(edge) != 2 or any(
            isinstance(n, bool) or not isinstance(n, int) or not 0 <= n < wan['num_nodes'] for n in edge
        ) or edge[0] == edge[1]:
            raise ValueError(f'Invalid WAN edge: {edge}')
        key = tuple(sorted(edge))
        if key in seen_edges:
            raise ValueError(f'Duplicate WAN edge: {edge}')
        seen_edges.add(key)
    if fabric['type'] not in ('leaf_spine', 'fat_tree'):
        raise ValueError('fabric.type must be leaf_spine or fat_tree')
    for key in ('servers_per_rack', 'num_spines', 'fat_tree_k'):
        _number(fabric[key], f'fabric.{key}', integer=True, positive=True)
    if fabric['fat_tree_k'] % 2:
        raise ValueError('fabric.fat_tree_k must be even')
    for key, value in topo['server'].items():
        _number(value, f'server.{key}', positive=True)
    if not isinstance(topo['data_centers'], list) or not topo['data_centers']:
        raise ValueError('topology.data_centers must be a nonempty list')
    dc_ids = set()
    for dc in topo['data_centers']:
        if not isinstance(dc, dict) or set(dc) - {'id', 'wan_node', 'num_servers', 'cpu', 'memory_gb'}:
            raise ValueError('Each data center accepts id, wan_node, num_servers, cpu and memory_gb')
        if not {'id', 'wan_node', 'num_servers'} <= dc.keys():
            raise ValueError('Each data center requires id, wan_node and num_servers')
        if not isinstance(dc['id'], str) or not dc['id'] or '/' in dc['id'] or dc['id'] in dc_ids:
            raise ValueError('Data center IDs must be unique nonempty strings without /')
        dc_ids.add(dc['id'])
        _number(dc['wan_node'], 'dc.wan_node', integer=True)
        if dc['wan_node'] >= wan['num_nodes']:
            raise ValueError('dc.wan_node is outside the WAN')
        _number(dc['num_servers'], 'dc.num_servers', integer=True, positive=True)
        if fabric['type'] == 'fat_tree' and dc['num_servers'] != fabric['fat_tree_k'] ** 3 // 4:
            raise ValueError('For a full k-ary fat tree, num_servers must equal k**3 / 4')
        for key in ('cpu', 'memory_gb'):
            if key in dc:
                _number(dc[key], f'dc.{key}', positive=True)
    _number(work['num_requests'], 'workload.num_requests', integer=True, positive=True)
    for key in ('arrival_rate_per_hour', 'mean_lifetime_hours'):
        _number(work[key], f'workload.{key}', positive=True)
    ingress = work['ingress_nodes']
    if not isinstance(ingress, list) or not ingress or any(
        isinstance(n, bool) or not isinstance(n, int) or not 0 <= n < wan['num_nodes'] for n in ingress
    ) or len(set(ingress)) != len(ingress):
        raise ValueError('workload.ingress_nodes must be distinct WAN node IDs')
    for key in ('vnfs_per_request', 'cpu', 'memory_gb', 'bandwidth_mbps', 'max_latency_ms'):
        bounds = work[key]
        if key == 'max_latency_ms' and bounds is None:
            continue
        if not isinstance(bounds, list) or len(bounds) != 2:
            raise ValueError(f'workload.{key} must be [minimum, maximum]')
        for value in bounds:
            _number(value, f'workload.{key}', integer=key == 'vnfs_per_request', positive=True)
        if bounds[0] > bounds[1]:
            raise ValueError(f'workload.{key} bounds are reversed')
    _number(work['processing_delay_ms'], 'workload.processing_delay_ms')
    if solver['name'] not in ('random', 'first_fit_decreasing', 'best_fit', 'hre', 'ahq'):
        raise ValueError('solver.name must be random, first_fit_decreasing, best_fit, hre or ahq')
    if solver['scope'] not in ('single_dc', 'cross_dc'):
        raise ValueError('solver.scope must be single_dc or cross_dc')
    _number(solver['k_paths'], 'solver.k_paths', integer=True, positive=True)
    for label, value in [('solver.allow_colocation', solver['allow_colocation']),
                         ('workload.return_to_ingress', work['return_to_ingress']),
                         ('power.wan_always_on', config['power']['wan_always_on'])]:
        if not isinstance(value, bool):
            raise ValueError(f'{label} must be a YAML boolean')
    for key, value in config['power'].items():
        if key != 'wan_always_on':
            _number(value, f'power.{key}')
    p = config['power']
    if not p['server_off_w'] <= p['server_idle_w'] <= p['server_peak_w']:
        raise ValueError('Server power must satisfy off <= idle <= peak')
    if p['switch_off_w'] > p['switch_idle_w']:
        raise ValueError('Switch power must satisfy off <= idle')
    _validate_hybrid(config)
    return config


def _validate_hybrid(config):
    h, control = config['hybrid'], config['control_plane']
    for key in ('max_dcs', 'neighborhood_size', 'candidates_per_vnf', 'max_neighborhoods',
                'max_variables', 'max_backend_calls', 'max_validations'):
        _number(h[key], f'hybrid.{key}', integer=True, positive=True)
    for label, value in [('hybrid.budget_ms', h['budget_ms']),
                         ('control_plane.decision_timeout_ms', control['decision_timeout_ms'])]:
        _number(value, label, positive=True)
    for value in (h['optimize_existing'], h['qa']['enabled'], control['enabled']):
        if not isinstance(value, bool):
            raise ValueError('hybrid/control flags must be YAML booleans')
    if h['incumbent_solver'] not in ('hre', 'best_fit', 'first_fit_decreasing'):
        raise ValueError('Invalid hybrid.incumbent_solver')
    if config['solver']['name'] == 'ahq' and (
        config['solver']['scope'] != 'single_dc' or config['solver']['allow_colocation']
    ):
        raise ValueError('AHQ v1 requires single_dc and allow_colocation: false')
    for key, value in h['objective'].items():
        _number(value, f'hybrid.objective.{key}', positive='reference' in key)
    if h['objective']['power_weight'] + h['objective']['latency_weight'] <= 0:
        raise ValueError('At least one objective weight must be positive')
    policy = h['policy']
    if policy['name'] not in ('rule', 'static', 'learned'):
        raise ValueError('hybrid.policy.name must be rule, static or learned')
    if policy['static_backend'] not in ('exact', 'sa', 'qaoa_sim', 'qa'):
        raise ValueError('Invalid static_backend')
    if policy['quantum_backend'] not in ('qaoa_sim', 'qa'):
        raise ValueError('quantum_backend must be qaoa_sim or qa')
    for key in ('exact_max_assignments',):
        _number(policy[key], f'policy.{key}', positive=True, integer=True)
    for key in ('exact_ms_per_assignment', 'minimum_remaining_ms', 'quantum_expected_gain',
                'quantum_minimum_gain', 'quantum_expected_ms'):
        _number(policy[key], f'policy.{key}', positive=key in ('exact_ms_per_assignment', 'quantum_expected_ms'))
    if not isinstance(policy['model_path'], str) or (policy['name'] == 'learned' and not policy['model_path']):
        raise ValueError('Learned policy requires model_path')
    _number(h['exact']['max_assignments'], 'exact.max_assignments', integer=True, positive=True)
    for section, fields in [('sa', ('reads', 'sweeps')), ('qaoa', ('max_variables', 'layers', 'evaluations', 'shots')),
                            ('qa', ('num_reads',))]:
        for key in fields:
            _number(h[section][key], f'{section}.{key}', integer=True, positive=True)
    for key in ('beta_start', 'beta_end'):
        _number(h['sa'][key], f'sa.{key}', positive=True)
    if h['sa']['beta_start'] > h['sa']['beta_end']:
        raise ValueError('sa.beta_start must not exceed beta_end')
    if h['qaoa']['max_variables'] > 20:
        raise ValueError('Statevector simulator is limited to 20 variables to bound memory')
    _number(h['qaoa']['readout_flip_probability'], 'qaoa.readout_flip_probability')
    if h['qaoa']['readout_flip_probability'] > 1:
        raise ValueError('readout_flip_probability must be in [0, 1]')
    qa = h['qa']
    for key in ('annealing_time_us', 'timeout_ms', 'estimated_cost_per_call', 'max_cost_per_run'):
        _number(qa[key], f'qa.{key}', positive=key in ('annealing_time_us', 'timeout_ms'))
    _number(qa['max_calls_per_run'], 'qa.max_calls_per_run', integer=True)
    if not isinstance(qa['solver'], str):
        raise ValueError('qa.solver must be a solver name or empty string')
    if qa['enabled'] and (qa['max_calls_per_run'] == 0 or qa['estimated_cost_per_call'] > qa['max_cost_per_run']):
        raise ValueError('Enabled QA requires a positive call budget and sufficient estimated cost budget')
    _number(control['workers'], 'control_plane.workers', integer=True, positive=True)
    if control['latency_mode'] not in ('modeled', 'measured', 'trace'):
        raise ValueError('latency_mode must be modeled, measured or trace')
    for key in ('base_latency_ms', 'validation_latency_ms'):
        _number(control[key], f'control_plane.{key}')
    for key, value in control['backend_latency_ms'].items():
        _number(value, f'backend_latency_ms.{key}')
    if not isinstance(control['trace_ms'], list):
        raise ValueError('trace_ms must be a list indexed by request ID')
    for value in control['trace_ms']:
        _number(value, 'trace_ms')
    if control['latency_mode'] == 'trace' and len(control['trace_ms']) != config['workload']['num_requests']:
        raise ValueError('trace_ms requires one duration per offered request')


def load_config(path):
    with Path(path).open(encoding='utf-8') as stream:
        supplied = yaml.safe_load(stream)
    # Explicit marker prevents accidentally treating an old Hydra file as a DC experiment.
    if not isinstance(supplied, dict) or supplied.get('simulation') != 'distributed_dc':
        raise ValueError('The YAML file must declare simulation: distributed_dc')
    config = validate_config(_merge(DEFAULTS, supplied))
    model_path = config['hybrid']['policy']['model_path']
    if model_path and not Path(model_path).is_absolute():
        config['hybrid']['policy']['model_path'] = str((Path(path).resolve().parent / model_path).resolve())
    return config
