"""Event-driven simulation and reproducible, machine-readable experiment artifacts."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import time
import uuid

import networkx as nx
import yaml

from virne.distributed.config import load_config
from virne.distributed.control import execute_events
from virne.distributed.model import Substrate, generate_requests
from virne.distributed.solver import HeuristicSolver


def _json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')


def _csv(path, rows):
    if not rows:
        return
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def simulate(config):
    substrate = Substrate(config)
    if config['solver']['name'] == 'ahq':
        from virne.distributed.hybrid.solver import HybridSolver
        solver = HybridSolver(config)
    else:
        solver = HeuristicSolver(config)
    requests = generate_requests(config)
    offered = [r.to_dict() for r in requests]
    workload_hash = hashlib.sha256(json.dumps(offered, sort_keys=True).encode()).hexdigest()
    wall_start = time.perf_counter()
    execution = execute_events(config, substrate, solver, requests)
    records, snapshots, allocations = (execution[k] for k in ('records', 'events', 'allocations'))
    counters, integrals = execution['counters'], execution['integrals']
    horizon, peak_active_servers = execution['horizon'], execution['peak_active_servers']
    accepted = counters['accepted']
    latencies = sorted(r['latency_ms'] for r in records if r['accepted'])
    energy_kwh = integrals['power_w'] / 3_600_000.0
    def percentile(values, percent):
        values = sorted(values)
        return values[max(0, (percent * len(values) + 99) // 100 - 1)] if values else None

    admission_delays = [r['admission_delay_ms'] for r in records if r['accepted']]
    backend_calls = execution['backend_calls']
    decisions = execution['decisions']
    migrations = execution['migrations']
    migration_decisions = execution['migration_decisions']
    profile_hash = hashlib.sha256(json.dumps(execution['migration_profiles'], sort_keys=True).encode()).hexdigest()
    service_latency_integral = service_seconds = 0.0
    for interval in allocations:
        duration = interval['end_s'] - interval['start_s']
        duration -= sum(max(0.0, min(interval['end_s'], outage['end_s'])
                            - max(interval['start_s'], outage['start_s']))
                        for outage in execution['outages'] if outage['request_id'] == interval['request_id'])
        service_seconds += max(0.0, duration)
        service_latency_integral += max(0.0, duration) * interval['latency_ms']
    summary = {
        'schema_version': 1, 'experiment': config['experiment']['name'],
        'solver': config['solver']['name'], 'scope': config['solver']['scope'],
        'seed': config['experiment']['seed'], 'workload_sha256': workload_hash,
        'num_data_centers': len(config['topology']['data_centers']),
        'num_servers': len(substrate.servers),
        'total_cpu': sum(substrate.graph.nodes[h]['cpu'] for h in substrate.servers),
        'total_memory_gb': sum(substrate.graph.nodes[h]['memory_gb'] for h in substrate.servers),
        'num_requests': len(requests), 'accepted_requests': accepted,
        'acceptance_ratio': accepted / len(requests), 'accepted_vnfs': counters['accepted_vnfs'],
        'rejected_compute': counters['rejected_compute'], 'rejected_bandwidth': counters['rejected_bandwidth'],
        'rejected_latency': counters['rejected_latency'], 'cross_dc_requests': counters['cross_dc_requests'],
        'rejected_budget': counters['rejected_budget'], 'rejected_deadline': counters['rejected_deadline'],
        'rejected_stale': counters['rejected_stale'],
        'controller_enabled': config['control_plane']['enabled'],
        'controller_latency_mode': config['control_plane']['latency_mode'],
        'mean_decision_delay_ms': sum(r['admission_delay_ms'] for r in records) / len(records),
        'mean_queue_delay_ms': sum(r['queue_delay_ms'] for r in records) / len(records),
        'p50_admission_delay_ms': percentile(admission_delays, 50),
        'p95_admission_delay_ms': percentile(admission_delays, 95),
        'p99_admission_delay_ms': percentile(admission_delays, 99),
        'backend_calls': len(backend_calls),
        'classical_backend_calls': sum(c['kind'] == 'classical' for c in backend_calls),
        'qaoa_sim_calls': sum(c['backend'] == 'qaoa_sim' for c in backend_calls),
        'qpu_dispatch_attempts': sum(c['metadata'].get('remote_dispatch_attempted', False) for c in backend_calls),
        'qpu_successful_calls': sum(c['backend'] == 'qa' and c['status'] == 'complete' for c in backend_calls),
        'estimated_qpu_cost': sum(c['metadata'].get('estimated_cost', 0) for c in backend_calls),
        'qpu_completion_unknown': sum(c['metadata'].get('remote_completion_unknown', False) for c in backend_calls),
        'backend_wall_time_ms': sum(c['metadata'].get('wall_time_ms', 0) for c in backend_calls),
        'samples': sum(c['samples'] for c in backend_calls),
        'placement_feasible_samples': sum(c['placement_feasible_samples'] for c in backend_calls),
        'routing_feasible_samples': sum(c['routing_feasible_samples'] for c in backend_calls),
        'rescued_proposals': sum(d.get('rescued', False) for d in decisions),
        'admitted_rescues': sum(d.get('rescued', False) and d['admitted'] for d in decisions),
        'improved_proposals': sum(d.get('improved', False) for d in decisions),
        'decision_budget_exhaustions': sum(d.get('budget_exhausted', False) for d in decisions),
        'migration_enabled': config['migration']['enabled'], 'migration_profiles_sha256': profile_hash,
        'migration_attempts': len(migrations),
        'migration_completed': sum(m['committed'] for m in migrations),
        'migration_aborted': sum(not m['committed'] for m in migrations),
        'migration_decisions': len(migration_decisions),
        'migration_rounds_busy': counters['migration_rounds_busy'],
        'migration_candidates': sum(d['candidates'] for d in migration_decisions),
        'migration_candidates_timeout': sum(d['rejected_candidates'].get('timeout', 0) for d in migration_decisions),
        'migration_candidates_non_convergent': sum(d['rejected_candidates'].get('non_convergent', 0) for d in migration_decisions),
        'migration_candidates_temporary_capacity': sum(d['rejected_candidates'].get('temporary_capacity', 0) for d in migration_decisions),
        'migration_candidates_no_improvement': sum(d['rejected_candidates'].get('no_improvement', 0) for d in migration_decisions),
        'migration_planning_wall_time_ms': sum(d['wall_time_ms'] for d in migration_decisions),
        'migration_data_mbit': sum(m['transferred_mbit'] for m in migrations),
        'migration_data_mbit_hops': sum(m['transferred_mbit_hops'] for m in migrations),
        'migration_downtime_s': sum(m['downtime_s'] for m in migrations),
        'migration_p95_downtime_s': percentile([m['downtime_s'] for m in migrations], 95),
        'migration_normalized_cost': sum(m['actual_normalized_cost'] for m in migrations),
        'migration_route_changes': sum(m['applied_route_changes'] for m in migrations),
        'migration_standby_core_seconds': integrals['migration_reserved_cpu'],
        'migration_copy_core_seconds': integrals['migration_copy_cpu'],
        'migration_reserved_gb_seconds': integrals['migration_reserved_memory_gb'],
        'time_weighted_service_latency_ms': service_latency_integral / service_seconds if service_seconds else None,
        'horizon_s': horizon, 'energy_kwh': energy_kwh,
        'server_energy_kwh': integrals['server_power_w'] / 3_600_000.0,
        'network_energy_kwh': integrals['network_power_w'] / 3_600_000.0,
        'energy_kwh_per_accepted_request': energy_kwh / accepted if accepted else None,
        'mean_power_w': integrals['power_w'] / horizon,
        'mean_cpu_utilization': integrals['cpu_utilization'] / horizon,
        'mean_memory_utilization': integrals['memory_utilization'] / horizon,
        'mean_wan_utilization': integrals['wan_utilization'] / horizon
        if substrate.snapshot()['wan_utilization'] is not None else None,
        'mean_wan_bandwidth_mbps_hops': integrals['wan_bandwidth_mbps_hops'] / horizon,
        'mean_active_servers': integrals['active_servers'] / horizon,
        'mean_active_requests': integrals['active_requests'] / horizon,
        'peak_active_servers': peak_active_servers,
        'mean_latency_ms': sum(latencies) / accepted if accepted else None,
        'p95_latency_ms': latencies[max(0, (95 * accepted + 99) // 100 - 1)] if accepted else None,
        'mean_placement_time_ms': sum(r['placement_time_ms'] for r in records) / len(records),
        'runtime_s': time.perf_counter() - wall_start,
        'resources_released': not substrate.active and not substrate.reservations and all(v == 0 for resource in (
            substrate.cpu, substrate.memory, substrate.bandwidth, substrate.paused_cpu) for v in resource.values()),
    }
    per_dc = []
    for dc in config['topology']['data_centers']:
        dc_id = dc['id']
        hosts = {h for h in substrate.servers if substrate.graph.nodes[h]['dc'] == dc_id}
        hosted_vnfs, hosted_requests = set(), set()
        cpu_seconds = memory_seconds = 0.0
        for allocation in allocations:
            request = requests[allocation['request_id']]
            local_vnfs = [request.vnfs[i] for i, h in allocation['placement'].items() if h in hosts]
            if local_vnfs:
                hosted_requests.add(request.id)
            hosted_vnfs.update((request.id, i) for i, h in allocation['placement'].items() if h in hosts)
            duration = allocation['end_s'] - allocation['start_s']
            cpu_seconds += sum(v.cpu for v in local_vnfs) * duration
            memory_seconds += sum(v.memory_gb for v in local_vnfs) * duration
        per_dc.append({'data_center': dc_id, 'num_servers': len(hosts), 'hosted_requests': len(hosted_requests),
                       'hosted_vnfs': len(hosted_vnfs),
                       'mean_cpu_utilization': cpu_seconds / horizon / sum(substrate.graph.nodes[h]['cpu'] for h in hosts),
                       'mean_memory_utilization': memory_seconds / horizon / sum(substrate.graph.nodes[h]['memory_gb'] for h in hosts)})
    return {'summary': summary, 'records': records, 'events': snapshots, 'allocations': allocations,
            'workload': offered, 'per_dc': per_dc, 'substrate': substrate,
            'backend_calls': execution['backend_calls'], 'decisions': execution['decisions'],
            'migrations': migrations, 'migration_decisions': migration_decisions,
            'migration_profiles': execution['migration_profiles'], 'outages': execution['outages'],
            'policy_model': getattr(getattr(solver, 'policy', None), 'model', None)}


def run(config, source=None):
    result = simulate(config)
    exp = config['experiment']
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output = Path(exp['output_dir']).expanduser().resolve() / exp['name'] / f'{stamp}-{uuid.uuid4().hex[:8]}'
    output.mkdir(parents=True, exist_ok=False)
    (output / 'config.yaml').write_text(yaml.safe_dump(config, sort_keys=False), encoding='utf-8')
    for name in ('summary', 'workload', 'allocations', 'backend_calls', 'decisions',
                 'migrations', 'migration_decisions', 'migration_profiles', 'outages'):
        _json(output / f'{name}.json', result[name])
    policy_hash = None
    if result['policy_model'] is not None:
        _json(output / 'policy_model.json', result['policy_model'])
        policy_hash = hashlib.sha256((output / 'policy_model.json').read_bytes()).hexdigest()
    for name in ('records', 'events', 'per_dc'):
        _csv(output / f'{name}.csv', result[name])
    _csv(output / 'summary.csv', [result['summary']])
    _csv(output / 'migrations.csv', [{k: v for k, v in row.items() if k not in (
        'rounds', 'copy_path', 'old_placement', 'new_placement', 'old_paths', 'new_paths')}
                                     for row in result['migrations']])
    graph = result['substrate'].graph
    _json(output / 'topology.json', {
        'directed': False, 'nodes': [{'id': n, **attrs} for n, attrs in graph.nodes(data=True)],
        'edges': [{'source': u, 'target': v, **attrs} for u, v, attrs in graph.edges(data=True)],
    })
    source_files = sorted(Path(__file__).parent.rglob('*.py')) + [Path(__file__).parents[1] / 'cli.py']
    source_hash = hashlib.sha256()
    for path in source_files:
        source_hash.update(str(path.relative_to(Path(__file__).parents[1])).replace('\\', '/').encode())
        source_hash.update(path.read_bytes())
    _json(output / 'manifest.json', {'source_config': str(Path(source).resolve()) if source else None,
                                   'source_sha256': source_hash.hexdigest(),
                                   'python_version': platform.python_version(),
                                   'networkx_version': nx.__version__, 'pyyaml_version': yaml.__version__,
                                   'hybrid_policy': config['hybrid']['policy']['name'] if config['solver']['name'] == 'ahq' else None,
                                   'qpu_enabled': config['hybrid']['qa']['enabled'],
                                   'policy_model_sha256': policy_hash,
                                   'controller_latency_mode': config['control_plane']['latency_mode'],
                                   'migration_enabled': config['migration']['enabled'],
                                   'migration_model': 'serial_single_vnf_bounded_precopy',
                                   'placement_history_schema_version': 2,
                                   'migration_profiles_sha256': result['summary']['migration_profiles_sha256'],
                                   'created_utc': stamp, 'workload_sha256': result['summary']['workload_sha256']})
    return output, result['summary']


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run a multi-data-center SFC baseline from local source.')
    parser.add_argument('--config', required=True, type=Path, help='Standalone baseline YAML file')
    parser.add_argument('--validate-only', action='store_true', help='Validate configuration and topology without running')
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.validate_only:
            substrate = Substrate(config)
            if config['solver']['name'] == 'ahq' and config['hybrid']['policy']['name'] == 'learned':
                from virne.distributed.hybrid.policy import SelectionPolicy
                SelectionPolicy(config['hybrid'])
            print(f"Valid: {config['experiment']['name']} ({len(substrate.servers)} servers)")
            return
        print(f"Start: {config['experiment']['name']} / {config['solver']['name']}", flush=True)
        output, summary = run(config, source=args.config)
    except (OSError, ValueError, yaml.YAMLError) as error:
        parser.error(str(error))
    print(f"Complete: {summary['accepted_requests']}/{summary['num_requests']} accepted "
          f"({summary['acceptance_ratio']:.1%}), energy={summary['energy_kwh']:.4f} kWh")
    print(f'Results: {output}')
