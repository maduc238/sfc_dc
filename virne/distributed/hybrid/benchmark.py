"""Matched-workload baseline runs with per-seed outputs and bootstrap intervals."""

import argparse
import csv
from copy import deepcopy
import json
import math
from pathlib import Path
import random
from statistics import mean

from virne.distributed.config import load_config
from virne.distributed.runner import run


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


def run_benchmark(paths, seeds, output):
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
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    for seed in seeds:
        workload_hash = None
        profile_hash = None
        for path, original in zip(paths, configs):
            c = deepcopy(original)
            c['experiment'].update(seed=seed, output_dir=str(output / 'runs'))
            folder, summary = run(c, source=path)
            if workload_hash is None:
                workload_hash = summary['workload_sha256']
                profile_hash = summary['migration_profiles_sha256']
            if summary['workload_sha256'] != workload_hash:
                raise ValueError('Offered workload mismatch')
            if summary['migration_profiles_sha256'] != profile_hash:
                raise ValueError('Migration profiles mismatch')
            row = {**summary, 'output_dir': str(folder), 'source_config': str(Path(path).resolve())}
            rows.append(row)
            # Checkpoint successful runs; interrupted sweeps retain useful results.
            (output / 'comparison.json').write_text(json.dumps(rows, indent=2, allow_nan=False), encoding='utf-8')
            print(f"{summary['experiment']} seed={seed}: {summary['accepted_requests']}/{summary['num_requests']} "
                  f"accepted; calls={summary['backend_calls']}", flush=True)
    aggregated = aggregate(rows)
    for name, data in [('comparison', rows), ('aggregate', aggregated)]:
        with (output / f'{name}.csv').open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(data[0]))
            writer.writeheader()
            writer.writerows(data)
    (output / 'aggregate.json').write_text(json.dumps(aggregated, indent=2, allow_nan=False), encoding='utf-8')
    return rows


def main():
    parser = argparse.ArgumentParser(description='Compare baseline YAMLs under matched workload/controller budgets.')
    parser.add_argument('--configs', required=True, nargs='+', type=Path)
    parser.add_argument('--seeds', nargs='+', type=int, default=[11, 22, 33])
    parser.add_argument('--output', required=True, type=Path, help='New output directory; existing runs are never overwritten')
    args = parser.parse_args()
    try:
        run_benchmark(args.configs, args.seeds, args.output)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    main()
