"""Matched-workload baseline runs with per-seed outputs and bootstrap intervals."""

import argparse
import csv
from copy import deepcopy
import json
from pathlib import Path
import random
from statistics import mean

from virne.distributed.config import load_config
from virne.distributed.runner import run


METRICS = ('acceptance_ratio', 'energy_kwh', 'mean_decision_delay_ms', 'mean_queue_delay_ms',
           'backend_calls', 'qaoa_sim_calls', 'qpu_dispatch_attempts', 'estimated_qpu_cost')


def aggregate(rows):
    rng = random.Random(1701)
    grouped = {}
    for row in rows:
        grouped.setdefault(row['experiment'], []).append(row)
    result = []
    for name, group in sorted(grouped.items()):
        for metric in METRICS:
            values = [row[metric] for row in group]
            lower = upper = None
            if len(values) >= 2:
                estimates = sorted(mean(rng.choices(values, k=len(values))) for _ in range(2000))
                lower, upper = estimates[49], estimates[1949]
            result.append({'experiment': name, 'metric': metric, 'runs': len(values),
                           'mean': mean(values), 'bootstrap_95_low': lower, 'bootstrap_95_high': upper})
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
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    rows = []
    for seed in seeds:
        workload_hash = None
        for path, original in zip(paths, configs):
            c = deepcopy(original)
            c['experiment'].update(seed=seed, output_dir=str(output / 'runs'))
            folder, summary = run(c, source=path)
            if workload_hash is None:
                workload_hash = summary['workload_sha256']
            if summary['workload_sha256'] != workload_hash:
                raise ValueError('Offered workload mismatch')
            row = {**summary, 'output_dir': str(folder), 'source_config': str(Path(path).resolve())}
            rows.append(row)
            # Checkpoint successful runs; interrupted sweeps retain useful results.
            (output / 'comparison.json').write_text(json.dumps(rows, indent=2, allow_nan=False), encoding='utf-8')
            print(f"{summary['experiment']} seed={seed}: {summary['accepted_requests']}/{summary['num_requests']} "
                  f"accepted; calls={summary['backend_calls']}", flush=True)
    for name, data in [('comparison', rows), ('aggregate', aggregate(rows))]:
        with (output / f'{name}.csv').open('w', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(data[0]))
            writer.writeheader()
            writer.writerows(data)
    (output / 'aggregate.json').write_text(json.dumps(aggregate(rows), indent=2, allow_nan=False), encoding='utf-8')
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
