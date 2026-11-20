"""Time-weighted operating windows: exclude warm-up and post-arrival drain.

Admission metrics use the arrival cohort in [start, end); power and active
service metrics use overlap with that time window, including services admitted
before warm-up ended. Migration bytes follow their actual serialization phases.
"""

from collections import defaultdict
import math


REJECTION_REASONS = ('compute', 'bandwidth', 'latency', 'budget', 'deadline', 'stale')
STATE_METRICS = ('power_w', 'cpu_utilization', 'memory_utilization', 'wan_utilization',
                 'active_servers', 'active_requests', 'queued_requests', 'busy_workers', 'peak_fabric_link_utilization')


def operating_window(config, result):
    work = config['workload']
    start = config['measurement']['warmup_hours'] * 3600
    end = (work['duration_hours'] * 3600 if work['duration_hours'] is not None
           else max((r['arrival'] for r in result['workload']), default=0.0))
    width = config['measurement']['bin_s']
    count = max(0, math.ceil((end - start) / width))
    if count > 1_000_000:
        raise ValueError('Measurement would create more than one million time bins')
    rows = []
    areas = [defaultdict(float) for _ in range(count)]
    weights = [defaultdict(float) for _ in range(count)]
    delays = [[] for _ in range(count)]
    for i in range(count):
        rows.append({'start_s': start + i * width, 'end_s': min(end, start + (i + 1) * width),
                     'offered_requests': 0, 'accepted_requests': 0, 'solver_wall_ms': 0.0,
                     'started_decisions': 0, 'queue_expirations': 0,
                     **{'rejected_' + reason: 0 for reason in REJECTION_REASONS},
                     'served_vnf_hours': 0.0,
                     'migrations_completed': 0, 'migrations_aborted': 0,
                     'migration_data_mbit_hops': 0.0, 'migration_downtime_s': 0.0,
                     'migration_route_changes': 0, 'migration_planning_wall_ms': 0.0})

    def index(t):
        return int((t - start) // width) if start <= t < end else None

    def spread(a, b, name, value=1.0, integral=False):
        a, b = max(a, start), min(b, end)
        if a >= b or value is None:
            return
        i = int((a - start) // width)
        while i < count and rows[i]['start_s'] < b:
            overlap = max(0.0, min(b, rows[i]['end_s']) - max(a, rows[i]['start_s']))
            if integral:
                rows[i][name] += overlap * value
            else:
                areas[i][name] += overlap * value
                weights[i][name] += overlap
            i += 1

    for a, b in zip(result['events'], result['events'][1:]):
        for name in STATE_METRICS:
            spread(a['time_s'], b['time_s'], name, a.get(name))
    for record in result['records']:
        i = index(record['arrival_s'])
        if i is not None:
            rows[i]['offered_requests'] += 1
            rows[i]['accepted_requests'] += int(record['accepted'])
            rows[i]['solver_wall_ms'] += record['placement_time_ms']
            started = record.get('solver_started', record['accepted'] or record['placement_time_ms'] > 0)
            rows[i]['started_decisions'] += int(started)
            reason = record.get('reason')
            rows[i]['queue_expirations'] += int(reason == 'deadline' and not started)
            if reason in REJECTION_REASONS and not record['accepted']:
                rows[i]['rejected_' + reason] += 1
            if record['accepted']:
                delays[i].append(record['admission_delay_ms'])
    outages = defaultdict(list)
    for outage in result['outages']:
        outages[outage['request_id']].append((outage['start_s'], outage['end_s']))
        spread(outage['start_s'], outage['end_s'], 'migration_downtime_s', integral=True)
    for allocation in result['allocations']:
        def service(a, b):
            spread(a, b, 'service_latency_ms', allocation['latency_ms'])
            spread(a, b, 'served_vnf_hours', len(allocation['placement']) / 3600, integral=True)
        cursor, stop = max(start, allocation['start_s']), min(end, allocation['end_s'])
        for a, b in sorted(outages[allocation['request_id']]):
            if b <= cursor:
                continue
            service(cursor, min(a, stop))
            cursor = max(cursor, b)
            if cursor >= stop:
                break
        service(cursor, stop)
    m = config['migration']
    for record in result['migrations']:
        committed = record['cutover_s']
        i = index(committed) if committed is not None else None
        if i is not None:
            rows[i]['migrations_completed'] += 1
            rows[i]['migration_route_changes'] += record['applied_route_changes']
        i = index(record['end_s'])
        if i is not None and not record['committed']:
            rows[i]['migrations_aborted'] += 1
        position = record['start_s'] + m['setup_s']
        volumes = [(r['volume_mbit'], r['duration_s']) for r in record['rounds']]
        remaining = max(0.0, record['predicted_transferred_mbit'] - sum(v for v, _ in volumes))
        volumes.append((remaining, 0.0))
        for volume, duration in volumes:
            if record['bandwidth_mbps'] > 0:
                wire_end = min(record['end_s'], position + volume / record['bandwidth_mbps'])
                spread(position, wire_end, 'migration_data_mbit_hops',
                       record['bandwidth_mbps'] * max(0, len(record['copy_path']) - 1), integral=True)
            position += duration
    for decision in result['migration_decisions']:
        i = index(decision['start_s'])
        if i is not None:
            rows[i]['migration_planning_wall_ms'] += decision['wall_time_ms']

    def percentile(values):
        ordered = sorted(values)
        return ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)] if ordered else None

    objective = m['objective']
    all_delays = []
    for i, row in enumerate(rows):
        offered, accepted = row['offered_requests'], row['accepted_requests']
        row['acceptance_ratio'] = accepted / offered if offered else None
        row['energy_kwh'] = areas[i]['power_w'] / 3_600_000
        row['energy_kwh_per_served_vnf_hour'] = (row['energy_kwh'] / row['served_vnf_hours']
                                                if row['served_vnf_hours'] else None)
        for name in (*STATE_METRICS, 'service_latency_ms'):
            row['mean_' + name] = areas[i][name] / weights[i][name] if weights[i][name] else None
        row['p95_admission_delay_ms'] = percentile(delays[i])
        row['mean_placement_time_ms'] = row['solver_wall_ms'] / offered if offered else None
        row['migration_normalized_cost'] = (
            row['migration_data_mbit_hops'] / objective['transfer_reference_mbit_hops']
            + row['migration_downtime_s'] / objective['downtime_reference_s']
            + row['migration_route_changes'] / objective['route_reference_changes'])
        all_delays.extend(delays[i])
    offered = sum(r['offered_requests'] for r in rows)
    accepted = sum(r['accepted_requests'] for r in rows)
    solver_ms = sum(r['solver_wall_ms'] for r in rows)
    started = sum(r['started_decisions'] for r in rows)
    energy = sum(r['energy_kwh'] for r in rows)
    vnf_hours = sum(r['served_vnf_hours'] for r in rows)
    summary = {'measurement_start_s': start, 'measurement_end_s': end,
               'measurement_duration_hours': max(0, end - start) / 3600,
               'window_offered_requests': offered, 'window_accepted_requests': accepted,
               'window_acceptance_ratio': accepted / offered if offered else None,
               'window_energy_kwh': energy,
               'window_served_vnf_hours': vnf_hours,
               'window_energy_kwh_per_served_vnf_hour': energy / vnf_hours if vnf_hours else None,
               'window_energy_kwh_per_accepted_request': energy / accepted if accepted else None,
               'window_mean_placement_time_ms': solver_ms / offered if offered else None,
               'window_mean_solver_ms_per_started_request': solver_ms / started if started else None,
               'window_p95_admission_delay_ms': percentile(all_delays)}
    for name in (*STATE_METRICS, 'service_latency_ms'):
        denominator = sum(w[name] for w in weights)
        summary['window_mean_' + name] = sum(a[name] for a in areas) / denominator if denominator else None
    for name in ('started_decisions', 'queue_expirations', *('rejected_' + reason for reason in REJECTION_REASONS),
                 'migrations_completed', 'migrations_aborted', 'migration_downtime_s',
                 'migration_data_mbit_hops', 'migration_route_changes', 'migration_normalized_cost',
                 'migration_planning_wall_ms'):
        summary['window_' + name] = sum(r[name] for r in rows)
    return summary, rows
