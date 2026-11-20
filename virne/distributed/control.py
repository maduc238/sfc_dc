"""FCFS controller workers, delayed decisions and atomic stale-result validation.

Solver computation is performed synchronously on the start-time snapshot; its
proposal is delivered at a modeled/measured/replayed completion event. No live
capacity is reserved before completion, so concurrent decisions can conflict.
"""

from collections import Counter, deque
import heapq
import time

from virne.distributed.config import HYBRID_SOLVERS

from virne.distributed.hybrid.budget import Budget
from virne.distributed.migration import MigrationManager, generate_profiles
from virne.distributed.validation import EmbeddingValidator


def execute_events(config, substrate, solver, requests):
    control = config['control_plane']
    enabled = control['enabled']
    timeout_s = control['decision_timeout_ms'] / 1000 if enabled else 0.0
    # Common horizon includes every offered request's maximum admission delay.
    offered_end = (config['workload']['duration_hours'] or 0.0) * 3600
    horizon = max([offered_end] + [r.arrival + timeout_s + r.lifetime for r in requests])
    lookup = {r.id: r for r in requests}
    # Departure, migration phase, admission completion, arrival, queue deadline,
    # migration decision, periodic consolidation (in that order at equal times).
    events = [(r.arrival, 3, r.id) for r in requests]
    heapq.heapify(events)
    waiting, pending, finished = deque(), {}, set()
    arrival_times = deque(maxlen=17) if enabled and getattr(solver, 'uses_arrival_rate', False) else None
    records, allocations, snapshots, calls, decisions = [], [], [], [], []
    migration_decisions, current_intervals = [], {}
    profiles = generate_profiles(config, requests)
    migration = MigrationManager(config, substrate, profiles)
    if config['migration']['enabled'] and config['migration']['interval_s'] < horizon:
        heapq.heappush(events, (config['migration']['interval_s'], 6, -1))
    counters, integrals = Counter(), Counter()
    audit = EmbeddingValidator(config)
    previous = 0.0
    peak_active_servers = 0

    def snapshot():
        return {**substrate.snapshot(), 'queued_requests': sum(i not in finished for i in waiting),
                'busy_workers': len(pending)}

    def integrate(now):
        nonlocal previous
        for key, value in snapshot().items():
            if value is not None:
                integrals[key] += value * (now - previous)
        previous = now

    def finish(request, now, proposal=None, reason=None, job=None):
        if request.id in finished:
            return 'ignored'
        finished.add(request.id)
        accepted = False
        fallback_used = False
        choices = ([proposal] if proposal else []) + (job['fallbacks'] if job else [])
        if enabled and now > request.arrival + timeout_s + 1e-9:
            reason, choices = 'deadline', []
        if reason == 'deadline':
            choices = []
        for option in choices:
            if audit.is_currently_feasible(substrate, request, option):
                fallback_used = option is not proposal
                proposal = option
                accepted = True
                break
        if choices and not accepted:
            reason = 'stale'
        if accepted:
            substrate.commit(proposal)
            counters['accepted'] += 1
            counters['accepted_vnfs'] += len(request.vnfs)
            dcs = sorted({substrate.graph.nodes[h]['dc'] for h in proposal.placement.values()})
            counters['cross_dc_requests'] += int(len(dcs) > 1)
            interval = {'request_id': request.id, 'placement': proposal.placement,
                                'paths': proposal.paths, 'latency_ms': proposal.latency_ms,
                                'admission_s': now, 'departure_s': now + request.lifetime,
                                'start_s': now, 'end_s': now + request.lifetime, 'generation': 0}
            allocations.append(interval)
            current_intervals[request.id] = interval
            heapq.heappush(events, (now + request.lifetime, 0, request.id))
            reason = None
        else:
            counters[f'rejected_{reason or "compute"}'] += 1
            dcs = []
        delay_ms = max(0, (now - request.arrival) * 1000)
        start = job['start_s'] if job else now
        records.append({
            'request_id': request.id, 'arrival_s': request.arrival, 'lifetime_s': request.lifetime,
            'ingress': request.ingress, 'num_vnfs': len(request.vnfs), 'accepted': accepted,
            'reason': reason or '', 'latency_ms': proposal.latency_ms if accepted else None,
            'max_latency_ms': request.max_latency_ms, 'data_centers': ';'.join(dcs),
            'inter_dc_virtual_links': proposal.inter_dc_virtual_links if accepted else 0,
            'placement_time_ms': job['wall_ms'] if job else 0.0,
            'decision_start_s': start, 'decision_complete_s': now,
            'queue_delay_ms': (start - request.arrival) * 1000, 'admission_delay_ms': delay_ms,
            'controller_service_ms': (now - start) * 1000,
            'snapshot_version': job['version'] if job else substrate.version,
            'solver_started': job is not None,
            'commit_version': substrate.version, 'fallback_used': fallback_used,
            'backend_calls': len(job['calls']) if job else 0,
        })
        if job:
            decision = dict(job['decision'])
            decision.update(request_id=request.id, admitted=accepted, final_reason=reason,
                            admission_delay_ms=delay_ms, fallback_used=fallback_used)
            decisions.append(decision)
            if hasattr(solver, 'observe_completion'):
                solver.observe_completion(decision)
            for call in job['calls']:
                call['admitted'] = accepted
                call['final_reason'] = reason
                call['proposal_reward'] = call['reward']
                if reason in ('deadline', 'stale'):
                    call['reward'] = min(0.0, call['reward']) - 1.0
                calls.append(call)
        return 'arrival_accepted' if accepted else 'arrival_rejected'

    def start_job(request, now):
        remaining = max(0, (request.arrival + timeout_s - now) * 1000) if enabled else None
        if enabled and remaining <= 1e-9:
            finish(request, now, reason='deadline')
            return None
        milliseconds = config['hybrid']['budget_ms'] if config['solver']['name'] in HYBRID_SOLVERS else remaining
        if enabled and milliseconds is not None:
            milliseconds = min(milliseconds, remaining)
        budget = Budget(milliseconds) if milliseconds is not None else None
        wall_start = time.perf_counter()
        if hasattr(solver, 'decision_context'):
            solver.decision_context = {'queued_requests': sum(i not in finished for i in waiting),
                                       'workers': control['workers'], 'remaining_ms': remaining,
                                       'snapshot_version': substrate.version}
            if arrival_times is not None and len(arrival_times) >= 5:
                solver.decision_context['mean_interarrival_ms'] = (
                    1000 * (arrival_times[-1] - arrival_times[0]) / (len(arrival_times) - 1))
        try:
            allocation, reason = solver.solve(substrate, request, budget=budget)
        except TimeoutError:
            allocation, reason = None, 'budget'
        wall_ms = (time.perf_counter() - wall_start) * 1000
        job = {'start_s': now, 'wall_ms': wall_ms, 'proposal': allocation, 'reason': reason,
               'calls': list(getattr(solver, 'last_calls', [])),
               'decision': dict(getattr(solver, 'last_decision', {})),
               'fallbacks': list(getattr(solver, 'fallback_allocations', [])), 'version': substrate.version}
        if config['output']['detail'] in ('compact', 'summary'):
            # Drop per-shot/QUBO payloads before accumulating a long run in memory.
            job['calls'] = [{k: v for k, v in call.items() if k not in ('qubo', 'sample_histogram')}
                            for call in job['calls']]
        if not enabled:
            return finish(request, now, allocation, reason, job)
        if control['latency_mode'] == 'trace':
            duration_ms = control['trace_ms'][request.id]
        elif control['latency_mode'] == 'measured':
            duration_ms = wall_ms + control['validation_latency_ms']
        else:
            duration_ms = (control['base_latency_ms'] + control['validation_latency_ms']
                           + sum(control['backend_latency_ms'][call['backend']] for call in job['calls']
                                 if call['status'] not in ('disabled_or_budget_limit', 'unavailable', 'size_limit')))
        complete = now + duration_ms / 1000
        job['misses_deadline'] = complete > request.arrival + timeout_s + 1e-9
        pending[request.id] = job
        heapq.heappush(events, (min(complete, request.arrival + timeout_s), 2, request.id))
        return 'decision_started'

    def dispatch(now):
        while waiting and len(pending) < control['workers']:
            request_id = waiting.popleft()
            if request_id not in finished:
                start_job(lookup[request_id], now)

    def plan_migration(now):
        # Admission has priority; one migration decision occupies one shared worker.
        if migration.current is not None or -1 in pending or len(pending) >= control['workers'] or waiting:
            counters['migration_rounds_busy'] += 1
            return
        if not substrate.active:
            return
        budget = Budget(config['migration']['budget_ms'])
        plan = migration.planner.choose(substrate, lookup, now, migration.last_completed,
                                        migration.downtime, budget)
        stats = dict(migration.planner.last_search)
        record = {'start_s': now, 'complete_s': None, 'selected_request_id': plan.request.id if plan else None,
                  'selected_vnf_index': plan.vnf_index if plan else None, **stats,
                  'started': False, 'reason': ''}
        migration_decisions.append(record)
        duration_ms = (stats['wall_time_ms'] if control['latency_mode'] == 'measured'
                       else config['migration']['decision_latency_ms']) + control['validation_latency_ms']
        pending[-1] = {'plan': plan, 'record': record}
        heapq.heappush(events, (min(horizon, now + duration_ms / 1000.0), 5, -1))

    snapshots.append({'time_s': 0.0, 'event': 'start', 'request_id': -1, 'migration_id': -1, **snapshot()})
    while events:
        now, event_type, request_id = heapq.heappop(events)
        if event_type == 1 and (migration.current is None or migration.current['key'] != request_id):
            continue  # A cancelled phase can be past the common simulation horizon.
        integrate(now)
        migration_id = -1
        if event_type == 0:
            migration.cancel_departure(request_id, now)
            current_intervals[request_id]['end_s'] = now
            substrate.release(request_id)
            event = 'departure'
        elif event_type == 1:
            migration_id = request_id
            request_id = migration.current['plan'].request.id
            phase = migration.current['phase']
            changed = migration.advance(migration_id, now)
            event = {'preparing': 'migration_prepared', 'copying': 'migration_stopped',
                     'stopping': 'migration_cutover', 'cleanup': 'migration_completed'}[phase]
            if changed is not None:
                old_interval = current_intervals[request_id]
                old_interval['end_s'] = now
                interval = {**old_interval, 'placement': changed.new.placement, 'paths': changed.new.paths,
                            'latency_ms': changed.new.latency_ms, 'start_s': now,
                            'end_s': old_interval['departure_s'], 'generation': old_interval['generation'] + 1}
                allocations.append(interval)
                current_intervals[request_id] = interval
            if (next_phase := migration.next_event()) is not None:
                heapq.heappush(events, (next_phase[0], 1, next_phase[1]))
        elif event_type == 2:
            request = lookup[request_id]
            job = pending.pop(request_id)
            reason = 'deadline' if job['misses_deadline'] else job['reason']
            finish(request, now, job['proposal'], reason, job)
            event = 'decision_complete'
        elif event_type == 3:
            request = lookup[request_id]
            if arrival_times is not None:
                arrival_times.append(now)
            if enabled:
                waiting.append(request_id)
                heapq.heappush(events, (request.arrival + timeout_s, 4, request_id))
                event = 'arrival_queued'
            else:
                event = start_job(request, now)
        elif event_type == 4:
            request = lookup[request_id]
            if request_id in finished or request_id in pending:
                continue
            finish(request, now, reason='deadline')
            event = 'queue_deadline'
        elif event_type == 5:
            job = pending.pop(-1)
            record = job['record']
            record['complete_s'] = now
            if job['plan'] is not None:
                started, reason = migration.start(job['plan'], now)
                record.update(started=started, reason=reason or '')
                if started:
                    next_phase = migration.next_event()
                    heapq.heappush(events, (next_phase[0], 1, next_phase[1]))
            else:
                record['reason'] = 'no_improving_plan'
            event = 'migration_decision_complete'
        else:
            if now + config['migration']['interval_s'] < horizon:
                heapq.heappush(events, (now + config['migration']['interval_s'], 6, -1))
            plan_migration(now)
            event = 'migration_round'
        if enabled:
            dispatch(now)
        state = snapshot()
        peak_active_servers = max(peak_active_servers, state['active_servers'])
        snapshots.append({'time_s': now, 'event': event, 'request_id': request_id,
                          'migration_id': migration_id, **state})
    integrate(horizon)
    snapshots.append({'time_s': horizon, 'event': 'end', 'request_id': -1, 'migration_id': -1, **snapshot()})
    for record in records:
        rid = record['request_id']
        record.update(migration_count=sum(m['request_id'] == rid and m['committed'] for m in migration.records),
                      downtime_s=migration.downtime[rid],
                      availability=1.0 - migration.downtime[rid] / record['lifetime_s'] if record['accepted'] else None)
    return {'records': sorted(records, key=lambda r: r['request_id']), 'events': snapshots,
            'allocations': allocations, 'backend_calls': calls, 'decisions': decisions,
            'counters': counters, 'integrals': integrals, 'horizon': horizon,
            'peak_active_servers': peak_active_servers, 'migrations': migration.records,
            'migration_decisions': migration_decisions, 'outages': migration.outages,
            'migration_profiles': [{'request_id': rid, 'vnf_index': v, 'state_mb': p.state_mb,
                                    'dirty_rate_mbps': p.dirty_rate_mbps} for (rid, v), p in profiles.items()]}
