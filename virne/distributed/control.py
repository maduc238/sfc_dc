"""FCFS controller workers, delayed decisions and atomic stale-result validation.

Solver computation is performed synchronously on the start-time snapshot; its
proposal is delivered at a modeled/measured/replayed completion event. No live
capacity is reserved before completion, so concurrent decisions can conflict.
"""

from collections import Counter, deque
import heapq
import time

from virne.distributed.hybrid.budget import Budget
from virne.distributed.validation import EmbeddingValidator


def execute_events(config, substrate, solver, requests):
    control = config['control_plane']
    enabled = control['enabled']
    timeout_s = control['decision_timeout_ms'] / 1000 if enabled else 0.0
    # Common horizon includes every offered request's maximum admission delay.
    horizon = max(r.arrival + timeout_s + r.lifetime for r in requests)
    lookup = {r.id: r for r in requests}
    # departure < completion < arrival < waiting deadline at the same instant.
    events = [(r.arrival, 2, r.id) for r in requests]
    heapq.heapify(events)
    waiting, pending, finished = deque(), {}, set()
    records, allocations, snapshots, calls, decisions = [], [], [], [], []
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
            allocations.append({'request_id': request.id, 'placement': proposal.placement,
                                'paths': proposal.paths, 'latency_ms': proposal.latency_ms,
                                'admission_s': now, 'departure_s': now + request.lifetime})
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
            'commit_version': substrate.version, 'fallback_used': fallback_used,
            'backend_calls': len(job['calls']) if job else 0,
        })
        if job:
            decision = dict(job['decision'])
            decision.update(request_id=request.id, admitted=accepted, final_reason=reason,
                            admission_delay_ms=delay_ms, fallback_used=fallback_used)
            decisions.append(decision)
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
        milliseconds = config['hybrid']['budget_ms'] if config['solver']['name'] == 'ahq' else remaining
        if enabled and milliseconds is not None:
            milliseconds = min(milliseconds, remaining)
        budget = Budget(milliseconds) if milliseconds is not None else None
        wall_start = time.perf_counter()
        try:
            allocation, reason = solver.solve(substrate, request, budget=budget)
        except TimeoutError:
            allocation, reason = None, 'budget'
        wall_ms = (time.perf_counter() - wall_start) * 1000
        job = {'start_s': now, 'wall_ms': wall_ms, 'proposal': allocation, 'reason': reason,
               'calls': list(getattr(solver, 'last_calls', [])),
               'decision': dict(getattr(solver, 'last_decision', {})),
               'fallbacks': list(getattr(solver, 'fallback_allocations', [])), 'version': substrate.version}
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
        heapq.heappush(events, (min(complete, request.arrival + timeout_s), 1, request.id))
        return 'decision_started'

    def dispatch(now):
        while waiting and len(pending) < control['workers']:
            request_id = waiting.popleft()
            if request_id not in finished:
                start_job(lookup[request_id], now)

    snapshots.append({'time_s': 0.0, 'event': 'start', 'request_id': -1, **snapshot()})
    while events:
        now, event_type, request_id = heapq.heappop(events)
        integrate(now)
        request = lookup[request_id]
        if event_type == 0:
            substrate.release(request_id)
            event = 'departure'
        elif event_type == 1:
            job = pending.pop(request_id)
            reason = 'deadline' if job['misses_deadline'] else job['reason']
            finish(request, now, job['proposal'], reason, job)
            event = 'decision_complete'
        elif event_type == 2:
            if enabled:
                waiting.append(request_id)
                heapq.heappush(events, (request.arrival + timeout_s, 3, request_id))
                event = 'arrival_queued'
            else:
                event = start_job(request, now)
        else:
            if request_id in finished or request_id in pending:
                continue
            finish(request, now, reason='deadline')
            event = 'queue_deadline'
        if enabled:
            dispatch(now)
        state = snapshot()
        peak_active_servers = max(peak_active_servers, state['active_servers'])
        snapshots.append({'time_s': now, 'event': event, 'request_id': request_id, **state})
    integrate(horizon)
    snapshots.append({'time_s': horizon, 'event': 'end', 'request_id': -1, **snapshot()})
    return {'records': sorted(records, key=lambda r: r['request_id']), 'events': snapshots,
            'allocations': allocations, 'backend_calls': calls, 'decisions': decisions,
            'counters': counters, 'integrals': integrals, 'horizon': horizon,
            'peak_active_servers': peak_active_servers}
