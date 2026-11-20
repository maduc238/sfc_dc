"""PACE-SFC: Pressure-Aware Candidate Evaluation for SFC Embedding."""

from collections import Counter
import time

from virne.distributed.hybrid.budget import Budget
from virne.distributed.hybrid.candidates import features, neighborhoods
from virne.distributed.hybrid.guided import OpportunityProfiles, guided_call_record
from virne.distributed.hybrid.qubo import build_qubo, true_objective
from virne.distributed.hybrid.screening import CachedRoutingValidator, PlacementBound, RequestScreen, TopologyIndex
from virne.distributed.hybrid.solver import HybridSolver, _phase
from virne.distributed.model import EPSILON


PACE_COUNTERS = ('light_bypasses', 'backlog_bypasses', 'time_stops', 'arrival_budget_stops',
               'stopped_before_backend', 'stopped_after_0',
               'stopped_after_4', 'stopped_after_8', 'stopped_after_16', 'stopped_after_other',
               'dc_rejected_compute', 'dc_rejected_bandwidth', 'empty_compute_windows',
               'empty_bandwidth_windows', 'refocused_windows', 'bound_pruned', 'sla_pruned', 'bound_wall_time_ms',
               'segment_cache_hits', 'segment_searches', 'proposal_power_gain',
               'proposal_latency_gain', 'proposal_fragmentation_gain')


class BatchClock:
    """Estimate the cost of opening a pool, resampling it or routing more placements."""

    def __init__(self, prior_ms):
        self.ms = dict(open=prior_ms, resample=prior_ms, validation=prior_ms)
        self.totals, self.counts = Counter(), Counter()

    def observe(self, action, elapsed_ms):
        self.ms[action] = .8 * self.ms[action] + .2 * elapsed_ms
        self.totals[action] += elapsed_ms
        self.counts[action] += 1

    def for_request(self):
        local = BatchClock(self.ms['validation'])
        local.ms.update(self.ms)
        return local

    def affordable(self, count, available_ms, action=None):
        setup = self.ms[action] if action is not None else 0.0
        return min(count, max(0, int((available_ms - setup) / max(.001, self.ms['validation']))))


class PaceSolver(HybridSolver):
    uses_arrival_rate = True

    def __init__(self, config):
        super().__init__(config)
        self.guided = self.settings['guided']
        self.profiles = OpportunityProfiles(config)
        self.clock = BatchClock(self.guided['prior_validation_ms'])
        self.incumbent_solver.capture_feedback = self.guided['feedback']
        self.decision_context = {}
        self.topology = None

    def observe_completion(self, decision):
        for action, average_ms in decision.pop('pace_timing_observations', {}).items():
            self.clock.observe(action, average_ms)

    def solve(self, substrate, request, budget=None):
        started = time.perf_counter()
        budget = budget or Budget(self.settings['budget_ms'])
        self.last_calls, self.last_decision, self.fallback_allocations = [], {}, []
        if self.topology is None or self.topology.graph is not substrate.graph:
            self.topology = TopologyIndex(substrate)
        self.profiles.observe(request)
        search = RequestSearch(self, substrate, request, budget, started)
        return search.solve()


class RequestSearch:
    """All mutable placement, route and bound information belongs to this decision."""

    def __init__(self, solver, substrate, request, budget, started):
        self.solver, self.s, self.r = solver, substrate, request
        self.settings, self.guided = solver.settings, solver.guided
        self.budget, self.started = budget, started
        self.clock = solver.clock.for_request()
        self.stats = Counter()
        self.phases = dict.fromkeys(('incumbent', 'candidate', 'qubo', 'sampling', 'validation'), 0.0)
        self.validator = CachedRoutingValidator(solver.config, self.stats)
        self.validator.budget = budget
        self.screen = RequestScreen(solver.topology, substrate, request, budget, self.guided['feedback'], self.stats)
        self.loss = solver.profiles.costs(substrate, request, per_host=True)
        self.bound = PlacementBound(solver.topology, substrate, request, self.settings, self.host_cost)
        self.seen, self.emitted, self.pair_costs, self.feedback = set(), set(), {}, {}
        self.incumbent = self.best = self.score = self.initial_score = None
        self.validations = self.batches = self.duplicates = self.updates = self.repairs = 0
        self.reason, self.stop = 'compute', 'candidate_exhausted'
        self.cpu = sum(substrate.cpu.values()) / solver.topology.cpu_capacity
        self.loss_weight = self.guided['fragmentation_weight'] / self.guided['fragmentation_reference_servers']

    def host_cost(self, v, h):
        return self.loss_weight * self.loss(v, h) if self.loss_weight else 0.0

    def objective(self, allocation):
        return true_objective(self.s, allocation, self.settings) + sum(
            self.host_cost(v, h) for v, h in allocation.placement.items())

    def pressure(self):
        usage = max(self.cpu, sum(self.s.memory.values()) / self.solver.topology.memory_capacity)
        for edge, demand in self.incumbent.bandwidth.items():
            capacity = self.s.graph.edges[edge]['capacity_mbps']
            if capacity is not None:
                usage = max(usage, (self.s.bandwidth[edge] + demand) / capacity)
        return usage

    def allowance(self, count, action=None):
        self.budget.check()
        if not self.guided['adaptive_validation']:
            return count
        available = self.budget.remaining_ms
        ctx = self.solver.decision_context
        elapsed = (time.perf_counter() - self.started) * 1000
        workers = max(1, ctx.get('workers', 1))
        if ctx.get('remaining_ms') is not None:
            share = 1 + ctx.get('queued_requests', 0) / workers
            available = min(available, ctx['remaining_ms'] / share - elapsed)
        arrival_quota = float('inf')
        if ctx.get('mean_interarrival_ms') is not None:
            arrival_quota = ctx['mean_interarrival_ms'] * workers * self.guided['controller_utilization_target'] - elapsed
        affordable = self.clock.affordable(count, min(available, arrival_quota), action)
        if affordable == 0 and arrival_quota < available:
            self.stats['arrival_budget_stops'] += 1
        return affordable

    def next_count(self):
        return next(m - self.validations for m in self.guided['validation_milestones'] if m > self.validations)

    def note_failure(self, reason):
        constraints = ('compute', 'bandwidth', 'latency')
        if reason in constraints and constraints.index(reason) > constraints.index(self.reason):
            self.reason = reason

    def prepare_pool(self, attempt):
        with _phase(self.phases, 'candidate'):
            groups = neighborhoods(self.s, self.r, self.best, self.settings, self.budget,
                                   feedback=self.feedback, attempt=attempt, screen=self.screen,
                                   host_cost=lambda v, h: self.host_cost(v, h) * self.settings['objective']['power_reference_w'])
            for group in groups:
                signature = (group.dc, tuple(sorted(group.fixed.items())),
                             tuple((v, tuple(h)) for v, h in sorted(group.candidates.items())))
                if signature in self.emitted:
                    continue
                self.emitted.add(signature)
                if sum(map(len, group.candidates.values())) <= self.settings['max_variables']:
                    break
            else:
                self.note_failure(self.screen.last_failure)
                return None
            info = features(self.s, self.r, group, self.best, self.settings, cpu_utilization=self.cpu)
        backend, selected = self.solver.policy.select(info, self.budget.remaining_ms, self.solver.pool)
        if backend == 'keep':
            self.stop = selected
            return None
        with _phase(self.phases, 'qubo'):
            qubo = build_qubo(self.s, self.r, group, self.settings, self.budget, host_cost=self.host_cost,
                              pair_costs=self.pair_costs, distance_from=self.solver.topology.distances)
        return group, info, qubo, backend, selected

    def candidates(self, qubo, result, record):
        pending, local = [], set()
        for sample in sorted(result.samples, key=lambda s: (s.energy, s.bits)):
            self.budget.check()
            placement = qubo.decode(sample.bits)
            if placement is None:
                continue
            record['placement_feasible_samples'] += sample.occurrences
            key = tuple(placement[i] for i in range(len(self.r.vnfs)))
            if key in self.seen or key in local:
                self.duplicates += 1
                continue
            local.add(key)
            lower = None
            if self.guided['bound_pruning']:
                with _phase(self.stats, 'bound_wall_time_ms'):
                    lower, delay = self.bound.evaluate(placement)
                if self.r.max_latency_ms is not None and delay > self.r.max_latency_ms + EPSILON:
                    self.seen.add(key)
                    self.stats['sla_pruned'] += 1
                    self.note_failure('latency')
                    continue
            pending.append((lower, key, placement, sample.occurrences))
        return pending

    def promising(self, pending):
        keep = []
        for item in pending:
            lower, key = item[:2]
            # Keep near ties when floating-point roundoff could change the decision.
            if self.is_dominated(lower):
                self.seen.add(key)
                self.stats['bound_pruned'] += 1
            else:
                keep.append(item)
        return keep

    def is_dominated(self, lower):
        return (lower is not None and self.score is not None and
                lower > self.score + max(1.0, abs(self.score)) * 1e-12)

    def route_candidate(self, item, group, record):
        _, key, placement, occurrences = item
        self.seen.add(key)
        self.validations += 1
        record['validated_samples'] += 1
        started = time.perf_counter()
        try:
            with _phase(self.phases, 'validation'):
                allocation, reason = self.validator.evaluate(self.s, self.r, placement)
                if allocation is not None:
                    value = self.objective(allocation)
        finally:
            elapsed = (time.perf_counter() - started) * 1000
            record['validation_ms'] += elapsed
            self.clock.observe('validation', elapsed)
        if allocation is None:
            self.note_failure(reason)
            if self.guided['feedback']:
                self.feedback = dict(self.validator.last_feedback, dc=group.dc)
                seg = self.feedback.get('segment')
                if seg is not None:
                    right = -1 if seg == len(self.r.vnfs) else seg
                    pair = seg - 1, self.feedback['source'], right, self.feedback['target']
                    self.pair_costs[pair] = self.pair_costs.get(pair, 0.0) + self.guided['feedback_penalty']
                    self.updates += 1
            return False
        record['routing_feasible_samples'] += occurrences
        if record['best_true_objective'] is None or value < record['best_true_objective']:
            record['best_true_objective'] = value
        if self.best is None or value < self.score - 1e-12:
            self.best, self.score = allocation, value
        return True

    def check_pool(self, pending, group, record, can_repair):
        while pending and self.validations < self.settings['max_validations']:
            pending = self.promising(pending)
            if not pending:
                break
            count = self.allowance(min(self.next_count(), len(pending)))
            if count == 0:
                self.stop = 'time_gate'
                break
            self.batches += 1
            record['validation_batches'] += 1
            feasible = False
            previous_updates = self.updates
            for item in pending[:count]:
                self.budget.check()
                # A preceding route in this same batch may have lowered the incumbent.
                if self.is_dominated(item[0]):
                    self.seen.add(item[1])
                    self.stats['bound_pruned'] += 1
                    continue
                feasible |= self.route_candidate(item, group, record)
                if self.incumbent is None and self.best is not None:
                    self.stop = 'rescued'
                    return
            pending = pending[count:]
            if not feasible and self.updates > previous_updates and can_repair:
                break

    def optimize(self):
        retry = None
        for attempt in range(self.settings['max_backend_calls']):
            if self.validations >= self.settings['max_validations']:
                self.stop = 'validation_limit'
                break
            action = 'resample' if retry else 'open'
            if self.allowance(self.next_count(), action) == 0:
                self.stop = 'time_gate'
                break
            started = time.perf_counter()
            pool = retry or self.prepare_pool(attempt)
            retry = None
            if pool is None:
                break
            group, info, qubo, backend, selected = pool
            with _phase(self.phases, 'sampling'):
                self.budget.check()
                result = self.solver.pool.sample(backend, qubo, self.r.id, attempt, self.budget)
            record = guided_call_record(self.r, group, info, qubo, result, selected)
            record['call_id'] = attempt
            self.solver.last_calls.append(record)
            if action == 'open' and self.feedback.get('segment') is not None:
                self.repairs += 1
            pending = self.candidates(qubo, result, record)
            self.clock.observe(action, (time.perf_counter() - started) * 1000)
            before = self.score
            self.check_pool(pending, group, record, attempt + 1 < self.settings['max_backend_calls'])
            fully_routed = (result.metadata.get('exhaustive', False) and
                            record['validated_samples'] == len(pending) == len(result.samples))
            record['validation_candidates_truncated'] = not fully_routed
            record['neighborhood_search_complete'] = fully_routed
            record['neighborhood_optimal'] = bool(fully_routed and record['best_true_objective'] is not None)
            gain = (before - self.score if before is not None else float(self.best is not None))
            record['reward'] = gain - (time.perf_counter() - started) * 1000 / self.settings['budget_ms']
            if self.stop in ('time_gate', 'rescued'):
                break
            if backend == 'qaoa_sim' and not record['routing_feasible_samples'] and (
                    not result.samples or not record['placement_feasible_samples'] or record['validated_samples']):
                retry = group, info, qubo, 'sa', 'backend_fallback'
        if self.validations >= self.settings['max_validations'] and self.stop != 'rescued':
            self.stop = 'validation_limit'

    def solve(self):
        try:
            with _phase(self.phases, 'incumbent'):
                self.solver.incumbent_solver.last_feedback = {}
                self.incumbent, reason = self.solver.incumbent_solver.solve(self.s, self.r, self.budget)
                self.note_failure(reason)
                self.best = self.incumbent
                if self.best is not None:
                    self.initial_score = self.score = self.objective(self.best)
                elif self.guided['feedback']:
                    self.feedback = dict(self.solver.incumbent_solver.last_feedback)
                    if self.feedback.get('segment') == 0:
                        self.feedback['dc'] = self.s.graph.nodes[self.feedback['target']]['dc']
            if self.best is not None and not self.settings['optimize_existing']:
                self.stop = 'incumbent_sufficient'
            elif self.best is not None and self.guided['adaptive_validation'] and (
                    self.pressure() < self.guided['pressure_threshold']):
                self.stop = 'light_load'
                self.stats['light_bypasses'] += 1
            elif (self.best is not None and self.guided['adaptive_validation'] and
                  self.solver.decision_context.get('queued_requests', 0) >=
                  self.solver.decision_context.get('workers', 1)):
                self.stop = 'controller_backlog'
                self.stats['backlog_bypasses'] += 1
            else:
                self.optimize()
        except TimeoutError:
            self.stop = 'budget'
        if self.budget.expired:
            self.stop = 'budget'
        if self.stop == 'time_gate':
            self.stats['time_stops'] += 1
        if not self.solver.last_calls:
            self.stats['stopped_before_backend'] += 1
        else:
            bucket = str(self.validations) if self.validations in (0, 4, 8, 16) else 'other'
            self.stats['stopped_after_' + bucket] += 1
        improved = self.incumbent is not None and self.best is not self.incumbent
        if improved:
            self.solver.fallback_allocations = [self.incumbent]
            obj = self.settings['objective']
            latency_gain = (self.incumbent.latency_ms - self.best.latency_ms) * obj['latency_weight'] / obj['latency_reference_ms']
            frag_gain = sum(self.host_cost(i, h) - self.host_cost(i, self.best.placement[i])
                            for i, h in self.incumbent.placement.items())
            self.stats.update(proposal_latency_gain=latency_gain, proposal_fragmentation_gain=frag_gain,
                              proposal_power_gain=self.initial_score - self.score - latency_gain - frag_gain)
        loss_fraction = (sum(self.loss(i, h) for i, h in self.best.placement.items()) / len(self.s.servers)
                         if self.best is not None else None)
        elapsed = (time.perf_counter() - self.started) * 1000
        self.phases['other'] = max(0.0, elapsed - sum(self.phases.values()))
        self.solver.last_decision = {
            'request_id': self.r.id, 'incumbent_feasible': self.incumbent is not None,
            'rescued': self.incumbent is None and self.best is not None, 'improved': improved,
            'best_true_objective': self.score, 'backend_calls': len(self.solver.last_calls),
            'budget_exhausted': self.stop == 'budget', 'validation_attempts': self.validations,
            'policy_reasons': [self.stop], 'snapshot_version': self.s.version, 'wall_time_ms': elapsed,
            'phase_times_ms': self.phases, 'guided_enabled': True, 'guided_stop_reason': self.stop,
            'guided_validation_batches': self.batches, 'guided_feedback_updates': self.updates,
            'guided_feedback_repairs': self.repairs, 'guided_duplicate_skips': self.duplicates,
            'guided_fragmentation_loss': loss_fraction,
            'pace_stats': dict(self.stats), 'pace_timing_observations': {
                a: total / self.clock.counts[a] for a, total in self.clock.totals.items()},
        }
        reason = 'budget' if self.stop in ('budget', 'time_gate') else self.reason
        return self.best, None if self.best is not None else reason
