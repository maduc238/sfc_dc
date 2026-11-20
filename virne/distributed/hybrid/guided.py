"""Validation-guided SFC search: causal observations, bounded work and safe fallback.

No training/test traces or holding times are consulted. Probability/gain/cost
estimates use explicit priors and completed observations from this simulation.
They are scheduling heuristics, not optimality or quantum-advantage claims.
"""

from collections import defaultdict
import time

from virne.distributed.hybrid.budget import Budget
from virne.distributed.hybrid.candidates import features, neighborhoods
from virne.distributed.hybrid.qubo import build_qubo, true_objective
from virne.distributed.hybrid.solver import HybridSolver, _phase
from virne.distributed.model import EPSILON


class OpportunityProfiles:
    """CPU/RAM compatibility loss, separable because same-chain co-location is off."""

    def __init__(self, config):
        self.bounds = [config['workload'][key] for key in ('cpu', 'memory_gb')]
        self.bins = config['hybrid']['guided']['profile_bins']
        # Uniform pseudo-counts over configured demand ranges; observed requests
        # subsequently update them. These are not samples of future requests.
        self.counts = [1.0] * self.bins ** 2
        self.groups = [(self._center(0, i), self._center(1, j))
                       for i in range(self.bins) for j in range(self.bins)]

    def _center(self, dimension, index):
        low, high = self.bounds[dimension]
        return low + (high - low) * (index + 0.5) / self.bins

    def observe(self, request):
        for vnf in request.vnfs:
            indices = []
            for value, (low, high) in zip((vnf.cpu, vnf.memory_gb), self.bounds):
                indices.append(0 if high == low else min(
                    self.bins - 1, max(0, int((value - low) * self.bins / (high - low)))))
            self.counts[indices[0] * self.bins + indices[1]] += 1

    def costs(self, substrate, request, *, per_host=False):
        total = sum(self.counts)
        weights = [value / total for value in self.counts]
        divisor = 1 if per_host else len(substrate.servers)
        cache = {}

        def cost(v, host):
            key = v, host
            if key not in cache:
                attrs = substrate.graph.nodes[host]
                cpu = attrs['cpu'] - substrate.cpu[host]
                memory = attrs['memory_gb'] - substrate.memory[host]
                demand = request.vnfs[v]
                loss = sum(weight for (c, m), weight in zip(self.groups, weights)
                           if cpu + EPSILON >= c and memory + EPSILON >= m
                           and (cpu - demand.cpu + EPSILON < c or memory - demand.memory_gb + EPSILON < m))
                cache[key] = loss / divisor
            return cache[key]
        return cost


class ValidationPolicy:
    """Smoothed empirical benefit/cost estimates, bucketed by observed pressure."""

    def __init__(self, settings):
        self.settings = settings
        self.rows = defaultdict(lambda: {'n': 0, 'wins': 0, 'gain': 0.0, 'ms': 0.0})
        self.preparation_ms = settings['prior_validation_ms']
        self.completions = self.stale = 0

    @staticmethod
    def key(pressure, incumbent, backend):
        return min(3, int(pressure * 4)), bool(incumbent), backend

    def utility(self, pressure, incumbent, backend, remaining_ms, queue_depth=0, workers=1, count=4):
        if remaining_ms <= 0:
            return float('-inf')
        row = self.rows[self.key(pressure, incumbent, backend)]
        s = self.settings
        prior = s['minimum_observations']
        expected_gain = count * (prior * s['prior_improvement_probability'] * s['prior_gain'] + row['gain']) / (prior + row['n'])
        per_validation = (prior * s['prior_validation_ms'] + row['ms']) / (prior + row['n'])
        cost = self.preparation_ms + count * per_validation
        if cost >= remaining_ms:
            return float('-inf')
        stale_rate = self.stale / self.completions if self.completions else 0.0
        return (expected_gain - s['time_weight'] * (1 + queue_depth / max(1, workers)) * cost / remaining_ms
                - s['stale_weight'] * stale_rate)

    def observe(self, pressure, incumbent, backend, gains, elapsed_ms):
        row = self.rows[self.key(pressure, incumbent, backend)]
        row['n'] += len(gains)
        row['wins'] += sum(g > 1e-12 for g in gains)
        row['gain'] += sum(gains)
        row['ms'] += elapsed_ms

    def completed(self, decision):
        # Credit is supplied with the completed job, not the latest solve's state.
        if decision.get('best_true_objective') is None:
            return
        self.completions += 1
        self.stale += int(decision.get('final_reason') == 'stale' or decision.get('fallback_used', False))


class GuidedHybridSolver(HybridSolver):
    def __init__(self, config):
        super().__init__(config)
        self.guided = self.settings['guided']
        self.profiles = OpportunityProfiles(config)
        self.guidance_policy = ValidationPolicy(self.guided)
        self.validator.capture_feedback = True
        self.incumbent_solver.capture_feedback = True
        self.decision_context = {}
        self.requests_seen = 0

    def observe_completion(self, decision):
        self.guidance_policy.completed(decision)

    def solve(self, substrate, request, budget=None):
        budget = budget or Budget(self.settings['budget_ms'])
        self.validator.budget = budget
        self.last_calls, self.last_decision, self.fallback_allocations = [], {}, []
        self.requests_seen += 1
        started = time.perf_counter()
        phases = dict.fromkeys(('incumbent', 'candidate', 'qubo', 'sampling', 'validation'), 0.0)
        self.profiles.observe(request)
        raw_cost = self.profiles.costs(substrate, request)
        weight = self.guided['fragmentation_weight']
        host_cost = lambda v, h: weight * raw_cost(v, h) if weight else 0.0
        def objective(allocation):
            return true_objective(substrate, allocation, self.settings) + sum(
                host_cost(v, h) for v, h in allocation.placement.items())
        incumbent = best = None
        score = initial_score = None
        reason, stop = 'compute', 'candidate_exhausted'
        validations = duplicates = feedback_updates = batches = repairs = 0
        open_batch = None
        seen, emitted, pair_costs, feedback = set(), set(), {}, {}
        fallback_backend = None
        retry = None
        pressure = substrate.snapshot()['cpu_utilization']
        context = self.decision_context
        exploratory = (self.guided['adaptive_validation']
                       and self.requests_seen % self.guided['exploration_interval'] == 0)
        self.incumbent_solver.last_feedback = {}
        try:
            with _phase(phases, 'incumbent'):
                incumbent, reason = self.incumbent_solver.solve(substrate, request, budget)
                best = incumbent
                initial_score = score = objective(best) if best else None
            if self.guided['feedback'] and incumbent is None:
                feedback = dict(self.incumbent_solver.last_feedback)
                if 'source' in feedback:
                    feedback['dc'] = substrate.graph.nodes[feedback['target']].get('dc')
            initial_backend = (self.settings['policy']['static_backend']
                               if self.settings['policy']['name'] == 'static' else 'exact')
            utility = self.guidance_policy.utility(pressure, incumbent is not None, initial_backend, budget.remaining_ms,
                                                   context.get('queued_requests', 0), context.get('workers', 1))
            allowed = (incumbent is None or not self.guided['adaptive_validation'] or exploratory or utility > 0)
            if not allowed:
                stop = 'utility_stop'
            elif incumbent is not None and not self.settings['optimize_existing']:
                stop = 'incumbent_sufficient'
            else:
                for attempt in range(self.settings['max_backend_calls']):
                    if validations >= self.settings['max_validations']:
                        stop = 'validation_limit'
                        break
                    budget.check()
                    prep = time.perf_counter()
                    reuse = retry is not None
                    with _phase(phases, 'candidate'):
                        if reuse:
                            neighborhood, info, qubo = retry
                            retry = None
                        else:
                            groups = neighborhoods(
                                substrate, request, best, self.settings, budget,
                                feedback=feedback if self.guided['feedback'] else None,
                                attempt=attempt,
                                host_cost=lambda v, h: host_cost(v, h) * self.settings['objective']['power_reference_w'])
                            neighborhood = None
                            for group in groups:
                                signature = (group.dc, tuple(sorted(group.fixed.items())),
                                             tuple((v, tuple(hosts)) for v, hosts in sorted(group.candidates.items())))
                                if signature not in emitted:
                                    neighborhood = group
                                    emitted.add(signature)
                                    break
                        if neighborhood is None:
                            break
                        if not reuse:
                            info = features(substrate, request, neighborhood, best, self.settings)
                    if info['variables'] > self.settings['max_variables']:
                        continue
                    backend, selected = ((fallback_backend, 'backend_fallback') if fallback_backend else
                                         self.policy.select(info, budget.remaining_ms, self.pool))
                    fallback_backend = None
                    if backend == 'keep':
                        stop = selected
                        break
                    if not reuse:
                        with _phase(phases, 'qubo'):
                            qubo = build_qubo(substrate, request, neighborhood, self.settings, budget,
                                              host_cost=host_cost, pair_costs=pair_costs)
                    with _phase(phases, 'sampling'):
                        result = self.pool.sample(backend, qubo, request.id, len(self.last_calls), budget)
                    preparation_ms = (time.perf_counter() - prep) * 1000
                    self.guidance_policy.preparation_ms = .8 * self.guidance_policy.preparation_ms + .2 * preparation_ms
                    record = guided_call_record(request, neighborhood, info, qubo, result, selected)
                    record['call_id'] = len(self.last_calls)
                    self.last_calls.append(record)
                    repairs += int(attempt > 0 and bool(feedback.get('reason')) and self.guided['feedback'] and not reuse)
                    pending = []
                    for sample in sorted(result.samples, key=lambda s: (s.energy, s.bits)):
                        placement = qubo.decode(sample.bits)
                        if placement is not None:
                            record['placement_feasible_samples'] += sample.occurrences
                            pending.append((sample, placement))
                    cursor = 0
                    call_gains = []
                    change = False
                    while cursor < len(pending) and validations < self.settings['max_validations']:
                        target = next((v for v in self.guided['validation_milestones'] if v > validations),
                                      self.settings['max_validations'])
                        gains, batch_feasible, batch_feedback = [], 0, None
                        batch_started = time.perf_counter()
                        open_batch = batch_started
                        record['validation_batches'] += 1
                        batches += 1
                        while cursor < len(pending) and validations < target:
                            budget.check()
                            sample, placement = pending[cursor]
                            cursor += 1
                            signature = tuple(sorted(placement.items()))
                            if signature in seen:
                                duplicates += 1
                                continue
                            seen.add(signature)
                            # Count attempted validations including an interrupted evaluation.
                            validations += 1
                            record['validated_samples'] += 1
                            with _phase(phases, 'validation'):
                                allocation, failure = self.validator.evaluate(substrate, request, placement)
                                gain = 0.0
                                if allocation is not None:
                                    batch_feasible += 1
                                    record['routing_feasible_samples'] += sample.occurrences
                                    value = objective(allocation)
                                    if record['best_true_objective'] is None or value < record['best_true_objective']:
                                        record['best_true_objective'] = value
                                    if best is None or value < score - 1e-12:
                                        gain = score - value if best is not None else 1.0
                                        best, score = allocation, value
                                else:
                                    reason = failure
                                    if self.guided['feedback']:
                                        batch_feedback = dict(self.validator.last_feedback, dc=neighborhood.dc)
                                        segment = batch_feedback.get('segment')
                                        if segment is not None:
                                            left, right = segment - 1, segment
                                            if right == len(request.vnfs):
                                                right = -1
                                            key = (left, batch_feedback['source'], right, batch_feedback['target'])
                                            pair_costs[key] = pair_costs.get(key, 0.0) + self.guided['feedback_penalty']
                                            feedback_updates += 1
                            gains.append(gain)
                            call_gains.append(gain)
                            if incumbent is None and best is not None:
                                stop = 'rescued'
                                break
                        elapsed = (time.perf_counter() - batch_started) * 1000
                        open_batch = None
                        record['validation_ms'] += elapsed
                        self.guidance_policy.observe(pressure, incumbent is not None, backend, gains, elapsed)
                        if stop == 'rescued':
                            break
                        if batch_feedback:
                            feedback = batch_feedback
                        utility = self.guidance_policy.utility(
                            pressure, incumbent is not None, backend, budget.remaining_ms,
                            context.get('queued_requests', 0), context.get('workers', 1))
                        if best is not None and self.guided['adaptive_validation'] and utility <= 0 and not exploratory:
                            stop = 'utility_stop'
                            break
                        if not batch_feasible and batch_feedback and attempt + 1 < self.settings['max_backend_calls']:
                            change = True
                            break
                    complete = (cursor >= len(pending) and result.metadata.get('exhaustive', False))
                    record['validation_candidates_truncated'] = not complete
                    record['neighborhood_search_complete'] = bool(complete and result.backend == 'exact')
                    record['neighborhood_optimal'] = bool(record['neighborhood_search_complete'] and record['best_true_objective'] is not None)
                    record['reward'] = sum(call_gains) - (preparation_ms + record['validation_ms']) / self.settings['budget_ms']
                    if backend in ('qaoa_sim', 'qa') and not record['routing_feasible_samples']:
                        fallback_backend = 'sa'
                        retry = neighborhood, info, qubo
                    if stop in ('rescued', 'utility_stop'):
                        break
                    if validations >= self.settings['max_validations']:
                        stop = 'validation_limit'
                        break
                    if change:
                        stop = 'feedback_repair'
                    else:
                        stop = 'candidate_exhausted'
        except TimeoutError:
            stop = 'budget'
            if self.last_calls:
                if open_batch is not None:
                    self.last_calls[-1]['validation_ms'] += (time.perf_counter() - open_batch) * 1000
                self.last_calls[-1]['validation_candidates_truncated'] = True
                self.last_calls[-1]['neighborhood_search_complete'] = False
                self.last_calls[-1]['neighborhood_optimal'] = False
        if budget.expired:
            stop = 'budget'
        if incumbent is not None and best is not incumbent:
            self.fallback_allocations = [incumbent]
        elapsed = (time.perf_counter() - started) * 1000
        phases['other'] = max(0.0, elapsed - sum(phases.values()))
        self.last_decision = {
            'request_id': request.id, 'incumbent_feasible': incumbent is not None,
            'rescued': incumbent is None and best is not None,
            'improved': incumbent is not None and best is not None and score < initial_score - 1e-12,
            'best_true_objective': score, 'backend_calls': len(self.last_calls),
            'budget_exhausted': stop == 'budget' or budget.expired,
            'validation_attempts': validations, 'policy_reasons': [stop],
            'snapshot_version': substrate.version, 'wall_time_ms': elapsed, 'phase_times_ms': phases,
            'guided_enabled': True, 'guided_stop_reason': stop, 'guided_validation_batches': batches,
            'guided_feedback_updates': feedback_updates, 'guided_duplicate_skips': duplicates,
            'guided_feedback_repairs': repairs,
            'guided_exploration': exploratory and bool(self.last_calls),
            'guided_fragmentation_loss': sum(raw_cost(v, h) for v, h in best.placement.items()) if best else None,
        }
        return best, None if best is not None else ('budget' if stop == 'budget' else reason or 'compute')


def guided_call_record(request, group, info, qubo, result, selected):
    return {'request_id': request.id, 'backend': result.backend, 'kind': result.kind,
            'status': result.status, 'selection_reason': selected, 'dc': group.dc,
            'features': info, 'variables': len(qubo.variables), 'couplers': len(qubo.quadratic),
            'penalty': qubo.penalty, 'fixed_vnfs': len(group.fixed), 'optimized_vnfs': len(group.candidates),
            'samples': sum(s.occurrences for s in result.samples), 'unique_samples': len(result.samples),
            'placement_feasible_samples': 0, 'validated_samples': 0, 'routing_feasible_samples': 0,
            'best_true_objective': None, 'metadata': result.metadata, 'reward': 0.0,
            'validation_ms': 0.0, 'validation_batches': 0, 'neighborhood_optimal': False,
            'neighborhood_search_complete': False, 'validation_candidates_truncated': True,
            'best_qubo_energy': min((s.energy for s in result.samples), default=None)}
