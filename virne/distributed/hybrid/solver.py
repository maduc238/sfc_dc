"""AHQ-SFC: bounded neighborhood optimization with a shared feasibility validator."""

from copy import deepcopy
from contextlib import contextmanager
import time

from virne.distributed.hybrid.backends import BackendPool
from virne.distributed.hybrid.budget import Budget
from virne.distributed.hybrid.candidates import features, neighborhoods
from virne.distributed.hybrid.policy import SelectionPolicy
from virne.distributed.hybrid.qubo import build_qubo, true_objective
from virne.distributed.solver import HeuristicSolver
from virne.distributed.validation import EmbeddingValidator


@contextmanager
def _phase(times, name):
    started = time.perf_counter()
    try:
        yield
    finally:
        times[name] += (time.perf_counter() - started) * 1000


def _timed_neighborhoods(iterator, times):
    while True:
        try:
            with _phase(times, 'candidate'):
                group = next(iterator)
        except StopIteration:
            return
        yield group


class HybridSolver:
    def __init__(self, config):
        self.config = config
        self.settings = config['hybrid']
        baseline = deepcopy(config)
        baseline['solver']['name'] = self.settings['incumbent_solver']
        self.incumbent_solver = HeuristicSolver(baseline)
        self.validator = EmbeddingValidator(config)
        self.pool = BackendPool(self.settings, config['experiment']['seed'])
        self.policy = SelectionPolicy(self.settings)
        self.last_calls = []
        self.last_decision = {}
        self.fallback_allocations = []

    def solve(self, substrate, request, budget=None):
        budget = budget or Budget(self.settings['budget_ms'])
        self.validator.budget = budget
        self.last_calls, self.fallback_allocations = [], []
        started = time.perf_counter()
        incumbent = best = None
        score = None
        reason = 'compute'
        validations = 0
        budget_hit = False
        selected_policies = []
        phases = dict.fromkeys(('incumbent', 'candidate', 'qubo'), 0.0)
        try:
            with _phase(phases, 'incumbent'):
                incumbent, reason = self.incumbent_solver.solve(substrate, request, budget)
            best = incumbent
            score = true_objective(substrate, best, self.settings) if best else None
            if incumbent is not None and not self.settings['optimize_existing']:
                selected_policies.append('incumbent_sufficient')
            else:
                groups = _timed_neighborhoods(iter(neighborhoods(substrate, request, incumbent, self.settings, budget)), phases)
                while len(self.last_calls) < self.settings['max_backend_calls']:
                    try:
                        neighborhood = next(groups)
                    except StopIteration:
                        break
                    budget.check()
                    info = features(substrate, request, neighborhood, best, self.settings)
                    if info['variables'] > self.settings['max_variables']:
                        selected_policies.append('variable_limit')
                        continue
                    backend, selection_reason = self.policy.select(info, budget.remaining_ms, self.pool)
                    selected_policies.append(selection_reason)
                    if backend == 'keep':
                        break
                    with _phase(phases, 'qubo'):
                        qubo = build_qubo(substrate, request, neighborhood, self.settings, budget)
                    backend_queue = [(backend, selection_reason)]
                    while backend_queue and len(self.last_calls) < self.settings['max_backend_calls']:
                        action, action_reason = backend_queue.pop(0)
                        result = self.pool.sample(action, qubo, request.id, len(self.last_calls), budget)
                        best, score, failure, validations, record = self._evaluate_samples(
                            substrate, request, qubo, result, info, action_reason, best, score, validations, budget)
                        if failure and best is None:
                            reason = failure
                        if budget.expired:
                            budget_hit = True
                            break
                        # Failed quantum calls, including all-invalid samples, use the same
                        # candidate pool for a bounded classical fallback.
                        quantum_failed = action in ('qa', 'qaoa_sim') and record['routing_feasible_samples'] == 0
                        empty_incomplete = not result.samples and not result.metadata.get('exhaustive', False)
                        if action != 'sa' and (quantum_failed or empty_incomplete):
                            backend_queue.append(('sa', 'backend_fallback'))
                    if budget.expired or validations >= self.settings['max_validations']:
                        break
        except TimeoutError:
            budget_hit = True
        if incumbent is not None and best is not incumbent:
            self.fallback_allocations = [incumbent]
        improved = (incumbent is not None and best is not None
                    and score < true_objective(substrate, incumbent, self.settings) - 1e-12)
        elapsed = (time.perf_counter() - started) * 1000
        phases['sampling'] = sum(c['metadata'].get('wall_time_ms', 0) for c in self.last_calls)
        phases['validation'] = sum(c['validation_ms'] for c in self.last_calls)
        phases['other'] = max(0.0, elapsed - sum(phases.values()))
        self.last_decision = {
            'request_id': request.id, 'incumbent_feasible': incumbent is not None,
            'rescued': incumbent is None and best is not None,
            'improved': improved,
            'best_true_objective': score, 'backend_calls': len(self.last_calls),
            'budget_exhausted': budget_hit or budget.expired, 'validation_attempts': validations,
            'policy_reasons': selected_policies, 'snapshot_version': substrate.version,
            'wall_time_ms': elapsed, 'phase_times_ms': phases,
        }
        return best, None if best is not None else ('budget' if budget_hit or budget.expired else reason or 'compute')

    def _evaluate_samples(self, substrate, request, qubo, result, info, selection_reason,
                          best, score, validations, budget):
        before_score = score
        neighborhood = qubo.neighborhood
        record = {
            'request_id': request.id, 'call_id': len(self.last_calls), 'backend': result.backend,
            'kind': result.kind, 'status': result.status, 'selection_reason': selection_reason,
            'dc': neighborhood.dc, 'features': info, 'variables': len(qubo.variables),
            'couplers': len(qubo.quadratic), 'penalty': qubo.penalty,
            'fixed_vnfs': len(neighborhood.fixed), 'optimized_vnfs': len(neighborhood.candidates),
            'samples': sum(s.occurrences for s in result.samples),
            'unique_samples': len(result.samples), 'placement_feasible_samples': 0,
            'validated_samples': 0, 'routing_feasible_samples': 0,
            'best_qubo_energy': min((s.energy for s in result.samples), default=None),
            'best_true_objective': None, 'neighborhood_optimal': False,
            'metadata': result.metadata, 'reward': 0.0,
            'qubo': {'variables': qubo.variables, 'linear': qubo.linear,
                     'quadratic': [[i, j, value] for (i, j), value in qubo.quadratic.items()],
                     'offset': qubo.offset, 'fixed_placement': neighborhood.fixed},
            'sample_histogram': [{'bits': ''.join(map(str, sample.bits)), 'energy': sample.energy,
                                 'occurrences': sample.occurrences} for sample in result.samples],
        }
        self.last_calls.append(record)
        started = time.perf_counter()
        reason = None
        decoded = [(sample, qubo.decode(sample.bits)) for sample in result.samples]
        record['placement_feasible_samples'] = sum(s.occurrences for s, placement in decoded if placement is not None)
        eligible = sorted(((sample, placement) for sample, placement in decoded if placement is not None),
                          key=lambda item: (item[0].energy, item[0].bits))
        limit = self.settings['validation_top_k']
        truncated = bool(limit and len(eligible) > limit)
        record['validation_top_k'] = limit
        record['validation_candidates_truncated'] = truncated
        fully_validated = not truncated
        for sample, placement in (eligible[:limit] if limit else eligible):
            if budget.expired or validations >= self.settings['max_validations']:
                fully_validated = False
                break
            try:
                allocation, failure = self.validator.evaluate(substrate, request, placement)
            except TimeoutError:
                fully_validated = False
                break
            validations += 1
            record['validated_samples'] += 1
            if allocation is None:
                reason = failure
                continue
            record['routing_feasible_samples'] += sample.occurrences
            current_score = true_objective(substrate, allocation, self.settings)
            if record['best_true_objective'] is None or current_score < record['best_true_objective']:
                record['best_true_objective'] = current_score
            if best is None or current_score < score - 1e-12:
                best, score = allocation, current_score
        validation_ms = (time.perf_counter() - started) * 1000
        record['validation_ms'] = validation_ms
        record['neighborhood_search_complete'] = bool(
            result.backend == 'exact' and result.metadata.get('exhaustive') and fully_validated)
        record['neighborhood_optimal'] = record['neighborhood_search_complete'] and record['best_true_objective'] is not None
        gain = (1.0 if before_score is None and best is not None else
                max(0, before_score - score) if before_score is not None and score is not None else 0.0)
        record['reward'] = gain - (result.metadata.get('wall_time_ms', 0) + validation_ms) / self.settings['budget_ms']
        return best, score, reason, validations, record
