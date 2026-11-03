"""Local exact/SA, native QAOA statevector and explicitly enabled remote annealing."""

from collections import Counter
from dataclasses import dataclass, field
from itertools import product
import math
import multiprocessing
import random
import time


@dataclass
class Sample:
    bits: tuple
    energy: float
    occurrences: int = 1


@dataclass
class BackendResult:
    backend: str
    kind: str
    status: str
    samples: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


def exact_sample(qubo, settings, seed, budget):
    samples = []
    variables = sorted(qubo.neighborhood.candidates)
    visited = 0
    complete = True
    for hosts in product(*(qubo.neighborhood.candidates[v] for v in variables)):
        if budget.expired or visited >= settings['max_assignments']:
            complete = False
            break
        visited += 1
        placement = dict(qubo.neighborhood.fixed)
        placement.update(zip(variables, hosts))
        if len(set(placement.values())) != len(placement):
            continue
        bits = qubo.encode(placement)
        samples.append(Sample(bits, qubo.energy(bits)))
    samples.sort(key=lambda s: (s.energy, s.bits))
    return BackendResult('exact', 'classical', 'complete' if complete else 'budget_exhausted', samples,
                         {'assignments_visited': visited, 'exhaustive': complete,
                          'optimality_scope': 'reduced_placement_QUBO_only'})


def sa_sample(qubo, settings, seed, budget):
    rng = random.Random(seed)
    n = len(qubo.variables)
    scale = max([1.0] + [abs(v) for v in qubo.linear.values()] + [abs(v) for v in qubo.quadratic.values()])
    neighbors = [[] for _ in range(n)]
    for (i, j), coefficient in qubo.quadratic.items():
        neighbors[i].append((j, coefficient))
        neighbors[j].append((i, coefficient))
    found = Counter()
    warm = qubo.encode(qubo.neighborhood.warm_start)
    completed = 0
    for read in range(settings['reads']):
        if budget.expired:
            break
        bits = list(warm) if read == 0 else [rng.randrange(2) for _ in range(n)]
        best, best_energy = tuple(bits), qubo.energy(bits)
        for sweep in range(settings['sweeps']):
            if budget.expired:
                break
            fraction = sweep / max(1, settings['sweeps'] - 1)
            beta = settings['beta_start'] * (settings['beta_end'] / settings['beta_start']) ** fraction
            order = list(range(n))
            rng.shuffle(order)
            for i in order:
                delta = (1 - 2 * bits[i]) * (qubo.linear[i] + sum(v * bits[j] for j, v in neighbors[i]))
                if delta <= 0 or rng.random() < math.exp(-beta * delta / scale):
                    bits[i] ^= 1
            energy = qubo.energy(bits)
            if energy < best_energy:
                best, best_energy = tuple(bits), energy
        found[best] += 1
        completed += 1
    samples = sorted((Sample(bits, qubo.energy(bits), count) for bits, count in found.items()),
                     key=lambda sample: (sample.energy, sample.bits))
    return BackendResult('sa', 'classical', 'complete' if not budget.expired else 'budget_exhausted', samples,
                         {'reads_completed': completed, 'sweeps_per_read': settings['sweeps'],
                          'coefficient_scale': scale, 'warm_started': True})


def qaoa_statevector(energies, gammas, betas, budget=None):
    """Apply exp(-i gamma H_QUBO), exp(-i beta sum X) to |+>^n.

    State index bit i corresponds to placement variable i. Ideal statevector;
    no fabricated hardware qubit counts, transpilation or QPU timing.
    """
    import numpy as np

    n = (len(energies) - 1).bit_length()
    if len(energies) != 1 << n or len(gammas) != len(betas):
        raise ValueError('Invalid QAOA state dimension or parameter layers')
    state = np.full(1 << n, 1 / math.sqrt(1 << n), dtype=np.complex128)
    for gamma, beta in zip(gammas, betas):
        if budget is not None:
            budget.check()
        state *= np.exp(-1j * gamma * energies)
        for bit in range(n):
            blocks = state.reshape(-1, 2, 1 << bit)
            low, high = blocks[:, 0, :].copy(), blocks[:, 1, :].copy()
            blocks[:, 0, :] = math.cos(beta) * low - 1j * math.sin(beta) * high
            blocks[:, 1, :] = math.cos(beta) * high - 1j * math.sin(beta) * low
    return state


def qaoa_sample(qubo, settings, seed, budget):
    n = len(qubo.variables)
    if n > settings['max_variables']:
        return BackendResult('qaoa_sim', 'quantum_simulation', 'size_limit')
    try:
        import numpy as np
    except ImportError:
        return BackendResult('qaoa_sim', 'quantum_simulation', 'unavailable', metadata={'dependency': 'numpy'})
    rng = np.random.default_rng(seed)
    states = np.arange(1 << n, dtype=np.uint64)
    energies = np.full(1 << n, qubo.offset)
    for i, value in qubo.linear.items():
        energies += value * ((states >> i) & 1)
    for (i, j), value in qubo.quadratic.items():
        energies += value * ((states >> i) & 1) * ((states >> j) & 1)
    scale = max(1.0, max(abs(v) for v in qubo.linear.values()),
                max((abs(v) for v in qubo.quadratic.values()), default=0.0))
    normalized = (energies - qubo.offset) / scale
    layers = settings['layers']
    best_params = np.r_[np.full(layers, 0.5), np.full(layers, 0.25)]
    best_value = float('inf')
    best_state = None
    evaluated = 0
    # Bounded seeded pattern search, with exact expectations on a CPU statevector.
    for iteration in range(settings['evaluations']):
        if budget.expired:
            break
        params = best_params.copy()
        if iteration:
            params += rng.normal(0, 0.7 / math.sqrt(iteration), 2 * layers)
        try:
            state = qaoa_statevector(normalized, params[:layers], params[layers:], budget)
        except TimeoutError:
            break
        value = float(np.dot(np.abs(state) ** 2, normalized))
        evaluated += 1
        if value < best_value:
            best_value, best_state, best_params = value, state, params
    metadata = {'logical_qubits': n, 'layers': layers, 'optimizer_evaluations': evaluated,
                'shots': settings['shots'] if best_state is not None else 0,
                'logical_zz_terms': len(qubo.quadratic), 'physical_qubits': None,
                'transpiled_depth': None, 'coefficient_scale': scale,
                'expectation_mode': 'exact_statevector',
                'readout_flip_probability': settings['readout_flip_probability']}
    if best_state is None:
        return BackendResult('qaoa_sim', 'quantum_simulation', 'budget_exhausted', metadata=metadata)
    probabilities = np.abs(best_state) ** 2
    probabilities /= probabilities.sum()
    outcomes = rng.choice(1 << n, settings['shots'], p=probabilities)
    found = Counter()
    for outcome in outcomes:
        bits = tuple(int((int(outcome) >> i) & 1) ^ int(rng.random() < settings['readout_flip_probability'])
                     for i in range(n))
        found[bits] += 1
    samples = sorted((Sample(bits, qubo.energy(bits), count) for bits, count in found.items()),
                     key=lambda sample: (sample.energy, sample.bits))
    return BackendResult('qaoa_sim', 'quantum_simulation',
                         'budget_exhausted' if budget.expired else 'complete', samples, metadata)


def _qa_worker(connection, qubo, settings, timeout_s):
    """Spawned process so SDK/network calls cannot block the controller forever."""
    try:
        from dwave.system import DWaveSampler, EmbeddingComposite

        kwargs = {'request_timeout': timeout_s, 'polling_timeout': timeout_s}
        if settings['solver']:
            kwargs['solver'] = settings['solver']
        with DWaveSampler(**kwargs) as sampler:
            response = EmbeddingComposite(sampler).sample_qubo(
                qubo, num_reads=settings['num_reads'], annealing_time=settings['annealing_time_us'],
                return_embedding=True)
            response.resolve()
            embedding = response.info.get('embedding_context', {}).get('embedding', {})
            samples = [(tuple(int(row.sample[i]) for i in range(len({i for e in qubo for i in e}))),
                        int(row.num_occurrences)) for row in response.data(fields=['sample', 'num_occurrences'])]
            metadata = {
                'physical_qubits': len({q for chain in embedding.values() for q in chain}),
                'maximum_chain_length': max((len(chain) for chain in embedding.values()), default=0),
                'timing_us': {key: float(value) for key, value in response.info.get('timing', {}).items()
                              if isinstance(value, (int, float))},
                'solver_id': str(sampler.solver.id),
            }
            if 'chain_break_fraction' in response.record.dtype.names:
                metadata['mean_chain_break_fraction'] = float(
                    sum(response.record.chain_break_fraction * response.record.num_occurrences)
                    / sum(response.record.num_occurrences))
            connection.send(('complete', samples, metadata))
    except ImportError:
        connection.send(('unavailable', [], {'dependency': 'dwave-system'}))
    except Exception as error:
        # SDK messages can contain URLs/credentials; persist only the exception type.
        connection.send(('backend_error', [], {'error_type': type(error).__name__}))
    finally:
        connection.close()


class BackendPool:
    def __init__(self, settings, seed):
        self.settings = settings
        self.seed = seed
        self.remote_calls = 0
        self.estimated_cost = 0.0

    def available(self, name, variables=0):
        if name == 'qa':
            q = self.settings['qa']
            return (q['enabled'] and self.remote_calls < q['max_calls_per_run']
                    and self.estimated_cost + q['estimated_cost_per_call'] <= q['max_cost_per_run'] + 1e-12)
        if name == 'qaoa_sim':
            return variables <= self.settings['qaoa']['max_variables']
        return name in ('exact', 'sa')

    def sample(self, name, qubo, request_id, call_id, budget):
        started = time.perf_counter()
        seed = self.seed + 1_000_003 * request_id + 97 * call_id
        if name == 'qa':
            result = self._qa(qubo, budget)
        elif name == 'exact':
            result = exact_sample(qubo, self.settings['exact'], seed, budget)
        elif name == 'sa':
            result = sa_sample(qubo, self.settings['sa'], seed, budget)
        elif name == 'qaoa_sim':
            result = qaoa_sample(qubo, self.settings['qaoa'], seed, budget)
        else:
            raise ValueError(f'Unknown hybrid backend: {name}')
        result.metadata.update(wall_time_ms=(time.perf_counter() - started) * 1000, seed=seed)
        return result

    def _qa(self, qubo, budget):
        settings = self.settings['qa']
        if not self.available('qa'):
            return BackendResult('qa', 'quantum_hardware', 'disabled_or_budget_limit')
        if budget.expired:
            return BackendResult('qa', 'quantum_hardware', 'budget_exhausted')
        self.remote_calls += 1  # Charge dispatch attempts, including uncertain timeouts.
        self.estimated_cost += settings['estimated_cost_per_call']
        timeout = min(budget.remaining_ms, settings['timeout_ms']) / 1000
        context = multiprocessing.get_context('spawn')
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=_qa_worker, args=(sender, qubo.as_qubo(), settings, timeout), daemon=True)
        process.start()
        sender.close()
        metadata = {'estimated_cost': settings['estimated_cost_per_call'], 'logical_variables': len(qubo.variables),
                    'physical_qubits': None, 'remote_completion_unknown': False, 'remote_dispatch_attempted': True}
        try:
            if receiver.poll(min(timeout, budget.remaining_ms / 1000)):
                try:
                    status, rows, details = receiver.recv()
                except EOFError:
                    status, rows, details = 'backend_error', [], {'error_type': 'WorkerExited'}
                metadata.update(details)
                samples = [Sample(bits, qubo.energy(bits), count) for bits, count in rows]
                return BackendResult('qa', 'quantum_hardware', status, samples, metadata)
            metadata['remote_completion_unknown'] = True
            return BackendResult('qa', 'quantum_hardware', 'timeout', metadata=metadata)
        finally:
            if process.is_alive():
                process.terminate()
            process.join(timeout=1)
            receiver.close()
