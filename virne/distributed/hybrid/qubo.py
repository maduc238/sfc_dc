"""Placement QUBO: exact one-hot/distinct-host penalties, routing surrogate only."""

from dataclasses import dataclass
from itertools import combinations, pairwise

import networkx as nx

from virne.distributed.hybrid.candidates import incremental_server_power


@dataclass
class PlacementQUBO:
    variables: list
    linear: dict
    quadratic: dict
    offset: float
    penalty: float
    neighborhood: object

    def energy(self, bits):
        return (self.offset + sum(value * bits[i] for i, value in self.linear.items())
                + sum(value * bits[i] * bits[j] for (i, j), value in self.quadratic.items()))

    def encode(self, placement):
        return tuple(int(placement.get(v) == s) for v, s in self.variables)

    def decode(self, bits):
        if len(bits) != len(self.variables) or any(b not in (0, 1) for b in bits):
            return None
        result = dict(self.neighborhood.fixed)
        for bit, (v, host) in zip(bits, self.variables):
            if bit:
                if v in result:
                    return None
                result[v] = host
        if not self.neighborhood.candidates.keys() <= result.keys() or len(set(result.values())) != len(result):
            return None
        return result

    def as_qubo(self):
        return {**{(i, i): value for i, value in self.linear.items()}, **self.quadratic}


def build_qubo(substrate, request, neighborhood, settings, budget):
    variables = [(v, h) for v, hosts in sorted(neighborhood.candidates.items()) for h in hosts]
    linear = dict.fromkeys(range(len(variables)), 0.0)
    quadratic = {}
    index = {var: i for i, var in enumerate(variables)}
    objective = settings['objective']
    for i, (v, h) in enumerate(variables):
        linear[i] = (objective['power_weight'] * incremental_server_power(substrate, h, request.vnfs[v].cpu)
                     / objective['power_reference_w'])
    graph = substrate.graph
    distances = {}

    def delay(source, target):
        # Server leaves cannot be intermediate nodes in these generated fabrics.
        if source not in distances:
            budget.check()
            distances[source] = nx.single_source_dijkstra_path_length(graph, source, weight='latency_ms')
        return distances[source][target] * objective['latency_weight'] / objective['latency_reference_ms']

    def choices(v):
        if v == -1:
            return [(request.ingress, None)]
        if v in neighborhood.fixed:
            return [(neighborhood.fixed[v], None)]
        return [(h, index[v, h]) for h in neighborhood.candidates[v]]

    chain = [-1] + list(range(len(request.vnfs))) + ([-1] if request.return_to_ingress else [])
    for left, right in pairwise(chain):
        for source, i in choices(left):
            for target, j in choices(right):
                cost = delay(source, target)
                if i is not None and j is not None:
                    key = tuple(sorted((i, j)))
                    quadratic[key] = quadratic.get(key, 0.0) + cost
                elif i is not None or j is not None:
                    k = i if i is not None else j
                    linear[k] += cost
                # Constant fixed placement/processing cost does not affect ranking.
    bound = sum(abs(v) for v in linear.values()) + sum(abs(v) for v in quadratic.values())
    penalty = bound + 1.0
    offset = penalty * len(neighborhood.candidates)
    for v, hosts in neighborhood.candidates.items():
        indices = [index[v, h] for h in hosts]
        for i in indices:
            linear[i] -= penalty
        for i, j in combinations(indices, 2):
            key = tuple(sorted((i, j)))
            quadratic[key] = quadratic.get(key, 0.0) + 2 * penalty
    for i, (v, host) in enumerate(variables):
        for j in range(i + 1, len(variables)):
            w, other = variables[j]
            if v != w and host == other:
                quadratic[i, j] = quadratic.get((i, j), 0.0) + penalty
    return PlacementQUBO(variables, linear, quadratic, offset, penalty, neighborhood)


def true_objective(substrate, allocation, settings):
    """Incremental power of the union of allocated resources, with no live mutation."""
    p = substrate.power_config
    delta = sum(incremental_server_power(substrate, h, cpu) for h, cpu in allocation.cpu.items())
    switches = {n for e in allocation.bandwidth for n in e if substrate.graph.nodes[n]['kind'] == 'switch'}
    for node in switches:
        if not substrate.switch_is_active(node):
            delta += p['switch_idle_w'] - p['switch_off_w']
    for edge in allocation.bandwidth:
        if substrate.bandwidth[edge] <= 1e-8:
            delta += p['switch_port_w'] * sum(substrate.graph.nodes[n]['kind'] == 'switch' for n in edge)
    obj = settings['objective']
    return (obj['power_weight'] * delta / obj['power_reference_w']
            + obj['latency_weight'] * allocation.latency_ms / obj['latency_reference_ms'])
