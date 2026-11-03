"""Transparent heuristic baselines; HRE is a bounded adaptation of the paper."""

from collections import defaultdict
import random

import networkx as nx

from virne.distributed.model import Allocation, EPSILON
from virne.distributed.validation import EmbeddingValidator


def block_order(request):
    # A placement order only: logical chain edges always retain their original indices.
    def block(index):
        bandwidth = request.bandwidths[index]
        if index + 1 < len(request.bandwidths):
            bandwidth += request.bandwidths[index + 1]
        return request.vnfs[index].cpu * bandwidth
    return sorted(range(len(request.vnfs)), key=lambda i: (-block(i), i))


def pyramid_orders(request):
    """FFD first, then move its maximum through ascending-left/descending-right orders.

    This explicit O(n) sweep is a reproducible PPA-inspired interpretation, not an
    exhaustive permutation search (the paper does not supply reference code).
    """
    descending = block_order(request)
    yield descending
    for pointer in range(1, len(descending)):
        yield list(reversed(descending[1:pointer + 1])) + [descending[0]] + descending[pointer + 1:]


class HeuristicSolver(EmbeddingValidator):
    def __init__(self, config):
        super().__init__(config)
        # Never let tie-breaking consume the workload RNG stream.
        self.rng = random.Random(config['experiment']['seed'] + 1_000_003)

    def _groups(self, substrate, request):
        graph = substrate.graph
        by_dc = defaultdict(list)
        for host in substrate.servers:
            by_dc[graph.nodes[host]['dc']].append(host)
        distance = nx.single_source_dijkstra_path_length(graph, request.ingress, weight='latency_ms')
        dcs = sorted(by_dc, key=lambda dc: (min(distance[h] for h in by_dc[dc]), dc))
        name = self.settings['name']
        if name == 'random':
            self.rng.shuffle(dcs)
        groups = []
        for dc in dcs:
            hosts = by_dc[dc]
            if name == 'hre':
                # Near (rack), middle (pod), then far (entire DC).
                for attr in ('rack', 'pod'):
                    local = defaultdict(list)
                    for host in hosts:
                        local[graph.nodes[host][attr]].append(host)
                    groups.extend(local.values())
            groups.append(hosts)
        if self.settings['scope'] == 'cross_dc':
            # Append a global pool; energy ranking may prefer it over a local candidate.
            groups.append([host for dc in dcs for host in by_dc[dc]])
        seen = set()
        for group in groups:
            key = tuple(group)
            if key not in seen:
                seen.add(key)
                yield group

    def _place(self, substrate, request, order, hosts):
        trial = Allocation(request.id)
        name = self.settings['name']
        candidates = list(hosts)
        if name == 'random':
            self.rng.shuffle(candidates)
        elif name == 'hre':
            candidates.sort(key=lambda h: substrate.cpu[h] <= EPSILON)
        for index in order:
            vnf = request.vnfs[index]
            feasible = [h for h in candidates if (
                self.settings['allow_colocation'] or h not in trial.cpu
            ) and substrate.can_host(h, vnf, trial)]
            if not feasible:
                return None
            if name == 'best_fit':
                def remaining(host):
                    attrs = substrate.graph.nodes[host]
                    return ((attrs['cpu'] - substrate.cpu[host] - trial.cpu.get(host, 0) - vnf.cpu)
                            / attrs['cpu'] + (attrs['memory_gb'] - substrate.memory[host]
                            - trial.memory.get(host, 0) - vnf.memory_gb) / attrs['memory_gb'])
                feasible.sort(key=remaining)
            trial.reserve_server(index, feasible[0], vnf)
        return trial

    def solve(self, substrate, request, budget=None):
        """Return a feasible transaction or the deepest constraint reached by any candidate."""
        self.budget = budget
        name = self.settings['name']
        if name == 'hre':
            orders = pyramid_orders(request)
        else:
            order = block_order(request)
            if name == 'random':
                self.rng.shuffle(order)
            orders = [order]
        groups = list(self._groups(substrate, request))
        reasons = {'compute'}
        for order in orders:
            if budget is not None:
                budget.check()
            placements = [trial for hosts in groups
                          if (trial := self._place(substrate, request, order, hosts)) is not None]
            if name == 'hre':
                # Stable ties retain proximity/group order.
                placements.sort(key=lambda trial: sum(substrate.cpu[h] <= EPSILON for h in trial.cpu))
            for trial in placements:
                if budget is not None:
                    budget.check()
                reason = self._route(substrate, request, trial)
                if reason is None:
                    return trial, None
                reasons.add(reason)
        return None, 'latency' if 'latency' in reasons else 'bandwidth' if 'bandwidth' in reasons else 'compute'
