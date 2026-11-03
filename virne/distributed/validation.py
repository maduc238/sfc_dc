"""Shared routing and full feasibility validation for classical and hybrid solvers."""

from itertools import islice, pairwise

import networkx as nx

from virne.distributed.model import Allocation, EPSILON, edge_key


class EmbeddingValidator:
    def __init__(self, config):
        self.settings = config['solver']
        self.budget = None

    def _path(self, substrate, trial, source, target, demand):
        if source == target:
            return [source]
        graph = substrate.graph

        def allowed_edge(u, v):
            capacity = graph.edges[u, v]['capacity_mbps']
            edge = edge_key(u, v)
            return capacity is None or (substrate.bandwidth[edge] + trial.bandwidth.get(edge, 0)
                                        + demand <= capacity + EPSILON)

        residual = nx.subgraph_view(
            graph,
            filter_node=lambda n: graph.nodes[n]['kind'] != 'server' or n in (source, target),
            filter_edge=allowed_edge,
        )
        energy_aware = self.settings['name'] in ('hre', 'ahq')
        try:
            paths = []
            for path in islice(nx.shortest_simple_paths(
                residual, source, target, weight=None if energy_aware else 'latency_ms'
            ), self.settings['k_paths']):
                if self.budget is not None:
                    self.budget.check()
                paths.append(path)
        except nx.NetworkXNoPath:
            return None
        if not paths:
            return None

        def cost(path):
            delay = sum(graph.edges[u, v]['latency_ms'] for u, v in pairwise(path))
            if not energy_aware:
                return (delay, len(path), tuple(path))
            power = substrate.power_config
            increment = 0.0
            for node in path:
                if graph.nodes[node]['kind'] == 'switch' and not substrate.switch_is_active(node, trial):
                    increment += power['switch_idle_w'] - power['switch_off_w']
            for u, v in pairwise(path):
                edge = edge_key(u, v)
                if substrate.bandwidth[edge] + trial.bandwidth.get(edge, 0) <= EPSILON:
                    increment += power['switch_port_w'] * sum(
                        graph.nodes[n]['kind'] == 'switch' for n in (u, v))
            return (len(path), increment, delay, tuple(path))
        return min(paths, key=cost)

    def _route(self, substrate, request, trial):
        endpoints = [request.ingress] + [trial.placement[i] for i in range(len(request.vnfs))]
        if request.return_to_ingress:
            endpoints.append(request.ingress)
        segments = list(enumerate(pairwise(endpoints)))
        segments.sort(key=lambda segment: (-request.bandwidths[segment[0]], segment[0]))
        trial.latency_ms = len(request.vnfs) * request.processing_delay_ms
        for index, (source, target) in segments:
            if self.budget is not None:
                self.budget.check()
            demand = request.bandwidths[index]
            path = self._path(substrate, trial, source, target, demand)
            if path is None:
                return 'bandwidth'
            trial.reserve_path(index, path, demand)
            trial.latency_ms += sum(substrate.graph.edges[u, v]['latency_ms'] for u, v in pairwise(path))
            src_dc, dst_dc = (substrate.graph.nodes[n].get('dc') for n in (source, target))
            if src_dc is not None and dst_dc is not None and src_dc != dst_dc:
                trial.inter_dc_virtual_links += 1
        if request.max_latency_ms is not None and trial.latency_ms > request.max_latency_ms + EPSILON:
            return 'latency'
        return None

    def evaluate(self, substrate, request, placement):
        """Evaluate a complete placement with fresh tentative reservations."""
        if set(placement) != set(range(len(request.vnfs))):
            return None, 'compute'
        if not self.settings['allow_colocation'] and len(set(placement.values())) != len(placement):
            return None, 'compute'
        trial = Allocation(request.id)
        for index, host in sorted(placement.items()):
            if host not in substrate.cpu or not substrate.can_host(host, request.vnfs[index], trial):
                return None, 'compute'
            trial.reserve_server(index, host, request.vnfs[index])
        if self.settings['scope'] == 'single_dc' and len({
            substrate.graph.nodes[h]['dc'] for h in placement.values()
        }) > 1:
            return None, 'compute'
        reason = self._route(substrate, request, trial)
        return (None, reason) if reason else (trial, None)

    def is_currently_feasible(self, substrate, request, allocation):
        """Audit an existing proposal without changing its paths or live resources.

        Used at asynchronous completion: stale snapshots are safe to commit only
        if the *whole* transaction still fits. No uncharged re-optimization here.
        """
        if allocation.request_id != request.id or set(allocation.placement) != set(range(len(request.vnfs))):
            return False
        placement = allocation.placement
        if not self.settings['allow_colocation'] and len(set(placement.values())) != len(placement):
            return False
        check = Allocation(request.id)
        for index, host in sorted(placement.items()):
            if host not in substrate.cpu or not substrate.can_host(host, request.vnfs[index], check):
                return False
            check.reserve_server(index, host, request.vnfs[index])
        if self.settings['scope'] == 'single_dc' and len({
            substrate.graph.nodes[h]['dc'] for h in placement.values()
        }) > 1:
            return False
        endpoints = [request.ingress] + [placement[i] for i in range(len(request.vnfs))]
        if request.return_to_ingress:
            endpoints.append(request.ingress)
        if set(allocation.paths) != set(range(len(endpoints) - 1)):
            return False
        delay = len(request.vnfs) * request.processing_delay_ms
        for index, (source, target) in enumerate(pairwise(endpoints)):
            path = allocation.paths[index]
            if not path or path[0] != source or path[-1] != target or len(set(path)) != len(path):
                return False
            if any(n not in substrate.graph or substrate.graph.nodes[n]['kind'] == 'server'
                   for n in path[1:-1]):
                return False
            for u, v in pairwise(path):
                if not substrate.graph.has_edge(u, v):
                    return False
                delay += substrate.graph.edges[u, v]['latency_ms']
            check.reserve_path(index, path, request.bandwidths[index])
        for edge, amount in check.bandwidth.items():
            capacity = substrate.graph.edges[edge]['capacity_mbps']
            if capacity is not None and substrate.bandwidth[edge] + amount > capacity + EPSILON:
                return False
        for actual, claimed in ((check.cpu, allocation.cpu), (check.memory, allocation.memory),
                                (check.bandwidth, allocation.bandwidth)):
            if actual.keys() != claimed.keys() or any(abs(v - claimed[k]) > EPSILON for k, v in actual.items()):
                return False
        return (abs(delay - allocation.latency_ms) <= EPSILON
                and (request.max_latency_ms is None or delay <= request.max_latency_ms + EPSILON))

