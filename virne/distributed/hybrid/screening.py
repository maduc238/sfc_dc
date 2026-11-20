"""Snapshot-local feasibility screens and lower bounds for PACE-SFC."""

from collections import defaultdict
from functools import lru_cache
from itertools import pairwise

import networkx as nx

from virne.distributed.hybrid.candidates import incremental_server_power
from virne.distributed.model import EPSILON, edge_key
from virne.distributed.validation import EmbeddingValidator


class TopologyIndex:
    """Only immutable topology data may be shared between requests."""

    def __init__(self, substrate):
        self.graph = graph = substrate.graph
        self.by_dc = defaultdict(list)
        self.cpu_capacity = self.memory_capacity = 0.0
        self.host_links = {}
        for host in substrate.servers:
            attrs = graph.nodes[host]
            self.by_dc[attrs['dc']].append(host)
            self.cpu_capacity += attrs['cpu']
            self.memory_capacity += attrs['memory_gb']
            if graph.degree(host) == 1:
                self.host_links[host] = edge_key(host, next(iter(graph[host])))
        borders = defaultdict(list)
        for u, v in graph.edges:
            a, b = graph.nodes[u]['dc'], graph.nodes[v]['dc']
            if a != b:
                for dc in (a, b):
                    if dc is not None:
                        borders[dc].append(edge_key(u, v))
        self.sole_border = {dc: edges[0] for dc, edges in borders.items() if len(edges) == 1}
        self.distances = lru_cache(maxsize=64)(
            lambda node: nx.single_source_dijkstra_path_length(graph, node, weight='latency_ms'))


class RequestScreen:
    def __init__(self, topology, substrate, request, budget, feedback, stats):
        self.topology, self.substrate, self.request = topology, substrate, request
        self.budget, self.feedback, self.stats = budget, feedback, stats
        self.components, self.dc_results, self.focus_by_dc = {}, {}, {}
        self.last_failure = None

    def reachable_from(self, source, demand):
        key = source, demand
        if key in self.components:
            return self.components[key]
        graph = self.substrate.graph
        visited, pending = {source}, [source]
        while pending:
            self.budget.check()
            node = pending.pop()
            for other, edge in graph[node].items():
                if other in visited:
                    continue
                capacity = edge['capacity_mbps']
                if capacity is not None and (
                    self.substrate.bandwidth[edge_key(node, other)] + demand > capacity + EPSILON
                ):
                    continue
                visited.add(other)
                if graph.nodes[other]['kind'] != 'server':
                    pending.append(other)
        self.components[key] = visited
        return visited

    def dc_failure(self, dc):
        s, r = self.substrate, self.request
        hosts = self.topology.by_dc[dc]
        min_cpu = min(v.cpu for v in r.vnfs)
        min_memory = min(v.memory_gb for v in r.vnfs)
        free = [(h, s.graph.nodes[h]['cpu'] - s.cpu[h] + EPSILON,
                 s.graph.nodes[h]['memory_gb'] - s.memory[h] + EPSILON) for h in hosts]
        free = [(h, cpu, mem) for h, cpu, mem in free if cpu >= min_cpu and mem >= min_memory]
        if len(free) < len(r.vnfs):
            return 'compute'
        boundaries = [(0, r.vnfs[0])]
        if r.return_to_ingress:
            boundaries.append((len(r.vnfs), r.vnfs[-1]))
        for segment, vnf in boundaries:
            compatible = [h for h, cpu, mem in free if cpu >= vnf.cpu and mem >= vnf.memory_gb]
            if not compatible:
                return 'compute'
            reached = self.reachable_from(r.ingress, r.bandwidths[segment])
            if not any(h in reached for h in compatible):
                return 'bandwidth'
        return None

    def select_dcs(self, ordered, limit):
        selected = []
        for dc in ordered:
            self.budget.check()
            if dc not in self.dc_results:
                failure = self.dc_failure(dc)
                self.dc_results[dc] = failure
                if failure:
                    self.stats['dc_rejected_' + failure] += 1
                    self.last_failure = failure
            if self.dc_results[dc] is None:
                selected.append(dc)
                if len(selected) == limit:
                    break
        return selected

    def filter_hosts(self, dc, hosts, boundaries):
        if not hosts:
            self.stats['empty_compute_windows'] += 1
            self.last_failure = 'compute'
            return []
        for endpoint, segment in boundaries:
            reached = self.reachable_from(endpoint, self.request.bandwidths[segment])
            hosts = [h for h in hosts if h in reached]
            if not hosts:
                self.stats['empty_bandwidth_windows'] += 1
                self.last_failure = 'bandwidth'
                if self.feedback:
                    self.focus_by_dc[dc] = segment
                break
        return hosts


class PlacementBound:
    """Lower bound for any feasible routing of this placement on this snapshot."""

    def __init__(self, topology, substrate, request, settings, host_cost):
        self.topology, self.substrate, self.request = topology, substrate, request
        obj = settings['objective']
        self.power_scale = obj['power_weight'] / obj['power_reference_w']
        self.latency_scale = obj['latency_weight'] / obj['latency_reference_ms']
        self.host_cost = host_cost
        self.host_terms, self.port_terms, self.switch_terms = {}, {}, {}

    def evaluate(self, placement):
        r, s = self.request, self.substrate
        hosts = [placement[i] for i in range(len(r.vnfs))]
        endpoints = [r.ingress, *hosts]
        if r.return_to_ingress:
            endpoints.append(r.ingress)
        delay = len(hosts) * r.processing_delay_ms + sum(
            self.topology.distances(a)[b] for a, b in pairwise(endpoints))
        # Each host is a distinct leaf in the supported embedding model. The
        # single DC border must also be crossed by traffic arriving from WAN.
        edges = {self.topology.host_links[h] for h in hosts if h in self.topology.host_links}
        dc = s.graph.nodes[hosts[0]]['dc']
        if dc in self.topology.sole_border and s.graph.nodes[r.ingress]['dc'] != dc:
            edges.add(self.topology.sole_border[dc])
        switches = {n for e in edges for n in e if s.graph.nodes[n]['kind'] == 'switch'}
        p = s.power_config
        for edge in edges:
            if edge not in self.port_terms:
                count = sum(s.graph.nodes[n]['kind'] == 'switch' for n in edge)
                self.port_terms[edge] = p['switch_port_w'] * count if s.bandwidth[edge] <= EPSILON else 0.0
        for n in switches:
            if n not in self.switch_terms:
                self.switch_terms[n] = 0.0 if s.switch_is_active(n) else p['switch_idle_w'] - p['switch_off_w']
        network = sum(self.port_terms[e] for e in edges) + sum(self.switch_terms[n] for n in switches)
        for i, h in enumerate(hosts):
            if (i, h) not in self.host_terms:
                self.host_terms[i, h] = (self.power_scale * incremental_server_power(s, h, r.vnfs[i].cpu)
                                         + self.host_cost(i, h))
        value = sum(self.host_terms[i, h] for i, h in enumerate(hosts))
        return value + self.power_scale * network + self.latency_scale * delay, delay


class CachedRoutingValidator(EmbeddingValidator):
    """Reuse a segment only for identical endpoints, demand and tentative link reservations."""

    def __init__(self, config, stats):
        super().__init__(config)
        self.capture_feedback = True
        self.cache, self.stats = {}, stats
        self.snapshot = None

    def _path(self, substrate, trial, source, target, demand):
        snapshot = substrate, substrate.version
        if self.snapshot != snapshot:
            self.cache.clear()
            self.snapshot = snapshot
        key = source, target, demand, frozenset(trial.bandwidth.items())
        if key not in self.cache:
            self.stats['segment_searches'] += 1
            self.cache[key] = super()._path(substrate, trial, source, target, demand)
        else:
            self.stats['segment_cache_hits'] += 1
        return self.cache[key]
