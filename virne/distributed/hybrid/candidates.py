"""Bounded DC/candidate reduction; never inspect future arrivals or holding times."""

from dataclasses import dataclass
from math import prod

import networkx as nx

from virne.distributed.model import Allocation, EPSILON, edge_key


@dataclass
class Neighborhood:
    dc: str
    fixed: dict
    candidates: dict
    warm_start: dict
    original_candidates: int

    @property
    def assignment_count(self):
        return prod(len(hosts) for hosts in self.candidates.values())


def incremental_server_power(substrate, host, cpu):
    p = substrate.power_config
    dynamic = (p['server_peak_w'] - p['server_idle_w']) * cpu / substrate.graph.nodes[host]['cpu']
    return dynamic + (p['server_idle_w'] - p['server_off_w'] if substrate.cpu[host] <= EPSILON else 0)


def neighborhoods(substrate, request, incumbent, settings, budget):
    """Try DCs for each bounded window; freeze other VNFs with CPU/RAM first-fit.

    When no incumbent exists, a compute-only seed may be partial. Every unplaced
    VNF must be included in the window; otherwise that neighborhood is skipped.
    """
    graph = substrate.graph
    distance = nx.single_source_dijkstra_path_length(graph, request.ingress, weight='latency_ms')
    by_dc = {}
    for host in substrate.servers:
        by_dc.setdefault(graph.nodes[host]['dc'], []).append(host)
    dcs = sorted(by_dc, key=lambda dc: (min(distance[h] for h in by_dc[dc]), dc))
    if incumbent is not None:
        home = graph.nodes[next(iter(incumbent.placement.values()))]['dc']
        dcs.remove(home)
        dcs.insert(0, home)
    dcs = dcs[:settings['max_dcs']]
    seeds = {}
    reachability = {}

    def reachable(source, target, demand):
        key = (source, target, demand)
        if key not in reachability:
            budget.check()
            residual = nx.subgraph_view(
                graph, filter_node=lambda n: graph.nodes[n]['kind'] != 'server' or n in (source, target),
                filter_edge=lambda u, v: graph.edges[u, v]['capacity_mbps'] is None or (
                    substrate.bandwidth[edge_key(u, v)] + demand <= graph.edges[u, v]['capacity_mbps'] + EPSILON))
            reachability[key] = nx.has_path(residual, source, target)
        return reachability[key]
    for dc in dcs:
        budget.check()
        hosts = by_dc[dc]
        if incumbent is not None and all(graph.nodes[h]['dc'] == dc for h in incumbent.placement.values()):
            seeds[dc] = dict(incumbent.placement)
            continue
        trial = Allocation(request.id)
        for index in sorted(range(len(request.vnfs)), key=lambda i: (-request.vnfs[i].cpu, i)):
            feasible = [h for h in hosts if h not in trial.cpu and substrate.can_host(h, request.vnfs[index], trial)]
            if feasible:
                host = min(feasible, key=lambda h: (incremental_server_power(substrate, h, request.vnfs[index].cpu),
                                                   distance[h], h))
                trial.reserve_server(index, host, request.vnfs[index])
        seeds[dc] = trial.placement
    count = len(request.vnfs)
    width = min(count, settings['neighborhood_size'])
    # Stress from few feasible hosts and high boundary bandwidth; indices remain semantic.
    empty = Allocation(request.id)
    scarcity = {i: sum(substrate.can_host(h, v, empty) for dc in dcs for h in by_dc[dc])
                for i, v in enumerate(request.vnfs)}
    starts = sorted(range(count - width + 1), key=lambda start: (
        sum(scarcity[i] for i in range(start, start + width)),
        -sum(request.bandwidths[start:start + width]), start))
    emitted = set()
    for round_index in range(settings['max_neighborhoods']):
        for dc in dcs:
            budget.check()
            seed = seeds[dc]
            missing = set(range(count)) - seed.keys()
            if len(missing) > width:
                continue
            # Prioritize fixing unplaced nodes; expand around them where possible.
            start = starts[round_index % len(starts)]
            selected = set(range(start, start + width)) | missing
            while len(selected) > width:
                selected.remove(max(selected - missing, key=lambda i: (scarcity[i], i)))
            signature = (dc, tuple(sorted(selected)))
            if signature in emitted:
                continue
            emitted.add(signature)
            fixed = {i: h for i, h in seed.items() if i not in selected}
            if len(fixed) + len(selected) != count:
                continue
            fixed_trial = Allocation(request.id)
            for index, host in fixed.items():
                fixed_trial.reserve_server(index, host, request.vnfs[index])
            candidates = {}
            original_count = 0
            for index in sorted(selected):
                feasible = [h for h in by_dc[dc] if h not in fixed_trial.cpu
                            and substrate.can_host(h, request.vnfs[index], fixed_trial)]
                # Necessary pairwise bandwidth checks to ingress/fixed neighbors.
                # Full joint routing remains the validator's responsibility.
                boundaries = []
                if index == 0:
                    boundaries.append((request.ingress, request.bandwidths[0]))
                elif index - 1 in fixed:
                    boundaries.append((fixed[index - 1], request.bandwidths[index]))
                if index + 1 in fixed:
                    boundaries.append((fixed[index + 1], request.bandwidths[index + 1]))
                if index == count - 1 and request.return_to_ingress:
                    boundaries.append((request.ingress, request.bandwidths[-1]))
                feasible = [h for h in feasible if all(reachable(h, target, bw) for target, bw in boundaries)]
                original_count += len(feasible)
                feasible.sort(key=lambda h: (incremental_server_power(substrate, h, request.vnfs[index].cpu),
                                             distance[h], h))
                # Different offsets avoid giving every VNF exactly the same three hosts.
                best = feasible[:settings['candidates_per_vnf']]
                if feasible:
                    diverse = feasible[index % len(feasible)]
                    if diverse not in best:
                        best[-1] = diverse
                if index in seed and seed[index] in feasible and seed[index] not in best:
                    best[-1] = seed[index]
                candidates[index] = best
            if not all(candidates.values()):
                continue
            warm = {i: h for i, h in seed.items() if i in candidates and h in candidates[i]}
            yield Neighborhood(dc, fixed, candidates, warm, original_count)


def features(substrate, request, neighborhood, incumbent, settings):
    sets = [set(c) for c in neighborhood.candidates.values()]
    union = set().union(*sets)
    total = sum(len(c) for c in sets)
    bandwidth_headroom = []
    for edge, used in substrate.bandwidth.items():
        capacity = substrate.graph.edges[edge]['capacity_mbps']
        if capacity is not None:
            bandwidth_headroom.append(max(0, (capacity - used) / capacity))
    slack = 1.0
    if incumbent is not None and request.max_latency_ms is not None:
        slack = max(0, (request.max_latency_ms - incumbent.latency_ms) / request.max_latency_ms)
    return {
        'has_incumbent': float(incumbent is not None),
        'variables': total,
        'assignments': neighborhood.assignment_count,
        'candidate_overlap': 1 - len(union) / total,
        'cpu_utilization': substrate.snapshot()['cpu_utilization'],
        'bandwidth_headroom': min(bandwidth_headroom, default=1.0),
        'sla_slack': slack,
        'candidate_retention': total / neighborhood.original_candidates,
    }
