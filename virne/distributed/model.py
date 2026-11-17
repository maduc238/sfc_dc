"""Physical topology, immutable SFC demands and transactional resource accounting."""

from dataclasses import asdict, dataclass, field
from copy import copy
from itertools import pairwise
from math import ceil
import random

import networkx as nx


EPSILON = 1e-8


@dataclass(frozen=True)
class VNF:
    cpu: float
    memory_gb: float


@dataclass(frozen=True)
class Request:
    id: int
    arrival: float  # seconds
    lifetime: float
    ingress: str
    vnfs: tuple[VNF, ...]
    bandwidths: tuple[float, ...]  # ingress->VNF0, chain links, optional return link
    max_latency_ms: float | None
    processing_delay_ms: float
    return_to_ingress: bool = False

    def to_dict(self):
        return asdict(self)


def generate_requests(config):
    """A separate RNG stream ensures solvers and topologies see identical offered traffic."""
    rng = random.Random(config['experiment']['seed'])
    w = config['workload']
    now = 0.0
    requests = []
    for i in range(w['num_requests']):
        now += rng.expovariate(w['arrival_rate_per_hour'] / 3600.0)
        lifetime = rng.expovariate(1.0 / (w['mean_lifetime_hours'] * 3600.0))
        ingress = f"wan/{rng.choice(w['ingress_nodes'])}"
        count = rng.randint(*w['vnfs_per_request'])
        vnfs = tuple(VNF(rng.uniform(*w['cpu']), rng.uniform(*w['memory_gb'])) for _ in range(count))
        bandwidths = tuple(rng.uniform(*w['bandwidth_mbps']) for _ in range(
            count + int(w['return_to_ingress'])))
        latency = rng.uniform(*w['max_latency_ms']) if w['max_latency_ms'] is not None else None
        requests.append(Request(i, now, lifetime, ingress, vnfs, bandwidths, latency,
                                w['processing_delay_ms'], w['return_to_ingress']))
    return requests


def edge_key(u, v):
    return tuple(sorted((u, v)))


def build_topology(config):
    """WAN routers -> DC gateways -> leaf/spine or full k-ary fat tree -> servers."""
    t = config['topology']
    wan, fabric = t['wan'], t['fabric']
    graph = nx.Graph()

    def switch(name, dc=None, tier='wan', **attrs):
        graph.add_node(name, kind='switch', dc=dc, tier=tier, **attrs)
        return name

    def link(u, v, kind='fabric'):
        settings = wan if kind == 'wan' else fabric
        graph.add_edge(u, v, kind=kind, capacity_mbps=settings['bandwidth_mbps'],
                       latency_ms=settings['latency_ms'])

    for i in range(wan['num_nodes']):
        switch(f'wan/{i}')
    for u, v in wan['edges']:
        link(f'wan/{u}', f'wan/{v}', 'wan')
    if not nx.is_connected(graph):
        raise ValueError('The WAN must be connected (isolated ingress nodes cannot be served)')

    for dc in t['data_centers']:
        name = dc['id']
        root = f'dc/{name}'
        gateway = switch(f'{root}/gateway', name, 'gateway')
        link(f"wan/{dc['wan_node']}", gateway, 'access')
        cpu, memory = (dc.get(key, t['server'][key]) for key in ('cpu', 'memory_gb'))

        def server(index, leaf, pod):
            host = f'{root}/server/{index}'
            graph.add_node(host, kind='server', dc=name, rack=leaf, pod=pod,
                           cpu=cpu, memory_gb=memory)
            link(host, leaf)

        if fabric['type'] == 'leaf_spine':
            spines = [switch(f'{root}/spine/{i}', name, 'spine') for i in range(fabric['num_spines'])]
            for spine in spines:
                link(gateway, spine)
            per_rack = fabric['servers_per_rack']
            for rack in range(ceil(dc['num_servers'] / per_rack)):
                leaf = switch(f'{root}/leaf/{rack}', name, 'leaf')
                for spine in spines:
                    link(leaf, spine)
                for index in range(rack * per_rack, min((rack + 1) * per_rack, dc['num_servers'])):
                    server(index, leaf, f'{root}/fabric')
        else:
            k = fabric['fat_tree_k']
            half = k // 2
            cores = [[switch(f'{root}/core/{a}/{b}', name, 'core') for b in range(half)]
                     for a in range(half)]
            for row in cores:
                for core in row:
                    link(gateway, core)
            index = 0
            for pod in range(k):
                aggregates = [switch(f'{root}/pod/{pod}/agg/{a}', name, 'aggregation')
                              for a in range(half)]
                for a, aggregate in enumerate(aggregates):
                    for core in cores[a]:
                        link(aggregate, core)
                for leaf_id in range(half):
                    leaf = switch(f'{root}/pod/{pod}/leaf/{leaf_id}', name, 'leaf')
                    for aggregate in aggregates:
                        link(leaf, aggregate)
                    for _ in range(half):
                        server(index, leaf, f'{root}/pod/{pod}')
                        index += 1
    return graph


@dataclass
class Allocation:
    request_id: int
    placement: dict[int, str] = field(default_factory=dict)
    cpu: dict[str, float] = field(default_factory=dict)
    memory: dict[str, float] = field(default_factory=dict)
    bandwidth: dict[tuple[str, str], float] = field(default_factory=dict)
    paths: dict[int, list[str]] = field(default_factory=dict)
    latency_ms: float = 0.0
    inter_dc_virtual_links: int = 0

    def reserve_server(self, index, host, vnf):
        self.placement[index] = host
        self.cpu[host] = self.cpu.get(host, 0.0) + vnf.cpu
        self.memory[host] = self.memory.get(host, 0.0) + vnf.memory_gb

    def reserve_path(self, index, path, bandwidth):
        self.paths[index] = path
        for u, v in pairwise(path):
            edge = edge_key(u, v)
            self.bandwidth[edge] = self.bandwidth.get(edge, 0.0) + bandwidth


class Substrate:
    """All trial reservations live in Allocation; rejection never mutates live state."""

    def __init__(self, config):
        self.graph = build_topology(config)
        self.power_config = config['power']
        self.servers = [n for n, data in self.graph.nodes(data=True) if data['kind'] == 'server']
        self.cpu = dict.fromkeys(self.servers, 0.0)
        self.memory = dict.fromkeys(self.servers, 0.0)
        self.bandwidth = dict.fromkeys((edge_key(*e) for e in self.graph.edges), 0.0)
        self.active = {}
        self.reservations = {}
        self.paused_cpu = dict.fromkeys(self.servers, 0.0)
        self.version = 0

    def can_host(self, host, vnf, trial):
        attrs = self.graph.nodes[host]
        return (self.cpu[host] + trial.cpu.get(host, 0) + vnf.cpu <= attrs['cpu'] + EPSILON
                and self.memory[host] + trial.memory.get(host, 0) + vnf.memory_gb
                <= attrs['memory_gb'] + EPSILON)

    def commit(self, allocation):
        if allocation.request_id in self.active:
            raise ValueError('Request already committed')
        # Validate the whole transaction before touching any live resource.
        for host, amount in allocation.cpu.items():
            if self.cpu[host] + amount > self.graph.nodes[host]['cpu'] + EPSILON:
                raise ValueError('CPU capacity exceeded')
        for host, amount in allocation.memory.items():
            if self.memory[host] + amount > self.graph.nodes[host]['memory_gb'] + EPSILON:
                raise ValueError('Memory capacity exceeded')
        for edge, amount in allocation.bandwidth.items():
            capacity = self.graph.edges[edge]['capacity_mbps']
            if capacity is not None and self.bandwidth[edge] + amount > capacity + EPSILON:
                raise ValueError('Link capacity exceeded')
        for used, reserved in ((self.cpu, allocation.cpu), (self.memory, allocation.memory),
                               (self.bandwidth, allocation.bandwidth)):
            for key, amount in reserved.items():
                used[key] += amount
        self.active[allocation.request_id] = allocation
        self.version += 1

    def release(self, request_id):
        allocation = self.active.pop(request_id)
        self.version += 1
        for used, reserved in ((self.cpu, allocation.cpu), (self.memory, allocation.memory),
                               (self.bandwidth, allocation.bandwidth)):
            for key, amount in reserved.items():
                used[key] -= amount
                if abs(used[key]) < EPSILON:
                    used[key] = 0.0

    def fork(self):
        """Cheap isolated ledger view; topology and immutable allocations are shared."""
        view = copy(self)
        for key in ('cpu', 'memory', 'bandwidth', 'active', 'reservations', 'paused_cpu'):
            setattr(view, key, dict(getattr(self, key)))
        return view

    def without_request(self, request_id):
        view = self.fork()
        view.release(request_id)
        return view

    def reserve_temporary(self, key, allocation, standby_cpu, copy_cpu, migration_bandwidth):
        """A reservation consumes capacity without creating a second service request."""
        if key in self.reservations:
            raise ValueError('Reservation already exists')
        view = self.fork()
        # Reuse the transactional capacity checks without adding to active services.
        temporary_id = -1
        if temporary_id in view.active:
            raise ValueError('Negative request IDs are reserved for internal transactions')
        transaction = copy(allocation)
        transaction.request_id = temporary_id
        view.commit(transaction)
        for used, extra in ((self.cpu, allocation.cpu), (self.memory, allocation.memory),
                            (self.bandwidth, allocation.bandwidth)):
            for resource, amount in extra.items():
                used[resource] += amount
        self.reservations[key] = {'allocation': allocation, 'standby_cpu': dict(standby_cpu),
                                  'copy_cpu': dict(copy_cpu), 'migration_bandwidth': dict(migration_bandwidth)}
        self.version += 1

    def release_temporary(self, key):
        allocation = self.reservations.pop(key)['allocation']
        for used, extra in ((self.cpu, allocation.cpu), (self.memory, allocation.memory),
                            (self.bandwidth, allocation.bandwidth)):
            for resource, amount in extra.items():
                used[resource] -= amount
                if abs(used[resource]) < EPSILON:
                    used[resource] = 0.0
        self.version += 1

    def adopt(self, view):
        """Publish a fully checked ledger replacement as one live transaction."""
        for key in ('cpu', 'memory', 'bandwidth', 'active', 'reservations', 'paused_cpu'):
            setattr(self, key, getattr(view, key))
        self.version += 1

    def switch_is_active(self, node, trial=None):
        if self.graph.nodes[node]['tier'] == 'wan' and self.power_config['wan_always_on']:
            return True
        extra = trial.bandwidth if trial is not None else {}
        return any(self.bandwidth[edge_key(node, other)] + extra.get(edge_key(node, other), 0)
                   > EPSILON for other in self.graph.neighbors(node))

    def snapshot(self):
        p = self.power_config
        standby = {h: sum(r['standby_cpu'].get(h, 0) for r in self.reservations.values())
                   for h in self.servers}
        server_w = sum(p['server_idle_w'] + (p['server_peak_w'] - p['server_idle_w'])
                       * max(0.0, self.cpu[n] - standby[n] - self.paused_cpu[n]) / self.graph.nodes[n]['cpu']
                       if self.cpu[n] > EPSILON or self.memory[n] > EPSILON
                       else p['server_off_w'] for n in self.servers)
        switch_w = 0.0
        active_switches = 0
        for node, attrs in self.graph.nodes(data=True):
            if attrs['kind'] != 'switch':
                continue
            active = self.switch_is_active(node)
            active_switches += int(active)
            ports = sum(self.bandwidth[edge_key(node, v)] > EPSILON for v in self.graph.neighbors(node))
            switch_w += p['switch_idle_w'] + ports * p['switch_port_w'] if active else p['switch_off_w']
        cpu_capacity = sum(self.graph.nodes[n]['cpu'] for n in self.servers)
        mem_capacity = sum(self.graph.nodes[n]['memory_gb'] for n in self.servers)
        wan_edges = [e for e in self.bandwidth if self.graph.edges[e]['kind'] == 'wan']
        wan_capacity = sum(self.graph.edges[e]['capacity_mbps'] or 0 for e in wan_edges)
        finite_wan = bool(wan_edges) and all(self.graph.edges[e]['capacity_mbps'] is not None for e in wan_edges)
        return {
            'active_requests': len(self.active),
            'active_servers': sum(self.cpu[h] > EPSILON or self.memory[h] > EPSILON for h in self.servers),
            'active_switches': active_switches,
            'cpu_utilization': sum(self.cpu.values()) / cpu_capacity,
            'memory_utilization': sum(self.memory.values()) / mem_capacity,
            'wan_bandwidth_mbps_hops': sum(self.bandwidth[e] for e in wan_edges),
            'wan_utilization': sum(self.bandwidth[e] for e in wan_edges) / wan_capacity if finite_wan else None,
            'server_power_w': server_w, 'network_power_w': switch_w, 'power_w': server_w + switch_w,
            'active_migrations': len(self.reservations),
            'migration_reserved_cpu': sum(standby.values()),
            'migration_copy_cpu': sum(sum(r['copy_cpu'].values()) for r in self.reservations.values()),
            'migration_reserved_memory_gb': sum(sum(r['allocation'].memory.values()) for r in self.reservations.values()),
            'migration_bandwidth_mbps_hops': sum(sum(r['migration_bandwidth'].values()) for r in self.reservations.values()),
        }
