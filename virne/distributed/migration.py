"""Serial, single-VNF consolidation with bounded pre-copy and explicit reservations.

All planning is classical CPU work. Migration is an event-time model, not a VM
implementation: images are ready, state is uncompressed, and bandwidth is fixed
after reservation. No future arrivals or request holding times enter planning.
"""

from collections import Counter
from dataclasses import dataclass
from itertools import pairwise
import random
import time

import networkx as nx

from virne.distributed.model import Allocation, EPSILON, edge_key
from virne.distributed.validation import EmbeddingValidator


class MigrationRejected(ValueError):
    pass


@dataclass(frozen=True)
class MigrationProfile:
    state_mb: float
    dirty_rate_mbps: float


def generate_profiles(config, requests):
    rng = random.Random(config['experiment']['seed'] + 2_000_033)
    profile = config['migration']['profile']
    return {(r.id, v): MigrationProfile(state := rng.uniform(*profile['state_mb']),
                                      rng.uniform(*profile['dirty_rate_mbps']) if state > 0 else 0.0)
            for r in requests for v in range(len(r.vnfs))}


@dataclass(frozen=True)
class Transfer:
    rounds: tuple
    remaining_mbit: float
    precopy_s: float
    downtime_s: float
    total_s: float
    transferred_mbit: float
    final_copy_s: float


def estimate_precopy(profile, bandwidth, latency_s, settings):
    """MB is decimal; bandwidth and dirty rate are Mbit/s. Abort before downtime."""
    remaining = initial = profile.state_mb * 8.0
    if initial > 0 and bandwidth <= EPSILON:
        raise MigrationRejected('copy_bandwidth')
    rounds = []
    elapsed = 0.0
    while True:
        final_copy = remaining / bandwidth + latency_s if remaining > EPSILON else 0.0
        downtime = final_copy + settings['restore_s'] + settings['route_update_s']
        if downtime <= settings['max_downtime_s'] + EPSILON:
            break
        if len(rounds) >= settings['max_precopy_rounds']:
            raise MigrationRejected('non_convergent')
        duration = remaining / bandwidth + latency_s
        rounds.append((remaining, duration))
        elapsed += duration
        remaining = min(initial, profile.dirty_rate_mbps * duration)
        if settings['setup_s'] + elapsed + settings['cleanup_s'] > settings['timeout_s']:
            raise MigrationRejected('timeout')
    total = settings['setup_s'] + elapsed + downtime + settings['cleanup_s']
    if total > settings['timeout_s'] + EPSILON:
        raise MigrationRejected('timeout')
    return Transfer(tuple(rounds), remaining, elapsed, downtime, total,
                    sum(volume for volume, _ in rounds) + remaining, final_copy)


def route_changes(old, new):
    def edges(allocation):
        return {(segment, u, v) for segment, path in allocation.paths.items() for u, v in pairwise(path)}
    return len(edges(old) ^ edges(new))


@dataclass
class MigrationPlan:
    request: object
    vnf_index: int
    old: Allocation
    new: Allocation
    source: str
    target: str
    profile: MigrationProfile
    path: list
    bandwidth_mbps: float
    transfer: Transfer
    reservation: Allocation
    after: Allocation
    standby_cpu: dict
    copy_cpu: dict
    copy_bandwidth: dict
    cost: float
    changes: int
    gain: float = 0.0
    energy_saving_j: float = 0.0


def after_cutover(substrate, plan, key):
    """Build the complete replacement first; caller publishes only on success."""
    view = substrate.fork()
    view.release_temporary(key)
    view.paused_cpu[plan.source] = max(0.0, view.paused_cpu[plan.source] - plan.request.vnfs[plan.vnf_index].cpu)
    view.release(plan.request.id)
    view.commit(plan.new)
    view.reserve_temporary(key, plan.after, {}, plan.copy_cpu, {})
    return view


def score_plan(substrate, plan, settings):
    objective = settings['objective']
    horizon = objective['horizon_s']
    if plan.transfer.total_s > horizon:
        raise MigrationRejected('forecast_horizon')
    view = substrate.fork()
    view.reserve_temporary('forecast', plan.reservation, plan.standby_cpu,
                           plan.copy_cpu, plan.copy_bandwidth)
    preparing_power = view.snapshot()['power_w']
    view.paused_cpu[plan.source] += plan.request.vnfs[plan.vnf_index].cpu
    stopped_power = view.snapshot()['power_w']
    view = after_cutover(view, plan, 'forecast')
    cleanup_power = view.snapshot()['power_w']
    view.release_temporary('forecast')
    new_power = view.snapshot()['power_w']
    predicted_energy = (preparing_power * (settings['setup_s'] + plan.transfer.precopy_s)
                        + stopped_power * plan.transfer.downtime_s
                        + cleanup_power * settings['cleanup_s']
                        + new_power * (horizon - plan.transfer.total_s))
    plan.energy_saving_j = substrate.snapshot()['power_w'] * horizon - predicted_energy
    plan.gain = (objective['energy_weight'] * plan.energy_saving_j / objective['energy_reference_j']
                 + objective['latency_weight'] * (plan.old.latency_ms - plan.new.latency_ms)
                 / objective['latency_reference_ms'] - objective['migration_weight'] * plan.cost)
    return plan.gain


class MigrationPlanner:
    def __init__(self, config, profiles):
        self.settings = config['migration']
        self.profiles = profiles
        self.audit = EmbeddingValidator(config)
        self.last_search = {}

    def make_plan(self, substrate, request, index, target):
        old = substrate.active[request.id]
        source = old.placement[index]
        graph = substrate.graph
        if (source == target or target in old.placement.values()
                or graph.nodes[source]['dc'] != graph.nodes[target]['dc']):
            raise MigrationRejected('placement_scope')
        placement = {**old.placement, index: target}
        new, reason = self.audit.evaluate(substrate.without_request(request.id), request, placement)
        if new is None:
            raise MigrationRejected(reason)
        hold = {e: max(0.0, amount - old.bandwidth.get(e, 0.0)) for e, amount in new.bandwidth.items()}
        profile = self.profiles[request.id, index]
        path, bandwidth, latency = [], 0.0, 0.0
        if profile.state_mb > 0:
            dc = graph.nodes[source]['dc']

            def available(u, v):
                edge = edge_key(u, v)
                capacity = graph.edges[edge]['capacity_mbps']
                return capacity is None or capacity - substrate.bandwidth[edge] - hold.get(edge, 0) > EPSILON

            residual = nx.subgraph_view(
                graph, filter_node=lambda n: graph.nodes[n].get('dc') == dc and (
                    graph.nodes[n]['kind'] != 'server' or n in (source, target)), filter_edge=available)
            try:
                path = nx.shortest_path(residual, source, target, weight='latency_ms')
            except nx.NetworkXNoPath:
                raise MigrationRejected('copy_bandwidth') from None
            bandwidth = self.settings['bandwidth_mbps']
            for u, v in pairwise(path):
                edge = edge_key(u, v)
                capacity = graph.edges[edge]['capacity_mbps']
                if capacity is not None:
                    bandwidth = min(bandwidth, capacity - substrate.bandwidth[edge] - hold.get(edge, 0))
                latency += graph.edges[edge]['latency_ms'] / 1000.0
        transfer = estimate_precopy(profile, bandwidth, latency, self.settings)
        vnf = request.vnfs[index]
        copy_cpu = {source: self.settings['copy_cpu'], target: self.settings['copy_cpu']}
        standby_cpu = {target: vnf.cpu}
        copy_bandwidth = {edge_key(u, v): bandwidth for u, v in pairwise(path)}
        reservation = Allocation(request.id)
        reservation.cpu = {h: amount + standby_cpu.get(h, 0.0) for h, amount in copy_cpu.items()}
        buffer_gb = self.settings['copy_buffer_mb'] / 1000.0
        reservation.memory = {source: buffer_gb, target: vnf.memory_gb + buffer_gb}
        reservation.bandwidth = {e: amount + copy_bandwidth.get(e, 0.0) for e, amount in hold.items()}
        for e, amount in copy_bandwidth.items():
            if e not in reservation.bandwidth:
                reservation.bandwidth[e] = amount
        after = Allocation(request.id)
        after.cpu = dict(copy_cpu)
        after.memory = {source: buffer_gb, target: buffer_gb}
        objective = self.settings['objective']
        changes = route_changes(old, new)
        cost = (transfer.transferred_mbit * max(0, len(path) - 1) / objective['transfer_reference_mbit_hops']
                + transfer.downtime_s / objective['downtime_reference_s']
                + changes / objective['route_reference_changes'])
        plan = MigrationPlan(request, index, old, new, source, target, profile, path,
                             bandwidth, transfer, reservation, after, standby_cpu, copy_cpu,
                             copy_bandwidth, cost, changes)
        try:
            score_plan(substrate, plan, self.settings)
        except ValueError as error:
            if isinstance(error, MigrationRejected):
                raise
            raise MigrationRejected('temporary_capacity') from error
        return plan

    def choose(self, substrate, lookup, now, last_completed, downtime, budget):
        started = time.perf_counter()
        rejected = Counter()
        candidates = 0
        best = None
        self.audit.budget = budget
        tasks = [(substrate.cpu[host] / substrate.graph.nodes[host]['cpu'], rid, v)
                 for rid, old in substrate.active.items() for v, host in old.placement.items()
                 if now - last_completed.get(rid, -float('inf')) >= self.settings['cooldown_s']]
        try:
            for _, rid, index in sorted(tasks):
                budget.check()
                old = substrate.active[rid]
                source = old.placement[index]
                targets = [h for h in substrate.servers if h not in old.placement.values()
                           and substrate.cpu[h] > EPSILON
                           and substrate.graph.nodes[h]['dc'] == substrate.graph.nodes[source]['dc']]
                targets.sort(key=lambda h: (-substrate.cpu[h] / substrate.graph.nodes[h]['cpu'], h))
                for target in targets:
                    budget.check()
                    if candidates >= self.settings['max_candidates']:
                        return best
                    candidates += 1
                    try:
                        plan = self.make_plan(substrate, lookup[rid], index, target)
                    except MigrationRejected as error:
                        rejected[str(error)] += 1
                        continue
                    if downtime.get(rid, 0) + plan.transfer.downtime_s > self.settings['downtime_budget_s'] + EPSILON:
                        rejected['downtime_budget'] += 1
                    elif plan.gain <= self.settings['objective']['minimum_improvement']:
                        rejected['no_improvement'] += 1
                    elif best is None or plan.gain > best.gain:
                        best = plan
        except TimeoutError:
            rejected['planning_budget'] += 1
        finally:
            self.audit.budget = None
            self.last_search = {'candidates': candidates, 'rejected_candidates': dict(rejected),
                                'wall_time_ms': (time.perf_counter() - started) * 1000}
        return best


class MigrationManager:
    """One plan at a time: prepare -> copy -> stop -> atomic cutover -> cleanup."""

    def __init__(self, config, substrate, profiles):
        self.settings = config['migration']
        self.substrate = substrate
        self.planner = MigrationPlanner(config, profiles)
        self.current = None
        self.records = []
        self.last_completed = {}
        self.downtime = Counter()
        self.outages = []
        self._serial = 0

    def start(self, plan, now):
        if self.current is not None:
            return False, 'busy'
        old = self.substrate.active.get(plan.request.id)
        if old != plan.old:
            return False, 'stale'
        if now - self.last_completed.get(plan.request.id, -float('inf')) < self.settings['cooldown_s']:
            return False, 'cooldown'
        if self.downtime[plan.request.id] + plan.transfer.downtime_s > self.settings['downtime_budget_s'] + EPSILON:
            return False, 'downtime_budget'
        try:
            # Re-score this fixed plan only; do not re-route or search at completion.
            score_plan(self.substrate, plan, self.settings)
            if plan.gain <= self.settings['objective']['minimum_improvement']:
                return False, 'no_improvement'
            self._serial += 1
            key = self._serial
            self.substrate.reserve_temporary(key, plan.reservation, plan.standby_cpu,
                                             plan.copy_cpu, plan.copy_bandwidth)
        except ValueError:
            return False, 'stale_capacity'
        stop = now + self.settings['setup_s'] + plan.transfer.precopy_s
        cutover = stop + plan.transfer.downtime_s
        record = {'migration_id': key, 'request_id': plan.request.id, 'vnf_index': plan.vnf_index,
                  'source': plan.source, 'target': plan.target, 'copy_path': plan.path,
                  'old_placement': dict(plan.old.placement), 'new_placement': dict(plan.new.placement),
                  'old_paths': plan.old.paths, 'new_paths': plan.new.paths,
                  'state_mb': plan.profile.state_mb, 'dirty_rate_mbps': plan.profile.dirty_rate_mbps,
                  'bandwidth_mbps': plan.bandwidth_mbps, 'precopy_rounds': len(plan.transfer.rounds),
                  'start_s': now, 'stop_s': None, 'cutover_s': None, 'end_s': None,
                  'predicted_total_s': plan.transfer.total_s, 'predicted_downtime_s': plan.transfer.downtime_s,
                  'predicted_transferred_mbit': plan.transfer.transferred_mbit,
                  'normalized_cost': plan.cost, 'predicted_gain': plan.gain,
                  'predicted_energy_saving_j': plan.energy_saving_j,
                  'planned_route_changes': plan.changes, 'applied_route_changes': 0,
                  'transferred_mbit': 0.0, 'transferred_mbit_hops': 0.0, 'downtime_s': 0.0,
                  'committed': False, 'status': 'preparing', 'reason': '',
                  'rounds': [{'volume_mbit': v, 'duration_s': t} for v, t in plan.transfer.rounds]}
        self.records.append(record)
        self.current = {'key': key, 'plan': plan, 'record': record, 'phase': 'preparing',
                        'next_s': now + self.settings['setup_s'], 'stop_s': stop,
                        'cutover_s': cutover, 'end_s': cutover + self.settings['cleanup_s']}
        return True, None

    def next_event(self):
        if self.current is None:
            return None
        return self.current['next_s'], self.current['key']

    def _close_downtime(self, now):
        record = self.current['record']
        if record['stop_s'] is not None and record['cutover_s'] is None:
            duration = max(0.0, now - record['stop_s'])
            record['downtime_s'] = duration
            self.downtime[record['request_id']] += duration
            self.outages.append({'request_id': record['request_id'], 'start_s': record['stop_s'], 'end_s': now})

    def _finish(self, now, reason=''):
        current = self.current
        plan, record = current['plan'], current['record']
        position = record['start_s'] + self.settings['setup_s']
        transferred = 0.0
        for volume, duration in (*plan.transfer.rounds, (plan.transfer.remaining_mbit, plan.transfer.final_copy_s)):
            transferred += min(volume, max(0.0, now - position) * plan.bandwidth_mbps)
            position += duration
        record.update(end_s=now, transferred_mbit=transferred,
                      transferred_mbit_hops=transferred * max(0, len(plan.path) - 1),
                      status='completed' if record['committed'] else 'aborted', reason=reason)
        objective = self.settings['objective']
        record['actual_normalized_cost'] = (
            record['transferred_mbit_hops'] / objective['transfer_reference_mbit_hops']
            + record['downtime_s'] / objective['downtime_reference_s']
            + record['applied_route_changes'] / objective['route_reference_changes'])
        self.substrate.release_temporary(current['key'])
        self.last_completed[plan.request.id] = now
        self.current = None

    def cancel_departure(self, request_id, now):
        if self.current is None or self.current['plan'].request.id != request_id:
            return
        record = self.current['record']
        self._close_downtime(now)
        if not record['committed'] and record['stop_s'] is not None:
            source = self.current['plan'].source
            self.substrate.paused_cpu[source] = max(0.0, self.substrate.paused_cpu[source]
                                                    - self.current['plan'].request.vnfs[self.current['plan'].vnf_index].cpu)
        self._finish(now, 'departure')

    def advance(self, key, now):
        """Ignore a stale phase event after departure/cancellation or a newer plan."""
        if self.current is None or self.current['key'] != key:
            return None
        current = self.current
        plan, record = current['plan'], current['record']
        phase = current['phase']
        if phase == 'preparing':
            current.update(phase='copying', next_s=current['stop_s'])
            record['status'] = 'copying'
        elif phase == 'copying':
            self.substrate.paused_cpu[plan.source] += plan.request.vnfs[plan.vnf_index].cpu
            self.substrate.version += 1
            record.update(status='stopping', stop_s=now)
            current.update(phase='stopping', next_s=current['cutover_s'])
        elif phase == 'stopping':
            # Reservations guarantee capacity; no topology failures are injected in v1.
            view = after_cutover(self.substrate, plan, key)
            self._close_downtime(now)
            self.substrate.adopt(view)
            record.update(status='cleanup', committed=True, cutover_s=now, applied_route_changes=plan.changes)
            current.update(phase='cleanup', next_s=current['end_s'])
            return plan
        else:
            self._finish(now)
        return None
