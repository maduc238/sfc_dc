"""Budget-aware rules and a frozen offline contextual action-value policy."""

import argparse
import hashlib
import json
import math
from pathlib import Path


FEATURE_NAMES = ['bias', 'has_incumbent', 'log_assignments', 'candidate_overlap',
                 'cpu_utilization', 'bandwidth_headroom', 'sla_slack', 'candidate_retention']
ACTIONS = ('keep', 'exact', 'sa', 'qaoa_sim', 'qa')


def vector(features):
    return [1.0, features['has_incumbent'], math.log1p(features['assignments']) / 10,
            features['candidate_overlap'], features['cpu_utilization'], features['bandwidth_headroom'],
            features['sla_slack'], features['candidate_retention']]


class SelectionPolicy:
    def __init__(self, settings):
        self.settings = settings
        self.model = None
        if settings['policy']['name'] == 'learned':
            self.model = json.loads(Path(settings['policy']['model_path']).read_text(encoding='utf-8'))
            if self.model.get('schema_version') != 1 or self.model.get('features') != FEATURE_NAMES:
                raise ValueError('Incompatible learned policy schema')
            weights = self.model.get('weights', {})
            if not weights or set(weights) - set(ACTIONS):
                raise ValueError('Learned policy has unknown or missing actions')
            for row in weights.values():
                if not isinstance(row, list) or len(row) != len(FEATURE_NAMES) or not all(
                    isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) for v in row
                ):
                    raise ValueError('Invalid learned policy coefficients')

    def select(self, features, remaining_ms, pool):
        p = self.settings['policy']
        if remaining_ms < p['minimum_remaining_ms']:
            return 'keep', 'insufficient_budget'
        if features['has_incumbent'] and not self.settings['optimize_existing']:
            return 'keep', 'incumbent_sufficient'
        if p['name'] == 'static':
            return p['static_backend'], 'static_control'
        if self.model is not None:
            values = {'keep': 0.0}
            x = vector(features)
            for action, weights in self.model['weights'].items():
                if action == 'keep' or pool.available(action, features['variables']):
                    values[action] = sum(a * b for a, b in zip(weights, x))
            return max(values, key=values.get), 'offline_learned_value'
        exact_limit = min(p['exact_max_assignments'], self.settings['exact']['max_assignments'])
        if (features['assignments'] <= exact_limit
                and features['assignments'] * p['exact_ms_per_assignment'] < remaining_ms):
            return 'exact', 'small_enumerable_neighborhood'
        quantum = p['quantum_backend']
        if (pool.available(quantum, features['variables'])
                and p['quantum_expected_gain'] > p['quantum_minimum_gain']
                and p['quantum_expected_ms'] < remaining_ms):
            return quantum, 'configured_gain_and_latency_gate'
        return 'sa', 'classical_search_within_budget'


def fit_policy(rows, ridge=1.0):
    """Fit per-action regularized values offline; never update on test traces.

    This is the exploitation component of a contextual bandit. Observational
    logs do not establish unbiased counterfactual effects or quantum advantage.
    Prefer balanced/counterfactual collection on training instances.
    """
    import numpy as np

    if ridge <= 0 or not math.isfinite(ridge):
        raise ValueError('ridge must be finite and positive')
    weights, counts = {}, {}
    for action in ACTIONS:
        selected = [row for row in rows if row.get('backend') == action and 'reward' in row and 'features' in row]
        if len(selected) < 2:
            continue
        x = np.asarray([vector(row['features']) for row in selected], dtype=float)
        y = np.asarray([row['reward'] for row in selected], dtype=float)
        if not np.isfinite(x).all() or not np.isfinite(y).all():
            raise ValueError('Training rows contain non-finite features or rewards')
        weights[action] = np.linalg.solve(x.T @ x + ridge * np.eye(x.shape[1]), x.T @ y).tolist()
        counts[action] = len(selected)
    if not weights:
        raise ValueError('Need at least two valid training observations for an action')
    return {'schema_version': 1, 'features': FEATURE_NAMES, 'weights': weights,
            'training_counts': counts, 'ridge': ridge,
            'method': 'offline_ridge_action_value', 'updates_during_evaluation': False}


def main():
    parser = argparse.ArgumentParser(description='Fit a frozen contextual policy using TRAINING logs only.')
    parser.add_argument('--training-log', required=True, nargs='+', type=Path, help='backend_calls.json from TRAINING runs')
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--ridge', type=float, default=1.0)
    args = parser.parse_args()
    try:
        digest = hashlib.sha256()
        rows = []
        for path in args.training_log:
            content = path.read_bytes()
            digest.update(content)
            batch = json.loads(content)
            if not isinstance(batch, list):
                raise ValueError('Training log must be a JSON list')
            rows.extend(batch)
        model = fit_policy(rows, args.ridge)
        model['training_sha256'] = digest.hexdigest()
        model['training_log_count'] = len(args.training_log)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(model, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    except (OSError, ValueError, KeyError, ImportError) as error:
        parser.error(str(error))
    print(f'Frozen policy: {args.output}')


if __name__ == '__main__':
    main()
