**Virne** is a simulator and benchmark for **resource allocation (RA) in Network Functions Virtualisation (NFV)**, with unified support for traditional and **reinforcement learning (RL)**-based algorithms.

> In the literature, RA in NFV is often termed Virtual Network Embedding (VNE), Virtual Network Function (VNF) placement, service function chain (SFC) deployment, or network slicing in 5G.

Virne offers a unified and comprehensive framework for NFV-RA, with the following key features:

* 1️⃣ **Highly Customizable Simulations**: Simulates diverse network environments (e.g., cloud, edge, 5G), with user-defined topologies, resources, and service requirements.
* 2️⃣ **Extensive Algorithm Suite**: Registers exact, heuristic, meta-heuristic, and learning-based solvers behind a common interface.
* 3️⃣ **Reinforcement Learning Support**: Provides standardized RL pipelines and Gymnasium-compatible environments for rapid development and benchmarking of RL-based solutions.
* 4️⃣ **In-depth Evaluation Aspects**: Enables insightful analysis beyond effectiveness, covering multiple practicality perspectives (e.g., solvability, generalization, and scalability).

> [!IMPORTANT]
> 🎉 The [Virne benchmark paper](https://arxiv.org/abs/2507.19234) has been accepted at ICLR 2026. Welcome to check it out!
>
> ✨ If you have any questions, please open a new issue or contact me via email (wtfly2018@gmail.com)

![Virne architecture: simulation, solver, environment, and evaluation components](https://raw.githubusercontent.com/GeminiLight/virne/main/resources/figures/virne-architecture.png)

#### Benchmark Paper

**[ICLR, 2026] Virne** ([paper](https://arxiv.org/abs/2507.19234))

```bibtex
@inproceedings{tfwang-2026-virne,
  title={Virne: A Comprehensive Benchmark for RL-based Network Resource Allocation in NFV},
  author={Wang, Tianfu and Deng, Liwei and Chen, Xi and Wang, Junyang and He, Huiguo and Hu, Zhengyu and Wu, Wei and Ding, Leilei and Fan, Qilin and Xiong, Hui},
  booktitle={The Fourteenth International Conference on Learning Representations},
  year={2026},
}
```

#### Algorithmic Papers

**[IJCAI, 2024] FlagVNE** ([paper](https://arxiv.org/pdf/2404.12633) & [code](https://github.com/GeminiLight/flag-vne))

```bibtex
@INPROCEEDINGS{ijcai-2024-flagvne,
  title={FlagVNE: A Flexible and Generalizable Reinforcement Learning Framework for Network Resource Allocation},
  author={Wang, Tianfu and Fan, Qilin and Wang, Chao and Ding, Leilei and Yuan, Nicholas Jing and Xiong, Hui},
  booktitle={Proceedings of the 33rd International Joint Conference on Artificial Intelligence},
  year={2024},
}
```

**[TSC, 2023] HRL-ACRA** ([paper](https://arxiv.org/pdf/2406.17334) & [code](https://github.com/GeminiLight/hrl-acra))

```bibtex
@ARTICLE{tsc-2023-hrl-acra,
  author={Wang, Tianfu and Shen, Li and Fan, Qilin and Xu, Tong and Liu, Tongliang and Xiong, Hui},
  journal={IEEE Transactions on Services Computing},
  title={Joint Admission Control and Resource Allocation of Virtual Network Embedding Via Hierarchical Deep Reinforcement Learning},
  volume={17},
  number={03},
  pages={1001--1015},
  year={2024},
}
```

**[ICC, 2021] DRL-SFCP** ([paper](https://ieeexplore.ieee.org/document/9500964) & [code](https://github.com/GeminiLight/drl-sfcp))

```bibtex
@INPROCEEDINGS{icc-2021-drl-sfcp,
  author={Wang, Tianfu and Fan, Qilin and Li, Xiuhua and Zhang, Xu and Xiong, Qingyu and Fu, Shu and Gao, Min},
  booktitle={ICC 2021 - IEEE International Conference on Communications}, 
  title={DRL-SFCP: Adaptive Service Function Chains Placement with Deep Reinforcement Learning}, 
  year={2021},
  pages={1-6},
}
```

## Run Distributed Data Center Experiments from Source

This checkout includes an online SFC simulator with a WAN, multiple data centers,
and explicit switches and servers in each data center. Run a standalone baseline
YAML directly with `main.py`; installing the `virne` package is unnecessary.

```bash
python -m pip install -r requirements-simulation.txt
python main.py --config configs/baselines/distributed_hre.yaml
```

The DC simulator requires only NetworkX and PyYAML. Use a Python 3.12+ environment.
It supports CPU/RAM constraints, bandwidth-aware routing, optional latency SLOs,
online arrivals/departures, and time-integrated server/network energy.

| Baseline config | Purpose |
| --- | --- |
| `configs/baselines/distributed_random.yaml` | Seeded random placement |
| `configs/baselines/distributed_ffd.yaml` | Distributed First-Fit Decreasing |
| `configs/baselines/distributed_best_fit.yaml` | Distributed Best-Fit |
| `configs/baselines/distributed_hre.yaml` | Distributed HRE-inspired heuristic |
| `configs/baselines/distributed_cross_dc.yaml` | Allow a chain to span data centers |
| `configs/baselines/distributed_hre_fat_tree.yaml` | Full k-ary fat-tree DC fabrics |

Every preset uses the same offered workload and 64-server compute budget. HRE is an explicitly documented adaptation of the supplied paper, not an exact reproduction; the paper's VNFG and ONP splitting baselines are not implemented.

```bash
# Validate without running
python main.py --config configs/baselines/distributed_hre.yaml --validate-only

# Run another baseline
python main.py --config configs/baselines/distributed_best_fit.yaml

# Verify the distributed simulator without installing the legacy RL stack
python -m pip install -r requirements-hybrid.txt
python -m unittest discover -s tests/distributed -v
```

Results are saved in unique directories under `results/distributed/<experiment-name>/`, including resolved YAML, topology, workload, placements, per-request/per-event CSVs, per-DC metrics and summaries.

See the [Vietnamese model and experiment guide](docs/distributed-data-centers.md) for setup, parameters, algorithm definitions, units, paper correspondence and limitations.

### Adaptive Hierarchical Hybrid SFC Embedding (AHQ-SFC)

AHQ-SFC adds bounded candidate reduction, a placement QUBO, exact/SA controls, local QAOA statevector simulation, and an optional D-Wave adapter. Its controller models queued decisions, deadlines and stale-state validation before admission.

```bash
python -m pip install -r requirements-hybrid.txt
python main.py --config configs/hybrid/distributed_ahq_adaptive.yaml
python main.py --config configs/hybrid/distributed_hybrid_qaoa_sim.yaml
```

The standalone presets in `configs/hybrid/` share offered traffic and controller settings. Experiments run on CPU. QAOA uses a local NumPy statevector simulator. See the [AHQ-SFC run and evaluation guide](docs/ahq-sfc-running.md) for backend selection, matched-seed benchmarking, offline policy fitting, quantum labeling and limits.

All baseline and hybrid presets include controller timing and periodic single-VNF migration within the source DC. State transfer, downtime, reservations and route changes are part of the system cost, with the same migration settings across solvers. The [simulation guide](docs/distributed-data-centers.md) describes the model, parameters and output.

### Existing Virne Experiments

The existing Hydra/VNE/RL pipeline remains available with its original full runtime dependencies and configuration schema, for example:

```bash
python main.py solver.solver_name=nrm_rank v_sim_setting.num_v_nets=10 training.use_cuda=false 'logger.backends=[console]'
```

These legacy solvers do not automatically use the new DC model. Use the baseline YAML files above for the distributed data center experiments.

## Related

- [nfv-resource-allocation](https://github.com/GeminiLight/nfv-resource-allocation) — a research task package built on this simulator: online VNE heuristics under a decision-isolated, integrity-audited evaluation runtime.
