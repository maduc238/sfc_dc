Quickstart
==========

Run the distributed data center simulator directly from the source checkout:

.. code-block:: bash

   python -m pip install -r requirements-simulation.txt
   python main.py --config configs/baselines/distributed_hre.yaml

Each YAML in ``configs/baselines`` is a standalone baseline. Available choices
include centralized/distributed FFD and HRE-inspired placement, Random, Best-Fit,
cross-DC chains, and fat-tree fabrics. All presets use the same workload and
64-server compute budget. HRE is an adaptation, not an exact paper reproduction.

.. code-block:: bash

   python main.py --config configs/baselines/distributed_best_fit.yaml
   python main.py --config configs/baselines/distributed_hre.yaml --validate-only
   python -m pip install -r requirements-hybrid.txt
   python -m unittest discover -s tests/distributed -v

Outputs appear under ``results/distributed/<experiment-name>/<unique-run>/``:
resolved configuration, topology, offered workload, successful placements,
per-request and per-event records, per-DC metrics, and JSON/CSV summaries.
The full model, units, algorithms and paper correspondence are documented in
``docs/distributed-data-centers.md`` in the source repository.

Legacy VNE/RL Experiments
------------------------

The original Hydra pipeline remains available with the full legacy runtime.
Its solvers and configuration schema are separate from the DC simulator:

.. code-block:: bash

   python main.py \
     solver.solver_name=nrm_rank \
     v_sim_setting.num_v_nets=10 \
     training.use_cuda=false \
     'logger.backends=[console]'

Virne generates a physical network and ten virtual requests, solves each
arrival event, and prints a summary. A successful run ends with:

.. code-block:: text

   --------------------   Complete   --------------------

The progress bar also shows the running acceptance rate (``ac``),
revenue-to-cost ratio (``r2c``), and number of in-service requests.

Inspect the Results
-------------------

Hydra places the run under ``results/``. The most useful files are:

.. code-block:: text

   results/virne/nrm_rank/<run-id>/config.yaml
   results/virne/nrm_rank/<run-id>/summary.csv
   results/virne/nrm_rank/<run-id>/records/*.csv

``config.yaml`` is the fully resolved experiment configuration,
``summary.csv`` contains one row of run-level metrics, and ``records/*.csv``
contains per-event details. See :doc:`../evaluation/metrics` for the field
definitions.

Override the Configuration
--------------------------

Virne uses `Hydra <https://hydra.cc/>`_, so any existing configuration value
can be overridden with ``key=value``. Prefix a new key with ``+``.

.. code-block:: bash

   # Select another registered solver
   python main.py solver.solver_name=random_rank

   # Load a physical topology from a GML file
   python main.py +p_net_setting.topology.file_path=/path/to/Geant.gml

   # Use the offline network system
   python main.py system.if_offline_system=true

   # Preview the resolved configuration without running a simulation
   python main.py --cfg job

Use the :doc:`solver registry <../solver/overview>` for valid solver names and
:doc:`simulation scenarios <simulation>` for the canonical PN and VN settings.
For the DC experiment workflow, pass ``--config`` as shown at the top of this page.
