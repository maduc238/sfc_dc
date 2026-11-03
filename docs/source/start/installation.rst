Installation
============

The distributed data center experiments run directly from this source checkout.
Use Python 3.12 or newer; installing the ``virne`` package is unnecessary.

Create a Virtual Environment
----------------------------

.. code-block:: bash

    python3 -m venv .venv
    source .venv/bin/activate

Install Simulation Dependencies
-------------------------------

From the repository root, install the two libraries used by the DC simulator:

.. code-block:: bash

    python -m pip install -r requirements-simulation.txt
    python main.py --config configs/baselines/distributed_hre.yaml --validate-only

On Windows, use ``.venv\Scripts\python.exe`` after creating the virtual
environment. The new simulator needs NetworkX and PyYAML only. Continue to the
:doc:`Quickstart <running>` to run a baseline.

Legacy VNE/RL Runtime
--------------------

The existing VNE/RL pipeline has additional dependencies. For that pipeline,
the source installation script supports CPU environments on
Linux and macOS, plus CUDA 12.6, 13.0, and 13.2 on Linux. It installs PyTorch
2.13.0, PyG 2.8.0.post1, Gymnasium 1.3.0, Virne in editable mode, and the
matching optional ``pyg_lib`` acceleration wheel.

.. code-block:: bash

    git clone https://github.com/GeminiLight/virne.git
    cd virne

    # CPU-only PyTorch and PyG
    bash install.sh -c cpu

    # CUDA 12.6; 13.0 and 13.2 are also supported
    bash install.sh -c 12.6

If ``-c`` is omitted, the script installs the CPU build. This explicit default
avoids selecting a CUDA runtime that is incompatible with the host driver.

Verify the Installation
-----------------------

Keep the virtual environment active and run:

.. code-block:: bash

    python main.py --version
    python -c "import gymnasium, torch, torch_geometric, virne; print(virne.__version__, gymnasium.__version__, torch.__version__, torch_geometric.__version__)"

The command should print the installed Virne, Gymnasium, PyTorch, and PyG
versions. You can then continue to the :doc:`Quickstart <running>`.
