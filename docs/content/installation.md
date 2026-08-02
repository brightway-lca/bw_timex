---
icon: lucide/download
---

# Installation

`bw_timex` is a Python software package. It's available via [`pip`](https://pypi.org/project/pip/) and [`conda`](https://docs.conda.io/en/latest/) / [`mamba`](https://mamba.readthedocs.io/en/latest/).

!!! note

    `bw_timex` depends on Brightway25, and will install bw25-compatible versions of the bw packages. This means that it cannot be added to existing environments that are based on Brightway2, e.g., environments containing [Activity Browser](https://github.com/LCA-ActivityBrowser/activity-browser).

!!! tip "Writing your study with an AI coding agent?"

    Once your environment is set up, check out our [bw_timex agent skill](ai_agent_skill.md) — it teaches assistants like Claude Code the whole `bw_timex` workflow so they don't have to guess at the API.

## Installing `bw_timex` using `uv`

[uv](https://docs.astral.sh/uv/) is a fast, modern Python package manager written in Rust. It's significantly faster than pip and handles dependency resolution more reliably. Installing uv is a [one-liner](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer).

=== "Linux, Windows, or macOS (x64)"

    === "New Project"

        Create a new project with uv:

        ```bash
        uv init my-timex-project
        cd my-timex-project
        uv add bw_timex pypardiso
        ```

        Run your scripts with:

        ```bash
        uv run python my_script.py
        ```

        Or directly get started in [JupyterLab](https://jupyter.org):

        ```bash
        uv run --with jupyter jupyter lab
        ```

        By default, `jupyter lab` will start the server at [http://localhost:8888/lab](http://localhost:8888/lab).

    === "New Environment"

        Create and activate a new virtual environment:

        ```bash
        uv venv .venv
        source .venv/bin/activate  # Linux/macOS
        # or: .venv\Scripts\activate  # Windows
        ```

        Install `bw_timex`:

        ```bash
        uv pip install bw_timex pypardiso
        ```

    === "Existing Environment"

        If you already have an activated virtual environment:

        ```bash
        uv pip install bw_timex pypardiso
        ```

=== "macOS (Apple Silicon/ARM)"

    !!! note

        Fast calculations need `SuiteSparse` through [scikit-umfpack](https://github.com/scikit-umfpack/scikit-umfpack/). `uv` manages your Python environment, but you must first install the system-level `SuiteSparse` dependencies via [Homebrew](https://brew.sh).

    Install system dependencies:

    ```bash
    brew install swig suite-sparse
    ```

    === "New Project"

        ```bash
        uv init my-timex-project
        cd my-timex-project
        uv add bw_timex scikit-umfpack
        ```

        Run Jupyter (without adding it to the project):

        ```bash
        uv run --with jupyter jupyter lab
        ```

    === "New/Existing Environment"

        ```bash
        uv venv .venv  # skip if reusing an existing environment
        source .venv/bin/activate
        uv pip install bw_timex scikit-umfpack
        ```

## Installing `bw_timex` using `pip`

=== "Linux, Windows, or macOS (x64)"

    1. Install `python` from [the website](https://www.python.org/downloads/), your system package manager, or [Homebrew](https://docs.brew.sh/Homebrew-and-Python).

    2. Create a directory for your virtual environments, such as `C:/Users/me/virtualenvs/`.

    3. In a console or terminal window, create a new virtual environment:

    ```bash
    python -m venv C:/Users/me/virtualenvs/timex
    ```

    4. Activate the virtual environment. The exact syntax depends on your operating system; it will look something like:

    ```bash
    source C:/Users/me/virtualenvs/timex/bin/activate # Linux/macOS
    # or: C:\Users\me\virtualenvs\timex\Scripts\activate # Windows
    ```

    5. Install `bw_timex`:

    ```bash
    pip install bw_timex pypardiso
    ```

    You can also use pip to install useful libraries like `jupyterlab`.

=== "macOS (Apple Silicon/ARM)"

    !!! note

        Fast calculations need `SuiteSparse` through [scikit-umfpack](https://github.com/scikit-umfpack/scikit-umfpack/). This background library can be installed via [homebrew](https://brew.sh/), as shown in this section, or via `conda` or `mamba`, as shown below.

    1. Install `python` from [Homebrew](https://docs.brew.sh/Homebrew-and-Python).

    2. Install the requirements for `SuiteSparse` via `homebrew`:

    ```bash
    brew install swig suite-sparse
    ```

    3. In a terminal window, create a directory for your virtual environments. This can be anywhere; we will use the home directory here as an example:

    ```bash
    cd
    mkdir virtualenvs
    ```

    4. Create and activate a virtualenv:

    ```bash
    python -m venv virtualenvs/timex
    source virtualenvs/timex/bin/activate
    ```

    5. Install `bw_timex`:

    ```bash
    pip install bw_timex scikit-umfpack
    ```

    You can also use pip to install useful libraries like `jupyterlab`.

## Installing `bw_timex` using `conda` or `mamba`

!!! important "Prerequisites"

    1. A working installation of [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) or [`mamba`](https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html). If you are using `conda`, we recommend installing the [libmamba solver](https://www.anaconda.com/blog/a-faster-conda-for-a-growing-community).
    2. Basic knowledge of [Conda environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)

=== "Linux, Windows, or macOS (x64)"

    1. Create a new Conda environment with `bw_timex`:

    ```bash
    conda create -n timex -c conda-forge -c cmutel -c diepers bw_timex
    ```

    2. Activate the environment:

    ```bash
    conda activate timex
    ```

    3. (Optional but recommended) You can also use conda to install useful libraries like `jupyterlab`:

    ```bash
    conda install -c conda-forge jupyterlab
    ```

=== "macOS (Apple Silicon/ARM)"

    !!! note

        Brightway runs on the new Apple Silicon ARM architecture. However, the super-fast linear algebra software library `pypardiso` is not compatible with the ARM processor architecture. To avoid critical errors during instruction that would break core functionality, a different version of Brightway (`brightway_nosolver`) and a different linear algebra software library (`scikit-umfpack`) must be installed.

    1. Create a new Conda environment with `bw_timex`:

    ```bash
    conda create -n timex -c conda-forge -c cmutel -c diepers bw_timex brightway25_nosolver numpy">=2" scikit-umfpack">=0.4.2"
    ```

    2. Activate the environment:

    ```bash
    conda activate timex
    ```

    3. (Optional but recommended) You can also use conda to install useful libraries like `jupyterlab`:

    ```bash
    conda install -c conda-forge jupyterlab
    ```
