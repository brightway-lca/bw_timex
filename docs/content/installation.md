# Installation

`bw_timex` is a Python software package. It's available via [`conda`](https://docs.conda.io/en/latest/) / [`mamba`](https://mamba.readthedocs.io/en/latest/) and [`pip`](https://pypi.org/project/pip/).

```{note}
`bw_timex` depends on Brightway25, and will install bw25-compatible versions of the bw packages. This means that it cannot be added to existing environments that are based on Brightway2, e.g., environments containing [Activity Browser](https://github.com/LCA-ActivityBrowser/activity-browser).
```

## Installing `bw_timex` using `conda` or `mamba`

```{admonition} Prerequisites
:class: important
1. A working installation of [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) or [`mamba`](https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html). If you are using `conda`, we recommend installing the [libmamba solver](https://www.anaconda.com/blog/a-faster-conda-for-a-growing-community).
2. Basic knowledge of [Conda environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
```

::::{tab-set}

:::{tab-item} Linux, Windows, or MacOS (x64)

1. Create a new Conda environment with `bw_timex`:

```console
conda create -n timex -c conda-forge -c cmutel -c diepers bw_timex
```

2. Activate the environment:

```console
conda activate timex
```

3. (Optional but recommended) You can also use conda to install useful libraries like `jupyterlab`:

```console
conda install -c conda-forge jupyterlab
```

:::

:::{tab-item} macOS (Apple Silicon/ARM)

```{note}
Brightway runs on the new Apple Silicon ARM architecture. However, the super-fast linear algebra software library `pypardiso` is not compatible with the ARM processor architecture. To avoid critical errors during instruction that would break core functionality, a different version of Brightway (`brightway_nosolver`) and a different linear algebra software library (`scikit-umfpack`) must be installed.
```

1. Create a new Conda environment (in this example named `timex`):

```
conda create -n timex -c conda-forge -c cmutel -c diepers bw_timex brightway25_nosolver scikit-umfpack
```

2. Activate the environment:

```
conda activate timex
```

3. (Optional but recommended) You can also use conda to install useful libraries like `jupyterlab`:

```console
conda install -c conda-forge jupyterlab
```

::::

## Installing `bw_timex` using `pip`

::::{tab-set}

:::{tab-item} Linux, Windows, or MacOS (x64)

1. Install `python` from [the website](https://www.python.org/downloads/), your system package manager, or [Homebrew](https://docs.brew.sh/Homebrew-and-Python).

2. Create a directory for your virtual environments, such as `C:/Users/me/virtualenvs/`.

3. In a console or terminal window, create a new virtual environment:

```console
python -m venv C:/Users/me/virtualenvs/timex
```

4. Activate the virtual environment. The exact syntax depends on your operating system; it will look something like:

```console
source C:/Users/me/virtualenvs/timex/bin/activate
```

5. Install `bw_timex`:

```console
pip install bw_timex pypardiso
```

You can also use pip to install useful libraries like `jupyterlab`.

:::

:::{tab-item} MacOS (Apple Silicon/ARM)

```{note}
Due to [an upstream bug](https://github.com/scikit-umfpack/scikit-umfpack/issues/98), there is currently no reliable way to install the fast sparse library `umfpack` on Apple Silicon using `pip`, and the `pypardiso` library is only for x64 systems. If you are doing computationally-intensive workflows, we recommend installing Brightway using `conda` or `mamba` for now. If you are doing fewer calculations or software development installation via `pip` is fine.
```

1. Install `python` from [the website](https://www.python.org/downloads/), your system package manager, or [Homebrew](https://docs.brew.sh/Homebrew-and-Python).

2. In a terminal window, create a directory for your virtual environments. This can be anywhere; we will use the home directory here as an example:

```console
cd
mkdir virtualenvs
```

3. Create and activate a virtualenv:

```console
python -m venv virtualenvs/timex
source virtualenvs/timex/bin/activate
```

4. Install `bw_timex`:

```console
pip install bw_timex
```

You can also use pip to install useful libraries like `jupyterlab`.

::::
