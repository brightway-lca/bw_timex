# Installation

`bw_timex` is a Python software package. It's available via [`pip`](https://pypi.org/project/pip/) or  [`conda`](https://docs.conda.io/en/latest/) / [`mamba`](https://mamba.readthedocs.io/en/latest/).

```{note}
1) We recommend installation via `conda` or `mamba`.
2) bw_timex depends on Brightway25, and will install bw25-compatible versions of the bw packages. This means that it cannot be added to existing virtual environments that are based on Brightway2, e.g., environments containing [Activity Browser](https://github.com/LCA-ActivityBrowser/activity-browser). Please install bw_timex in a separate environment following the instructions below.
```

## Installing `bw_timex` using `conda` or `mamba`

```{admonition} Prerequisites
:class: important
1. A working installation of `conda` or `mamba`. If you are using `conda`, we recommend installing the [libmamba solver](https://www.anaconda.com/blog/a-faster-conda-for-a-growing-community).
2. Basic knowledge of [Conda environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
```

::::{tab-set}

:::{tab-item} Linux, Windows, or MacOS (x64)

1. Create a new Conda environment (in this example named `timex`):

```console
conda create -n timex -c conda-forge -c cmutel -c diepers brightway25 bw_timex
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
conda create -n timex -c conda-forge -c cmutel -c diepers brightway25_nosolver scikit-umfpack bw_timex
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

## Updating `bw_timex`

`bw_timex` is being actively developed, with frequent new releases. To update bw_timex using `conda` or `mamba`, activate your environment, and run:

``` bash
conda update -c diepers bw_timex
```

```{warning}
Newer versions of `bw_timex` can introduce breaking changes, see [Changelog](changelog.md). We recommend to create a new environment for each project, and only update `bw_timex` when you are ready to update your project.
```
