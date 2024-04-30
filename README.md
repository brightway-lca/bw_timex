# timex_lca

[![Read the Docs](https://img.shields.io/readthedocs/timex?label=documentation)](https://timex.readthedocs.io/en/latest/)
[![PyPI - Version](https://img.shields.io/pypi/v/timex-lca?color=%2300549f)](https://pypi.org/project/timex-lca/)
[![Conda Version](https://img.shields.io/conda/v/diepers/timex_lca?label=conda)](https://anaconda.org/diepers/timex_lca)
![Conda - License](https://img.shields.io/conda/l/diepers/timex_lca)

*This package is still under development and some functionalities may change in the future.*

This is a python package for __time explicit Life Cycle Assessment (LCA)__, building on top of the [Brightway](https://docs.brightway.dev/en/latest) LCA framework. 

Time explicit LCA aims to treat time more coherently in LCA, by __streamlining dynamic LCI and LCIA with prospective LCI databases__. As such, `timex_lca` enables the consideration of both the __timing of processes and emissions__ (e.g., end-of-life treatment occurs 20 years after construction), as well as the __changing state__ of the production system (e.g., increasing shares of renewable electricity in the future). 

Users can define the timing of process and emission exchanges as *temporal distributions*. `timex_lca` automatically propoagates the temporal information through the supply chain and sources the LCI data from the corresponding time explicit database. As the timing of each emission is recorded, `timex_lca` supports __dynamic LCIA methods__, which are provided for the climate change metrics radiative forcing and global warming potential. 

Time explicit LCA can provide more representative LCA results for use cases, such as:
- products with variable or evolving production systems
- long-lived products
- products with biogenic carbon

### Quick links:
All documentation, including detailed description of `timex_lca`: [![Read the Docs](https://img.shields.io/readthedocs/timex?label=documentation)](https://timex.readthedocs.io/en/latest/)

Installation instructions: [![Installation Instructions](https://img.shields.io/badge/installation-instructions-blue)](https://timex.readthedocs.io/en/latest/content/installation.html)

Example notebook: [![Example Notebook](https://img.shields.io/badge/example-notebook-green)](https://github.com/TimoDiepers/timex/blob/main/notebooks/example_setac.ipynb)


### Questions and remarks:
For suggestions of improvements or reporting of bugs, please open an issue on the Github page, send a pull request or directly contact the maintainers.

