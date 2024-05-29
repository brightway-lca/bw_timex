<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/_static/logo_dark_nomargins.svg" height="50">
    <img alt="timex_lca logo" src="docs/_static/logo_light_nomargins.svg" height="50">
  </picture>
</h1>

[![Read the Docs](https://img.shields.io/readthedocs/timex?label=documentation)](https://timex.readthedocs.io/en/latest/)
[![PyPI - Version](https://img.shields.io/pypi/v/timex-lca?color=%2300549f)](https://pypi.org/project/timex-lca/)
[![Conda Version](https://img.shields.io/conda/v/diepers/timex_lca?label=conda)](https://anaconda.org/diepers/timex_lca)
![Conda - License](https://img.shields.io/conda/l/diepers/timex_lca)

> ℹ️ _This package is still under development and some functionalities may change in the future._

This is a python package for time-explicit Life Cycle Assessment that helps you assess the environmental impacts of products and processes over time. `timex_lca` builds on top of the [Brightway LCA framework](https://docs.brightway.dev/en/latest).

## Features:
This package enables you to account for: 
- **Timing of processes** throughout the supply chain (e.g., end-of-life treatment occurs 20 years after construction)
- **Variable** and/or **evolving** supply chains & technologies (e.g., increasing shares of renewable electricity in the future)
- **Timing of emissions** (by applying dynamic characterization functions)

You can define temporal distributions for process and emission exchanges, which are then *automatically* propagated through the supply chain and mapped to corresponding time-explicit databases. The resulting time-explicit LCI reflects the current technology status within the production system at the actual time of each process. Also, `timex_lca` keeps track of the timing of emissions which means that you can apply dynamic characterization functions.

## Use cases:
`timex_lca` is ideal for cases with:
- **Variable** or strongly **evolving production systems**
- **Long-lived** products
- **Biogenic** carbon

## Documentation and Resources:
- [Full Documentation](https://timex.readthedocs.io/en/latest/)
- [Installation Guide](https://timex.readthedocs.io/en/latest/content/installation.html)
- [Example Notebook](https://github.com/TimoDiepers/timex/blob/main/notebooks/example_setac.ipynb)

## Contributing:
We welcome contributions! If you have suggestions or want to fix a bug, please:
- [Open an Issue](https://github.com/TimoDiepers/timex/issues)
- [Send a Pull Request](https://github.com/TimoDiepers/timex/pulls)

## Support: 
If you have any questions or need help, do not hesitate to contact us:
- Timo Diepers ([timo.diepers@ltt.rwth-aachen.de](mailto:timo.diepers@ltt.rwth-aachen.de))
- Amelie Müller ([a.muller@cml.leidenuniv.nl](mailto:a.muller@cml.leidenuniv.nl))
