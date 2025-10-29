<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/_static/bw_timex_dark_nomargins.svg" height="50">
    <img alt="bw_timex logo" src="docs/_static/bw_timex_light_nomargins.svg" height="50">
  </picture>
</h1>

[![Read the Docs](https://img.shields.io/readthedocs/timex?label=documentation)](https://docs.brightway.dev/projects/bw-timex/en/latest/)
[![tests](https://img.shields.io/github/actions/workflow/status/brightway-lca/bw_timex/python-test.yml?label=tests)](https://github.com/brightway-lca/bw_timex/actions/workflows/python-test.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/bw-timex?color=%2300549f)](https://pypi.org/project/bw-timex/)
[![Conda Version](https://img.shields.io/conda/v/diepers/bw_timex?label=conda)](https://anaconda.org/diepers/bw_timex)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/brightway-lca/bw_timex/HEAD?labpath=notebooks%2Fgetting_started.ipynb)
[![Conda - License](https://img.shields.io/conda/l/diepers/bw_timex)](https://github.com/brightway-lca/bw_timex/blob/main/LICENSE)

This is a python package for [time-explicit Life Cycle Assessment](https://docs.brightway.dev/projects/bw-timex/en/latest/content/theory.html#terminology) that helps you assess the environmental impacts of products and processes over time. `bw_timex` builds on top of the [Brightway LCA framework](https://docs.brightway.dev/en/latest).

## ✨ Features
This package enables you to account for:
- **Timing of processes** throughout the supply chain (e.g., end-of-life treatment occurs 20 years after construction)
- **Variable** and/or **evolving** supply chains & technologies (e.g., increasing shares of renewable electricity in the future)
- **Timing of emissions** (by applying dynamic characterization functions)

You can define temporal distributions for process and emission exchanges, which are then *automatically* propagated through the supply chain and mapped to corresponding time-explicit databases. The resulting time-explicit LCI reflects the current technology status within the production system at the actual time of each process. Also, `bw_timex` keeps track of the timing of emissions which means that you can apply [dynamic characterization functions](https://github.com/brightway-lca/dynamic_characterization).

## 💡 Use Cases
`bw_timex` is ideal for cases with:
- **Variable** or strongly **evolving production systems**
- **Long-lived** products
- **Biogenic** carbon

Still unsure if you need time-explicit LCA? Overwhelmed by all the options? [Check out our decision tree](https://docs.brightway.dev/projects/bw-timex/en/latest/content/decisiontree.html) for some guidance.

## 👩‍💻 Getting Started
- [Installation Guide](https://docs.brightway.dev/projects/bw-timex/en/latest/content/installation.html)
- [Getting Started Tutorial](https://docs.brightway.dev/projects/bw-timex/en/latest/content/getting_started/index.html)
- [Example Collection](https://docs.brightway.dev/projects/bw-timex/en/latest/content/examples/index.html)

## 📚 Citation
If `bw_timex` supports your scientific work, please consider citing our companion publications:
- The conceptual framework and formalization of time-explicit LCA are described in our [methodology paper](https://doi.org/10.1007/s11367-025-02539-3)
- The implementation of this methodology in `bw_timex` is covered by our [JOSS paper (preprint)](https://github.com/openjournals/joss-papers/blob/joss.07981/joss.07981/10.21105.joss.07981.pdf)

## 🤝 Contributing
We welcome contributions! If you have suggestions or want to fix a bug, please:
- [Open an Issue](https://github.com/brightway-lca/bw_timex/issues)
- [Send a Pull Request](https://github.com/brightway-lca/bw_timex/pulls)

## 💬 Support
If you have any questions or need help, do not hesitate to contact us:
- Timo Diepers ([timo.diepers@ltt.rwth-aachen.de](mailto:timo.diepers@ltt.rwth-aachen.de))
- Amelie Müller ([a.muller@cml.leidenuniv.nl](mailto:a.muller@cml.leidenuniv.nl))
- Arthur Jakobs ([artos.jakobs@psi.ch](mailto:artos.jakobs@psi.ch))
