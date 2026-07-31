<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/_static/bw_timex_dark_nomargins.svg" height="50">
    <img alt="bw_timex logo" src="docs/_static/bw_timex_light_nomargins.svg" height="50">
  </picture>
</h1>

[![Read the Docs](https://img.shields.io/readthedocs/timex?label=documentation)](https://docs.brightway.dev/projects/bw-timex/en/latest/)
[![tests](https://img.shields.io/github/actions/workflow/status/brightway-lca/bw_timex/python-test.yml?label=tests)](https://github.com/brightway-lca/bw_timex/actions/workflows/python-test.yml)
[![codecov](https://codecov.io/gh/brightway-lca/bw_timex/graph/badge.svg)](https://codecov.io/gh/brightway-lca/bw_timex)
[![PyPI - Version](https://img.shields.io/pypi/v/bw-timex?color=%2300549f)](https://pypi.org/project/bw-timex/)
[![Conda Version](https://img.shields.io/conda/v/diepers/bw_timex?label=conda)](https://anaconda.org/diepers/bw_timex)
[![Conda - License](https://img.shields.io/conda/l/diepers/bw_timex)](https://github.com/brightway-lca/bw_timex/blob/main/LICENSE)
[![status](https://joss.theoj.org/papers/eb9021af0207b86e02439768a4841670/status.svg)](https://joss.theoj.org/papers/eb9021af0207b86e02439768a4841670)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/brightway-lca/bw_timex/HEAD?labpath=notebooks%2Fgetting_started.ipynb)

**`bw_timex` makes your LCA use the right data for the right point in time.** It's a python package for [time-explicit Life Cycle Assessment](https://docs.brightway.dev/projects/bw-timex/en/latest/content/theory.html#terminology), built on top of the [Brightway LCA framework](https://docs.brightway.dev/en/latest).

## ⏳ The problem
An LCA normally evaluates your **entire supply chain with data for a single point in time**. But an electric car built in 2025 has a battery made in 2024, is driven on an evolving electricity mix until 2040, and is recycled with 2040 technologies. Anything long-lived, or in a system that changes over time, ends up being assessed with **data that doesn't match when processes actually happen**.

You *can* work around this by hand: split each exchange into one copy per point in time, work out which year every process upstream lands in — a delay at one level shifts every other process connected to it — and wire each copy to a different time-specific background database. It's tedious, easy to get wrong, and you redo it every time the model changes.

## ✨ What `bw_timex` does
**You bring** your normal Brightway model, plus:
- **temporal distributions**, specifying when each exchange happens relative to the process consuming it — either a single shift ("2 years earlier") or spread over time ("30% two years earlier, 50% now, 20% four years later")
- **time-specific process data**, like background databases representing different points in time (e.g. from [`premise`](https://github.com/polca/premise)) and, for your own foreground processes, how they change over time (e.g. efficiency gains)

**`bw_timex` figures out** when every process in the supply chain actually happens, relinks each one to the background database matching that date (interpolating between databases in between), and applies your time-specific amounts.

**You get** a time-explicit inventory: each process assessed with data from the time it actually occurs, and each emission tagged with when it happens — so you can characterize it with [dynamic characterization functions](https://github.com/brightway-lca/dynamic_characterization) instead of static factors.

Most useful for long-lived products, strongly evolving production systems, and biogenic carbon. Still unsure whether you need time-explicit LCA? [Check out our decision tree](https://docs.brightway.dev/projects/bw-timex/en/latest/content/decisiontree.html) for some guidance.

## 👩‍💻 Getting Started
- [Installation Guide](https://docs.brightway.dev/projects/bw-timex/en/latest/content/installation.html)
- [Getting Started Tutorial](https://docs.brightway.dev/projects/bw-timex/en/latest/content/getting_started/index.html)
- [Example Collection](https://docs.brightway.dev/projects/bw-timex/en/latest/content/examples/index.html)

## 📚 Citation
If `bw_timex` supports your scientific work, please consider citing our companion publications:
- The conceptual framework and formalization of time-explicit LCA are described in our [methodology paper](https://doi.org/10.1007/s11367-025-02539-3)
- The implementation of this methodology in `bw_timex` is covered by our [JOSS paper](https://doi.org/10.21105/joss.09621)

## 🤝 Contributing
We welcome contributions! If you have suggestions or want to fix a bug, please:
- [Open an Issue](https://github.com/brightway-lca/bw_timex/issues)
- [Send a Pull Request](https://github.com/brightway-lca/bw_timex/pulls)

## 💬 Support
If you have any questions or need help, do not hesitate to contact us:
- Timo Diepers ([timo.diepers@ltt.rwth-aachen.de](mailto:timo.diepers@ltt.rwth-aachen.de))
- Amelie Müller ([a.muller@cml.leidenuniv.nl](mailto:a.muller@cml.leidenuniv.nl))
- Arthur Jakobs ([artos.jakobs@psi.ch](mailto:artos.jakobs@psi.ch))
