> ⚠️ This package is still under development and some functionalities may change in the future.

# `timex_lca`

[Timex_lca](https://github.com/TimoDiepers/timex) is a python package for time-explicit Life Cycle Assessment that helps you assess the environmental impacts of products and processes over time. `timex_lca` builds on top of the [Brightway LCA framework](https://docs.brightway.dev/en/latest).

## Features:
This package enables you to account for: 
- **Timing of processes** throughout the supply chain (e.g., end-of-life treatment occurs 20 years after construction)
- **Variable** and/or **evolving** supply chains & technologies (e.g., increasing shares of renewable electricity in the future)
- **Timing of emissions** (by applying dynamic characterization functions)

You can define temporal distributions for process and emission exchanges, which are then *automatically* propagated through the supply chain and mapped to corresponding time-explicit databases. The resulting time-explicit LCI reflects the current technology status within the production system at the actual time of each process. Also, `timex_lca` keeps track of the timing of emissions, which means that you can apply dynamic characterization functions.

## Use cases:
`timex_lca` is ideal for cases with:
- **Variable** or strongly **evolving production systems**
- **Long-lived** products
- **Biogenic** carbon

Here's a visualization of what `timex_lca` can do for you:

```{image} content/data/timex_dark.svg
:class: only-dark
```
```{image} content/data/timex_light.svg
:class: only-light
```

## Support: 
If you have any questions or need help, do not hesitate to contact us:
- Timo Diepers ([timo.diepers@ltt.rwth-aachen.de](mailto:timo.diepers@ltt.rwth-aachen.de))
- Amelie Müller ([a.muller@cml.leidenuniv.nl](mailto:a.muller@cml.leidenuniv.nl))

```{toctree}
---
hidden:
maxdepth: 1
---
Installation <content/installation>
Theory <content/theory>
Examples <content/examples/index>
API Reference <content/api/index>
Contributing <content/contributing>
Code of Conduct <content/codeofconduct>
License <content/license>
Changelog <content/changelog>
```