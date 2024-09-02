# Time-explicit LCA with `bw_timex`

`bw_timex` is a python package for [time-explicit Life Cycle Assessment](content/theory.md#terminology) that helps you assess the environmental impacts of products and processes over time. `bw_timex` builds on top of the [Brightway LCA framework](https://docs.brightway.dev/en/latest).

## Features
This package enables you to account for:
- **Timing of processes** throughout the supply chain (e.g., end-of-life treatment occurs 20 years after construction)
- **Variable** and/or **evolving** supply chains & technologies (e.g., increasing shares of renewable electricity in the future)
- **Timing of emissions** (by applying dynamic characterization functions)

You can define temporal distributions for process and emission exchanges, which are then *automatically* propagated through the supply chain and mapped to corresponding time-explicit databases. The resulting time-explicit LCI reflects the current technology status within the production system at the actual time of each process. Also, `bw_timex` keeps track of the timing of emissions, which means that you can apply dynamic characterization functions.

## Use cases
`bw_timex` is ideal for cases with:
- **Variable** or strongly **evolving production systems**
- **Long-lived** products
- **Biogenic** carbon

## Support
If you have any questions or need help, do not hesitate to contact us:
- Timo Diepers ([timo.diepers@ltt.rwth-aachen.de](mailto:timo.diepers@ltt.rwth-aachen.de))
- Amelie Müller ([a.muller@cml.leidenuniv.nl](mailto:a.muller@cml.leidenuniv.nl))
- Arthur Jakobs ([artos.jakobs@psi.ch](mailto:artos.jakobs@psi.ch))

```{toctree}
---
hidden:
maxdepth: 1
---
Installation <content/installation>
Getting Started <content/getting_started/index>
Theory <content/theory>
Examples <content/examples/index>
API <content/api/index>
Contributing <content/contributing>
Code of Conduct <content/codeofconduct>
License <content/license>
Changelog <content/changelog>
```
