```{toctree}
---
hidden:
maxdepth: 1
---
Installation <content/installation>
Example <content/examples/index>
Tutorial <content/tutorial>
Theory <content/theory>
API Reference <content/api/index>
Contributing <content/contributing>
Code of Conduct <content/codeofconduct>
License <content/license>
Changelog <content/changelog>
```
> ⚠️ This is a work in progress. Please refer to the [Timex  Readme on GitHub](https://github.com/TimoDiepers/timex) for now.

# timex
*This package is still under development and some functionalities may change in the future.*

This is a python package for time explicit Life Cycle Assessment, building on top of the [Brightway](https://docs.brightway.dev/en/latest) LCA framework. `timex` enables consideration of both the timing of processes & emissions (e.g., end-of-life treatment occurs 20 years after construction), as well as the changing state of the production system (e.g., increasing shares of renewable electricity in the future). Users can define temporal distributions for process and emission exchanges, which are then automatically mapped to corresponding time explicit databases. Consequently, the resulting time explicit LCI reflects the current technology status within the production system at the actual time of each process.

```{image} content/data/timex_dark.svg
:class: only-dark
```
```{image} content/data/timex_light.svg
:class: only-light
```