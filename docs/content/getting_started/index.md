# Getting Started

This section will help you quickly getting started with your time-explicit LCA project. We're keeping it simple here - no deep dives into how things work in the background, no exploring of all the features and options `bw_timex` has. Just a quick walkthrough of the different steps of a `TimexLCA`. Here's a rundown:

```{image} ../data/method_small_steps_light.svg
:class: only-light
:height: 450px
:align: center
```

```{image} ../data/method_small_steps_dark.svg
:class: only-dark
:height: 450px
:align: center
```
<br />

In the following sections, we'll walk through the steps 1-4, considering a very simple dummy system. If you directly want to look at a more complex example, take a look at our [example collection](../examples/index.md). If you're interested in the full details on how `bw_timex` works, you can also skip to our [Theory Section](../theory.md).

```{admonition} You want more interaction?
:class: admonition-launch

[Launch this tutorial on Binder!](https://mybinder.org/v2/gh/brightway-lca/bw_timex/HEAD?labpath=notebooks%2Fexample_electric_vehicle_standalone.ipynb) In this interactive environment, you can directly run the bw_timex code yourself whilst following along.
```

```{toctree}
---
hidden:
maxdepth: 1
---
Step 1 - Adding temporal information <adding_temporal_information>
Step 2 - Building the process timeline <build_process_timeline>
Step 3 - Calculating the time-explicit LCI <time_explicit_lci>
Step 4 - Dynamic impact assessment <dynamic_lcia>
```