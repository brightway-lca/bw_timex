---
tags:
  - example
---

# Example Collection

Here are some examples on how you can use `bw_timex`. They come in three flavours,
mirroring the folders in the [`notebooks/`](https://github.com/brightway-lca/bw_timex/tree/main/notebooks)
directory of the repository - every page below is generated from the notebook of the
same name, so you can run any of them yourself.

## Tutorials

*Step-by-step walk-throughs that build their own data from scratch, so you can run them
anywhere without access to a commercial background database.*

<div class="grid cards" markdown>

-   **🚗⚡ Electric Car from Scratch**

    ---

    Time-explicit LCA of the entire life cycle of an electric car, on background databases the notebook makes up itself.

    ![](./data/ev_lifecycle_light.svg#only-light){ .off-glb }
    ![](./data/ev_lifecycle_dark.svg#only-dark){ .off-glb }

    [:material-arrow-right: View Tutorial](./tutorials/electric_vehicle_from_scratch.md)

    *by @TimoDiepers*

-   **🌿📈 Dynamic Characterization**

    ---

    The dynamic characterization capabilities that come with a `TimexLCA`.

    ![](./data/dynamic_characterization.svg){ .off-glb }

    [:material-arrow-right: View Tutorial](./tutorials/dynamic_characterization.md)

    *by @muelleram*

-   **📁💻 Import foreground system from Excel**

    ---

    How to import your modelled product system, temporal distributions included, from an Excel file.

    <img class="off-glb" src="https://upload.wikimedia.org/wikipedia/commons/thumb/6/60/Microsoft_Office_Excel_%282025%E2%80%93present%29.svg/250px-Microsoft_Office_Excel_%282025%E2%80%93present%29.svg.png" style="display: block; width: 50%; margin: 0 auto;" />

    [:material-arrow-right: View Tutorial](./tutorials/import_model_from_excel.md)

    *by @jakobsarthur & @muelleram*

</div>

## Case Studies

*Complete studies on real data - they expect a Brightway project with ecoinvent and
prospective databases built with [`premise`](https://github.com/polca/premise).*

<div class="grid cards" markdown>

-   **🚗🔋 Electric Car with premise**

    ---

    The same electric car, but on ecoinvent and premise data, kept as compact as possible.

    ![](./data/ev_lifecycle_light.svg#only-light){ .off-glb }
    ![](./data/ev_lifecycle_dark.svg#only-dark){ .off-glb }

    [:material-arrow-right: View Case Study](./case_studies/electric_vehicle_premise.md)

    *by @TimoDiepers*

-   **🚗🔍 Electric Car with premise (in detail)**

    ---

    The same premise case study, with every modelling step and every additional option of `bw_timex` spelled out.

    ![](./data/ev_lifecycle_light.svg#only-light){ .off-glb }
    ![](./data/ev_lifecycle_dark.svg#only-dark){ .off-glb }

    [:material-arrow-right: View Case Study](./case_studies/electric_vehicle_premise_detailed.md)

    *by @TimoDiepers*

-   **📄🚗 EV Case Study for our paper**

    ---

    The notebook used to calculate the time-explicit LCAs and create the Figures for our paper on time-explicit LCA.

    <img class="off-glb" src="./data/paper_radiative_forcing.svg" alt="" style="display: block; background-color: white;" />

    [:material-arrow-right: View Case Study](./case_studies/paper_case_study.md)

    *by @TimoDiepers*

</div>

## Advanced

*Deep dives into one optional feature each, for when the standard workflow isn't enough.*

<div class="grid cards" markdown>

-   **🌍⏳ Temporal Distributions in the Background**

    ---

    Let the temporal graph traversal continue into the background system, using `traverse_background`.

    [:material-arrow-right: View Example](./advanced/background_temporal_distributions.md)

    *by @TimoDiepers*

-   **🌍🔬 Background Temporalization with premise**

    ---

    The same feature, but on real ecoinvent and premise data instead of a dummy system.

    [:material-arrow-right: View Example](./advanced/background_temporal_distributions_premise.md)

    *by @TimoDiepers*

-   **🎲📦 Uncertainty with Datapackages**

    ---

    Run scenarios, sensitivity analyses and Monte Carlo on a solved `TimexLCA` by handing `bw2calc` extra datapackages.

    [:material-arrow-right: View Example](./advanced/uncertainty_with_datapackages.md)

    *by @TimoDiepers*

</div>

## Anything to add? 🧐

Please contact us if you want to share your super cool example - or open a pull request
adding your notebook to `notebooks/`, and it will show up here.
