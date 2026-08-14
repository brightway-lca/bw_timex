# `bw_timex` notebooks

This folder is the single source of truth for all `bw_timex` notebooks. The pages in the
[Examples section of the docs](https://docs.brightway.dev/projects/bw-timex/en/latest/content/examples/)
are generated from the notebooks in `tutorials/`, `examples/` and `advanced/` by
`docs/convert_notebooks.py` - so edit the notebook, never the generated Markdown.

| Folder | What's in it | Published to the docs |
| --- | --- | --- |
| [`tutorials/`](tutorials) | Step-by-step walk-throughs that build their own data, runnable anywhere | yes (except `1_getting_started`, which the User Guide covers) |
| [`examples/`](examples) | Complete case studies on ecoinvent + [premise](https://github.com/polca/premise) data | yes |
| [`advanced/`](advanced) | Deep dives into one optional feature each | yes |
| [`teaching/`](teaching) | Course material: a guided example plus an exercise and its solution | no |
| [`development/`](development) | Benchmarking and other maintainer scratch space | no |
| [`data/`](data) | Assets shared by the notebooks (Excel model, figures) | copied on demand |

## Tutorials

No commercial database needed - every tutorial creates the data it uses.

| Notebook | What it shows |
| --- | --- |
| [`1_getting_started.ipynb`](tutorials/1_getting_started.ipynb) | The four steps of a `TimexLCA` on a minimal dummy system |
| [`2_electric_vehicle_from_scratch.ipynb`](tutorials/2_electric_vehicle_from_scratch.ipynb) | Cradle-to-grave time-explicit LCA of an electric car, on background databases the notebook makes up itself |
| [`3_dynamic_characterization.ipynb`](tutorials/3_dynamic_characterization.ipynb) | Dynamic LCIA: radiative forcing and GWP, fixed vs. relative time horizons |
| [`4_import_model_from_excel.ipynb`](tutorials/4_import_model_from_excel.ipynb) | Importing a foreground system incl. temporal distributions from [an Excel file](data/electric_vehicle_foreground.xlsx) |

## Examples

These need a Brightway project holding ecoinvent (cutoff) plus prospective databases
generated from it with premise. Each notebook states the exact versions and scenarios
it expects.

| Notebook | What it shows |
| --- | --- |
| [`electric_vehicle_premise.ipynb`](examples/electric_vehicle_premise.ipynb) | The same electric car on ecoinvent + premise data, in as few steps as possible |
| [`electric_vehicle_premise_detailed.ipynb`](examples/electric_vehicle_premise_detailed.ipynb) | The same premise case study with every modelling step and option spelled out |
| [`paper_case_study.ipynb`](examples/paper_case_study.ipynb) | The case study and figures of our paper on time-explicit LCA |

## Advanced

| Notebook | What it shows | Needs premise data |
| --- | --- | --- |
| [`background_temporal_distributions.ipynb`](advanced/background_temporal_distributions.ipynb) | Temporal distributions *inside* background databases, via `traverse_background` | no |
| [`background_temporal_distributions_premise.ipynb`](advanced/background_temporal_distributions_premise.ipynb) | The same feature on ecoinvent + premise data | yes |
| [`uncertainty_with_datapackages.ipynb`](advanced/uncertainty_with_datapackages.ipynb) | Scenarios, sensitivity and Monte Carlo on a solved `TimexLCA` via extra `bw_processing` datapackages | no |

## Teaching

| Notebook | What it is |
| --- | --- |
| [`ev_walkthrough_premise.ipynb`](teaching/ev_walkthrough_premise.ipynb) | Guided electric-car example used in `bw_timex` courses |
| [`exercise_ev_vs_petrol.ipynb`](teaching/exercise_ev_vs_petrol.ipynb) | Exercise: compare a battery-electric and a petrol car under two IAM scenarios |
| [`exercise_ev_vs_petrol_solutions.ipynb`](teaching/exercise_ev_vs_petrol_solutions.ipynb) | The same exercise, with solutions |

## Regenerating the docs pages

```bash
python docs/convert_notebooks.py
```

The script converts each published notebook to Markdown, folds `hide-input` cells into
collapsed admonitions, and adapts what only makes sense inside Jupyter: links to sibling
notebooks become links to their docs pages (or to GitHub, for notebooks that aren't
published), `data/` assets are copied into the docs, and a line marked
`<!-- hide-in-docs -->` is dropped from the page. Which notebooks get published, and with
which icon and tags, is set in `NOTEBOOK_META` at the top of that script.
