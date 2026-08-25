---
icon: lucide/database
tags:
  - premise
  - background database
---

# Creating `premise`-Databases On-the-fly

If your project does not hold any `premise` databases yet, `bw_timex` can build them
with premise on-the-fly. If you only want to create the databases, call the
standalone shorthand `ensure_scenario_databases`:

```python
from bw_timex import ensure_scenario_databases

database_dates = ensure_scenario_databases(
    {
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "system_model": "cutoff",
        "ecoinvent_version": "3.12",
        "years": [2020, 2030, 2040],
    },
    premise_key="dummy_premise_decryption_key",              # or $PREMISE_KEY
    ecoinvent_credentials=("dummy_user", "dummy_password"),  # or $ECOINVENT_USERNAME / _PASSWORD
)
```

The returned `database_dates` mapping contains the databases found or built and
the point in time each represents. 

You can also trigger the same builder while
creating a `TimexLCA` object by adding the years to the scenario and passing
`create_missing=True`:

```python
tlca = TimexLCA(
    demand={("foreground", "ev"): 1},
    method=("EF v3.1", "climate change", "global warming potential (GWP100)"),
    scenario={
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "system_model": "cutoff",
        "ecoinvent_version": "3.12",
        "years": [2020, 2030, 2040],
    },
    create_missing=True,
    premise_key="dummy_premise_decryption_key",              # or $PREMISE_KEY
    ecoinvent_credentials=("dummy_user", "dummy_password"),  # or $ECOINVENT_USERNAME / _PASSWORD
)
```
