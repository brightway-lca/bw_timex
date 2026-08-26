---
icon: lucide/database
tags:
  - premise
  - background database
---

# Automatically Create Scenario Databases

If your project does not hold any scenario databases yet, `bw_timex` can automatically create them 
using `premise`, either in a single function call of on-the-fly in a `TimexLCA`. 

## Standalone download

If you only want to create the databases, call the
standalone shorthand `ensure_scenario_databases`. Running this requires a premise key (to be asked from `premise` maintainers) and valid ecoinvent credentials. These can be set as environment variables (`PREMISE_KEY`, `ECOINVENT_USERNAME`, `ECOINVENT_PASSWORD`) or passed to the function directly.

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

## Creating scenario databases on-the-fly

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

This can be particularly useful when using the `compare()` method:

```python
base_settings = TimexLCASettings(
    demand = {("foreground", "ev"): 1},
    method = ("ecoinvent-3.12", "EF v3.1", "climate change", "global warming potential (GWP100)"),
)

comparison = TimexLCA.compare(
    [
        replace(
            base_settings,
            scenario={
                "pathway": pathway,
                "iam_model": "remind-eu",
                "ecoinvent_version": "3.12",
                "system_model": "cutoff",
                "years": [2020, 2030, 2040],
            },
            create_missing=True,
            label=f"Pathway {pathway}",
        )
        for pathway in ("SSP2-PkBudg650", "SSP2-PkBudg1000")
    ]
)
```