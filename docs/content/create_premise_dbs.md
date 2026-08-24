---
icon: lucide/database
tags:
  - premise
  - background database
---

# Creating `premise`-Databases On-the-fly

If your project does not hold any `premise` databases yet, `bw_timex` can build them
with premise on-the-fly. Add the years to the scenario and pass
`create_missing=True`, alongside the premise key and ecoinvent credentials:

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
