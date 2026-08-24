---
icon: lucide/list-ordered
tags:
  - tutorial
  - timeline
  - background databases
---

# Step 2 - Building the process timeline

With all the temporal information prepared, we can now instantiate our TimexLCA object.
This is just like a normal Brightway LCA object:

```python
from bw_timex import TimexLCA

tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
)
```

If you have multiple background databases representing different scenarios in your Brightway-project, 
you additionally need to specify which scenario you wish to assess. 

```python
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    scenario={"pathway": "SSP2-PkBudg500"},
)
```

Any metadata key filters (`iam_model`, `pathway`, `system_model`, ...), and databases
that don't carry it - your foreground, your own vintages - are kept. To compare several
scenarios, hand them all to
[`TimexLCA.compare()`](configured_runs.md#comparing-scenarios), which runs them into one
table.

!!! tip "Mapping the databases by hand"

    You can also explicitly pass `database_dates` to `TimexLCA`, not requiring any database metadata:

    ```python
    tlca = TimexLCA(demand, method, database_dates={
        "background": datetime(2020, 1, 1),
        "background_2030": datetime(2030, 1, 1),
        "foreground": "dynamic",
    })
    ```

    Handy to restrict a calculation to a subset of your project.

Using our new `tlca` object, we can now build the timeline of processes that leads to our functional unit "A". If not specified otherwise, it's assumed that the demand occurs in the current year. In our case, we're specifying the time of demand to the year 2024, with the attribute 'starting_datetime`.. Building the timeline is very simple:
```python
tlca.build_timeline(starting_datetime=datetime.strptime("2024-01-01", "%Y-%m-%d"))
```

The timeline that is returned looks like this:

| date_producer | producer_name | date_consumer | consumer_name | amount | temporal_market_shares                          |
|---------------|---------------|---------------|---------------|--------|------------------------------------------------|
| 2022-01-01    | B             | 2024-01-01    | A             | 0.9    | {'background': 0.8, 'background_2030': 0.2}    |
| 2024-01-01    | B             | 2024-01-01    | A             | 1.5    | {'background': 0.6, 'background_2030': 0.4}    |
| 2024-01-01    | A             | 2024-01-01    | -1            | 1.0    | None                                           |
| 2028-01-01    | B             | 2024-01-01    | A             | 0.6    | {'background': 0.2, 'background_2030': 0.8}    |

Here, we can see how much of an exchange happens at what point in time. Additionally, the "temporal_market_shares" already tell us what share of an exchange should come from which database. With this info, we can calculate our time-explicit LCI in the next step.
