---
icon: lucide/list-ordered
tags:
  - tutorial
  - timeline
  - background databases
---

# Step 2 - Building the process timeline

With all the temporal information prepared, we can now instantiate our TimexLCA object.
This is just like a normal Brightway LCA object - the timing of the background databases
comes from their metadata:

```python
from bw_timex import TimexLCA

tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
)
```

The metadata is written by premise >= 2.4.9.2, or by you with
`set_database_metadata` (see [Step 1](adding_temporal_information.md)).

If your project holds several IAM scenarios, say which one you want - `bw_timex` lists
what it found rather than guessing:

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
[`TimexLCA.compare()`](scenarios.md#comparing-scenarios), which runs them into one
table.

!!! tip "Mapping the databases by hand"

    `database_dates` maps database names to dates yourself and replaces the metadata
    entirely:

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
