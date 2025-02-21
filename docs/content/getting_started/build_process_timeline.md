# Step 2 - Building the process timeline

With all the temporal information prepared, we can now instantiate our TimexLCA object. This is very similar to a normal Brightway LCA object, but with the additional argument of our `database_dates`:

```python
from bw_timex import TimexLCA

tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    database_dates=database_dates,
)
```

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
