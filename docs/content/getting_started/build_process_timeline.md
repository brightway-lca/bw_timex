# Step 2 - Building the process timeline

With all the temporal information prepared, we can now instantiate our TimexLCA object. This is very similar to a normal Brightway LCA object, but with the additional argument of our `database_date_dict`:

```python
from bw_timex import TimexLCA

tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    database_date_dict=database_date_dict,
)
```

Using our new `tlca` object, we can now build the timeline of processes that leads to our functional unit, "A". If not specified otherwise, it's assumed that the demand occurs in the current year, which is 2024 at the time of writing this. Actually building the timeline is now very simple on the user-side:
```python
tlca.build_timeline()
```

The timeline that is returned looks like this:

| date_producer | producer_name | date_consumer | consumer_name | amount | interpolation_weights                          |
|---------------|---------------|---------------|---------------|--------|------------------------------------------------|
| 2022-01-01    | B             | 2024-01-01    | A             | 0.9    | {'background': 0.8, 'background_2030': 0.2}    |
| 2024-01-01    | B             | 2024-01-01    | A             | 1.5    | {'background': 0.6, 'background_2030': 0.4}    |
| 2024-01-01    | A             | 2024-01-01    | -1            | 1.0    | None                                           |
| 2028-01-01    | B             | 2024-01-01    | A             | 0.6    | {'background': 0.2, 'background_2030': 0.8}    |
