---
icon: lucide/trending-up
tags:
  - example
  - dynamic characterization
---


<div hidden data-source-edit-path="docs/content/examples/example_simple_dynamic_characterization.ipynb" data-source-view-path="docs/content/examples/example_simple_dynamic_characterization.ipynb"></div>
# Dynamic characterization
This notebook is meant to explain the options for dynamic characterization in `bw_timex`.
In this example, there is a very simple database containing only one node with a pulse emission of CH4, occuring at a 10 year delay.

Further down, there is an example with multiple greenhouse gases.



```python
import bw2data as bd
import numpy as np
from bw_temporalis import TemporalDistribution

project_name = "timex_example_dynamic_characterization"
if project_name in bd.projects:
    bd.projects.delete_project(project_name) # making sure to start from scratch
    bd.projects.purge_deleted_directories()

bd.projects.set_current(project_name)

bd.Database("temporalis-bio").write(
    {
        ("temporalis-bio", "CH4"): {  # only biosphere flow is CH4
            "type": "emission",
            "name": "methane",
            "temporalis code": "ch4",
        },
    }
)

bd.Database("test").write(  # dummy system containing 1 activity
    {
        ("test", "A"): {
            "name": "A",
            "location": "somewhere",
            "reference product": "a",
            "exchanges": [
                {
                    "amount": 1,
                    "type": "production",
                    "input": ("test", "A"),
                },
                {
                    "amount": 1,
                    "type": "biosphere",
                    "input": ("temporalis-bio", "CH4"),
                    "temporal_distribution": TemporalDistribution(
                        date=np.array([10], dtype="timedelta64[Y]"),
                        amount=np.array([1]),
                    ),  # emission of CH4 10 years after execution of process A
                },
            ],
        },
    }
)

bd.Method(("GWP", "example")).write(
    [
        (("temporalis-bio", "CH4"), 29.8),  # GWP100 from IPCC AR6
    ]
)
```

    100%|██████████| 1/1 [00:00<00:00, 4369.07it/s]


    Vacuuming database 
    Not able to determine geocollections for all datasets. This database is not ready for regionalization.


    100%|██████████| 1/1 [00:00<00:00, 17189.77it/s]

    Vacuuming database 


    


We select the demand and the method and calculate a LCA with `bw_timex`



```python
demand = {("test", "A"): 1}
gwp = ("GWP", "example")
```


```python
from bw_timex import TimexLCA

tlca = TimexLCA(demand, gwp)
```

    /Users/timodiepers/Documents/Coding/timex/bw_timex/bw_timex.py:100: UserWarning: No database_dates provided. Treating the databases containing the functional unit as dynamic. No remapping to time explicit databases will be done.
      warnings.warn(



```python
tlca.build_timeline()
```

    Calculation count: 0


    /Users/timodiepers/Documents/Coding/timex/bw_timex/bw_timex.py:182: UserWarning: No edge filter function provided. Skipping all edges within background databases.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/timeline_builder.py:191: Warning: No time-explicit databases are provided. Mapping to time-explicit databases is not possible.
      warnings.warn(





<div class="md-typeset__table"><table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date_producer</th>
      <th>producer_name</th>
      <th>date_consumer</th>
      <th>consumer_name</th>
      <th>amount</th>
      <th>temporal_market_shares</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2024-01-01</td>
      <td>A</td>
      <td>2024-01-01</td>
      <td>-1</td>
      <td>1.0</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
tlca.lci()
```


```python
tlca.static_lcia()
tlca.static_score
```




    29.799999237060547



## Dynamic characterization

While users can profile their own dynamic LCIA methods, `bw_timex` has implemenented default dynamic characterization functions for 2 climate change metrics, based on IPCC AR6:
- radiative forcing [W/m2]
- Global warming potential (GWP) [kg CO2eq],

For these dynamic LCIA metrics, users can select the length of the considered time horizon (`time_horizon`) and whether it is a fixed time horizon (`fixed_time_horizon`).

Conventional metrics usually consider a time horizon of 100 years, but this has been more of a value choice.
Fixed time horizon means that the time horizon for all emissions (no matter when they occur) starts counting at the time of the functional unit, resulting in shorter time horizons for emissions occuring later. This approach has been proposed by [Levasseur et al. 2010](https://pubs.acs.org/doi/10.1021/es9030003) to harmonize the time frame chosen for the analysis and the time period covered by the LCA results.
If the time horizon is not fixed (this is what conventional impact assessment factors assume), it starts counting from the timing of the emission.


First, import the dynamic characterization function of CH4 and have a look at it:



```python
from bw_timex.dynamic_characterization import characterize_ch4

characterize_ch4?
```

    Signature: characterize_ch4(series, period: int = 100, cumulative=False) -> pandas.core.frame.DataFrame
    Docstring:
    Calculate the cumulative or marginal radiative forcing (CRF) from CH4 for each year in a given period. 
    
    Based on characterize_methane from bw_temporalis, but updated numerical values from IPCC AR6 Ch7 & SM.
    
    This DOES include indirect effects of CH4 on ozone and water vapor, but DOES NOT include the decay to CO2. 
    For more info on that, see the deprecated version of bw_temporalis.
    
    If `cumulative` is True, the cumulative CRF is calculated. If `cumulative` is False, the marginal CRF is calculated.
    Takes a single row of the TimeSeries Pandas DataFrame (corresponding to a set of (`date`/`amount`/`flow`/`activity`).
    For earch year in the given period, the CRF is calculated.
    Units are watts/square meter/kilogram of CH4.
    
    Parameters
    ----------
    series : array-like
        A single row of the TimeSeries dataframe.
    period : int, optional
        Time period for calculation (number of years), by default 100
    cumulative : bool, optional
        Should the RF amounts be summed over time?
    
    Returns
    -------
    A TimeSeries dataframe with the following columns:
    - date: datetime64[s]
    - amount: float
    - flow: str
    - activity: str
    
    See also
    --------
    Joos2013: Relevant scientific publication on CRF: https://doi.org/10.5194/acp-13-2793-2013
    Schivley2015: Relevant scientific publication on the numerical calculation of CRF: https://doi.org/10.1021/acs.est.5b01118
    Forster2023: Updated numerical values from IPCC AR6 Chapter 7 (Table 7.15): https://doi.org/10.1017/9781009157896.009
    File:      ~/Documents/Coding/timex/bw_timex/dynamic_characterization.py
    Type:      function

Then, we can create the characterization_functions where we map the function to the corresponding flow via its ID:


```python
characterization_functions_ch4 = {
    bd.get_node(code="CH4").id: characterize_ch4,
}
```

Now, the dynamic LCIA can be calculated:


```python
tlca.dynamic_lcia(
    metric="radiative_forcing",
    fixed_time_horizon=False,
    characterization_functions=characterization_functions_ch4,
)
```




<div class="md-typeset__table"><table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>amount</th>
      <th>flow</th>
      <th>flow_name</th>
      <th>activity</th>
      <th>activity_name</th>
      <th>amount_sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2034-12-31 16:01:12</td>
      <td>1.922234e-13</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>1.922234e-13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2035-12-31 21:50:24</td>
      <td>1.766044e-13</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>3.688278e-13</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2036-12-31 03:39:36</td>
      <td>1.622546e-13</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>5.310824e-13</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2037-12-31 09:28:48</td>
      <td>1.490707e-13</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>6.801531e-13</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2038-12-31 15:18:00</td>
      <td>1.369581e-13</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>8.171112e-13</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>94</th>
      <td>2128-12-31 11:06:00</td>
      <td>6.670712e-17</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>2.364951e-12</td>
    </tr>
    <tr>
      <th>95</th>
      <td>2129-12-31 16:55:12</td>
      <td>6.128689e-17</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>2.365012e-12</td>
    </tr>
    <tr>
      <th>96</th>
      <td>2130-12-31 22:44:24</td>
      <td>5.630707e-17</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>2.365068e-12</td>
    </tr>
    <tr>
      <th>97</th>
      <td>2132-01-01 04:33:36</td>
      <td>5.173189e-17</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>2.365120e-12</td>
    </tr>
    <tr>
      <th>98</th>
      <td>2132-12-31 10:22:48</td>
      <td>4.752846e-17</td>
      <td>1</td>
      <td>methane</td>
      <td>2</td>
      <td>(test, A)</td>
      <td>2.365167e-12</td>
    </tr>
  </tbody>
</table>
<p>99 rows × 7 columns</p>
</div>




```python
tlca.plot_dynamic_characterized_inventory()
```


    
![png](example_simple_dynamic_characterization_files/output_15_0.png)
    


CH4 has a half-life time of 8.6 years, so the decay curve is quite steep and it doesn't cause much atmospheric warming in later years.



```python
print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon} years)]"
)
```

    characterized dynamic score: 2.3651673669270527e-12 [radiative_forcing (100 years)]


If we evaluate radiative forcing over a shorter time horizon, the score gets smaller. This is equivalent to taking a shorter integral of the radiative forcing curve above.



```python
tlca.dynamic_lcia(
    metric="radiative_forcing",
    fixed_time_horizon=False,
    time_horizon=20,
    characterization_functions=characterization_functions_ch4,
)

print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon} years)]"
)
```

    characterized dynamic score: 1.892909832719887e-12 [radiative_forcing (20 years)]


With `fixed_time_horizon = True`, we evaluate all emissions from time of the functional unit, regardless when they actually occur. As our CH4 emission occurs 10 year later than the functional unit, this means that it is only assessed for 90 years (100 years time horizon - 10 years of delay in emission). As CH4 is barely causing warming between year 90 to 100, this doesn't change the overall score too much, but can cause larger differences for more long-lived GHGs.



```python
tlca.dynamic_lcia(
    metric="radiative_forcing",
    fixed_time_horizon=True,
    time_horizon=100,
    characterization_functions=characterization_functions_ch4,
)

print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon} years)]"
)

tlca.plot_dynamic_characterized_inventory()
```

    characterized dynamic score: 2.3644506236849053e-12 [radiative_forcing (100 years)]



    
![png](example_simple_dynamic_characterization_files/output_21_1.png)
    


Note that the tail of the curve stops in 21**2**4 (100 years after the functional unit), in 21**3**4 (100 years after the emission) in the figure a few cells above.


### Global warming potential (GWP)


GWP describes the warming of a GHG in comparison to that of the reference gas CO2. As such, it divides the integral of radiative forcing of a GHG over a certain time horizon by the integral of radiative forcing of the reference gas CO2 over the same time horizon:

![image.png](example_simple_dynamic_characterization_files/image.png)

[KTH, 2014](https://www.energy.kth.se/applied-thermodynamics/key-research-areas/heating-systems/low-gwp-news/nagot-om-hur-gwp-varden-bestams-1.474589)


GWP can be calculated in `bw_timex` with the same options as radiative forcing:

- time horizon can vary (default 100 years)
- fixed or flexible time horizon


Let's evaluate GWP20:



```python
tlca.dynamic_lcia(
    metric="GWP",
    time_horizon=20,
    characterization_functions=characterization_functions_ch4,
)

print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon})]"
)
```

    characterized dynamic score: 81.38137397247695 [GWP (20)]


    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(


With `fixed_time_horizon = True` and `time_horizon = 20` years, the difference in results is substantial, as the 10-years delayed CH4 emission is only counted for 10 years (20 year time horizon starting at the functional unit - 10 year emissiond delay).



```python
tlca.dynamic_lcia(
    metric="GWP",
    fixed_time_horizon=True,
    time_horizon=20,
    characterization_functions=characterization_functions_ch4,
)

print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon})]"
)
```

    characterized dynamic score: 54.271464562232225 [GWP (20)]


    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(


## Example with more GHGs

Lastly, let's look at a system with multiple GHGs spread over time.



```python
def write_database_multi_emission():

    project_name = "__test_database1__"
    if project_name in bd.projects:
        bd.projects.delete_project(project_name)
        bd.projects.purge_deleted_directories()

    bd.projects.set_current(project_name)

    bd.Database("temporalis-bio").write(
        {
            ("temporalis-bio", "CH4"): {
                "type": "emission",
                "name": "methane",
                "temporalis code": "ch4",
            },
            ("temporalis-bio", "CO2"): {
                "type": "emission",
                "name": "carbon dioxide",
                "temporalis code": "co2",
            },
            ("temporalis-bio", "N2O"): {
                "type": "emission",
                "name": "nitrious oxide",
                "temporalis code": "n2o",
            },
        }
    )

    bd.Database(
        "test"
    ).write(  # dummy system containing 1 activity with multiple emissions
        {
            ("test", "A"): {
                "name": "A",
                "location": "somewhere",
                "reference product": "a",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": ("test", "A"),
                    },
                    {
                        "amount": 0.5,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CH4"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([10, 15], dtype="timedelta64[Y]"),
                            amount=np.array([0.5, 0.5]),
                        ),  # emission of CH4 10 and 15 years after execution of process A
                    },
                    {
                        "amount": 20,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "CO2"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([-10, 5], dtype="timedelta64[Y]"),
                            amount=np.array([0.5, 0.5]),
                        ),  # emission of CO2 10 and 5 years before
                    },
                    {
                        "amount": 0.05,
                        "type": "biosphere",
                        "input": ("temporalis-bio", "N2O"),
                        "temporal_distribution": TemporalDistribution(
                            date=np.array([0], dtype="timedelta64[Y]"),
                            amount=np.array([1]),
                        ),  # emission of N2O at the same time
                    },
                ],
            },
        }
    )

    bd.Method(("GWP", "example")).write(
        [
            (("temporalis-bio", "CH4"), 29.8),  # GWP100 from IPCC AR6
            (("temporalis-bio", "N2O"), 273),
            (("temporalis-bio", "CO2"), 1),
        ]
    )
```


```python
write_database_multi_emission()
```

    100%|██████████| 3/3 [00:00<00:00, 46091.25it/s]


    Vacuuming database 
    Not able to determine geocollections for all datasets. This database is not ready for regionalization.


    100%|██████████| 1/1 [00:00<00:00, 25420.02it/s]

    Vacuuming database 


    


Import additional default dynamic characterization function for N2O and calculate time-explicit LCA.



```python
from bw_timex.dynamic_characterization import characterize_n2o, characterize_co2

demand = {("test", "A"): 1}
gwp = ("GWP", "example")

tlca = TimexLCA(demand, gwp)
tlca.build_timeline()
tlca.lci()
tlca.static_lcia()
```

    Calculation count: 0


    /Users/timodiepers/Documents/Coding/timex/bw_timex/bw_timex.py:100: UserWarning: No database_dates provided. Treating the databases containing the functional unit as dynamic. No remapping to time explicit databases will be done.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/bw_timex.py:182: UserWarning: No edge filter function provided. Skipping all edges within background databases.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/timeline_builder.py:191: Warning: No time-explicit databases are provided. Mapping to time-explicit databases is not possible.
      warnings.warn(


match dynamic charcterization functions to biosphere flows:



```python
characterization_functions = {
    bd.get_node(code="CH4").id: characterize_ch4,
    bd.get_node(code="CO2").id: characterize_co2,
    bd.get_node(code="N2O").id: characterize_n2o,
}
```

### Radiative forcing:



```python
tlca.dynamic_lcia(
    metric="radiative_forcing",
    time_horizon=100,
    characterization_functions=characterization_functions,
)
tlca.plot_dynamic_characterized_inventory()

print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon})]"
)
```


    
![png](example_simple_dynamic_characterization_files/output_38_0.png)
    


    characterized dynamic score: 4.121262160040557e-12 [radiative_forcing (100)]



```python
tlca.plot_dynamic_characterized_inventory(cumsum=True)
```


    
![png](example_simple_dynamic_characterization_files/output_39_0.png)
    


### Global warming potential:



```python
tlca.dynamic_lcia(
    metric="GWP",
    time_horizon=100,
    characterization_functions=characterization_functions,
)
tlca.plot_dynamic_characterized_inventory()
print(
    f"characterized dynamic score: {tlca.dynamic_score} [{tlca.metric} ({tlca.time_horizon})]"
)
```

    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(



    
![png](example_simple_dynamic_characterization_files/output_41_1.png)
    


    characterized dynamic score: 46.53439734089549 [GWP (100)]


Ultimately, let's compare how changing the length of the time horizon affects the GWP results:



```python
import pandas as pd
import matplotlib.pyplot as plt

gwp_flexible_TH = {}
gwp_fixed_TH = {}

for time_horizon in range(20, 110, 10):  # 20 to 100 TH in steps of 10 years
    tlca.dynamic_lcia(
        metric="GWP",
        time_horizon=time_horizon,
        fixed_time_horizon=True,
        characterization_functions=characterization_functions,
    )
    gwp_fixed_TH[time_horizon] = tlca.dynamic_score

    tlca.dynamic_lcia(
        metric="GWP",
        time_horizon=time_horizon,
        fixed_time_horizon=False,
        characterization_functions=characterization_functions,
    )
    gwp_flexible_TH[time_horizon] = tlca.dynamic_score

# add values for 500 years:
tlca.dynamic_lcia(
    metric="GWP",
    time_horizon=500,
    fixed_time_horizon=True,
    characterization_functions=characterization_functions,
)
gwp_fixed_TH[500] = tlca.dynamic_score

tlca.dynamic_lcia(
    metric="GWP",
    time_horizon=500,
    fixed_time_horizon=False,
    characterization_functions=characterization_functions,
)
gwp_flexible_TH[500] = tlca.dynamic_score

df = pd.DataFrame(
    {
        "Time horizon": list(gwp_fixed_TH.keys()),
        "GWP (fixed time horizon)": list(gwp_fixed_TH.values()),
        "GWP (flexible time horizon)": list(gwp_flexible_TH.values()),
    }
)
```

    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(
    /Users/timodiepers/Documents/Coding/timex/bw_timex/dynamic_characterization.py:122: UserWarning: Using bw_timex's default CO2 characterization function for GWP reference.
      warnings.warn(



```python
df
```




<div class="md-typeset__table"><table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Time horizon</th>
      <th>GWP (fixed time horizon)</th>
      <th>GWP (flexible time horizon)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>20</td>
      <td>56.289341</td>
      <td>74.144077</td>
    </tr>
    <tr>
      <th>1</th>
      <td>30</td>
      <td>61.835629</td>
      <td>66.502783</td>
    </tr>
    <tr>
      <th>2</th>
      <td>40</td>
      <td>59.907858</td>
      <td>60.944071</td>
    </tr>
    <tr>
      <th>3</th>
      <td>50</td>
      <td>56.939684</td>
      <td>56.883182</td>
    </tr>
    <tr>
      <th>4</th>
      <td>60</td>
      <td>54.201931</td>
      <td>53.825761</td>
    </tr>
    <tr>
      <th>5</th>
      <td>70</td>
      <td>51.887404</td>
      <td>51.439676</td>
    </tr>
    <tr>
      <th>6</th>
      <td>80</td>
      <td>49.952471</td>
      <td>49.511978</td>
    </tr>
    <tr>
      <th>7</th>
      <td>90</td>
      <td>48.317374</td>
      <td>47.906378</td>
    </tr>
    <tr>
      <th>8</th>
      <td>100</td>
      <td>46.912377</td>
      <td>46.534397</td>
    </tr>
    <tr>
      <th>9</th>
      <td>500</td>
      <td>30.052980</td>
      <td>29.977030</td>
    </tr>
  </tbody>
</table>
</div>



One can see that a longer time horizon leads to smaller differences between fixed (time horizon starts at FU for all flows) and flexible time horizons (time horizon starts at each emissions seperately). An increase in time horizon also leads to lower overall scores, because the system contains multiple short-lived GHGs, such as CH4 and N2O, whose CO2-equivalence value decreases when assessing longer time horizons.

