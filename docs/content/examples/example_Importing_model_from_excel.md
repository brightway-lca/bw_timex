---
icon: lucide/table
tags:
  - example
  - excel
---


<div hidden data-source-edit-path="docs/content/examples/example_Importing_model_from_excel.ipynb" data-source-view-path="docs/content/examples/example_Importing_model_from_excel.ipynb"></div>
# Loading your LCA model with temporal distributions from an excel file


This notebook is essentially a short version of the [example_electric_vehicle_standalone](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/example_electric_vehicle_standalone.ipynb) example notebook, but shows how to import the foreground model from an [excel file](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/data/example_electric_vehicle_standalone.xlsx). For a more detailed explaination of how timex works, and the the different temporal distributions, please see one of the other notebooks. 


The example Excel file can be found [HERE](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/data/example_electric_vehicle_standalone.xlsx). 

The excel file is in a standard BrightWay Excel format and the various different temporal distributions,  are supported (see excel for the different supported names). But in short, each exchange in your activity gets the following additional columns (in adddition to the standard columns):

| temporal_distribution | date |value | resolution |
|---|---|---|---|
| timedelta64 | -2,-1,0	| 0.7,0.1,0.2 | Y |
| timedelta64 | -1 | 1 | Y |
| timedelta64 | -1 | 1 | Y |

The following formats are accepted:

**Relative temporal distributions:**

| temporal_distribution | date |value | resolution |
|---|---|---|---|
| timedelta64 | -2,-1,0	| 0.7,0.1,0.2 | Y |
| relative | -1 | 1 | Y |
| delta | -1 | 1 | Y |

**Easy Time Delta:**

| temporal_distribution | date | value | resolution | start | end | steps | td_kind | td_param | comment |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| easy_timedelta |    |      | m | -3	| -1 | 3	| triangular |	-2 |	
| easy_td        |    |      | Y | 14  | 16 | 3 | normal | 3 |
| easy_timedelta_distribution | | | y | 0 | 15 | 16 | | | if td_kind and td_param are left empty a uniform td is assumed |


Analoguesly absolute temporal distritbutions are supported. 



```python { .notebook-cell }
# Set up a bw project

import bw2data as bd

bd.projects.set_current("electric_vehicle_standalone_excel")
```


```python { .notebook-cell }
# Fresh start
for db in list(bd.databases):
    del bd.databases[db]
```


```python { .notebook-cell }
# Add some background databases

biosphere = bd.Database("biosphere")
biosphere.register()
biosphere.write(
    {
        ("biosphere", "CO2"): {
            "type": "emission",
            "name": "carbon dioxide",
        },
    }
)

background_2020 = bd.Database("background_2020")
background_2020.register()

background_2030 = bd.Database("background_2030")
background_2030.register()

background_2040 = bd.Database("background_2040")
background_2040.register()

background_2020.write({})
background_2030.write({})
background_2040.write({})

background_databases = [
    background_2020,
    background_2030,
    background_2040,
]
```

    100%|██████████| 1/1 [00:00<00:00, 7256.58it/s]

    17:01:00+0200 [info     ] Vacuuming database            


    


We now create some very simple processes within these databases. These process get only one aggregated CO2-emission each. The amounts of these emissions change over time.


```python { .notebook-cell }
process_co2_emissions = {
    "glider": (10, 5, 2.5), # for 2020, 2030 and 2040
    "powertrain": (20, 10, 7.5),
    "battery": (10, 5, 4),
    "electricity": (0.5, 0.25, 0.075),
    "glider_eol": (0.01, 0.0075, 0.005),
    "powertrain_eol": (0.01, 0.0075, 0.005),
    "battery_eol": (1, 0.5, 0.25),
}

node_co2 = biosphere.get("CO2")

for component_name, gwis in process_co2_emissions.items():
    for database, gwi in zip(background_databases, gwis):
        database.new_node(component_name, name=component_name, location="somewhere").save()
        component = database.get(component_name)
        component["reference product"] = component_name        
        component.save()
        production_amount = -1 if "eol" in component_name else 1
        component.new_edge(input=component, amount=production_amount, type="production").save()
        component.new_edge(input=node_co2, amount=gwi, type="biosphere").save()

# register the databases
for db in bd.databases:
    bd.Database(db).process()   
    
```

## Case study setup


In this study, we consider the following production system for our ev. Purple boxes are foreground, cyan boxes are background (i.e., ecoinvent/premise).

```mermaid
flowchart LR
    glider_production(glider production):::ei-->ev_production
    powertrain_production(powertrain production):::ei-->ev_production
    battery_production(battery production):::ei-->ev_production
    ev_production(ev production):::fg-->driving
    electricity_generation(electricity generation):::ei-->driving
    driving(driving):::fg-->used_ev
    used_ev(used ev):::fg-->glider_eol(glider eol):::ei
    used_ev-->powertrain_eol(powertrain eol):::ei
    used_ev-->battery_eol(battery eol):::ei

    classDef ei color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#9c5ffd, stroke:none;
```

### Importing the product system from Excel

As an alternative to generating your temporal foreground system in code as above, you can also import the case study processes from a BW25-excel file.
You need `bw2io > 0.9.14`, which supports the import of `TemporalDistributions` in the ExcelImporter or CSVImporter. You can consult the sample excel file under notebooks/data for valid input formats for the TDs.

Please make sure you created and processed the biosphere and the background databases for 2020, 2030 and 2040 using the code above.


```python { .notebook-cell }
if "foreground" in bd.databases:
    del bd.databases["foreground"] # to make sure we import the foreground from scratch from the excel file

import bw2io as bi

ei = bi.ExcelImporter("data/example_electric_vehicle_standalone.xlsx", sheet_name="easy_tds") 
ei.apply_strategies()
ei.match_database("background_2020",  fields=["name", "reference product"])
ei.match_database("biosphere", fields=["name", "categories"])

ei.statistics() #0 unique unlinked edges means that all foreground exchanges are linked and we can successfully import the database.

ei.write_database()
```

    Extracted 1 worksheets in 0.00 seconds
    Applying strategy: csv_restore_tuples
    Applying strategy: csv_restore_booleans
    Applying strategy: csv_numerize
    Applying strategy: csv_drop_unknown
    Applying strategy: csv_restore_temporal_distributions
    Applying strategy: csv_add_missing_exchanges_section
    Applying strategy: normalize_units
    Applying strategy: strip_biosphere_exc_locations
    Applying strategy: set_code_by_activity_hash
    Applying strategy: link_iterable_by_fields
    Applying strategy: assign_only_product_as_production
    Applying strategy: link_technosphere_by_activity_hash
    Applying strategy: drop_falsey_uncertainty_fields_but_keep_zeros
    Applying strategy: convert_uncertainty_types_to_integers
    Applying strategy: convert_activity_parameters_to_list
    Applied 15 strategies in 0.02 seconds
    Applying strategy: link_iterable_by_fields
    Applying strategy: link_iterable_by_fields
    Graph statistics for `foreground` importer:
    3 graph nodes:
    	None: 3
    12 graph edges:
    	technosphere: 9
    	production: 3
    12 edges to the following databases:
    	background_2020: 7
    	foreground: 5
    0 unique unlinked edges (0 total):
    
    
    17:01:00+0200 [warning  ] Not able to determine geocollections for all datasets. This database is not ready for regionalization.


    100%|██████████| 3/3 [00:00<00:00, 4463.61it/s]

    17:01:00+0200 [info     ] Vacuuming database            
    Created database: foreground


    


let's check if the ExcelImporter imported the `TemporalDistributions` correctly:


```python { .notebook-cell }
driving = bd.get_node(database="foreground", name="driving an electric vehicle")
ev_to_driving = next(exc for exc in driving.technosphere() if exc.input["name"] == "electricity")

print(ev_to_driving["temporal_distribution"].date) # original resolution was timedelta64[M], which gets converted to seconds
print(ev_to_driving["temporal_distribution"].amount)
print(ev_to_driving["temporal_distribution"].date.dtype)

ev_to_driving["temporal_distribution"].graph(resolution="M")
```

    [        0  31556952  63113904  94670856 126227808 157784760 189341712
     220898664 252455616 284012568 315569520 347126472 378683424 410240376
     441797328 473354280]
    [0.0625 0.0625 0.0625 0.0625 0.0625 0.0625 0.0625 0.0625 0.0625 0.0625
     0.0625 0.0625 0.0625 0.0625 0.0625 0.0625]
    timedelta64[s]





    <Axes: xlabel='Time (Months)', ylabel='Amount'>




    
![png](example_Importing_model_from_excel_files/output_13_2.png)
    



```python { .notebook-cell }
ev_to_driving["temporal_distribution"]
```




    TemporalDistribution instance with 16 values and total: 1



### Add a characterization method

Finally, we need some characterization method. Again, this is just a simple made-up one:


```python { .notebook-cell }
bd.Method(("GWP", "example")).write(
    [
        (("biosphere", "CO2"), 1),
    ]
)
```

## LCA using `bw_timex`


Now that the data is set up, we can get startet with the actual time-explicit LCA. As usual, we need to select a method first:


```python { .notebook-cell }
method = ("GWP", "example")
```

`bw_timex` needs to know the representative time of the databases:


```python { .notebook-cell }
from datetime import datetime

database_dates = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "background_2040": datetime.strptime("2040", "%Y"),
    "foreground": "dynamic",  # flag databases that should be temporally distributed with "dynamic"
}
```

Now, we can instantiate a `TimexLCA`. It's structure is similar to a normal `bw2calc.LCA`, but with the additional argument `database_dates`.

Not sure about the required inputs? Check the documentation using `?`. All our classes and methods have docstrings!


```python { .notebook-cell }
from bw_timex import TimexLCA
```

Let's create a `TimexLCA` object for our EV life cycle:


```python { .notebook-cell }
driving = bd.get_node(database="foreground", code="driving", name="driving an electric vehicle", unit="transport over an ev lifetime")
```


```python { .notebook-cell }
# intialize the TimexLCA object with the functional unit, method, and database dates
tlca = TimexLCA({driving: 1}, method, database_dates)
# build the timeline with a temporal grouping of "month"
tlca.build_timeline(temporal_grouping="month")
# calculate the time-explicit LCI
tlca.lci()
```

    2026-06-25 17:01:01.438 | INFO     | bw_timex.timex_lca:__init__:115 - Initializing TimexLCA object...
    2026-06-25 17:01:01.438 | INFO     | bw_timex.timex_lca:__init__:131 - Calculating base LCA...
    2026-06-25 17:01:01.447 | INFO     | bw_timex.timex_lca:__init__:148 - Collecting node infos...


Let's check the dynamic invenrotry in a human readale format


```python { .notebook-cell }
tlca.create_labelled_dynamic_inventory_dataframe()
```




<table>
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>amount</th>
      <th>flow</th>
      <th>activity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2025-01-01</td>
      <td>13671.000000</td>
      <td>carbon dioxide</td>
      <td>glider</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2026-01-01</td>
      <td>6090.000000</td>
      <td>carbon dioxide</td>
      <td>battery</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2026-01-01</td>
      <td>3480.000000</td>
      <td>carbon dioxide</td>
      <td>powertrain</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2026-01-01</td>
      <td>1827.000000</td>
      <td>carbon dioxide</td>
      <td>glider</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2027-01-01</td>
      <td>3402.000000</td>
      <td>carbon dioxide</td>
      <td>glider</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2027-01-01</td>
      <td>632.812500</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2028-01-01</td>
      <td>585.937500</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2029-01-01</td>
      <td>539.062500</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2030-01-01</td>
      <td>492.187500</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2031-01-01</td>
      <td>452.343750</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>10</th>
      <td>2032-01-01</td>
      <td>419.531251</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>11</th>
      <td>2033-01-01</td>
      <td>386.718751</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>12</th>
      <td>2034-01-01</td>
      <td>353.906252</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>13</th>
      <td>2035-01-01</td>
      <td>321.093753</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>14</th>
      <td>2036-01-01</td>
      <td>288.281253</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>15</th>
      <td>2037-01-01</td>
      <td>255.468754</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>16</th>
      <td>2038-01-01</td>
      <td>222.656254</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>17</th>
      <td>2039-01-01</td>
      <td>189.843755</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>18</th>
      <td>2040-01-01</td>
      <td>157.031255</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>22</th>
      <td>2041-01-01</td>
      <td>140.625006</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>19</th>
      <td>2041-01-01</td>
      <td>23.225060</td>
      <td>carbon dioxide</td>
      <td>battery_eol</td>
    </tr>
    <tr>
      <th>21</th>
      <td>2041-01-01</td>
      <td>1.393504</td>
      <td>carbon dioxide</td>
      <td>glider_eol</td>
    </tr>
    <tr>
      <th>20</th>
      <td>2041-01-01</td>
      <td>0.132715</td>
      <td>carbon dioxide</td>
      <td>powertrain_eol</td>
    </tr>
    <tr>
      <th>26</th>
      <td>2042-01-01</td>
      <td>140.625006</td>
      <td>carbon dioxide</td>
      <td>electricity</td>
    </tr>
    <tr>
      <th>23</th>
      <td>2042-01-01</td>
      <td>23.549881</td>
      <td>carbon dioxide</td>
      <td>battery_eol</td>
    </tr>
    <tr>
      <th>25</th>
      <td>2042-01-01</td>
      <td>1.412993</td>
      <td>carbon dioxide</td>
      <td>glider_eol</td>
    </tr>
    <tr>
      <th>24</th>
      <td>2042-01-01</td>
      <td>0.134571</td>
      <td>carbon dioxide</td>
      <td>powertrain_eol</td>
    </tr>
    <tr>
      <th>27</th>
      <td>2043-01-01</td>
      <td>23.225060</td>
      <td>carbon dioxide</td>
      <td>battery_eol</td>
    </tr>
    <tr>
      <th>29</th>
      <td>2043-01-01</td>
      <td>1.393504</td>
      <td>carbon dioxide</td>
      <td>glider_eol</td>
    </tr>
    <tr>
      <th>28</th>
      <td>2043-01-01</td>
      <td>0.132715</td>
      <td>carbon dioxide</td>
      <td>powertrain_eol</td>
    </tr>
  </tbody>
</table>



Now we can do all further analysis as detailed in the other example notebook [example_electric_vehicle_standalone](https://github.com/brightway-lca/bw_timex/blob/main/notebooks/example_electric_vehicle_standalone.ipynb). Below you just find the quick calculations. For the full dynamic characterization please see the linked notebook.


```python { .notebook-cell }
# Static LCIA from time-explicit LCI
tlca.static_lcia()
tlca.static_score   #kg CO2-eq
```




    34122.72503901273



At this point, we can already compare these time-explicit results to the results of an "ordinary", completely static LCA. These already exist within the TimexLCA class, originally to set the priorities for the graph traversal:


```python { .notebook-cell }
# compare to fully static non time-explicit LCIA score
tlca.base_lca.score
```




    28089.199999794364


