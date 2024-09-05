# Theory

This section explains some of the theory behind time-explicit LCAs with `bw_timex`. In contrast to the [Getting Started section](getting_started/index.md), we explain a bit more of what`s going on in the background here. If this is still to vague for you, you can always check out our [API reference](api/index).

## Terminology
LCA terminology can be confusing sometimes. Here's an attempt of visualizing what we mean with "time-explicit LCA". Essentially, it combines dynamic LCA, where processes are spread out over time, with prospective LCA, where processes change over time:

```{image} data/dynamic_prospective_timeexplicit_dark.svg
:class: only-dark
```
```{image} data/dynamic_prospective_timeexplicit_light.svg
:class: only-light
```

## Data requirements

For a time-explicit LCA, 3 inputs are required:

1.  a static foreground system model with
2.  temporal information using the attribute `temporal_distribution` on
    technosphere or biosphere exchanges in the foreground system model,
    and
3.  a set of background databases, which must have a reference in time.

```{note}
-   The foreground system must have exchanges linked to one of the
    background databases. These exchanges at the intersection between
    foreground and background databases will be relinked by `bw_timex`.
-   Temporal distributions can occur at technosphere and biosphere
    exchanges and can be given in various forms, see
    [bw_temporalis](https://github.com/brightway-lca/bw_temporalis/tree/main),
    including absolute (e.g. 2024-03-18) or relative (e.g. 3 years
    before) types and can have different temporal resolution (down to
    seconds but later aggregation supports resolutions down to hours).
-   Temporal distributions are optional. If none are provided, no delay
    between producing and consuming process is assumed and the timing of
    the consuming process is adopted also for the producing process.
```

## Graph traversal

`bw_timex` uses the graph traversal from
[bw_temporalis](https://github.com/brightway-lca/bw_temporalis/tree/main)
to propagate the temporal information along the supply chain. An in-depth
description of how this works is available in the [brightway-docs](https://docs.brightway.dev/en/latest/content/theory/graph_traversal.html). In short,
it is a priority-first supply chain graph traversal, following the most
impactful node in the graph based on the static pre-calculated LCIA score
for a chosen impact category. Several input arguments for the graph traversal,
such as maximum calculation count or cut-off, can be passed to the `TimexLCA`
instance.

By default, only the foreground system is traversed, but nodes to be
skipped during traversal can be specified by a `edge_filter_function`.
At each process, the graph traversal uses convolution to combine the
temporal distributions of the process and the exchange it consumes into
the resoluting combined temporal distribution of the upstream producer
of the exchange.

## Building the process timeline

The graph traversal returns a timeline that lists the time of each
technosphere exchange in the temporalized foreground system. Exchanges
that flow from same producer to the same consumer within a certain
time-window (`temporal_grouping`, default is \'year\') are grouped
together. This is done to avoid countless exchanges in the timeline, as
the temporal distributions at exchange level can have temporal
resolutions down to seconds while one may not have a similar temporal
resolution for the databases. We recommend aligning `temporal_grouping`
to the temporal resolution of the available databases.

````{admonition} Example
:class admonition-example

Let's consider the following system: a process A that consumes an
exchange b from a process B, which emits an emission X and both the
exchange b and the emission X occur at a certain point in time.
```{mermaid}
flowchart LR
subgraph background[" "]
    B_2020(Process B \n 2020):::bg
    B_2030(Process B \n 2030):::bg
end

subgraph foreground[" "]
    A(Process A):::fg
end

subgraph biosphere[" "]
    CO2:::b
end

B_2020-->|"amounts: [30%,50%,20%] * 3 kg\n dates:[-2,0,+4]" years|A
A-.->|"amounts: [60%, 40%] * 5 kg\n dates: [0,+1]" years|CO2
B_2020-.->|"amounts: [100%] * <span style='color:#9c5ffd'><b>11 kg</b></span>\n dates:[0]" years|CO2
B_2030-.->|"amounts: [100%] * <span style='color:#9c5ffd'><b>7 kg</b></span>\n dates:[0]" years|CO2

classDef bg color:#222832, fill:#3fb1c5, stroke:none;
classDef fg color:#222832, fill:#3fb1c5, stroke:none;
classDef b color:#222832, fill:#9c5ffd, stroke:none;
style foreground fill:none, stroke:none;
style background fill:none, stroke:none;
style biosphere fill:none, stroke:none;

```

The resulting timeline looks like this:
| date_producer | producer_name | date_consumer | consumer_name | amount | interpolation_weights                          |
|---------------|---------------|---------------|---------------|--------|------------------------------------------------|
| 2022-01-01    | B             | 2024-01-01    | A             | 0.9    | {'background': 0.8, 'background_2030': 0.2}    |
| 2024-01-01    | B             | 2024-01-01    | A             | 1.5    | {'background': 0.6, 'background_2030': 0.4}    |
| 2024-01-01    | A             | 2024-01-01    | -1            | 1.0    | None                                           |
| 2028-01-01    | B             | 2024-01-01    | A             | 0.6    | {'background': 0.2, 'background_2030': 0.8}    |
````

## Time mapping

Based on the timing of the processes in the timeline, `bw_timex` matches
the processes at the intersection between foreground and background to
the best available background databases. Available matching strategies
are closest database or linear interpolation between two closest
databases based on temporal proximity. The new best-fitting background
producer(s) are mapped on the same name, reference product and location
as the old background producer.

## Modifying the matrices

`bw_timex` now modifies the technopshere and biosphere matrices using
`datapackages` from
[bw_processing](https://github.com/brightway-lca/bw_processing?tab=readme-ov-file).

### Technosphere matrix modifications

1.  For each temporalized process in the timeline, a new process copy is
    created, which links to its new temporalized producers and
    consumers. The timing of the processes is stored in the
    `activity_time_mapping_dict`, which maps the process ids to process
    timing.
2.  For those processes linking to the background databases, `bw_timex`
    relinks the exchanges to the new producing processes from the
    best-fitting background database(s).

### Biosphere matrix modifications

Depending on the user\'s choice, two different biosphere matrices are
created:

1.  If `TimexLCA.lci()` is executed, the \'static\' biosphere matrix is
    expanded, by adding the original biosphere flows for the new
    temporalized process copies. With this, static LCI with inputs from
    the time-explicit databases are calculated and stored in
    `TimexLCA.lca.inventory`.
2.  If `TimexLCA.lci(build_dynamic_biosphere=True)` is executed, a
    \'dynamic\' biosphere matrix is created, which next to the links to
    LCI from the time-explicit databases also contains the timing of
    emissions. `build_dynamic_biosphere=True` is the default, so it has
    to be set to `False` to skip this step. The matrix
    `TimexLCA.dynamic_inventory` and the more readable dataframe
    `TimexLCA.dynamic_inventory_df` contain the emissions of the system
    per biosphere flow including its timestamp and its emitting process.

```{admonition} Example
:class: admonition-example
For the simple system above, a schematic representation of the matrix
modifications looks like this:
~~~{image} data/matrix_light.svg
:class: only-light
:height: 300px
:align: center
~~~
~~~{image} data/matrix_stuff_dark.svg
:class: only-dark
:align: center
~~~
```

## Static or dynamic impact assessment

`bw_timex` allows to use conventional static impact assessment methods,
which are executed using `TimexLCA.static_lcia()`.

To take advantage of the detailed temporal information at the inventory
level, dynamic LCIA can be applied, using `TimexLCA.dynamic_lcia()`.
Users can define or import their own dynamic LCIA functions. Out of the
box, we provide dynamic LCIA functions for the climate change metrics
\'radiative forcing\' and \'global warming potential (GWP)\' for all
greenhouse gases in the [IPCC AR6 report Chapter 7 Table
7.SM.7](https://www.ipcc.ch/report/ar6/wg1/chapter/chapter-7/).

The time horizon `time_horizon`, over which both metrics are evaluated,
defaults to 100 years, but can be set flexibly in years. Additionally,
both metrics can be applied with a fixed or flexible time horizon. Fixed
time horizon means that the all emissions are evaluated starting from
the timing of the functional unit until the end of the time horizon,
meaning that later emissions are counted for shorter, and flexible time
horizon means that each emission is evaluated starting from its own
timing until the end of the time horizon. The former is the approach of
[Levasseur et al. 2010](https://pubs.acs.org/doi/10.1021/es9030003).
This behaviour is set with the boolean `fixed_time_horizon`.
