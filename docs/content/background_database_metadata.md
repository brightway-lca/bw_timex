---
icon: lucide/calendar-clock
tags:
  - background databases
---

# What a database represents

`bw_timex` needs to know which point in time each background database stands for.
That information lives in the database's own Brightway metadata, so it only has to
be recorded once - not in every script.

```python
import bw2data as bd

bd.databases["ei_cutoff_3.10.1_remind_SSP2-PkBudg500_2050"]
```

```python
{
    # written by brightway
    "format": "Ecoinvent XML", "backend": "sqlite", "number": 43648, ...,
    # written by premise
    "premise_version": "2.4.9.3",
    "iam_model": "remind",
    "pathway": "SSP2-PkBudg500",
    "representative_time": "2050-01-01T00:00:00",
    "ecoinvent_version": "3.10.1",
    "system_model": "cutoff",
}
```

Only `representative_time` is required. `TimexLCA` reads it from every database of
your project, so a study on premise databases needs no timing argument at all:

```python
tlca = TimexLCA(demand={("foreground", "A"): 1}, method=("our", "method"))
```

!!! info "premise version"

    premise writes this metadata from the version following 2.4.9.2 onwards. For
    databases written by an earlier version, set it yourself as shown below - it is
    a one-liner per database.

## Setting it yourself

For databases you built yourself, use
[`set_database_metadata`][bw_timex.database_metadata.set_database_metadata]:

```python
from datetime import datetime
from bw_timex import set_database_metadata

set_database_metadata("background_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("background_2030", representative_time=datetime(2030, 1, 1))
```

The value is stored as an ISO 8601 string, because Brightway keeps database
metadata as JSON. You only do this once per database: it is stored in the project,
not in your script.

Your foreground doesn't represent a point in time - its processes get distributed
over time. `TimexLCA` treats the databases holding your functional unit as
`"dynamic"` automatically, but you can also say so explicitly:

```python
set_database_metadata("foreground", representative_time="dynamic")
```

## Several databases for the same point in time

More than one database may carry the same date. This is useful when you modify
background processes: keep the modified copies in your own database per point in
time, instead of writing them into ecoinvent or premise.

```python
set_database_metadata("my_background_2020", representative_time=datetime(2020, 1, 1))
set_database_metadata("my_background_2030", representative_time=datetime(2030, 1, 1))
```

For each process, `bw_timex` interpolates only between the databases that actually
contain it, matched on `name`, `reference product` and `location`.

## Choosing a scenario

A project often holds more than one IAM scenario. `bw_timex` refuses to guess and
tells you what it found:

```
Several background scenarios found in this project:
  pathway=SSP2-PkBudg500: ei_..._2030, ei_..._2040, ei_..._2050
  pathway=SSP2-Base: ei_..._2030, ei_..._2040, ei_..._2050
Select one, e.g. scenario={'pathway': '...'}, or map the databases explicitly with
`database_dates`.
```

Pick one with the `scenario` argument, which filters the databases on their
metadata:

```python
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    scenario={"pathway": "SSP2-PkBudg500"},
)
```

Any metadata key works - `iam_model`, `pathway`, `system_model`,
`ecoinvent_version`, `premise_version`, or anything you set yourself. Databases
that don't carry the key at all (your foreground, your own vintages) are never
filtered out. A key no database declares, or a filter that matches nothing, is
also an error rather than a silent empty result - `bw_timex` reports what it
actually found so you can spot a typo.

Comparing scenarios is then a loop over filters:

```python
scores = {}
for pathway in ("SSP2-Base", "SSP2-PkBudg500"):
    tlca = TimexLCA(demand, method, scenario={"pathway": pathway})
    tlca.build_timeline()
    tlca.lci()
    tlca.static_lcia()
    scores[pathway] = tlca.static_score
```

!!! warning "Superstructure databases"

    Databases holding several scenarios at once (premise superstructure or
    scenario-array exports) are skipped: they have no single technosphere per point
    in time. Use one database per scenario and year.

## Mapping the databases explicitly

`database_dates` still does what it always did, and takes over completely: when you
pass it, metadata is not read at all and only the databases you list are used. It
cannot be combined with `scenario`.

```python
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("our", "method"),
    database_dates={
        "background": datetime(2020, 1, 1),
        "background_2030": datetime(2030, 1, 1),
        "foreground": "dynamic",
    },
)
```

Use it when you want to restrict a calculation to a subset of the databases in your
project, or when a database's metadata is wrong and you don't want to change it.
It is also the fastest option on a large project: without it, `TimexLCA` reads
metadata from and loads node data for *every* registered database that carries
`representative_time`, including ones your demand does not actually depend on,
which costs setup time at premise scale - narrow it down with `scenario`, or bypass
metadata resolution entirely with an explicit `database_dates`.
