# Representative time as database metadata

## Problem

`TimexLCA` learns what a background database represents in one way only: the
`database_dates` argument the user hand-writes at every call site.

```python
database_dates = {
    "ei310_remind_SSP2-PkBudg500_2030": datetime(2030, 1, 1),
    "ei310_remind_SSP2-PkBudg500_2040": datetime(2040, 1, 1),
    "ei310_remind_SSP2-PkBudg500_2050": datetime(2050, 1, 1),
    "foreground": "dynamic",
}
```

The information is already in the database — a premise export knows the year it was
built for — but it lives only in the database *name*, so every study re-types it, and
a typo either raises (`Database 'x' not available`) or, worse, silently maps a vintage
to the wrong year.

premise [PR #303](https://github.com/polca/premise/pull/303) (merged to `master`, not
in 2.4.9.2) closes the gap on the producing side: exported Brightway databases now
carry what they represent in their `bd.databases[name]` metadata.

```python
{
    # written by brightway
    "format": "Ecoinvent XML", "depends": [...], "backend": "sqlite",
    "number": 43648, "modified": "...", "processed": "...",
    # written by premise
    "premise_version": "2.4.9.1",
    "iam_model": "remind",
    "pathway": "SSP2-PkBudg500",
    "representative_time": "2050-01-01T00:00:00",
    "ecoinvent_version": "3.10.1",
    "system_model": "cutoff",
}
```

Multi-scenario exports (superstructure, scenario arrays) instead carry a `scenarios`
list of such mappings, and a top-level `representative_time` only when all their
scenarios share a year. User (external) scenarios are listed under
`external_scenarios`.

## Goal

`TimexLCA` reads the databases' own metadata by default, so the common case needs no
timing argument at all:

```python
tlca = TimexLCA(demand={("foreground", "A"): 1}, method=("GWP", "example"))
```

A project holding several IAM scenarios stays unambiguous: `TimexLCA` refuses to guess
and tells the user how to pick.

```python
tlca = TimexLCA(
    demand={("foreground", "A"): 1},
    method=("GWP", "example"),
    scenario={"pathway": "SSP2-PkBudg500"},
)
```

Databases that carry no metadata (hand-built vintages, the foreground) get it from a
one-line helper instead of a repeated argument.

## Non-goals

- Removing or deprecating `database_dates`. It stays, unchanged in meaning, and
  scripts that pass it behave exactly as they do today.
- Reading anything from database *names*. No year parsing, no naming convention.
- Making superstructure / scenario-array databases usable in `TimexLCA`. They are
  recognised and skipped, not supported.
- Writing metadata on import of ecoinvent or any other database. Only the explicit
  helper writes.
- Changing how a resolved `database_dates` mapping is used downstream. Everything
  after resolution — timeline, temporal markets, matrix modification — is untouched.

## Design

### Public interface

```python
TimexLCA(
    demand: dict,
    method: tuple,
    database_dates: dict = None,
    scenario: dict = None,
    use_global_lci_cache: bool = True,
)
```

`scenario` is a mapping of database metadata key to required value. Any key that
appears in database metadata is allowed — `iam_model`, `pathway`, `system_model`,
`ecoinvent_version`, `premise_version`, and whatever premise adds later. It is a dict
rather than a set of explicit keywords so that the signature stays closed (a
misspelled `use_global_lci_cache` raises `TypeError` instead of being swallowed as a
filter), the call site reads as background selection, and one dict can be reused
across a comparison loop.

```python
from bw_timex import set_database_metadata

set_database_metadata("db_2030", representative_time=datetime(2030, 1, 1))
set_database_metadata(
    "my_2050_variant",
    representative_time="2050-01-01",
    iam_model="remind",
    pathway="SSP2-PkBudg500",
)
```

### Resolution

`TimexLCA.__init__` resolves `self.database_dates` before anything else, in
`_resolve_database_dates`. Two mutually exclusive branches:

**`database_dates` given.** It is the whole mapping. Metadata is not read, `scenario`
must be `None` (passing both raises `ValueError`), and demand databases missing from
it raise in validation as they do today. This keeps every existing script
bit-for-bit unchanged: a legacy call in a project that also holds ten premise
vintages must not silently pull those ten in.

**`database_dates` not given.** Resolve from metadata:

1. **Candidates.** Every database in `bd.databases` whose metadata has a
   `representative_time`.
2. **Skip multi-scenario databases.** A candidate that also has a non-empty
   `scenarios` list is dropped with a `logger.info` naming it. `bw_timex` needs one
   technosphere per point in time and cannot pick a scenario out of a superstructure
   database. Such a database can still be used by naming it in `database_dates`.
3. **Filter by `scenario`.** A candidate is dropped only if it *declares* a filtered
   key with a different value. A candidate that does not declare the key at all is
   kept — a hand-built 2020 database, an untouched ecoinvent, or the foreground has no
   `pathway`, and filtering it out would break every mixed setup.
   `external_scenarios` (a list) compares order-insensitively as a set; all other
   values compare with `==` after `str` coercion of both sides.
4. **Ambiguity check.** Over the surviving candidates that declare at least one
   scenario key, build a signature from
   `("iam_model", "pathway", "system_model", "ecoinvent_version", "external_scenarios")`
   (missing key → `None`). Bookkeeping keys such as `premise_version` are deliberately
   not part of the signature: re-running premise must not look like a second scenario.
   More than one distinct signature raises `ValueError`, reporting only the keys whose
   values actually differ:

   ```
   Several background scenarios found in this project:
     pathway='SSP2-PkBudg500': ei310_remind_SSP2-PkBudg500_2030,
                               ei310_remind_SSP2-PkBudg500_2040,
                               ei310_remind_SSP2-PkBudg500_2050
     pathway='SSP2-Base':      ei310_remind_SSP2-Base_2030,
                               ei310_remind_SSP2-Base_2040,
                               ei310_remind_SSP2-Base_2050
   Select one, e.g. scenario={'pathway': 'SSP2-PkBudg500'}, or map the databases
   explicitly with database_dates.
   ```

   Databases that declare no scenario key at all never appear in this check and are
   always kept.
5. **Normalize values.** `datetime` passes through; a string parses with
   `datetime.fromisoformat`; the literal `"dynamic"` passes through. Anything else
   raises `ValueError` naming the database, the key and the offending value.
6. **Demand databases.** Every database holding a demand key that is not already
   mapped is added as `"dynamic"`.
7. **Nothing found.** If no database carries `representative_time`, log the existing
   "no remapping will be done" message and fall back to today's behaviour: demand
   databases marked `"dynamic"`.

An unknown filter key — one that no candidate database declares — raises rather than
filtering everything away, listing the keys and values present in the project. That is
what buys back the autocomplete a dict does not give.

### Setter helper

`bw_timex.utils.set_database_metadata(database, **metadata)`, re-exported from
`bw_timex`:

- `database` may be a name or a `bd.Database`; unregistered → `ValueError`.
- `representative_time` accepts a `datetime` (serialized with `.isoformat()`), an ISO
  string (validated by round-tripping through `fromisoformat`), or `"dynamic"`.
  Brightway metadata is stored as JSON, so a `datetime` object left in it breaks
  `bd.databases.flush()`; converting is the point of the helper.
- Any other key is written as given, after a JSON-serializability check.
- Writes into `bd.databases[name]` and calls `bd.databases.flush()`, so the value
  survives a project reload.
- Returns the resulting metadata mapping.

### Validation

`TimexLCAInputs` gains `scenario: Optional[dict]`, validating that keys are strings
and values are scalars or lists of scalars, and that `scenario` and `database_dates`
are not both given. The metadata-side errors (unparseable value, ambiguity, unknown
filter key) are raised in `_resolve_database_dates`, which owns the metadata, not in
the pydantic model.

`set_database_metadata` gets its own `DatabaseMetadataInputs` model, matching how the
other user-facing helpers in `utils.py` validate.

## Interactions and limits

- **Several databases per date** ([#205](https://github.com/brightway-lca/bw_timex/pull/205))
  still works: metadata discovery can map two databases to the same
  `representative_time`, which is exactly the modified-copy case. Two full ecoinvent
  copies of the same vintage (e.g. from two premise runs) collide on process identity
  and raise there, as designed; the fix is a `scenario` filter on `premise_version` or
  an explicit `database_dates`.
- **Setup cost.** `TimexLCA.__init__` loads node metadata for every database in
  `database_dates`, so auto-discovery costs one node-metadata load per matching
  database. A project holding vintages from an unrelated study pays for them; the
  escape hatches are `scenario` or `database_dates`.
- **premise version.** The metadata is written by premise `master` (post-2.4.9.2).
  Databases written by older premise carry nothing, and the docs say so; those users
  either write metadata with the helper or keep using `database_dates`.

## Documentation

- `docs/content/getting_started/quickstart.md`: step 3 becomes "the databases already
  know when they are"; `database_dates` shown once as the explicit alternative; the
  cheat-sheet row for background timing updated.
- `docs/content/getting_started/adding_temporal_information.md` and
  `build_process_timeline.md`: update the passages that name `database_dates`.
- New section in `docs/content/getting_started/` on what a database represents:
  the metadata keys, `set_database_metadata`, scenario selection and its error, the
  premise-version caveat, and `database_dates` as the explicit override.
- `docs/api/utils.md` picks up the new helper through the existing `::: bw_timex.utils`
  block; no edit needed beyond the intro sentence.
- `CHANGES.md`: entry under `[Unreleased]`.

## Notebooks

Every notebook that builds its own databases writes metadata with
`set_database_metadata` and drops the `database_dates` argument; the premise notebooks
rely on premise-written metadata and show `scenario` where a project holds more than
one pathway.

- `notebooks/tutorials/1_getting_started.ipynb`
- `notebooks/tutorials/2_electric_vehicle_from_scratch.ipynb`
- `notebooks/tutorials/3_dynamic_characterization.ipynb`
- `notebooks/tutorials/4_import_model_from_excel.ipynb`
- `notebooks/advanced/background_temporal_distributions.ipynb`
- `notebooks/advanced/background_temporal_distributions_premise.ipynb`
- `notebooks/advanced/uncertainty_with_datapackages.ipynb`
- `notebooks/teaching/ev_walkthrough_premise.ipynb`
- `notebooks/teaching/exercise_ev_vs_petrol_solutions.ipynb`
- `notebooks/examples/electric_vehicle_premise.ipynb`
- `notebooks/examples/electric_vehicle_premise_detailed.ipynb`
- `notebooks/development/benchmarking.ipynb`

`notebooks/examples/paper_case_study.ipynb` is **not** touched: it reproduces a
published study and must keep its exact code.

## Testing

New `tests/test_database_metadata.py`, on the existing small fixtures:

- Timing resolved from `representative_time` metadata with no `database_dates`.
- ISO string and `datetime` metadata values both resolve; `"dynamic"` in metadata
  marks a database dynamic; a garbage value raises naming the database.
- Demand database defaults to `"dynamic"` when its metadata says nothing.
- `database_dates` is exclusive: a project full of metadata-carrying databases plus an
  explicit `database_dates` resolves to exactly that mapping.
- `database_dates` together with `scenario` raises.
- `scenario` filter selects one pathway out of two; databases without scenario
  metadata survive the filter.
- Two scenario sets and no `scenario` raises, and the message names the differing key
  and both values.
- Same scenario written by two premise versions does not raise (bookkeeping keys are
  outside the signature).
- A database carrying `scenarios` is skipped, and named in the log.
- An unknown filter key raises listing the available keys.
- `set_database_metadata` round-trips through `bd.databases.flush()` and a re-read;
  a `datetime` lands as an ISO string; an unregistered database raises.
- End-to-end: an existing scenario test rewritten to use metadata gives the same
  score as the `database_dates` version.
