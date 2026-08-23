# Creating missing scenario databases with premise

## Problem

`TimexLCA(scenario={...})` selects background databases that already exist in the
project. If they do not, it raises and lists what the project actually holds:

```
scenario={'pathway': 'SSP2-PkBudg500'} matched no database in this project.
Values actually declared for its key(s) by this project's databases:
'pathway': ['SSP2-Base']. Check for a typo in the filter.
```

For a typo that is the right answer. For the other case — the user simply has not
built that scenario yet — it is a dead end that sends them to a premise notebook,
where they hand-write a `NewDatabase` call whose `model`, `pathway`, `system_model`
and `ecoinvent_version` must match, key for key, the `scenario` filter they will pass
to `TimexLCA` afterwards. The same facts are typed twice, in two libraries, and only
the second one checks them.

## Goal

One recipe, stated once, that either finds the vintages or builds them:

```python
tlca = TimexLCA(
    demand={("foreground", "ev"): 1},
    method=("EF v3.1", "climate change", "global warming potential (GWP100)"),
    scenario={
        "iam_model": "remind",
        "pathway": "SSP2-PkBudg500",
        "system_model": "cutoff",
        "ecoinvent_version": "3.10.1",
        "years": [2020, 2030, 2040],
    },
    create_missing=True,
)
```

Missing years are built with premise, missing ecoinvent is imported first, and the
resulting databases carry the metadata that makes them findable on the next run — so
the second call to the same code builds nothing.

## Non-goals

- Changing the default. `create_missing` defaults to `False`; without it the
  behaviour is exactly today's, down to the error text (plus one added line saying
  auto-building exists).
- Deferring `scenario` to `lci()`, or a settings-object / `run()` API. Everything
  after `_resolve_database_dates` in `__init__` — base LCA, node collections, time
  mapping — depends on the resolved database set, so resolution stays eager. A
  lazier `TimexLCA` is a separate change.
- Making premise a hard dependency. It is an optional extra, imported only when a
  build actually happens.
- Reproducing premise's own API surface. Only the arguments needed to build a
  vintage set for a `TimexLCA` are exposed; anything more exotic (external
  scenarios, `system_model_args`, incremental databases, custom IAM files) is built
  in premise directly and then simply found by the scenario filter.
- Superstructure / scenario-array exports. Unchanged: recognised and skipped.

## Design

### Public interface

```python
TimexLCA(
    demand: dict,
    method: tuple,
    database_dates: dict = None,
    scenario: dict = None,
    create_missing: bool = False,
    premise_key: str = None,
    ecoinvent_credentials: tuple[str, str] = None,
    use_global_lci_cache: bool = True,
)
```

and the same thing standalone, for users who would rather build first and calculate
later:

```python
from bw_timex import ensure_scenario_databases

ensure_scenario_databases(scenario, premise_key=None, ecoinvent_credentials=None)
```

`ensure_scenario_databases` returns the name → `datetime` mapping of the vintages it
found or built. `TimexLCA` ignores that return value and re-reads the metadata through
the existing `_resolve_database_dates`, so there is exactly one code path that decides
what a database represents.

#### Build keys

`scenario` gains three keys that describe *how to build*, as opposed to *what to
match*:

| key | meaning | required |
|---|---|---|
| `years` | vintages to find or build, as integer years, e.g. `[2020, 2030, 2040]` — premise takes a year, not a date | yes, when `create_missing=True` |
| `sectors` | premise sectors to update; absent → all | no |
| `source_database` | name of the ecoinvent database to build from | no |

They are stripped before the metadata filter runs. They must be: `years` is a list
and no database's `representative_time` could ever equal it, so leaving it in would
match nothing. The four filter keys (`iam_model`, `pathway`, `system_model`,
`ecoinvent_version`) keep their current meaning and are additionally what premise is
called with.

Putting the years in `scenario` rather than in a separate argument keeps one object
describing one background: it can be built once and reused across a comparison loop,
and there is no way to pass years that disagree with the filter they belong to.

### Resolution and build

`ensure_scenario_databases`:

1. **Split** `scenario` into filter keys and build keys.
2. **Match per year.** A year is satisfied by a registered database whose
   `representative_time` falls in that year and that the scenario filter would keep.
   "Would keep" is the resolver's own rule, not a stricter one: a database is kept
   unless it *declares* a filter key with a different value, so a hand-built 2020
   vintage carrying only `representative_time` satisfies 2020 and is not rebuilt.
   Sharing the rule with `resolve_database_dates_from_metadata` is what guarantees
   that a database counted here is a database the resolver accepts afterwards —
   any stricter match would build a second 2020 database that the resolver then
   maps to the same date as the first.
3. **Early return.** No missing years → return, without importing premise. This is
   the steady-state path: every re-run of a study takes it, and it must stay free.
4. **Source database.** `scenario["source_database"]` if given; otherwise
   `ecoinvent-{ecoinvent_version}-{system_model}`, the name
   `bw2io.import_ecoinvent_release` writes. Absent from the project → import it with
   `bw2io.import_ecoinvent_release(version, system_model, username, password)`. The
   biosphere name is derived the same way (`ecoinvent-{version}-biosphere`) and
   passed to premise explicitly, since premise defaults to `"biosphere3"`, which a
   namespaced ecoinvent import does not create.
5. **One premise run for every missing year.**
   `NewDatabase(scenarios=[{"model": iam_model, "pathway": pathway, "year": y} for y
   in missing], source_db=..., source_version=..., system_model=...,
   biosphere_name=..., key=...)`. One run, not one per year: premise caches the
   extracted source database, so N runs would re-extract ecoinvent N times.
6. **Update.** `ndb.update(sectors)` if `sectors` was given, else `ndb.update()`.
7. **Write.** `write_db_to_brightway(name=[...])` with deterministic names,
   `ei_{system_model}_{ecoinvent_version}_{iam_model}_{pathway}_{year}`.
8. **Metadata.** premise >= 2.4.9.2 writes `representative_time`, `iam_model`,
   `pathway`, `system_model`, `ecoinvent_version` and `premise_version` on export, so
   nothing is duplicated here. Two additions only:
   - `sectors` is written with `set_database_metadata` when it was narrowed, because
     premise does not record it and two runs of the same pathway with different
     sectors would otherwise be indistinguishable to the scenario filter.
   - after the write, assert `representative_time` is present on each new database.
     Missing → `RuntimeError` naming the premise pin. One check instead of
     version-sniffing branches.

**Overwrite guard.** Before step 5, every target name is checked against
`bd.databases`. A name that exists but whose metadata does not match the scenario
aborts the build with a `ValueError`: `write_db_to_brightway` deletes and rewrites a
colliding name silently, and that database is someone's multi-gigabyte work. A name
that exists *and* matches cannot occur — step 2 would have counted its year as
satisfied.

**Announcement.** Before step 4, a `logger.info` states which years will be built,
which sectors, that each vintage is a full ecoinvent copy of roughly 2-4 GB, and that
this takes tens of minutes. The user asked for it, but not necessarily knowing that.

### Module layout

A new module, `bw_timex/scenario_builder.py`, holding `ensure_scenario_databases` and
the two seams below. `database_metadata.py` stays what its name says: reading and
writing metadata, no side effects, no third-party imports. `timex_lca.py` is already
2500 lines and gains only the argument plumbing.

`ensure_scenario_databases` is re-exported from the `bw_timex` top-level namespace,
next to `set_database_metadata`.

### Third-party seams

All premise and bw2io contact happens in two module-level functions that do nothing
else:

```python
def _run_premise(scenarios, source_database, source_version, system_model,
                 biosphere, sectors, names, key) -> None
def _import_ecoinvent(version, system_model, credentials) -> str
```

Both import their library inside the function body. Tests monkeypatch them (see
Testing). Keeping them free of logic is what makes the rest of the module testable
without premise installed.

### Dependency

```toml
[project.optional-dependencies]
premise = ["premise[bw25]>=2.4.9.2"]
```

2.4.9.2 is both the current PyPI release and the first that writes the metadata this
design relies on, so the floor costs nobody anything.

The `[bw25]` extra is not optional: bare `premise` declares `bw2data` and `bw2io`
unpinned, and pins them to Brightway 2.5 versions (`bw2data>=4.3`, `bw2io>=0.9.4`)
only under that extra. Without it a resolver may satisfy them with the bw2-era pins
(`bw2data==3.6.6`), which contradicts bw_timex's `bw2data>=4.6`. `bw2io` arrives
through the same extra, so it needs no entry of its own.

### Credentials

Resolved in `ensure_scenario_databases`, once, before any expensive work:

| value | argument | environment fallback |
|---|---|---|
| premise decryption key | `premise_key` | `PREMISE_KEY` |
| ecoinvent user | `ecoinvent_credentials[0]` | `ECOINVENT_USERNAME` |
| ecoinvent password | `ecoinvent_credentials[1]` | `ECOINVENT_PASSWORD` |

An explicit argument always wins. The environment fallback exists so that notebooks
that get committed do not carry secrets in a cell; the explicit argument exists so
that a one-off interactive call does not require setting the environment first.
Ecoinvent credentials are read only on the branch that actually imports ecoinvent.

### Validation and failure modes

Everything below raises before premise is imported, and long before anything is
written:

| condition | raises |
|---|---|
| premise not installed | `ImportError`: `pip install "bw_timex[premise]"` |
| `create_missing=True`, no `years` | `ValueError` |
| `create_missing=True` and `database_dates` given | `ValueError`, mirroring the existing `scenario` + `database_dates` rule |
| `create_missing=True`, scenario missing any of the four filter keys | `ValueError` naming them; premise needs all four |
| no premise key | `ValueError` naming `PREMISE_KEY` |
| ecoinvent absent and no credentials | `ValueError` naming `ECOINVENT_USERNAME` / `ECOINVENT_PASSWORD` |
| target name exists with foreign metadata | `ValueError`, nothing written |
| `representative_time` absent after write | `RuntimeError` naming the premise pin |
| `create_missing=False` and the filter matched nothing | today's `ValueError`, plus one line on how to auto-build |

`create_missing=True` with `scenario=None` raises as well: there is no recipe to
build from.

A new pydantic model in `validation.py`, `ScenarioBuildInputs`, carries the argument
checks, matching the existing `TimexLCAInputs` pattern. `TimexLCAInputs` gains
`create_missing`, `premise_key` and `ecoinvent_credentials`.

## Testing

premise cannot run in CI: it needs a decryption key, a licensed ecoinvent, and tens
of minutes and gigabytes per vintage. The two seams above are therefore monkeypatched
with fakes that register small in-memory databases carrying premise-style metadata.
Everything on this side of the seam — splitting, matching, guarding, credential
resolution, metadata — is exercised for real.

- **Reuse**: all years already present → `_run_premise` never called, and `premise`
  never enters `sys.modules`.
- **Partial**: years `[2020, 2030, 2040]` with 2030 present → `_run_premise` called
  once, with exactly `[2020, 2040]`.
- **Sectors**: a narrowed list reaches the fake and lands in the new databases'
  metadata; absent → the fake is called with `sectors=None` (meaning `update()`).
- **Metadata round-trip**: after a fake build, `TimexLCA.database_dates` resolves the
  built databases to the right datetimes through the normal resolver.
- **Overwrite guard**: a colliding name with foreign metadata raises and
  `_run_premise` is never called.
- **Source database**: an explicit `source_database` is passed through; a missing
  ecoinvent triggers `_import_ecoinvent`; a present one does not.
- **Credentials**: explicit argument beats environment; missing values raise naming
  the exact variable (with a monkeypatched `os.environ`).
- **Validation**: no `years`; missing filter keys; `create_missing` together with
  `database_dates`; `create_missing` without `scenario`.
- **`ImportError`** when premise is absent, **`RuntimeError`** when the fake omits
  `representative_time`.
- **Integration**: an existing fixture project plus `TimexLCA(..., create_missing=True)`
  with faked seams completes `__init__` and calculates against the built vintages.

A real premise run stays manual: one documented snippet, run by hand, recorded in the
docs.

## Documentation

- `docs/content/getting_started`: a short section on letting `TimexLCA` build the
  background, next to the existing scenario-filter documentation.
- `installation.md`: the `bw_timex[premise]` extra and the two environment variables.
- `CHANGES.md`: one entry.
