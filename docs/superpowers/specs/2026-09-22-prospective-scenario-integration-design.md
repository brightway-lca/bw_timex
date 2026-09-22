# Prospective characterization scenarios in bw_timex

Date: 2026-09-22
Status: approved design, not yet implemented
Repos touched: `bw_timex`, `dynamic_characterization`

## Problem

`dynamic_characterization` gained prospective, scenario-dependent characterization
factors (Watanabe et al. 2026), exposed through the metrics `pGWP`, `pGTP` and
`prospective_radiative_forcing`. The scenario those factors depend on lives in
module-global state, set once per session:

```python
from dynamic_characterization.prospective import set_scenario
set_scenario(iam="IMAGE", ssp="SSP1", rcp="2.6")
```

`bw_timex` knows nothing about it. `TimexLCASettings` has no field for it,
`TimexLCA._result_row` does not record it, and `TimexLCA.dynamic_lcia` passes
nothing to `characterize` about it. Three consequences:

1. **The characterization scenario cannot be varied in a comparison.**
   `TimexLCA.compare` builds one `TimexLCA` per background (`_background_key`,
   `timex_lca.py:926`) and so handles background scenarios well, but every row
   reads the same module global for its characterization factors.

2. **A background sweep under a prospective metric is silently wrong.** Running
   `SSP1-PkBudg500` and `SSP1-Base` vintages side by side with `metric="pGWP"`
   characterizes both with whichever RCP was last set. At most one row is
   internally consistent, and nothing in `summary` says which.

3. **The two packages describe a scenario differently and nothing bridges them.**
   `bw_timex` uses premise's metadata (`iam_model="image"`,
   `pathway="SSP1-PkBudg500"`). `dynamic_characterization` uses Watanabe's
   `iam`/`ssp`/`rcp`. Keeping them consistent is left entirely to the user, who
   must state the same future twice.

Secondary defects found while surveying:

4. `TimexLCA.dynamic_lcia` drops three arguments `characterize` accepts:
   `time_varying_re`, `fallback_to_ipcc`, `characterize_biogenic_uptake`.
5. `create_missing` is in `TimexLCASettings.FIXED_FIELDS` (`timex_lca.py:101`)
   and guarded in `run()` (`timex_lca.py:732`), but `_background_key`
   (`timex_lca.py:931-935`) omits it, so two settings differing only in
   `create_missing` share one `TimexLCA` — contradicting the guard.
6. `_get_year_index` (`prospective/radiative_forcing.py:55`) warns per call when
   an emission year falls outside the RE data bounds. Characterizing a full
   dynamic inventory emits thousands of identical warnings.

## Non-goals

- Renaming anything users type today. `TimexLCASettings.scenario` keeps premise's
  key names; the metric strings `pGWP`/`pGTP`/`prospective_radiative_forcing`
  stay primary; `set_scenario()` keeps working.
- Unifying the two scenario vocabularies into one object. They describe different
  things and both are established; they are bridged, not merged.
- Inferring a Watanabe scenario where the pairing is not exact. See below.

## Design

### 1. `PROSPECTIVE_SCENARIO_MAP` — one public bridging table

A single module-level mapping in `bw_timex`, keyed on the whole premise pair and
yielding the whole Watanabe triple. No two-stage inference (no "derive the SSP,
then separately derive the RCP"): one lookup, one place to read, one place to
cite.

```python
bw_timex.PROSPECTIVE_SCENARIO_MAP
# ("image",  "SSP1-RCP19")      -> {"iam": "IMAGE",  "ssp": "SSP1", "rcp": "2.6"}
# ("image",  "SSP1-RCP26")      -> {"iam": "IMAGE",  "ssp": "SSP1", "rcp": "2.6"}
# ("image",  "SSP1-PkBudg500")  -> {"iam": "IMAGE",  "ssp": "SSP1", "rcp": "2.6"}
# ("image",  "SSP1-Base")       -> {"iam": "IMAGE",  "ssp": "SSP1", "rcp": "8.5"}
# ("remind", "SSP5-Base")       -> {"iam": "REMIND", "ssp": "SSP5", "rcp": "8.5"}
# ...
```

An entry exists **only where the IAM and SSP pairing is exact**. Watanabe et al.
pair each IAM with one SSP (IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4,
REMIND-SSP5); premise does not. The very common premise setup `remind` +
`SSP2-PkBudg500` therefore has no entry and raises rather than substituting
REMIND-SSP5 (wrong SSP) or MESSAGE-SSP2 (wrong IAM). Substituting silently would
be an undisclosed scientific choice inside a number the user will publish.

The table is normal data: users may extend it in their own session if they accept
a pairing bw_timex will not make for them.

Lookups are case-insensitive on `iam_model` (premise writes `"remind"`,
metadata elsewhere may say `"REMIND"`).

### 1b. Discoverability: `available_scenarios()`

Today only one of the two scenario lists exists. `dynamic_characterization`
exports `prospective.VALID_SCENARIOS` (the 19 valid IAM/SSP/RCP tuples), but
bw_timex has no list of premise scenarios at all: `scenario_builder` hands
`iam_model`/`pathway` straight to `premise.NewDatabase`
(`scenario_builder.py:93`), so a typo fails inside premise, often after
ecoinvent has already been imported. `database_metadata.py:237` only reports the
keys this project's databases happen to declare.

bw_timex therefore exposes:

```python
bw_timex.PROSPECTIVE_SCENARIO_MAP   # the bridge itself
bw_timex.VALID_SCENARIOS            # re-export of the dynamic_characterization set
bw_timex.available_scenarios()      # one table over both sides
```

**The two sides are listed independently, not just their intersection.** Plenty
of users want a premise background and a conventional `GWP`, or want to explore
the prospective factors without premise in the picture at all. The table is a
full outer join of the two catalogues, with one tick column per side:

| iam_model | pathway | iam | ssp | rcp | premise | prospective | in_project |
|---|---|---|---|---|---|---|---|
| image | SSP1-RCP26 | IMAGE | SSP1 | 2.6 | ✓ | ✓ | 2020, 2030, 2040 |
| image | SSP1-Base | IMAGE | SSP1 | 8.5 | ✓ | ✓ | — |
| remind | SSP2-PkBudg500 | — | — | — | ✓ | — | 2020, 2030 |
| — | — | GCAM4 | SSP4 | 6.0 | — | ✓ | — |

Reading the rows:

- `premise ✓`, `prospective —` — a background you can build, characterized with
  IPCC AR6 factors only. Most premise scenarios are here.
- `premise ✓`, `prospective ✓` — usable end to end with `pGWP`/`pGTP`/
  `prospective_radiative_forcing` and no manual scenario pairing.
- `premise —`, `prospective ✓` — Watanabe scenarios with no premise counterpart.
  Reachable via an explicit `characterization_scenario` on any background,
  including hand-built vintages mapped with `database_dates`.

`usable_for` narrows it: `available_scenarios(usable_for="premise")`,
`"prospective"`, or `"both"`; the default lists everything. It is a plain
DataFrame, so `.query(...)` works for anything finer.

Where the rows come from:

- **premise side** — enumerated from the scenario files premise bundles
  (`premise/data/iam_output_files/`), whose names carry `iam_model` and
  `pathway`; readable without the decryption key. When premise is not installed,
  the column falls back to the keys of `PROSPECTIVE_SCENARIO_MAP` plus whatever
  this project's databases declare, and the docstring plus a note in the return
  say the listing is partial.
- **prospective side** — `VALID_SCENARIOS`, always available; it is bundled data
  in a package bw_timex already depends on.
- **`in_project`** — the years already built, read from database metadata, so the
  same table answers "what do I still have to build?".

Every error about a missing pairing points at this function.

### 2. `characterization_scenario` — a new optional LCIA setting

```python
lcia={"metric": "pGWP",
      "time_horizon": 100,
      "characterization_scenario": {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "2.6"}}
```

Same three keys `dynamic_characterization` already uses, so there is nothing new
to learn and the dict can be handed straight to it. Added to
`TimexLCASettings`'s flat fields and to `STAGE_GROUPS["lcia"]`. Validated by
`DynamicLCIAInputs` against `VALID_SCENARIOS` when given.

### 3. `characterize(scenario=...)` — an argument, not a session setting

`dynamic_characterization.characterize` gains `scenario: dict | None = None`.
When given, that scenario applies for the duration of the call and nothing else
changes; when omitted, behaviour is exactly as today.

Mechanically this is a scoped override of the module state, not a rebinding of
the characterization functions:

```python
@contextmanager
def scenario_context(scenario: dict | None):
    """Apply `scenario` for the duration of the block, then restore."""
```

`characterize` wraps its body in `scenario_context(scenario)`; the previous value
is restored on exit, including when the body raises. `scenario_context` is
exported from `dynamic_characterization.prospective` alongside `set_scenario`.

Rebinding the CF functions with `functools.partial` was considered and rejected:
`_characterize_pgtp` (`dynamic_characterization.py:680-695`) dispatches on
*function identity* (`char_func in (prospective_characterize_co2, ...)`) to pick
the matching AGTP function, and `_CHARACTERIZATION_FUNCTION_CACHE` is keyed
without a scenario because the CF functions are scenario-independent today.
Partials would break both. A scoped override keeps every identity check, the
cache, the `agtp.*` helpers that read the scenario themselves, and any
user-supplied CF that reads the global — all unchanged.

The scope is a single `characterize` call, so `compare()` can pass a different
scenario per row without leaking between rows and without clobbering what the
user set in their session. The override is not thread-safe; that is stated in
the docstring, and matches how the module already behaves.

### 4. Resolution and precedence in `TimexLCA`

Resolution happens per run, inside `dynamic_lcia`, and only when the metric is
prospective. Non-prospective metrics never consult the table and never raise on a
missing entry.

Precedence, highest first:

1. `characterization_scenario` from the settings (or the `dynamic_lcia` argument)
2. `PROSPECTIVE_SCENARIO_MAP[(iam_model, pathway)]`, from `settings.scenario`
3. the `set_scenario()` session default, if one is set
4. otherwise raise

The session default sits **below** the table deliberately: someone who called
`set_scenario` once in a notebook and then compares two backgrounds should get
the two matching CF sets, not one stale one carried across both. When the session
default is what gets used, that is logged.

`characterization_scenario` becomes a per-run field, not a `FIXED_FIELD`: it does
not select databases, so holding the background fixed and varying only the CF
scenario reuses one `TimexLCA` and re-runs only `dynamic_lcia`. The sensitivity
sweep "same future, different CF assumption" is therefore nearly free.

### 5. What `compare()` gives the user

One `scenario=` switch moves the background **and** the characterization:

```python
base = TimexLCASettings(demand={driving: 1}, method=method,
                        lcia={"metric": "pGWP", "time_horizon": 100})

comparison = TimexLCA.compare([
    replace(base, scenario={"iam_model": "image", "pathway": "SSP1-RCP26"}, label="RCP2.6"),
    replace(base, scenario={"iam_model": "image", "pathway": "SSP1-Base"},  label="Baseline"),
])
```

Row 1 gets IMAGE-SSP1-RCP26 vintages *and* IMAGE-SSP1-RCP2.6 radiative
efficiencies; row 2 gets Base vintages *and* RCP8.5 efficiencies. There is no
second place to keep in sync.

`ComparisonResult.summary` gains `cf_iam`, `cf_ssp`, `cf_rcp` columns alongside
the existing `scenario_*` ones, filled from the resolved scenario, and `NaN` for
non-prospective rows. A finished comparison then states which factors produced
each number — today that is invisible.

`_background_key` also gains `create_missing`, fixing defect 5.

### 6. Logging and errors

One INFO line per prospective run, naming where the scenario came from:

```
pGWP100: characterization scenario IMAGE-SSP1-RCP2.6
  from background image / SSP1-PkBudg500 (PROSPECTIVE_SCENARIO_MAP)
```

and the corresponding variants for an explicit `characterization_scenario` and
for the session default.

When the table has no entry:

```
ValueError: metric='pGWP' needs a prospective characterization scenario, and the
background scenario remind / SSP2-PkBudg500 has no entry in
bw_timex.PROSPECTIVE_SCENARIO_MAP (Watanabe et al. 2026 pairs each IAM with one
SSP: IMAGE-SSP1, MESSAGE-SSP2, AIM-SSP3, GCAM4-SSP4, REMIND-SSP5).

Choose one explicitly:
    lcia={"metric": "pGWP",
          "characterization_scenario": {"iam": "MESSAGE", "ssp": "SSP2", "rcp": "2.6"}}
```

Same shape when `database_dates` was used instead of `scenario` (nothing to
derive from), minus the pairing sentence.

Emission-year clamping (defect 6) is deduplicated to one warning per
(flow, bound) per characterization call.

### 7. Forwarded LCIA arguments

`time_varying_re`, `fallback_to_ipcc` and `characterize_biogenic_uptake` are
added to `TimexLCASettings` and to `STAGE_GROUPS["lcia"]`, passed through
`dynamic_lcia` to `characterize`. Defaults match `characterize`'s own
(`False`, `True`, `True`), so nothing changes for existing code.

## Testing

`bw_timex`:

- `PROSPECTIVE_SCENARIO_MAP` lookup: hit, case-insensitive `iam_model`, miss.
- Precedence: explicit beats table beats session default beats raise; each path
  logs the line that names its source.
- A non-prospective metric with an unmappable `scenario` runs without raising.
- `database_dates` + prospective metric + no `characterization_scenario` raises.
- `compare()` over two backgrounds under `pGWP` passes two *different* scenarios
  to `characterize` (asserted on a patched `characterize`) and fills `cf_*`.
- `compare()` over two `characterization_scenario`s on one background builds one
  `TimexLCA` and produces two different `dynamic_score`s.
- `characterization_scenario` may change between `run()` calls; `create_missing`
  may not, and settings differing only in it no longer share an object.
- The forwarded LCIA arguments arrive at `characterize`.
- `available_scenarios()` lists premise-only, prospective-only and shared rows,
  marks the missing side with `—`, and reports the years present in the project.
- `usable_for="premise"` / `"prospective"` / `"both"` each return the right
  subset, and the premise side degrades to a partial listing, without raising,
  when premise is not installed.

`dynamic_characterization`:

- `characterize(scenario=...)` produces the same numbers as `set_scenario(...)`
  followed by `characterize()`, and leaves the session scenario as it found it
  (including when it was unset).
- Two `characterize` calls with different `scenario` arguments in one process
  give different results.
- `characterize` with no scenario and none set raises `NO_SCENARIO_MESSAGE` for
  a prospective metric, unchanged.
- `scenario_context` restores the previous value, including on exception, and
  restores "unset" when nothing was set.
- `_characterize_pgtp`'s identity dispatch still picks the right AGTP function
  under a scoped scenario (a CO2 row characterized with `scenario=` gives
  `pGTP == 1.0`).
- Out-of-bounds emission years warn once, not once per row.

## Documentation

Prospective metrics are currently undocumented in bw_timex: no notebook and no
docs page mentions `pGWP`, `pGTP` or `set_scenario`. The docs pages under
`docs/content/examples/` are generated from the notebooks under `notebooks/`, so
the notebook is the source and the `.md` plus its `_files/` directory are
regenerated from it.

**`notebooks/tutorials/5_scenario_comparison.ipynb`** (and its generated
`docs/content/examples/tutorials/scenario_comparison.md`) — the main deliverable.

The tutorial is currently built on `remind-eu` + `SSP2-PkBudg650`, which has no
exact Watanabe pairing (REMIND is paired with SSP5 there), so a prospective
metric on it can only ever demonstrate the override. The notebook switches to a
pairing that is exact on both sides — **`image` + `SSP1-RCP26`** against
**`image` + `SSP1-Base`** — which maps to IMAGE-SSP1-RCP2.6 and
IMAGE-SSP1-RCP8.5 respectively. Both background *and* characterization then
differ between the two rows, from one `scenario=` switch, which is exactly the
point the tutorial is making. The switch applies to the whole notebook, not just
the new section, so the databases it builds are the ones the prospective section
needs.

A new final section then runs the same two scenarios under a prospective metric:

```python
comparison = TimexLCA.compare([
    replace(
        base,
        scenario={"iam_model": "image", "pathway": pathway, ...},
        lcia={"metric": "pGWP", "time_horizon": 100},
        label=pathway,
    )
    for pathway in ("SSP1-RCP26", "SSP1-Base")
])
comparison.summary[["label", "static_score", "dynamic_score", "cf_iam", "cf_ssp", "cf_rcp"]]
```

No `characterization_scenario` anywhere: the log lines and the `cf_*` columns
show the two different CF sets being derived. The section also shows
`bw_timex.available_scenarios()`, and closes with a short paragraph on the
pairings that have no entry (naming `remind` + `SSP2-*`, since that is what most
readers will reach for next) and the explicit `characterization_scenario` that
handles them.

**`notebooks/tutorials/3_dynamic_characterization.ipynb`** (and
`docs/content/examples/tutorials/dynamic_characterization.md`) — introduce the
prospective metrics where dynamic characterization is first taught: what `pGWP`,
`pGTP` and `prospective_radiative_forcing` are, the Watanabe reference, the
`iam`/`ssp`/`rcp` triple, and `characterization_scenario` on a single run.
Currently this notebook stops at `GWP` and `radiative_forcing`.

**`docs/content/examples/tutorials/index.md` / `zensical.toml`** — no new nav
entries needed; both tutorials already exist in the nav.

**Docstrings and API reference** — `TimexLCA.dynamic_lcia`,
`TimexLCASettings` (the `lcia` group listing) and `TimexLCA.compare` gain the new
settings; `PROSPECTIVE_SCENARIO_MAP` is added to `docs/api/timex_lca.md` so the
table renders in the reference.

**`docs/content/getting_started/lcia.md`** — add the prospective metrics to the
list of available metrics, with the scenario requirement and a pointer to
`available_scenarios()`.

**`docs/content/getting_started/configured_runs.md`** — add
`characterization_scenario` to the `lcia` group shown there, and one sentence
that a prospective metric takes its scenario from `scenario` unless overridden.

**`docs/content/create_premise_dbs.md`** — a short note that the premise pathway
chosen here also decides the prospective characterization scenario, with a link
to the table.

**`dynamic_characterization`** — its README and docs still say the scenario must
be set with `set_scenario()` before calling; update to lead with
`characterize(scenario=...)` and present `set_scenario` as the session default.
Its example notebook gains a cell passing `scenario=` directly.

Regeneration: after editing each notebook, re-run it and regenerate the
corresponding `.md` and `_files/` directory so the published docs match.

## Migration

Nothing is removed and no existing spelling changes meaning, with one deliberate
exception: a prospective run that previously picked up a stale session scenario
now either derives the matching one from its background or raises. That is the
defect being fixed, and the log line makes the new choice visible.

Docs and notebooks to update are listed under Documentation above; they are
part of this change, not a follow-up.
