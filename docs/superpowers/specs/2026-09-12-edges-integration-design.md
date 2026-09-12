# Design: `edges` integration in bw_timex — time-resolved characterisation

Date: 2026-09-12
Status: approved design, not yet implemented

## Goal

Characterise a bw_timex time-explicit inventory with [`edges`](https://edges.readthedocs.io)
methods, evaluating every exchange's characterisation factor (CF) at **that exchange's own
year**. This combines the two libraries' complementary time dimensions:

- `edges` varies CFs with the **year of the exchange** (symbolic, scenario-parameterised CFs),
  and additionally offers regionalised CFs and technosphere CFs.
- `bw_timex` knows **when** each process runs and each emission occurs.
- `dynamic_characterization` (already integrated) varies impact with **time since emission**.

`edges` and `dynamic_characterization` do not overlap; this feature is orthogonal to
`dynamic_lcia()`.

## Scope

In scope (v1):

- Expanded-matrix runs only: `lci(expand_technosphere=True)`.
- Biosphere CFs, evaluated at the **emission date** (temporal-distribution resolved).
- Technosphere CFs (e.g. GeoPolRisk), evaluated at the consuming process's time.
- A CF table carrying the date/year used per exchange.

Out of scope (v1):

- The timeline route (`lci(expand_technosphere=False)`) — no expanded matrix exists there.
- CF uncertainty / Monte Carlo (`edges`' `use_distributions=True`).
- Life cycle costing (`edges.CostLCIA`).

## Why the static expanded matrix cannot be used for biosphere flows

`MatrixModifier.create_biosphere_datapackage()` (`matrix_modifier.py:173-178`) writes each
biosphere exchange of a temporalised producer as a single cell
`(exc.input.id, new_producer_id) → exc.amount`. It carries no date. Temporal distributions on
biosphere exchanges are unfolded **only** in `DynamicBiosphereBuilder`
(`dynamic_biosphere_builder.py:205-212`), which emits rows keyed `(flow, date)` through
`biosphere_time_mapping`.

Consequently one process column may emit in several years, while the static expanded biosphere
matrix holds exactly one cell per `(flow, process)` and can therefore carry only one CF year.
Any route through `lca.inventory` would silently characterise TD-spread emissions at the
process's year. The biosphere side therefore characterises the **dynamic inventory**, which is
the only object carrying TD-resolved dates.

There is no dynamic technosphere counterpart, so technosphere CFs use the consuming process's
vintage. This is a documented limitation, not a configuration option.

## Mechanism

Chosen approach: **year loop over masked CF matrices**, using only `edges`' public API.

Exchange matching in `edges` is year-independent and is the expensive step, so it runs once.
`evaluate_cfs()` is cheap and is the intended per-year loop (see the `edges` user guide,
"Parameterized CFs"). In deterministic mode `EdgeLCIA.lcia()` is an elementwise product of a 2D
sparse CF matrix with the inventory, so building an equivalent product per year is legitimate.

For each distinct year `Y` in the time-explicit system:

1. `evaluate_cfs(scenario_idx=str(Y))`.
2. Read `characterization_matrices["biosphere"]` / `["technosphere"]` (2D scipy sparse).
3. Build that year's inventory slice:
   - biosphere `D_Y`: dynamic-inventory non-zeros whose emission-date year is `Y`, rows remapped
     `(flow, date) → lca.dicts.biosphere[flow]`, columns already in activity-position space;
     duplicates summed by `coo_matrix`.
   - technosphere `T_Y`: columns of the technosphere flow matrix whose time-mapped timestamp
     year is `Y`.
4. `impact_Y = (C_Y ∘ slice).sum()`; accumulate the score and retain the triples for the CF
   table.

Alternatives considered and rejected:

- **Per-position CF evaluation** (walk `cfs_mapping` and evaluate each expression per position).
  Fewer matrix builds, but requires `edges`' private helpers
  (`_evaluate_cf_numeric_value`, `_resolve_parameters_for_scenario`) → brittle across releases.
  Remains available later as an optimisation behind the same public signature.
- **One full `EdgeLCIA` run per year.** No matrix surgery, but repeats `lci()` and matching per
  year and makes CF-table assembly awkward.

## Public API

One method on `TimexLCA`, mirroring `dynamic_lcia()`'s one-shot style:

```python
def edges_lcia(
    self,
    method: tuple | str | Path,
    parameters: dict | None = None,
    scenario: str | None = None,
    weight: str = "population",
    filepath: str | None = None,
    allowed_functions: dict | None = None,
    regionalized: bool = True,
    use_disaggregated_lci: bool = False,
) -> pd.DataFrame
```

Internal sequence: adapter `lci()` → `map_exchanges()` → if `regionalized`,
`map_aggregate_locations()`, `map_dynamic_locations()`, `map_contained_locations()`,
`map_remaining_locations_to_global()` → year loop → accumulate.

`method` is the `edges` method and is never defaulted from `TimexLCA.method`, which is a
different (bw2) object.

Returns the CF table: `edges`' columns (supplier, consumer, location, CF, amount, impact,
`supplier matrix`, `direction`) plus `date`, `year`, and `activity` (time-mapped id).

Stored state:

- `edges_score` — property returning the float score, raising the usual "not yet calculated"
  `AttributeError` like `static_score` / `dynamic_score`.
- `edges_characterized_inventory` — the returned DataFrame.
- `edges_lcia_object` — the adapter instance, keeping `statistics()`, `scenario_cfs` and
  unmatched-exchange inspection reachable behind the one-shot surface. Named with the suffix
  because the plain name is taken by the method itself.

`use_disaggregated_lci=True` uses `dynamic_inventory_disaggregated`, which has the same
structure, and triggers `disaggregate_background_lci()` when absent, as `dynamic_lcia()` does.

## Adapter internals

`_TimexEdgeLCIA(EdgeLCIA)` in a new module `bw_timex/edges_lcia.py`.

**Construction.** `super().__init__(demand=timex_lca.fu, method=..., lca=timex_lca.lca, ...)`,
reusing the already-built expanded LCA so no second solve occurs.

**`lci()` override.** `edges`' implementation calls `self.lca.lci(factorize=True)` and resolves
`lca.dicts.activity` keys through `bw2data.get_activities()` (`edges/utils.py:393-447`). Both are
wrong here: the inventory is already solved, and bw_timex's time-mapped ids do not exist in
bw2data. The override:

- asserts the inventory is present and never re-solves;
- builds `biosphere_flows` by delegating to `edges.utils.get_flow_matrix_positions` unchanged,
  since biosphere rows are real Brightway ids;
- builds `technosphere_flows` by resolving each `(time_mapped_id → position)` through
  `activity_time_mapping.reversed → (("db", "code"), timestamp)`, fetching each unique
  `(db, code)` node once into a cache, and emitting `edges`' exact schema: `name`,
  `reference product`, `categories`, `unit`, `location`, `classifications`, `type`, `position`.
  Several vintages of one node yield several positions sharing metadata; `edges` is
  position-keyed, so this is well-formed;
- records a `position → timestamp` side table for the technosphere year and for reporting;
- builds `technosphere_edges` only when the method declares technosphere suppliers
  (`edges`' `_uses_technosphere_supplier_matrix()`).

**Year extraction.** `activity_time_mapping` timestamps are integers `YYYY`, `YYYYMM`,
`YYYYMMDD` or `YYYYMMDDHH` depending on `temporal_grouping`, or the string `"dynamic"` for
foreground columns whose timing was not resolved. Years are derived with the existing
`convert_date_string_to_datetime(temporal_grouping, ...)`. `"dynamic"` columns are excluded from
the technosphere side, with a single aggregated warning naming how many were skipped.

**Coupling surface to `edges`**, deliberately narrow: constructor kwargs, `lci`, `map_*`,
`evaluate_cfs`, `characterization_matrices`, `_uses_*_supplier_matrix`, and
`get_flow_matrix_positions`. A version floor plus one smoke test guard against drift.

## Errors and validation

Raised before any expensive work, in the style of `static_lcia()` / `dynamic_lcia()`:

- no `lci()` yet → `AttributeError`, pointing at `TimexLCA.lci()`;
- `expanded_technosphere is False` → `NotImplementedError` (timeline route out of scope);
- biosphere method without `dynamic_inventory` → `ValueError` explaining that biosphere-exchange
  temporal distributions would be lost, requiring `lci(build_dynamic_biosphere=True)`. No
  silent process-time fallback;
- `edges` not importable → `ImportError` naming `pip install bw_timex[edges]` and the Python
  `<3.13` ceiling.

Argument validation via a new `EdgesLCIAInputs` pydantic model in `validation.py`, following
`LCIInputs` / `DynamicLCIAInputs`.

## Packaging

- New optional extra: `edges = ["edges>=1.4.1"]`. The import is deferred into the method so the
  core install is unaffected.
- `edges` requires `>=3.10,<3.13` while bw_timex requires `>=3.11`; the extra is therefore
  installable on 3.11 and 3.12 only. CI gains an `edges` job restricted to those versions; the
  core matrix is unchanged.

## Testing

TDD. New `tests/test_edges_lcia.py`, guarded by `pytest.importorskip("edges")`, plus a small
symbolic year-dependent method JSON in `tests/fixtures/`.

1. **Conservation** — a constant-CF `edges` method reproduces the bw2calc static score, proving
   the dynamic-inventory route loses nothing (the dynamic inventory sums back to the static
   inventory per `(flow, process)`).
2. **The feature** — a year-dependent CF over a fixture whose biosphere exchange carries a
   temporal distribution spanning at least two years, asserted against a hand-computed
   `Σ amount · CF(year(emission))`. This test fails for any process-time shortcut.
3. **Translation** — position dicts carry the correct name and location per position, including
   two vintages of the same node.
4. **Year extraction** — all four `temporal_grouping` values; `"dynamic"` sentinel excluded and
   warned.
5. **Error paths** — the four errors above.
6. **API-drift smoke test** over the narrow `edges` surface listed above.

## Documentation

- `CHANGES.md` entry.
- Method docstring stating the technosphere-timing limitation explicitly.
- Example notebook under `docs/content/examples/`.

## Known limitations

- Technosphere CFs resolve no finer than the consuming process's vintage, because bw_timex has
  no dynamic technosphere inventory.
- Temporal markets carry no biosphere flows of their own (`matrix_modifier.py:161-167`); their
  emissions appear under the real background nodes they link to, which is where CFs attach.
- CF uncertainty and the timeline route are deferred.
