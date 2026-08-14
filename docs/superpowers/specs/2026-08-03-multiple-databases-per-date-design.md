# Multiple background databases per point in time

## Problem

`bw_timex` maps each background database to one point in time via `database_dates`.
Internally it assumes the reverse mapping is also unique: `TimelineBuilder` builds
`reversed_database_dates = {date: database}` (`timeline_builder.py:549`) and the
background-traversal extractor builds the same inversion
(`edge_extractor.py:125`). Two databases sharing a date collapse into one, so only
one of them is ever reachable.

This forces users to keep modified copies of background processes inside the
background database they were copied from, which violates the practice of leaving
ecoinvent/premise databases untouched. The electric-vehicle premise notebooks do
exactly that: they write `..., without EOL` copies back into
`ei312_REMIND-EU_SSP2_NDC_<year>`.

## Goal

Allow several static databases to share a date, so a study can keep its modified
background processes in its own databases — one per vintage — while the untouched
vintages stay read-only.

## Non-goals

- Changing the `database_dates` signature or any other public API.
- Overlay/shadow semantics, database priority lists, or explicit vintage groups.
  These are only needed for same-name overrides, which this design rejects with an
  error instead (see "Ambiguity").
- Relaxing the cross-vintage matching key. Matching stays
  `(name, reference product, location)`.

## Design

### Public interface

Unchanged. `database_dates` is `{database_name: datetime | "dynamic"}`; its keys are
database names, so duplicate datetime values are already syntactically valid. They
simply have to stop breaking internally.

```python
database_dates = {
    "ei312_2020": datetime(2020, 1, 1),
    "ei312_2030": datetime(2030, 1, 1),
    "ev_background_2020": datetime(2020, 1, 1),   # modified copies
    "ev_background_2030": datetime(2030, 1, 1),
    "foreground": "dynamic",
}
```

`validation.py` already accepts this — it validates each value independently and
never checks uniqueness — so no validation change is required.

### Resolution rule: per producer, by content

Temporal market shares are currently computed per *producer date* and then remapped
to database names through the global date -> database inversion. They become
per *producer*:

1. Candidate databases for a producer = the static databases that contain a
   `(name, reference product, location)` match for it.
2. The producer's interpolation runs over the dates of its candidates only.
3. Each resulting date maps back to the database through that producer's own
   candidate map.

With the example above, `glider production, passenger car, without EOL` exists only
in `ev_background_2020` and `ev_background_2030`, so it interpolates over those.
`market group for electricity, low voltage` exists only in `ei312_2020` and
`ei312_2030`, so it interpolates over those. No configuration is needed for either.

### Ambiguity: same triplet, same date

If a producer matches in more than one database at the *same* date — e.g. a copy
that keeps the original's name, reference product and location — the model is
genuinely ambiguous. Raise a `ValueError` naming the producer and the colliding
databases, and state the fix: give the copy a distinct name, reference product or
location.

The check runs lazily, only for producers that actually become temporal markets, so
it never scans the full background.

### Partial coverage

A producer that exists in fewer vintages than are configured (e.g. a copy made once,
into a single dated database) interpolates over the vintages it does exist in. With
one candidate that means a constant `{database: 1}` — a time-invariant background
process, which is legitimate.

Emit one `logger.warning` per such producer, naming it and the databases used, so a
forgotten vintage copy is visible.

### Components

**`TimelineBuilder.add_column_temporal_market_shares_to_timeline`**
(`timeline_builder.py:508-596`)

Replace `self.reversed_database_dates` with a per-producer map
`{producer_id: {date: database_name}}`, built in a single pass over `self.nodes`
metadata restricted to the market producers (the leaf background frontier — normally
a handful of nodes). `self.nodes` holds `LazyActivity` proxies whose `name`,
`reference product` and `location` come from scalar columns, so the pass does not
unpickle data blobs.

Interpolation helpers (`find_closest_date`,
`get_weights_for_interpolation_between_nearest_years`) are unchanged; each producer
passes its own `sorted_dates`. Weights are memoized per
`(candidate_dates_tuple, producer_date)` so producers sharing a candidate set do not
recompute.

`add_interpolation_weights_at_intersection_to_background`
(`timeline_builder.py:667`) is the only other reader of
`reversed_database_dates` and has no call site; delete it.

**`edge_extractor.VariantSplitMixin._variant_shares_for_date`**
(`edge_extractor.py:115-137`)

Takes the producer's `node_id` in addition to the date. Candidates come from
`interdatabase_activity_mapping[node_id]` intersected with the static databases;
under `traverse_background` the full mapping is already built up front
(`TimexLCA.add_full_interdatabase_activity_mapping`). Same ambiguity error and
partial-coverage warning. `_resolve_in_variant` is unchanged. The single call site is
`edge_extractor.py:379`, inside `_emit_variant_split*`, which already has `node_id`.

**`TimexLCA.add_interdatabase_activity_mapping_from_timeline`**
(`timex_lca.py:1568`)

The triplet pass in the timeline builder produces exactly the `{producer: {db: id}}`
data this method recomputes after the timeline is built. Populate
`interdatabase_activity_mapping` from the timeline builder's pass and let this method
reuse it, so the scan happens once. Net effect on setup time is neutral or better.

**`TimexLCA.add_full_interdatabase_activity_mapping`** (`timex_lca.py:1537`)

Picks one arbitrary anchor per triplet across all static databases
(`tuples_dict.setdefault`). Under a same-date collision the ambiguity error fires
first, so the arbitrary choice is never reached; no change needed.

### Deliberately unchanged

These already key on database name rather than on date, and work as soon as the
shares dict carries the right names:

- `matrix_modifier.py:287` — iterates `{database: share}` and looks the producer up
  per database through `interdatabase_activity_mapping.find_match`.
- `helper_classes.InterDatabaseMapping` — `{anchor_id: {database: id}}`.
- `activity_time_mapping` — keyed `((database, code), date_hash)`, unique across
  same-date databases because codes differ.
- `TimexLCA.prepare_base_lca_inputs` / `create_node_collections` — reach the new
  databases through `find_graph_dependents` from the foreground, as with any other
  linked background database.

## Testing

Test-first. New fixture: `background_2020` / `background_2030` plus
`background_mod_2020` / `background_mod_2030` at identical dates, with the foreground
linked to the modified copies.

1. Shares of a copied producer route to the `_mod_` family; shares of an untouched
   producer route to the plain family; weights match the single-database-per-date
   values.
2. A producer with the same triplet in two databases at one date raises `ValueError`
   naming both databases.
3. A producer present in a single vintage yields `{database: 1}` at every date and
   logs a warning.
4. The same routing holds with `traverse_background=True`.
5. Existing unique-date tests pass unchanged (regression).

## Follow-up

Update `notebooks/example_electric_vehicle_premise_simple.ipynb`, the full
`example_electric_vehicle_premise.ipynb`, and
`notebooks/teaching/teaching_example_ev_premise.ipynb` to write the `without EOL`
copies into `ev_background_<year>` databases instead of into the premise databases,
and document duplicate dates in the `database_dates` docs.
