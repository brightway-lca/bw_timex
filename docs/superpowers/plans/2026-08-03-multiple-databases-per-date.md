# Multiple background databases per point in time — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let several static background databases share the same date in `database_dates`, so modified copies of background processes can live in their own databases instead of being written into ecoinvent/premise.

**Architecture:** Today `TimelineBuilder` and the background-traversal edge extractor invert `database_dates` into `{date: database}`, which collapses same-date databases. Both are replaced by a *per producer* resolution: a producer's candidate databases are the static databases containing a `(name, reference product, location)` match for it, and interpolation runs over the dates of those candidates only. Public API is unchanged.

**Tech Stack:** Python, bw2data / bw2calc / bw_temporalis, pandas, pytest, loguru.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-03-multiple-databases-per-date-design.md`.
- No public API change. `database_dates` stays `{database_name: datetime | "dynamic"}`.
- Cross-database matching key stays `(name, reference product, location)`.
- Same triplet in two databases at the same date is an error, never a silent pick.
- A producer present in fewer vintages than exist is allowed, with one `logger.warning`.
- TDD: write the failing test, watch it fail, then implement.
- Commit messages: Conventional Commits, no Claude/AI attribution, no `Co-Authored-By` trailer.
- Run tests with the project venv: `.venv/bin/pytest`.

---

### Task 1: Per-producer temporal market shares

**Files:**
- Create: `tests/fixtures/same_date_databases_fixture.py`
- Create: `tests/test_same_date_databases.py`
- Modify: `tests/conftest.py`
- Modify: `bw_timex/timeline_builder.py:508-596` (`add_column_temporal_market_shares_to_timeline`) and `:667-693` (delete `add_interpolation_weights_at_intersection_to_background`)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `TimelineBuilder.candidate_databases_for_producers(producers: set) -> dict[int, dict[datetime, str]]`
  - `TimelineBuilder.market_producer_matches: dict[int, dict[str, int]]` — `{producer_id: {database_name: node_id}}`, set as a side effect of the above. Task 3 consumes it.
  - pytest fixture `same_date_db` (no return value; writes the databases into a fresh project).

- [ ] **Step 1: Write the fixture**

Create `tests/fixtures/same_date_databases_fixture.py`:

```python
import bw2data as bd
import pytest
from bw2data.tests import bw2test


@pytest.fixture
@bw2test
def same_date_db():
    """Four static background databases on two dates.

    `background_2020` / `background_2030` hold an untouched `electricity`
    process. `modified_2020` / `modified_2030` hold a copy of `steel` with its
    end-of-life removed, named `steel, without EOL`; they carry the *same*
    dates as the two `background_*` databases. The foreground consumes one of
    each, so both must become temporal markets that interpolate within their
    own family of databases.

    CO2 amounts differ per vintage so the interpolation is visible in the score:
    electricity 10 (2020) / 5 (2030), steel 20 (2020) / 10 (2030).
    """
    biosphere = bd.Database("biosphere")
    biosphere.write({("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}})
    node_co2 = biosphere.get("CO2")

    amounts = {
        "background_2020": {"electricity": 10, "steel": 20},
        "background_2030": {"electricity": 5, "steel": 10},
    }

    for year in ("2020", "2030"):
        background = bd.Database(f"background_{year}")
        background.register()
        modified = bd.Database(f"modified_{year}")
        modified.register()

        electricity = background.new_node("electricity", name="electricity", unit="kWh")
        electricity["reference product"] = "electricity"
        electricity["location"] = "GLO"
        electricity.save()
        electricity.new_edge(input=electricity, amount=1, type="production").save()
        electricity.new_edge(
            input=node_co2,
            amount=amounts[f"background_{year}"]["electricity"],
            type="biosphere",
        ).save()

        steel = background.new_node("steel", name="steel", unit="kg")
        steel["reference product"] = "steel"
        steel["location"] = "GLO"
        steel.save()
        steel.new_edge(input=steel, amount=1, type="production").save()
        steel.new_edge(
            input=node_co2,
            amount=amounts[f"background_{year}"]["steel"],
            type="biosphere",
        ).save()

        # The study's own copy of `steel`, without EOL, in its own database.
        steel_copy = steel.copy(code="steel_without_eol", database=f"modified_{year}")
        steel_copy["name"] = "steel, without EOL"
        steel_copy["reference product"] = "steel, without EOL"
        steel_copy.save()

    foreground = bd.Database("foreground")
    foreground.register()
    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu["location"] = "GLO"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()
    fu.new_edge(
        input=bd.Database("background_2020").get("electricity"), amount=1, type="technosphere"
    ).save()
    fu.new_edge(
        input=bd.Database("modified_2020").get("steel_without_eol"),
        amount=1,
        type="technosphere",
    ).save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])

    for dbname in bd.databases:
        bd.Database(dbname).process()
```

Register it in `tests/conftest.py` by adding this import next to the other fixture imports:

```python
from .fixtures.same_date_databases_fixture import same_date_db
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_same_date_databases.py`:

```python
"""Several static background databases may share the same date."""

from datetime import datetime

import bw2data as bd
import pytest
from loguru import logger

from bw_timex import TimexLCA

METHOD = ("GWP", "example")
DATABASE_DATES = {
    "background_2020": datetime.strptime("2020", "%Y"),
    "background_2030": datetime.strptime("2030", "%Y"),
    "modified_2020": datetime.strptime("2020", "%Y"),
    "modified_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}


def _shares_by_producer(timeline):
    return {
        row.producer_name: row.temporal_market_shares
        for row in timeline.itertuples()
        if row.temporal_market_shares
    }


def test_shares_route_within_each_database_family(same_date_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")
    shares = _shares_by_producer(tlca.timeline)

    assert set(shares["electricity"]) == {"background_2020", "background_2030"}
    assert set(shares["steel, without EOL"]) == {"modified_2020", "modified_2030"}
    assert shares["electricity"]["background_2020"] == pytest.approx(0.5, abs=0.01)
    assert shares["steel, without EOL"]["modified_2020"] == pytest.approx(0.5, abs=0.01)


def test_score_interpolates_within_each_family(same_date_db):
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")
    tlca.lci()
    tlca.static_lcia()
    # electricity: 0.5*10 + 0.5*5 = 7.5; steel: 0.5*20 + 0.5*10 = 15
    assert tlca.static_score == pytest.approx(22.5, abs=0.2)


def test_same_triplet_at_same_date_raises(same_date_db):
    """A copy that keeps name/reference product/location is ambiguous."""
    collision = bd.Database("modified_2020").new_node(
        "electricity_collision", name="electricity", unit="kWh"
    )
    collision["reference product"] = "electricity"
    collision["location"] = "GLO"
    collision.save()
    collision.new_edge(input=collision, amount=1, type="production").save()
    bd.Database("modified_2020").process()

    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    with pytest.raises(ValueError, match="more than one database"):
        tlca.build_timeline(starting_datetime="2025-01-01")


def test_producer_in_a_single_vintage_warns_and_is_time_invariant(same_date_db):
    """A copy made into only one vintage stays constant over time, with a warning."""
    bd.Database("modified_2030").get("steel_without_eol").delete()
    bd.Database("modified_2030").process()

    messages = []
    sink_id = logger.add(messages.append, level="WARNING")
    try:
        tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
        tlca.build_timeline(starting_datetime="2025-01-01")
    finally:
        logger.remove(sink_id)

    shares = _shares_by_producer(tlca.timeline)
    assert shares["steel, without EOL"] == {"modified_2020": 1}
    assert any("steel, without EOL" in message for message in messages)
```

- [ ] **Step 3: Run the tests and watch them fail**

Run: `.venv/bin/pytest tests/test_same_date_databases.py -v`
Expected: failures. `test_shares_route_within_each_database_family` fails because
`reversed_database_dates` maps each date to a single database, so both producers get
the same two database names (whichever of `background_*` / `modified_*` won the
inversion); `test_same_triplet_at_same_date_raises` fails because nothing raises.

- [ ] **Step 4: Add the candidate resolver**

In `bw_timex/timeline_builder.py`, insert this method directly above
`add_column_temporal_market_shares_to_timeline`:

```python
    def candidate_databases_for_producers(self, producers: set) -> dict:
        """Map each producer to the static databases that hold a match for it.

        Returns ``{producer_id: {date: database_name}}``. A candidate is a static
        background database containing a node with the same ``(name, reference
        product, location)`` as the producer. Several databases may share a date;
        if two of them hold a match for the same producer, the model is ambiguous
        and this raises.

        As a side effect, ``self.market_producer_matches`` is filled with
        ``{producer_id: {database_name: node_id}}``, which is exactly what
        ``TimexLCA.add_interdatabase_activity_mapping_from_timeline`` needs.
        """
        triplets = {}
        for producer in producers:
            node = self.nodes[producer]
            key = (node["name"], node.get("reference product"), node["location"])
            triplets.setdefault(key, []).append(producer)

        candidates = {producer: {} for producer in producers}
        matches = {producer: {} for producer in producers}
        for node in self.nodes.values():
            date = self.database_dates_static.get(node["database"])
            if date is None:
                continue
            key = (node["name"], node.get("reference product"), node["location"])
            for producer in triplets.get(key, ()):
                already = candidates[producer].get(date)
                if already is not None and already != node["database"]:
                    raise ValueError(
                        f"Producer '{node['name']}' was found in more than one database "
                        f"at {date:%Y-%m-%d}: '{already}' and '{node['database']}'. "
                        "bw_timex cannot tell which one its temporal market should use. "
                        "Give the copy a distinct name, reference product or location."
                    )
                candidates[producer][date] = node["database"]
                matches[producer][node["database"]] = node.id

        number_of_dates = len(set(self.database_dates_static.values()))
        for producer, producer_candidates in candidates.items():
            if len(producer_candidates) < number_of_dates:
                logger.warning(
                    "Producer '{}' was only found in {} of {} time-explicit database "
                    "date(s): {}. Its temporal market can only draw on those.",
                    self.nodes[producer]["name"],
                    len(producer_candidates),
                    number_of_dates,
                    sorted(producer_candidates.values()),
                )

        self.market_producer_matches = matches
        return candidates
```

- [ ] **Step 5: Rewrite the share computation**

Replace the body of `add_column_temporal_market_shares_to_timeline` from the
`dates_list = [` assignment through the `return tl_df` (currently
`timeline_builder.py:538-596`) with:

```python
        if "date_producer" not in list(tl_df.columns):
            raise ValueError("The timeline does not contain dates.")

        if interpolation_type not in ("linear", "nearest"):
            raise ValueError(
                f"Sorry, but {interpolation_type} interpolation is not available yet."
            )

        if self.traverse_background:
            market_producers = self._leaf_background_producers(tl_df)
        else:
            market_producers = self.node_collections["first_level_background_static"]

        producers_in_timeline = {
            producer
            for producer in tl_df["producer"].unique()
            if producer in market_producers
        }
        candidate_databases = self.candidate_databases_for_producers(
            producers_in_timeline
        )

        weight_cache = {}
        shares = []
        for producer, producer_date in zip(tl_df["producer"], tl_df["date_producer"]):
            if producer not in producers_in_timeline:
                shares.append(None)
                continue
            candidates = candidate_databases[producer]
            sorted_dates = tuple(sorted(candidates))
            cache_key = (sorted_dates, producer_date)
            if cache_key not in weight_cache:
                if interpolation_type == "nearest":
                    weights = self.find_closest_date(producer_date, sorted_dates)
                else:
                    weights = self.get_weights_for_interpolation_between_nearest_years(
                        producer_date, sorted_dates, interpolation_type
                    )
                weight_cache[cache_key] = {
                    candidates[date]: share for date, share in weights.items()
                }
            shares.append(weight_cache[cache_key])

        tl_df["temporal_market_shares"] = shares

        return tl_df
```

Keep the `if not self.database_dates_static:` early return at the top of the method
exactly as it is.

- [ ] **Step 6: Delete the dead method**

Delete `add_interpolation_weights_at_intersection_to_background`
(`bw_timex/timeline_builder.py:667-693`). It is the only other reader of the removed
`self.reversed_database_dates` and has no call site.

Verify: `grep -rn "reversed_database_dates\|add_interpolation_weights_at_intersection" bw_timex/ tests/`
Expected: no matches.

- [ ] **Step 7: Run the new tests**

Run: `.venv/bin/pytest tests/test_same_date_databases.py -v`
Expected: 4 passed.

- [ ] **Step 8: Run the full suite for regressions**

Run: `.venv/bin/pytest tests/ -q`
Expected: all pass. `tests/test_timeline_builder.py::TestGetWeightsForInterpolation::test_unsupported_interpolation_type` still passes because
`get_weights_for_interpolation_between_nearest_years` keeps its own type check.

- [ ] **Step 9: Commit**

```bash
git add bw_timex/timeline_builder.py tests/test_same_date_databases.py \
        tests/fixtures/same_date_databases_fixture.py tests/conftest.py
git commit -m "feat: resolve temporal market databases per producer

Several static databases may now share a date in database_dates. A
producer's candidate databases are those holding a (name, reference
product, location) match for it, so copies kept in a separate database
interpolate within their own family."
```

---

### Task 2: Same-date routing when traversing the background

**Files:**
- Modify: `bw_timex/edge_extractor.py:115-137` (`_variant_shares_for_date`) and its call site at `:379`
- Modify: `tests/fixtures/same_date_databases_fixture.py`
- Modify: `tests/test_same_date_databases.py`

**Interfaces:**
- Consumes: `TimelineBuilder.candidate_databases_for_producers` from Task 1 (behaviour only — the extractor resolves candidates from `interdatabase_activity_mapping`, which `TimexLCA.add_full_interdatabase_activity_mapping` fills up front whenever `traverse_background=True`).
- Produces: `VariantSplitMixin._candidate_databases_for_node(node_id: int) -> dict[datetime, str]` and the new signature `_variant_shares_for_date(producer_date, node_id: int) -> dict[str, float]`.

- [ ] **Step 1: Add a second fixture with a background chain**

The traversal test needs a chain *inside* the modified family, but adding it to
`same_date_db` would change the scores asserted in Task 1. Refactor the fixture file
into a shared writer plus two fixtures. Replace the whole of
`tests/fixtures/same_date_databases_fixture.py` with:

```python
import bw2data as bd
import numpy as np
import pytest
from bw2data.tests import bw2test
from bw_temporalis import TemporalDistribution


def _write_same_date_databases(with_background_chain: bool = False):
    """Write four static background databases on two dates.

    `background_2020` / `background_2030` hold an untouched `electricity`
    process. `modified_2020` / `modified_2030` hold a copy of `steel` with its
    end-of-life removed, named `steel, without EOL`; they carry the *same*
    dates as the two `background_*` databases. The foreground consumes one of
    each, so both must become temporal markets that interpolate within their
    own family of databases.

    CO2 amounts differ per vintage so the interpolation is visible in the score:
    electricity 10 (2020) / 5 (2030), steel 20 (2020) / 10 (2030).

    With `with_background_chain=True`, each modified database also holds a
    `smelting` process that the copy consumes through a temporal distribution.
    It exists only in the modified family and is reached only when the
    background is traversed.
    """
    biosphere = bd.Database("biosphere")
    biosphere.write({("biosphere", "CO2"): {"type": "emission", "name": "carbon dioxide"}})
    node_co2 = biosphere.get("CO2")

    amounts = {
        "background_2020": {"electricity": 10, "steel": 20},
        "background_2030": {"electricity": 5, "steel": 10},
    }

    for year in ("2020", "2030"):
        background = bd.Database(f"background_{year}")
        background.register()
        modified = bd.Database(f"modified_{year}")
        modified.register()

        electricity = background.new_node("electricity", name="electricity", unit="kWh")
        electricity["reference product"] = "electricity"
        electricity["location"] = "GLO"
        electricity.save()
        electricity.new_edge(input=electricity, amount=1, type="production").save()
        electricity.new_edge(
            input=node_co2,
            amount=amounts[f"background_{year}"]["electricity"],
            type="biosphere",
        ).save()

        steel = background.new_node("steel", name="steel", unit="kg")
        steel["reference product"] = "steel"
        steel["location"] = "GLO"
        steel.save()
        steel.new_edge(input=steel, amount=1, type="production").save()
        steel.new_edge(
            input=node_co2,
            amount=amounts[f"background_{year}"]["steel"],
            type="biosphere",
        ).save()

        # The study's own copy of `steel`, without EOL, in its own database.
        steel_copy = steel.copy(code="steel_without_eol", database=f"modified_{year}")
        steel_copy["name"] = "steel, without EOL"
        steel_copy["reference product"] = "steel, without EOL"
        steel_copy.save()

        if with_background_chain:
            # Reached only by descending into the background. The 10-year offset
            # pushes it towards the 2030 vintage of the modified family.
            smelting = modified.new_node("smelting", name="smelting", unit="kg")
            smelting["reference product"] = "smelting"
            smelting["location"] = "GLO"
            smelting.save()
            smelting.new_edge(input=smelting, amount=1, type="production").save()
            smelting.new_edge(
                input=node_co2,
                amount=amounts[f"background_{year}"]["steel"],
                type="biosphere",
            ).save()

            copy_to_smelting = steel_copy.new_edge(
                input=smelting, amount=1, type="technosphere"
            )
            copy_to_smelting["temporal_distribution"] = TemporalDistribution(
                date=np.array([10], dtype="timedelta64[Y]"),
                amount=np.array([1.0]),
            )
            copy_to_smelting.save()

    foreground = bd.Database("foreground")
    foreground.register()
    fu = foreground.new_node("fu", name="fu", unit="unit")
    fu["reference product"] = "fu"
    fu["location"] = "GLO"
    fu.save()
    fu.new_edge(input=fu, amount=1, type="production").save()
    fu.new_edge(
        input=bd.Database("background_2020").get("electricity"), amount=1, type="technosphere"
    ).save()
    fu.new_edge(
        input=bd.Database("modified_2020").get("steel_without_eol"),
        amount=1,
        type="technosphere",
    ).save()

    bd.Method(("GWP", "example")).write([(("biosphere", "CO2"), 1)])

    for dbname in bd.databases:
        bd.Database(dbname).process()


@pytest.fixture
@bw2test
def same_date_db():
    """Four static background databases on two dates, no background chain."""
    _write_same_date_databases()


@pytest.fixture
@bw2test
def same_date_deep_db():
    """Same as `same_date_db`, plus a `smelting` chain in the modified family."""
    _write_same_date_databases(with_background_chain=True)
```

Add the second fixture to `tests/conftest.py`, next to the existing import:

```python
from .fixtures.same_date_databases_fixture import same_date_db, same_date_deep_db
```

(replacing the single-name import added in Task 1).

- [ ] **Step 2: Write the failing test**

Append to `tests/test_same_date_databases.py`:

```python
def test_background_traversal_routes_within_the_modified_family(same_date_deep_db):
    """Descending into the background must not confuse same-date databases."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(
        starting_datetime="2025-01-01",
        graph_traversal="bfs",
        traverse_background=True,
    )
    producers = set(tlca.timeline["producer_name"])
    assert "smelting" in producers

    # `smelting` exists only in the modified family, so no background_* database
    # may be picked up for it.
    smelting_rows = tlca.timeline[tlca.timeline["producer_name"] == "smelting"]
    for shares in smelting_rows["temporal_market_shares"]:
        if shares:
            assert set(shares) <= {"modified_2020", "modified_2030"}

    tlca.lci()
    tlca.static_lcia()
    assert tlca.static_score > 0
```

- [ ] **Step 3: Run it and watch it fail**

Run: `.venv/bin/pytest tests/test_same_date_databases.py::test_background_traversal_routes_within_the_modified_family -v`
Expected: FAIL — `_variant_shares_for_date` inverts `database_dates_static` globally, so
the 2030 slot resolves to whichever of `background_2030` / `modified_2030` came last, and
resolving `smelting` there raises `KeyError` (or routes to the wrong family).

- [ ] **Step 4: Resolve candidates per node in the extractor**

In `bw_timex/edge_extractor.py`, replace `_variant_shares_for_date` with:

```python
    def _candidate_databases_for_node(self, node_id: int) -> dict:
        """``{date: database_name}`` for the static databases holding a match.

        Candidates come from the interdatabase mapping (built up front by
        ``TimexLCA.add_full_interdatabase_activity_mapping`` whenever the
        background is traversed), plus the node's own database.
        """
        dates_static = getattr(self, "database_dates_static", None) or {}
        try:
            siblings = dict(self.interdatabase_activity_mapping[node_id])
        except KeyError:
            siblings = {}
        db_names = set(siblings)
        node = self.bw_node_proxies.get(node_id)
        if node is not None:
            db_names.add(node["database"])

        candidates = {}
        for db_name in sorted(db_names):
            date = dates_static.get(db_name)
            if date is None:
                continue
            if date in candidates:
                raise ValueError(
                    f"Node {node_id} was found in more than one database at "
                    f"{date:%Y-%m-%d}: '{candidates[date]}' and '{db_name}'. "
                    "bw_timex cannot tell which one to use. Give the copy a "
                    "distinct name, reference product or location."
                )
            candidates[date] = db_name
        return candidates

    def _variant_shares_for_date(self, producer_date, node_id: int) -> dict:
        """Return ``{db_name: weight}`` interpolation shares for a cohort date.

        Maps the producer's absolute cohort date onto the static background
        databases that actually hold the producer, using the same interpolation
        as the timeline builder so leaf and descended routing agree.
        """
        from datetime import datetime as _dt

        candidates = self._candidate_databases_for_node(node_id)
        sorted_dates = tuple(sorted(candidates))
        if not sorted_dates:
            return {}

        if isinstance(producer_date, np.datetime64):
            producer_date = producer_date.astype("datetime64[s]").astype(_dt)

        if getattr(self, "interpolation_type", "linear") == "nearest":
            weights = nearest_date_weight(producer_date, sorted_dates)
        else:
            weights = linear_interpolation_weights(producer_date, sorted_dates)
        return {candidates[d]: w for d, w in (weights or {}).items()}
```

- [ ] **Step 5: Update the call site**

At `bw_timex/edge_extractor.py:379`, inside `_emit_variant_split_single_consumer`,
change:

```python
            for db_name, weight in self._variant_shares_for_date(date).items():
```

to:

```python
            for db_name, weight in self._variant_shares_for_date(date, node_id).items():
```

`node_id` is a keyword-only parameter of that method's signature; confirm with
`grep -n "def _emit_variant_split" -A 4 bw_timex/edge_extractor.py` before editing, and
if a caller passes it under another name, use that name.

- [ ] **Step 6: Run the traversal tests**

Run: `.venv/bin/pytest tests/test_same_date_databases.py tests/test_background_traversal.py -v`
Expected: all pass.

- [ ] **Step 7: Run the full suite**

Run: `.venv/bin/pytest tests/ -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add bw_timex/edge_extractor.py tests/fixtures/same_date_databases_fixture.py \
        tests/test_same_date_databases.py
git commit -m "feat: route traversed background nodes per node, not per date

The variant split resolved a cohort date through a global date -> database
inversion, which collapsed databases sharing a date. Candidates now come
from the node's own interdatabase mapping."
```

---

### Task 3: Reuse the triplet scan for the interdatabase mapping

**Files:**
- Modify: `bw_timex/timex_lca.py:1568-1621` (`add_interdatabase_activity_mapping_from_timeline`)
- Modify: `tests/test_same_date_databases.py`

**Interfaces:**
- Consumes: `TimelineBuilder.market_producer_matches` (`{producer_id: {database_name: node_id}}`) from Task 1.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_same_date_databases.py`:

```python
def test_interdatabase_mapping_is_filled_by_the_timeline_builder(same_date_db):
    """The builder's triplet scan feeds the mapping; no second scan is needed."""
    tlca = TimexLCA({("foreground", "fu"): 1}, METHOD, DATABASE_DATES)
    tlca.build_timeline(starting_datetime="2025-01-01")

    steel_copy_2020 = bd.Database("modified_2020").get("steel_without_eol")
    steel_copy_2030 = bd.Database("modified_2030").get("steel_without_eol")
    assert (
        tlca.interdatabase_activity_mapping.find_match(steel_copy_2020.id, "modified_2030")
        == steel_copy_2030.id
    )

    electricity_2020 = bd.Database("background_2020").get("electricity")
    electricity_2030 = bd.Database("background_2030").get("electricity")
    assert (
        tlca.interdatabase_activity_mapping.find_match(electricity_2020.id, "background_2030")
        == electricity_2030.id
    )
    # The copy has no counterpart in the untouched family, and none is invented.
    with pytest.raises(KeyError):
        tlca.interdatabase_activity_mapping.find_match(steel_copy_2020.id, "background_2030")
```

- [ ] **Step 2: Run it**

Run: `.venv/bin/pytest tests/test_same_date_databases.py::test_interdatabase_mapping_is_filled_by_the_timeline_builder -v`
Expected: PASS already — `add_interdatabase_activity_mapping_from_timeline` builds the
same mapping with its own scan. This test pins the behaviour that the next step must not
break. If it fails, stop and fix the mapping before continuing.

- [ ] **Step 3: Reuse the builder's matches**

In `bw_timex/timex_lca.py`, in `add_interdatabase_activity_mapping_from_timeline`,
insert directly after the `if not hasattr(self, "timeline"): raise AttributeError(...)`
block:

```python
        # The timeline builder already resolved every temporal-market producer to
        # its counterparts while computing the market shares. Reuse that instead
        # of scanning every background node a second time.
        matches = getattr(self.timeline_builder, "market_producer_matches", None)
        if matches:
            self.interdatabase_activity_mapping.update(matches)
            self.interdatabase_activity_mapping.make_reciprocal()
            return
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest tests/test_same_date_databases.py -v`
Expected: all pass, including the mapping test from Step 1.

- [ ] **Step 5: Run the full suite**

Run: `.venv/bin/pytest tests/ -q`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add bw_timex/timex_lca.py tests/test_same_date_databases.py
git commit -m "perf: reuse the timeline builder's producer matches

The market-share computation already resolves every temporal-market
producer to its counterparts in the other static databases, so the
post-timeline scan over all background nodes is redundant."
```

---

### Task 4: Document databases sharing a date

**Files:**
- Modify: `docs/content/getting_started/adding_temporal_information.md:266-276`

**Interfaces:**
- Consumes: the behaviour from Tasks 1 and 2.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the documentation**

In `docs/content/getting_started/adding_temporal_information.md`, after the `!!! note`
block that ends with the premise link (currently line 275), insert:

```markdown
### Several databases for the same point in time

More than one database may carry the same date. This is useful when you modify
background processes: keep the modified copies in your own database per point in
time, instead of writing them into ecoinvent or premise.

```python
database_dates = {
    "ecoinvent_2020": datetime.strptime("2020", "%Y"),
    "ecoinvent_2030": datetime.strptime("2030", "%Y"),
    "my_background_2020": datetime.strptime("2020", "%Y"),  # your modified copies
    "my_background_2030": datetime.strptime("2030", "%Y"),
    "foreground": "dynamic",
}
```

For each process, `bw_timex` interpolates only between the databases that actually
contain it, matched on `name`, `reference product` and `location`. A copy that only
exists in `my_background_2020` and `my_background_2030` is therefore sourced from
those two, while an untouched process is sourced from the `ecoinvent_*` ones.

!!! warning

    Give your copies a distinct `name`, `reference product` or `location`. If the
    same triplet occurs in two databases that share a date, `bw_timex` cannot tell
    which one you mean and raises an error. A process that exists in only some of
    the points in time is used unchanged for all of them, and logs a warning.
```

- [ ] **Step 2: Check the docs build**

Run: `.venv/bin/python -c "import pathlib; print(pathlib.Path('docs/content/getting_started/adding_temporal_information.md').read_text().count('Several databases for the same point in time'))"`
Expected: `1`

- [ ] **Step 3: Commit**

```bash
git add docs/content/getting_started/adding_temporal_information.md
git commit -m "docs: document several databases sharing a point in time"
```

---

### Task 5: Move the notebook copies out of the premise databases

**Files:**
- Modify: `notebooks/example_electric_vehicle_premise_simple.ipynb` (the "Standard brightway modelling" cell, cell id `cell-4`, and the `database_dates` cell, `cell-12`, and the `add_temporal_distribution_to_exchange` cell, `cell-10`)
- Modify: `notebooks/example_electric_vehicle_premise.ipynb` (same copy loop; find it with `grep -n "glider_production_without_eol"`)
- Modify: `notebooks/teaching/teaching_example_ev_premise.ipynb` (same)

**Interfaces:**
- Consumes: the behaviour from Tasks 1-4.
- Produces: nothing.

**Prerequisite:** this task needs the local Brightway project `ei312_REMIND_EU` with the
three premise databases. If it is not available, stop and report — do not fake the run.

- [ ] **Step 1: Rewrite the copy loop in the simple notebook**

In `notebooks/example_electric_vehicle_premise_simple.ipynb`, replace the copy loop
(the block starting at the comment `# The ecoinvent processes for the ev parts already
contain their end-of-life treatment.` and ending at the `exc.delete()` line) with:

```python
# The ecoinvent processes for the ev parts already contain their end-of-life treatment.
# We want to model the end of life separately, so we create copies without it. Those
# copies live in our own databases - one per point in time - so the premise databases
# stay untouched. bw_timex allows several databases to share a date.
modified_dbs = {}
for db in [db_2020, db_2030, db_2040]:
    year = db.name[-4:]
    modified_name = f"ev_background_{year}"
    if modified_name in bd.databases:
        del bd.databases[modified_name]
    modified_db = bd.Database(modified_name)
    if modified_name not in bd.databases:
        modified_db.register()
    modified_dbs[db.name] = modified_db

    for name, code_, eol_name in [
        (
            "glider production, passenger car",
            "glider_production_without_eol",
            "market for used glider, passenger car",
        ),
        (
            "powertrain production, for electric passenger car",
            "powertrain_production_without_eol",
            "market for used powertrain from electric passenger car, manual dismantling",
        ),
        # For the battery, some waste treatment is buried in the cell production.
        # For simplicity, we just leave it in there.
        (
            "battery production, Li-ion, LiMn2O4, rechargeable",
            "battery_production_without_eol",
            None,
        ),
    ]:
        without_eol = db.get(name=name).copy(code=code_, database=modified_name)
        without_eol["name"] = f"{name}, without EOL"
        without_eol.save()
        if eol_name:
            for exc in without_eol.exchanges():
                if exc.input["name"] == eol_name:
                    exc.delete()

    modified_db.process()
```

- [ ] **Step 2: Point the foreground at the new databases**

In the same cell, change the three lookups of the copies from `db_2020` to the modified
database:

```python
# Background processes our foreground links to
ev_background_2020 = modified_dbs[db_2020.name]
glider_production = ev_background_2020.get(code="glider_production_without_eol")
powertrain_production = ev_background_2020.get(code="powertrain_production_without_eol")
battery_production = ev_background_2020.get(code="battery_production_without_eol")
```

The remaining lookups (`glider_eol`, `powertrain_eol`, `battery_eol`,
`electricity_production`) stay on `db_2020`.

- [ ] **Step 3: Update the temporal distribution calls**

In the `add_temporal_distribution_to_exchange` cell, change `input_database=db_2020.name`
to `input_database=ev_background_2020.name` for the three `without EOL` inputs (glider,
powertrain, battery). The electricity and end-of-life calls keep `db_2020.name`.

- [ ] **Step 4: Add the new databases to `database_dates`**

In the `database_dates` cell:

```python
database_dates = {
    db_2020.name: datetime.strptime("2020", "%Y"),
    db_2030.name: datetime.strptime("2030", "%Y"),
    db_2040.name: datetime.strptime("2040", "%Y"),
    "ev_background_2020": datetime.strptime("2020", "%Y"),
    "ev_background_2030": datetime.strptime("2030", "%Y"),
    "ev_background_2040": datetime.strptime("2040", "%Y"),
    "foreground": "dynamic",
}
```

- [ ] **Step 5: Run the notebook end to end**

Run: `.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/example_electric_vehicle_premise_simple.ipynb`
Expected: no errors; the timeline's `temporal_market_shares` for the `without EOL`
producers name `ev_background_*` databases, and `tlca.static_score` stays close to the
previously recorded 10744 kg CO2-eq (the model is unchanged, only where the copies live).

- [ ] **Step 6: Apply the same change to the other two notebooks**

`notebooks/example_electric_vehicle_premise.ipynb` and
`notebooks/teaching/teaching_example_ev_premise.ipynb` contain the same copy loop.
Locate it with `grep -n "glider_production_without_eol" notebooks/example_electric_vehicle_premise.ipynb notebooks/teaching/teaching_example_ev_premise.ipynb`,
and apply Steps 1-4 there, keeping each notebook's own variable names for the premise
databases.

Run: `.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/example_electric_vehicle_premise.ipynb`
Run: `.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/teaching/teaching_example_ev_premise.ipynb`
Expected: both execute without errors.

- [ ] **Step 7: Commit**

```bash
git add notebooks/example_electric_vehicle_premise_simple.ipynb \
        notebooks/example_electric_vehicle_premise.ipynb \
        notebooks/teaching/teaching_example_ev_premise.ipynb
git commit -m "docs: keep the modified ev background processes in their own databases"
```

---

## Verification

- [ ] `.venv/bin/pytest tests/ -q` — all pass
- [ ] `grep -rn "reversed_database_dates" bw_timex/` — no matches
- [ ] The three premise notebooks execute end to end and no longer write into the
      `ei312_REMIND-EU_SSP2_NDC_*` databases
