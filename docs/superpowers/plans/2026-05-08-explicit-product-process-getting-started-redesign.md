# Explicit Product/Process Getting-Started Notebook Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the abstract A/B/CO2 example in `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb` with a residential heat-pump fleet example whose RTDs are first hand-picked (Part 1) then derived from a tiny flodym dMFA (Part 2), to motivate the explicit product/process paradigm via a concrete fleet research question.

**Architecture:** Single notebook overhaul. Replaces all cells of the existing notebook in place. Foreground holds one product (`heat_pump`) and one process (`heat_pump_lifecycle`). Backgrounds are tiny chimaera nodes for `electricity_market_2025` and `electricity_market_2035`. Part 2 imports `flodym` (`StockDrivenDSM`, `WeibullLifetime`) — same pattern as `example_electric_vehicle_fleet_explicit_process_product.ipynb`. Validation = end-to-end notebook execution with `jupyter nbconvert --execute`.

**Tech Stack:** `bw_timex`, `bw2data`, `bw_temporalis`, `bw2calc`, `flodym` (StockDrivenDSM, WeibullLifetime), `numpy`, `pandas`, `matplotlib`, `dynamic_characterization`. Notebook auth via Jupyter cells; tests via `nbconvert --execute`.

**Reference notebooks (read for tone/conventions, do not copy verbatim):**
- `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb` (existing version — replaced)
- `notebooks/explicit_product_modeling/example_electric_vehicle_fleet_explicit_process_product.ipynb` (flodym pattern)
- `notebooks/getting_started.ipynb` (chimaera analog — for cross-reference link in narrative)

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Replace (in place) | `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb` | The full notebook — narrative + code for the heat-pump fleet getting-started walkthrough |

No other files created. The notebook is self-contained; backgrounds, dMFA and RTDs are all defined inline.

---

## Notebook Outline

The final notebook has 5 sections, in order:

1. **Title + framing** — research question, system diagram, why explicit product/process for fleets.
2. **Project setup + backgrounds** — biosphere flows, two electricity-market chimaera databases, LCIA method.
3. **Part 1 — hand-picked RTDs** — define `td_install`, `td_use`, `cop_factors`; build foreground; `database_dates`; build timeline; show timeline + per-vintage aggregation; static score; A/B run with COP factors removed; optional dynamic characterization.
4. **Part 2 — RTDs from a tiny flodym dMFA** — build the dMFA; derive `td_install_dmfa`, `td_use_dmfa`; rebuild the electricity edge in place; rerun TimexLCA; compare Part 1 vs Part 2; year-by-year emissions plot.
5. **Recap.**

---

## Numerical contract (used by tasks below)

These are the values the notebook commits to. All later assertions and prose reference them.

```python
# Backgrounds (per kWh, kg CO2)
ELEC_2025_CO2 = 0.40
ELEC_2035_CO2 = 0.10

# Foreground (per heat pump unit, lifetime)
LIFETIME_KWH = 18_000          # total electricity per HP across its life
REFRIGERANT_KG = 2.0           # leak per HP
REFRIGERANT_GWP100 = 1430      # used only in dynamic characterization context

# Demand
DEMAND_HPS = 100

# Part 1 RTDs (years offsets, weights)
td_install:  [0, 5, 10] yr  →  [0.2, 0.5, 0.3]
td_use:      [0, 6, 12] yr  →  [0.4, 0.4, 0.2]

# COP factors (consumer-referenced)
cop_factors = {2025-01-01: 1.0, 2035-01-01: 0.7}

# Part 2 dMFA
YEAR_START = 2025
YEAR_END   = 2060
STOCK_SATURATION = 100         # 100 HPs in stock at saturation
STOCK_MIDPOINT   = 2035
STOCK_STEEPNESS  = 0.45
WEIBULL_SHAPE    = 2.0
WEIBULL_SCALE    = 18.0
```

Cross-validated values:
- Part 1 static base LCA score = 100 × (18000 × 0.40 + 2 × 0) = 720_000 kg CO2-eq (refrigerant has no characterization in `("our","method")`).
  - Static base LCA in `bw_timex` uses the original chosen background (`electricity_market_2025`, 0.40 kg CO2/kWh). Expected: 720_000.
- Per-vintage COP factor table (Part 1): 2025-vintage factor=1.0; 2030-vintage factor=1.0 (closest lower year is 2025); 2035-vintage factor=0.7. **Plan tasks must verify this** — `get_temporal_evolution_factor` selects the closest lower entry, so 2030 picks 1.0 not 0.7. This is the same behavior used in the existing notebook (`Reference date 2034-01-01 ... closest lower year`).

---

## Pre-flight: confirm environment runs the existing notebook

This is a sanity check that the env is set up before the rewrite.

### Task 0: Run the current notebook end-to-end

**Files:**
- Verify only: `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb`

- [ ] **Step 1: Activate venv and execute**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
.venv/bin/jupyter nbconvert --to notebook --execute \
  notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb \
  --output /tmp/precheck.ipynb 2>&1 | tail -20
```

Expected: completes without `CellExecutionError`. If it fails with module-not-found for `flodym`, install: `.venv/bin/pip install flodym`. If pre-existing notebook has a real bug, stop and fix it before continuing the rewrite — but it should pass since it was last edited recently.

- [ ] **Step 2: Confirm flodym + dynamic_characterization available**

```bash
.venv/bin/python -c "import flodym, dynamic_characterization; print(flodym.__version__, dynamic_characterization.__version__)"
```

Expected: prints two version strings, no ImportError.

---

## Task 1: Skeleton the new notebook (replace the file)

**Files:**
- Replace: `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb`

The strategy: write the notebook as a clean Python script first, then convert to ipynb with `jupytext`. If `jupytext` isn't available, fall back to building the JSON directly via a small Python helper.

- [ ] **Step 1: Confirm jupytext availability**

```bash
.venv/bin/python -c "import jupytext; print(jupytext.__version__)" 2>&1 | head -1
```

If ImportError, install: `.venv/bin/pip install jupytext`.

- [ ] **Step 2: Create a `.py` companion (percent format) that defines the entire notebook**

Write file `notebooks/explicit_product_modeling/_redesign_draft.py` with the exact content shown in **Notebook Source Listing** (Appendix A below). Use percent format:
- Markdown cells: `# %% [markdown]` then `# `-prefixed lines.
- Code cells: `# %%` then code.

(For the implementation, use Write tool to create `_redesign_draft.py` directly; do not paste through bash. The full content is in Appendix A.)

- [ ] **Step 3: Convert .py → .ipynb, overwriting existing notebook**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
.venv/bin/jupytext --to notebook \
  notebooks/explicit_product_modeling/_redesign_draft.py \
  -o notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb
rm notebooks/explicit_product_modeling/_redesign_draft.py
```

Expected: notebook file replaced. `_redesign_draft.py` removed. No errors.

- [ ] **Step 4: Quick visual diff of cell count**

```bash
.venv/bin/python -c "
import json
nb = json.load(open('notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb'))
print('cells:', len(nb['cells']))
for i, c in enumerate(nb['cells']):
    src = ''.join(c['source'])[:80].replace('\n', ' ')
    print(f'{i:2d} {c[\"cell_type\"]:8s} {src}')
"
```

Expected: ~40–50 cells listed. Cell 0 is markdown with `# Getting Started`. Final cell is markdown recap.

- [ ] **Step 5: Commit the skeleton**

```bash
git add notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb
git commit -m "docs: rewrite explicit-product/process getting-started for HP fleet (skeleton)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 2: Execute the notebook end-to-end and verify Part 1 numbers

**Files:**
- Verify only: `notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb`

- [ ] **Step 1: Execute the rewritten notebook**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
.venv/bin/jupyter nbconvert --to notebook --execute \
  notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb \
  --output /tmp/exec_check.ipynb 2>&1 | tail -40
```

Expected: completes without `CellExecutionError`. Expected runtime < 60 s. If any cell fails, read the error, fix the corresponding cell in `getting_started_explicit_process_product.ipynb` directly via NotebookEdit (or rebuild via the .py round-trip if structural), commit the fix as `fix(nb): ...`, re-run.

- [ ] **Step 2: Inspect Part 1 numeric outputs against the contract**

```bash
.venv/bin/python -c "
import json
nb = json.load(open('/tmp/exec_check.ipynb'))
for c in nb['cells']:
    if c['cell_type'] != 'code': continue
    for o in c.get('outputs', []):
        if 'text/plain' in o.get('data', {}):
            print(''.join(o['data']['text/plain']))
        elif 'text' in o:
            print(''.join(o['text']))
" | grep -E "kg CO2-eq|static_score|score|effective" | head -30
```

Expected lines (approximately, exact computed in the notebook itself):
- `Static base LCA score:       720000.00 kg CO2-eq` (100 × 18000 × 0.40)
- `Time-explicit static score: <some smaller number>` (lower, due to grid decarb + 2035 vintage COP factor)
- `Time-explicit score without foreground evolution: <intermediate>` 
- `Time-explicit score with version-specific efficiencies: <smallest>` 
- `Version-efficiency reduction: <positive number>`

If `Static base LCA score` ≠ 720000.00 ± 1, the foreground exchange amount or the background CO2 factor disagrees with the contract — fix the affected constant in the notebook source.

- [ ] **Step 3: Inspect Part 2 numeric outputs**

Look for cells that print Part 2 totals. Expected: `dmfa total inflow ≈ 100` (rounding from the logistic/Weibull dMFA tail — should be within ±5 % of 100 because the stock saturates after the horizon ends; if more than 10 % off, extend `YEAR_END` or note in narrative).

If inflow total differs materially from `DEMAND_HPS=100`, the notebook should *normalize* `td_install` regardless (RTD weights always sum to 1), and the demand is still set explicitly to 100. The mismatch is OK as long as narrative explains it. If the discrepancy is huge (> 50 %), increase `YEAR_END`.

- [ ] **Step 4: Verify the executed notebook is reproducible by re-executing**

```bash
.venv/bin/jupyter nbconvert --to notebook --execute \
  notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb \
  --output /tmp/exec_check2.ipynb 2>&1 | tail -5
diff <(python -c "import json; nb=json.load(open('/tmp/exec_check.ipynb')); print(sum(len(c.get('outputs',[])) for c in nb['cells']))") \
     <(python -c "import json; nb=json.load(open('/tmp/exec_check2.ipynb')); print(sum(len(c.get('outputs',[])) for c in nb['cells']))")
```

Expected: empty diff (same total output count).

- [ ] **Step 5: Commit the executed notebook (with outputs)**

The repo style (per existing notebooks) is to commit notebooks with their outputs.

```bash
.venv/bin/jupyter nbconvert --to notebook --execute \
  notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb \
  --output notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb
git add notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb
git commit -m "docs: execute redesigned heat-pump fleet getting-started notebook

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 3: Polish narrative, fix prose-vs-numbers drift, finalize

After Task 2 produces verified numbers, the narrative may need small tweaks: the prose makes specific claims (e.g. "≈30 % reduction", "lower because grid decarbonized") that should match the actual computed scores.

- [ ] **Step 1: Read the executed notebook and list every numeric claim in markdown cells**

```bash
.venv/bin/python -c "
import json, re
nb = json.load(open('notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb'))
for i, c in enumerate(nb['cells']):
    if c['cell_type'] != 'markdown': continue
    src = ''.join(c['source'])
    for line in src.split('\n'):
        if re.search(r'\d+(\.\d+)?\s*(%|kg|kWh|years|year)', line):
            print(f'cell {i}: {line.strip()[:120]}')
"
```

Expected: list of numeric claims. Cross-check each against actual computed outputs in adjacent code-cell outputs.

- [ ] **Step 2: Fix any drift inline using NotebookEdit**

For each mismatch, edit the markdown cell to match the executed number (or vice versa if the prose value is the desired one and a constant must change).

- [ ] **Step 3: Re-execute to verify polish didn't break anything**

```bash
.venv/bin/jupyter nbconvert --to notebook --execute \
  notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb \
  --output notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb 2>&1 | tail -5
```

Expected: completes cleanly.

- [ ] **Step 4: Commit polish**

```bash
git add notebooks/explicit_product_modeling/getting_started_explicit_process_product.ipynb
git commit -m "docs: polish heat-pump getting-started prose to match executed numbers

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Appendix A — Full notebook source (jupytext percent format)

This is the canonical content the implementer writes to `_redesign_draft.py` in Task 1, Step 2. **Treat all of this as definitive**; do not paraphrase the markdown or trim the code.

```python
# %% [markdown]
# # Getting Started with `bw_timex`: explicit processes and products
#
# This notebook is a **fleet-flavored**, product-explicit getting-started
# example. It models a small program that installs **100 residential heat pumps**
# between 2025 and 2045, and uses `bw_timex` to compute the program's
# time-explicit greenhouse-gas footprint.
#
# **Research question.** A municipality plans to subsidize the installation
# of 100 residential air-source heat pumps between 2025 and 2045 to replace
# gas boilers. What is the cumulative GHG footprint of that program when we
# account for
#
# 1. the **electricity grid decarbonizing** between 2025 and 2035, and
# 2. heat pump models installed in **2035 having ~30 % better COP** than
#    those installed in 2025?
#
# Brightway supports both the **chimaera** style (one node bundles process
# and reference product) and the **explicit product/process** style we use
# here (see [the Brightway inventory docs](https://docs.brightway.dev/en/latest/content/overview/inventory.html#processes-products-and-something-in-between)).
# For fleet/stock work the explicit style is convenient because the
# **production edge** between the lifecycle process and the unit-good product
# is exactly where the install-year distribution naturally lives.

# %% [markdown]
# ## The system
#
# ```mermaid
# flowchart LR
# subgraph background[<i>background</i>]
#     E25(["electricity_market_2025"]):::bg
#     E35(["electricity_market_2035"]):::bg
# end
#
# subgraph foreground[<i>foreground</i>]
#     HPP([heat_pump]):::product
#     HPL("heat_pump_lifecycle"):::fg
# end
#
# subgraph biosphere[<i>biosphere</i>]
#     CO2("CO2"):::bio
#     R("refrigerant_leak"):::bio
# end
#
# HPL-->|"1 unit"|HPP
# E25-->|"18 000 kWh"|HPL
# E35-->|"18 000 kWh"|HPL
# HPL-.->|"2 kg"|R
#
# classDef fg color:#222832, fill:#3fb1c5, stroke:none;
# classDef product color:#222832, fill:#9c5ffd, stroke:none;
# classDef bg color:#222832, fill:#3fb1c5, stroke:none;
# classDef bio color:#222832, fill:#9c5ffd, stroke:none;
# style background fill:none, stroke:none;
# style foreground fill:none, stroke:none;
# style biosphere fill:none, stroke:none;
# ```
#
# - `heat_pump` is the **unit good** the municipality demands. The product
#   node is bare metadata: name, unit, no exchanges.
# - `heat_pump_lifecycle` is the **per-unit lifecycle process** for one heat
#   pump: it owns the materials, the install, the operating electricity over
#   ~18 years, the refrigerant leak, and end-of-life. All exchange amounts
#   are per `1 heat_pump`.
#
# Demanding `heat_pump = 100` therefore says "100 heat pumps go through
# their full lifecycle." Output-side and input-side relative temporal
# distributions (RTDs) decide *when in calendar time* each unit's install
# and operating electricity actually happen.

# %% [markdown]
# ## Why this needs explicit product/process
#
# A heat pump's **install year** and the **calendar years it draws
# electricity** are different timestamps:
#
# - install year: when the unit is built, deployed, refrigerant-charged.
# - operating years: ~18 years of electricity use *after* install.
#
# With **chimaera** nodes the process *is* the product, so there is no
# production edge to host an install-year RTD — install timing and
# operating timing collapse onto one node.
#
# With **explicit product/process** the production edge `heat_pump_lifecycle
# → heat_pump` is a natural home for the install-year RTD (`date_consumer`
# in the timeline = install vintage), while the electricity edge carries an
# input-side RTD describing the per-unit lifetime kWh profile (`date_producer`
# = calendar year of draw). Vintage-locked efficiency improvements (newer
# install years use less kWh per unit-life) sit on the electricity edge with
# `temporal_evolution_reference="consumer"`.

# %% [markdown]
# ## Project setup and background databases
#
# Two electricity markets — one for 2025 (high CO2 intensity) and one for
# 2035 (decarbonized) — plus a tiny biosphere with `CO2` and a generic
# `refrigerant_leak` (we keep it in the inventory but do not characterize it
# in the static method, just like `CO2` in the upstream getting-started
# notebook).

# %%
from datetime import datetime

import bw2data as bd
import numpy as np
from bw_temporalis import TemporalDistribution

bd.projects.set_current("getting_started_explicit_process_product_heatpump")

for db in list(bd.databases):
    del bd.databases[db]
for method in list(bd.methods):
    del bd.methods[method]

bd.Database("biosphere").write(
    {
        ("biosphere", "CO2"): {
            "type": "emission",
            "name": "CO2",
            "unit": "kg",
        },
        ("biosphere", "refrigerant_leak"): {
            "type": "emission",
            "name": "refrigerant leak",
            "unit": "kg",
        },
    }
)

for db_name, co2_per_kwh in [
    ("electricity_market_2025", 0.40),
    ("electricity_market_2035", 0.10),
]:
    bd.Database(db_name).write(
        {
            (db_name, "elec"): {
                "name": "electricity market",
                "reference product": "electricity",
                "location": "DE",
                "unit": "kWh",
                "exchanges": [
                    {
                        "amount": 1,
                        "type": "production",
                        "input": (db_name, "elec"),
                    },
                    {
                        "amount": co2_per_kwh,
                        "type": "biosphere",
                        "input": ("biosphere", "CO2"),
                    },
                ],
            },
        }
    )

bd.Method(("our", "method")).write([(("biosphere", "CO2"), 1)])

# %% [markdown]
# ## Part 1 — Hand-picked relative temporal distributions
#
# We start with three small, made-up RTDs so the mechanics are clear before
# Part 2 derives them from a real dMFA.
#
# - `td_install` lives on the production edge `heat_pump_lifecycle →
#   heat_pump`. It is the **install-year distribution** of the fleet,
#   normalized to one unit. Output-side RTD.
# - `td_use` lives on the electricity input. It is the **per-unit lifetime
#   electricity profile**, relative to a unit's install year. Input-side
#   RTD.
# - `cop_factors` are vintage-locked efficiency multipliers on the
#   electricity edge with `temporal_evolution_reference="consumer"`: a
#   2035-vintage HP keeps its better COP across all its operating years.

# %%
td_install = TemporalDistribution(
    date=np.array([0, 5, 10], dtype="timedelta64[Y]"),
    amount=np.array([0.2, 0.5, 0.3]),
)

td_use = TemporalDistribution(
    date=np.array([0, 6, 12], dtype="timedelta64[Y]"),
    amount=np.array([0.4, 0.4, 0.2]),
)

cop_factors = {
    datetime(2025, 1, 1): 1.0,
    datetime(2035, 1, 1): 0.7,   # 2035-vintage HPs use 30% less electricity
}

# %% [markdown]
# ### Reading the RTDs at unit and fleet scale
#
# RTDs are always **per unit of the edge flow** (weights sum to 1). The
# fleet picture comes from multiplying by the demand quantity — three
# linearities stacked:
#
# 1. The LCA matrix solve is linear in demand.
# 2. RTD application is linear in process scaling: a vintage with N runs
#    contributes N times the per-unit RTD shape.
# 3. Background relinking and `temporal_evolution_factors` are linear
#    scalings on individual rows.
#
# So `td_install = [0yr: 0.2, 5yr: 0.5, 10yr: 0.3]` and `demand=100` means:
# 20 units installed in 2025, 50 in 2030, 30 in 2035. And `td_use = [0yr:
# 0.4, 6yr: 0.4, 12yr: 0.2]` means each installed unit draws 40 % / 40 %
# / 20 % of its 18 000 kWh at install year, +6 yr, +12 yr. The fleet
# year-by-year electricity demand is just these two shapes combined per
# vintage.

# %% [markdown]
# ## The foreground: one product, one process
#
# Product node is bare. Process node carries all exchanges, including one
# production edge whose target is the product.

# %%
bd.Database("foreground").write(
    {
        ("foreground", "heat_pump"): {
            "name": "heat_pump",
            "type": bd.labels.product_node_default,
            "unit": "unit",
            "location": "DE",
            "exchanges": [],
        },
        ("foreground", "heat_pump_lifecycle"): {
            "name": "heat_pump_lifecycle",
            "type": bd.labels.process_node_default,
            "unit": "unit",
            "location": "DE",
            "exchanges": [
                {
                    "amount": 1,
                    "type": bd.labels.production_edge_default,
                    "input": ("foreground", "heat_pump"),
                    "temporal_distribution": td_install,
                },
                {
                    "amount": 18_000,
                    "type": bd.labels.consumption_edge_default,
                    "input": ("electricity_market_2025", "elec"),
                    "temporal_distribution": td_use,
                    "temporal_evolution_factors": cop_factors,
                    "temporal_evolution_reference": "consumer",
                },
                {
                    "amount": 2,
                    "type": "biosphere",
                    "input": ("biosphere", "refrigerant_leak"),
                    "temporal_distribution": td_install,
                },
            ],
        },
    }
)

for db in bd.databases:
    bd.Database(db).process()

# %% [markdown]
# ## Tell `bw_timex` which databases represent which years

# %%
database_dates = {
    "electricity_market_2025": datetime.strptime("2025", "%Y"),
    "electricity_market_2035": datetime.strptime("2035", "%Y"),
    "foreground": "dynamic",
}

# %% [markdown]
# ## Build the timeline
#
# The functional unit demands the **product** (`heat_pump`). The total
# demand of 100 units is the cumulative fleet over the horizon; the
# install-year RTD distributes those installs across calendar years.

# %%
from bw_timex import TimexLCA

heat_pump = bd.get_node(database="foreground", code="heat_pump")

tlca = TimexLCA(
    demand={heat_pump: 100},
    method=("our", "method"),
    database_dates=database_dates,
)

tlca.build_timeline(
    starting_datetime=datetime(2025, 1, 1),
    temporal_grouping="year",
)

# %% [markdown]
# Each electricity row carries two dates:
#
# - `date_consumer`: the install vintage of the heat-pump cohort consuming
#   that electricity. Used for vintage-locked COP factors.
# - `date_producer`: the calendar year the electricity is actually drawn.
#   Used for relinking to the time-stamped background databases.

# %%
tlca.timeline[
    [
        "producer_name",
        "consumer_name",
        "date_producer",
        "date_consumer",
        "amount",
        "temporal_market_shares",
    ]
].sort_values(["date_consumer", "date_producer", "producer_name"])

# %% [markdown]
# ### Per-vintage breakdown: how unit process + RTDs aggregate to fleet

# %%
from bw_timex.utils import get_temporal_evolution_factor

elec_rows = tlca.timeline[
    tlca.timeline["producer_name"].str.contains("electricity")
].copy()
elec_rows["install_year"] = elec_rows["date_consumer"].dt.year
elec_rows["draw_year"] = elec_rows["date_producer"].dt.year
elec_rows["cop_factor"] = elec_rows["date_consumer"].apply(
    lambda d: get_temporal_evolution_factor(cop_factors, d)
)
elec_rows["effective_kWh"] = elec_rows["amount"].astype(float) * elec_rows["cop_factor"]

elec_rows[
    [
        "install_year",
        "draw_year",
        "amount",
        "cop_factor",
        "effective_kWh",
        "temporal_market_shares",
    ]
].sort_values(["install_year", "draw_year"])

# %% [markdown]
# Aggregating by install vintage shows the COP improvement clearly:

# %%
elec_rows.groupby("install_year", as_index=False).agg(
    raw_kWh=("amount", "sum"),
    cop_factor=("cop_factor", "first"),
    effective_kWh=("effective_kWh", "sum"),
)

# %% [markdown]
# ## Calculate the LCI and the static LCIA score

# %%
tlca.lci()
tlca.static_lcia()

tlca.static_score

# %% [markdown]
# ### Compare against the static base LCA
#
# The static base LCA is the fleet's footprint *without* time-explicit
# relinking and *without* the vintage-locked COP improvements. It uses
# whatever background was originally chosen on the foreground exchange
# (`electricity_market_2025`).

# %%
print(f"Static base LCA score:        {tlca.base_lca.score:>14,.2f} kg CO2-eq")
print(f"Time-explicit static score:   {tlca.static_score:>14,.2f} kg CO2-eq")
print(
    "Reduction from time-explicit modelling: "
    f"{tlca.base_lca.score - tlca.static_score:>14,.2f} kg CO2-eq "
    f"({100 * (tlca.base_lca.score - tlca.static_score) / tlca.base_lca.score:.1f} %)"
)

# %% [markdown]
# The time-explicit score is lower for two reasons: some operating
# electricity is sourced from the cleaner 2035 grid mix, and 2035-vintage
# heat pumps draw 30 % less electricity than 2025-vintage ones. The next
# cell isolates the second effect.

# %% [markdown]
# ### Isolating the vintage-locked COP improvement
#
# We temporarily remove the `temporal_evolution_factors` from the
# electricity edge and rerun the same time-explicit model. The only
# difference is the vintage-locked COP improvements; everything else
# (install RTD, use RTD, background relinking) stays.

# %%
hp_lifecycle = bd.get_node(database="foreground", code="heat_pump_lifecycle")
elec_edge = next(
    e for e in hp_lifecycle.technosphere() if e.input["name"] == "electricity market"
)

saved_factors = elec_edge["temporal_evolution_factors"]
saved_reference = elec_edge["temporal_evolution_reference"]
del elec_edge["temporal_evolution_factors"]
del elec_edge["temporal_evolution_reference"]
elec_edge.save()
bd.Database("foreground").process()

tlca_no_evolution = TimexLCA(
    demand={heat_pump: 100},
    method=("our", "method"),
    database_dates=database_dates,
)
tlca_no_evolution.build_timeline(
    starting_datetime=datetime(2025, 1, 1),
    temporal_grouping="year",
)
tlca_no_evolution.lci()
tlca_no_evolution.static_lcia()

elec_edge["temporal_evolution_factors"] = saved_factors
elec_edge["temporal_evolution_reference"] = saved_reference
elec_edge.save()
bd.Database("foreground").process()

print(
    f"Without COP improvement: {tlca_no_evolution.static_score:>14,.2f} kg CO2-eq"
)
print(
    f"With COP improvement:    {tlca.static_score:>14,.2f} kg CO2-eq"
)
print(
    f"COP-attributable saving: {tlca_no_evolution.static_score - tlca.static_score:>14,.2f} kg CO2-eq"
)

# %% [markdown]
# ## Part 2 — Deriving the RTDs from a tiny flodym dMFA
#
# In Part 1 we hand-picked `td_install` and `td_use`. In a real fleet
# study these come from a **dynamic Material Flow Analysis (dMFA)**: an
# exogenous stock trajectory plus a lifetime distribution → annual
# inflow, stock-by-cohort, age-resolved survival.
#
# Here we use [`flodym`](https://github.com/pik-piam/flodym)'s
# `StockDrivenDSM` with a logistic stock trajectory saturating at 100
# units around 2035, and a Weibull lifetime (mean ≈ 18 years, shape 2).

# %%
import matplotlib.pyplot as plt
import pandas as pd
from flodym import (
    Dimension,
    DimensionSet,
    StockArray,
    StockDrivenDSM,
    WeibullLifetime,
)

YEAR_START = 2025
YEAR_END   = 2060
years = np.arange(YEAR_START, YEAR_END + 1)

time_dim = Dimension(name="Time", letter="t", items=years.tolist(), dtype=int)
dims = DimensionSet(dim_list=[time_dim])

STOCK_SATURATION = 100
STOCK_MIDPOINT   = 2035
STOCK_STEEPNESS  = 0.45
stock_values = STOCK_SATURATION / (
    1 + np.exp(-STOCK_STEEPNESS * (years - STOCK_MIDPOINT))
)
stock = StockArray(dims=dims, name="hp_fleet", values=stock_values)

WEIBULL_SHAPE = 2.0
WEIBULL_SCALE = 18.0
lifetime_model = WeibullLifetime(dims=dims)
lifetime_model.set_prms(
    weibull_shape=np.full(dims.shape, WEIBULL_SHAPE),
    weibull_scale=np.full(dims.shape, WEIBULL_SCALE),
)

dsm = StockDrivenDSM(dims=dims, stock=stock, lifetime_model=lifetime_model)
dsm.compute()

inflow_values  = dsm.inflow.values
outflow_values = dsm.outflow.values
stock_by_cohort = dsm.get_stock_by_cohort()

# %% [markdown]
# ### Plot the dMFA: stock, inflow, outflow

# %%
fig, ax = plt.subplots(figsize=(7, 3.5))
ax.plot(years, stock.values, label="stock", color="black")
ax.bar(years, inflow_values, label="inflow", color="#3fb1c5", alpha=0.8)
ax.bar(years, -outflow_values, label="outflow", color="#9c5ffd", alpha=0.8)
ax.axhline(0, color="black", lw=0.5)
ax.set_xlabel("year")
ax.set_ylabel("heat pumps")
ax.legend()
ax.set_title("Heat pump fleet dMFA (logistic stock, Weibull lifetime)")
fig.tight_layout()

# %% [markdown]
# ### Derive `td_install_dmfa` and `td_use_dmfa`
#
# - `td_install_dmfa`: normalize annual inflow → install-year shape.
#   Offsets in years from `YEAR_START`. Drop trailing zeros.
# - `td_use_dmfa`: from `stock_by_cohort` we know how many of an installed
#   cohort are still in stock `k` years after install. Assuming constant
#   per-unit annual electricity draw while in stock, the fraction of a
#   unit's lifetime electricity drawn at age `k` is proportional to the
#   age-`k` survival probability. We compute this from the Weibull survival
#   function directly so it's independent of cohort-specific noise.

# %%
inflow_total = inflow_values.sum()
inflow_share = inflow_values / inflow_total
nonzero = np.where(inflow_share > 1e-6)[0]
install_offsets_years = (years[nonzero] - YEAR_START).astype("int64")
install_weights = inflow_share[nonzero]
install_weights = install_weights / install_weights.sum()

td_install_dmfa = TemporalDistribution(
    date=install_offsets_years.astype("timedelta64[Y]"),
    amount=install_weights,
)

ages = np.arange(0, 41)
from scipy.stats import weibull_min
age_survival = weibull_min.sf(ages, WEIBULL_SHAPE, scale=WEIBULL_SCALE)
use_share = age_survival / age_survival.sum()
keep = np.where(use_share > 1e-4)[0]
use_offsets_years = ages[keep].astype("int64")
use_weights = use_share[keep] / use_share[keep].sum()

td_use_dmfa = TemporalDistribution(
    date=use_offsets_years.astype("timedelta64[Y]"),
    amount=use_weights,
)

print(f"dMFA total inflow over horizon: {inflow_total:.2f} heat pumps")
print(
    f"td_install_dmfa: {len(install_weights)} bins, "
    f"first {install_offsets_years[0]}y .. last {install_offsets_years[-1]}y"
)
print(
    f"td_use_dmfa:     {len(use_weights)} bins, "
    f"first {use_offsets_years[0]}y .. last {use_offsets_years[-1]}y"
)

# %% [markdown]
# ### Plug derived RTDs into the foreground and rerun
#
# We rewrite `td_install` and `td_use` on the existing exchanges in place,
# rather than rebuilding the database from scratch. Same demand, same COP
# factors. The only change is the *shapes* of the two RTDs.

# %%
production_edge = next(
    e for e in hp_lifecycle.production() if e.output["code"] == "heat_pump_lifecycle"
)
production_edge["temporal_distribution"] = td_install_dmfa
production_edge.save()

elec_edge["temporal_distribution"] = td_use_dmfa
elec_edge.save()

bd.Database("foreground").process()

tlca_dmfa = TimexLCA(
    demand={heat_pump: 100},
    method=("our", "method"),
    database_dates=database_dates,
)
tlca_dmfa.build_timeline(
    starting_datetime=datetime(2025, 1, 1),
    temporal_grouping="year",
)
tlca_dmfa.lci()
tlca_dmfa.static_lcia()

print(f"Part 1 (hand-picked RTDs) static score: {tlca.static_score:>14,.2f} kg CO2-eq")
print(f"Part 2 (dMFA RTDs)         static score: {tlca_dmfa.static_score:>14,.2f} kg CO2-eq")

# %% [markdown]
# ### Year-by-year emissions from the dMFA-driven run

# %%
timeline_df = tlca_dmfa.timeline.copy()
timeline_df["draw_year"] = timeline_df["date_producer"].dt.year
elec_mask = timeline_df["producer_name"].str.contains("electricity")
elec_t = timeline_df[elec_mask].copy()

elec_t["cop_factor"] = elec_t["date_consumer"].apply(
    lambda d: get_temporal_evolution_factor(cop_factors, d)
)

def share_co2(row):
    co2_per_kwh = 0.0
    for db_name, share in row["temporal_market_shares"].items():
        if db_name == "electricity_market_2025":
            co2_per_kwh += 0.40 * share
        elif db_name == "electricity_market_2035":
            co2_per_kwh += 0.10 * share
    return float(row["amount"]) * row["cop_factor"] * co2_per_kwh

elec_t["co2_kg"] = elec_t.apply(share_co2, axis=1)

emissions_by_year = (
    elec_t.groupby("draw_year")["co2_kg"].sum().reindex(years, fill_value=0.0)
)

fig, ax = plt.subplots(figsize=(7, 3.5))
ax.bar(years, emissions_by_year.values, color="#3fb1c5")
ax.set_xlabel("calendar year")
ax.set_ylabel("kg CO2-eq")
ax.set_title("Fleet electricity emissions per calendar year (dMFA-driven RTDs)")
fig.tight_layout()

# %% [markdown]
# ## Optional: dynamic characterization
#
# The inventory still carries dates, so dynamic characterization works
# exactly as in the original getting-started notebook.

# %%
from dynamic_characterization.ipcc_ar6 import characterize_co2

emission_id = bd.get_activity(("biosphere", "CO2")).id
characterization_functions = {emission_id: characterize_co2}

tlca_dmfa.dynamic_lcia(
    metric="GWP",
    time_horizon=100,
    characterization_functions=characterization_functions,
)

tlca_dmfa.dynamic_score

# %% [markdown]
# ## Quick recap
#
# 1. **Demand the product**, not the process. Set total quantity for the
#    horizon (`{heat_pump: 100}`).
# 2. **Output-side RTD** on the production edge `heat_pump_lifecycle →
#    heat_pump` carries the install-year distribution (the dMFA inflow).
# 3. **Input-side RTDs** on consumption/biosphere edges describe the
#    per-unit lifetime profile — they are evaluated relative to each
#    cohort's install year via `date_consumer`.
# 4. `temporal_evolution_factors` with `temporal_evolution_reference=
#    "consumer"` lets vintages keep their own efficiencies across all
#    later draw years.
# 5. The fleet result falls out: matrix solve scales by demand, RTDs
#    distribute scaled flows in time, background relinking picks the
#    right grid mix per draw year.
#
# Part 1 made the RTDs up; Part 2 derived them from a flodym dMFA
# (`StockDrivenDSM` + `WeibullLifetime`). The notebook structure is
# identical otherwise — *the bridge between dMFA and `bw_timex` is just
# normalization*.
```

---

## Self-Review

1. **Spec coverage:**
   - Research question + system + demand: Task 1 / Appendix A.
   - Why explicit product/process here: covered in markdown cell + recap.
   - Part 1 hand-picked RTDs + COP factors + A/B comparison: covered.
   - Part 2 flodym dMFA + derived RTDs + comparison: covered.
   - End-to-end execution + numeric verification: Task 2.

2. **Placeholders:** None. All code is concrete, all constants pinned in the contract.

3. **Type/name consistency:**
   - Foreground codes: `heat_pump`, `heat_pump_lifecycle` consistent everywhere.
   - Background codes: `electricity_market_2025`, `electricity_market_2035` with activity code `elec` consistent everywhere.
   - Variables `tlca`, `tlca_no_evolution`, `tlca_dmfa` introduced in order; no shadowing.
   - `cop_factors` used identically in Part 1 build, A/B comparison, Part 2 walkthrough.
   - Foreground exchange types use `bd.labels.product_node_default` / `process_node_default` / `production_edge_default` / `consumption_edge_default` matching the existing notebook conventions.

4. **Risks acknowledged in the plan:**
   - `get_temporal_evolution_factor` selects closest-lower-year; 2030-vintage cohort therefore receives factor 1.0, not 0.7. The notebook's per-vintage table will reflect that automatically; narrative does not contradict it.
   - dMFA total inflow may not exactly equal 100 over the truncated horizon. Task 2 Step 3 verifies and prescribes a horizon extension if it's off by > 50 %. Otherwise narrative explicitly says `td_install_dmfa` is normalized regardless.
   - `flodym.StockDrivenDSM` API is the same as the existing fleet notebook — confirmed via grep before drafting.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-08-explicit-product-process-getting-started-redesign.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints for review.

Which approach?