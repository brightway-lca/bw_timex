# Zensical Documentation Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Sphinx / pydata-sphinx-theme documentation build of `bw_timex` with a Zensical build that mirrors the sibling project `optimex`, adapting every Sphinx-only feature the current docs rely on.

**Architecture:** A root `zensical.toml` replaces `docs/conf.py`; MyST directives in `docs/**/*.md` are rewritten to Material for MkDocs syntax; `sphinx-autoapi` is replaced by handwritten mkdocstrings pages under `docs/api/`; the four executed example notebooks are converted to Markdown by a `docs/convert_notebooks.py` script ported from `optimex`; Read the Docs switches from a conda + Sphinx build to a `build.commands` block running `zensical build`.

**Tech Stack:** zensical (0.0.37 at time of writing), mkdocstrings[python], mkdocs-autorefs, pymdown-extensions >= 10.0, nbconvert >= 7.0, MathJax 3 via CDN.

**Spec:** `superpowers/specs/2026-07-31-zensical-docs-migration-design.md`

## Global Constraints

- **Reference implementation:** `/Users/timodiepers/Documents/Coding/optimex` — copy its file layout, `zensical.toml` structure, `docs/convert_notebooks.py`, `docs/javascripts/source-overrides.js` and `docs/stylesheets/extra.css` rather than inventing new patterns. Deviations are listed per task.
- **Prose is ported, not rewritten.** Only syntax changes. Do not reword paragraphs, retitle sections, or reorder content unless a step says so.
- **No "grid cards" on the landing page or the API pages** (explicit user decision). The examples index keeps cards because it already uses them today.
- **Tabbed navigation** grouped like optimex: Home / User Guide / Theory / Examples / API Reference, with `navigation.tabs` + `navigation.indexes` enabled. Sections that own an index page (Getting Started, Examples, API Reference) use it as the section landing page, with their sub-pages nested beneath. (This reverses an earlier flat-nav decision; commits `10ab75a` and later.)
- **Python tooling is `uv`.** Use `uv pip install`, never bare `pip` or `conda`, for local work. The `.venv` in the repo root is Python 3.12.12 and already has `bw_timex` installed editable.
- **Commit messages carry no Claude attribution trailers.** No `Co-Authored-By: Claude`, no `Claude-Session:`.
- **Branch:** `docs/zensical-migration`, already created off `origin/main` @ `5f158e3`. Do not merge or open a PR.
- **Admonition syntax:** classic Material (`!!! type "Title"`) with **4-space indented body**. Every nested code fence, image, and mermaid block inside an admonition must be indented 4 spaces too.
- **Light/dark images:** Zensical supports this natively via the URL fragments `#only-light` and `#only-dark`. Verified in `zensical/templates/assets/stylesheets/*/palette.*.min.css`. **Do not write custom CSS for it.**
- **Custom CSS is capped at optimex parity.** `docs/stylesheets/extra.css` is optimex's stylesheet verbatim, minus its project-specific `.example-flowchart` rule. **Do not add a single rule beyond it.** Where the Sphinx docs used a bespoke style, use the nearest built-in Zensical/Material feature instead — a built-in admonition type, a built-in palette color, a built-in theme feature. Brand color comes from the built-in `light-green` palette in `zensical.toml`, not from CSS variables. If a step appears to require a new CSS rule, that step is wrong: report it rather than writing the rule.
- **Do not delete or modify anything under `docs/content/data/`, `docs/content/examples/data/`, or `docs/_static/`** except where a step says so. 12 SVGs in `content/data/` are already unreferenced today; leave them.
- **Do not touch the top-level `notebooks/` directory.** The notebooks the docs render live in `docs/content/examples/` and are separate, output-bearing copies.

---

## File Structure

**Created:**
- `zensical.toml` — site config, nav, theme, markdown extensions, mkdocstrings config
- `docs/stylesheets/extra.css` — optimex's stylesheet verbatim, minus its `.example-flowchart` rule
- `docs/requirements.txt` — docs build dependencies for RTD
- `docs/javascripts/source-overrides.js` — repoints edit/view buttons on notebook-derived pages
- `docs/convert_notebooks.py` — nbconvert wrapper for the four example notebooks
- `docs/api/index.md` + 7 module pages — mkdocstrings API reference

**Modified:**
- `docs/index.md`, `docs/content/installation.md`, `docs/content/theory.md`, `docs/content/decisiontree.md`, `docs/content/examples/index.md`, `docs/content/getting_started/*.md` (5 files), `docs/content/{license,contributing,changelog,codeofconduct,funding}.md`
- `.readthedocs.yaml`, `pyproject.toml`, `.gitignore`

**Deleted:**
- `docs/conf.py`, `docs/environment.yaml`, `docs/_templates/` (incl. `autoapi_templates/`), `docs/_static/custom.css`, `docs/content/other/`

---

### Task 1: Zensical build scaffolding

Stands up the build so every later task can verify itself with a real build. Nav covers only the pages that exist right now; API and notebook pages are added by their own tasks.

**Files:**
- Create: `zensical.toml`, `docs/requirements.txt`, `docs/stylesheets/extra.css`
- Modify: `.gitignore`

**Interfaces:**
- Produces: a working `zensical build` writing to `site/`; the `nav` array that Tasks 8 and 9 extend.

- [ ] **Step 1: Install the docs toolchain**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
cat > docs/requirements.txt <<'EOF'
zensical
mkdocstrings[python]>=1.0.0
mkdocs-autorefs
pymdown-extensions>=10.0
nbconvert>=7.0
EOF
uv pip install --python .venv/bin/python -r docs/requirements.txt
```

Expected: installs cleanly. Confirm with `.venv/bin/zensical --version`.

- [ ] **Step 2: Write `zensical.toml`**

Create at the repository root. This is the complete file:

```toml
[project]
site_name = "bw_timex"
site_description = "Time-explicit Life Cycle Assessment"
site_author = "Timo Diepers, Amelie Müller, Arthur Jakobs"
site_url = "https://docs.brightway.dev/projects/bw-timex/en/latest/"

repo_name = "brightway-lca/bw_timex"
repo_url = "https://github.com/brightway-lca/bw_timex"
edit_uri = "edit/main/docs/"

copyright = "Copyright &copy; 2026 bw_timex developers"

nav = [
  { Overview = "index.md" },
  { Installation = "content/installation.md" },
  { "Getting Started" = [
    { Overview = "content/getting_started/index.md" },
    { "Step 1 - Adding temporal information" = "content/getting_started/adding_temporal_information.md" },
    { "Step 2 - Building the process timeline" = "content/getting_started/build_process_timeline.md" },
    { "Step 3 - Calculating the time-explicit LCI" = "content/getting_started/time_explicit_lci.md" },
    { "Step 4 - Impact assessment" = "content/getting_started/lcia.md" },
  ]},
  { Theory = "content/theory.md" },
  { Examples = [
    { Overview = "content/examples/index.md" },
  ]},
  { "What LCA should I do?" = "content/decisiontree.md" },
  { "Modeling paradigms" = "content/dev/explicit_process_product_paradigm.md" },
  { Contributing = "content/contributing.md" },
  { "Code of Conduct" = "content/codeofconduct.md" },
  { License = "content/license.md" },
  { Changelog = "content/changelog.md" },
  { Funding = "content/funding.md" },
]

extra_javascript = [
  "https://cdnjs.cloudflare.com/ajax/libs/mathjax/3.2.0/es5/tex-mml-chtml.min.js",
  "javascripts/source-overrides.js",
]

extra_css = [
  "stylesheets/extra.css",
]

[project.theme]
language = "en"
logo = "_static/favicon.svg"
favicon = "_static/favicon.svg"
features = [
  "announce.dismiss",
  "content.action.edit",
  "content.action.view",
  "content.code.copy",
  "content.code.annotate",
  "content.tabs.link",
  "content.tooltips",
  "navigation.footer",
  "navigation.instant",
  "navigation.instant.prefetch",
  "navigation.path",
  "navigation.top",
  "navigation.tracking",
  "search.highlight",
  "search.share",
  "search.suggest",
  "toc.follow",
]

[[project.theme.palette]]
media = "(prefers-color-scheme: light)"
scheme = "default"
primary = "light-green"
accent = "light-green"
toggle.icon = "lucide/sun"
toggle.name = "Switch to dark mode"

[[project.theme.palette]]
media = "(prefers-color-scheme: dark)"
scheme = "slate"
primary = "light-green"
accent = "light-green"
toggle.icon = "lucide/moon-star"
toggle.name = "Switch to light mode"

[project.theme.icon]
repo = "fontawesome/brands/github"

[[project.extra.social]]
icon = "fontawesome/brands/github"
link = "https://github.com/brightway-lca/bw_timex"
name = "Open this Repo on GitHub"

[[project.extra.social]]
icon = "lucide/rocket"
link = "https://mybinder.org/v2/gh/brightway-lca/bw_timex/HEAD?labpath=notebooks%2Fgetting_started.ipynb"
name = "Launch interactive Demo on Binder"

[project.markdown_extensions.abbr]
[project.markdown_extensions.admonition]
[project.markdown_extensions.attr_list]
[project.markdown_extensions.def_list]
[project.markdown_extensions.footnotes]
[project.markdown_extensions.md_in_html]
[project.markdown_extensions.toc]
permalink = true
[project.markdown_extensions.pymdownx.arithmatex]
generic = true
[project.markdown_extensions.pymdownx.betterem]
smart_enable = "all"
[project.markdown_extensions.pymdownx.caret]
[project.markdown_extensions.pymdownx.details]
[project.markdown_extensions.pymdownx.emoji]
emoji_index = "zensical.extensions.emoji.twemoji"
emoji_generator = "zensical.extensions.emoji.to_svg"
[project.markdown_extensions.pymdownx.highlight]
anchor_linenums = true
line_spans = "__span"
pygments_lang_class = true
[project.markdown_extensions.pymdownx.inlinehilite]
[project.markdown_extensions.pymdownx.keys]
[project.markdown_extensions.pymdownx.mark]
[project.markdown_extensions.pymdownx.smartsymbols]
[project.markdown_extensions.pymdownx.snippets]
[project.markdown_extensions.pymdownx.superfences]
custom_fences = [
  { name = "mermaid", class = "mermaid", format = "pymdownx.superfences.fence_code_format" },
]
[project.markdown_extensions.pymdownx.tabbed]
alternate_style = true
[project.markdown_extensions.pymdownx.tasklist]
custom_checkbox = true
[project.markdown_extensions.pymdownx.tilde]

[project.plugins.mkdocstrings]
# Temporarily disabled: mkdocstrings registers a block processor that fires on any
# line starting with ":::", which collides fatally with the MyST colon-fence
# directives still present in theory.md, installation.md, examples/index.md and
# getting_started/adding_temporal_information.md. Task 9 flips this to true, by
# which point Tasks 3/4/5/7 have removed every colon fence.
enabled = false
default_handler = "python"

[project.plugins.mkdocstrings.handlers.python]
paths = ["."]

[project.plugins.mkdocstrings.handlers.python.options]
docstring_style = "numpy"
docstring_section_style = "table"
show_root_heading = true
show_root_toc_entry = true
show_root_full_path = false
show_object_full_path = false
show_category_heading = true
show_symbol_type_heading = true
show_symbol_type_toc = true
members_order = "source"
group_by_category = true
show_if_no_docstring = false
show_signature = true
show_signature_annotations = true
separate_signature = false
signature_crossrefs = true
merge_init_into_class = true
show_source = true
inherited_members = false
filters = ["!^_", "!^__"]

[project.plugins.tags]
```

One deliberate difference from `optimex`: `mkdocstrings.handlers.python.paths = ["."]`, because `bw_timex` uses a flat package layout, not `src/`.

- [ ] **Step 3: Copy optimex's stylesheet**

Copy `/Users/timodiepers/Documents/Coding/optimex/docs/stylesheets/extra.css` to
`docs/stylesheets/extra.css` and delete only its `.example-flowchart` rule (optimex-specific).
Add nothing else — no brand-palette block, no admonition rules. Brand color comes from the
built-in `light-green` palette already set in the `zensical.toml` above.

- [ ] **Step 4: Ignore the build output**

Append to `.gitignore`:

```
# Zensical build output
/site
```

- [ ] **Step 5: Build and check the palette contingency**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
.venv/bin/zensical build 2>&1 | tail -30
```

Expected: build completes and `site/index.html` exists. Pages will render badly (raw MyST directives) — that is expected at this stage; only the build succeeding matters.

- [ ] **Step 6: Confirm the nav rendered**

```bash
grep -c 'md-nav__link' site/index.html
```

Expected: a non-zero count, and `grep -o 'Modeling paradigms' site/index.html` finds the entry — proving the nav including the previously orphaned dev page was picked up.

- [ ] **Step 7: Commit**

```bash
git add zensical.toml docs/requirements.txt docs/stylesheets/extra.css .gitignore
git commit -m "docs: add Zensical build configuration"
```

---

### Task 2: Landing page and file-include pages

**Files:**
- Modify: `docs/index.md`, `docs/content/license.md`, `docs/content/contributing.md`, `docs/content/changelog.md`, `docs/content/codeofconduct.md`, `docs/content/funding.md`

**Interfaces:**
- Consumes: `pymdownx.snippets` from Task 1, whose base path is the process working directory — i.e. the repository root, which is where `zensical build` runs. Paths in `--8<--` are therefore repo-root-relative.

- [ ] **Step 1: Strip the toctree from `docs/index.md`**

Delete the entire trailing fenced block starting at ` ```{toctree} ` and ending at the closing ` ``` ` (lines 32-48). Change nothing else on the page — no cards, no front-matter, no rewording.

- [ ] **Step 2: Convert the four whole-file includes**

`docs/content/contributing.md` becomes exactly:

```markdown
--8<-- "CONTRIBUTING.md"
```

`docs/content/changelog.md`:

```markdown
--8<-- "CHANGES.md"
```

`docs/content/codeofconduct.md`:

```markdown
--8<-- "CODE_OF_CONDUCT.md"
```

`docs/content/funding.md`:

```markdown
--8<-- "FUNDING.md"
```

- [ ] **Step 3: Convert the literalinclude**

`docs/content/license.md` becomes:

````markdown
# License

```
--8<-- "LICENSE"
```
````

The `{literalinclude}` used `language: none`, so a plain unlabelled fence is the equivalent.

- [ ] **Step 4: Build and verify the includes resolved**

```bash
.venv/bin/zensical build 2>&1 | tail -20
grep -c 'Redistribution and use in source and binary forms' site/content/license/index.html
grep -c 'toctree' site/index.html
```

Expected: license grep returns `1` (the BSD text was inlined, not a broken `--8<--` literal); toctree grep returns `0`. Also confirm `grep -c '8<' site/content/changelog/index.html` returns `0`.

- [ ] **Step 5: Commit**

```bash
git add docs/index.md docs/content/license.md docs/content/contributing.md docs/content/changelog.md docs/content/codeofconduct.md docs/content/funding.md
git commit -m "docs: convert landing page and file includes to Zensical syntax"
```

---

### Task 3: Installation page

**Files:**
- Modify: `docs/content/installation.md`

- [ ] **Step 1: Convert the two tab sets**

Replace each `::::{tab-set}` / `:::{tab-item} TITLE` construct with `pymdownx.tabbed` syntax. **Tab bodies must be indented 4 spaces.** The first tab set becomes:

````markdown
=== "Linux, Windows, or MacOS (x64)"

    1. Install `python` from [the website](https://www.python.org/downloads/), your system package manager, or [Homebrew](https://docs.brew.sh/Homebrew-and-Python).

    2. Create a directory for your virtual environments, such as `C:/Users/me/virtualenvs/`.

    3. In a console or terminal window, create a new virtual environment:

    ```console
    python -m venv C:/Users/me/virtualenvs/timex
    ```

    ... (remaining steps 4-5, indented identically)

=== "MacOS (Apple Silicon/ARM)"

    !!! note

        Fast calculations need `SuiteSparse` through [scikit-umfpack](https://github.com/scikit-umfpack/scikit-umfpack/). This background library can be installed via [homebrew](https://brew.sh/), as shown in this section, or via `conda` or `mamba`, as shown below.

    ... (remaining steps, indented identically)
````

Apply the same treatment to the second tab set under "Installing `bw_timex` using `conda` or `mamba`", whose tabs are titled "Linux, Windows, or MacOS (x64)" and "macOS (Apple Silicon/ARM)". Note the second tab set is missing its closing `::::` in the source; the converted version simply ends at the end of the file.

The bare ```` ``` ```` fences in the conda ARM tab (two of them, with no language) should become ```` ```console ```` for consistency with their siblings.

- [ ] **Step 2: Convert the three admonitions**

The page-level note near the top:

```markdown
!!! note

    `bw_timex` depends on Brightway25, and will install bw25-compatible versions of the bw packages. This means that it cannot be added to existing environments that are based on Brightway2, e.g., environments containing [Activity Browser](https://github.com/LCA-ActivityBrowser/activity-browser).
```

The `{admonition} Prerequisites` / `:class: important` block:

```markdown
!!! important "Prerequisites"

    1. A working installation of [`conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) or [`mamba`](https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html). If you are using `conda`, we recommend installing the [libmamba solver](https://www.anaconda.com/blog/a-faster-conda-for-a-growing-community).
    2. Basic knowledge of [Conda environments](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
```

And the `{note}` inside the conda ARM tab, indented one further level (8 spaces for its body, since it sits inside a tab).

- [ ] **Step 3: Build and verify**

```bash
.venv/bin/zensical build 2>&1 | tail -20
grep -c 'tabbed-set' site/content/installation/index.html
grep -c 'admonition important' site/content/installation/index.html
grep -c 'tab-set\|tab-item' site/content/installation/index.html
```

Expected: `tabbed-set` returns `2`, `admonition important` returns `1`, and the raw-directive grep returns `0`.

- [ ] **Step 4: Commit**

```bash
git add docs/content/installation.md
git commit -m "docs: convert installation tabs and admonitions to Zensical syntax"
```

---

### Task 4: Getting Started section

**Files:**
- Modify: `docs/content/getting_started/index.md`, `docs/content/getting_started/adding_temporal_information.md`, `docs/content/getting_started/lcia.md`, `docs/content/getting_started/build_process_timeline.md`, `docs/content/getting_started/time_explicit_lci.md`

**Interfaces:**
- Consumes: nothing beyond the build config from Task 1.

- [ ] **Step 1: Convert the images on `getting_started/index.md`**

The light/dark `{image}` pair (lines 5-15) becomes two Markdown images using the native fragment convention, with `attr_list` carrying the sizing that `:height:`/`:align:` used to provide:

```markdown
![Overview of the four bw_timex steps](../data/method_small_steps_light.svg#only-light){ style="height:450px;display:block;margin:0 auto" }
![Overview of the four bw_timex steps](../data/method_small_steps_dark.svg#only-dark){ style="height:450px;display:block;margin:0 auto" }
```

- [ ] **Step 2: Convert the launch admonition on the same page**

The pydata theme styled this one with a bespoke `admonition-launch` class and a FontAwesome rocket. There is no rocket among Zensical's built-in admonition types, and no CSS rule may be added beyond optimex's stylesheet, so use the built-in `tip` type — the closest match in intent for a "try this yourself" callout:

```markdown
!!! tip "You want more interaction?"

    [Launch this tutorial on Binder!](https://mybinder.org/v2/gh/brightway-lca/bw_timex/HEAD?labpath=notebooks%2Fgetting_started.ipynb) In this interactive environment, you can directly run the bw_timex code yourself whilst following along.
```

Then delete the trailing `{toctree}` block from the page (the nav in `zensical.toml` covers it).

- [ ] **Step 3: Convert the three dropdowns in `adding_temporal_information.md`**

Each `:::{dropdown} <span …>TEXT</style>` with `:icon: codescan` becomes a collapsed details block. Note the source has mismatched `<span>…</style>` tags — drop the inline HTML entirely and use a plain title:

```markdown
??? note "Here's the code to set this up with brightway - but this is not essential here"

    ```python
    ... (existing code, indented 4 spaces)
    ```
```

Titles for the other two: "Here's the code to add this information to our modeled production system in Brightway" and "Again, here's the code in case you're interested". The `:icon: codescan` option has no Material equivalent and is dropped; the default note icon is used.

- [ ] **Step 4: Convert the two images in `lcia.md`**

These have no light/dark variants, only `:align: center` and `:alt:`:

```markdown
![Plot showing the radiative forcing over time](../data/dynamic_characterized_inventory_radiative_forcing.svg){ style="display:block;margin:0 auto" }
```

and the same pattern for `dynamic_characterized_inventory_gwp.svg`. Keep the surrounding `<br />` tags as they are.

- [ ] **Step 5: Check the remaining two pages**

`build_process_timeline.md` and `time_explicit_lci.md` contain only headings, prose, Python fences and a pipe table — all valid Material Markdown. Read them and confirm no MyST directive remains; make no edits if none is found.

- [ ] **Step 6: Build and verify**

```bash
.venv/bin/zensical build 2>&1 | tail -20
grep -c 'only-light' site/content/getting_started/index.html
grep -c 'admonition tip' site/content/getting_started/index.html
grep -c 'details' site/content/getting_started/adding_temporal_information/index.html
grep -rc '{image}\|{dropdown}\|{toctree}\|{admonition}' site/content/getting_started/ | grep -v ':0' || echo "no raw directives"
```

Expected: `only-light` ≥ 1, `admonition tip` returns `1`, the details grep is ≥ 3, and the last command prints `no raw directives`.

- [ ] **Step 7: Serve and eyeball the light/dark swap**

```bash
.venv/bin/zensical serve
```

Open `http://localhost:8000/content/getting_started/`, toggle the palette switch, and confirm exactly one of the two step-overview images is visible in each scheme. Stop the server.

- [ ] **Step 8: Commit**

```bash
git add docs/content/getting_started
git commit -m "docs: convert getting started section to Zensical syntax"
```

---

### Task 5: Theory page

The heaviest page: four large `admonition-example` blocks containing nested code fences, images and mermaid, plus the two carousels.

**Files:**
- Modify: `docs/content/theory.md`

- [ ] **Step 1: Convert the top image pair**

Lines 12-17 become:

```markdown
![Time-explicit LCA overview](data/timeexplicit_lca_light.svg#only-light)
![Time-explicit LCA overview](data/timeexplicit_lca_dark.svg#only-dark)
```

Note the order flips relative to the source (light first) purely for readability; behaviour is identical.

- [ ] **Step 2: Convert the four example admonitions**

Each ` ````{admonition} Example: X ` / `:class: admonition-example` block becomes Material's **built-in** `example` admonition — its icon is a beaker, matching the FontAwesome flask (`\f518`) the pydata theme used, so no custom CSS is needed:

```markdown
!!! example "Example: Convolution"

    Let's say we have two temporal distributions. …
```

The four titles are: `Example: Convolution`, `Example: temporal evolution`, `Example: Timeline`, `Example: Matrix modifications`.

**Every line of each block's body must be indented 4 spaces**, including the ` ```python ` fences, the ` ```{mermaid} ` fences (which become ` ```mermaid `), the images, and the `</br>` tags. This is the main mechanical risk in this task — after conversion, grep for any line inside these blocks that starts at column 0 and is not blank.

The `:name: example-matrix-modifications` anchor on the fourth block has no Material equivalent as an admonition option; if any page links to `#example-matrix-modifications`, add `{ #example-matrix-modifications }` via `attr_list` on the admonition title line. Check first with `grep -rn "example-matrix-modifications" docs/` — if the only hit is the definition itself, drop the anchor.

- [ ] **Step 3: Convert the nested `~~~{image}` blocks**

Inside the converted admonitions, the three temporal-distribution images become (at 4-space indent):

```markdown
    ![Temporal distribution: two and four years ahead](data/td_two_and_four_years_ahead.svg){ style="display:block;margin:0 auto" }
```

and likewise for `td_spread_over_four_months.svg` and `td_convolved.svg`.

- [ ] **Step 4: Replace the two carousels with one tab set**

The `:::{div} only-light` and `:::{div} only-dark` wrappers and both `{carousel}` blocks (lines 280-330) collapse into a single four-step tab set inside the `Example: Matrix modifications` admonition. At 4-space indent (admonition body), with the tab bodies at 8 spaces:

```markdown
!!! example "Example: Matrix modifications"

    For the simple system above, these are the modifications we apply to the matrices:

    === "1. Original matrices"

        ![Matrix modification step 1](data/matrix1_light.svg#only-light){ style="display:block;margin:0 auto" }
        ![Matrix modification step 1](data/matrix1_dark.svg#only-dark){ style="display:block;margin:0 auto" }

    === "2. Time-explicit rows and columns"

        ![Matrix modification step 2](data/matrix2_light.svg#only-light){ style="display:block;margin:0 auto" }
        ![Matrix modification step 2](data/matrix2_dark.svg#only-dark){ style="display:block;margin:0 auto" }

    === "3. Temporal markets"

        ![Matrix modification step 3](data/matrix3_light.svg#only-light){ style="display:block;margin:0 auto" }
        ![Matrix modification step 3](data/matrix3_dark.svg#only-dark){ style="display:block;margin:0 auto" }

    === "4. Dynamic biosphere"

        ![Matrix modification step 4](data/matrix4_light.svg#only-light){ style="display:block;margin:0 auto" }
        ![Matrix modification step 4](data/matrix4_dark.svg#only-dark){ style="display:block;margin:0 auto" }
```

Before finalising the tab labels, open the four SVGs (`docs/content/data/matrix{1,2,3,4}_light.svg`) and confirm the labels above describe what each step actually shows; adjust the wording to match the images if they differ. The prose immediately after the carousels ("The timings from the timeline and the inventory information…") stays as the closing paragraph of the admonition.

- [ ] **Step 5: Convert the mermaid fences**

` ```{mermaid} ` becomes ` ```mermaid `. Leave diagram source untouched, including the hardcoded `classDef fill:#…` colors — they read acceptably in both schemes, and Zensical themes the surrounding chrome natively. The `mermaid_init_js` MutationObserver from `conf.py` is not ported.

- [ ] **Step 6: Convert any remaining plain `{note}` blocks**

The page has `:::{note}` blocks (3 in total across the file per the directive census); each becomes `!!! note` with a 4-space indented body.

- [ ] **Step 7: Build and verify**

```bash
.venv/bin/zensical build 2>&1 | tail -20
grep -c 'admonition example' site/content/theory/index.html
grep -c 'tabbed-set' site/content/theory/index.html
grep -c 'class="mermaid"' site/content/theory/index.html
grep -c 'carousel\|{admonition}\|{image}\|{note}' site/content/theory/index.html
```

Expected: 4 example admonitions, 1 tabbed set, ≥ 2 mermaid blocks, and `0` raw directives.

- [ ] **Step 8: Serve and eyeball**

Run `.venv/bin/zensical serve`, open `http://localhost:8000/content/theory/`, and confirm: the four example boxes render as admonitions (not as literal text), the matrix tab set switches images, the mermaid diagrams draw, and the light/dark image pairs swap with the palette toggle. Stop the server.

- [ ] **Step 9: Commit**

```bash
git add docs/content/theory.md
git commit -m "docs: convert theory page to Zensical syntax, carousels to tabs"
```

---

### Task 6: Decision tree and modeling paradigms pages

**Files:**
- Modify: `docs/content/decisiontree.md`
- Read/verify: `docs/content/dev/explicit_process_product_paradigm.md`

- [ ] **Step 1: Convert the mermaid fence in `decisiontree.md`**

` ```{mermaid} ` → ` ```mermaid `. Leave the diagram body — including `classDef` colors and `\n` line breaks in node labels — untouched.

- [ ] **Step 2: Verify the paradigms page needs no changes**

`docs/content/dev/explicit_process_product_paradigm.md` uses only headings, prose, bullet lists and ` ```text ` fences. Read it and confirm no MyST directive is present. It was orphaned (in no toctree, zero inbound links) and is reachable for the first time via the nav entry added in Task 1. Make no content edits.

- [ ] **Step 3: Build and verify**

```bash
.venv/bin/zensical build 2>&1 | tail -20
grep -c 'class="mermaid"' site/content/decisiontree/index.html
test -f site/content/dev/explicit_process_product_paradigm/index.html && echo "paradigms page built"
```

Expected: `1` mermaid block and the confirmation line.

- [ ] **Step 4: Commit**

```bash
git add docs/content/decisiontree.md
git commit -m "docs: convert decision tree mermaid diagram to Zensical syntax"
```

---

### Task 7: Examples index

**Files:**
- Modify: `docs/content/examples/index.md`
- Delete: `docs/content/other/support.md` (and the now-empty `docs/content/other/`)

- [ ] **Step 1: Convert the card grid**

The `::::{grid} 1 2 2 2` / `:::{grid-item-card}` construct becomes Material's card grid. Cards are kept here — this page already uses cards today. Links change from `./x.html` to `./x.md` (Zensical resolves them). The `+++` footer becomes an italic byline. Full replacement for the grid section:

```markdown
<div class="grid cards" markdown>

-   **🚗⚡ Life Cycle of an Electric Car**

    ---

    Example of a time-explicit LCA of the entire life cycle of an electric car.

    ![](./data/ev_lifecycle_light.svg#only-light)
    ![](./data/ev_lifecycle_dark.svg#only-dark)

    [:material-arrow-right: View Example](./example_electric_vehicle_premise.md)

    *by @TimoDiepers*

-   **🌿📈 Dynamic Characterization**

    ---

    Example of some of the dynamic characterization capabilities that come with a TimexLCA.

    ![](./data/dynamic_characterization.svg)

    [:material-arrow-right: View Example](./example_simple_dynamic_characterization.md)

    *by @muelleram*

-   **📄🚗 EV Case Study for our paper**

    ---

    This is the notebook used to calculate the time-explicit LCAs and create the Figures for our paper on time-explicit LCA.

    ![](./data/paper_radiative_forcing.svg)

    [:material-arrow-right: View Example](./paper_case_study.md)

    *by @TimoDiepers*

-   **📁💻 Import foreground system from Excel**

    ---

    This notebook shows how to import your modelled product system from an Excel file.

    [:material-arrow-right: View Example](./example_Importing_model_from_excel.md)

    *by @jakobsarthur & @muelleram*

-   **Or do you have anything to add? 🧐**

    ---

    Please contact us if you want to share your super cool example!

    *by @You?*

</div>
```

Two deliberate fixes carried over from the source: the duplicated fragment "on time-explicit LCA." at the end of the Excel card's description is dropped (it was a copy-paste artifact), and the remotely hosted Wikimedia Excel logo `<img>` is dropped rather than hotlinked. Everything else is the original wording.

- [ ] **Step 2: Delete the trailing toctree**

Remove the ` ```{toctree} ` block at the end of the page.

- [ ] **Step 3: Delete the orphan support page**

```bash
git rm docs/content/other/support.md
```

It is in no toctree, has zero inbound links, and its one internal link (`../contributing/contributing.md`) points at a path that does not exist.

- [ ] **Step 4: Build and verify**

```bash
.venv/bin/zensical build 2>&1 | tail -20
grep -c 'grid cards' site/content/examples/index.html
grep -c 'grid-item-card\|{toctree}' site/content/examples/index.html
test ! -e site/content/other && echo "orphan page gone"
```

Expected: `1` card grid, `0` raw directives, and the confirmation line. The four `View Example` links will 404 until Task 8 — that is expected here.

- [ ] **Step 5: Commit**

```bash
git add docs/content/examples/index.md
git commit -m "docs: convert examples index to Material card grid, drop orphan support page"
```

---

### Task 8: Notebook conversion

**Files:**
- Create: `docs/convert_notebooks.py`, `docs/javascripts/source-overrides.js`
- Modify: `zensical.toml` (nav)
- Generated and committed: `docs/content/examples/{stem}.md` and `docs/content/examples/{stem}_files/` for four notebooks

**Interfaces:**
- Consumes: `nav` from Task 1, the `Examples` section of which currently holds only the overview entry.
- Produces: four Markdown pages whose paths the nav references.

- [ ] **Step 1: Port `docs/javascripts/source-overrides.js`**

Copy `/Users/timodiepers/Documents/Coding/optimex/docs/javascripts/source-overrides.js` to `docs/javascripts/source-overrides.js` and change both GitHub URLs from `RWTH-LTT/optimex` to `brightway-lca/bw_timex`. Nothing else changes.

- [ ] **Step 2: Port `docs/convert_notebooks.py`**

Copy `/Users/timodiepers/Documents/Coding/optimex/docs/convert_notebooks.py` and apply these changes:

```python
REPO_ROOT = Path(__file__).parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "docs" / "content" / "examples"
OUTPUT_DIR = REPO_ROOT / "docs" / "content" / "examples"

NOTEBOOK_META: dict[str, tuple[str, list[str]]] = {
    "example_electric_vehicle_premise": (
        "lucide/car-front",
        ["example", "premise"],
    ),
    "example_simple_dynamic_characterization": (
        "lucide/trending-up",
        ["example", "dynamic characterization"],
    ),
    "paper_case_study": (
        "lucide/file-text",
        ["case study", "paper"],
    ),
    "example_Importing_model_from_excel": (
        "lucide/table",
        ["example", "excel"],
    ),
}

NOTEBOOK_SOURCE_PATHS: dict[str, str] = {
    stem: f"docs/content/examples/{stem}.ipynb" for stem in NOTEBOOK_META
}
```

Delete the two optimex-specific rewrite blocks: the `product_system.svg` white-backdrop `re.sub` and the `example-flowchart` `re.sub`. Keep the ANSI stripping, the `output_N_M.png` → `<stem>_files/` rewrite, the `](../data/` → `](data/` rewrite, the front-matter injection and the source-override div.

Because `NOTEBOOKS_DIR` and `OUTPUT_DIR` are the same directory, keep the `OUTPUT_DIR.mkdir(parents=True, exist_ok=True)` call (a no-op) and the stray-`output_*.png` cleanup loop.

- [ ] **Step 3: Run the conversion**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
.venv/bin/python docs/convert_notebooks.py
ls docs/content/examples/*.md
du -sh docs/content/examples/*_files 2>/dev/null
```

Expected: four `Converted … → …` lines, four new `.md` files alongside `index.md`, and `_files/` directories for the notebooks that have image outputs.

- [ ] **Step 4: Add the four pages to the nav**

In `zensical.toml`, extend the `Examples` entry:

```toml
  { Examples = [
    { Overview = "content/examples/index.md" },
    { "Life Cycle of an Electric Car" = "content/examples/example_electric_vehicle_premise.md" },
    { "Dynamic Characterization" = "content/examples/example_simple_dynamic_characterization.md" },
    { "EV Case Study for our Paper" = "content/examples/paper_case_study.md" },
    { "Import from Excel" = "content/examples/example_Importing_model_from_excel.md" },
  ]},
```

- [ ] **Step 5: Build and verify**

```bash
.venv/bin/zensical build 2>&1 | tail -30
for p in example_electric_vehicle_premise example_simple_dynamic_characterization paper_case_study example_Importing_model_from_excel; do
  test -f "site/content/examples/$p/index.html" && echo "OK $p" || echo "MISSING $p"
done
grep -c 'data-source-edit-path' site/content/examples/paper_case_study/index.html
grep -rc '\x1b\[' docs/content/examples/*.md | grep -v ':0' || echo "no ANSI escapes"
```

Expected: four `OK` lines, the source-override div present, and no ANSI escapes.

- [ ] **Step 6: Check images actually resolve**

```bash
.venv/bin/python - <<'EOF'
import re, pathlib
root = pathlib.Path("site/content/examples")
missing = []
for page in root.glob("*/index.html"):
    for src in re.findall(r'<img[^>]+src="([^"]+)"', page.read_text(encoding="utf-8")):
        if src.startswith(("http", "data:")):
            continue
        target = (page.parent / src).resolve()
        if not target.exists():
            missing.append((page.name, src))
print("missing:", missing or "none")
EOF
```

Expected: `missing: none`. If notebook-local `data/…` assets are missing, the `](../data/` rewrite needs adjusting for how these particular notebooks reference their SVGs.

- [ ] **Step 7: Commit**

```bash
git add docs/convert_notebooks.py docs/javascripts/source-overrides.js zensical.toml docs/content/examples
git commit -m "docs: convert example notebooks to Markdown for Zensical"
```

---

### Task 9: API reference

**Files:**
- Create: `docs/api/index.md`, `docs/api/timex_lca.md`, `docs/api/timeline_builder.md`, `docs/api/matrix_modifier.md`, `docs/api/dynamic_biosphere_builder.md`, `docs/api/edge_extractor.md`, `docs/api/helper_classes.md`, `docs/api/utils.md`
- Modify: `zensical.toml` (nav)

**Interfaces:**
- Consumes: the mkdocstrings handler config from Task 1 (`paths = ["."]`, numpy docstring style).
- Produces: the `api/` nav subtree.

- [ ] **Step 1: Write `docs/api/index.md`**

Prose only — **no card grid** (explicit user decision):

```markdown
# API Reference

This section contains the API documentation generated from the `bw_timex` source code docstrings.

The main user-facing class is [`TimexLCA`](timex_lca.md). It orchestrates the other components, which most users never need to instantiate directly:

- [`timex_lca`](timex_lca.md) — the `TimexLCA` class: the entry point for a time-explicit LCA.
- [`timeline_builder`](timeline_builder.md) — traverses the graph and builds the process timeline.
- [`matrix_modifier`](matrix_modifier.md) — expands the technosphere and biosphere matrices with time-explicit rows and columns.
- [`dynamic_biosphere_builder`](dynamic_biosphere_builder.md) — builds the dynamic biosphere matrix carrying emission timing.
- [`edge_extractor`](edge_extractor.md) — extracts and convolves temporal distributions during graph traversal.
- [`helper_classes`](helper_classes.md) — supporting data structures used across the package.
- [`utils`](utils.md) — utility functions.
```

- [ ] **Step 2: Write the seven module pages**

Each page is a title, a one-line description, and an mkdocstrings block. `timex_lca.md`:

```markdown
# TimexLCA

The main user-facing class of `bw_timex`. A `TimexLCA` takes a Brightway demand, an LCIA method and a set of time-specific databases, and produces a time-explicit inventory.

::: bw_timex.timex_lca
```

The other six follow the same shape with `::: bw_timex.timeline_builder`, `::: bw_timex.matrix_modifier`, `::: bw_timex.dynamic_biosphere_builder`, `::: bw_timex.edge_extractor`, `::: bw_timex.helper_classes`, `::: bw_timex.utils`. Write each one-line description from the module's own docstring or, if it has none, from what its public classes do — read the module before writing the line.

`validation.py`, `_lci_cache.py` and `bw_timex/data/` get no page, matching the old `autoapi_ignore` list and the `filters = ["!^_", "!^__"]` handler option.

- [ ] **Step 3: Re-enable mkdocstrings**

Task 1 shipped `zensical.toml` with `[project.plugins.mkdocstrings] enabled = false` and an explanatory comment, because mkdocstrings' block processor fires on any line starting with `:::` and that collided with the MyST colon-fences then still present in four content pages. Tasks 3, 4, 5 and 7 have since removed all of them. Set `enabled = true` and delete the now-stale comment.

Before building, confirm no colon fences remain anywhere:

```bash
grep -rn '^\s*:::' docs --include='*.md' | grep -v '/superpowers/'
```

Expected: no output. If any hit appears, it belongs to whichever page task missed it — fix that page here rather than leaving mkdocstrings disabled.

- [ ] **Step 4: Add the API subtree to the nav**

Insert after the `Examples` entry in `zensical.toml`, preserving the toctree's original position (API came after Examples, before the decision tree):

```toml
  { "API Reference" = [
    { Overview = "api/index.md" },
    { TimexLCA = "api/timex_lca.md" },
    { "Timeline Builder" = "api/timeline_builder.md" },
    { "Matrix Modifier" = "api/matrix_modifier.md" },
    { "Dynamic Biosphere Builder" = "api/dynamic_biosphere_builder.md" },
    { "Edge Extractor" = "api/edge_extractor.md" },
    { "Helper Classes" = "api/helper_classes.md" },
    { Utils = "api/utils.md" },
  ]},
```

- [ ] **Step 5: Build and verify the docstrings rendered**

```bash
.venv/bin/zensical build 2>&1 | tail -30
grep -c 'doc-class\|doc-function\|doc-object' site/api/timex_lca/index.html
grep -c 'TimexLCA' site/api/timex_lca/index.html
grep -c 'field-list\|doc-md-description' site/api/timeline_builder/index.html
```

Expected: non-zero counts. A page containing only the heading and no `doc-` markup means mkdocstrings failed to import the module — check the build log for an import error (the package must be importable from the venv, which it is via the editable install).

- [ ] **Step 6: Confirm numpy-style sections parsed**

Open `site/api/timeline_builder/index.html` and confirm parameter tables are rendered as tables rather than as a preformatted blob. `bw_timex` docstrings use numpydoc `Parameters` / `----------` sections and the handler is configured with `docstring_style = "numpy"`; a preformatted blob means the style setting is not being applied.

- [ ] **Step 7: Commit**

```bash
git add docs/api zensical.toml
git commit -m "docs: replace sphinx-autoapi with mkdocstrings API reference"
```

---

### Task 10: Read the Docs, packaging, and Sphinx teardown

**Files:**
- Modify: `.readthedocs.yaml`, `pyproject.toml`
- Delete: `docs/conf.py`, `docs/environment.yaml`, `docs/_templates/`, `docs/_static/custom.css`

- [ ] **Step 1: Rewrite `.readthedocs.yaml`**

Replace the whole file:

```yaml
# .readthedocs.yaml
# Read the Docs configuration file
# See https://docs.readthedocs.io/en/stable/config-file/v2.html for details

# Required
version: 2

submodules:
   include: all

build:
   os: "ubuntu-lts-latest"
   tools:
      python: "3.12"
   commands:
      - pip install -r docs/requirements.txt
      - pip install -e .
      - python docs/convert_notebooks.py
      - zensical build
      - mkdir -p $READTHEDOCS_OUTPUT/html
      - cp -r site/* $READTHEDOCS_OUTPUT/html/
```

The conda block and the `sphinx.configuration` key are gone; `submodules` is retained from the original file. Python 3.12 matches the local venv and the package's `requires-python = ">=3.11"`.

- [ ] **Step 2: Update the `docs` extra in `pyproject.toml`**

Replace the existing `docs = [...]` list (ipython, pydata-sphinx-theme, myst-parser, sphinx-click, sphinx-design, sphinx-notfound-page, sphinx-favicon, sphinx-copybutton, sphinx-autobuild) with:

```toml
docs = [
  "zensical",
  "mkdocstrings[python]>=1.0.0",
  "mkdocs-autorefs",
  "pymdown-extensions>=10.0",
  "nbconvert>=7.0",
]
```

- [ ] **Step 3: Delete the Sphinx setup**

```bash
git rm docs/conf.py docs/environment.yaml docs/_static/custom.css
git rm -r docs/_templates
```

`docs/_templates/` includes `autoapi_templates/index.rst`, `layout.html`, `footer.html` and `support.html` — all pydata-theme templates with no Zensical equivalent. `custom.css` is superseded by `docs/stylesheets/extra.css`; its remaining rules targeted pydata classes (`.bd-header`, `.bd-page-width`, `#rtd-footer-container`) that no longer exist.

- [ ] **Step 4: Confirm nothing still references the deleted files**

```bash
grep -rn "conf.py\|environment.yaml\|_templates\|custom.css\|sphinx" --include='*.toml' --include='*.yaml' --include='*.yml' --include='*.md' --include='*.cff' . \
  | grep -v '^./superpowers/' | grep -v '^./site/' | grep -v '^./.venv/'
```

Expected: no hits pointing at the deleted docs files. Hits inside `superpowers/` (this plan and the spec) are fine. If `.github/workflows/*` or `CONTRIBUTING.md` mention building the docs with Sphinx, update those references to `zensical build` in this step.

- [ ] **Step 5: Full clean build**

```bash
rm -rf site
.venv/bin/zensical build 2>&1 | tee /tmp/zensical-build.log | tail -30
grep -ci "warning\|error" /tmp/zensical-build.log
```

Expected: build succeeds. Read every warning; missing-file and broken-nav warnings must be zero.

- [ ] **Step 6: Commit**

```bash
git add .readthedocs.yaml pyproject.toml
git commit -m "docs: switch Read the Docs to Zensical and remove Sphinx setup"
```

---

### Task 11: Final verification sweep

No new files. This task proves the migration is complete rather than merely building.

- [ ] **Step 1: Assert no MyST directives survive anywhere**

```bash
cd /Users/timodiepers/Documents/Coding/bw_timex
grep -rnE '^\s*(:{3,}|`{3,})\{[a-z0-9_-]+\}' docs --include='*.md' | grep -v '/superpowers/'
```

Expected: no output. Any hit is an unconverted directive.

- [ ] **Step 2: Assert every nav target exists**

```bash
.venv/bin/python - <<'EOF'
import re, pathlib, tomllib
cfg = tomllib.loads(pathlib.Path("zensical.toml").read_text())
missing = []
def walk(node):
    if isinstance(node, str):
        if node.endswith(".md") and not (pathlib.Path("docs") / node).exists():
            missing.append(node)
    elif isinstance(node, list):
        for item in node: walk(item)
    elif isinstance(node, dict):
        for value in node.values(): walk(value)
walk(cfg["project"]["nav"])
print("missing nav targets:", missing or "none")
EOF
```

Expected: `missing nav targets: none`.

- [ ] **Step 3: Assert every internal link resolves**

```bash
rm -rf site && .venv/bin/zensical build >/dev/null 2>&1
.venv/bin/python - <<'EOF'
import re, pathlib
site = pathlib.Path("site")
broken = []
for page in site.rglob("index.html"):
    html = page.read_text(encoding="utf-8", errors="ignore")
    for href in re.findall(r'href="([^"#?]+)"', html):
        if href.startswith(("http", "mailto:", "data:", "//")):
            continue
        target = (site / href.lstrip("/")) if href.startswith("/") else (page.parent / href)
        target = target.resolve()
        if target.is_dir():
            target = target / "index.html"
        if not target.exists():
            broken.append((str(page.relative_to(site)), href))
print(f"broken links: {len(broken)}")
for item in broken[:25]:
    print(" ", item)
EOF
```

Expected: `broken links: 0`. Cross-page links written as `content/theory.md#terminology` in the source must have been rewritten by the builder; any that appear here need fixing on the source page.

- [ ] **Step 4: Page-by-page visual check**

```bash
.venv/bin/zensical serve
```

Walk every nav entry and confirm:
- Landing page renders with no leftover toctree.
- Installation: both tab sets switch.
- Getting Started: images swap with the palette toggle; the Binder callout renders as a tip admonition; the three code dropdowns expand.
- Theory: four example admonitions, the four-step matrix tab set, both mermaid diagrams.
- Decision tree: mermaid flowchart renders.
- Examples: five cards; all four notebook pages open with their plots.
- API: seven module pages with rendered docstring tables.
- License / Changelog / Contributing / Code of Conduct / Funding: included file content is present, not a literal `--8<--`.
- Search returns results (top-right search box).
- The edit and view-source buttons on a notebook page point at the `.ipynb`, not the generated `.md`.

Stop the server.

- [ ] **Step 5: Report**

Summarise for the user: what was migrated, what was intentionally dropped (custom 404 body, inheritance diagrams, intersphinx, the mermaid theme-observer JS, the orphaned support page, the hotlinked Excel logo), and anything found during verification that needs a follow-up decision. Note that the branch is left unmerged, as agreed.

---

## Notes for the executor

- **`zensical serve` blocks.** Run it in the background or stop it before continuing.
- **Rebuild before grepping `site/`.** The verification greps read build output, not source.
- **Indentation is the number-one failure mode.** Material admonitions, tabs and details blocks all take indented bodies; a fence that is not indented silently escapes its container and renders as a sibling block. When a converted block looks wrong in the browser, check indentation before anything else.
- **`docs/index.md` has an unmerged rewrite** on the branch `docs/readme-problem-framing`. This plan ports the version on `main`. If that branch lands later, the only change it needs is toctree removal.
