# Design: Migrate `bw_timex` docs from Sphinx to Zensical

Date: 2026-07-31
Branch: `docs/zensical-migration` (off `origin/main` @ `5f158e3`)

## Goal

Replace the Sphinx / pydata-sphinx-theme documentation build with Zensical (Material for
MkDocs lineage), mirroring the setup already in use in the sibling project
`optimex` (`/Users/timodiepers/Documents/Coding/optimex`). Every feature the current
Sphinx docs rely on must either have a Zensical equivalent or a deliberate, documented
adaptation. Prose content is ported, not rewritten.

## Non-goals

- Rewriting or restructuring the documentation text.
- Adding Material "grid cards" to pages that do not already use cards (explicitly ruled
  out by the user for the landing page and the API section).
- Regrouping the navigation into top-level tabs. The current flat sidebar order stays.
- Touching `notebooks/` — the notebooks rendered in the docs live in
  `docs/content/examples/` and are separate, output-bearing copies.

## Current state

| Aspect | Today |
|---|---|
| Builder | Sphinx 7.3, `docs/conf.py`, conda env `docs/environment.yaml` |
| Theme | `pydata_sphinx_theme` 0.15.3 |
| Markdown | MyST (`myst_nb` handles both `.md` and `.ipynb`) |
| Notebooks | `myst_nb`, `nb_execution_mode = "off"` — 4 executed notebooks in `docs/content/examples/` |
| API | `sphinx-autoapi` over `../bw_timex`, `autoapi_root = "content/api"`, inheritance diagrams via graphviz |
| Components | `sphinx-design` (grid/cards, tab-set, dropdown), `sphinx-carousel`, `sphinxcontrib-mermaid`, `sphinx-copybutton`, `sphinx-favicon`, `sphinx-notfound-page` |
| Hosting | Read the Docs, conda build, `sphinx.configuration: docs/conf.py` |

Target reference setup (`optimex`): root `zensical.toml`, `docs/requirements.txt`,
`docs/convert_notebooks.py`, `docs/stylesheets/extra.css`,
`docs/javascripts/source-overrides.js`, RTD `build.commands` running `zensical build`
into `site/`.

## Architecture

### Build

- **`zensical.toml`** at the repository root. Structure copied from `optimex`, with:
  - `site_name = "bw_timex"`, `site_url = "https://docs.brightway.dev/projects/bw-timex/en/latest/"`,
    `repo_url = "https://github.com/brightway-lca/bw_timex"`, `edit_uri = "edit/main/docs/"`.
  - `nav` replacing the three `{toctree}` blocks (see *Navigation*).
  - Theme: logo/favicon from `docs/_static/`, the same `features` list as `optimex`
    minus `navigation.tabs` (flat nav), palette in brand green rather than indigo.
  - `markdown_extensions` identical to `optimex` — critically `pymdownx.tabbed`
    (`alternate_style = true`), `pymdownx.details`, `pymdownx.snippets`,
    `pymdownx.superfences` with the mermaid custom fence, `pymdownx.arithmatex`
    (`generic = true`), `attr_list`, `md_in_html`.
  - `extra_javascript` = MathJax CDN + `javascripts/source-overrides.js`;
    `extra_css` = `stylesheets/extra.css`.
  - `[project.plugins.mkdocstrings.handlers.python] paths = ["bw_timex"]` (flat layout,
    not `src/`), options block copied from `optimex` — `docstring_style = "numpy"`
    matches the existing numpydoc-style docstrings.
- **`docs/requirements.txt`** (new): `zensical`, `mkdocstrings[python]>=1.0.0`,
  `mkdocs-autorefs`, `pymdown-extensions>=10.0`, `nbconvert>=7.0`.
- **`.readthedocs.yaml`**: replace the conda + `sphinx.configuration` config with a
  `build.commands` block: install `docs/requirements.txt`, `pip install -e .`,
  `python docs/convert_notebooks.py`, `zensical build`, copy `site/*` into
  `$READTHEDOCS_OUTPUT/html`. Keeps `submodules: include: all`.
- **`pyproject.toml`**: replace the `[docs]` optional-dependency list (currently
  pydata-sphinx-theme, myst-parser, sphinx-click, sphinx-design, sphinx-notfound-page,
  sphinx-favicon, sphinx-copybutton, sphinx-autobuild) with the Zensical set above.
- **`.gitignore`**: add `site/`.
- **Deleted**: `docs/conf.py`, `docs/environment.yaml`, `docs/_templates/` (including
  `autoapi_templates/`). Git history retains them.

### Notebook conversion

`docs/convert_notebooks.py`, ported from `optimex` with these changes:

- `NOTEBOOKS_DIR` = `docs/content/examples/` (the executed copies), **not** `notebooks/`.
  Verified: the `docs/` copies carry outputs and differ from the `notebooks/` copies
  (e.g. 1.4 MB vs 38 KB for `example_electric_vehicle_premise.ipynb`).
- `NOTEBOOK_META` covers the four notebooks with a Zensical icon and tags each:
  `example_electric_vehicle_premise`, `example_simple_dynamic_characterization`,
  `paper_case_study`, `example_Importing_model_from_excel`.
- `NOTEBOOK_SOURCE_PATHS` points at `docs/content/examples/<stem>.ipynb` so the
  "edit / view source" buttons on the generated pages resolve to the notebook rather
  than the generated Markdown, via `docs/javascripts/source-overrides.js` (ported with
  the repo URL changed to `brightway-lca/bw_timex`).
- The optimex-specific `product_system.svg` white-backdrop rewrite is dropped; the
  bw_timex notebooks reference assets under `docs/content/examples/data/`, which are
  already committed and keep working as relative links.
- Generated `.md` and `<stem>_files/` output **are** committed, as in `optimex` (where
  `docs/content/examples/basic_example.md` and its `_files/` directory are tracked). This
  keeps `zensical build` working without first running the conversion script, at the cost
  of noisy diffs when a notebook is re-executed. The source `.ipynb` files stay in place
  alongside them.

## Navigation

Flat, in the current toctree order:

```
Overview                  index.md
Installation              content/installation.md
Getting Started           content/getting_started/index.md
  Step 1 …                content/getting_started/adding_temporal_information.md
  Step 2 …                content/getting_started/build_process_timeline.md
  Step 3 …                content/getting_started/time_explicit_lci.md
  Step 4 …                content/getting_started/lcia.md
Theory                    content/theory.md
Examples                  content/examples/index.md
  (4 generated notebook pages)
API                       api/index.md + 7 module pages
What LCA should I do?     content/decisiontree.md
Modeling paradigms        content/dev/explicit_process_product_paradigm.md
Contributing              content/contributing.md
Code of Conduct           content/codeofconduct.md
License                   content/license.md
Changelog                 content/changelog.md
Funding                   content/funding.md
```

## Feature mapping

| Sphinx / MyST feature | Zensical replacement |
|---|---|
| `{toctree}` (3 blocks) | `nav` in `zensical.toml`; the directives are deleted from the pages |
| `{image}` + `:class: only-light/only-dark` (10 pairs) | `![](x.svg#only-light)` / `#only-dark` — Material's native fragment convention, backed by CSS in `extra.css` |
| `{image}` `:height:` / `:align:` | `attr_list`: `{ width="450" style="display:block;margin:auto" }` |
| `::::{grid}` / `:::{grid-item-card}` (examples index) | `<div class="grid cards" markdown>` — kept only here, because this page already uses cards |
| `::::{tab-set}` / `:::{tab-item}` (installation) | `=== "Tab title"` (`pymdownx.tabbed`) |
| `{note}`, `{admonition} X :class: important` | `!!! note`, `!!! important "X"` |
| `{admonition} … :class: admonition-launch` (1×, getting_started) | custom `!!! launch` admonition type, defined in `extra.css` |
| `{admonition} … :class: admonition-example` (4×, theory) | custom `!!! example-box` admonition type, defined in `extra.css` (`example` is already a built-in Material type, so the custom one needs a distinct name) |
| `:::{dropdown}` (3×, adding_temporal_information) | `??? note "title"` (`pymdownx.details`) |
| ` ```{mermaid} ` | superfences custom fence `mermaid`; Zensical themes diagrams natively, so conf.py's 40-line `mermaid_init_js` MutationObserver is dropped |
| `{include} ../../FILE.md` | `--8<-- "FILE.md"` (`pymdownx.snippets`, base path = repo root) |
| `{literalinclude} ../../LICENSE` | fenced block containing `--8<-- "LICENSE"` |
| myst `dollarmath` / `amsmath` (enabled in conf.py, **not actually used** in any `.md`) | `pymdownx.arithmatex` generic mode + MathJax CDN, kept as a precaution for notebook-derived pages |
| `sphinx-copybutton` | theme feature `content.code.copy` |
| `use_edit_page_button` | theme features `content.action.edit` / `content.action.view` + `edit_uri` |
| `icon_links` (Binder, GitHub) | `[[project.extra.social]]` entries |
| `sphinx-favicon` | `theme.favicon` + `_static/favicons` assets |
| light/dark logo pair | `theme.logo` set to the light asset, dark variant swapped by CSS in `extra.css` |
| `sphinx-autoapi` (+ `autoapisummary`, `class.rst` template) | mkdocstrings, one handwritten page per module (below) |
| `sphinx.ext.inheritance_diagram` / graphviz | **dropped** — no mkdocstrings equivalent |
| `sphinx-notfound-page` custom 404 body | **dropped** — Material's built-in 404 is used; the jokey copy would need a theme override |
| `sphinx.ext.intersphinx`, `extlinks`, `viewcode` | not carried over; `mkdocs-autorefs` covers intra-doc references, mkdocstrings `show_source` covers viewcode |
| `sphinx-carousel` (2 carousels, 4 slides each) | tabbed step sets (below) |

### Carousels → tabbed steps

`content/theory.md` currently holds two `{carousel}` blocks wrapped in
`:::{div} only-light` / `:::{div} only-dark`, showing `matrix1…matrix4` in light and
dark variants. These collapse into **one** tab set of four steps, each step containing
the light/dark image pair via `#only-light` / `#only-dark`:

```markdown
=== "1"
    ![Matrix step 1](data/matrix1_light.svg#only-light)
    ![Matrix step 1](data/matrix1_dark.svg#only-dark)

=== "2"
    …
```

The `{div}` wrappers disappear because the fragment convention handles the light/dark
split per image. Tab labels get short descriptive names taken from the surrounding
prose rather than bare numbers.

### API reference

`docs/api/` replaces the autoapi-generated `content/api/` tree:

- `api/index.md` — plain prose overview (no cards, per user decision): what the package
  exposes, which class is user-facing (`TimexLCA`), links to the module pages.
- One page per module, each with a short intro and a `::: bw_timex.<module>` block:
  `timex_lca`, `timeline_builder`, `matrix_modifier`, `dynamic_biosphere_builder`,
  `edge_extractor`, `helper_classes`, `utils`.
- `validation.py`, `_lci_cache.py` and `data/` are excluded, matching the current
  `autoapi_ignore` list and the leading-underscore convention.

### Styling

`docs/_static/custom.css` is replaced by `docs/stylesheets/extra.css`, seeded from the
optimex file and extended with:

1. `img[src$="#only-light"]` / `#only-dark` visibility rules keyed on
   `[data-md-color-scheme]`, plus the same treatment for the header logo pair.
2. Custom `launch` and `example-box` admonition types — colors from the Material palette
   variables, icons as SVG mask data URIs replacing pydata's FontAwesome glyph hack
   (`\f135` rocket / `\f518` flask).
3. Brand palette overrides pinning primary/accent to the logo greens `#74b944` /
   `#316733` on top of a Material named palette.

The pydata-specific rules in `custom.css` (`#rtd-footer-container`, `.bd-header`,
`.bd-page-width`, sidebar-toggle padding) are dropped — they target a theme that no
longer exists.

## Orphan pages

Two pages are in no toctree and have zero inbound links today:

- `content/dev/explicit_process_product_paradigm.md` (258 lines) — real content, and it
  overlaps with a section of `decisiontree.md`. It is **added to the nav** as
  "Modeling paradigms".
- `content/other/support.md` — links to a nonexistent path
  (`../contributing/contributing.md`) and duplicates upstream Brightway material. It is
  **deleted**, along with the now-empty `content/other/`.

## Risks and verification

- **Zensical version drift**: optimex pins nothing; it currently resolves to zensical
  0.0.37. Behavior of `nav`, snippets base path, and mkdocstrings integration is
  verified by building locally, not assumed.
- **`docs/index.md` divergence**: the unmerged branch `docs/readme-problem-framing`
  contains a rewritten landing page. This migration ports the version on `main`; if that
  branch lands later, the same syntax changes (toctree removal only) apply.
- **Acceptance**: `zensical build` completes with no warnings about missing files or
  broken nav entries; every page in the nav renders; the four notebook pages render with
  their images; the API pages show rendered docstrings; light/dark image swapping works
  in both schemes; math renders in `theory.md`; both mermaid diagrams render.
- Verification is a local `zensical build` plus `zensical serve` spot-check of the
  affected pages, not only the exit status of the build.
