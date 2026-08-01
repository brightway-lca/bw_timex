# Zensical Reader Niceties Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn on instant link previews, styled tag icons, glossary tooltips, image lightbox, link validation, and header autohide for the `bw_timex` Zensical docs site.

**Architecture:** All changes are additive TOML blocks in the single root `zensical.toml`, plus one new Markdown file (`includes/abbreviations.md`) for the glossary. No theme code, no new build steps, no external services. Each task adds one config block, rebuilds the site with `zensical build`, and greps the generated HTML in `site/` for a concrete marker proving the feature rendered — this is the closest equivalent to a red/green test cycle for a static-site config change (there is no unit-test suite for the docs site).

**Tech Stack:** Zensical 0.0.52 (bundled `.venv`), `uv run zensical build` / `uv run zensical build --strict`.

## Global Constraints

- Spec: `superpowers/specs/2026-08-01-zensical-docs-niceties-design.md`.
- All work happens on the existing branch `docs/zensical-migration`. Do not create a new branch.
- `site_url` is already `https://timodiepers.github.io/bw_timex/` (the fork's GH Pages preview) — do not change it as part of this plan.
- No `--strict` flag in `.readthedocs.yaml` or `.github/workflows/docs-preview.yml` — validation stays warning-only in automated builds (Task 5 only runs `--strict` manually, once, for verification).
- No new tags, no nav changes, no page-content rewrites beyond the new glossary file.
- Commit messages: no Claude/AI attribution trailers (repo convention).
- Every task's build command is run from the repo root: `/Users/timodiepers/Documents/Coding/bw_timex`.

---

### Task 1: Instant previews

**Files:**
- Modify: `zensical.toml` (add a new top-level TOML table after `[project.plugins.tags]`, the last existing table in the file)

**Interfaces:**
- Produces: `[project.markdown_extensions.zensical.extensions.preview]` table in `zensical.toml`, consumed by no other task (fully independent).

- [ ] **Step 1: Confirm the baseline has no preview markup (red)**

Run:
```bash
rm -rf site
uv run zensical build
grep -rl "data-preview" site --include="*.html" | wc -l
```
Expected: `0`

- [ ] **Step 2: Add the preview extension config**

Append to the end of `zensical.toml`:

```toml
[project.markdown_extensions.zensical.extensions.preview]
configurations = [{ targets.include = ["**"] }]
```

`targets.include = ["**"]` applies to every page. The extension itself skips external links, footnotes, and header-permalinks automatically — no additional exclusion config needed. No theme `features` change is required: `navigation.instant` and `content.tooltips` (both prerequisites) are already enabled.

- [ ] **Step 3: Rebuild and verify preview markup appears (green)**

Run:
```bash
rm -rf site
uv run zensical build
grep -rl "data-preview" site --include="*.html" | wc -l
```
Expected: a number greater than `0` (verified during design research: `11` files contained it on the pre-nicety content).

Also confirm no new build warnings: the build output must end with `No issues found` / `Build finished in ...s`, same as before this change.

- [ ] **Step 4: Commit**

```bash
git add zensical.toml
git commit -m "docs: enable zensical instant previews on all internal links"
```

---

### Task 2: Tag icons

**Files:**
- Modify: `zensical.toml` (add two new top-level TOML tables)

**Interfaces:**
- Produces: `[project.extra.tags]` and `[project.theme.icon.tag]` tables in `zensical.toml`. Independent of Task 1.

- [ ] **Step 1: Confirm the baseline tag markup has no icon class (red)**

Run:
```bash
rm -rf site
uv run zensical build
python3 -c "
data = open('site/content/examples/paper_case_study/index.html', encoding='utf-8').read()
idx = data.find('md-tags')
print(data[idx-20:idx+400])
"
```
Expected: two `<span class="md-tag">case study</span>` / `<span class="md-tag">paper</span>` elements, **without** an `md-tag-icon` class.

- [ ] **Step 2: Add the tag identifier and icon mapping**

Append to the end of `zensical.toml`:

```toml
[project.extra.tags]
example = "example"
premise = "premise"
"dynamic characterization" = "dynchar"
"case study" = "case-study"
paper = "paper"
excel = "excel"

[project.theme.icon.tag]
default = "lucide/hash"
example = "lucide/flask-conical"
premise = "lucide/leaf"
dynchar = "lucide/trending-up"
"case-study" = "lucide/file-text"
paper = "lucide/scroll-text"
excel = "lucide/table"
```

This maps the 6 tag values already used in front matter across the 4 example pages (`docs/convert_notebooks.py`'s `NOTEBOOK_META`) to distinct Lucide icons. No front matter or nav changes.

- [ ] **Step 3: Rebuild and verify icon classes appear (green)**

Run:
```bash
rm -rf site
uv run zensical build
python3 -c "
data = open('site/content/examples/paper_case_study/index.html', encoding='utf-8').read()
idx = data.find('md-tags')
print(data[idx-20:idx+600])
"
```
Expected: `<span class="md-tag md-tag-icon md-tag--case-study">case study</span>` and `<span class="md-tag md-tag-icon md-tag--paper">paper</span>` — the `md-tag-icon` class and the `md-tag--<identifier>` modifier class must both be present (verified during design research).

Confirm the same check on the other 3 tagged pages (`example_electric_vehicle_premise`, `example_simple_dynamic_characterization`, `example_Importing_model_from_excel`) shows `md-tag-icon` present for each of their tags too.

- [ ] **Step 4: Commit**

```bash
git add zensical.toml
git commit -m "docs: add icons to zensical tag chips"
```

---

### Task 3: Glossary tooltips

**Files:**
- Create: `includes/abbreviations.md` (repository root, **not** under `docs/`)
- Modify: `zensical.toml` (add `auto_append` to the existing empty `[project.markdown_extensions.pymdownx.snippets]` table)

**Interfaces:**
- Produces: `includes/abbreviations.md`, referenced by `zensical.toml`'s `pymdownx.snippets.auto_append`. Independent of Tasks 1–2.

- [ ] **Step 1: Confirm the baseline has no `<abbr>` markup (red)**

Run:
```bash
rm -rf site
uv run zensical build
grep -rl "<abbr" site --include="*.html" | wc -l
```
Expected: `0`

- [ ] **Step 2: Create the glossary file**

Create `includes/abbreviations.md` at the repo root:

```markdown
*[LCA]: Life Cycle Assessment
*[LCI]: Life Cycle Inventory
*[LCIA]: Life Cycle Impact Assessment
*[GWP]: Global Warming Potential
*[EOL]: End of Life
*[CRF]: Cumulative Radiative Forcing
*[EV]: Electric Vehicle
```

These 7 acronyms were chosen by frequency in `docs/content` + `docs/api` (125/111/100/36/23/23/12 occurrences respectively) — no invented terms.

- [ ] **Step 3: Wire up auto-append**

In `zensical.toml`, change:

```toml
[project.markdown_extensions.pymdownx.snippets]
```

to:

```toml
[project.markdown_extensions.pymdownx.snippets]
auto_append = ["includes/abbreviations.md"]
```

(`pymdownx.snippets` is already enabled; this just adds the one key. The `abbr` extension is also already enabled — no other extension changes needed.)

- [ ] **Step 4: Rebuild and verify tooltip markup appears (green)**

Run:
```bash
rm -rf site
uv run zensical build
python3 -c "
data = open('site/index.html', encoding='utf-8').read()
idx = data.find('<abbr title=\"Life Cycle Assessment\">LCA</abbr>')
print(idx)
"
```
Expected: an index `>= 0` (not `-1`) — confirmed present in `site/index.html` during design research (`makes your <abbr title=\"Life Cycle Assessment\">LCA</abbr> use the right data...`).

- [ ] **Step 5: Commit**

```bash
git add includes/abbreviations.md zensical.toml
git commit -m "docs: add site-wide glossary tooltips for LCA jargon"
```

---

### Task 4: GLightbox image zoom

**Files:**
- Modify: `zensical.toml` (add one new top-level TOML table)

**Interfaces:**
- Produces: `[project.markdown_extensions.zensical.extensions.glightbox]` table in `zensical.toml`. Independent of Tasks 1–3.

- [ ] **Step 1: Confirm the baseline has no glightbox markup (red)**

Run:
```bash
rm -rf site
uv run zensical build
grep -rl "glightbox" site --include="*.html" | wc -l
```
Expected: `0`

- [ ] **Step 2: Enable the extension**

Append to the end of `zensical.toml`:

```toml
[project.markdown_extensions.zensical.extensions.glightbox]
```

Empty table = default config (`auto = true`), which wraps every content `<img>` in a click-to-zoom overlay.

- [ ] **Step 3: Rebuild and verify lightbox markup appears (green)**

Run:
```bash
rm -rf site
uv run zensical build
grep -rl "glightbox" site --include="*.html" | wc -l
```
Expected: a number greater than `0` (`8` files during design research).

Then specifically check `content/theory.md`'s rendered output, since it has 4 light/dark image pairs that were the subject of several recent bug-fix commits:

```bash
python3 -c "
import re
data = open('site/content/theory/index.html', encoding='utf-8').read()
matches = re.findall(r'<a class=\"glightbox\"[^>]*href=\"([^\"]*)\"', data)
for m in matches:
    print(m)
"
```
Expected: pairs of `..._light.svg#only-light` / `..._dark.svg#only-dark` hrefs, each wrapped individually. This confirms both the visible and CSS-hidden variant get a lightbox link — verify in Step 4 that only the *visible* one is actually clickable.

- [ ] **Step 4: Manual visual check (light/dark correctness)**

Run:
```bash
uv run zensical serve --open
```
Navigate to the Theory page in both light and dark mode (toggle via the sun/moon icon in the header). Click one of the 4 matrix diagrams in each mode. Confirm:
- The lightbox opens showing the diagram matching the current color scheme (not the hidden one).
- Closing the lightbox returns to the normal page with no layout shift.

Stop the server (Ctrl-C) when done.

- [ ] **Step 5: Commit**

```bash
git add zensical.toml
git commit -m "docs: enable glightbox image zoom for content images"
```

---

### Task 5: Link validation

**Files:**
- Modify: `zensical.toml` (add one new top-level TOML table)

**Interfaces:**
- Produces: `[project.validation]` table in `zensical.toml`. Independent of Tasks 1–4. Does not touch `.readthedocs.yaml` or `.github/workflows/docs-preview.yml` — stays warning-only, never `--strict` in an automated build.

- [ ] **Step 1: Confirm baseline strict build passes (sanity check before adding explicit config)**

Run:
```bash
rm -rf site
uv run zensical build --strict
echo "exit: $?"
```
Expected: `exit: 0`, output ends with `No issues found`. (Zensical's defaults already have `invalid_links`/`invalid_link_anchors` on — this task makes that explicit and documents intent, it should not change behavior.)

- [ ] **Step 2: Add explicit validation config**

Append to the end of `zensical.toml`:

```toml
[project.validation]
invalid_links = true
invalid_link_anchors = true
```

- [ ] **Step 3: Re-run strict build and confirm identical result (green)**

Run:
```bash
rm -rf site
uv run zensical build --strict
echo "exit: $?"
```
Expected: `exit: 0`, `No issues found` — same as Step 1. If this now reports warnings that Step 1 did not, something in an earlier task (1–4) introduced a broken link or anchor — stop and investigate before continuing; do not silence the warning by turning the check back off.

- [ ] **Step 4: Confirm no automated build was made strict**

Run:
```bash
grep -n "strict" .readthedocs.yaml .github/workflows/docs-preview.yml
```
Expected: no matches (empty output). If either file already contains `--strict` from unrelated prior work, leave it as-is — this task must not add it.

- [ ] **Step 5: Commit**

```bash
git add zensical.toml
git commit -m "docs: make zensical link validation explicit"
```

---

### Task 6: Header autohide

**Files:**
- Modify: `zensical.toml` (add one entry to the existing `features` list in `[project.theme]`)

**Interfaces:**
- Produces: `"header.autohide"` entry in the `features` array. Independent of Tasks 1–5.

- [ ] **Step 1: Confirm the baseline config JSON has no autohide feature (red)**

Run:
```bash
rm -rf site
uv run zensical build
python3 -c "
import re
data = open('site/index.html', encoding='utf-8').read()
m = re.search(r'\"features\":\[[^\]]*\]', data)
print(m.group(0))
"
```
Expected: a JSON array of feature strings that does **not** contain `header.autohide`.

- [ ] **Step 2: Add the feature flag**

In `zensical.toml`, in the `[project.theme]` block's `features` list, add `"header.autohide"` as a new entry (after `"navigation.tracking"`, alongside the other feature strings — exact position within the list doesn't matter, TOML arrays aren't order-sensitive for this theme).

- [ ] **Step 3: Rebuild and verify the feature flag appears (green)**

Run:
```bash
rm -rf site
uv run zensical build
python3 -c "
import re
data = open('site/index.html', encoding='utf-8').read()
m = re.search(r'\"features\":\[[^\]]*\]', data)
print(m.group(0))
"
```
Expected: the JSON array now contains `\"header.autohide\"` (confirmed present during design research, embedded in the inline `<script type=\"application/json\">` config block that precedes `bundle.*.min.js`).

- [ ] **Step 4: Manual visual check**

Run:
```bash
uv run zensical serve --open
```
On any page with enough content to scroll, scroll down — the header should hide. Scroll up — it should reappear. Stop the server (Ctrl-C) when done.

- [ ] **Step 5: Commit**

```bash
git add zensical.toml
git commit -m "docs: enable header autohide on scroll"
```

---

### Task 7: Final full-site verification

**Files:** none (verification only)

**Interfaces:**
- Consumes: the final `zensical.toml` and `includes/abbreviations.md` from Tasks 1–6.

- [ ] **Step 1: Full clean build**

```bash
rm -rf site
uv run zensical build --strict
echo "exit: $?"
```
Expected: `exit: 0`, `No issues found`.

- [ ] **Step 2: Confirm all 6 markers together**

```bash
echo "previews:  $(grep -rl 'data-preview' site --include='*.html' | wc -l)"
echo "tag icons: $(grep -rl 'md-tag-icon' site --include='*.html' | wc -l)"
echo "abbr:      $(grep -rl '<abbr' site --include='*.html' | wc -l)"
echo "glightbox: $(grep -rl 'glightbox' site --include='*.html' | wc -l)"
python3 -c "
import re
data = open('site/index.html', encoding='utf-8').read()
m = re.search(r'\"features\":\[[^\]]*\]', data)
print('autohide: ', 'header.autohide' in m.group(0))
"
```
Expected: all counts `> 0`, `autohide: True`.

- [ ] **Step 3: Interactive spot-check**

```bash
uv run zensical serve --open
```
Cover, per the spec's acceptance criteria:
- Hover an internal link on 3 different pages (e.g. `index.md`, `content/getting_started/index.md`, `content/theory.md`) → preview card appears.
- Visit each of the 4 tagged example pages → tag chips show their new icons.
- Hover `LCA` / `GWP` / etc. anywhere in body text → glossary tooltip appears.
- Click a `theory.md` diagram and a notebook plot image, in both light and dark mode → lightbox opens with the correct-scheme image.
- Scroll down/up on any long page → header hides/reappears.

Stop the server (Ctrl-C) when done. This task produces no commit — it's a checkpoint confirming Tasks 1–6 compose correctly.

## Self-Review

- **Spec coverage:** all 6 spec items (previews, tag icons, glossary, glightbox, validation, autohide) map 1:1 to Tasks 1–6; the spec's "Acceptance" verification list maps to Task 7.
- **Placeholders:** none — every step's exact TOML/Markdown/command content is given verbatim, and every expected result was empirically confirmed against a real `zensical build` during design research (see design doc's Risks section), not guessed.
- **Type/name consistency:** tag identifiers (`example`, `premise`, `dynchar`, `case-study`, `paper`, `excel`) match between `[project.extra.tags]` and `[project.theme.icon.tag]` in Task 2; the glossary filename (`includes/abbreviations.md`) matches between Task 3's file creation and its `auto_append` reference.
- **Scope:** single subsystem (Zensical config), 7 small independent tasks, each leaves the site in a working, buildable state — no task depends on another's TOML content, so they can be reordered or done individually without breaking the build.
