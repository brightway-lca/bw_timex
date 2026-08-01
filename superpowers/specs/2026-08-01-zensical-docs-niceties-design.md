# Design: Zensical reader niceties for `bw_timex` docs

Date: 2026-08-01
Branch: `docs/zensical-migration` (continuing the existing docs branch)

## Goal

Turn on a set of Zensical features that were left unused (or under-configured) by the
Sphinx → Zensical migration (`2026-07-31-zensical-docs-migration-design.md`): instant
link previews, styled tag icons, glossary tooltips, image lightbox, link validation, and
header autohide. All reader-facing, all config/content only — no theme code, no new
build steps, no external services.

## Non-goals

- Tag listing/index pages — not supported by Zensical yet (confirmed against docs and
  source; only per-page tag chips exist).
- `--strict` in the Read the Docs build — validation stays warning-only so a latent
  broken link can't block a deploy.
- Social cards, versioning (mike), comment system (giscus) — all need external
  service/hosting decisions, out of scope for "niceties."
- Any change to page content beyond the new glossary file (no rewriting prose, no new
  pages, no nav changes).

## Current state

- `zensical.toml` features already include `navigation.instant`,
  `navigation.instant.prefetch`, and `content.tooltips` — both are prerequisites for
  instant previews and are already satisfied, so no feature-flag change is needed for
  that piece.
- `[project.plugins.tags]` is enabled with no options: the 4 example notebook pages
  (`example_electric_vehicle_premise.md`, `example_simple_dynamic_characterization.md`,
  `example_Importing_model_from_excel.md`, `paper_case_study.md`) carry front-matter tags
  (`example`, `premise`, `dynamic characterization`, `case study`, `paper`, `excel`,
  6 distinct values total, sourced from `docs/convert_notebooks.py`'s `NOTEBOOK_META`)
  but render with the default hash icon — no `[project.extra.tags]` /
  `[project.theme.icon.tag]` mapping exists.
- `content.tooltips` is on but nothing exercises it: no abbreviations are defined
  anywhere in the docs, so the feature currently has no visible effect.
- No lightbox extension is configured; the ~39 content images across `theory.md` (13),
  the example notebooks (9 + 6 + 2 + 1), `examples/index.md` (3), and
  `getting_started/{index,lcia}.md` (2 each) are static.
- `[project.validation]` is absent from `zensical.toml` (defaults apply: `invalid_links`
  and `invalid_link_anchors` already on by default per Zensical, but making this explicit
  documents the intent and survives a future default change).
- `site_url` is set to `https://timodiepers.github.io/bw_timex/` (the GitHub Pages
  preview URL this branch actually deploys to via `.github/workflows/docs-preview.yml`,
  hosted on the `TimoDiepers/bw_timex` fork — confirmed live via `gh api
  repos/TimoDiepers/bw_timex/pages` — not the production RTD URL, since RTD only builds
  `main` on the upstream `brightway-lca/bw_timex`). Instant previews require `site_url`
  to match the deployed host, as they resolve targets via the generated `sitemap.xml`.

## Changes

### 1. Instant previews

Add to `zensical.toml`:

```toml
[project.markdown_extensions.zensical.extensions.preview]
configurations = [{ targets.include = ["**"] }]
```

`targets.include = ["**"]` applies to every page. The extension itself already skips
external links, footnotes, and header-permalinks (verified in
`zensical/extensions/preview.py`), so this can't leak previews onto links that shouldn't
have them. No theme-feature change needed (see Current state).

### 2. Tag icons

Add to `zensical.toml`:

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

Maps the 6 existing tag values to distinct lucide icons. No new tags, no front-matter
changes, no nav changes.

### 3. Glossary tooltips

New file `includes/abbreviations.md` at the **repository root** (outside `docs/`, per
Zensical's own recommendation, so the build doesn't treat it as an orphan/unreferenced
docs page):

```markdown
*[LCA]: Life Cycle Assessment
*[LCI]: Life Cycle Inventory
*[LCIA]: Life Cycle Impact Assessment
*[GWP]: Global Warming Potential
*[EOL]: End of Life
*[CRF]: Cumulative Radiative Forcing
*[EV]: Electric Vehicle
```

These 7 acronyms were picked by grepping `docs/content` + `docs/api` for actual usage
frequency (125/111/100/36/23/23/12 hits respectively) — no invented terms, no coverage
of one-off jargon.

Wire it up via the already-enabled `pymdownx.snippets` extension:

```toml
[project.markdown_extensions.pymdownx.snippets]
auto_append = ["includes/abbreviations.md"]
```

### 4. GLightbox

Add to `zensical.toml`:

```toml
[project.markdown_extensions.zensical.extensions.glightbox]
```

Default config (`auto = true`) wraps every content `<img>` in a click-to-zoom overlay.

### 5. Link validation

Add to `zensical.toml`:

```toml
[project.validation]
invalid_links = true
invalid_link_anchors = true
```

Warnings only — `.readthedocs.yaml`'s `build.commands` is untouched, so this cannot fail
a deploy. A manual `zensical build --strict` run is part of verification below, to catch
anything latent before merging, but strict mode is not wired into any automated build.

### 6. Header autohide

Add `"header.autohide"` to the `features` list in `zensical.toml`.

## Risks and verification

- **`site_url` points at the fork's GH Pages, not RTD prod**: needed now so the preview
  deployment (the only thing actually being reviewed on this branch) generates a correct
  `sitemap.xml` for instant previews. Must be switched back to
  `https://docs.brightway.dev/projects/bw-timex/en/latest/` before/when this branch
  merges to `main` and RTD takes over as the production build — flagging here so it
  isn't forgotten, not fixing it now.
- **Light/dark image pairs in `theory.md`**: this file has 4 image pairs
  (`#only-light`/`#only-dark`) that were the subject of several recent bug-fix commits
  (rounded corners, table overflow, mobile insets). GLightbox wraps every `<img>`,
  including the CSS-hidden variant of each pair. Since the hidden image has
  `display: none`, it should be unclickable and never enter the lightbox — verify this
  explicitly in both light and dark mode rather than assuming it.
- **Tag identifier collisions**: `[project.extra.tags]` keys with spaces
  (`"dynamic characterization"`, `"case study"`) must be quoted in TOML — already
  reflected above.
- **Icon names**: all `lucide/*` icons chosen (`flask-conical`, `leaf`, `trending-up`,
  `file-text`, `scroll-text`, `table`, `hash`) must exist in the bundled Lucide set;
  verify by building and visually inspecting tag chips (the repo already uses `lucide/*`
  icons elsewhere, e.g. `car-front`, `trending-up`, so the icon family itself is
  confirmed present).
- **Preview extension needs `site_url`**: already satisfied, but note the dependency so
  a future `site_url` removal doesn't silently break previews.
- **Acceptance**: `zensical build` completes with no new warnings; `zensical build
  --strict` is run once manually to confirm no pre-existing broken links; `zensical
  serve` spot-check covers: hovering an internal link on at least 3 different pages shows
  a preview card; the 4 tagged example pages show their new icons; hovering `LCA`/`GWP`/
  etc. anywhere in body text shows the glossary tooltip; clicking a theory.md diagram and
  a notebook plot opens the lightbox in both light and dark mode; the header hides on
  scroll-down and reappears on scroll-up.
