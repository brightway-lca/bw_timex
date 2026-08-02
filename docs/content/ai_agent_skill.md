---
icon: lucide/bot
---

# Using `bw_timex` with an AI coding agent

Most `bw_timex` studies start the same way: spin up a fresh environment, `pip install bw_timex`, and write a script or Jupyter notebook that sets up temporal distributions, background vintages, and a `TimexLCA`. If you're doing that with an AI coding agent (e.g. [Claude Code](https://claude.com/claude-code)), we maintain a **skill** that teaches it this workflow, including the parts agents most often get wrong (temporal distribution dates default to being relative to the *consumer*, not the producer, but can also be set as absolute dates instead; cross-vintage matching is by name/reference product/location, not by `code`) — so it writes correct `bw_timex` code on the first try instead of guessing at the API.

## Installing the skill

The skill lives in this repo at [`.claude/skills/bw-timex-analysis/SKILL.md`](https://github.com/brightway-lca/bw_timex/tree/main/.claude/skills/bw-timex-analysis). Since your study is typically its own project/environment (not a clone of `bw_timex` itself), copy the file into:

- `~/.claude/skills/bw-timex-analysis/SKILL.md` — available in every project on your machine, or
- `<your-study>/.claude/skills/bw-timex-analysis/SKILL.md` — scoped to one study.

```bash
mkdir -p ~/.claude/skills/bw-timex-analysis
curl -o ~/.claude/skills/bw-timex-analysis/SKILL.md \
  https://raw.githubusercontent.com/brightway-lca/bw_timex/main/.claude/skills/bw-timex-analysis/SKILL.md
```

Claude Code picks it up automatically and loads it whenever your prompt looks like a time-explicit LCA task. Other [agentskills.io](https://agentskills.io)-compatible tools (Codex, Gemini CLI via `~/.agents/skills/`) work the same way.

## What it covers

- [x] Writing the Brightway model and multi-vintage background databases, and `database_dates`
- [x] Temporal distributions and temporal evolution on exchanges (`add_temporal_distribution_to_exchange`, `add_temporal_evolution_to_exchange`)
- [x] The full `TimexLCA` workflow: `build_timeline()`, `lci()`, `static_lcia()`/`dynamic_lcia()` and their key parameters
- [x] Contribution analysis, plotting, and a table of common mistakes

!!! note

    The skill is a living document — if you hit a gap or a wrong assumption while using it, please [open an issue](https://github.com/brightway-lca/bw_timex/issues) or send a PR against the `SKILL.md` file.
