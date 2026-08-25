# Contributing

We welcome contributions! If you have any questions, [open a discussion](https://github.com/brightway-lca/bw_timex/discussions) or [get in touch directly with the `bw_timex` developers ](mailto:timo.diepers@ltt.rwth-aachen.de)

## Report bugs or errors

Something is not working as expected? You have two options:

### 🥈 Report an error
Please open a new issue in the `bw_timex` [repository](https://github.com/brightway-lca/bw_timex/issues), describing the error and where you found it.
A member of the bw_timex developer community will then take care of the issue, but it may take some time for your issue to be resolved.

### 🥇  Fix an error yourself
If you have a solution to the error, you can [create a fork](https://github.com/brightway-lca/bw_timex/forks) of the `bw_timex` repository, make your changes and [create a pull request](https://github.com/brightway-lca/bw_timex/pulls). The developers will assess the changes and be eternally grateful!

## Contributing to the code, examples or documentation

If you want to contribute to the development our code with a new feature, want to share your timex-example or add to the documentation, please follow the [GitHub contribution workflow (fork, branch, PR)](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests) to share your work.

### Docstring conventions

The API reference is generated from the docstrings with [mkdocstrings](https://mkdocstrings.github.io/), using the numpydoc style. Two sections need a specific format to render properly:

- **`Examples`** (plural - `Example` is not a section name mkdocstrings knows) with the code in a fenced block, not as `>>>` doctest lines:

  ````markdown
  Examples
  --------
  ```python
  tlca = TimexLCA(demand, method, database_dates)
  tlca.build_timeline()
  ```
  ````

- **`See Also`** as a markdown list, where each entry links to its target. Objects documented in this package use the [mkdocstrings cross-reference syntax](https://mkdocstrings.github.io/usage/#cross-references) (`[text][identifier]`, with the identifier being the full dotted path), everything else uses a normal markdown link:

  ```markdown
  See Also
  --------
  - [`TimelineBuilder`][bw_timex.timeline_builder.TimelineBuilder]: Class that builds the timeline.
  - [`dynamic_characterization`](https://dynamic-characterization.readthedocs.io/en/latest/): Package handling the dynamic characterization.
  ```

Both sections end up as admonitions in the rendered docs. Build the docs with `zensical build` and check that it reports no `griffe:` warnings - those point at docstrings that don't parse the way they look like they should.

## Building the documentation
Locally build the documentation via zensical through:
```bash
uv run docs/convert_notebooks.py && uv run --extra docs zensical serve
```

## Releasing a new version

`bw_timex` is published in three places, and each one is triggered differently:

| Target | Triggered by | Automated |
|---|---|---|
| [PyPI](https://pypi.org/project/bw_timex/) | creating the GitHub release | yes, by `python-package-deploy.yml` |
| [GitHub release](https://github.com/brightway-lca/bw_timex/releases) | `gh release create` | no |
| [conda `diepers` channel](https://anaconda.org/diepers/bw_timex) | `conda/build_upload.sh` | no, run locally |

The version number lives in exactly one place, `bw_timex/__init__.py`. `pyproject.toml` reads it through `tool.setuptools.dynamic`, and the conda recipe reads it with a regex, so bumping that one line is enough.

### 1. Prepare the release PR

Branch off `main`, never commit the bump directly:

```bash
git checkout -b release/vX.Y.Z origin/main
```

Bump `__version__` in `bw_timex/__init__.py`, following [semantic versioning](https://semver.org) - new features mean a minor bump, fixes alone a patch.

Then close out the changelog. `CHANGES.md` collects entries under `## Unreleased` as PRs merge; turn that heading into the new version with today's date:

```markdown
## [X.Y.Z] - YYYY-MM-DD
```

Before opening the PR, check the section is actually complete. Entries are easy to lose when a change lands outside the usual PR flow - anything pushed straight to `main`, or shipped as a side effect of a larger branch. Compare against the commits since the last tag:

```bash
git log --oneline vLAST..origin/main
```

Every user-visible change needs a line, each ending in a link to its PR (`([#207](https://github.com/brightway-lca/bw_timex/pull/207))`) or, if it never went through one, to its commit.

Open the PR, let CI pass, merge it.

### 2. Tag and create the GitHub release

Tag the **merge commit** of that PR, not your local branch tip:

```bash
git checkout main && git pull --ff-only
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

Release notes are the changelog section for that version, copied verbatim - no rewriting, no summarising. Extract it and hand it to `gh`:

```bash
awk '/^## \[X\.Y\.Z\]/{f=1;next} /^## \[/{f=0} f' CHANGES.md | sed '/^$/d' > /tmp/notes.md
gh release create vX.Y.Z --title "vX.Y.Z" --notes-file /tmp/notes.md --verify-tag
```

!!! warning "This is the point of no return"

    Creating the release is the irreversible step. `python-package-deploy.yml` runs `on: release: [created]` and publishes to PyPI, and a version number can never be reused there - not even after deleting the release. Make sure the tag points where you think it does before running this.

Watch the run and confirm the version actually landed:

```bash
gh run list --limit 3
curl -s https://pypi.org/pypi/bw_timex/json | python -c "import sys,json; print(json.load(sys.stdin)['info']['version'])"
```

### 3. Publish to conda

The conda package is built and uploaded by hand, from a checkout that is already at the released version - the recipe builds the working tree (`source: path: ..`), so make sure `main` is checked out, clean, and pulled.

One-time setup:

```bash
conda install -n base conda-build anaconda-client
anaconda login          # needs write access to the `diepers` channel
```

Then:

```bash
bash conda/build_upload.sh
```

The recipe generates its `run:` requirements from `pyproject.toml`, so the conda package and the wheel cannot declare different dependencies. If you add a dependency whose conda name differs from its PyPI name, add it to the `conda_names` mapping at the top of `conda/meta.yaml`.

!!! note "Why one dependency is unpinned on conda"

    `pyproject.toml` requires `bw_graph_tools >=0.9`, but conda has no such build - it exists only on the `cmutel` channel, which stops at 0.6 - so the constraint is dropped through the `unpinned_on_conda` list in the recipe. `bw_timex` never imports the package directly and the test suite passes against 0.6, but this does mean conda and PyPI users can end up on different versions of it. `pip check` reports the mismatch during the build without failing it. Remove the entry once a new enough build reaches a conda channel.

Confirm the upload:

```bash
conda search -c diepers bw_timex
```

### 4. Check the docs rebuilt

[Read the Docs](https://docs.brightway.dev/projects/bw-timex) builds from `.readthedocs.yaml` on every push to `main`, so the release PR already triggered it. Confirm the new version renders and that the changelog page shows the new section.



[code of conduct]: codeofconduct
