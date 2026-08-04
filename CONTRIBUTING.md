# Contributing

We welcome contributions! If you have any questions, [open a discussion](https://github.com/brightway-lca/bw_timex/discussions) or [get in touch directly with the `bw_timex` developers ](mailto:timo.diepers@ltt.rwth-aachen.de)


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

## Report bugs or errors

Something is not working as expected? You have two options:

### 🥈 Report an error
Please open a new issue in the `bw_timex` [repository](https://github.com/brightway-lca/bw_timex/issues), describing the error and where you found it.
A member of the bw_timex developer community will then take care of the issue, but it may take some time for your issue to be resolved.

### 🥇  Fix an error yourself
If you have a solution to the error, you can [create a fork](https://github.com/brightway-lca/bw_timex/forks) of the `bw_timex` repository, make your changes and [create a pull request](https://github.com/brightway-lca/bw_timex/pulls). The developers will assess the changes and be eternally grateful!

[code of conduct]: codeofconduct
