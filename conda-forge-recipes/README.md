# conda-forge Recipes for Brightway Temporal LCA Packages

This directory contains conda-forge recipes for submitting packages to conda-forge.

## Packages

| Package | Version | Dependencies on conda-forge |
|---------|---------|----------------------------|
| `bw_graph_tools` | 0.6 | All deps available |
| `dynamic_characterization` | 1.2.0 | All deps available |
| `bw_temporalis` | 1.2.0 | Depends on `bw_graph_tools` |
| `bw_timex` | 0.3.3 | Depends on all above |

## Submission Order

Due to dependencies, submit in this order:

1. **First batch** (can be submitted together):
   - `bw_graph_tools`
   - `dynamic_characterization`

2. **Second batch** (after first is merged):
   - `bw_temporalis`

3. **Final** (after bw_temporalis is merged):
   - `bw_timex`

**Alternative**: Submit all 4 in a single PR - conda-forge CI handles build order.

## How to Submit

### 1. Fork staged-recipes

```bash
# Fork https://github.com/conda-forge/staged-recipes on GitHub, then:
git clone https://github.com/YOUR_USERNAME/staged-recipes.git
cd staged-recipes
```

### 2. Copy recipes

```bash
# Copy all recipes to staged-recipes
cp -r /path/to/bw_timex/conda-forge-recipes/bw_graph_tools recipes/
cp -r /path/to/bw_timex/conda-forge-recipes/dynamic_characterization recipes/
cp -r /path/to/bw_timex/conda-forge-recipes/bw_temporalis recipes/
cp -r /path/to/bw_timex/conda-forge-recipes/bw_timex recipes/
```

### 3. Create branch and commit

```bash
git checkout -b add-brightway-temporal-packages
git add recipes/
git commit -m "Add bw_graph_tools, dynamic_characterization, bw_temporalis, bw_timex"
git push origin add-brightway-temporal-packages
```

### 4. Open Pull Request

- Go to https://github.com/conda-forge/staged-recipes
- Click "New Pull Request"
- Select your fork and branch
- Fill in the PR template
- Wait for CI to build and reviewers to approve

### 5. After Merge

Once merged, conda-forge automatically:
- Creates feedstock repos (e.g., `bw_timex-feedstock`)
- Adds you as maintainer
- Builds and publishes to conda-forge channel
- Auto-updates when you release new versions on PyPI

## Testing Recipes Locally

```bash
# Install conda-build
conda install conda-build

# Test a recipe
conda build recipes/bw_timex --check
```

## Expected Result

After all packages are on conda-forge, installation simplifies to:

```bash
conda install -c conda-forge bw_timex
```

No more `-c cmutel -c diepers` channels needed!

## Maintainer Notes

- Update SHA256 hashes when releasing new versions
- conda-forge bots will auto-create PRs for new PyPI releases
- Review and merge bot PRs to publish updates

## Contact

- conda-forge help: https://conda-forge.org/docs/
- Brightway: https://brightway.dev
