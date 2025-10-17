# AllEdgeExtractor

## Overview

`AllEdgeExtractor` is an alternative to `EdgeExtractor` that provides faster graph traversal for smaller foregrounds by traversing the entire supply chain without priority-first approach.

## Key Differences from EdgeExtractor

### EdgeExtractor
- **Inherits from**: `TemporalisLCA`
- **Traversal**: Priority-first using a heap (processes nodes with highest impact first)
- **Overhead**: Calculates LCA results on-the-go to determine priorities
- **Best for**: Large supply chains where prioritization helps avoid unnecessary calculations

### AllEdgeExtractor
- **Inherits from**: Nothing (standalone class)
- **Traversal**: Breadth-first using a simple FIFO queue (processes all edges systematically)
- **Overhead**: Minimal - just convolves temporal distributions without LCA calculations
- **Best for**: Smaller foregrounds where the overhead of prioritization is not worth it

## Usage

```python
from bw_timex.edge_extractor import AllEdgeExtractor
from bw2calc import LCA

# Assuming you have a prepared LCA object and starting temporal distribution
lca = LCA(demand={...}, ...)
lca.lci()

# Create AllEdgeExtractor
extractor = AllEdgeExtractor(
    lca=lca,
    starting_datetime=starting_td,  # TemporalDistribution
    edge_filter_function=my_filter,  # optional
    static_activity_indices=static_set,  # optional
    cutoff=1e-9,  # optional
    max_calc=2000  # optional
)

# Build edge timeline
edge_timeline = extractor.build_edge_timeline()
```

## Performance

For smaller foreground systems (< 100 processes), `AllEdgeExtractor` can provide significant speed improvements by avoiding:
1. LCA score calculations during traversal
2. Heap operations for priority management
3. The overhead of inheriting from TemporalisLCA

## Limitations

- Does not support edge type detection as comprehensively as EdgeExtractor
- No dynamic graph pruning based on impact significance
- Should not be used for very large supply chains where priority-first traversal is beneficial

## Implementation Details

The class works by:
1. Taking a prepared `bw2calc.LCA` object with technosphere matrix
2. Starting from the functional unit (demand)
3. Using breadth-first traversal to visit all edges
4. Convolving temporal distributions along the supply chain
5. Yielding absolute occurrence of all edges

Unlike `EdgeExtractor`, it does not require the full `bw_temporalis.TemporalisLCA` infrastructure and works directly with the technosphere matrix from `bw2calc.LCA`.
