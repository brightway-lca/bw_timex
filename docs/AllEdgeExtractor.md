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

### Using with TimexLCA (Recommended)

The easiest way to use `AllEdgeExtractor` is through the `TimexLCA.build_timeline()` method with the `use_all_edge_extractor` parameter:

```python
from bw_timex import TimexLCA
from datetime import datetime

# Set up your demand and method
demand = {('my_foreground_database', 'my_process'): 1}
method = ("some_method_family", "some_category", "some_method")
database_dates = {
    'my_background_database_one': datetime.strptime("2020", "%Y"),
    'my_background_database_two': datetime.strptime("2030", "%Y"),
    'my_foreground_database': 'dynamic'
}

# Create TimexLCA instance
tlca = TimexLCA(demand, method, database_dates)

# Build timeline using AllEdgeExtractor (faster for small foregrounds)
tlca.build_timeline(
    starting_datetime="2024-01-01",
    use_all_edge_extractor=True  # <-- Enable AllEdgeExtractor
)

# Continue with your analysis
tlca.lci()
tlca.static_lcia()
```

### Basic Usage (Direct)

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

### Using in TimelineBuilder

`AllEdgeExtractor` is now integrated into `TimelineBuilder` and can be enabled via the `use_all_edge_extractor` parameter:

```python
from bw_timex.timeline_builder import TimelineBuilder

timeline_builder = TimelineBuilder(
    base_lca,
    starting_datetime=starting_datetime,
    edge_filter_function=edge_filter_function,
    database_dates=database_dates,
    database_dates_static=database_dates_static,
    activity_time_mapping=activity_time_mapping,
    node_collections=node_collections,
    nodes=nodes,
    temporal_grouping="year",
    use_all_edge_extractor=True  # <-- Enable AllEdgeExtractor
)
```

## Performance

For smaller foreground systems (< 100 processes), `AllEdgeExtractor` can provide significant speed improvements by avoiding:
1. LCA score calculations during traversal
2. Heap operations for priority management
3. The overhead of inheriting from TemporalisLCA

## When to Use AllEdgeExtractor

### Use AllEdgeExtractor when:
- Your foreground system is small (< 100 processes)
- You need to traverse the entire foreground anyway
- You want faster graph traversal without LCA overhead
- Your system doesn't benefit from impact-based pruning

### Use EdgeExtractor (default) when:
- Your supply chain is large and complex
- You want to skip low-impact branches
- You need accurate edge type detection
- Priority-based traversal provides meaningful speedup

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
