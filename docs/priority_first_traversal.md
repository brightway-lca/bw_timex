# Priority-First vs Breadth-First Graph Traversal in EdgeExtractor

## Overview

The `EdgeExtractor` class now supports two different graph traversal methods for building edge timelines:

1. **Priority-First Traversal (default)**: Uses a heap-based approach that prioritizes nodes with the highest cumulative score
2. **Breadth-First Traversal**: Processes nodes in breadth-first order without requiring cumulative_score attributes

## Usage

### Default Behavior (Priority-First)

By default, `EdgeExtractor` uses priority-first traversal:

```python
from bw_timex import EdgeExtractor

extractor = EdgeExtractor(
    base_lca,
    starting_datetime=starting_datetime,
    edge_filter_function=my_filter_function
)

timeline = extractor.build_edge_timeline()
```

### Using Breadth-First Traversal

To use breadth-first traversal, set `priority_first_traversal=False`:

```python
from bw_timex import EdgeExtractor

extractor = EdgeExtractor(
    base_lca,
    starting_datetime=starting_datetime,
    edge_filter_function=my_filter_function,
    priority_first_traversal=False  # Enable breadth-first traversal
)

timeline = extractor.build_edge_timeline()
```

### Usage in TimelineBuilder

The parameter can also be passed through `TimexLCA.build_timeline()`:

```python
from bw_timex import TimexLCA
from datetime import datetime

tlca = TimexLCA(
    demand={activity.key: 1},
    method=("GWP", "example"),
    database_dates=database_dates,
)

tlca.build_timeline(
    starting_datetime=datetime(2024, 1, 1),
    priority_first_traversal=False  # Use breadth-first traversal
)
```

## When to Use Each Method

### Priority-First Traversal
- **Use when**: You want to prioritize high-impact nodes first
- **Requires**: Nodes must have `cumulative_score` attributes (computed by TemporalisLCA)
- **Benefits**: Focuses on most significant contributions first

### Breadth-First Traversal
- **Use when**: Working with graph traversal results that don't have cumulative_score attributes
- **Use when**: You want a systematic level-by-level traversal
- **Benefits**: Simpler traversal logic, no score calculation needed

## Implementation Details

Both traversal methods:
- Process all edges in the supply chain
- Propagate temporal distributions through convolution
- Stop at leaf nodes (filtered by `edge_filter_function`)
- Generate the same set of edges (in different order)

The key difference is the order in which nodes are processed:
- **Priority-first**: Orders by `1/cumulative_score` (highest impact first)
- **Breadth-first**: Orders by depth in the supply chain graph (FIFO queue)

## Requirements

For breadth-first traversal to work:
- `bw_graph_tools>=0.4` must be installed (already a dependency of bw_timex)
- If `bw_graph_tools` is not available and `priority_first_traversal=False`, an `ImportError` will be raised
