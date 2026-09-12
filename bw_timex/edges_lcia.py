"""
Characterisation of a bw_timex time-explicit inventory with the `edges` package.

`edges` (https://edges.readthedocs.io) characterises *exchanges* rather than flows, and its
characterisation factors can be symbolic expressions evaluated per scenario year. bw_timex knows
when each process runs and when each emission occurs, so the two combine into characterisation
factors evaluated at each exchange's own year.
"""

from __future__ import annotations

EDGES_IMPORT_ERROR = (
    "The `edges` package is required for TimexLCA.edges_lcia(). Install it with "
    "`pip install bw_timex[edges]`. Note that edges supports Python >=3.10,<3.13, while "
    "bw_timex itself also runs on 3.13 - on 3.13 the extra installs nothing."
)

try:
    from edges import EdgeLCIA
    from edges.matrix_builders import build_technosphere_edges_matrix
    from edges.utils import get_flow_matrix_positions
except ImportError as exc:
    raise ImportError(EDGES_IMPORT_ERROR) from exc
