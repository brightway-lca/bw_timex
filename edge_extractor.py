from dataclasses import dataclass
from heapq import heappop, heappush
from typing import Callable, List

from bw_temporalis import TemporalisLCA, TemporalDistribution


@dataclass
class Edge:
    """
    Class for storing a temporal edge with source and target.

    Leaf edges link to a source process which is a leaf in
    our graph traversal (either through cutoff or a filter
    function).

    Attributes
    ----------
    distribution : TemporalDistribution
    leaf : bool
    consumer : int
    producer : int

    """

    distribution: TemporalDistribution
    leaf: bool
    consumer: int
    producer: int


class EdgeExtracter(TemporalisLCA):
    def __init__(self, *args, edge_filter_function: Callable = None, **kwargs):
        super().__init__(*args, **kwargs)
        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False

    def build_edge_timeline(self, node_timeline: bool | None = False) -> List:
        heap = []
        timeline = []

        for edge in self.edge_mapping[self.unique_id]:
            node = self.nodes[edge.producer_unique_id]
            heappush(
                heap,
                (
                    1 / node.cumulative_score,
                    self.t0 * edge.amount,
                    node,
                ),
            )
            timeline.append(
                Edge(
                    distribution=self.t0 * edge.amount,
                    leaf=False,
                    consumer=self.unique_id,
                    producer=node.activity_datapackage_id,
                )
            )

        while heap:
            _, td, node = heappop(heap)

            for edge in self.edge_mapping[node.unique_id]:
                row_id = self.nodes[edge.producer_unique_id].activity_datapackage_id
                col_id = node.activity_datapackage_id
                exchange = self.get_technosphere_exchange(
                    input_id=row_id,
                    output_id=col_id,
                )
                value = (
                    self._exchange_value(
                        exchange=exchange,
                        row_id=row_id,
                        col_id=col_id,
                        matrix_label="technosphere_matrix",
                    )
                    / node.reference_product_production_amount
                )
                producer = self.nodes[edge.producer_unique_id]
                leaf = self.edge_ff(row_id)

                distribution = (td * value).simplify()
                timeline.append(
                    Edge(
                        distribution=distribution,
                        leaf=leaf,
                        consumer=node.activity_datapackage_id,
                        producer=producer.activity_datapackage_id,
                    )
                )
                if not leaf:
                    heappush(
                        heap,
                        (
                            1 / node.cumulative_score,
                            distribution,
                            producer,
                        ),
                    )
        return timeline