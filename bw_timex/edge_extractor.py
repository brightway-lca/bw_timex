from dataclasses import dataclass
from heapq import heappop, heappush
from numbers import Number
from typing import Callable

import numpy as np
from bw_temporalis import TemporalDistribution, TemporalisLCA

try:
    from bw_graph_tools import BreadthFirstGT
except ImportError:
    BreadthFirstGT = None

datetime_type = np.dtype("datetime64[s]")
timedelta_type = np.dtype("timedelta64[s]")


@dataclass
class Edge:
    """
    Class for storing a temporal edge with source and target.

    Leaf edges link to a source process which is a leaf in
    our graph traversal (either through cutoff or a filter
    function).

    """

    edge_type: str
    distribution: TemporalDistribution
    leaf: bool
    consumer: int
    producer: int
    td_producer: TemporalDistribution
    td_consumer: TemporalDistribution
    abs_td_producer: TemporalDistribution = None
    abs_td_consumer: TemporalDistribution = None


class EdgeExtractor(TemporalisLCA):
    """
    Child class of TemporalisLCA that traverses the supply chain just as the parent class but can create a timeline of edges, in addition timeline of flows or nodes.

    The edge timeline is then used to match the timestamp of edges to that of background
    databases and to replace these edges with edges from these background databases
    using Brightway Datapackages.
    """

    def __init__(
        self,
        *args,
        edge_filter_function: Callable = None,
        priority_first_traversal: bool = True,
        **kwargs,
    ) -> None:
        """
        Initialize the EdgeExtractor class and traverses the supply chain using
        functions of the parent class TemporalisLCA.

        Parameters
        ----------
        *args : Variable length argument list
        edge_filter_function : Callable, optional
            A callable that filters edges. If not provided, a function that always
            returns False is used.
        priority_first_traversal : bool, optional
            If True (default), uses priority-first graph traversal with a heap based on
            cumulative_score. If False, uses breadth-first graph traversal from
            bw_graph_tools.BreadthFirstGT, which does not require cumulative_score.
        **kwargs : Arbitrary keyword arguments

        Returns
        -------
        None
            stores the output of the TemporalisLCA graph traversal (incl. relation of edges (edge_mapping) and nodes (node_mapping) in the instance of the class.

        """
        super().__init__(*args, **kwargs)  # use __init__ of TemporalisLCA

        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False

        self.priority_first_traversal = priority_first_traversal

        # Validate that BreadthFirstGT is available if needed
        if not self.priority_first_traversal and BreadthFirstGT is None:
            raise ImportError(
                "BreadthFirstGT is not available. Please install bw_graph_tools>=0.4 "
                "or set priority_first_traversal=True."
            )

    def build_edge_timeline(self) -> list:
        """
        Creates a timeline of the edges from the output of the graph traversal.
        
        If priority_first_traversal is True (default):
            Starting from the edges of the functional unit node, it goes through
            each node using a heap, selecting the node with the highest impact first.
        
        If priority_first_traversal is False:
            Uses breadth-first graph traversal from bw_graph_tools.BreadthFirstGT,
            which processes nodes in breadth-first order without using cumulative_score.
        
        It propagates the TemporalDistributions of the edges from node to node through time 
        using convolution-operators. It stops in case the current edge is known to have no 
        temporal distribution (=leaf) (e.g. part of background database).

        Parameters
        ----------
        None

        Returns
        -------
        list
            A list of Edge instances with timestamps and amounts, and ids of its producing
            and consuming node.

        """
        if self.priority_first_traversal:
            return self._build_edge_timeline_priority_first()
        else:
            return self._build_edge_timeline_breadth_first()

    def _build_edge_timeline_priority_first(self) -> list:
        """
        Build edge timeline using priority-first graph traversal with heap.
        
        This is the original implementation that uses cumulative_score to prioritize
        nodes with the highest impact first.

        Returns
        -------
        list
            A list of Edge instances.
        """
        heap = []
        timeline = []

        for edge in self.edge_mapping[
            self.unique_id
        ]:  # starting at the edges of the functional unit
            node = self.nodes[edge.producer_unique_id]
            heappush(
                heap,
                (
                    1 / node.cumulative_score,
                    self.t0 * edge.amount,
                    self.t0,
                    self.t0,
                    node,
                ),
            )

            timeline.append(
                Edge(
                    edge_type="production",  # FU exchange always type production (?)
                    distribution=self.t0 * edge.amount,
                    leaf=False,
                    consumer=self.unique_id,
                    producer=node.activity_datapackage_id,
                    td_producer=edge.amount,
                    td_consumer=self.t0,
                    abs_td_producer=self.t0,
                )
            )

        while heap:
            _, td, td_parent, abs_td, node = heappop(heap)

            for edge in self.edge_mapping[node.unique_id]:
                row_id = self.nodes[edge.producer_unique_id].activity_datapackage_id
                col_id = node.activity_datapackage_id
                exchange = self.get_technosphere_exchange(
                    input_id=row_id,
                    output_id=col_id,
                )

                edge_type = exchange.data[
                    "type"
                ]  # can be technosphere, substitution, production or other string

                td_producer = (  # td_producer is the TemporalDistribution of the edge
                    self._exchange_value(
                        exchange=exchange,
                        row_id=row_id,
                        col_id=col_id,
                        matrix_label="technosphere_matrix",
                    )
                    / abs(node.reference_product_production_amount)
                )
                producer = self.nodes[edge.producer_unique_id]
                leaf = self.edge_ff(row_id)

                # If an edge does not have a TD, give it a td with timedelta=0 and the amount= 'edge value'
                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )

                distribution = (
                    td * td_producer
                ).simplify()  # convolution-multiplication of TemporalDistribution of consuming node (td) and consumed edge (edge) gives TD of producing node

                timeline.append(
                    Edge(
                        edge_type=edge_type,
                        distribution=distribution,
                        leaf=leaf,
                        consumer=node.activity_datapackage_id,
                        producer=producer.activity_datapackage_id,
                        td_producer=td_producer,
                        td_consumer=td_parent,
                        abs_td_producer=self.join_datetime_and_timedelta_distributions(
                            td_producer, abs_td
                        ),
                        abs_td_consumer=abs_td,
                    )
                )
                if not leaf:
                    heappush(
                        heap,
                        (
                            1 / node.cumulative_score,
                            distribution,
                            td_producer,
                            self.join_datetime_and_timedelta_distributions(
                                td_producer, abs_td
                            ),
                            producer,
                        ),
                    )
        return timeline

    def _build_edge_timeline_breadth_first(self) -> list:
        """
        Build edge timeline using breadth-first graph traversal.
        
        This uses BreadthFirstGT from bw_graph_tools, which does not require
        cumulative_score and processes nodes in breadth-first order.

        Returns
        -------
        list
            A list of Edge instances.
        """
        # Create a queue for breadth-first traversal
        # Each item is a tuple: (node, td, td_parent, abs_td)
        from collections import deque
        queue = deque()
        timeline = []
        visited = set()

        # Start with the functional unit edges
        for edge in self.edge_mapping[
            self.unique_id
        ]:  # starting at the edges of the functional unit
            node = self.nodes[edge.producer_unique_id]
            
            # Add to queue (no cumulative_score needed for breadth-first)
            queue.append(
                (
                    node,
                    self.t0 * edge.amount,
                    self.t0,
                    self.t0,
                )
            )

            timeline.append(
                Edge(
                    edge_type="production",  # FU exchange always type production (?)
                    distribution=self.t0 * edge.amount,
                    leaf=False,
                    consumer=self.unique_id,
                    producer=node.activity_datapackage_id,
                    td_producer=edge.amount,
                    td_consumer=self.t0,
                    abs_td_producer=self.t0,
                )
            )

        # Process queue in breadth-first order
        while queue:
            node, td, td_parent, abs_td = queue.popleft()

            # Mark as visited to avoid reprocessing
            node_key = (node.unique_id, id(td))
            if node_key in visited:
                continue
            visited.add(node_key)

            for edge in self.edge_mapping[node.unique_id]:
                row_id = self.nodes[edge.producer_unique_id].activity_datapackage_id
                col_id = node.activity_datapackage_id
                exchange = self.get_technosphere_exchange(
                    input_id=row_id,
                    output_id=col_id,
                )

                edge_type = exchange.data[
                    "type"
                ]  # can be technosphere, substitution, production or other string

                td_producer = (  # td_producer is the TemporalDistribution of the edge
                    self._exchange_value(
                        exchange=exchange,
                        row_id=row_id,
                        col_id=col_id,
                        matrix_label="technosphere_matrix",
                    )
                    / abs(node.reference_product_production_amount)
                )
                producer = self.nodes[edge.producer_unique_id]
                leaf = self.edge_ff(row_id)

                # If an edge does not have a TD, give it a td with timedelta=0 and the amount= 'edge value'
                if isinstance(td_producer, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([td_producer]),
                    )

                distribution = (
                    td * td_producer
                ).simplify()  # convolution-multiplication of TemporalDistribution of consuming node (td) and consumed edge (edge) gives TD of producing node

                timeline.append(
                    Edge(
                        edge_type=edge_type,
                        distribution=distribution,
                        leaf=leaf,
                        consumer=node.activity_datapackage_id,
                        producer=producer.activity_datapackage_id,
                        td_producer=td_producer,
                        td_consumer=td_parent,
                        abs_td_producer=self.join_datetime_and_timedelta_distributions(
                            td_producer, abs_td
                        ),
                        abs_td_consumer=abs_td,
                    )
                )
                
                # Add to queue if not a leaf (breadth-first, no cumulative_score needed)
                if not leaf:
                    queue.append(
                        (
                            producer,
                            distribution,
                            td_producer,
                            self.join_datetime_and_timedelta_distributions(
                                td_producer, abs_td
                            ),
                        )
                    )
        
        return timeline

    def join_datetime_and_timedelta_distributions(
        self, td_producer: TemporalDistribution, td_consumer: TemporalDistribution
    ) -> TemporalDistribution:
        """
        Joins a datetime TemporalDistribution (td_producer) with a timedelta
        TemporalDistribution (td_consumer) to create a new TemporalDistribution.

        If the producer does not have a TemporalDistribution, the consumer's
        TemporalDistribution is returned to continue the timeline.
        If both the producer and consumer have TemporalDistributions, they are joined together.

        Parameters
        ----------
        td_producer : TemporalDistribution
            TemporalDistribution of the producer. Expected to be a datetime TemporalDistribution.
        td_consumer : TemporalDistribution
            TemporalDistribution of the consumer. Expected to be a timedelta TemporalDistribution.

        Returns
        -------
        TemporalDistribution
            A new TemporalDistribution that is the result of joining the producer
            and consumer TemporalDistributions.

        Raises
        ------
        ValueError
            If the dtype of `td_consumer.date` is not `datetime64[s]` or the dtype
            of `td_producer.date` is not `timedelta64[s]`.

        """
        # if an edge does not have a TD, then return the consumer_td so that the timeline continues
        if isinstance(td_consumer, TemporalDistribution) and isinstance(
            td_producer, Number
        ):
            return td_consumer

        # Else, if both consumer and producer have a td (absolute and relative, respectively) join to TDs
        if isinstance(td_producer, TemporalDistribution) and isinstance(
            td_consumer, TemporalDistribution
        ):
            if not (td_consumer.date.dtype == datetime_type):
                raise ValueError(
                    f"`td_consumer.date` must have dtype `datetime64[s]`, but got `{td_consumer.date.dtype}`"
                )
            if not (td_producer.date.dtype == timedelta_type):
                raise ValueError(
                    f"`td_producer.date` must have dtype `timedelta64[s]`, but got `{td_producer.date.dtype}`"
                )
            date = (
                td_consumer.date.reshape((-1, 1)) + td_producer.date.reshape((1, -1))
            ).ravel()
            amount = np.array(len(td_consumer) * [td_producer.amount]).ravel()
            return TemporalDistribution(date, amount)
        else:
            raise ValueError(
                f"Can't join TemporalDistribution and something else: Trying with {type(td_consumer.date)} and {type(td_producer.date)}"
            )
