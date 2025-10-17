from dataclasses import dataclass
from heapq import heappop, heappush
from numbers import Number
from typing import Callable

import numpy as np
from bw2calc import LCA
from bw_temporalis import TemporalDistribution, TemporalisLCA

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

    def __init__(self, *args, edge_filter_function: Callable = None, **kwargs) -> None:
        """
        Initialize the EdgeExtractor class and traverses the supply chain using
        functions of the parent class TemporalisLCA.

        Parameters
        ----------
        *args : Variable length argument list
        edge_filter_function : Callable, optional
            A callable that filters edges. If not provided, a function that always
            returns False is used.
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

    def build_edge_timeline(self) -> list:
        """
        Creates a timeline of the edges from the output of the graph traversal.
        Starting from the edges of the functional unit node, it goes through
        each node using a heap, selecting the node with the highest impact first.
        It, then, propagates the TemporalDistributions of the edges from node to
        node through time using convolution-operators. It stops in case the current edge
        is known to have no temporal distribution (=leaf) (e.g. part of background database).

        Parameters
        ----------
        None

        Returns
        -------
        list
            A list of Edge instances with timestamps and amounts, and ids of its producing
            and consuming node.

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


class AllEdgeExtractor:
    """
    Alternative to EdgeExtractor that traverses the entire supply chain graph
    without priority-first approach and without calculating LCA impacts.
    
    This class provides faster graph traversal for smaller foregrounds by
    simply convolving temporal distributions of edges and yielding the
    absolute occurrence of all edges, without the overhead of LCA calculations
    and prioritization.
    
    Unlike EdgeExtractor, this class does NOT inherit from TemporalisLCA.
    Instead, it builds directly on top of a bw2calc.LCA object.
    
    Example
    -------
    >>> from bw_timex.edge_extractor import AllEdgeExtractor
    >>> from bw2calc import LCA
    >>> from bw_temporalis import TemporalDistribution
    >>> import numpy as np
    >>> 
    >>> # Prepare LCA
    >>> lca = LCA(demand={my_activity: 1}, ...)
    >>> lca.lci()
    >>> 
    >>> # Create starting temporal distribution
    >>> t0 = TemporalDistribution(
    ...     date=np.array(['2024-01-01'], dtype='datetime64[s]'),
    ...     amount=np.array([1.0])
    ... )
    >>> 
    >>> # Create extractor
    >>> extractor = AllEdgeExtractor(
    ...     lca=lca,
    ...     starting_datetime=t0,
    ...     cutoff=1e-9,
    ...     max_calc=2000
    ... )
    >>> 
    >>> # Build timeline
    >>> edge_timeline = extractor.build_edge_timeline()
    
    See Also
    --------
    EdgeExtractor : Priority-first graph traversal with LCA calculations
    """

    def __init__(
        self, 
        lca: LCA,
        starting_datetime: TemporalDistribution,
        edge_filter_function: Callable = None,
        static_activity_indices: set = None,
        cutoff: float = 1e-9,
        max_calc: int = 2000,
        **kwargs
    ) -> None:
        """
        Initialize the AllEdgeExtractor.
        
        Parameters
        ----------
        lca : LCA
            A static LCA object that provides the technosphere matrix and
            activity mappings for the supply chain traversal.
        starting_datetime : TemporalDistribution
            The temporal distribution representing the functional unit timing.
        edge_filter_function : Callable, optional
            A callable that filters edges based on activity ID. If not provided,
            a function that always returns False is used.
        static_activity_indices : set, optional
            Set of activity indices to skip during traversal.
        cutoff : float, optional
            Cutoff threshold for edge amounts. Default is 1e-9.
        max_calc : int, optional
            Maximum number of edges to process. Default is 2000.
        **kwargs : dict
            Additional keyword arguments for compatibility.
        """
        self.lca = lca
        self.t0 = starting_datetime
        
        if edge_filter_function:
            self.edge_ff = edge_filter_function
        else:
            self.edge_ff = lambda x: False
        
        self.static_activity_indices = static_activity_indices or set()
        self.cutoff = cutoff
        self.max_calc = max_calc
        
        # Get mappings from LCA object
        self.activity_dict = lca.dicts.activity
        self.technosphere_matrix = lca.technosphere_matrix.tocsr()
        
        # Unique ID for functional unit (consumer in edge timeline)
        self.unique_id = -1
        
    def build_edge_timeline(self) -> list:
        """
        Traverse the entire supply chain graph and create a timeline of edges.
        
        Unlike EdgeExtractor's priority-first approach, this method traverses
        all edges in the supply chain without prioritization based on LCA scores.
        It simply convolves temporal distributions and tracks the absolute
        occurrence of all edges.
        
        The traversal uses a simple queue (FIFO) instead of a priority queue,
        processing all edges without calculating cumulative scores or impacts.
        
        Returns
        -------
        list
            A list of Edge instances with timestamps and amounts, and ids of
            producing and consuming activities.
        """
        timeline = []
        queue = []  # Simple queue for breadth-first traversal
        visited_activities = set()  # Track visited activities
        
        # Initialize with functional unit
        # Handle demand - could be dict or array
        if hasattr(self.lca, 'demand') and isinstance(self.lca.demand, dict):
            demand_dict = self.lca.demand
        else:
            # Construct demand dict from demand_array if needed
            demand_dict = {}
            if hasattr(self.lca, 'demand_array'):
                for idx, amount in enumerate(self.lca.demand_array):
                    if amount != 0:
                        activity_id = self.activity_dict.reversed.get(idx)
                        if activity_id:
                            demand_dict[activity_id] = amount
        
        for activity_id, demand_amount in demand_dict.items():
            # Get the matrix index for this activity
            matrix_idx = self.activity_dict.get(activity_id)
            if matrix_idx is None:
                continue
            
            # Create initial temporal distribution
            initial_td = self.t0 * demand_amount
            
            # Create td_producer as TemporalDistribution
            td_producer_fu = TemporalDistribution(
                date=np.array([0], dtype="timedelta64[Y]"),
                amount=np.array([demand_amount]),
            )
            
            # Add functional unit edge to timeline
            timeline.append(
                Edge(
                    edge_type="production",
                    distribution=initial_td,
                    leaf=False,
                    consumer=self.unique_id,
                    producer=activity_id,
                    td_producer=td_producer_fu,
                    td_consumer=self.t0,
                    abs_td_producer=self.t0,
                    abs_td_consumer=None,
                )
            )
            
            # Add to queue for processing
            queue.append({
                'activity_id': activity_id,
                'matrix_idx': matrix_idx,
                'td': initial_td,
                'td_relative': TemporalDistribution(
                    date=np.array([0], dtype="timedelta64[Y]"),
                    amount=np.array([demand_amount]),
                ),
                'abs_td': self.t0,
            })
        
        calc_count = 0
        
        # Process queue
        while queue and calc_count < self.max_calc:
            current = queue.pop(0)
            activity_id = current['activity_id']
            matrix_idx = current['matrix_idx']
            td = current['td']
            td_relative = current['td_relative']
            abs_td = current['abs_td']
            
            # Skip if already visited
            if matrix_idx in visited_activities:
                continue
            
            visited_activities.add(matrix_idx)
            
            # Skip if filtered or static
            if self.edge_ff(activity_id) or activity_id in self.static_activity_indices:
                continue
            
            # Get reference production amount (diagonal element)
            ref_production = abs(self.technosphere_matrix[matrix_idx, matrix_idx])
            if ref_production == 0:
                ref_production = 1.0
            
            # Get all inputs to this activity (column in technosphere matrix)
            # Process column of technosphere matrix
            col = self.technosphere_matrix.getcol(matrix_idx).tocoo()
            
            for row_idx, amount in zip(col.row, col.data):
                # Skip diagonal (production exchange)
                if row_idx == matrix_idx:
                    continue
                
                # Skip near-zero amounts
                if abs(amount) < self.cutoff:
                    continue
                
                calc_count += 1
                
                # Get producer activity ID
                producer_id = self.activity_dict.reversed.get(row_idx)
                if producer_id is None:
                    continue
                
                # Normalize by reference production
                normalized_amount = amount / ref_production
                
                # Create temporal distribution for this edge
                if isinstance(normalized_amount, Number):
                    td_producer = TemporalDistribution(
                        date=np.array([0], dtype="timedelta64[Y]"),
                        amount=np.array([normalized_amount]),
                    )
                else:
                    td_producer = normalized_amount
                
                # Convolve temporal distributions
                distribution = (td * td_producer).simplify()
                
                # Check if this is a leaf
                leaf = self.edge_ff(producer_id)
                
                # Calculate absolute temporal distribution
                abs_td_producer = self.join_datetime_and_timedelta_distributions(
                    td_producer, abs_td
                )
                
                # Determine edge type (simplified - would need more info for accurate type)
                edge_type = "technosphere"
                
                # Add edge to timeline
                timeline.append(
                    Edge(
                        edge_type=edge_type,
                        distribution=distribution,
                        leaf=leaf,
                        consumer=activity_id,
                        producer=producer_id,
                        td_producer=td_producer,
                        td_consumer=td_relative,
                        abs_td_producer=abs_td_producer,
                        abs_td_consumer=abs_td,
                    )
                )
                
                # Add to queue if not a leaf
                if not leaf:
                    queue.append({
                        'activity_id': producer_id,
                        'matrix_idx': row_idx,
                        'td': distribution,
                        'td_relative': td_producer,
                        'abs_td': abs_td_producer,
                    })
        
        return timeline
    
    def join_datetime_and_timedelta_distributions(
        self, td_producer: TemporalDistribution, td_consumer: TemporalDistribution
    ) -> TemporalDistribution:
        """
        Join a datetime TemporalDistribution with a timedelta TemporalDistribution.
        
        This method is identical to the one in EdgeExtractor to ensure
        compatibility with the timeline building process.
        
        Parameters
        ----------
        td_producer : TemporalDistribution
            TemporalDistribution of the producer (timedelta).
        td_consumer : TemporalDistribution
            TemporalDistribution of the consumer (datetime).
        
        Returns
        -------
        TemporalDistribution
            A new TemporalDistribution combining both.
        """
        # If producer has no TD, return consumer's TD
        if isinstance(td_consumer, TemporalDistribution) and isinstance(
            td_producer, Number
        ):
            return td_consumer
        
        # Join both TDs
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
