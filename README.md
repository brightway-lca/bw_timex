# timex
*This package is still under development and some functionalities may change in the future.*

This is a python package for time explicit Life Cycle Assessment, building on top of the [Brightway](https://docs.brightway.dev/en/latest) LCA framework. `timex` enables consideration of both the timing of processes & emissions (e.g., end-of-life treatment occurs 20 years after construction), as well as the changing state of the production system (e.g., increasing shares of renewable electricity in the future). Users can define temporal distributions for process and emission exchanges, which are then automatically mapped to corresponding time explicit databases. Consequently, the resulting time explicit LCI reflects the current technology status within the production system at the actual time of each process.

<p align=center><img width="428" alt="image" src="https://github.com/TimoDiepers/timex/assets/90762029/319089a6-7e16-4aa6-8b68-64e1e9e6f4bb"></p>

### User inputs:
Upon creation of the foreground database, the user needs to specify temporally distributed exchanges. `Temporal distributions` can occur at technosphere and biosphere exchanges, can be absolute (e.g. 2024-03-18) or relative (e.g. 3 years before) and can have flexible temporal resolution (years to days). They can also be flexibly spread over time, see `bw_temporalis Temporal distributions`. If no time of occurrence is given, it is assumed that the process or emission occurs at the same time as its consuming or emitting activity. 

Second, the user needs to specify which background databases represents what time in a mapping dictionary. A filter function can be applied to exclude a (sub)set of databases from the graph traversal to speed up computation time with large databases.

### Computation of time explicit inventory:
When a `timexLCA` is calculated, it first calculates a static LCA of the system. Using functionalities of the [`bw_temporalis`](https://github.com/brightway-lca/bw_temporalis) package, the static impact information is then used for a priority-first graph traversal, following the largest contributions within the supply chain. During traversal, it uses convolution to propagate the temporal profiles of processes through time. 

The temporal information of all technosphere exchanges in the system is stored in a timeline, containing all information to uniquely identify the time-explicit exchanges and link them to the most fitting background database(s) to be sourced from. In order to reduce complexity, technosphere exchanges from the same providing process to the same consuming process within a user-defined time window (e.g. 1 year or 1 month) are aggregated into one exchange. 

Next, the temporally-resolved supply chain from the graph traversal is translated into a matrix format using `datapackages`. This allows to use all conventional matrix-based LCA functionalities later on, which is not possible with a graph-traversal only approach. Datapackages modify the static LCA technosphere and biosphere matrices, adding new time-specific processes for each process in the timeline. At the intersection between the foreground and background system, temporal markets are created that source the corresponding exchange from the most suitable background database based on temporal proximity.
The matrix modification steps are shown in the figure below for a very simple system with temporal information, consisting of two processes, A and B, and one biosphere flow x occuring at B.

![flowchart for simple system](flowchart.png)

### Life cycle impact assessment options:
The user can choose to calculate static or dynamic impact assessment with this temporally-resolved LCA matrices. The time-mapped static LCIA uses static characterization with the temporally resolved LCA matrices. Thus, it will provide different static scores than the original LCIA, if there are differences in the LCIs in the background datbases. Dynamic impact assessment also takes the timing of the biosphere flows into account. Biosphere flows are assumed to occur at the same time as their emitting process, unless a `temporal distribution` is added to the biosphere exchange. In this case, the temporal profiles of the emitting process and the biosphere flow are propagated with convolution. Dynamic impact assessment is implemented for radiative forcing and GWP, with flexible time horizons (default of 100 years). Optionally, the time horizon can be fixed for the entire product system (Levasseur 2010 approach), which gives lower impact to emissions occurring later in the life cycle. 

### Questions and remarks:
An [example notebook](notebooks/example.ipynb) demonstrating the functionalities is provided. For suggestions of improvements or reporting of bugs, please open an issue on the Github page, send a pull request or  directly contact the maintainers.
