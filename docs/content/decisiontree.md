# What LCA should I do?

Not only are there many "types" of LCA already, `bw_timex` also adds lots of further options for time-explicit LCA alone. The following decision tree tries to give some guidance on what type of LCA is suitable in your case, and also navigate the options coming with `bw_timex`:

```{mermaid}
flowchart TD
    %% Define node classes
    classDef decision fill:#3fb1c5,color:black,stroke:none;
    classDef lcaType fill:#9c5ffd,color:black,stroke:none;
    classDef codeNode fill:#DBDBDB,text-align:left,color:black,stroke:none;

    TimingDecision{{"Do temporal aspects matter?"}}:::decision
    AspectDecision{{"Which aspects matter?"}}:::decision
    ConventionalLCA("Conventional LCA"):::lcaType
    WhenDecision{{"When?"}}:::decision
    ProspectiveLCA("Prospective LCA"):::lcaType
    RetrospectiveLCA("Retrospective LCA"):::lcaType
    TimeExplicitLCA("Time-explicit LCA"):::lcaType
    CodeTimeExplicit("tlca = bw_timex.TimexLCA(...)"):::codeNode
    DynamicLCA("Dynamic LCA"):::lcaType
    DynamicLCIADecision{{"Dynamic LCIA?"}}:::decision
    CodeStaticLCIA("tlca.lci(build_dynamic_biosphere=False)\n tlca.static_lcia()\nprint(tlca.static_score)"):::codeNode
    CodeDynamicLCI("tlca.lci(build_dynamic_biosphere=True)"):::codeNode
    BackgroundDecision{{"Interested \n in background system \n contributions?"}}:::decision
    CodeDynamicLCIA("tlca.dynamic_lcia()\nprint(tlca.dynamic_score)"):::codeNode
    CodeDisaggregatedLCIA("tlca.dynamic_lcia(use_disaggregated_background=True)\nprint(tlca.dynamic_score)"):::codeNode



    %% Define connections
    TimingDecision -- "yes" --> AspectDecision
    TimingDecision -- "no" --> ConventionalLCA
    AspectDecision -- "temporal evolution" --> WhenDecision
    AspectDecision -- "temporal distribution" --> DynamicLCA
    AspectDecision -- "temporal evolution \n and distribution" --> TimeExplicitLCA
    WhenDecision -- "in the future" --> ProspectiveLCA
    WhenDecision -- "in the past" --> RetrospectiveLCA
    TimeExplicitLCA --> CodeTimeExplicit
    CodeTimeExplicit --> DynamicLCIADecision
    DynamicLCIADecision -- "no, static LCIA" --> CodeStaticLCIA
    DynamicLCIADecision -- "yes" --> CodeDynamicLCI
    CodeDynamicLCI --> BackgroundDecision
    BackgroundDecision -- "no" --> CodeDynamicLCIA
    BackgroundDecision -- "yes" --> CodeDisaggregatedLCIA
```

## Modeling paradigm option: chimaera vs explicit process/product

Brightway supports more than one way to represent inventory data. See the Brightway inventory
overview on [processes, products, and something in between](https://docs.brightway.dev/en/latest/content/overview/inventory.html#processes-products-and-something-in-between)
for the broader data-model discussion. In short:

- **Explicit process/product** (`type="process"` + `type="product"`): products are separate
  nouns, processes are separate verbs, and production edges connect processes to products.
- **Chimaera** (`type="processwithreferenceproduct"`): one node acts as both process and
  reference product. This is common in existing LCI databases and compact for many models.

Both paradigms are valid modelling choices. `bw_timex` aims to support both when building
timelines and expanding matrices. The right choice depends on what you need to express and on the
data you start from.

### Terms used below

A **production-time group** is a set of product units supplied or produced at the same time. In
fleet and stock modelling, this is often called a **cohort**; for example, all vehicles produced in
2030 are the 2030 cohort. A **process/product version date** is the date that fixes a foreground
property of that group, such as vehicle efficiency or a material requirement. Some literature calls
this a **vintage**.

### When chimaera nodes are often pragmatic

Use chimaera activities when your data already comes this way, when each process has one clear
reference product, and when you do not need to attach separate temporal meaning to the product
output edge itself. Most conventional Brightway examples and many imported databases follow this
style.

For production-time group timing in a chimaera model, you usually add an intermediary foreground
activity and put the temporal distribution on a normal technosphere edge, e.g.:

```text
fleet_service -- production-time group TD --> fleet_driving
fleet_driving -- age TD --> electricity
```

This is a structural modelling pattern: `fleet_service` exists to give the production-time group
timing a place to live.

### When explicit process/product nodes are often clearer

Use explicit process/product nodes when distinguishing the demanded product from the operation
that produces it helps the model. This is especially useful when timing belongs naturally to the
production output edge, such as production of multiple production-time groups, delayed delivery, service availability, or
multi-output process modelling.

For production-time group timing in an explicit model, put the temporal distribution directly on
the production edge:

```text
fleet_process -- production-time group TD --> fleet_product
fleet_process -- age TD --> electricity
```

This makes production timing part of the graph topology instead of introducing a wrapper activity.
It also makes the two timeline dates easier to interpret:

- `date_consumer`: the process/product version date or demand-side process instance date.
- `date_producer`: the actual exchange event date.

### How this relates to temporal evolution

The modelling paradigm does not decide whether you use `consumer` or `producer` for temporal
evolution. That choice depends on what the evolving exchange amount means:

- Use `consumer` when the amount is a property of the consuming foreground process/product
  version date.
- Use `producer` when the amount is a property of the calendar year in which the exchange event
  happens.

Explicit process/product models often make this distinction more visible because a production-edge
temporal distribution can create distinct product/process version dates without an intermediary
node.
But the same conceptual rule applies in chimaera models if your graph structure creates meaningful
consumer and producer dates.

### Which temporal evolution reference should I use?

Foreground temporal evolution can be keyed to either the consumer timestamp or the producer timestamp. The names are graph terms:

- `temporal_evolution_reference="consumer"` means the factor is evaluated at `date_consumer`: the time of the foreground process instance using the exchange. In production-time group models, this is usually the **process/product version date**.
- `temporal_evolution_reference="producer"` means the factor is evaluated at `date_producer`: the time when the exchanged input/output event actually happens. This is the **calendar event date**.

Use `consumer` for version-locked properties, e.g. a vehicle produced in 2025 keeps its 2025 kWh/km in 2035. Use `producer` for calendar-year properties, e.g. a repair operation becomes more efficient for all active vehicles in 2035.

Rule of thumb:

```text
Property of the foreground process/product version date? -> consumer
Property of the exchange event year?                -> producer
```
