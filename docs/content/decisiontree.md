# Decision tree

To give a quick overview of when to use time-explicit LCA in comparison to other types of LCAs and to show which options are available when conducting a time-explicit LCA, we have created the following decision tree:

```{mermaid}
flowchart TD
    %% Define node classes
    classDef decision fill:#3fb1c5,color:black,stroke:none;
    classDef lcaType fill:#9c5ffd,color:black,stroke:none;
    classDef codeNode fill:#DBDBDB,text-align:left,color:black,stroke:none;

    TimingDecision{{"Does timing matter?"}}:::decision
    AspectDecision{{"Which aspects matter?"}}:::decision
    ConventionalLCA("Conventional LCA"):::lcaType
    WhenDecision{{"When?"}}:::decision
    ProspectiveLCA("Prospective LCA"):::lcaType
    RetrospectiveLCA("Retrospective LCA"):::lcaType
    TimeExplicitLCA("Time-explicit LCA"):::lcaType
    CodeTimeExplicit("tlca = bw_timex.TimexLCA(...)"):::codeNode
    DynamicLCA("Dynamic LCA"):::lcaType
    DynamicLCIADecision{{"Dynamic LCIA?"}}:::decision
    CodeStaticLCIA("tlca.lci(build_dynamic_biosphere=False) \n tlca.static_lcia()\nprint(tlca.static_score)"):::codeNode
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
