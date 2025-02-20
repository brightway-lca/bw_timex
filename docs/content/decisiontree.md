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
