# Step 1 - Adding temporal information

To get you started with time-explicit LCA, we'll investigate this very simple production system with two "technosphere" nodes A and B and a "biosphere" node representing some CO2 emissions. For the sake of this example, we'll assume that we demand Process A to run exactly once.
```{mermaid}
:caption: Example production system
flowchart LR
subgraph background[<i>background</i>]
    B(Process B):::bg
end

subgraph foreground[<i>foreground</i>]
    A(Process A):::fg
end

subgraph biosphere[<i>biosphere</i>]
    CO2(CO2):::bio
end

B-->|"3 kg \n &nbsp;"|A
A-.->|"5 kg \n  &nbsp;"|CO2
B-.->|"11 kg \n &nbsp;"|CO2

classDef fg color:#222832, fill:#3fb1c5, stroke:none;
classDef bg color:#222832, fill:#3fb1c5, stroke:none;
classDef bio color:#222832, fill:#9c5ffd, stroke:none;
style background fill:none, stroke:none;
style foreground fill:none, stroke:none;
style biosphere fill:none, stroke:none;
```
Let's set this up in brightway


Now, if you want to consider time in your LCA, you need to somehow add temporal information. For time-explicit LCA, we consider two kinds of temporal information, that will be discussed in the following.

## Temporal Distributions



Alright, continuing

```{mermaid}
:caption: Temporalized example production system
flowchart LR
subgraph background[" "]
    B_2020(Process B \n 2020):::bg
    B_2030(Process B \n 2030):::bg
end

subgraph foreground[" "]
    A(Process A):::fg
end

subgraph biosphere[" "]
    CO2:::b
end

    B_2020-->|"amounts: [30%,70%] * 3 kg\n dates:[-2,-1]" years|A
    A-.->|"amounts: [60%, 40%] * 5 kg\n dates: [0,+1]" years|CO2
    B_2020-.->|"amounts: [100%] * 11 kg\n dates:[0]" years|CO2
    B_2030-.->|"amounts: [100%] * 7 kg\n dates:[0]" years|CO2

    classDef bg color:#222832, fill:#3fb1c5, stroke:none;
    classDef fg color:#222832, fill:#3fb1c5, stroke:none;
    classDef b color:#222832, fill:#9c5ffd, stroke:none;
    style foreground fill:none, stroke:none;
    style background fill:none, stroke:none;
    style biosphere fill:none, stroke:none;

```
asdf