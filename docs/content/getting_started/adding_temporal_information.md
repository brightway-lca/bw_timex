# Step 1 - Adding temporal information

Let's look at a simple example:

```{mermaid}
:caption: Example production system
flowchart LR
subgraph technosphere
    A(Process A):::ei
    B(Process B):::ei
    B-->A
end

subgraph biosphere
    CO2(CO2):::fg
    B-.->CO2
    A-.->CO2
end

classDef ei color:#222832, fill:#3fb1c5, stroke:none;
classDef fg color:#222832, fill:#9c5ffd, stroke:none;
style technosphere fill:none, stroke:none;
style biosphere fill:none, stroke:none;
```

Alright, continuing