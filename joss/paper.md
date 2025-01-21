---
title: "`bw_timex`: a python package for time-explicit life cycle assessment"
tags:
  - Python
  - time-explicit LCA
  - life cycle assessment
  - prospective
  - dynamic

authors:
  - name: Timo Diepers
    orcid: 0009-0002-8566-8618
    affiliation: 1
  - name: Amelie Müller
    orcid: 0000-0001-5609-5609
    affiliation: "2,3"
  - name: Arthur Jakobs
    orcid: 0000-0003-0825-2184
    affiliation: 4

affiliations:
  - name: Institute of Technical Thermodynamics, RWTH Aachen University, Germany
    index: 1
  - name: Institute of Environmental Sciences (CML), Leiden University, The Netherlands
    index: 2
  - name: Flemish Institute for Technology Research (VITO), EnergyVille, Belgium
    index: 3
  - name: Technology Assessment Group, Laboratory for Energy Analysis, Center for Nuclear Engineering and Sciences & Center for Energy and Environmental Sciences, Paul Scherrer Institut (PSI), Villigen PSI, Switzerland
    index: 4

date: 01 January 2025
bibliography: paper.bib
---

# Summary

`bw_timex` is a Python package for time-explicit Life Cycle Assessment (LCA). Time-explicit LCA
enables the quantification of environmental impacts of products and processes over time, considering
their temporal distribution and evolution. Accordingly, `bw_timex` allows to simultaneously account
for:

- the timing of processes throughout the supply chain (e.g., end-of-life treatment occurs 20 years
  after construction),
- variable and/or evolving supply chains and technologies (e.g., increasing shares of renewable
  electricity in the future), and
- the timing of emissions (e.g., enabling the use of dynamic characterization functions).

To achieve this, `bw_timex` uses graph traversal to convolute process-relative temporal
distributions through the supply chain, and then automatically relinks Life Cycle Inventories (LCIs)
across time-specific LCI databases. The resulting time-explicit LCI reflects the current technology
status within the product system at the actual time of each process. Moreover, `bw_timex` preserves
the timing of emissions, enabling the application of dynamic characterization methods alongside
standard static characterization factors.

# Statement of need

LCA traditionally assumes a static LCI, in which all processes occur simultaneously and do not
change over time [@Heijungs:2002]. To add a temporal dimension in LCA, the fields of dynamic LCA
(dLCA) and prospective LCA (pLCA) have emerged. While dLCA focuses on when processes and emissions
occur and how impacts are distributed over time (*temporal distribution*), it typically assumes the
underlying product system remains the same [@Beloin:2020]. Conversely, pLCA tracks how processes
evolve (*temporal evolution*) using future scenarios, but generally only assesses a single (future)
point in time, overlooking that processes occur at different times across a product’s life cycle
[@Arvidsson:2024]. Both fields have seen open-source tool development in recent years, including
`Temporalis` [@Cardellini:2018] for dLCA and `premise` [@Sacchi:2022], `Futura`[@Joyce:2022] and
`pathways` [@Sacchi:2024] for pLCA. However, a comprehensive open-source package that allows
consideration of both temporal distribution and evolution in a time-explicit LCA is currently
lacking.

`bw_timex` addresses this gap by providing a framework for time-explicit LCA calculations within
the `Brightway` ecosystem [@Mutel:2017]. It combines considerations of temporal distribution and
evolution by accounting for both the timing of processes and emissions as well as the state of the
product system at the respective points in time. This makes `bw_timex` particularly useful for
studies involving variable or strongly evolving product systems, long-lived products, and biogenic
carbon.

# Acknowledgements

This work received funding from the European Union’s Horizon Europe Research and Innovation
Programme ForestPaths (ID No 101056755) and Swiss Center of Excellence on Net Zero Emissions (SCENE)
project.

# References
