---
title: "bw_timex: A Python Package for Time-Explicit Life Cycle Assessment"
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

`bw_timex` is a Python package for time-explicit Life Cycle Assessment (LCA). Unlike conventional LCA, time-explicit LCA allows the quantification of environmental impacts of products and processes *over time*, considering their temporal distribution and evolution. As such, `bw_timex` allows to account simultaneously for:

- the timing of processes throughout the supply chain (e.g., end-of-life treatment occurs 20 years after production),
- variable and/or evolving supply chains and technologies (e.g., increasing shares of renewable electricity in the future), and
- the timing of emissions (enabling dynamic characterization).

To achieve this, `bw_timex` uses graph traversal to convolve process-relative temporal distributions through the supply chain. From the resulting timeline of technosphere exchanges, Life Cycle Inventories (LCIs) are automatically linked across time-specific background databases. The resulting time-explicit LCI reflects the current technology status within the product system at the actual time of each process. Moreover, `bw_timex` preserves the timing of emissions, enabling both dynamic and static Life Cycle Impact Assessment.

# Statement of need

LCA traditionally assumes a static system, where all processes occur simultaneously and do not change over time [@Heijungs:2002]. To add a temporal dimension in LCA, the fields of dynamic LCA (dLCA) and prospective LCA (pLCA) have emerged. While dLCA focuses on when processes and emissions occur and how impacts are distributed over time (*temporal distribution*), it typically assumes that the underlying product system remains the same [@Beloin:2020]. Conversely, while pLCA tracks how processes evolve (*temporal evolution*) using future scenarios, it generally only assesses a single (future) point in time, ignoring that processes occur at different times across a product’s life cycle [@Arvidsson:2024]. Both fields have seen open-source tool development, including `Temporalis` [@Cardellini:2018] for dLCA and `premise` [@Sacchi:2022], `Futura` [@Joyce:2022] and `pathways` [@Sacchi:2024] for pLCA. However, a comprehensive open-source package that supports consideration of both temporal distribution and evolution is currently lacking.

`bw_timex` addresses this gap by providing a framework for time-explicit LCA calculations within the `Brightway` ecosystem [@Mutel:2017]. It combines considerations of temporal distribution and evolution by accounting for both the timing of processes and emissions as well as the state of the product system at the respective points in time. This makes `bw_timex` particularly useful for studies involving variable or strongly evolving product systems, long-lived products, biogenic carbon and scenario analyses.

# Workflow

A time-explicit LCA with `bw_timex` follows four main steps, as illustrated in \autoref{fig:workflow}. First, a conventional product system model is temporalized by adding process-relative temporal distributions (rTDs) to the exchanges (c.f. @Cardellini:2018). These rTDs describe how the amount of a technosphere or biosphere exchange is distributed over time, relative to the consuming or emitting process. In Step 2, a timeline of technosphere exchanges is constructed by convolving rTDs along the supply chain, starting from the absolute reference time for the demand, which is defined by the user. In Step 3, the exchanges in the timeline are re-linked to time-specific background databases that reflect the technology landscape at specific points in time. Based on the temporally re-linked product system, a time-explicit LCI is calculated, preserving the timing of processes and emissions. The inventory is calculated following the conventional matrix-based LCA formulation [@Heijungs:2002], with the time dimension embedded in the matrices through additional row/column pairs. In Step 4, these emissions are characterized, either using standard characterization factors or by applying dynamic characterization functions that take the emissions’ timing into account.

![Workflow of a time-explicit LCA with `bw_timex`.\label{fig:workflow}](workflow.pdf){width=9cm}

# Further reading

The documentation of the `bw_timex` package, including installation instructions, extensive example notebooks and detailed API reference, can be found at [https://docs.brightway.dev/projects/bw-timex](https://docs.brightway.dev/projects/bw-timex). For a detailed explanation of the framework of time-explicit LCA, please refer to our accompanying publication [@MuellerDiepers:2025].

# Acknowledgements

We thank Chris Mutel for his help in adapting the graph traversal algorithm. This work received funding from the European Union’s Horizon Europe Research and Innovation Programme ForestPaths (ID No 101056755) and from the ETH Board in the framework of the Joint Initiative SCENE, Swiss Center of Excellence on Net Zero Emissions.


# References
