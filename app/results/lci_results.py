"""
LCI Results view for bw_timex web application.

This module provides visualization and analysis of Life Cycle Inventory results.
"""

import panel as pn
import pandas as pd
import numpy as np


def view():
    """Create the LCI results view."""
    
    # Title
    title = pn.pane.Markdown("# LCI Results")
    
    # Instructions
    instructions = pn.pane.Markdown("""
    View and analyze Life Cycle Inventory (LCI) results from your time-explicit calculations.
    
    ## Features:
    - Time-explicit inventory matrices
    - Flow analysis by time period
    - Export functionality
    - Contribution analysis
    """)
    
    # Results summary
    results_summary = create_results_summary()
    
    # Inventory matrix view
    inventory_view = create_inventory_matrix_view()
    
    # Flow analysis
    flow_analysis = create_flow_analysis()
    
    # Export options
    export_options = create_export_options()
    
    return pn.Column(
        title,
        instructions,
        pn.Spacer(height=20),
        results_summary,
        pn.Spacer(height=20),
        inventory_view,
        pn.Spacer(height=20),
        flow_analysis,
        pn.Spacer(height=20),
        export_options,
        sizing_mode='stretch_width'
    )


def create_results_summary():
    """Create LCI results summary."""
    
    summary_data = {
        'Metric': [
            'Matrix Dimensions',
            'Non-zero Elements',
            'Time Periods',
            'Processes',
            'Flows',
            'Calculation Time'
        ],
        'Value': [
            '1250 × 850',
            '15,420',
            '12',
            '245',
            '1,850',
            '2.3 seconds'
        ]
    }
    
    summary_table = pn.widgets.Tabulator(
        summary_data,
        show_index=False,
        width=400
    )
    
    # Performance metrics
    performance = pn.pane.Markdown("""
    **Calculation Status:** ✅ Complete
    
    **Memory Usage:** 45.2 MB
    
    **Sparsity:** 98.5%
    """)
    
    return pn.Column(
        pn.pane.Markdown("### Results Summary"),
        pn.Row(summary_table, performance),
        pn.pane.Markdown("Overview of the calculated time-explicit LCI results.")
    )


def create_inventory_matrix_view():
    """Create inventory matrix visualization."""
    
    # Matrix selector
    matrix_selector = pn.widgets.Select(
        name="Matrix Type",
        options=["Technosphere (A)", "Biosphere (B)", "Combined"],
        value="Technosphere (A)",
        width=200
    )
    
    # Time period selector
    time_selector = pn.widgets.Select(
        name="Time Period",
        options=["All periods", "2024-01", "2024-02", "2024-03", "2024-04"],
        value="All periods",
        width=150
    )
    
    # View options
    view_sparse = pn.widgets.Checkbox(name="Show sparse matrix", value=True)
    view_heatmap = pn.widgets.Checkbox(name="Show as heatmap", value=False)
    
    # Sample matrix data
    matrix_data = create_sample_matrix_data()
    matrix_table = pn.widgets.Tabulator(
        matrix_data,
        pagination='remote',
        page_size=20,
        sizing_mode='stretch_width'
    )
    
    # Matrix plot
    matrix_plot = create_matrix_plot()
    
    return pn.Column(
        pn.pane.Markdown("### Inventory Matrix"),
        pn.Row(matrix_selector, time_selector),
        pn.Row(view_sparse, view_heatmap),
        pn.Row(
            pn.Column(matrix_table, width=600),
            pn.Column(matrix_plot, width=500)
        )
    )


def create_sample_matrix_data():
    """Create sample matrix data for display."""
    np.random.seed(42)
    
    n_rows = 50
    processes = [f"Process_{i:03d}" for i in range(n_rows)]
    flows = [f"Flow_{i:03d}" for i in range(n_rows)]
    
    data = {
        'Process': processes,
        'Flow': flows,
        'Value': np.random.exponential(0.1, n_rows),
        'Unit': np.random.choice(['kg', 'MJ', 'kWh', 'm3'], n_rows),
        'Time_Period': np.random.choice(['2024-01', '2024-02', '2024-03'], n_rows)
    }
    
    return pd.DataFrame(data)


def create_matrix_plot():
    """Create matrix visualization plot."""
    try:
        import matplotlib.pyplot as plt
        
        # Sample sparse matrix visualization
        np.random.seed(42)
        matrix = np.random.exponential(0.1, (20, 20))
        matrix[matrix < 0.05] = 0  # Make it sparse
        
        fig, ax = plt.subplots(figsize=(8, 6))
        im = ax.imshow(matrix, cmap='viridis', aspect='auto')
        ax.set_title('Matrix Structure (Sample)')
        ax.set_xlabel('Columns')
        ax.set_ylabel('Rows')
        
        # Add colorbar
        plt.colorbar(im, ax=ax, label='Values')
        
        return pn.pane.Matplotlib(fig, width=500, height=400)
    except:
        return pn.pane.Markdown("*Matrix plot not available - install matplotlib*")


def create_flow_analysis():
    """Create flow analysis interface."""
    
    # Flow selection
    flow_selector = pn.widgets.Select(
        name="Flow Type",
        options=["All flows", "CO2 emissions", "Energy flows", "Material flows"],
        width=200
    )
    
    # Time aggregation
    time_agg = pn.widgets.Select(
        name="Time Aggregation",
        options=["Monthly", "Quarterly", "Yearly", "Total"],
        value="Monthly",
        width=150
    )
    
    # Flow analysis plot
    flow_plot = create_flow_plot()
    
    # Flow statistics
    flow_stats = create_flow_statistics()
    
    return pn.Column(
        pn.pane.Markdown("### Flow Analysis"),
        pn.Row(flow_selector, time_agg),
        pn.Row(
            pn.Column(flow_plot, width=700),
            pn.Column(flow_stats, width=300)
        )
    )


def create_flow_plot():
    """Create flow analysis plot."""
    try:
        import matplotlib.pyplot as plt
        
        # Sample time series data
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
        co2_emissions = [120, 135, 98, 145, 110, 88]
        energy_flows = [2500, 2800, 2200, 3100, 2600, 2000]
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
        
        # CO2 emissions
        ax1.plot(months, co2_emissions, 'o-', color='red', linewidth=2, markersize=6)
        ax1.set_title('CO2 Emissions over Time')
        ax1.set_ylabel('kg CO2-eq')
        ax1.grid(True, alpha=0.3)
        
        # Energy flows
        ax2.plot(months, energy_flows, 's-', color='blue', linewidth=2, markersize=6)
        ax2.set_title('Energy Flows over Time')
        ax2.set_ylabel('MJ')
        ax2.set_xlabel('Time Period')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return pn.pane.Matplotlib(fig, width=700, height=500)
    except:
        return pn.pane.Markdown("*Flow plot not available - install matplotlib*")


def create_flow_statistics():
    """Create flow statistics summary."""
    
    stats_data = {
        'Statistic': [
            'Total CO2',
            'Peak CO2',
            'Average Energy',
            'Peak Energy',
            'Total Flows',
            'Active Periods'
        ],
        'Value': [
            '696 kg',
            '145 kg',
            '2,533 MJ',
            '3,100 MJ',
            '1,245',
            '6'
        ]
    }
    
    stats_table = pn.widgets.Tabulator(
        stats_data,
        show_index=False,
        sizing_mode='stretch_width'
    )
    
    return pn.Column(
        pn.pane.Markdown("**Flow Statistics:**"),
        stats_table
    )


def create_export_options():
    """Create export options for LCI results."""
    
    # Export format selection
    export_format = pn.widgets.Select(
        name="Export Format",
        options=["CSV", "Excel", "JSON", "Pickle", "HDF5"],
        value="CSV",
        width=150
    )
    
    # Export content selection
    export_content = pn.widgets.CheckBoxGroup(
        name="Export Content",
        options=["Matrix data", "Flow summaries", "Metadata", "Plots"],
        value=["Matrix data", "Flow summaries"],
        inline=True
    )
    
    # Export buttons
    export_btn = pn.widgets.Button(
        name="Export Results",
        button_type="success",
        width=150
    )
    
    preview_btn = pn.widgets.Button(
        name="Preview Export",
        button_type="light",
        width=150
    )
    
    return pn.Column(
        pn.pane.Markdown("### Export Options"),
        pn.Row(export_format),
        export_content,
        pn.Row(export_btn, preview_btn),
        pn.pane.Markdown("Export your LCI results for further analysis or reporting.")
    )