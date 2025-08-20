"""
LCIA Results view for bw_timex web application.

This module provides visualization and analysis of Life Cycle Impact Assessment results.
"""

import panel as pn
import pandas as pd
import numpy as np


def view():
    """Create the LCIA results view."""
    
    # Title
    title = pn.pane.Markdown("# LCIA Results")
    
    # Instructions
    instructions = pn.pane.Markdown("""
    View and analyze Life Cycle Impact Assessment (LCIA) results from your time-explicit calculations.
    
    ## Features:
    - Static and dynamic LCIA scores
    - Time-explicit characterization
    - Impact category analysis
    - Contribution analysis
    - Uncertainty assessment
    """)
    
    # Results overview
    results_overview = create_results_overview()
    
    # Impact scores
    impact_scores = create_impact_scores_view()
    
    # Time-explicit analysis
    temporal_analysis = create_temporal_analysis()
    
    # Contribution analysis
    contribution_analysis = create_contribution_analysis()
    
    # Export and actions
    export_actions = create_export_actions()
    
    return pn.Column(
        title,
        instructions,
        pn.Spacer(height=20),
        results_overview,
        pn.Spacer(height=20),
        impact_scores,
        pn.Spacer(height=20),
        temporal_analysis,
        pn.Spacer(height=20),
        contribution_analysis,
        pn.Spacer(height=20),
        export_actions,
        sizing_mode='stretch_width'
    )


def create_results_overview():
    """Create LCIA results overview."""
    
    # Summary metrics
    overview_data = {
        'Metric': [
            'Static LCIA Score',
            'Dynamic LCIA Score',
            'Time Horizon',
            'Impact Categories',
            'Calculation Method',
            'Uncertainty Analysis'
        ],
        'Value': [
            '245.7 kg CO2-eq',
            '267.3 kg CO2-eq',
            '100 years',
            '3 categories',
            'IPCC 2013 GWP100',
            'Monte Carlo (1000 runs)'
        ]
    }
    
    overview_table = pn.widgets.Tabulator(
        overview_data,
        show_index=False,
        width=500
    )
    
    # Key insights
    insights = pn.pane.Markdown("""
    ### Key Insights
    
    🔍 **Dynamic vs Static:** +8.8% increase when considering timing
    
    📈 **Peak Impact:** Occurs in month 4 (145.2 kg CO2-eq)
    
    ⚡ **Hotspot:** Energy consumption (67% of total impact)
    
    📊 **Uncertainty:** ±12.4% (95% confidence interval)
    """)
    
    return pn.Column(
        pn.pane.Markdown("### Results Overview"),
        pn.Row(overview_table, insights),
        pn.pane.Markdown("Summary of calculated LCIA scores and key metrics.")
    )


def create_impact_scores_view():
    """Create impact scores visualization."""
    
    # Impact category selector
    category_selector = pn.widgets.Select(
        name="Impact Category",
        options=["Climate Change", "Acidification", "Eutrophication", "All Categories"],
        value="Climate Change",
        width=200
    )
    
    # Score type selector
    score_type = pn.widgets.RadioButtonGroup(
        name="Score Type",
        options=["Static", "Dynamic", "Comparison"],
        value="Dynamic",
        button_type="success"
    )
    
    # Impact scores plot
    scores_plot = create_impact_scores_plot()
    
    # Scores table
    scores_table = create_impact_scores_table()
    
    return pn.Column(
        pn.pane.Markdown("### Impact Scores"),
        pn.Row(category_selector, score_type),
        pn.Row(
            pn.Column(scores_plot, width=700),
            pn.Column(scores_table, width=400)
        )
    )


def create_impact_scores_plot():
    """Create impact scores plot."""
    try:
        import matplotlib.pyplot as plt
        
        # Sample data for different impact categories
        categories = ['Climate\nChange', 'Acidification', 'Eutrophication']
        static_scores = [245.7, 0.124, 0.067]
        dynamic_scores = [267.3, 0.135, 0.071]
        
        x = np.arange(len(categories))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars1 = ax.bar(x - width/2, static_scores, width, label='Static LCIA', 
                       color='skyblue', alpha=0.8)
        bars2 = ax.bar(x + width/2, dynamic_scores, width, label='Dynamic LCIA', 
                       color='lightcoral', alpha=0.8)
        
        ax.set_xlabel('Impact Category')
        ax.set_ylabel('Impact Score')
        ax.set_title('Static vs Dynamic LCIA Scores')
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.1f}',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom',
                           fontsize=9)
        
        plt.tight_layout()
        return pn.pane.Matplotlib(fig, width=700, height=400)
    except:
        return pn.pane.Markdown("*Impact scores plot not available - install matplotlib*")


def create_impact_scores_table():
    """Create impact scores table."""
    
    scores_data = {
        'Impact Category': [
            'Climate Change',
            'Acidification',
            'Eutrophication'
        ],
        'Static Score': [
            '245.7 kg CO2-eq',
            '0.124 kg SO2-eq',
            '0.067 kg PO4-eq'
        ],
        'Dynamic Score': [
            '267.3 kg CO2-eq',
            '0.135 kg SO2-eq',
            '0.071 kg PO4-eq'
        ],
        'Difference': [
            '+8.8%',
            '+8.9%',
            '+6.0%'
        ]
    }
    
    scores_table = pn.widgets.Tabulator(
        scores_data,
        show_index=False,
        sizing_mode='stretch_width'
    )
    
    return pn.Column(
        pn.pane.Markdown("**Impact Scores Comparison:**"),
        scores_table
    )


def create_temporal_analysis():
    """Create temporal analysis visualization."""
    
    # Time horizon selector
    time_horizon = pn.widgets.Select(
        name="Time Horizon",
        options=["20 years", "100 years", "500 years", "Custom"],
        value="100 years",
        width=150
    )
    
    # Characterization function selector
    char_function = pn.widgets.Select(
        name="Characterization Function",
        options=["GWP", "GTP", "AGWP", "AGTP"],
        value="GWP",
        width=150
    )
    
    # Temporal analysis plot
    temporal_plot = create_temporal_plot()
    
    # Temporal statistics
    temporal_stats = create_temporal_statistics()
    
    return pn.Column(
        pn.pane.Markdown("### Time-Explicit Analysis"),
        pn.Row(time_horizon, char_function),
        pn.Row(
            pn.Column(temporal_plot, width=700),
            pn.Column(temporal_stats, width=400)
        )
    )


def create_temporal_plot():
    """Create temporal analysis plot."""
    try:
        import matplotlib.pyplot as plt
        
        # Sample temporal impact data
        months = np.arange(1, 13)
        cumulative_impact = np.cumsum([20, 35, 25, 45, 30, 20, 15, 25, 30, 35, 20, 18])
        monthly_impact = [20, 35, 25, 45, 30, 20, 15, 25, 30, 35, 20, 18]
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
        
        # Monthly impact
        bars = ax1.bar(months, monthly_impact, color='lightblue', alpha=0.7, edgecolor='blue')
        ax1.set_title('Monthly Impact Distribution')
        ax1.set_ylabel('kg CO2-eq')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Cumulative impact
        ax2.plot(months, cumulative_impact, 'o-', color='red', linewidth=2, markersize=6)
        ax2.fill_between(months, cumulative_impact, alpha=0.3, color='red')
        ax2.set_title('Cumulative Impact over Time')
        ax2.set_xlabel('Month')
        ax2.set_ylabel('Cumulative kg CO2-eq')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return pn.pane.Matplotlib(fig, width=700, height=500)
    except:
        return pn.pane.Markdown("*Temporal plot not available - install matplotlib*")


def create_temporal_statistics():
    """Create temporal statistics summary."""
    
    temporal_data = {
        'Metric': [
            'Peak Month',
            'Peak Impact',
            'Average Monthly',
            'Total Impact',
            'Standard Deviation',
            'Time to 90%'
        ],
        'Value': [
            'Month 4',
            '45.0 kg',
            '26.5 kg',
            '318.0 kg',
            '9.7 kg',
            '10 months'
        ]
    }
    
    temporal_table = pn.widgets.Tabulator(
        temporal_data,
        show_index=False,
        sizing_mode='stretch_width'
    )
    
    return pn.Column(
        pn.pane.Markdown("**Temporal Statistics:**"),
        temporal_table
    )


def create_contribution_analysis():
    """Create contribution analysis interface."""
    
    # Contribution type selector
    contrib_type = pn.widgets.Select(
        name="Analysis Type",
        options=["Process Contribution", "Flow Contribution", "Time Contribution"],
        value="Process Contribution",
        width=200
    )
    
    # Top N selector
    top_n = pn.widgets.IntSlider(
        name="Show Top N",
        start=5,
        end=20,
        value=10,
        step=1,
        width=200
    )
    
    # Contribution plot
    contrib_plot = create_contribution_plot()
    
    # Contribution table
    contrib_table = create_contribution_table()
    
    return pn.Column(
        pn.pane.Markdown("### Contribution Analysis"),
        pn.Row(contrib_type, top_n),
        pn.Row(
            pn.Column(contrib_plot, width=600),
            pn.Column(contrib_table, width=500)
        )
    )


def create_contribution_plot():
    """Create contribution analysis plot."""
    try:
        import matplotlib.pyplot as plt
        
        # Sample contribution data
        processes = ['Energy\nProduction', 'Transport', 'Manufacturing', 'Raw\nMaterials', 'Others']
        contributions = [67.2, 15.8, 12.4, 3.8, 0.8]
        colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff99cc']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        # Pie chart
        wedges, texts, autotexts = ax1.pie(contributions, labels=processes, colors=colors,
                                          autopct='%1.1f%%', startangle=90)
        ax1.set_title('Process Contribution (Pie Chart)')
        
        # Bar chart
        bars = ax2.bar(processes, contributions, color=colors, alpha=0.7)
        ax2.set_title('Process Contribution (Bar Chart)')
        ax2.set_ylabel('Contribution (%)')
        ax2.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax2.annotate(f'{height:.1f}%',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom')
        
        plt.tight_layout()
        return pn.pane.Matplotlib(fig, width=600, height=400)
    except:
        return pn.pane.Markdown("*Contribution plot not available - install matplotlib*")


def create_contribution_table():
    """Create contribution analysis table."""
    
    contrib_data = {
        'Process': [
            'Energy Production',
            'Transport',
            'Manufacturing',
            'Raw Materials',
            'Waste Treatment',
            'Packaging',
            'Others'
        ],
        'Contribution (%)': [
            67.2,
            15.8,
            12.4,
            3.8,
            0.5,
            0.2,
            0.1
        ],
        'Impact (kg CO2-eq)': [
            179.6,
            42.2,
            33.1,
            10.2,
            1.3,
            0.5,
            0.4
        ]
    }
    
    contrib_table = pn.widgets.Tabulator(
        contrib_data,
        show_index=False,
        sizing_mode='stretch_width'
    )
    
    return pn.Column(
        pn.pane.Markdown("**Contribution Breakdown:**"),
        contrib_table
    )


def create_export_actions():
    """Create export and action options for LCIA results."""
    
    # Export format
    export_format = pn.widgets.Select(
        name="Export Format",
        options=["PDF Report", "Excel", "CSV", "JSON", "PNG Images"],
        value="PDF Report",
        width=150
    )
    
    # Report options
    report_options = pn.widgets.CheckBoxGroup(
        name="Include in Report",
        options=["Summary tables", "Plots", "Raw data", "Metadata", "Uncertainty"],
        value=["Summary tables", "Plots"],
        inline=True
    )
    
    # Action buttons
    export_btn = pn.widgets.Button(
        name="Export Results",
        button_type="success",
        width=150
    )
    
    compare_btn = pn.widgets.Button(
        name="Compare Scenarios",
        button_type="primary",
        width=150
    )
    
    uncertainty_btn = pn.widgets.Button(
        name="Run Uncertainty",
        button_type="light",
        width=150
    )
    
    return pn.Column(
        pn.pane.Markdown("### Export & Actions"),
        pn.Row(export_format),
        report_options,
        pn.Row(export_btn, compare_btn, uncertainty_btn),
        pn.pane.Markdown("Export your LCIA results or perform additional analysis.")
    )