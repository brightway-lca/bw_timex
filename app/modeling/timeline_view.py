"""
Timeline view for bw_timex web application.

This module provides the interface for configuring and visualizing timelines.
"""

import panel as pn
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def view():
    """Create the timeline configuration and visualization view."""
    
    # Title
    title = pn.pane.Markdown("# Timeline Configuration")
    
    # Instructions
    instructions = pn.pane.Markdown("""
    Configure temporal distributions and visualize the process timeline.
    
    ## Features:
    - Add temporal distributions to exchanges
    - Visualize timeline structure
    - Build process timeline
    - Export timeline data
    """)
    
    # Timeline configuration
    timeline_config = create_timeline_config()
    
    # Temporal distributions
    temporal_dist_config = create_temporal_distributions_config()
    
    # Timeline visualization
    timeline_viz = create_timeline_visualization()
    
    # Timeline actions
    timeline_actions = create_timeline_actions()
    
    return pn.Column(
        title,
        instructions,
        pn.Spacer(height=20),
        timeline_config,
        pn.Spacer(height=20),
        temporal_dist_config,
        pn.Spacer(height=20),
        timeline_viz,
        pn.Spacer(height=20),
        timeline_actions,
        sizing_mode='stretch_width'
    )


def create_timeline_config():
    """Create timeline configuration interface."""
    
    # Timeline parameters
    start_date = pn.widgets.DatePicker(
        name="Start Date",
        value=datetime.now(),
        width=200
    )
    
    end_date = pn.widgets.DatePicker(
        name="End Date", 
        value=datetime.now() + timedelta(days=365),
        width=200
    )
    
    time_resolution = pn.widgets.Select(
        name="Time Resolution",
        options=["year", "month", "week", "day", "hour"],
        value="month",
        width=150
    )
    
    return pn.Column(
        pn.pane.Markdown("### Timeline Parameters"),
        pn.Row(start_date, end_date, time_resolution),
        pn.pane.Markdown("Set the temporal scope and resolution for your analysis.")
    )


def create_temporal_distributions_config():
    """Create temporal distributions configuration interface."""
    
    # Exchange selection
    exchange_selector = pn.widgets.Select(
        name="Exchange",
        options=["A -> B", "B -> C", "C -> D"],
        width=200
    )
    
    # Distribution type
    dist_type = pn.widgets.Select(
        name="Distribution Type",
        options=["uniform", "normal", "triangular", "custom"],
        value="uniform",
        width=150
    )
    
    # Distribution parameters (simplified)
    param1 = pn.widgets.FloatInput(name="Parameter 1", value=0.0, width=100)
    param2 = pn.widgets.FloatInput(name="Parameter 2", value=1.0, width=100)
    
    # Preview plot area
    preview_plot = create_distribution_preview()
    
    # Actions
    add_dist_btn = pn.widgets.Button(name="Add Distribution", button_type="primary", width=150)
    remove_dist_btn = pn.widgets.Button(name="Remove", button_type="light", width=100)
    
    # Current distributions table
    distributions_data = {
        'Exchange': ['A -> B', 'B -> C'],
        'Type': ['uniform', 'normal'],
        'Parameters': ['[0, 12]', 'μ=6, σ=2'],
        'Description': ['Uniform over 1 year', 'Normal around 6 months']
    }
    
    distributions_table = pn.widgets.Tabulator(
        distributions_data,
        pagination='remote',
        page_size=5,
        sizing_mode='stretch_width'
    )
    
    return pn.Column(
        pn.pane.Markdown("### Temporal Distributions"),
        pn.Row(exchange_selector, dist_type),
        pn.Row(param1, param2, add_dist_btn, remove_dist_btn),
        preview_plot,
        pn.pane.Markdown("**Current Distributions:**"),
        distributions_table
    )


def create_distribution_preview():
    """Create a preview plot for temporal distributions."""
    
    # Generate sample data
    x = np.linspace(0, 12, 100)
    y = np.exp(-(x - 6)**2 / 8)  # Sample normal-like distribution
    
    # Create a simple plot
    try:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(x, y)
        ax.set_xlabel('Time (months)')
        ax.set_ylabel('Probability Density')
        ax.set_title('Temporal Distribution Preview')
        ax.grid(True, alpha=0.3)
        
        return pn.pane.Matplotlib(fig, width=600, height=300)
    except:
        return pn.pane.Markdown("*Plot preview not available - install matplotlib*")


def create_timeline_visualization():
    """Create timeline visualization interface."""
    
    # Timeline plot placeholder
    timeline_plot = create_timeline_plot()
    
    # Timeline summary
    timeline_summary = create_timeline_summary()
    
    return pn.Column(
        pn.pane.Markdown("### Timeline Visualization"),
        pn.Row(
            pn.Column(timeline_plot, width=600),
            pn.Column(timeline_summary, width=400)
        )
    )


def create_timeline_plot():
    """Create a timeline plot."""
    try:
        import matplotlib.pyplot as plt
        
        # Sample timeline data
        processes = ['Process A', 'Process B', 'Process C', 'Process D']
        start_times = [0, 2, 4, 6]
        durations = [3, 2, 4, 2]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Create Gantt-style chart
        for i, (process, start, duration) in enumerate(zip(processes, start_times, durations)):
            ax.barh(i, duration, left=start, height=0.6, alpha=0.8, 
                   label=process, color=f'C{i}')
            ax.text(start + duration/2, i, f'{duration}m', 
                   ha='center', va='center', fontweight='bold')
        
        ax.set_yticks(range(len(processes)))
        ax.set_yticklabels(processes)
        ax.set_xlabel('Time (months)')
        ax.set_title('Process Timeline')
        ax.grid(True, alpha=0.3, axis='x')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        return pn.pane.Matplotlib(fig, width=600, height=400)
    except:
        return pn.pane.Markdown("*Timeline plot not available - install matplotlib*")


def create_timeline_summary():
    """Create timeline summary statistics."""
    
    summary_data = {
        'Metric': [
            'Total Processes',
            'Timeline Duration',
            'Temporal Resolution',
            'Total Exchanges',
            'Temporalized Exchanges'
        ],
        'Value': [
            '4',
            '12 months',
            'monthly',
            '8',
            '3'
        ]
    }
    
    summary_table = pn.widgets.Tabulator(
        summary_data,
        show_index=False,
        sizing_mode='stretch_width'
    )
    
    return pn.Column(
        pn.pane.Markdown("**Timeline Summary:**"),
        summary_table
    )


def create_timeline_actions():
    """Create timeline action buttons."""
    
    build_timeline_btn = pn.widgets.Button(
        name="Build Timeline",
        button_type="success",
        width=150
    )
    
    export_timeline_btn = pn.widgets.Button(
        name="Export Timeline",
        button_type="primary",
        width=150
    )
    
    validate_timeline_btn = pn.widgets.Button(
        name="Validate Timeline",
        button_type="light",
        width=150
    )
    
    reset_timeline_btn = pn.widgets.Button(
        name="Reset",
        button_type="light",
        width=100
    )
    
    status = pn.pane.Markdown("**Status:** Timeline configuration ready")
    
    return pn.Column(
        pn.Row(
            build_timeline_btn,
            validate_timeline_btn,
            export_timeline_btn,
            reset_timeline_btn
        ),
        status
    )