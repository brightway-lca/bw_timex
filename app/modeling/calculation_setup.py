"""
Calculation Setup view for bw_timex web application.

This module provides the interface for setting up TimexLCA calculations.
"""

import panel as pn
from datetime import datetime
from typing import Dict, Optional

try:
    import bw2data as bd
    BD_AVAILABLE = True
except ImportError:
    BD_AVAILABLE = False
    bd = None

try:
    from bw_timex import TimexLCA
except ImportError:
    # Fallback for development
    TimexLCA = None


def view():
    """Create the calculation setup view."""
    
    # Title
    title = pn.pane.Markdown("# Calculation Setup")
    
    # Instructions
    instructions = pn.pane.Markdown("""
    Configure your time-explicit LCA calculation parameters below.
    
    ## Steps:
    1. Select or configure your project
    2. Define functional unit and demand
    3. Choose impact assessment method
    4. Set database dates mapping
    5. Configure calculation parameters
    """)
    
    # Project selection
    project_selector = create_project_selector()
    
    # Functional unit configuration
    functional_unit_config = create_functional_unit_config()
    
    # Method selection
    method_config = create_method_config()
    
    # Database dates configuration
    database_dates_config = create_database_dates_config()
    
    # Calculation parameters
    calc_params_config = create_calculation_parameters_config()
    
    # Action buttons
    action_buttons = create_action_buttons()
    
    # Status display
    status_display = pn.pane.Markdown("**Status:** Ready to configure calculation")
    
    return pn.Column(
        title,
        instructions,
        pn.Spacer(height=20),
        project_selector,
        pn.Spacer(height=20),
        functional_unit_config,
        pn.Spacer(height=20),
        method_config,
        pn.Spacer(height=20),
        database_dates_config,
        pn.Spacer(height=20),
        calc_params_config,
        pn.Spacer(height=20),
        action_buttons,
        status_display,
        sizing_mode='stretch_width'
    )


def create_project_selector():
    """Create project selection interface."""
    
    # Get available projects (mock for now)
    try:
        projects = list(bd.projects) if BD_AVAILABLE and bd else ["demo_project", "example_project"]
    except:
        projects = ["demo_project", "example_project"]
    
    project_select = pn.widgets.Select(
        name="Brightway Project",
        value=projects[0] if projects else None,
        options=projects,
        width=300
    )
    
    refresh_btn = pn.widgets.Button(name="Refresh Projects", button_type="primary", width=150)
    
    return pn.Column(
        pn.pane.Markdown("### Project Selection"),
        pn.Row(project_select, refresh_btn),
        pn.pane.Markdown("Select the Brightway2 project containing your LCA data.")
    )


def create_functional_unit_config():
    """Create functional unit configuration interface."""
    
    # Database selection
    database_select = pn.widgets.Select(
        name="Database",
        options=["foreground", "background_2020", "background_2030"],
        width=200
    )
    
    # Activity selection (simplified)
    activity_input = pn.widgets.TextInput(
        name="Activity Code/Name",
        placeholder="Enter activity identifier",
        width=300
    )
    
    # Amount input
    amount_input = pn.widgets.FloatInput(
        name="Amount",
        value=1.0,
        width=100
    )
    
    # Unit selection
    unit_select = pn.widgets.Select(
        name="Unit",
        options=["kg", "m3", "kWh", "MJ", "piece"],
        value="kg",
        width=100
    )
    
    return pn.Column(
        pn.pane.Markdown("### Functional Unit"),
        pn.Row(database_select, activity_input),
        pn.Row(amount_input, unit_select),
        pn.pane.Markdown("Define the functional unit for your analysis.")
    )


def create_method_config():
    """Create impact assessment method configuration."""
    
    # Method category
    method_category = pn.widgets.Select(
        name="Method Category",
        options=["IPCC 2013", "CML 2001", "ReCiPe 2016", "Custom"],
        width=200
    )
    
    # Method name
    method_name = pn.widgets.Select(
        name="Method",
        options=["climate change", "acidification", "eutrophication"],
        width=300
    )
    
    return pn.Column(
        pn.pane.Markdown("### Impact Assessment Method"),
        pn.Row(method_category, method_name),
        pn.pane.Markdown("Choose the impact assessment method for characterization.")
    )


def create_database_dates_config():
    """Create database dates mapping configuration."""
    
    # Database dates table (simplified)
    database_dates_data = {
        'Database': ['background_2020', 'background_2030', 'foreground'],
        'Date/Type': ['2020', '2030', 'dynamic'],
        'Description': [
            'Background data for year 2020',
            'Background data for year 2030', 
            'Dynamic foreground system'
        ]
    }
    
    database_table = pn.widgets.Tabulator(
        database_dates_data,
        pagination='remote',
        page_size=10,
        sizing_mode='stretch_width'
    )
    
    # Add database button
    add_db_btn = pn.widgets.Button(name="Add Database", button_type="primary", width=150)
    
    return pn.Column(
        pn.pane.Markdown("### Database Dates Mapping"),
        database_table,
        add_db_btn,
        pn.pane.Markdown("Map databases to their temporal representation.")
    )


def create_calculation_parameters_config():
    """Create calculation parameters configuration."""
    
    # Temporal grouping
    temporal_grouping = pn.widgets.Select(
        name="Temporal Grouping",
        options=["year", "month", "day", "hour"],
        value="year",
        width=150
    )
    
    # Edge filter function
    edge_filter = pn.widgets.TextInput(
        name="Edge Filter Function",
        placeholder="Optional Python function",
        width=300
    )
    
    # Maximum calculation count
    max_calc_count = pn.widgets.IntInput(
        name="Max Calculation Count",
        value=10000,
        width=150
    )
    
    # Cutoff threshold
    cutoff_threshold = pn.widgets.FloatInput(
        name="Cutoff Threshold",
        value=0.001,
        step=0.001,
        width=150
    )
    
    return pn.Column(
        pn.pane.Markdown("### Calculation Parameters"),
        pn.Row(temporal_grouping, max_calc_count),
        pn.Row(cutoff_threshold),
        edge_filter,
        pn.pane.Markdown("Configure advanced calculation parameters.")
    )


def create_action_buttons():
    """Create action buttons for the calculation setup."""
    
    save_config_btn = pn.widgets.Button(
        name="Save Configuration", 
        button_type="primary", 
        width=150
    )
    
    load_config_btn = pn.widgets.Button(
        name="Load Configuration", 
        button_type="light", 
        width=150
    )
    
    validate_btn = pn.widgets.Button(
        name="Validate Setup", 
        button_type="success", 
        width=150
    )
    
    create_tlca_btn = pn.widgets.Button(
        name="Create TimexLCA", 
        button_type="success", 
        width=150
    )
    
    return pn.Row(
        save_config_btn, 
        load_config_btn, 
        validate_btn, 
        create_tlca_btn,
        margin=(10, 5)
    )