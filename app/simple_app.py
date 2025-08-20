"""
Simplified Panel application for bw_timex.

This is a more basic version that should work reliably without complex routing.
"""

import panel as pn

# Simple Panel configuration
pn.extension()

def create_home_content():
    """Create home page content."""
    return pn.pane.Markdown("""
    # Welcome to bw_timex Web Interface
    
    This is the web interface for **bw_timex** - Time-explicit Life Cycle Assessment.
    
    ## Features
    - **Modeling**: Set up calculations and configure timelines
    - **Results**: View LCI and LCIA results
    
    ## Navigation
    Use the buttons above to navigate between different sections:
    - **Calculation Setup**: Define your time-explicit LCA setup
    - **Timeline**: Configure temporal distributions
    - **LCI Results**: View inventory results
    - **LCIA Results**: View impact assessment results
    
    ## Getting Started
    1. Click on "Calculation Setup" to configure your analysis
    2. Set up your "Timeline" 
    3. View your "Results" once calculations are complete
    
    **Status**: 🟢 Application is running successfully!
    """)

def create_calc_setup_content():
    """Create calculation setup content."""
    return pn.Column(
        pn.pane.Markdown("# Calculation Setup"),
        pn.pane.Markdown("Configure your time-explicit LCA calculation parameters."),
        pn.widgets.TextInput(name="Project Name", placeholder="Enter project name"),
        pn.widgets.Select(name="Database", options=["foreground", "background_2020", "background_2030"]),
        pn.widgets.FloatInput(name="Amount", value=1.0),
        pn.widgets.Button(name="Save Configuration", button_type="primary"),
        pn.pane.Markdown("**Status**: Ready for configuration")
    )

def create_timeline_content():
    """Create timeline content."""
    return pn.Column(
        pn.pane.Markdown("# Timeline Configuration"),
        pn.pane.Markdown("Configure temporal distributions for your analysis."),
        pn.widgets.DatePicker(name="Start Date"),
        pn.widgets.DatePicker(name="End Date"),
        pn.widgets.Select(name="Resolution", options=["year", "month", "day"]),
        pn.widgets.Button(name="Build Timeline", button_type="success"),
        pn.pane.Markdown("**Status**: Timeline configuration ready")
    )

def create_lci_content():
    """Create LCI results content."""
    return pn.Column(
        pn.pane.Markdown("# LCI Results"),
        pn.pane.Markdown("Life Cycle Inventory results from your calculations."),
        pn.pane.Markdown("""
        **Matrix Dimensions**: 1250 × 850  
        **Non-zero Elements**: 15,420  
        **Time Periods**: 12  
        **Status**: ✅ Calculation Complete
        """),
        pn.widgets.Button(name="Export Results", button_type="primary")
    )

def create_lcia_content():
    """Create LCIA results content."""
    return pn.Column(
        pn.pane.Markdown("# LCIA Results"),
        pn.pane.Markdown("Life Cycle Impact Assessment results."),
        pn.pane.Markdown("""
        **Static LCIA Score**: 245.7 kg CO2-eq  
        **Dynamic LCIA Score**: 267.3 kg CO2-eq  
        **Difference**: +8.8%  
        **Status**: ✅ Assessment Complete
        """),
        pn.widgets.Button(name="Export Report", button_type="success")
    )

def create_simple_app():
    """Create the main application."""
    
    # Header
    header = pn.pane.HTML("""
    <div style="background: linear-gradient(90deg, #3498db, #2c3e50); color: white; padding: 20px; text-align: center; margin-bottom: 20px;">
        <h1 style="margin: 0; color: white; border: none; font-size: 28px;">bw_timex Web Interface</h1>
        <p style="margin: 5px 0 0 0; opacity: 0.9; font-size: 16px;">Time-explicit Life Cycle Assessment</p>
    </div>
    """)
    
    # Current content display
    content = pn.Column(create_home_content(), sizing_mode='stretch_width')
    
    # Navigation buttons
    home_btn = pn.widgets.Button(name="🏠 Home", button_type='primary', width=140, margin=(5, 5))
    calc_btn = pn.widgets.Button(name="📊 Calculation Setup", button_type='light', width=180, margin=(5, 5))
    timeline_btn = pn.widgets.Button(name="⏱️ Timeline", button_type='light', width=140, margin=(5, 5))
    lci_btn = pn.widgets.Button(name="📈 LCI Results", button_type='light', width=150, margin=(5, 5))
    lcia_btn = pn.widgets.Button(name="🎯 LCIA Results", button_type='light', width=160, margin=(5, 5))
    
    # Button callbacks
    def show_home(event):
        content.clear()
        content.append(create_home_content())
        # Reset button styles
        home_btn.button_type = 'primary'
        calc_btn.button_type = 'light'
        timeline_btn.button_type = 'light'
        lci_btn.button_type = 'light'
        lcia_btn.button_type = 'light'
    
    def show_calc_setup(event):
        content.clear()
        content.append(create_calc_setup_content())
        # Update button styles
        home_btn.button_type = 'light'
        calc_btn.button_type = 'primary'
        timeline_btn.button_type = 'light'
        lci_btn.button_type = 'light'
        lcia_btn.button_type = 'light'
    
    def show_timeline(event):
        content.clear()
        content.append(create_timeline_content())
        # Update button styles
        home_btn.button_type = 'light'
        calc_btn.button_type = 'light'
        timeline_btn.button_type = 'primary'
        lci_btn.button_type = 'light'
        lcia_btn.button_type = 'light'
    
    def show_lci(event):
        content.clear()
        content.append(create_lci_content())
        # Update button styles
        home_btn.button_type = 'light'
        calc_btn.button_type = 'light'
        timeline_btn.button_type = 'light'
        lci_btn.button_type = 'primary'
        lcia_btn.button_type = 'light'
    
    def show_lcia(event):
        content.clear()
        content.append(create_lcia_content())
        # Update button styles
        home_btn.button_type = 'light'
        calc_btn.button_type = 'light'
        timeline_btn.button_type = 'light'
        lci_btn.button_type = 'light'
        lcia_btn.button_type = 'primary'
    
    # Connect button callbacks
    home_btn.on_click(show_home)
    calc_btn.on_click(show_calc_setup)
    timeline_btn.on_click(show_timeline)
    lci_btn.on_click(show_lci)
    lcia_btn.on_click(show_lcia)
    
    # Navigation bar
    navbar = pn.Row(
        home_btn, calc_btn, timeline_btn, lci_btn, lcia_btn,
        margin=(10, 20),
        styles={'background': '#f8f9fa', 'padding': '10px', 'border-radius': '5px'}
    )
    
    # Main layout
    layout = pn.Column(
        header,
        navbar,
        content,
        sizing_mode='stretch_width',
        margin=(0, 20)
    )
    
    return layout

# Create the app
app = create_simple_app()

# Serve the app
if __name__ == "__main__":
    app.servable()
else:
    # For importing
    def get_app():
        return app