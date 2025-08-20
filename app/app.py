"""
Main Panel application for bw_timex with URL-based routing.

This application provides a web interface for time-explicit Life Cycle Assessment
using the bw_timex library with proper URL routing and modular structure.
"""

import panel as pn
from typing import Dict, Callable
import os
from pathlib import Path

# Import view modules
try:
    from .modeling import calculation_setup, timeline_view
    from .results import lci_results, lcia_results
except ImportError:
    # Handle relative imports when running directly
    import sys
    sys.path.append(str(Path(__file__).parent))
    from modeling import calculation_setup, timeline_view
    from results import lci_results, lcia_results

# Enable Panel extensions - simplified
pn.extension('tabulator')

# Load CSS
css_path = Path(__file__).parent / "static" / "styles.css"
if css_path.exists():
    with open(css_path, 'r') as f:
        css_content = f.read()
    pn.config.raw_css = [css_content]

# Configure Panel
pn.config.sizing_mode = 'stretch_width'


def create_divider():
    """Create a simple divider."""
    return pn.pane.HTML('<hr style="margin: 20px 0; border: 1px solid #ddd;">')


class TimexApp:
    """Main application class with URL routing."""
    
    def __init__(self):
        self.routes = self._setup_routes()
        self.current_view = None
        self.current_path = '/'  # Track current path
        
    def _setup_routes(self) -> Dict[str, Callable]:
        """Setup URL routes mapping to view functions."""
        return {
            '/': self.home_view,
            '/modeling/calculation-setup': calculation_setup.view,
            '/modeling/timeline': timeline_view.view,
            '/results/lci': lci_results.view,
            '/results/lcia': lcia_results.view,
        }
    
    def home_view(self):
        """Home page view."""
        return pn.Column(
            pn.pane.Markdown("# Welcome to bw_timex Web Interface"),
            pn.pane.Markdown("""
            This is the web interface for **bw_timex** - Time-explicit Life Cycle Assessment.
            
            ## Features
            - **Modeling**: Set up calculations and configure timelines
            - **Results**: View LCI and LCIA results
            
            ## Navigation
            Use the menu above to navigate between different sections:
            - **Modeling**: Define your time-explicit LCA setup
            - **Results**: Analyze your calculation results
            
            ## Getting Started
            1. Go to [Calculation Setup](/modeling/calculation-setup) to configure your analysis
            2. Set up your [Timeline](/modeling/timeline) 
            3. View your [Results](/results/lci) once calculations are complete
            """),
            sizing_mode='stretch_width'
        )
    
    def create_navbar(self):
        """Create navigation bar with menu items."""
        # Simple navigation buttons that work with location routing
        nav_buttons = []
        
        # Home button
        home_btn = pn.widgets.Button(
            name="🏠 Home",
            button_type='primary',
            width=120,
            margin=(5, 5)
        )
        home_btn.on_click(lambda event: self.navigate_to('/'))
        nav_buttons.append(home_btn)
        
        # Modeling section
        calc_setup_btn = pn.widgets.Button(
            name="📊 Calculation Setup", 
            button_type='light',
            width=160,
            margin=(5, 5)
        )
        calc_setup_btn.on_click(lambda event: self.navigate_to('/modeling/calculation-setup'))
        nav_buttons.append(calc_setup_btn)
        
        timeline_btn = pn.widgets.Button(
            name="⏱️ Timeline",
            button_type='light', 
            width=120,
            margin=(5, 5)
        )
        timeline_btn.on_click(lambda event: self.navigate_to('/modeling/timeline'))
        nav_buttons.append(timeline_btn)
        
        # Results section
        lci_btn = pn.widgets.Button(
            name="📈 LCI Results",
            button_type='light',
            width=130,
            margin=(5, 5)
        )
        lci_btn.on_click(lambda event: self.navigate_to('/results/lci'))
        nav_buttons.append(lci_btn)
        
        lcia_btn = pn.widgets.Button(
            name="🎯 LCIA Results",
            button_type='light',
            width=140,
            margin=(5, 5)
        )
        lcia_btn.on_click(lambda event: self.navigate_to('/results/lcia'))
        nav_buttons.append(lcia_btn)
        
        # Create breadcrumb indicator
        breadcrumb = pn.pane.HTML(
            """<div style="margin: 5px 10px; color: #666; font-style: italic;">
            Current page will be highlighted based on URL
            </div>""",
            width=300
        )
        
        navbar = pn.Row(
            *nav_buttons,
            pn.Spacer(),
            breadcrumb,
            margin=(10, 10),
            styles={'background': '#f8f9fa'}
        )
        
        return navbar
    
    def navigate_to(self, path: str):
        """Navigate to a specific path and update the URL."""
        # Update the browser URL using Panel's location if available
        try:
            if hasattr(pn.state, 'location') and pn.state.location is not None:
                pn.state.location.pathname = path
        except:
            pass
        
        # Update content directly since location might not work in all cases
        self.current_path = path
        self.update_content_direct(path)
    
    def update_content_direct(self, path: str):
        """Update content directly for a given path."""
        try:
            view_func = self.routes.get(path, self.home_view)
            new_content = view_func()
            if hasattr(self, 'main_content') and self.main_content is not None:
                self.main_content.clear()
                self.main_content.append(new_content)
        except Exception as e:
            error_view = pn.pane.Markdown(f"# Error\n\nFailed to load view: {str(e)}")
            if hasattr(self, 'main_content') and self.main_content is not None:
                self.main_content.clear()
                self.main_content.append(error_view)
    
    def update_content(self):
        """Update the main content based on current URL."""
        try:
            # Default to home if no location available
            current_path = '/'
            try:
                if hasattr(pn.state, 'location') and pn.state.location is not None:
                    current_path = pn.state.location.pathname or '/'
            except:
                pass
            
            # Find matching route
            view_func = self.routes.get(current_path)
            if view_func is None:
                view_func = self.home_view
            
            # Update the content
            new_content = view_func()
            if hasattr(self, 'main_content') and self.main_content is not None:
                self.main_content.clear()
                self.main_content.append(new_content)
                
        except Exception as e:
            error_view = pn.pane.Markdown(f"""
            # Error
            
            Failed to load view for path: `{current_path if 'current_path' in locals() else 'unknown'}`
            
            Error: {str(e)}
            
            Please check the console for more details.
            """)
            if hasattr(self, 'main_content') and self.main_content is not None:
                self.main_content.clear()
                self.main_content.append(error_view)
    
    def create_app(self):
        """Create the main application layout."""
        # Create main content area
        self.main_content = pn.Column(sizing_mode='stretch_width')
        
        # Create navbar
        navbar = self.create_navbar()
        
        # Create header
        header = pn.pane.HTML("""
        <div style="background: linear-gradient(90deg, #3498db, #2c3e50); color: white; padding: 20px; text-align: center;">
            <h1 style="margin: 0; color: white; border: none;">bw_timex Web Interface</h1>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">Time-explicit Life Cycle Assessment</p>
        </div>
        """)
        
        # Create main layout
        layout = pn.Column(
            header,
            navbar,
            pn.Spacer(height=10),
            self.main_content,
            sizing_mode='stretch_width',
            min_height=600
        )
        
        # Initialize with home content
        self.update_content_direct('/')
        
        return layout


def create_app():
    """Factory function to create the Panel app."""
    app = TimexApp()
    return app.create_app()


# Create a simple servable app for testing
def simple_app():
    """Create a simple test version of the app."""
    # Test layout
    header = pn.pane.HTML("""
    <div style="background: linear-gradient(90deg, #3498db, #2c3e50); color: white; padding: 20px; text-align: center;">
        <h1 style="margin: 0; color: white; border: none;">bw_timex Web Interface</h1>
        <p style="margin: 5px 0 0 0; opacity: 0.9;">Time-explicit Life Cycle Assessment</p>
    </div>
    """)
    
    # Navigation buttons
    home_btn = pn.widgets.Button(name="🏠 Home", button_type='primary', width=120)
    calc_btn = pn.widgets.Button(name="📊 Calculation Setup", button_type='light', width=160)
    timeline_btn = pn.widgets.Button(name="⏱️ Timeline", button_type='light', width=120)
    lci_btn = pn.widgets.Button(name="📈 LCI Results", button_type='light', width=130)
    lcia_btn = pn.widgets.Button(name="🎯 LCIA Results", button_type='light', width=140)
    
    navbar = pn.Row(home_btn, calc_btn, timeline_btn, lci_btn, lcia_btn, margin=(10, 10))
    
    # Content area
    content = pn.pane.Markdown("""
    # Welcome to bw_timex Web Interface
    
    This is the web interface for **bw_timex** - Time-explicit Life Cycle Assessment.
    
    ## Features
    - **Modeling**: Set up calculations and configure timelines
    - **Results**: View LCI and LCIA results
    
    ## Navigation
    Use the menu above to navigate between different sections:
    - **Modeling**: Define your time-explicit LCA setup
    - **Results**: Analyze your calculation results
    
    ## Getting Started
    1. Click on "Calculation Setup" to configure your analysis
    2. Set up your "Timeline" 
    3. View your "Results" once calculations are complete
    
    **Note**: This is a demo interface. Full functionality will be available once the backend is connected.
    """)
    
    # Current view state
    current_view = pn.Column(content, sizing_mode='stretch_width')
    
    # Button callbacks
    def show_home(event):
        current_view.clear()
        current_view.append(content)
    
    def show_calc_setup(event):
        current_view.clear()
        current_view.append(calculation_setup.view())
    
    def show_timeline(event):
        current_view.clear()
        current_view.append(timeline_view.view())
    
    def show_lci(event):
        current_view.clear()
        current_view.append(lci_results.view())
    
    def show_lcia(event):
        current_view.clear()
        current_view.append(lcia_results.view())
    
    # Connect callbacks
    home_btn.on_click(show_home)
    calc_btn.on_click(show_calc_setup)
    timeline_btn.on_click(show_timeline)
    lci_btn.on_click(show_lci)
    lcia_btn.on_click(show_lcia)
    
    return pn.Column(
        header,
        navbar,
        pn.Spacer(height=10),
        current_view,
        sizing_mode='stretch_width'
    )


# Serve the application
if __name__ == "__main__":
    # Use the simple app for now
    app = simple_app()
    app.servable()
    
    # Alternative: run with pn.serve for development
    # pn.serve(app, port=5007, show=True, autoreload=True)