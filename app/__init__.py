"""
bw_timex Panel Web Application

A web interface for time-explicit Life Cycle Assessment using the bw_timex library.
Provides modular views for modeling setup and results analysis with URL-based routing.
"""

from .app import create_app

__version__ = "1.0.0"
__all__ = ["create_app"]