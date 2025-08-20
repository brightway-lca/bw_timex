#!/usr/bin/env python3
"""
Run script for the bw_timex Panel web application.

Usage:
    python run_app.py [--port PORT] [--host HOST] [--dev]
"""

import argparse
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import the app
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import panel as pn
except ImportError:
    print("Error: Panel is not installed. Please install it with:")
    print("pip install panel>=1.0.0")
    sys.exit(1)

from app.simple_app import get_app


def main():
    parser = argparse.ArgumentParser(description='Run bw_timex Panel application')
    parser.add_argument('--port', type=int, default=5007, help='Port to serve on (default: 5007)')
    parser.add_argument('--host', default='localhost', help='Host to serve on (default: localhost)')
    parser.add_argument('--dev', action='store_true', help='Run in development mode with autoreload')
    parser.add_argument('--show', action='store_true', help='Open browser automatically')
    
    args = parser.parse_args()
    
    # Create the app
    app = get_app()
    
    print(f"Starting bw_timex Panel application...")
    print(f"Server will run on http://{args.host}:{args.port}")
    print(f"Development mode: {'ON' if args.dev else 'OFF'}")
    
    try:
        # Serve the application
        pn.serve(
            app,
            port=args.port,
            host=args.host,
            show=args.show,
            autoreload=args.dev,
            title="bw_timex - Time-explicit LCA",
            admin=True,  # Enable admin panel for debugging
            allow_websocket_origin=[f"{args.host}:{args.port}"]
        )
    except KeyboardInterrupt:
        print("\nShutting down the application...")
    except Exception as e:
        print(f"Error starting application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()