#!/usr/bin/env python3
"""
Simple HTTP server to serve the static demo of bw_timex web interface.
"""

import http.server
import socketserver
import os
import webbrowser
from pathlib import Path

def serve_demo(port=8000, host='0.0.0.0'):
    """Serve the static demo HTML file."""
    
    # Change to the static directory
    static_dir = Path(__file__).parent / "static"
    os.chdir(static_dir)
    
    # Create server
    handler = http.server.SimpleHTTPRequestHandler
    
    class CustomHandler(handler):
        def end_headers(self):
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            super().end_headers()
    
    with socketserver.TCPServer((host, port), CustomHandler) as httpd:
        print(f"Serving bw_timex demo at http://{host}:{port}/demo.html")
        print(f"Press Ctrl+C to stop the server")
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")

if __name__ == "__main__":
    serve_demo()