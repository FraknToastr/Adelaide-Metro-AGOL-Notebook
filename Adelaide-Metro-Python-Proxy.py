# Save this file as proxy_server.py
import http.server
import socketserver
import requests
from urllib.parse import urlparse

# --- Configuration Constants ---
PORT = 5000
ADELAIDE_GTFS_URL = "https://gtfs.adelaidemetro.com.au/v1/realtime/vehicle_positions"

class MyHandler(http.server.SimpleHTTPRequestHandler):
    """
    A simple proxy handler that fetches data from the Adelaide Metro GTFS-RT API
    and serves it to the browser.
    """
    def do_GET(self):
        # The path from the browser is what comes after the domain.
        # e.g., for http://localhost:5000/adelaide-gtfs, the path is /adelaide-gtfs
        if self.path == "/adelaide-gtfs":
            print(f"Request received for {self.path}. Fetching data...")
            try:
                # Fetch the data from the original API
                # The timeout prevents the server from hanging indefinitely
                response = requests.get(ADELAIDE_GTFS_URL, timeout=10)
                
                # If the API call fails, raise an error
                response.raise_for_status() 

                # Set the response headers
                self.send_response(200)
                # The mimetype for GTFS-RT protobuf data is application/x-protobuf
                self.send_header('Content-type', 'application/x-protobuf')
                # This header is crucial for the browser to allow the connection
                self.send_header('Access-Control-Allow-Origin', '*') 
                self.end_headers()

                # Send the content back to the client
                self.wfile.write(response.content)
                print("Data fetched and served successfully.")
            
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                self.send_error(500, f"Error: {e}")
        else:
            # Handle other requests (e.g., requests for favicon)
            self.send_error(404, "File Not Found")

if __name__ == "__main__":
    try:
        # Create the server
        with socketserver.TCPServer(("", PORT), MyHandler) as httpd:
            print(f"Starting proxy server on port {PORT}...")
            print("To use this, open your web browser and navigate to the HTML file.")
            print("The server will run until you press Ctrl+C.")
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down the server.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
