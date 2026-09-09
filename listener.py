from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        print("\n--- INCOMING WEBHOOK RECEIVED ---")
        print(json.loads(body.decode('utf-8')))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status": "received"}')

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8000), WebhookHandler)
    print("Listening for webhooks on http://localhost:8000 ...")
    server.serve_forever()