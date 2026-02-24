#!/usr/bin/env python3
"""
AME Market Maker Dashboard Server

Serves static files from the project directory and handles dashboard
command dispatch via POST /api/command.

Usage:
    python3 serve.py [--port 3001]
    ./serve.py [--port 3001]
"""

import argparse
import json
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

VALID_COMMANDS = {"sell_all", "stop", "start", "restart", "cancel_all"}
STATE_DIR = "state"
COMMAND_FILE = os.path.join(STATE_DIR, "mm_command.json")


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress /state/* polling to keep output clean
        path = self.path.split("?")[0]
        if path.startswith("/state/"):
            return
        super().log_message(format, *args)

    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/status":
            self._handle_api_status()
        else:
            self._serve_file(path)

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/command":
            self._handle_command()
        else:
            self._send_error(404, "Not found")

    def _handle_api_status(self):
        try:
            result = {}

            # Pending command (if the bot hasn't consumed it yet)
            if os.path.exists(COMMAND_FILE):
                with open(COMMAND_FILE) as f:
                    result["pending_command"] = json.load(f)
            else:
                result["pending_command"] = None

            # Current bot status
            status_file = os.path.join(STATE_DIR, "mm_status.json")
            if os.path.exists(status_file):
                with open(status_file) as f:
                    result["status"] = json.load(f)
            else:
                result["status"] = None

            body = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_cors_headers()
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self._send_error(500, str(e))

    def _handle_command(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            data = json.loads(body)
            command = data.get("command", "")

            if command not in VALID_COMMANDS:
                self._send_error(
                    400,
                    f"Invalid command: {command!r}. "
                    f"Valid: {', '.join(sorted(VALID_COMMANDS))}",
                )
                return

            os.makedirs(STATE_DIR, exist_ok=True)
            cmd_payload = {
                "command": command,
                "timestamp": int(time.time()),
                "source": "dashboard",
            }
            with open(COMMAND_FILE, "w") as f:
                json.dump(cmd_payload, f)

            resp = json.dumps({"ok": True, "command": command}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp)))
            self.send_cors_headers()
            self.end_headers()
            self.wfile.write(resp)
        except json.JSONDecodeError:
            self._send_error(400, "Invalid JSON body")
        except Exception as e:
            self._send_error(500, str(e))

    def _serve_file(self, url_path):
        if url_path in ("/", ""):
            url_path = "/mm_dashboard.html"

        # Prevent path traversal
        base = Path(os.getcwd()).resolve()
        file_path = (base / url_path.lstrip("/")).resolve()
        if not str(file_path).startswith(str(base)):
            self._send_error(403, "Forbidden")
            return

        if not file_path.exists() or not file_path.is_file():
            self._send_error(404, f"Not found: {url_path}")
            return

        content_type = self._guess_type(str(file_path))
        try:
            with open(file_path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_cors_headers()
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self._send_error(500, str(e))

    @staticmethod
    def _guess_type(path):
        ext = os.path.splitext(path)[1].lower()
        return {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".json": "application/json",
            ".png": "image/png",
            ".ico": "image/x-icon",
            ".svg": "image/svg+xml",
        }.get(ext, "application/octet-stream")

    def _send_error(self, code, msg):
        body = json.dumps({"error": msg}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser(description="AME Market Maker Dashboard Server")
    parser.add_argument(
        "--port", type=int, default=3001, help="Port to serve on (default: 3001)"
    )
    args = parser.parse_args()

    # Always serve from the directory that contains this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    server = HTTPServer(("0.0.0.0", args.port), DashboardHandler)
    print(f"AME Dashboard Server  →  http://0.0.0.0:{args.port}")
    print(f"  Root : {script_dir}")
    print(f"  Dashboard : http://localhost:{args.port}/mm_dashboard.html")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
