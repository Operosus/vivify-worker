#!/usr/bin/env python3
"""HTTP front for the venue-hirers worker (Render web service).
POST /webhook/vivify-venue-hirers  {"search_id": <id>}  -> acks immediately, runs discovery in a
separate PROCESS (FE polls Supabase for status). GET / -> health check.

Why a process and not a thread: parsing a few hundred pages is CPU-bound regex work, and Python's
GIL meant it starved this HTTP server on Render's half-CPU instance. Health checks then timed out and
Render killed the instance mid-search, leaving the search stuck at 'searching' forever. A child
process leaves the server responsive, and if it dies we can see the exit code and fail the search
honestly instead of letting the UI spin."""
import json, threading, traceback, subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import worker

PORT = int(os.environ.get('PORT', '10000'))
WORKER_PY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'worker.py')

def run_search(sid):
    try:
        p = subprocess.run([sys.executable, WORKER_PY, str(sid)], timeout=1800)
        if p.returncode != 0:
            sys.stderr.write(f"worker for {sid} exited {p.returncode}\n")
            try: worker.set_status(sid, 'error')
            except Exception: pass
    except subprocess.TimeoutExpired:
        sys.stderr.write(f"worker for {sid} timed out\n")
        try: worker.set_status(sid, 'error')
        except Exception: pass
    except Exception:
        sys.stderr.write(f"worker error for {sid}:\n{traceback.format_exc()}\n")
        try: worker.set_status(sid, 'error')
        except Exception: pass

class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        self._send(200, {"ok": True, "service": "vivify-venue-hirers"})

    def do_POST(self):
        if not self.path.rstrip('/').endswith('vivify-venue-hirers'):
            return self._send(404, {"error": "not found"})
        try:
            n = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(n) or b'{}')
            sid = int(body.get('search_id'))
        except Exception:
            return self._send(400, {"error": "search_id required"})
        threading.Thread(target=run_search, args=(sid,), daemon=True).start()
        self._send(200, {"success": True, "search_id": sid, "status": "searching"})

    def log_message(self, *a): pass  # quiet default logging

if __name__ == '__main__':
    print(f"vivify venue-hirers worker listening on :{PORT}")
    ThreadingHTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
