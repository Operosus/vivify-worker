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

# Luke fires a run of venues one after another, so several searches land within seconds of each other.
# Each one reads up to 200 pages on half a CPU, and three at once took longer than the 30-minute cap and
# were killed with nothing to show for the DataForSEO spend. Two lanes, and the rest wait their turn:
# a search that starts late still finishes, and the watchdog now times from when the run actually
# started rather than from when the record was created, so queueing cannot be mistaken for a stall.
LANES = threading.BoundedSemaphore(int(os.environ.get('WORKER_LANES', '2')))

def run_search(sid, force=False):
    with LANES:
        _run_search(sid, force)

def _run_search(sid, force=False):
    try:
        cmd = [sys.executable, WORKER_PY, str(sid)] + (['--force'] if force else [])
        p = subprocess.run(cmd, timeout=1800)
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
            force = bool(body.get('force'))
        except Exception:
            return self._send(400, {"error": "search_id required"})
        threading.Thread(target=run_search, args=(sid, force), daemon=True).start()
        self._send(200, {"success": True, "search_id": sid, "status": "searching", "force": force})

    def log_message(self, *a): pass  # quiet default logging

if __name__ == '__main__':
    print(f"vivify venue-hirers worker listening on :{PORT}")
    ThreadingHTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
