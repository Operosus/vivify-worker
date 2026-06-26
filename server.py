#!/usr/bin/env python3
"""HTTP front for the venue-hirers worker (Render web service).
POST /webhook/vivify-venue-hirers  {"search_id": <id>}  -> acks immediately, runs discovery in a
background thread (FE polls Supabase for status, same as the old n8n flow). GET / -> health check."""
import json, threading, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import worker

PORT = int(os.environ.get('PORT', '10000'))

def run_search(sid):
    try:
        worker.run(sid)
    except Exception:
        sys.stderr.write(f"worker error for {sid}:\n{traceback.format_exc()}\n")
        try: worker.set_status(sid, 'error')
        except Exception: pass

class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
