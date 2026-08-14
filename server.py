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

def fail(sid):
    """Mark the search failed — unless it already finished. A worker can write its results, mark the
    search complete and then die on the way out; calling that an error throws away a good search and
    shows Vivify a failure that isn't one."""
    try:
        s = worker.get_search(sid) or {}
        if s.get('status') in ('complete', 'enriching'):
            sys.stderr.write(f"worker for {sid} died after finishing — leaving status {s.get('status')}\n")
            return
        worker.set_status(sid, 'error')
    except Exception:
        pass

def _run_search(sid, force=False):
    try:
        cmd = [sys.executable, WORKER_PY, str(sid)] + (['--force'] if force else [])
        p = subprocess.run(cmd, timeout=1800)
        if p.returncode != 0:
            sys.stderr.write(f"worker for {sid} exited {p.returncode}\n")
            fail(sid)
    except subprocess.TimeoutExpired:
        sys.stderr.write(f"worker for {sid} timed out\n")
        fail(sid)
    except Exception:
        sys.stderr.write(f"worker error for {sid}:\n{traceback.format_exc()}\n")
        fail(sid)

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
        # /diag?hosts=a.com,b.com — times DNS, TCP connect and the fetch separately for each host, from
        # the box. Hosts this instance gives up on answer in under a second from a laptop, and the fix
        # for a stalled resolver is nothing like the fix for a blocked IP range, so measure which it is.
        if self.path.startswith('/diag'):
            import socket, time as _t, urllib.parse as _up
            q = _up.parse_qs(_up.urlparse(self.path).query)
            # ?conc=N repeats the same hosts N times at once. Sequentially they all answer in under a
            # second from here, yet a search gives up on them, so the question is what our own
            # concurrency does to them — DNS in particular, which no timeout in the worker bounds.
            conc = int((q.get('conc', ['0'])[0]) or 0)
            if conc:
                import socket as _s, concurrent.futures as _cf, time as _tt
                hl = [h.strip() for h in q.get('hosts', [''])[0].split(',') if h.strip()]
                jobs = [hl[i % len(hl)] for i in range(conc)]
                def probe(h):
                    t = _tt.time()
                    try:
                        _s.getaddrinfo(h, 443, proto=_s.IPPROTO_TCP)
                        return ('dns_ok', round(_tt.time() - t, 1))
                    except Exception:
                        return ('dns_fail', round(_tt.time() - t, 1))
                t0 = _tt.time()
                with _cf.ThreadPoolExecutor(max_workers=conc) as _ex:
                    res = list(_ex.map(probe, jobs))
                times = sorted(r[1] for r in res)
                return self._send(200, {"conc": conc, "wall_s": round(_tt.time() - t0, 1),
                                        "dns_ok": sum(1 for r in res if r[0] == 'dns_ok'),
                                        "median_s": times[len(times) // 2], "worst_s": times[-1]})
            out = []
            for h in (q.get('hosts', [''])[0].split(',') if q.get('hosts') else []):
                h = h.strip()
                if not h: continue
                row = {"host": h}
                t0 = _t.time()
                try:
                    ip = socket.getaddrinfo(h, 443, proto=socket.IPPROTO_TCP)[0][4][0]
                    row["dns_s"] = round(_t.time() - t0, 1); row["ip"] = ip
                except Exception as e:
                    row["dns_s"] = round(_t.time() - t0, 1); row["dns_err"] = str(e)[:60]; out.append(row); continue
                t1 = _t.time()
                try:
                    s = socket.create_connection((ip, 443), timeout=10); s.close()
                    row["tcp_s"] = round(_t.time() - t1, 1)
                except Exception as e:
                    row["tcp_s"] = round(_t.time() - t1, 1); row["tcp_err"] = str(e)[:60]; out.append(row); continue
                t2 = _t.time()
                row["bytes"] = len(worker.fetch(f"https://{h}/")[1]); row["fetch_s"] = round(_t.time() - t2, 1)
                out.append(row)
            return self._send(200, {"diag": out})
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
