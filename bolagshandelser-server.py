#!/usr/bin/env python3
"""
Bolagshändelser server — persistent WebSocket connection to ImpactLoop +
Breakit pressreleases via their open API.

Polls every 10 min, pushes updates to browser via Server-Sent Events (SSE).

Run: python3 ~/Desktop/bolagshandelser-server.py
"""

import asyncio
import json
import re
import threading
import time
import queue
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

import websockets
import streamlit.proto.ForwardMsg_pb2 as ForwardMsg_pb2
import streamlit.proto.BackMsg_pb2 as BackMsg_pb2

APP_URL = "wss://impactloop-app-354439576771.europe-north1.run.app/_stcore/stream"
ORIGIN = "https://impactloop-app-354439576771.europe-north1.run.app"
BREAKIT_API = "https://api.breakit.se/api/1"
POLL_INTERVAL = 600  # 10 minutes

# Shared state — ImpactLoop events
events_lock = threading.Lock()
all_events = {}  # key -> event dict
sse_clients = []  # list of queue.Queue for SSE subscribers
last_fetch_time = None
fetch_count = 0

# Shared state — Breakit pressreleases
breakit_lock = threading.Lock()
breakit_pressreleases = {}  # id -> pressrelease dict
breakit_articles = []  # all fetched articles
breakit_extra = {"podcasts": [], "events": []}
breakit_last_fetch = None


def event_key(e):
    return f"{e.get('org_number')}_{e.get('date')}_{e.get('event_type')}_{e.get('company')}"


def parse_events_from_messages(ws_messages):
    """Parse Streamlit protobuf messages into event dicts."""
    events = []
    current_date = None
    current_event = {}

    for raw in ws_messages:
        fwd = ForwardMsg_pb2.ForwardMsg()
        fwd.ParseFromString(raw)
        msg_type = fwd.WhichOneof("type")

        if msg_type == "script_finished":
            break
        if msg_type != "delta":
            continue

        delta = fwd.delta
        if delta.WhichOneof("type") != "new_element":
            continue

        elem = delta.new_element
        elem_type = elem.WhichOneof("type")

        if elem_type == "markdown":
            text = elem.markdown.body
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
            if "font-family:monospace" in text and date_match:
                current_date = date_match.group(1)
                continue
            if text.startswith("**") and (
                "har " in text or "Konkurs" in text
                or "Styrelseändring" in text or "kapital" in text
            ):
                company = re.search(r"\*\*(.+?)\*\*", text)
                current_event = {
                    "date": current_date,
                    "company": company.group(1) if company else "Unknown",
                    "text": re.sub(r"\*\*|\*|_|📌", "", text).strip(),
                    "raw_markdown": text,
                }
                # Extract board member changes (name + role)
                changes = re.findall(r"([➕➖])\s*(.+?)\s*\(([^)]+)\)", text)
                if changes:
                    current_event["board_changes"] = [
                        {"action": "added" if c[0] == "➕" else "removed",
                         "name": c[1].strip(), "role": c[2].strip()}
                        for c in changes
                    ]
                continue
            if "eivora.com" in text:
                uuid_match = re.search(r"company/([a-f0-9-]+)", text)
                if current_event:
                    current_event["eivora_id"] = uuid_match.group(1) if uuid_match else None
                continue

        elif elem_type == "button":
            btn_id = elem.button.id
            if "skriv_" in btn_id and current_event:
                org_match = re.search(r"skriv_(\d{10})", btn_id)
                type_match = re.search(
                    r"(company_accounts|company_director|company_group_accounts|"
                    r"company_bankruptcy|company_capital|company_new_share_issue|"
                    r"share_issue|bankruptcy)",
                    btn_id,
                )
                current_event["org_number"] = org_match.group(1) if org_match else None
                current_event["event_type"] = type_match.group(1) if type_match else None
                events.append(current_event)
                current_event = {}

    # Classify events with null event_type based on text content
    for e in events:
        if e.get("event_type") is None:
            text = e.get("text", "")
            if "har tagit in" in text and "kapital" in text:
                e["event_type"] = "share_issue"
            elif "Konkurs" in text:
                e["event_type"] = "bankruptcy"

    return events


async def fetch_once():
    """Single WebSocket fetch of all events."""
    messages = []
    async with websockets.connect(
        APP_URL,
        additional_headers={"Origin": ORIGIN},
        subprotocols=["streamlit"],
        max_size=50 * 1024 * 1024,
        open_timeout=15,
    ) as ws:
        rerun = BackMsg_pb2.BackMsg()
        rerun.rerun_script.query_string = ""
        rerun.rerun_script.page_script_hash = ""
        await ws.send(rerun.SerializeToString())

        for _ in range(500):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                break
            if isinstance(raw, bytes):
                messages.append(raw)
                # Check if script finished
                fwd = ForwardMsg_pb2.ForwardMsg()
                fwd.ParseFromString(raw)
                if fwd.WhichOneof("type") == "script_finished":
                    break

    return parse_events_from_messages(messages)


def do_fetch():
    """Fetch events and merge into global store. Returns list of new events."""
    global last_fetch_time, fetch_count

    events = asyncio.run(fetch_once())
    new_events = []

    with events_lock:
        for e in events:
            key = event_key(e)
            if key not in all_events:
                all_events[key] = e
                new_events.append(e)
        fetch_count += 1
        last_fetch_time = time.time()

    return new_events, events


def notify_sse_clients(new_events):
    """Push new events to all SSE subscribers."""
    if not new_events:
        return
    data = json.dumps(new_events, ensure_ascii=False)
    dead = []
    for q in sse_clients:
        try:
            q.put_nowait(data)
        except queue.Full:
            dead.append(q)
    for q in dead:
        sse_clients.remove(q)


def breakit_get(endpoint):
    """GET from Breakit API, return parsed JSON."""
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(f"{BREAKIT_API}/{endpoint}", headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_breakit():
    """Fetch all available data from Breakit open API."""
    global breakit_last_fetch
    new_prs = []

    # Fetch pressreleases
    prs = breakit_get("pressreleases")
    with breakit_lock:
        for pr in prs:
            pr_id = pr.get("id")
            if pr_id and pr_id not in breakit_pressreleases:
                breakit_pressreleases[pr_id] = pr
                new_prs.append(pr)

    # Fetch front-page (has the latest articles with full body for free ones)
    try:
        fp = breakit_get("front-page")
        with breakit_lock:
            existing_ids = {a.get("id") for a in breakit_articles}
            for widget in fp:
                ad = widget.get("articleData", {})
                if ad and ad.get("id") and ad["id"] not in existing_ids:
                    ad["_source"] = "front-page"
                    breakit_articles.append(ad)
                    existing_ids.add(ad["id"])
                # Also grab related articles
                for rel in widget.get("relatedArticles", []):
                    if rel.get("id") and rel["id"] not in existing_ids:
                        rel["_source"] = "front-page-related"
                        breakit_articles.append(rel)
                        existing_ids.add(rel["id"])
    except Exception as e:
        print(f"  [!] Breakit front-page error: {e}")

    # Fetch latest articles from paginated endpoint (most recent pages)
    try:
        existing_ids = {a.get("id") for a in breakit_articles}
        for page in range(4194, 4180, -1):
            arts = breakit_get(f"articles?page={page}")
            if not arts:
                continue
            with breakit_lock:
                for a in arts:
                    if a.get("id") and a["id"] not in existing_ids:
                        a["_source"] = "articles"
                        breakit_articles.append(a)
                        existing_ids.add(a["id"])
    except Exception as e:
        print(f"  [!] Breakit articles error: {e}")

    # Fetch podcasts
    try:
        pods = breakit_get("podcasts")
        with breakit_lock:
            breakit_extra["podcasts"] = pods
    except Exception:
        pass

    # Fetch events (Breakit's own events)
    try:
        events = breakit_get("events")
        with breakit_lock:
            breakit_extra["events"] = events
    except Exception:
        pass

    # Sort articles by date descending
    with breakit_lock:
        breakit_articles.sort(key=lambda a: a.get("date", ""), reverse=True)
        breakit_last_fetch = time.time()

    return new_prs


def poll_loop():
    """Background thread: fetch every POLL_INTERVAL seconds."""
    while True:
        try:
            new_events, _ = do_fetch()
            ts = time.strftime("%H:%M:%S")
            with events_lock:
                total = len(all_events)
            print(f"  [{ts}] Poll: {len(new_events)} nya, {total} totalt")
            notify_sse_clients(new_events)
        except Exception as e:
            print(f"  [!] Poll error: {e}")

        try:
            new_prs = fetch_breakit()
            if new_prs:
                # Notify SSE clients about new pressreleases
                data = json.dumps({"type": "breakit", "pressreleases": new_prs}, ensure_ascii=False)
                dead = []
                for q in sse_clients:
                    try:
                        q.put_nowait(data)
                    except queue.Full:
                        dead.append(q)
                for q in dead:
                    sse_clients.remove(q)
                print(f"  [{ts}] Breakit: {len(new_prs)} nya pressreleaser")
        except Exception as e:
            print(f"  [!] Breakit fetch error: {e}")

        time.sleep(POLL_INTERVAL)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/fetch":
            # Return current cached events (fast)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with events_lock:
                data = list(all_events.values())
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

        elif self.path == "/api/refresh":
            # Force a fresh fetch from ImpactLoop
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                new_events, all_fetched = do_fetch()
                notify_sse_clients(new_events)
                with events_lock:
                    total = len(all_events)
                self.wfile.write(json.dumps({
                    "new": len(new_events),
                    "fetched": len(all_fetched),
                    "total": total,
                    "new_events": new_events,
                }, ensure_ascii=False).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/status":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with events_lock:
                total = len(all_events)
            self.wfile.write(json.dumps({
                "total": total,
                "fetch_count": fetch_count,
                "last_fetch": last_fetch_time,
                "poll_interval": POLL_INTERVAL,
            }).encode())

        elif self.path == "/api/stream":
            # Server-Sent Events endpoint
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            q = queue.Queue(maxsize=50)
            sse_clients.append(q)
            print(f"  SSE client connected ({len(sse_clients)} total)")

            try:
                # Send current data immediately
                with events_lock:
                    data = list(all_events.values())
                self.wfile.write(f"event: init\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode())
                self.wfile.flush()

                # Keep connection open, push updates
                while True:
                    try:
                        msg = q.get(timeout=30)
                        self.wfile.write(f"event: update\ndata: {msg}\n\n".encode())
                        self.wfile.flush()
                    except queue.Empty:
                        # Send heartbeat
                        self.wfile.write(f": heartbeat\n\n".encode())
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                if q in sse_clients:
                    sse_clients.remove(q)
                print(f"  SSE client disconnected ({len(sse_clients)} remaining)")

        elif self.path == "/api/breakit/pressreleases":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with breakit_lock:
                data = list(breakit_pressreleases.values())
            data.sort(key=lambda x: x.get("date", ""), reverse=True)
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

        elif self.path == "/api/breakit/articles":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with breakit_lock:
                data = list(breakit_articles)
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

        elif self.path == "/api/breakit/podcasts":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with breakit_lock:
                data = breakit_extra.get("podcasts", [])
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

        elif self.path == "/api/breakit/events":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            with breakit_lock:
                data = breakit_extra.get("events", [])
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

        elif self.path.startswith("/api/breakit/search"):
            query = ""
            if "?" in self.path:
                params = self.path.split("?", 1)[1]
                for p in params.split("&"):
                    if p.startswith("q="):
                        query = urllib.request.unquote(p[2:])
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                headers = {"User-Agent": "Mozilla/5.0"}
                req = urllib.request.Request(
                    f"{BREAKIT_API}/search?q={urllib.request.quote(query)}",
                    headers=headers,
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    result = resp.read().decode()
                self.wfile.write(result.encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET")
        self.end_headers()

    def log_message(self, fmt, *args):
        pass  # Suppress default logging


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


if __name__ == "__main__":
    port = 9999

    # Initial fetch
    print("=== Bolagshändelser Server ===")
    print("  Hämtar initial data från ImpactLoop...")
    try:
        new_events, all_fetched = do_fetch()
        print(f"  {len(all_fetched)} händelser hämtade")
    except Exception as e:
        print(f"  [!] Initial fetch failed: {e}")

    # Initial Breakit fetch
    print("  Hämtar pressreleaser från Breakit...")
    try:
        new_prs = fetch_breakit()
        with breakit_lock:
            total_prs = len(breakit_pressreleases)
            total_arts = len(breakit_articles)
        print(f"  {total_prs} pressreleaser, {total_arts} artiklar hämtade")
    except Exception as e:
        print(f"  [!] Breakit fetch failed: {e}")

    # Start background poller
    poller = threading.Thread(target=poll_loop, daemon=True)
    poller.start()
    print(f"  Auto-poll var {POLL_INTERVAL//60} min")

    # Start HTTP server
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"  Server: http://localhost:{port}")
    print(f"  SSE:    http://localhost:{port}/api/stream")
    print(f"  Ctrl+C för att avsluta\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStänger server...")
        server.shutdown()
