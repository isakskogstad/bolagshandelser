#!/usr/bin/env python3
"""
Fetch Bolagshändelser from ImpactLoop + Breakit and save as JSON files.
Designed to run in GitHub Actions on a cron schedule.
"""

import asyncio
import json
import os
import re
import sys
import urllib.request
from datetime import datetime

try:
    import websockets
    import streamlit.proto.ForwardMsg_pb2 as ForwardMsg_pb2
    import streamlit.proto.BackMsg_pb2 as BackMsg_pb2
except ImportError:
    print("ERROR: pip install streamlit websockets")
    sys.exit(1)

APP_URL = "wss://impactloop-app-354439576771.europe-north1.run.app/_stcore/stream"
ORIGIN = "https://impactloop-app-354439576771.europe-north1.run.app"
BREAKIT_API = "https://api.breakit.se/api/1"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


async def fetch_impactloop_events(max_messages=500, timeout=30):
    """Connect to Streamlit WebSocket and extract Bolagshändelser."""
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

        events = []
        current_date = None
        current_event = {}

        for _ in range(max_messages):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                break

            if not isinstance(raw, bytes):
                continue

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
        classify_event_type(e)

    return events


def breakit_get(endpoint):
    """GET from Breakit API, return parsed JSON."""
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(f"{BREAKIT_API}/{endpoint}", headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def fetch_breakit():
    """Fetch pressreleases and articles from Breakit."""
    pressreleases = []
    articles = []

    # Pressreleases
    try:
        prs = breakit_get("pressreleases")
        pressreleases = prs
        print(f"  Breakit: {len(prs)} pressreleaser")
    except Exception as e:
        print(f"  [!] Breakit pressreleases error: {e}")

    # Front-page articles
    try:
        fp = breakit_get("front-page")
        existing_ids = set()
        for widget in fp:
            ad = widget.get("articleData", {})
            if ad and ad.get("id") and ad["id"] not in existing_ids:
                ad["_source"] = "front-page"
                articles.append(ad)
                existing_ids.add(ad["id"])
            for rel in widget.get("relatedArticles", []):
                if rel.get("id") and rel["id"] not in existing_ids:
                    rel["_source"] = "front-page-related"
                    articles.append(rel)
                    existing_ids.add(rel["id"])
    except Exception as e:
        print(f"  [!] Breakit front-page error: {e}")

    # Paginated articles
    try:
        existing_ids = {a.get("id") for a in articles}
        for page in range(4194, 4180, -1):
            arts = breakit_get(f"articles?page={page}")
            if not arts:
                continue
            for a in arts:
                if a.get("id") and a["id"] not in existing_ids:
                    a["_source"] = "articles"
                    articles.append(a)
                    existing_ids.add(a["id"])
    except Exception as e:
        print(f"  [!] Breakit articles error: {e}")

    articles.sort(key=lambda a: a.get("date", ""), reverse=True)
    print(f"  Breakit: {len(articles)} artiklar")

    return pressreleases, articles


def classify_event_type(event):
    """Classify event_type based on text content if missing."""
    if event.get("event_type") is not None:
        return
    text = event.get("text", "")
    if "har tagit in" in text and "kapital" in text:
        event["event_type"] = "share_issue"
    elif "Konkurs" in text:
        event["event_type"] = "bankruptcy"


def merge_with_existing(new_data, filepath, key_func):
    """Merge new data with existing JSON file, avoiding duplicates."""
    existing = []
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            existing = []

    # Fix event_type on existing data too
    for item in existing:
        classify_event_type(item)

    existing_keys = {key_func(item) for item in existing}
    for item in new_data:
        k = key_func(item)
        if k not in existing_keys:
            existing.append(item)
            existing_keys.add(k)

    return existing


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    status = {
        "last_fetch": now,
        "events_count": 0,
        "pressreleases_count": 0,
        "articles_count": 0,
        "errors": [],
    }

    # Fetch ImpactLoop events
    print("Hämtar bolagshändelser från ImpactLoop...")
    try:
        events = asyncio.run(fetch_impactloop_events())
        print(f"  {len(events)} händelser hämtade")

        # Merge with existing
        events_file = os.path.join(DATA_DIR, "events.json")
        all_events = merge_with_existing(
            events, events_file,
            lambda e: f"{e.get('org_number')}_{e.get('date')}_{e.get('event_type')}_{e.get('company')}"
        )
        all_events.sort(key=lambda e: e.get("date", ""), reverse=True)

        with open(events_file, "w", encoding="utf-8") as f:
            json.dump(all_events, f, ensure_ascii=False, indent=2)

        status["events_count"] = len(all_events)
        print(f"  Totalt: {len(all_events)} händelser sparade")
    except Exception as e:
        print(f"  [!] ImpactLoop error: {e}")
        status["errors"].append(f"ImpactLoop: {str(e)}")

    # Fetch Breakit
    print("Hämtar data från Breakit...")
    try:
        pressreleases, articles = fetch_breakit()

        # Save pressreleases (merge)
        pr_file = os.path.join(DATA_DIR, "breakit-pressreleases.json")
        all_prs = merge_with_existing(
            pressreleases, pr_file,
            lambda p: str(p.get("id", ""))
        )
        all_prs.sort(key=lambda p: p.get("date", ""), reverse=True)
        with open(pr_file, "w", encoding="utf-8") as f:
            json.dump(all_prs, f, ensure_ascii=False, indent=2)
        status["pressreleases_count"] = len(all_prs)

        # Save articles (merge)
        art_file = os.path.join(DATA_DIR, "breakit-articles.json")
        all_arts = merge_with_existing(
            articles, art_file,
            lambda a: str(a.get("id", ""))
        )
        all_arts.sort(key=lambda a: a.get("date", ""), reverse=True)
        with open(art_file, "w", encoding="utf-8") as f:
            json.dump(all_arts, f, ensure_ascii=False, indent=2)
        status["articles_count"] = len(all_arts)

        print(f"  Totalt: {len(all_prs)} pressreleaser, {len(all_arts)} artiklar sparade")
    except Exception as e:
        print(f"  [!] Breakit error: {e}")
        status["errors"].append(f"Breakit: {str(e)}")

    # Save status
    with open(os.path.join(DATA_DIR, "status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

    print(f"\nKlart! Status: {json.dumps(status, ensure_ascii=False)}")

    # Exit with error if no data was fetched
    if status["events_count"] == 0 and not os.path.exists(os.path.join(DATA_DIR, "events.json")):
        sys.exit(1)


if __name__ == "__main__":
    main()
