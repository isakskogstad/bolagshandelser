#!/usr/bin/env python3
"""
Fetch Bolagshändelser from ImpactLoop Streamlit app.
No authentication required - data is publicly accessible via WebSocket.
"""

import asyncio
import websockets
import json
import re
import sys
from datetime import datetime

try:
    import streamlit.proto.ForwardMsg_pb2 as ForwardMsg_pb2
    import streamlit.proto.BackMsg_pb2 as BackMsg_pb2
except ImportError:
    print("ERROR: pip install streamlit")
    sys.exit(1)

APP_URL = "wss://impactloop-app-354439576771.europe-north1.run.app/_stcore/stream"
ORIGIN = "https://impactloop-app-354439576771.europe-north1.run.app"


async def fetch_events(max_messages=500, timeout=30):
    """Connect to Streamlit WebSocket and extract Bolagshändelser."""
    
    async with websockets.connect(
        APP_URL,
        additional_headers={"Origin": ORIGIN},
        subprotocols=["streamlit"],
        max_size=50 * 1024 * 1024,
        open_timeout=15,
    ) as ws:
        # Trigger page load
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

                # Date marker
                date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
                if "font-family:monospace" in text and date_match:
                    current_date = date_match.group(1)
                    continue

                # Event text (bold company name)
                if text.startswith("**") and ("har " in text or "Konkurs" in text or "Styrelseändring" in text or "kapital" in text):
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

                # Eivora link
                if "eivora.com" in text:
                    uuid_match = re.search(r"company/([a-f0-9-]+)", text)
                    if current_event:
                        current_event["eivora_id"] = uuid_match.group(1) if uuid_match else None
                    continue

            elif elem_type == "button":
                btn_id = elem.button.id
                label = elem.button.label

                if "skriv_" in btn_id and current_event:
                    # Extract org number and event type from button ID
                    # Format: ...-skriv_5569684250_2026-02-27_company_accounts_0
                    org_match = re.search(r"skriv_(\d{10})", btn_id)
                    type_match = re.search(r"(company_accounts|company_director|company_group_accounts|company_bankruptcy|company_capital|company_new_share_issue)", btn_id)

                    current_event["org_number"] = org_match.group(1) if org_match else None
                    current_event["event_type"] = type_match.group(1) if type_match else None
                    events.append(current_event)
                    current_event = {}

        return events


def main():
    events = asyncio.run(fetch_events())

    if "--json" in sys.argv:
        print(json.dumps(events, ensure_ascii=False, indent=2))
        return

    print(f"=== Bolagshändelser ({len(events)} st) ===")
    print(f"Hämtad: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    for e in events:
        print(f"[{e.get('date', '?')}] {e.get('company', '?')}  (org: {e.get('org_number', '?')})")
        print(f"  Typ: {e.get('event_type', '?')}")
        print(f"  {e.get('text', '')}")
        print()


if __name__ == "__main__":
    main()
