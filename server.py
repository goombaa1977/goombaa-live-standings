"""
Goombaa Control Center - Backend Web Server
File: server.py
Description: Full FastAPI backend with centralized backend KOTH slot rotation for both Player 1 and Player 2,
independent multi-tier win cascading (including Yearly archives), synchronized non-blocking Google Sheets background sync,
and automated time-based periodic resets with monthly historical archiving.
"""

import os
import sys
import json
import asyncio
import urllib.request
import urllib.parse
from datetime import datetime
from typing import List, Any, Dict
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

DAILY_FILE = os.path.join(SCRIPT_DIR, "standings_daily.json")
WEEKLY_FILE = os.path.join(SCRIPT_DIR, "standings_weekly.json")
MONTHLY_FILE = os.path.join(SCRIPT_DIR, "standings_monthly.json")
YEARLY_FILE = os.path.join(SCRIPT_DIR, "standings_yearly.json")
MASTER_FILE = os.path.join(SCRIPT_DIR, "standings.json")
QUEUE_FILE = os.path.join(SCRIPT_DIR, "queue_cache.json")
META_FILE = os.path.join(SCRIPT_DIR, "metadata.json")

GOOGLE_SHEET_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbys0_Xn7_xWLlIPMM5Dq99visJ7DcMlfDohkDv9nZR0Sn4E2ueWqwhyC41Aifb18enN_Q/exec"

DEFAULT_STANDINGS = [
    {"tag": "Goombaa", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "PhantomOrphan", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Alec", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Royal", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Someguy", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Brandy", "platform": "TikTok", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Jonathan", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Liam", "platform": "TikTok", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Not A Saint", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Nuber", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Ocu", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "TMO", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"}
]

def load_json_file(filepath: str, fallback_data: Any) -> Any:
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if data and data != []:
                    return data
        except Exception:
            pass
    return fallback_data

def save_json_file(filepath: str, data: Any):
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error saving {filepath}: {e}")

def zero_out_wins_preserve_names(list_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for player in list_data:
        player["wins"] = "0"
        player["points"] = "0"
        player["rank"] = "-"
    return list_data

def post_to_google_sheets(payload: dict):
    try:
        data_encoded = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            GOOGLE_SHEET_WEB_APP_URL,
            data=data_encoded,
            headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            res_data = json.loads(response.read().decode())
            return res_data
    except Exception as e:
        print(f"Error communicating with Google Sheets Web App: {e}")
        return {"status": "error", "message": str(e)}

app = FastAPI(title="Goombaa Stream Control Center")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

initial_daily = load_json_file(DAILY_FILE, list(DEFAULT_STANDINGS))
initial_weekly = load_json_file(WEEKLY_FILE, list(DEFAULT_STANDINGS))
initial_monthly = load_json_file(MONTHLY_FILE, list(DEFAULT_STANDINGS))

raw_yearly = load_json_file(YEARLY_FILE, {"ALL": list(DEFAULT_STANDINGS)})
if isinstance(raw_yearly, list):
    initial_yearly = {"ALL": raw_yearly}
else:
    initial_yearly = raw_yearly

initial_master = load_json_file(MASTER_FILE, list(DEFAULT_STANDINGS))
initial_queue = load_json_file(QUEUE_FILE, [])

def check_and_perform_automatic_resets():
    global initial_weekly, initial_monthly, initial_yearly, initial_master
    now = datetime.now()
    current_year, current_week, _ = now.isocalendar()
    current_month = now.strftime("%Y-%m")
    current_year_str = str(now.year)

    metadata = load_json_file(META_FILE, {})
    
    saved_week = metadata.get("last_weekly_reset_week")
    saved_month = metadata.get("last_month")
    saved_year = metadata.get("last_year")

    updated = False

    if saved_week != current_week:
        state["standings_weekly"] = zero_out_wins_preserve_names(state["standings_weekly"])
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
        metadata["last_weekly_reset_week"] = current_week
        updated = True

    if saved_month != current_month:
        if saved_month:
            if not isinstance(state["standings_yearly"], dict):
                state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
            
            parts = saved_month.split("-")
            if len(parts) == 2:
                y_str, m_str = parts
                m_names = {"01": "JAN", "02": "FEB", "03": "MAR", "04": "APR", "05": "MAY", "06": "JUN", "07": "JUL", "08": "AUG", "09": "SEP", "10": "OCT", "11": "NOV", "12": "DEC"}
                m_code = m_names.get(m_str, m_str)
                archive_key = f"yearly_{y_str}_{m_code}"
                # Snapshot current monthly standings safely before reset
                state["standings_yearly"][archive_key] = list(state["standings_monthly"])
                save_json_file(YEARLY_FILE, state["standings_yearly"])

        state["standings_monthly"] = zero_out_wins_preserve_names(state["standings_monthly"])
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
        metadata["last_month"] = current_month
        updated = True

    if saved_year != current_year_str:
        if not isinstance(state["standings_yearly"], dict):
            state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
        state["standings_yearly"]["ALL"] = zero_out_wins_preserve_names(state["standings_yearly"].get("ALL", list(DEFAULT_STANDINGS)))
        save_json_file(YEARLY_FILE, state["standings_yearly"])
        metadata["last_year"] = current_year_str
        updated = True

    if updated:
        save_json_file(META_FILE, metadata)

try:
    check_and_perform_automatic_resets()
except Exception as e:
    print(f"Error checking automatic time resets: {e}")

state: Dict[str, Any] = {
    "match": {
        "round": "FIGHTING NOW",
        "p1": "Player 1",
        "p2": "Player 2",
        "player1": "Player 1",
        "player2": "Player 2",
        "score1": 0,
        "score2": 0
    },
    "queue": initial_queue,
    "banner": {
        "active": False, "visible": False, "header": "NEXT HOUR", "text": "NEXT HOUR", "message": "", "subtext": ""
    },
    "cocommentator": {
        "active": False, "host": "goombaa1977", "cohost": "", "name": ""
    },
    "charity": {
        "raised": 20.0, "goal": 100.0
    },
    "standings": initial_master,
    "standings_daily": initial_daily,
    "standings_weekly": initial_weekly,
    "standings_monthly": initial_monthly,
    "standings_yearly": initial_yearly,
    "standings_master": initial_master
}

def get_standings_payload():
    payload = {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
    }
    yearly_val = state["standings_yearly"]
    if isinstance(yearly_val, dict):
        payload.update(yearly_val)
        payload["yearly"] = yearly_val.get("ALL", list(DEFAULT_STANDINGS))
    else:
        payload["yearly"] = yearly_val
    return payload

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        full_payload = dict(state)
        full_payload["standings"] = get_standings_payload()
        await websocket.send_text(json.dumps({"type": "FULL_STATE", "data": full_payload}))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message_type: str, payload: Any):
        message = json.dumps({"type": message_type, "data": payload})
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/api/state")
@app.get("/api/match")
async def get_match():
    return state["match"]

@app.post("/api/match")
async def post_match(req: Request):
    data = await req.json()
    if "round" in data: state["match"]["round"] = data["round"]
    p1_val = data.get("p1") or data.get("player1")
    p2_val = data.get("p2") or data.get("player2")
    if p1_val is not None:
        state["match"]["p1"] = p1_val
        state["match"]["player1"] = p1_val
    if p2_val is not None:
        state["match"]["p2"] = p2_val
        state["match"]["player2"] = p2_val
    await manager.broadcast("MATCH_UPDATE", state["match"])
    full_payload = dict(state)
    full_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_payload)
    return state["match"]

@app.get("/api/queue")
async def get_queue():
    return state["queue"]

@app.post("/api/queue")
async def set_queue(req: Request):
    data = await req.json()
    if isinstance(data, list):
        state["queue"] = data
        save_json_file(QUEUE_FILE, state["queue"])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    full_payload = dict(state)
    full_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_payload)
    return state["queue"]

@app.post("/api/queue/clear")
async def clear_queue():
    state["queue"] = []
    save_json_file(QUEUE_FILE, [])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    full_payload = dict(state)
    full_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_payload)
    return []

@app.get("/api/queue/next_match")
@app.post("/api/queue/next_match")
async def next_match(req: Request = None):
    winner = "p1"
    try:
        if req:
            body = await req.json()
            winner = str(body.get("winner", "p1")).lower()
    except Exception:
        pass

    if len(state["queue"]) > 0:
        next_player = state["queue"].pop(0)
        p1_current = state["match"].get("p1") or state["match"].get("player1")
        p2_current = state["match"].get("p2") or state["match"].get("player2")

        if winner == "p2" or winner == "player2":
            current_loser = p1_current
            state["match"]["p1"] = p2_current
            state["match"]["player1"] = p2_current
            state["match"]["p2"] = next_player
            state["match"]["player2"] = next_player
        else:
            current_loser = p2_current
            state["match"]["p2"] = next_player
            state["match"]["player2"] = next_player

        if current_loser and current_loser not in ["Player 1", "Player 2"] and current_loser not in state["queue"]:
            state["queue"].append(current_loser)

        save_json_file(QUEUE_FILE, state["queue"])

    await manager.broadcast("MATCH_UPDATE", state["match"])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    full_payload = dict(state)
    full_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_payload)
    return {"status": "success", "match": state["match"], "queue": state["queue"]}

@app.get("/api/standings")
async def get_standings():
    check_and_perform_automatic_resets()
    try:
        def fetch_google():
            req = urllib.request.Request(GOOGLE_SHEET_WEB_APP_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=2) as response:
                return json.loads(response.read().decode())

        data = await asyncio.to_thread(fetch_google)
        
        if data and isinstance(data, dict) and (len(data.get("daily", [])) > 0 or len(data.get("master", [])) > 0):
            for tier_key in ["daily", "weekly", "monthly", "yearly", "master"]:
                if tier_key in data and isinstance(data[tier_key], list) and len(data[tier_key]) > 0:
                    for p in data[tier_key]:
                        p["wins"] = str(p.get("wins", "0"))
                        p["points"] = "0"
            
            if len(data.get("daily", [])) > 0:
                state["standings_daily"] = data.get("daily")
                save_json_file(DAILY_FILE, state["standings_daily"])
            if len(data.get("weekly", [])) > 0:
                state["standings_weekly"] = data.get("weekly")
                save_json_file(WEEKLY_FILE, state["standings_weekly"])
            if len(data.get("monthly", [])) > 0:
                state["standings_monthly"] = data.get("monthly")
                save_json_file(MONTHLY_FILE, state["standings_monthly"])
            
            if "yearly" in data:
                if isinstance(data["yearly"], dict) and len(data["yearly"]) > 0:
                    state["standings_yearly"] = data["yearly"]
                elif isinstance(data["yearly"], list) and len(data["yearly"]) > 0:
                    if not isinstance(state["standings_yearly"], dict):
                        state["standings_yearly"] = {}
                    state["standings_yearly"]["ALL"] = data["yearly"]
                save_json_file(YEARLY_FILE, state["standings_yearly"])
            
            if "master" in data and isinstance(data["master"], list) and len(data["master"]) > 0:
                state["standings_master"] = data["master"]
                save_json_file(MASTER_FILE, state["standings_master"])
            
            state["standings"] = state["standings_master"]
    except Exception as e:
        print(f"Notice: Google Sheets fetch skipped/timed out, serving local cache: {e}")

    return get_standings_payload()

def update_wins_in_list(list_data: List[Dict[str, Any]], tag: str, amount: int) -> tuple:
    found_player = None
    for p in list_data:
        if p["tag"].lower() == tag.lower():
            current_wins = int(p.get("wins", "0")) + amount
            p["wins"] = str(max(0, current_wins))
            p["points"] = "0"
            w = int(p["wins"])
            if w >= 151: p["rank"] = "Platinum"
            elif w >= 101: p["rank"] = "Gold"
            elif w >= 51: p["rank"] = "Silver"
            elif w >= 1: p["rank"] = "Bronze"
            else: p["rank"] = "-"
            found_player = p
            break
    if not found_player:
        w = amount
        if w >= 151: r_tier = "Platinum"
        elif w >= 101: r_tier = "Gold"
        elif w >= 51: r_tier = "Silver"
        elif w >= 1: r_tier = "Bronze"
        else: r_tier = "-"
        found_player = {
            "tag": tag,
            "platform": "Twitch",
            "wins": str(amount),
            "points": "0",
            "rank": r_tier
        }
        list_data.append(found_player)
    return list_data, found_player

@app.post("/api/win")
async def add_win(req: Request):
    check_and_perform_automatic_resets()
    data = await req.json()
    tag = data.get("tag", "").strip()
    amount = int(data.get("amount", 1))
    auto_add = data.get("auto_add", True)

    if not tag or tag in ["Player 1", "Player 2"]:
        return {"status": "ignored"}

    state["standings_daily"], updated_p_daily = update_wins_in_list(state["standings_daily"], tag, amount)
    save_json_file(DAILY_FILE, state["standings_daily"])

    state["standings_weekly"], updated_p_weekly = update_wins_in_list(state["standings_weekly"], tag, amount)
    save_json_file(WEEKLY_FILE, state["standings_weekly"])

    state["standings_monthly"], updated_p_monthly = update_wins_in_list(state["standings_monthly"], tag, amount)
    save_json_file(MONTHLY_FILE, state["standings_monthly"])

    if not isinstance(state["standings_yearly"], dict):
        state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
    yearly_all_list = state["standings_yearly"].setdefault("ALL", list(DEFAULT_STANDINGS))
    state["standings_yearly"]["ALL"], updated_p_yearly = update_wins_in_list(yearly_all_list, tag, amount)
    save_json_file(YEARLY_FILE, state["standings_yearly"])

    state["standings_master"], updated_p_master = update_wins_in_list(state["standings_master"], tag, amount)
    state["standings"] = state["standings_master"]
    save_json_file(MASTER_FILE, state["standings_master"])

    async def background_sync_sheets():
        tiers_data = [
            ("daily", updated_p_daily),
            ("weekly", updated_p_weekly),
            ("monthly", updated_p_monthly),
            ("yearly", updated_p_yearly),
            ("master", updated_p_master)
        ]
        for tier_key, p_obj in tiers_data:
            await asyncio.to_thread(post_to_google_sheets, {
                "action": "update",
                "tier": tier_key,
                "tag": tag,
                "platform": p_obj.get("platform", "Twitch"),
                "wins": int(p_obj.get("wins", amount))
            })
            await asyncio.sleep(0.4)

    asyncio.create_task(background_sync_sheets())

    if auto_add:
        p1_current = str(state["match"].get("p1") or state["match"].get("player1") or "").strip()
        p2_current = str(state["match"].get("p2") or state["match"].get("player2") or "").strip()

        if p1_current.lower() == p2_current.lower() and len(state["queue"]) > 0:
            p2_current = state["queue"].pop(0)
            state["match"]["p2"] = p2_current
            state["match"]["player2"] = p2_current

        if len(state["queue"]) > 0:
            if tag.lower() == p2_current.lower():
                loser = p1_current
                state["match"]["p1"] = p2_current
                state["match"]["player1"] = p2_current

                next_challenger = state["queue"].pop(0)
                state["match"]["p2"] = next_challenger
                state["match"]["player2"] = next_challenger

                if loser and loser not in ["Player 1", "Player 2", ""] and loser not in state["queue"]:
                    state["queue"].append(loser)

            elif tag.lower() == p1_current.lower():
                loser = p2_current

                next_challenger = state["queue"].pop(0)
                state["match"]["p2"] = next_challenger
                state["match"]["player2"] = next_challenger

                if loser and loser not in ["Player 1", "Player 2", ""] and loser not in state["queue"]:
                    state["queue"].append(loser)

            save_json_file(QUEUE_FILE, state["queue"])
        else:
            if tag.lower() == p2_current.lower():
                state["match"]["p1"] = p2_current
                state["match"]["player1"] = p2_current
                state["match"]["p2"] = p1_current
                state["match"]["player2"] = p1_current

    payload_full = get_standings_payload()
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("MATCH_UPDATE", state["match"])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    full_state_payload = dict(state)
    full_state_payload["standings"] = payload_full
    await manager.broadcast("FULL_STATE", full_state_payload)
    return {"status": "success", "standings": payload_full, "match": state["match"], "queue": state["queue"]}

@app.post("/api/win/undo")
async def undo_win(req: Request):
    check_and_perform_automatic_resets()
    data = await req.json()
    target_tag = data.get("tag", "").strip()
    tier_target = str(data.get("tier", "master")).lower()

    if not target_tag:
        return {"status": "error", "message": "Missing tag"}

    updated_player_obj = None

    def undo_in_list(list_data):
        nonlocal updated_player_obj
        for p in list_data:
            if p["tag"].lower() == target_tag.lower():
                current_wins = int(p.get("wins", "0")) - 1
                p["wins"] = str(max(0, current_wins))
                p["points"] = "0"
                w = int(p["wins"])
                if w >= 151: p["rank"] = "Platinum"
                elif w >= 101: p["rank"] = "Gold"
                elif w >= 51: p["rank"] = "Silver"
                elif w >= 1: p["rank"] = "Bronze"
                else: p["rank"] = "-"
                updated_player_obj = p
                break
        return list_data

    if tier_target == "daily":
        state["standings_daily"] = undo_in_list(state["standings_daily"])
        save_json_file(DAILY_FILE, state["standings_daily"])
    elif tier_target == "weekly":
        state["standings_weekly"] = undo_in_list(state["standings_weekly"])
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
    elif tier_target == "monthly":
        state["standings_monthly"] = undo_in_list(state["standings_monthly"])
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
    elif tier_target == "yearly":
        if not isinstance(state["standings_yearly"], dict):
            state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
        state["standings_yearly"]["ALL"] = undo_in_list(state["standings_yearly"]["ALL"])
        save_json_file(YEARLY_FILE, state["standings_yearly"])
    else:
        tier_target = "master"
        state["standings_master"] = undo_in_list(state["standings_master"])
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    if updated_player_obj:
        post_to_google_sheets({
            "action": "update",
            "tier": tier_target,
            "tag": updated_player_obj.get("tag"),
            "platform": updated_player_obj.get("platform", "Twitch"),
            "wins": int(updated_player_obj.get("wins", 0))
        })

    payload_full = get_standings_payload()
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    full_state_payload = dict(state)
    full_state_payload["standings"] = payload_full
    await manager.broadcast("FULL_STATE", full_state_payload)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/edit")
async def edit_player_tag(req: Request):
    data = await req.json()
    old_tag = data.get("old_tag", "").strip()
    new_tag = data.get("new_tag", "").strip()
    new_platform = data.get("platform", "").strip()
    tier_target = str(data.get("tier", "master")).lower()

    if not old_tag:
        return {"status": "error", "message": "Missing tag parameter"}
    if not new_tag:
        new_tag = old_tag

    edited_player_obj = None

    def edit_in_list(list_data):
        nonlocal edited_player_obj
        for p in list_data:
            if p["tag"].lower() == old_tag.lower():
                p["tag"] = new_tag
                if new_platform in ["Twitch", "TikTok", "YouTube"]:
                    p["platform"] = new_platform
                edited_player_obj = p
                break
        return list_data

    if tier_target == "daily":
        state["standings_daily"] = edit_in_list(state["standings_daily"])
        save_json_file(DAILY_FILE, state["standings_daily"])
    elif tier_target == "weekly":
        state["standings_weekly"] = edit_in_list(state["standings_weekly"])
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
    elif tier_target == "monthly":
        state["standings_monthly"] = edit_in_list(state["standings_monthly"])
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
    elif tier_target == "yearly":
        if not isinstance(state["standings_yearly"], dict):
            state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
        state["standings_yearly"]["ALL"] = edit_in_list(state["standings_yearly"]["ALL"])
        save_json_file(YEARLY_FILE, state["standings_yearly"])
    else:
        tier_target = "master"
        state["standings_master"] = edit_in_list(state["standings_master"])
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    if edited_player_obj:
        post_to_google_sheets({
            "action": "update",
            "tier": tier_target,
            "tag": edited_player_obj.get("tag"),
            "platform": edited_player_obj.get("platform", "Twitch"),
            "wins": int(edited_player_obj.get("wins", 0))
        })

    payload_full = get_standings_payload()
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    full_state_payload = dict(state)
    full_state_payload["standings"] = payload_full
    await manager.broadcast("FULL_STATE", full_state_payload)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/delete")
async def delete_player_tag(req: Request):
    data = await req.json()
    tag = data.get("tag", "").strip()
    tier_target = str(data.get("tier", "master")).lower()

    if not tag:
        return {"status": "error", "message": "Missing tag parameter"}

    if tier_target == "daily":
        state["standings_daily"] = [p for p in state["standings_daily"] if p["tag"].lower() != tag.lower()]
        save_json_file(DAILY_FILE, state["standings_daily"])
    elif tier_target == "weekly":
        state["standings_weekly"] = [p for p in state["standings_weekly"] if p["tag"].lower() != tag.lower()]
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
    elif tier_target == "monthly":
        state["standings_monthly"] = [p for p in state["standings_monthly"] if p["tag"].lower() != tag.lower()]
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
    elif tier_target == "yearly":
        if not isinstance(state["standings_yearly"], dict):
            state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
        state["standings_yearly"]["ALL"] = [p for p in state["standings_yearly"]["ALL"] if p["tag"].lower() != tag.lower()]
        save_json_file(YEARLY_FILE, state["standings_yearly"])
    else:
        tier_target = "master"
        state["standings_master"] = [p for p in state["standings_master"] if p["tag"].lower() != tag.lower()]
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    post_to_google_sheets({
        "action": "delete",
        "tier": tier_target,
        "tag": tag
    })

    payload_full = get_standings_payload()
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    full_state_payload = dict(state)
    full_state_payload["standings"] = payload_full
    await manager.broadcast("FULL_STATE", full_state_payload)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/reset")
async def reset_standings(req: Request = None):
    scope = "daily"
    try:
        if req:
            body = await req.json()
            scope = str(body.get("scope", "daily")).lower()
    except Exception:
        pass

    def zero_out_list(list_data):
        for player in list_data:
            player["wins"] = "0"
            player["points"] = "0"
            player["rank"] = "-"
        return list_data

    if scope == "all":
        state["standings_daily"] = zero_out_list(state["standings_daily"])
        state["standings_weekly"] = zero_out_list(state["standings_weekly"])
        state["standings_monthly"] = zero_out_list(state["standings_monthly"])
        if isinstance(state["standings_yearly"], dict):
            # Only zero out the 'ALL' cumulative yearly list, preserve historical archive monthly keys (e.g. yearly_2026_AUG)
            for k in list(state["standings_yearly"].keys()):
                if k == "ALL":
                    zero_out_list(state["standings_yearly"][k])
        else:
            state["standings_yearly"] = {"ALL": zero_out_list(list(DEFAULT_STANDINGS))}
        state["standings_master"] = zero_out_list(state["standings_master"])
        state["standings"] = state["standings_master"]
        
        save_json_file(DAILY_FILE, state["standings_daily"])
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
        save_json_file(YEARLY_FILE, state["standings_yearly"])
        save_json_file(MASTER_FILE, state["standings_master"])

        for t in ["daily", "weekly", "monthly", "yearly", "master"]:
            post_to_google_sheets({"action": "reset", "tier": t})
    else:
        if scope == "daily":
            zero_out_list(state["standings_daily"])
            save_json_file(DAILY_FILE, state["standings_daily"])
        elif scope == "weekly":
            zero_out_list(state["standings_weekly"])
            save_json_file(WEEKLY_FILE, state["standings_weekly"])
        elif scope == "monthly":
            # Before resetting monthly, archive current monthly stats into the previous month's historical key if available
            now = datetime.now()
            prev_month_str = now.strftime("%Y-%m")
            parts = prev_month_str.split("-")
            if len(parts) == 2:
                y_str, m_str = parts
                # Calculate previous month for safety snapshot
                m_int = int(m_str) - 1
                if m_int < 1: 
                    m_int = 12
                    y_str = str(int(y_str) - 1)
                m_code = {"1": "JAN", "2": "FEB", "3": "MAR", "4": "APR", "5": "MAY", "6": "JUN", "7": "JUL", "8": "AUG", "9": "SEP", "10": "OCT", "11": "NOV", "12": "DEC"}.get(str(m_int), "SEP")
                archive_key = f"yearly_{y_str}_{m_code}"
                if not isinstance(state["standings_yearly"], dict):
                    state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
                state["standings_yearly"][archive_key] = list(state["standings_monthly"])

            zero_out_list(state["standings_monthly"])
            save_json_file(MONTHLY_FILE, state["standings_monthly"])
            save_json_file(YEARLY_FILE, state["standings_yearly"])
        elif scope == "yearly":
            if not isinstance(state["standings_yearly"], dict):
                state["standings_yearly"] = {"ALL": list(DEFAULT_STANDINGS)}
            zero_out_list(state["standings_yearly"].get("ALL", []))
            save_json_file(YEARLY_FILE, state["standings_yearly"])
        else:
            scope = "master"
            zero_out_list(state["standings_master"])
            state["standings"] = state["standings_master"]
            save_json_file(MASTER_FILE, state["standings_master"])

        post_to_google_sheets({"action": "reset", "tier": scope})

    payload_full = get_standings_payload()
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    full_state_payload = dict(state)
    full_state_payload["standings"] = payload_full
    await manager.broadcast("FULL_STATE", full_state_payload)
    return {"status": "success", "scope": scope, "standings": payload_full}

@app.get("/api/banner")
async def get_banner():
    return state["banner"]

@app.post("/api/banner")
async def post_banner(req: Request):
    data = await req.json()
    state["banner"].update(data)
    await manager.broadcast("BANNER_UPDATE", state["banner"])
    full_state_payload = dict(state)
    full_state_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_state_payload)
    return state["banner"]

@app.get("/api/cocommentator")
async def get_cocommentator():
    return state["cocommentator"]

@app.post("/api/cocommentator")
async def post_cocommentator(req: Request):
    data = await req.json()
    state["cocommentator"].update(data)
    await manager.broadcast("COMMENTATOR_UPDATE", state["cocommentator"])
    full_state_payload = dict(state)
    full_state_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_state_payload)
    return state["cocommentator"]

@app.get("/api/charity")
async def get_charity():
    return state["charity"]

@app.post("/api/charity")
async def post_charity(req: Request):
    data = await req.json()
    if "raised" in data:
        try:
            state["charity"]["raised"] = float(data["raised"])
        except (ValueError, TypeError):
            pass
    if "goal" in data:
        try:
            state["charity"]["goal"] = float(data["goal"])
        except (ValueError, TypeError):
            pass

    await manager.broadcast("CHARITY_UPDATE", state["charity"])
    full_state_payload = dict(state)
    full_state_payload["standings"] = get_standings_payload()
    await manager.broadcast("FULL_STATE", full_state_payload)
    return state["charity"]

@app.get("/standings.html")
async def serve_standings():
    file_path = os.path.join(SCRIPT_DIR, "standings.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "standings.html file not found"}

@app.get("/goombaa_charity_progress.html")
async def serve_charity_overlay():
    file_path = os.path.join(SCRIPT_DIR, "goombaa_charity_progress.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "goombaa_charity_progress.html file not found"}

@app.get("/overlay_horizontal.html")
async def serve_horizontal_overlay():
    file_path = os.path.join(SCRIPT_DIR, "overlay_horizontal.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "overlay_horizontal.html file not found"}

@app.get("/overlay_horizontal_v2.html")
async def serve_horizontal_overlay_v2():
    file_path = os.path.join(SCRIPT_DIR, "overlay_horizontal_v2.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "overlay_horizontal_v2.html file not found"}

@app.get("/dock_charity.html")
async def serve_dock_charity():
    file_path = os.path.join(SCRIPT_DIR, "dock_charity.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "dock_charity.html file not found"}

@app.get("/dock_match.html")
async def serve_dock_match():
    file_path = os.path.join(SCRIPT_DIR, "dock_match.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"app": "dock_match.html file not found"}

@app.get("/dock_broadcast.html")
async def serve_dock_broadcast():
    file_path = os.path.join(SCRIPT_DIR, "dock_broadcast.html")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "dock_broadcast.html file not found"}

app.mount("/", StaticFiles(directory=SCRIPT_DIR, html=True), name="static")

if __name__ == "__main__":
    print("[Goombaa Control Center] Running on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
