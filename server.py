"""
Goombaa Control Center - Backend Web Server
File: server.py
Description: FastAPI backend supporting Daily, Weekly, Monthly, and Master standings tiers, match management, queue, broadcast banners, co-hosts, charity goals, and full OBS dock routing.
"""

import os
import sys
import json
import csv
import io
import urllib.request
import asyncio
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
MASTER_FILE = os.path.join(SCRIPT_DIR, "standings.json")

DELETED_CACHE_FILE = os.path.join(SCRIPT_DIR, "deleted_tags.json")
SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1RWCtWhZ0f0paF7633Re8DyZiYgS9_tk-bP1fPnSd1E8/export?format=csv"
APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbys0_Xn7_xWLlIPMM5Dq99visJ7DcMlfDohkDv9nZR0Sn4E2ueWqwhyC41Aifb18enN_Q/exec"

DEFAULT_STANDINGS = [
    {"tag": "Goombaa", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "PhantomOrphan", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Alec", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Royal", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Someguy", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Brandy", "platform": "TikTok", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Jonathan", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Liam", "platform": "TikTok", "wins": "0", "points": "0", "rank": "-"},
    {"tag": "Not A Saint", "platform": "Twitch", "wins": "0", "points": "0", "rank": "-"}
]

def load_deleted_tags() -> set:
    if os.path.exists(DELETED_CACHE_FILE):
        try:
            with open(DELETED_CACHE_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(t.lower() for t in data)
        except Exception:
            pass
    return set()

def save_deleted_tags(deleted_set: set):
    try:
        with open(DELETED_CACHE_FILE, "w") as f:
            json.dump(list(deleted_set), f)
    except Exception as e:
        print(f"Error saving deleted tags cache: {e}")

def fetch_master_from_sheet() -> List[Dict[str, Any]]:
    deleted_tags = load_deleted_tags()
    try:
        req = urllib.request.Request(SHEET_CSV_URL, headers={'Cache-Control': 'no-cache', 'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            csv_data = response.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(csv_data))
            
            formatted_standings = []
            for row in reader:
                tag = str(row.get("Gamertag") or row.get("tag") or "").strip()
                if not tag or tag.lower() in deleted_tags:
                    continue

                platform = str(row.get("Platform") or row.get("platform") or "Twitch").strip()
                wins = str(row.get("Wins") or row.get("wins") or "0").strip()
                
                w = int(wins) if wins.isdigit() else 0
                if w >= 151: r_tier = "Platinum"
                elif w >= 101: r_tier = "Gold"
                elif w >= 51: r_tier = "Silver"
                elif w >= 1: r_tier = "Bronze"
                else: r_tier = "-"

                formatted_standings.append({
                    "tag": tag,
                    "platform": platform,
                    "wins": str(w),
                    "points": "0",
                    "rank": r_tier
                })
            if formatted_standings:
                return formatted_standings
    except Exception as e:
        print(f"Warning: Could not fetch Google Sheet CSV ({e}).")
    return DEFAULT_STANDINGS

def load_json_file(filepath: str, fallback_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    return data
        except Exception:
            pass
    return fallback_data

def save_json_file(filepath: str, data: List[Dict[str, Any]]):
    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error saving {filepath}: {e}")

def get_all_standings(force_sheet_sync: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    if force_sheet_sync or not os.path.exists(MASTER_FILE):
        master_data = fetch_master_from_sheet()
        save_json_file(MASTER_FILE, master_data)
    else:
        master_data = load_json_file(MASTER_FILE, DEFAULT_STANDINGS)
    
    daily_data = load_json_file(DAILY_FILE, [])
    weekly_data = load_json_file(WEEKLY_FILE, [])
    monthly_data = load_json_file(MONTHLY_FILE, [])
    
    return {
        "daily": daily_data,
        "weekly": weekly_data,
        "monthly": monthly_data,
        "master": master_data
    }

def sync_to_google_sheet(payload: dict):
    try:
        data_bytes = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            APPS_SCRIPT_URL,
            data=data_bytes,
            headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            pass
    except Exception as e:
        print(f"Warning: Could not sync change to Google Sheet via Apps Script ({e})")

app = FastAPI(title="Goombaa Stream Control Center")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

initial_standings = get_all_standings(force_sheet_sync=True)

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
    "queue": [],
    "banner": {
        "active": False, "visible": False, "header": "NEXT HOUR", "text": "NEXT HOUR", "message": "", "subtext": ""
    },
    "cocommentator": {
        "active": False, "host": "goombaa1977", "cohost": "", "name": ""
    },
    "charity": {
        "raised": 20.0, "goal": 100.0
    },
    "standings": initial_standings["master"],
    "standings_daily": initial_standings["daily"],
    "standings_weekly": initial_standings["weekly"],
    "standings_monthly": initial_standings["monthly"],
    "standings_master": initial_standings["master"]
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        await websocket.send_text(json.dumps({"type": "FULL_STATE", "data": state}))

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
    await manager.broadcast("FULL_STATE", state)
    return state["match"]

@app.get("/api/queue")
async def get_queue():
    return state["queue"]

@app.post("/api/queue")
async def set_queue(req: Request):
    data = await req.json()
    if isinstance(data, list):
        state["queue"] = data
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    await manager.broadcast("FULL_STATE", state)
    return state["queue"]

@app.post("/api/queue/clear")
async def clear_queue():
    state["queue"] = []
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    await manager.broadcast("FULL_STATE", state)
    return []

@app.get("/api/queue/next_match")
@app.post("/api/queue/next_match")
async def next_match(winner: str = "p1"):
    if len(state["queue"]) > 0:
        next_player = state["queue"].pop(0)
        if winner == "p1":
            state["match"]["p2"] = next_player
            state["match"]["player2"] = next_player
        else:
            state["match"]["p1"] = next_player
            state["match"]["player1"] = next_player
    await manager.broadcast("MATCH_UPDATE", state["match"])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "match": state["match"], "queue": state["queue"]}

@app.get("/api/standings")
async def get_standings():
    all_s = get_all_standings()
    return {
        "daily": all_s["daily"],
        "weekly": all_s["weekly"],
        "monthly": all_s["monthly"],
        "master": all_s["master"]
    }

def update_wins_in_list(list_data: List[Dict[str, Any]], tag: str, amount: int, auto_add: bool) -> tuple:
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
    if not found_player and auto_add:
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
    data = await req.json()
    tag = data.get("tag", "").strip()
    amount = int(data.get("amount", 1))
    auto_add = data.get("auto_add", True)

    if not tag or tag in ["Player 1", "Player 2"]:
        return {"status": "ignored"}

    all_s = get_all_standings()

    daily_list, _ = update_wins_in_list(all_s["daily"], tag, amount, auto_add)
    weekly_list, _ = update_wins_in_list(all_s["weekly"], tag, amount, auto_add)
    monthly_list, _ = update_wins_in_list(all_s["monthly"], tag, amount, auto_add)
    master_list, p_master = update_wins_in_list(all_s["master"], tag, amount, auto_add)

    save_json_file(DAILY_FILE, daily_list)
    save_json_file(WEEKLY_FILE, weekly_list)
    save_json_file(MONTHLY_FILE, monthly_list)
    save_json_file(MASTER_FILE, master_list)

    state["standings_daily"] = daily_list
    state["standings_weekly"] = weekly_list
    state["standings_monthly"] = monthly_list
    state["standings_master"] = master_list
    state["standings"] = master_list

    deleted_tags = load_deleted_tags()
    if tag.lower() in deleted_tags:
        deleted_tags.remove(tag.lower())
        save_deleted_tags(deleted_tags)

    if p_master:
        sync_to_google_sheet({
            "action": "update",
            "tag": p_master["tag"],
            "platform": p_master["platform"],
            "wins": p_master["wins"],
            "rank": p_master["rank"]
        })

    payload_full = {
        "daily": daily_list,
        "weekly": weekly_list,
        "monthly": monthly_list,
        "master": master_list
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "standings": payload_full}

@app.post("/api/win/undo")
async def undo_win(req: Request):
    data = await req.json()
    target_tag = data.get("tag", "").strip()
    p1 = data.get("p1", "").strip()
    p2 = data.get("p2", "").strip()

    tags_to_undo = []
    if target_tag:
        tags_to_undo.append(target_tag.lower())
    else:
        if p1 and p1 not in ["Player 1", "Player 2"]: tags_to_undo.append(p1.lower())
        if p2 and p2 not in ["Player 2", "Player 1"]: tags_to_undo.append(p2.lower())

    all_s = get_all_standings()

    def undo_in_list(list_data):
        updated = []
        for p in list_data:
            if p["tag"].lower() in tags_to_undo:
                current_wins = int(p.get("wins", "0")) - 1
                p["wins"] = str(max(0, current_wins))
                p["points"] = "0"
                w = int(p["wins"])
                if w >= 151: p["rank"] = "Platinum"
                elif w >= 101: p["rank"] = "Gold"
                elif w >= 51: p["rank"] = "Silver"
                elif w >= 1: p["rank"] = "Bronze"
                else: p["rank"] = "-"
                updated.append(p)
        return list_data, updated

    daily_list, _ = undo_in_list(all_s["daily"])
    weekly_list, _ = undo_in_list(all_s["weekly"])
    monthly_list, _ = undo_in_list(all_s["monthly"])
    master_list, u_master = undo_in_list(all_s["master"])

    save_json_file(DAILY_FILE, daily_list)
    save_json_file(WEEKLY_FILE, weekly_list)
    save_json_file(MONTHLY_FILE, monthly_list)
    save_json_file(MASTER_FILE, master_list)

    state["standings_daily"] = daily_list
    state["standings_weekly"] = weekly_list
    state["standings_monthly"] = monthly_list
    state["standings_master"] = master_list
    state["standings"] = master_list

    for p in u_master:
        sync_to_google_sheet({
            "action": "update",
            "tag": p["tag"],
            "platform": p["platform"],
            "wins": p["wins"],
            "rank": p["rank"]
        })

    payload_full = {
        "daily": daily_list,
        "weekly": weekly_list,
        "monthly": monthly_list,
        "master": master_list
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/edit")
async def edit_player_tag(req: Request):
    data = await req.json()
    old_tag = data.get("old_tag", "").strip()
    new_tag = data.get("new_tag", "").strip()
    new_platform = data.get("platform", "").strip()

    if not old_tag:
        return {"status": "error", "message": "Missing tag parameter"}
    
    if not new_tag:
        new_tag = old_tag

    deleted_tags = load_deleted_tags()
    if new_tag.lower() != old_tag.lower():
        deleted_tags.add(old_tag.lower())
        save_deleted_tags(deleted_tags)

    all_s = get_all_standings()

    def edit_in_list(list_data):
        target = None
        for p in list_data:
            if p["tag"].lower() == old_tag.lower():
                p["tag"] = new_tag
                if new_platform in ["Twitch", "TikTok", "YouTube"]:
                    p["platform"] = new_platform
                target = p
                break
        return list_data, target

    daily_list, _ = edit_in_list(all_s["daily"])
    weekly_list, _ = edit_in_list(all_s["weekly"])
    monthly_list, _ = edit_in_list(all_s["monthly"])
    master_list, t_master = edit_in_list(all_s["master"])

    save_json_file(DAILY_FILE, daily_list)
    save_json_file(WEEKLY_FILE, weekly_list)
    save_json_file(MONTHLY_FILE, monthly_list)
    save_json_file(MASTER_FILE, master_list)

    state["standings_daily"] = daily_list
    state["standings_weekly"] = weekly_list
    state["standings_monthly"] = monthly_list
    state["standings_master"] = master_list
    state["standings"] = master_list

    if t_master:
        if new_tag.lower() != old_tag.lower():
            sync_to_google_sheet({"action": "delete", "tag": old_tag})
        sync_to_google_sheet({
            "action": "update",
            "tag": t_master["tag"],
            "platform": t_master["platform"],
            "wins": t_master["wins"],
            "rank": t_master["rank"]
        })

    payload_full = {
        "daily": daily_list,
        "weekly": weekly_list,
        "monthly": monthly_list,
        "master": master_list
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/delete")
async def delete_player_tag(req: Request):
    data = await req.json()
    tag = data.get("tag", "").strip()

    if not tag:
        return {"status": "error", "message": "Missing tag parameter"}

    deleted_tags = load_deleted_tags()
    deleted_tags.add(tag.lower())
    save_deleted_tags(deleted_tags)

    all_s = get_all_standings()

    daily_list = [p for p in all_s["daily"] if p["tag"].lower() != tag.lower()]
    weekly_list = [p for p in all_s["weekly"] if p["tag"].lower() != tag.lower()]
    monthly_list = [p for p in all_s["monthly"] if p["tag"].lower() != tag.lower()]
    master_list = [p for p in all_s["master"] if p["tag"].lower() != tag.lower()]

    save_json_file(DAILY_FILE, daily_list)
    save_json_file(WEEKLY_FILE, weekly_list)
    save_json_file(MONTHLY_FILE, monthly_list)
    save_json_file(MASTER_FILE, master_list)

    state["standings_daily"] = daily_list
    state["standings_weekly"] = weekly_list
    state["standings_monthly"] = monthly_list
    state["standings_master"] = master_list
    state["standings"] = master_list

    sync_to_google_sheet({"action": "delete", "tag": tag})

    payload_full = {
        "daily": daily_list,
        "weekly": weekly_list,
        "monthly": monthly_list,
        "master": master_list
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/reset")
async def reset_standings(req: Request = None):
    scope = "daily"
    try:
        if req:
            body = await req.json()
            scope = body.get("scope", "daily")
    except Exception:
        pass

    all_s = get_all_standings()

    def zero_out_list(list_data, is_master=False):
        for player in list_data:
            player["wins"] = "0"
            player["points"] = "0"
            player["rank"] = "-"
            if is_master:
                sync_to_google_sheet({
                    "action": "update",
                    "tag": player["tag"],
                    "platform": player["platform"],
                    "wins": "0",
                    "rank": "-"
                })
        return list_data

    if scope == "all":
        all_s["daily"] = zero_out_list(all_s["daily"])
        all_s["weekly"] = zero_out_list(all_s["weekly"])
        all_s["monthly"] = zero_out_list(all_s["monthly"])
        all_s["master"] = zero_out_list(all_s["master"], is_master=True)
        
        save_json_file(DAILY_FILE, all_s["daily"])
        save_json_file(WEEKLY_FILE, all_s["weekly"])
        save_json_file(MONTHLY_FILE, all_s["monthly"])
        save_json_file(MASTER_FILE, all_s["master"])
    else:
        if scope == "daily":
            target_list = all_s["daily"]
            filepath = DAILY_FILE
            state_key = "standings_daily"
            is_m = False
        elif scope == "weekly":
            target_list = all_s["weekly"]
            filepath = WEEKLY_FILE
            state_key = "standings_weekly"
            is_m = False
        elif scope == "monthly":
            target_list = all_s["monthly"]
            filepath = MONTHLY_FILE
            state_key = "standings_monthly"
            is_m = False
        else:
            target_list = all_s["master"]
            filepath = MASTER_FILE
            state_key = "standings_master"
            is_m = True

        zero_out_list(target_list, is_master=is_m)
        save_json_file(filepath, target_list)
        state[state_key] = target_list

    payload_full = {
        "daily": all_s["daily"],
        "weekly": all_s["weekly"],
        "monthly": all_s["monthly"],
        "master": all_s["master"]
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "scope": scope, "standings": payload_full}

@app.get("/api/banner")
async def get_banner():
    return state["banner"]

@app.post("/api/banner")
async def post_banner(req: Request):
    data = await req.json()
    state["banner"].update(data)
    await manager.broadcast("BANNER_UPDATE", state["banner"])
    await manager.broadcast("FULL_STATE", state)
    return state["banner"]

@app.get("/api/cocommentator")
async def get_cocommentator():
    return state["cocommentator"]

@app.post("/api/cocommentator")
async def post_cocommentator(req: Request):
    data = await req.json()
    state["cocommentator"].update(data)
    await manager.broadcast("COMMENTATOR_UPDATE", state["cocommentator"])
    await manager.broadcast("FULL_STATE", state)
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
    await manager.broadcast("FULL_STATE", state)
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
    return {"error": "dock_match.html file not found"}

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
