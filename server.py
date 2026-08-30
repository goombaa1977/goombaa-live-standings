"""
Goombaa Control Center - Backend Web Server
File: server.py
Description: Lightning-fast local-first FastAPI backend for instant stream controls.
"""

import os
import sys
import json
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
QUEUE_FILE = os.path.join(SCRIPT_DIR, "queue_cache.json")
DELETED_CACHE_FILE = os.path.join(SCRIPT_DIR, "deleted_tags.json")

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

def load_json_file(filepath: str, fallback_data: Any) -> Any:
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if data:
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
initial_master = load_json_file(MASTER_FILE, list(DEFAULT_STANDINGS))
initial_queue = load_json_file(QUEUE_FILE, [])

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
    "standings_master": initial_master
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
        save_json_file(QUEUE_FILE, state["queue"])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    await manager.broadcast("FULL_STATE", state)
    return state["queue"]

@app.post("/api/queue/clear")
async def clear_queue():
    state["queue"] = []
    save_json_file(QUEUE_FILE, [])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    await manager.broadcast("FULL_STATE", state)
    return []

@app.get("/api/queue/next_match")
@app.post("/api/queue/next_match")
async def next_match(req: Request = None):
    winner = "p1"
    try:
        if req:
            body = await req.json()
            winner = str(body.get("winner", "p1"))
    except Exception:
        pass

    if len(state["queue"]) > 0:
        next_player = state["queue"].pop(0)
        if winner.lower() == "p2":
            current_loser = state["match"].get("p1") or state["match"].get("player1")
            state["match"]["p1"] = next_player
            state["match"]["player1"] = next_player
        else:
            current_loser = state["match"].get("p2") or state["match"].get("player2")
            state["match"]["p2"] = next_player
            state["match"]["player2"] = next_player

        if current_loser and current_loser not in ["Player 1", "Player 2"] and current_loser not in state["queue"]:
            state["queue"].append(current_loser)

        save_json_file(QUEUE_FILE, state["queue"])

    await manager.broadcast("MATCH_UPDATE", state["match"])
    await manager.broadcast("QUEUE_UPDATE", state["queue"])
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "match": state["match"], "queue": state["queue"]}

@app.get("/api/standings")
async def get_standings():
    return {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
    }

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
    data = await req.json()
    tag = data.get("tag", "").strip()
    amount = int(data.get("amount", 1))
    tier_target = data.get("tier", "master").lower()

    if not tag or tag in ["Player 1", "Player 2"]:
        return {"status": "ignored"}

    if tier_target == "daily":
        state["standings_daily"], _ = update_wins_in_list(state["standings_daily"], tag, amount)
        save_json_file(DAILY_FILE, state["standings_daily"])
    elif tier_target == "weekly":
        state["standings_weekly"], _ = update_wins_in_list(state["standings_weekly"], tag, amount)
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
    elif tier_target == "monthly":
        state["standings_monthly"], _ = update_wins_in_list(state["standings_monthly"], tag, amount)
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
    else:
        state["standings_master"], _ = update_wins_in_list(state["standings_master"], tag, amount)
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    payload_full = {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "standings": payload_full}

@app.post("/api/win/undo")
async def undo_win(req: Request):
    data = await req.json()
    target_tag = data.get("tag", "").strip()
    tier_target = data.get("tier", "master").lower()

    if not target_tag:
        return {"status": "error", "message": "Missing tag"}

    def undo_in_list(list_data):
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
    else:
        state["standings_master"] = undo_in_list(state["standings_master"])
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    payload_full = {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
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
    tier_target = data.get("tier", "master").lower()

    if not old_tag:
        return {"status": "error", "message": "Missing tag parameter"}
    if not new_tag:
        new_tag = old_tag

    def edit_in_list(list_data):
        for p in list_data:
            if p["tag"].lower() == old_tag.lower():
                p["tag"] = new_tag
                if new_platform in ["Twitch", "TikTok", "YouTube"]:
                    p["platform"] = new_platform
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
    else:
        state["standings_master"] = edit_in_list(state["standings_master"])
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    payload_full = {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
    }
    await manager.broadcast("STANDINGS_UPDATE", payload_full)
    await manager.broadcast("FULL_STATE", state)
    return {"status": "success", "standings": payload_full}

@app.post("/api/standings/delete")
async def delete_player_tag(req: Request):
    data = await req.json()
    tag = data.get("tag", "").strip()
    tier_target = data.get("tier", "master").lower()

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
    else:
        state["standings_master"] = [p for p in state["standings_master"] if p["tag"].lower() != tag.lower()]
        state["standings"] = state["standings_master"]
        save_json_file(MASTER_FILE, state["standings_master"])

    payload_full = {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
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
        state["standings_master"] = zero_out_list(state["standings_master"])
        state["standings"] = state["standings_master"]
        
        save_json_file(DAILY_FILE, state["standings_daily"])
        save_json_file(WEEKLY_FILE, state["standings_weekly"])
        save_json_file(MONTHLY_FILE, state["standings_monthly"])
        save_json_file(MASTER_FILE, state["standings_master"])
    else:
        if scope == "daily":
            zero_out_list(state["standings_daily"])
            save_json_file(DAILY_FILE, state["standings_daily"])
        elif scope == "weekly":
            zero_out_list(state["standings_weekly"])
            save_json_file(WEEKLY_FILE, state["standings_weekly"])
        elif scope == "monthly":
            zero_out_list(state["standings_monthly"])
            save_json_file(MONTHLY_FILE, state["standings_monthly"])
        else:
            zero_out_list(state["standings_master"])
            state["standings"] = state["standings_master"]
            save_json_file(MASTER_FILE, state["standings_master"])

    payload_full = {
        "daily": state["standings_daily"],
        "weekly": state["standings_weekly"],
        "monthly": state["standings_monthly"],
        "master": state["standings_master"]
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
