"""
═══════════════════════════════════════════════════════════════════
  JINKA BACKEND  v4  —  Python / FastAPI WebSocket Server
  Drop-in replacement for the old Flask app.py

  Why FastAPI instead of Flask?
  Flask's WebSocket support requires extra hacks (gevent / eventlet).
  FastAPI has native async WebSocket support — no hacks needed,
  and it works perfectly on Render.

  Same URL structure as before:
    GET  /           → health check (UptimeRobot keep-alive)
    WS   /ws/sync/?user=<userId>
═══════════════════════════════════════════════════════════════════
"""

import asyncio
import json
import time
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
import uvicorn

# ── App ────────────────────────────────────────────────────────────
app = FastAPI(title="Jinka Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve your existing /static/ files (logo.png etc.) unchanged
app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Constants ──────────────────────────────────────────────────────
ROOM_ID           = "jinka_main"
HEARTBEAT_TICK_S  = 5          # how often we ping clients (seconds)
CLIENT_TIMEOUT_S  = 20         # evict if no pong for this long
MAX_USERS         = 2


# ── In-memory room state ───────────────────────────────────────────
"""
room = {
  "users": { userId: UserState },
  "track_idx":       int | None,
  "current_time":    float,
  "is_playing":      bool,
  "last_state_at":   float,   ← time.time() snapshot
  "last_updated_by": str | None,
  "seq_counter":     int,
}

UserState = {
  "ws":          WebSocket,
  "user_id":     str,
  "name":        str,
  "joined_at":   float,
  "last_pong":   float,
  "is_playing":  bool,
  "current_time":float,
  "track_idx":   int | None,
  "sync_active": bool,
  "status":      str,
}
"""

rooms: dict = {}


def get_room() -> dict:
    if ROOM_ID not in rooms:
        rooms[ROOM_ID] = {
            "users":          {},
            "track_idx":      None,
            "current_time":   0.0,
            "is_playing":     False,
            "last_state_at":  time.time(),
            "last_updated_by": None,
            "seq_counter":    0,
        }
    return rooms[ROOM_ID]


# ── Helpers ────────────────────────────────────────────────────────

async def safe_send(ws: WebSocket, payload: dict):
    """Send JSON to one client, swallowing any error."""
    try:
        await ws.send_text(json.dumps(payload))
    except Exception:
        pass


async def broadcast(room: dict, payload: dict, exclude_user_id: Optional[str] = None):
    """Send to all users except the excluded one."""
    for uid, user in list(room["users"].items()):
        if uid != exclude_user_id:
            await safe_send(user["ws"], payload)


async def broadcast_all(room: dict, payload: dict):
    """Send to every user including sender."""
    for user in list(room["users"].values()):
        await safe_send(user["ws"], payload)


def estimated_room_time(room: dict) -> float:
    """Extrapolate current playback position from last known state."""
    if not room["is_playing"]:
        return room["current_time"]
    elapsed = time.time() - room["last_state_at"]
    return room["current_time"] + elapsed


async def evaluate_sync_state(room: dict):
    """Compute whether both users are in sync and broadcast the result."""
    users = list(room["users"].values())
    if len(users) < 2:
        await broadcast_all(room, {"type": "sync_state", "both_synced": False})
        return

    a, b = users[0], users[1]
    both_online  = a["status"] == "online" and b["status"] == "online"
    both_playing = a["is_playing"] and b["is_playing"]
    same_song    = (
        a["track_idx"] is not None
        and a["track_idx"] == b["track_idx"]
    )
    both_synced = both_online and both_playing and same_song
    await broadcast_all(room, {"type": "sync_state", "both_synced": both_synced})


# ── Background heartbeat task ─────────────────────────────────────
async def heartbeat_loop():
    """
    Runs forever in the background.
    Every HEARTBEAT_TICK_S seconds:
      - Evicts users who haven't responded in CLIENT_TIMEOUT_S
      - Sends a ping to remaining users
    """
    while True:
        await asyncio.sleep(HEARTBEAT_TICK_S)
        room = get_room()
        now  = time.time()
        to_evict = []

        for uid, user in list(room["users"].items()):
            if now - user["last_pong"] > CLIENT_TIMEOUT_S:
                to_evict.append(uid)

        for uid in to_evict:
            user = room["users"].pop(uid, None)
            if user:
                try:
                    await user["ws"].close()
                except Exception:
                    pass
                print(f"[Jinka] Evicted timed-out user: {uid}")
                await broadcast(room, {
                    "type": "presence", "status": "offline", "user": uid
                }, exclude_user_id=uid)
                await evaluate_sync_state(room)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(heartbeat_loop())
    print("✦ Jinka backend v4 started")


# ── HTTP endpoints ─────────────────────────────────────────────────

@app.get("/")
async def health():
    """Health check — also used by UptimeRobot to keep Render warm."""
    room  = get_room()
    users = [
        {
            "id":        u["user_id"],
            "name":      u["name"],
            "status":    u["status"],
            "is_playing": u["is_playing"],
            "track_idx": u["track_idx"],
        }
        for u in room["users"].values()
    ]
    return JSONResponse({
        "status":     "ok",
        "room_size":  len(room["users"]),
        "track_idx":  room["track_idx"],
        "is_playing": room["is_playing"],
        "users":      users,
    })


# ── WebSocket endpoint ─────────────────────────────────────────────

@app.websocket("/ws/sync/")
async def ws_sync(websocket: WebSocket):
    await websocket.accept()

    # Parse ?user= from query string
    user_id = (websocket.query_params.get("user") or "anon")[:64]
    room    = get_room()

    # Reject if room is full and this is a new (non-reconnecting) user
    if user_id not in room["users"] and len(room["users"]) >= MAX_USERS:
        await safe_send(websocket, {
            "type": "error", "message": "Room is full (max 2 users)."
        })
        await websocket.close(code=4001)
        return

    print(f"[Jinka] User connected: {user_id}  (room size: {len(room['users']) + 1})")

    user_state = {
        "ws":          websocket,
        "user_id":     user_id,
        "name":        "",
        "joined_at":   time.time(),
        "last_pong":   time.time(),
        "is_playing":  False,
        "current_time": 0.0,
        "track_idx":   None,
        "sync_active": True,
        "status":      "online",
    }
    room["users"][user_id] = user_state

    try:
        while True:
            raw = await websocket.receive_text()
            user_state["last_pong"] = time.time()   # any message = alive

            try:
                msg = json.loads(raw)
            except Exception:
                continue

            # Stamp sender id so recipients can filter their own echoes
            msg["user"] = user_id

            msg_type = msg.get("type", "")

            # ── presence ────────────────────────────────────────────
            if msg_type == "presence":
                user_state["status"] = msg.get("status", "online")
                await broadcast(room, msg, exclude_user_id=user_id)
                await evaluate_sync_state(room)

            # ── display name ────────────────────────────────────────
            elif msg_type == "name":
                user_state["name"] = str(msg.get("name", ""))[:64]
                await broadcast(room, {
                    "type": "name_update",
                    "name": user_state["name"],
                    "user": user_id,
                }, exclude_user_id=user_id)

            # ── NTP clock sync ──────────────────────────────────────
            # Client sends t0 = Date.now(); we echo with t1 = server ms
            elif msg_type == "clock_sync":
                await safe_send(websocket, {
                    "type": "clock_sync_response",
                    "t0":   msg.get("t0"),
                    "t1":   int(time.time() * 1000),
                })

            # ── application-level ping ──────────────────────────────
            elif msg_type == "ping":
                user_state["last_pong"] = time.time()
                await safe_send(websocket, {"type": "pong", "t": msg.get("t")})

            # ── periodic heartbeat from client ──────────────────────
            elif msg_type == "heartbeat":
                user_state["is_playing"]   = bool(msg.get("isPlaying", False))
                user_state["current_time"] = float(msg.get("time", 0))
                if msg.get("trackIdx") is not None:
                    user_state["track_idx"] = msg["trackIdx"]

                await broadcast(room, {
                    "type":      "partner_heartbeat",
                    "time":      user_state["current_time"],
                    "isPlaying": user_state["is_playing"],
                    "trackIdx":  user_state["track_idx"],
                    "user":      user_id,
                }, exclude_user_id=user_id)

                await evaluate_sync_state(room)

            # ── song label update ───────────────────────────────────
            elif msg_type == "song":
                await broadcast(room, msg, exclude_user_id=user_id)

            # ── track change ────────────────────────────────────────
            elif msg_type == "song_change":
                if isinstance(msg.get("index"), int):
                    room["track_idx"] = msg["index"]
                room["current_time"]   = 0.0
                room["is_playing"]     = bool(msg.get("autoplay", True))
                room["last_state_at"]  = time.time()
                room["last_updated_by"] = user_id
                user_state["track_idx"] = room["track_idx"]
                await broadcast(room, msg, exclude_user_id=user_id)

            # ── playback event (play / pause / seek) ────────────────
            elif msg_type == "sync_event":
                if not user_state["sync_active"]:
                    continue

                room["seq_counter"] += 1
                msg["seq"] = room["seq_counter"]

                event = msg.get("event", "")
                t     = float(msg.get("time", 0))

                if event == "play":
                    room["is_playing"]   = True
                    room["current_time"] = t
                    room["last_state_at"] = time.time()
                    user_state["is_playing"]   = True
                    user_state["current_time"] = t

                elif event == "pause":
                    room["is_playing"]   = False
                    room["current_time"] = t
                    room["last_state_at"] = time.time()
                    user_state["is_playing"]   = False
                    user_state["current_time"] = t

                elif event == "seek":
                    room["current_time"]  = t
                    room["last_state_at"] = time.time()
                    user_state["current_time"] = t

                await broadcast(room, msg, exclude_user_id=user_id)
                await evaluate_sync_state(room)

            # ── sync handshake ──────────────────────────────────────
            elif msg_type == "sync_request":
                await broadcast(room, msg, exclude_user_id=user_id)

            elif msg_type == "sync_response":
                await broadcast(room, msg, exclude_user_id=user_id)

            # ── sync toggle ─────────────────────────────────────────
            elif msg_type == "sync_toggle":
                user_state["sync_active"] = bool(msg.get("active", True))
                await broadcast(room, msg, exclude_user_id=user_id)

            # ── room state request (on reconnect / new join) ────────
            elif msg_type == "state_request":
                await safe_send(websocket, {
                    "type":        "room_state",
                    "trackIdx":    room["track_idx"],
                    "currentTime": estimated_room_time(room),
                    "isPlaying":   room["is_playing"],
                    "lastStateAt": int(room["last_state_at"] * 1000),
                })
                # Also push existing users' presence
                for uid, u in room["users"].items():
                    if uid != user_id:
                        await safe_send(websocket, {
                            "type":   "presence",
                            "status": u["status"],
                            "name":   u["name"],
                            "user":   uid,
                        })

            # ── live emoji reaction ─────────────────────────────────
            elif msg_type == "reaction":
                emoji = str(msg.get("emoji", ""))
                if 0 < len(emoji) <= 8:
                    await broadcast(room, msg, exclude_user_id=user_id)

            # ── queue activity (browsing) ───────────────────────────
            elif msg_type == "queue_activity":
                await broadcast(room, msg, exclude_user_id=user_id)

            # ── name_update forward ─────────────────────────────────
            elif msg_type == "name_update":
                await broadcast(room, msg, exclude_user_id=user_id)

            # ── unknown — relay as-is ───────────────────────────────
            else:
                await broadcast(room, msg, exclude_user_id=user_id)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[Jinka] Error for {user_id}: {e}")
    finally:
        room["users"].pop(user_id, None)
        print(f"[Jinka] User disconnected: {user_id}  (room size: {len(room['users'])})")
        await broadcast(room, {
            "type": "presence", "status": "offline", "user": user_id
        }, exclude_user_id=user_id)
        await evaluate_sync_state(room)


# ── Local dev entry point ──────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
