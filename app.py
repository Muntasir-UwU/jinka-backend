"""
Jinka Backend — FastAPI + WebSocket  (v2 — with name system & sync conflict resolution)

Endpoints:
  GET  /          → Serves jinki.html
  WS   /ws/sync/  → Real-time sync channel
"""

import json
import logging
import os
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Jinka Sync Server")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("jinka")

# ── In-memory state ────────────────────────────────────────────────────────
connected:     Dict[str, WebSocket]    = {}   # user_id → live socket
sync_active:   Dict[str, bool]         = {}   # user_id → sync toggled on?
current_track: Dict[str, Optional[int]]= {}   # user_id → track index
current_song:  Dict[str, dict]         = {}   # user_id → {title, thumb}
display_names: Dict[str, str]          = {}   # user_id → human display name
join_order:    List[str]               = []   # insertion-ordered join list


# ── Helpers ────────────────────────────────────────────────────────────────

def get_partner(user_id: str) -> Optional[str]:
    for uid in connected:
        if uid != user_id:
            return uid
    return None


async def send(user_id: str, payload: dict) -> None:
    ws = connected.get(user_id)
    if ws is None:
        return
    try:
        await ws.send_text(json.dumps(payload))
    except Exception as exc:
        logger.warning("send failed to %s: %s", user_id, exc)


def _both_synced() -> bool:
    return (
        len(connected) >= 2
        and all(sync_active.get(uid, True) for uid in connected)
    )


async def broadcast_sync_state() -> None:
    bs = _both_synced()
    for uid in list(connected):
        await send(uid, {"type": "sync_state", "both_synced": bs})


async def resolve_song_conflict() -> None:
    """
    Called the moment both users become synced.
    The LAST person to join is authoritative — push their song to the first joiner.
    This prevents the oscillating-song bug.
    """
    connected_in_order = [u for u in join_order if u in connected]
    if len(connected_in_order) < 2:
        return

    authority = connected_in_order[-1]   # last joiner  → their song wins
    follower  = connected_in_order[0]    # first joiner → must follow

    auth_track = current_track.get(authority)
    auth_song  = current_song.get(authority, {})

    logger.info("RESOLVE  authority=%s  follower=%s  track=%s", authority, follower, auth_track)

    if auth_track is not None:
        await send(follower, {
            "type":         "song_change",
            "index":        auth_track,
            "title":        auth_song.get("title", ""),
            "thumb":        auth_song.get("thumb", ""),
            "autoplay":     False,   # load but don't auto-play
            "user":         authority,
            "sync_resolve": True,    # client flag: skip re-broadcast
        })


# ── HTTP ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    forwarded = request.headers.get("x-forwarded-proto", "")
    is_https  = forwarded == "https" or request.url.scheme == "https"
    ws_scheme = "wss" if is_https else "ws"
    host      = request.headers.get("host", "localhost:8000")
    ws_url    = f"{ws_scheme}://{host}"
    return templates.TemplateResponse(
        "jinki.html", {"request": request, "ws_url": ws_url}
    )


@app.get("/health")
async def health():
    return {
        "status":          "ok",
        "connected_users": len(connected),
        "users":           {uid: display_names.get(uid, uid) for uid in connected},
    }


# ── WebSocket ──────────────────────────────────────────────────────────────

@app.websocket("/ws/sync/")
async def ws_sync(websocket: WebSocket, user: str = ""):
    user_id = user.strip() or f"anon_{id(websocket)}"
    await websocket.accept()

    connected[user_id]   = websocket
    sync_active[user_id] = True
    if user_id not in join_order:
        join_order.append(user_id)

    logger.info("CONNECT  user=%s  total=%d", user_id, len(connected))

    try:
        partner = get_partner(user_id)

        if partner:
            partner_name = display_names.get(partner, partner)
            my_name      = display_names.get(user_id, user_id)

            # Tell new joiner their partner is online (with name)
            await send(user_id, {
                "type":   "presence",
                "status": "online",
                "user":   partner,
                "name":   partner_name,
            })
            # Tell existing user that new person joined (with name)
            await send(partner, {
                "type":   "presence",
                "status": "online",
                "user":   user_id,
                "name":   my_name,
            })
            # Push partner's current song label to new joiner
            if partner in current_song:
                song = current_song[partner]
                await send(user_id, {
                    "type":  "song",
                    "title": song.get("title", ""),
                    "thumb": song.get("thumb", ""),
                    "user":  partner,
                    "name":  partner_name,
                })

            await broadcast_sync_state()

            # If both are already synced on connect, resolve conflict immediately
            if _both_synced():
                await resolve_song_conflict()

        # ── message loop ──
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg["user"] = user_id
            await handle_message(user_id, msg)

    except WebSocketDisconnect:
        logger.info("DISCONNECT  user=%s", user_id)
    except Exception as exc:
        logger.error("ERROR  user=%s  %s", user_id, exc)
    finally:
        connected.pop(user_id, None)
        sync_active.pop(user_id, None)
        # keep display_names & join_order so re-connects are smooth
        # (clear join_order entry so re-join is treated as new last-joiner)
        if user_id in join_order:
            join_order.remove(user_id)

        partner = get_partner(user_id)
        if partner:
            await send(partner, {
                "type":   "presence",
                "status": "offline",
                "user":   user_id,
                "name":   display_names.get(user_id, user_id),
            })
            await send(partner, {"type": "sync_state", "both_synced": False})

        logger.info("CLEANED  user=%s  remaining=%d", user_id, len(connected))


# ── Message dispatcher ─────────────────────────────────────────────────────

async def handle_message(user_id: str, msg: dict) -> None:
    partner  = get_partner(user_id)
    msg_type = msg.get("type", "")

    # ── name registration ──────────────────────────────────────────────────
    if msg_type == "name":
        name = str(msg.get("name", "")).strip()[:32] or user_id
        display_names[user_id] = name
        logger.info("NAME  user=%s  name=%s", user_id, name)
        # Tell partner the real name
        if partner:
            await send(partner, {
                "type":   "name_update",
                "user":   user_id,
                "name":   name,
            })

    # ── presence ──────────────────────────────────────────────────────────
    elif msg_type == "presence":
        if partner:
            msg["name"] = display_names.get(user_id, user_id)
            await send(partner, msg)

    # ── song label update (no track change) ───────────────────────────────
    elif msg_type == "song":
        current_song[user_id] = {
            "title": msg.get("title", ""),
            "thumb": msg.get("thumb", ""),
        }
        if partner:
            msg["name"] = display_names.get(user_id, user_id)
            await send(partner, msg)

    # ── song_change (user picked a new track) ─────────────────────────────
    elif msg_type == "song_change":
        idx = msg.get("index")
        if isinstance(idx, (int, float)):
            current_track[user_id] = int(idx)
        current_song[user_id] = {
            "title": msg.get("title", ""),
            "thumb": msg.get("thumb", ""),
        }

        if not partner:
            return

        both = _both_synced()

        if both:
            # Both synced → partner follows
            msg["name"] = display_names.get(user_id, user_id)
            await send(partner, msg)
        else:
            # Sync off → only update partner's "now playing" label
            await send(partner, {
                "type":  "song",
                "title": msg.get("title", ""),
                "thumb": msg.get("thumb", ""),
                "user":  user_id,
                "name":  display_names.get(user_id, user_id),
            })

    # ── sync_toggle ────────────────────────────────────────────────────────
    elif msg_type == "sync_toggle":
        was_both = _both_synced()
        sync_active[user_id] = bool(msg.get("active", True))
        now_both = _both_synced()

        await broadcast_sync_state()

        # Transition into both-synced → resolve any song conflict ONCE
        if not was_both and now_both:
            await resolve_song_conflict()

    # ── sync_event (play / pause / seek) ──────────────────────────────────
    elif msg_type == "sync_event":
        if not partner:
            return
        if not (sync_active.get(user_id, True) and sync_active.get(partner, True)):
            return

        my_idx      = current_track.get(user_id)
        partner_idx = current_track.get(partner)

        if (
            my_idx is not None
            and partner_idx is not None
            and my_idx != partner_idx
        ):
            await send(user_id, {
                "type":    "error",
                "message": (
                    f"Tracks don't match "
                    f"(you: #{my_idx}, partner: #{partner_idx}). "
                    "Sync them first."
                ),
                "user": "__server__",
            })
            return

        await send(partner, msg)

    # ── default: forward ──────────────────────────────────────────────────
    else:
        if partner:
            await send(partner, msg)


# ── Entry ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
