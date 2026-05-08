#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║              Jinka Backend  —  Debug Test Suite  v2          ║
║                                                              ║
║  Usage:                                                      ║
║    python debug.py                          # local (8000)   ║
║    python debug.py https://your.render.app  # deployed URL   ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import sys
import time
import urllib.request
from typing import List, Optional

try:
    import websockets
except ImportError:
    print("\n  ✗  'websockets' not installed.  Run:  pip install websockets\n")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────
BASE_URL = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8000"

if BASE_URL.startswith("https://"):
    WS_BASE = BASE_URL.replace("https://", "wss://")
elif BASE_URL.startswith("http://"):
    WS_BASE = BASE_URL.replace("http://", "ws://")
else:
    WS_BASE = f"ws://{BASE_URL}"

IS_REMOTE   = "localhost" not in BASE_URL and "127.0.0.1" not in BASE_URL
MSG_TIMEOUT = 6.0 if IS_REMOTE else 2.5
DRAIN_WAIT  = 1.5 if IS_REMOTE else 0.4
STEP_PAUSE  = 0.6 if IS_REMOTE else 0.15

PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
INFO = "\033[34m→\033[0m"
WARN = "\033[33m⚠\033[0m"

results: List[tuple] = []


def record(name: str, passed: bool, detail: str = "") -> None:
    results.append((name, passed, detail))
    icon = PASS if passed else FAIL
    suffix = f"  [{detail}]" if detail else ""
    print(f"  {icon}  {name}{suffix}")


async def recv_one(ws, timeout: float = None) -> Optional[dict]:
    t = timeout or MSG_TIMEOUT
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=t)
        return json.loads(raw)
    except Exception:
        return None


async def drain(ws, pause: float = None, max_msgs: int = 12) -> List[dict]:
    """Wait `pause` seconds then greedily flush the socket."""
    p = pause if pause is not None else DRAIN_WAIT
    if p > 0:
        await asyncio.sleep(p)
    collected = []
    for _ in range(max_msgs):
        m = await recv_one(ws, timeout=0.4)
        if m is None:
            break
        collected.append(m)
    return collected


async def collect_type(ws, want_type: str, timeout: float = None) -> Optional[dict]:
    """Read messages until one with the desired type arrives."""
    t = timeout or MSG_TIMEOUT
    deadline = asyncio.get_event_loop().time() + t
    while asyncio.get_event_loop().time() < deadline:
        remaining = deadline - asyncio.get_event_loop().time()
        m = await recv_one(ws, timeout=min(remaining, 1.5))
        if m is None:
            continue
        if m.get("type") == want_type:
            return m
    return None


def find(msgs, **kw):
    return next((m for m in msgs if all(m.get(k) == v for k, v in kw.items())), None)


# ── Test 1: HTTP health ──────────────────────────────────────────────────────
def test_http_health() -> None:
    print(f"\n{INFO} Test 1 — HTTP /health")
    url = f"{BASE_URL}/health"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            body = json.loads(r.read())
        record("Server is reachable", True, f"status={body.get('status')}")
    except Exception as exc:
        record("Server is reachable", False, str(exc))
        print(f"\n  {WARN}  Cannot reach {url} — is the server running?\n")
        sys.exit(1)


# ── WebSocket tests ──────────────────────────────────────────────────────────
async def run_ws_tests() -> None:
    user_a = "debug_user_A"
    user_b = "debug_user_B"

    # ── Test 2: Connect ──────────────────────────────────────────────────────
    print(f"\n{INFO} Test 2 — WebSocket Connect")
    try:
        ws_a = await websockets.connect(f"{WS_BASE}/ws/sync/?user={user_a}")
        record("User A connects", True)
        await asyncio.sleep(STEP_PAUSE)
        ws_b = await websockets.connect(f"{WS_BASE}/ws/sync/?user={user_b}")
        record("User B connects", True)
    except Exception as exc:
        record("WebSocket connect", False, str(exc))
        return

    # Let server process both joins
    await asyncio.sleep(DRAIN_WAIT)

    # ── Test 3: Presence on join ─────────────────────────────────────────────
    print(f"\n{INFO} Test 3 — Presence on Join")
    msgs_a = await drain(ws_a, pause=0)
    msgs_b = await drain(ws_b, pause=0)

    a_got = find(msgs_a, type="presence", status="online")
    b_got = find(msgs_b, type="presence", status="online")
    record("A notified of B joining", a_got is not None, str(a_got))
    record("B notified of A joining", b_got is not None, str(b_got))

    # ── Test 4: Song label ───────────────────────────────────────────────────
    print(f"\n{INFO} Test 4 — Partner Song Label")
    await ws_a.send(json.dumps({
        "type": "song",
        "title": "Tumi Robe Nirobe",
        "thumb": "https://example.com/thumb.jpg",
    }))
    msg = await collect_type(ws_b, "song")
    record(
        "B sees A's song label",
        msg is not None and msg.get("title") == "Tumi Robe Nirobe",
        str(msg),
    )

    # ── Test 5: Song change (both synced) ────────────────────────────────────
    print(f"\n{INFO} Test 5 — Song Change (both synced)")
    await drain(ws_a, pause=STEP_PAUSE)
    await drain(ws_b, pause=0)

    await ws_a.send(json.dumps({
        "type": "song_change",
        "index": 3,
        "title": "Aamar Shonar Bangla",
        "thumb": "https://example.com/t2.jpg",
        "autoplay": True,
    }))
    sc = await collect_type(ws_b, "song_change")
    record(
        "B receives song_change with correct index",
        sc is not None and sc.get("index") == 3,
        str(sc),
    )

    # ── Test 6: Play event forwarded ─────────────────────────────────────────
    print(f"\n{INFO} Test 6 — Play Event Forwarded")
    # Both must be on the same track
    await ws_b.send(json.dumps({"type": "song_change", "index": 3, "title": "x", "thumb": ""}))
    await drain(ws_a, pause=DRAIN_WAIT)
    await drain(ws_b, pause=0)

    await ws_a.send(json.dumps({
        "type": "sync_event", "event": "play",
        "time": 12.5, "sentAt": int(time.time() * 1000),
    }))
    play = await collect_type(ws_b, "sync_event")
    record(
        "B receives play sync_event",
        play is not None and play.get("event") == "play",
        str(play),
    )

    # ── Test 7: Sync toggle blocks events ────────────────────────────────────
    print(f"\n{INFO} Test 7 — Sync Toggle Blocks Events")
    await ws_b.send(json.dumps({"type": "sync_toggle", "active": False}))
    await drain(ws_a, pause=DRAIN_WAIT)
    await drain(ws_b, pause=0)

    await ws_a.send(json.dumps({
        "type": "sync_event", "event": "pause",
        "time": 20.0, "sentAt": int(time.time() * 1000),
    }))
    blocked = await recv_one(ws_b, timeout=2.5)
    record(
        "Pause blocked when partner sync is off",
        blocked is None or blocked.get("type") != "sync_event",
        f"received: {blocked}",
    )

    # Re-enable sync
    await ws_b.send(json.dumps({"type": "sync_toggle", "active": True}))
    await drain(ws_a, pause=DRAIN_WAIT)
    await drain(ws_b, pause=0)

    # ── Test 8: Track mismatch → error ───────────────────────────────────────
    print(f"\n{INFO} Test 8 — Track Mismatch Error")
    await ws_a.send(json.dumps({"type": "song_change", "index": 2, "title": "TrackA", "thumb": ""}))
    await asyncio.sleep(STEP_PAUSE)
    await ws_b.send(json.dumps({"type": "song_change", "index": 9, "title": "TrackB", "thumb": ""}))
    # Fully drain both sockets before firing the sync_event
    await drain(ws_a, pause=DRAIN_WAIT)
    await drain(ws_b, pause=DRAIN_WAIT)

    await ws_a.send(json.dumps({
        "type": "sync_event", "event": "play",
        "time": 0.0, "sentAt": int(time.time() * 1000),
    }))
    err = await collect_type(ws_a, "error")
    record(
        "A receives error on track mismatch",
        err is not None,
        str(err),
    )

    # ── Test 9: Disconnect → partner offline ─────────────────────────────────
    print(f"\n{INFO} Test 9 — Disconnect → Partner Offline")
    await drain(ws_b, pause=STEP_PAUSE)   # clear any residual messages
    await ws_a.close()
    offline = await collect_type(ws_b, "presence")
    record(
        "B receives offline presence when A disconnects",
        offline is not None and offline.get("status") == "offline",
        str(offline),
    )
    await ws_b.close()


# ── Summary ──────────────────────────────────────────────────────────────────
def print_summary() -> None:
    total  = len(results)
    passed = sum(1 for _, ok, _ in results if ok)
    failed = total - passed
    print("\n" + "═" * 58)
    print(f"  Results:  {passed}/{total} passed", end="")
    if failed:
        print(f"  ({failed} FAILED)", end="")
    print("\n" + "═" * 58)
    if failed:
        print(f"\n  {FAIL} Failed tests:")
        for name, ok, detail in results:
            if not ok:
                print(f"      • {name}")
                if detail:
                    print(f"        {detail}")
        print()
    else:
        print(f"\n  {PASS} All tests passed! Backend is working correctly.\n")


if __name__ == "__main__":
    print("═" * 58)
    print("  Jinka Backend — Debug Test Suite  v2")
    print(f"  Target : {BASE_URL}")
    print(f"  Mode   : {'Remote (longer timeouts)' if IS_REMOTE else 'Local'}")
    print("═" * 58)
    test_http_health()
    try:
        asyncio.run(run_ws_tests())
    except KeyboardInterrupt:
        print("\n  Interrupted.")
    print_summary()
