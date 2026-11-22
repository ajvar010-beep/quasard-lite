"""Инжектор команд в quasard-lite на колонке (отладка через adb).

Запускается НА КОЛОНКЕ:
  /data/opt/python/bin/python3.12 /tmp/send_lite.py say "включи кино группа крови"
  /data/opt/python/bin/python3.12 /tmp/send_lite.py ping

say  — текст «как из приложения»: glagol sendText -> TextInput в шлюз lite.
ping — снимок playerState (playing, progress) прямо из glagol колонки.

Серийник устройства в репозитории не хранится: берётся из device.json
(его кладёт deploy_lite.sh) или переменной LITE_DEVICE_ID.
"""
import asyncio
import json
import os
import ssl
import sys
import time
import uuid

sys.path.insert(0, "/data/opt/pylibs")
sys.path.insert(0, "/data/opt")

from column_lite.tokens import glagol_token, load_or_create_key  # noqa: E402

import websockets  # noqa: E402


def _device_id() -> str:
    dev = os.environ.get("LITE_DEVICE_ID", "").strip()
    if dev:
        return dev
    for path in ("/data/opt/column_lite/device.json", "device.json"):
        try:
            with open(path, encoding="utf-8") as f:
                dev = str((json.load(f) or {}).get("id") or "").strip()
            if dev:
                return dev
        except (OSError, ValueError):
            continue
    raise SystemExit("серийник не найден: нужен device.json или LITE_DEVICE_ID")


DEV = _device_id()
PLAT = os.environ.get("LITE_PLATFORM", "yandexmicro")
KEY = load_or_create_key("/data/opt/column_lite/glagol_jwt.key")


async def main(cmd: str, arg: str) -> None:
    token = glagol_token(DEV, PLAT, KEY)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    if cmd == "say":
        payload = {"command": "sendText", "text": arg}
    else:
        payload = {"command": "ping"}
    async with websockets.connect("wss://127.0.0.1:1961", ssl=ctx,
                                  open_timeout=5, close_timeout=2) as ws:
        await ws.send(json.dumps({"conversationToken": token,
                                  "id": str(uuid.uuid4()), "payload": payload,
                                  "sentTime": int(time.time() * 1000)},
                                 ensure_ascii=False))
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=4)
        except asyncio.TimeoutError:
            print("no-reply")
            return
    try:
        obj = json.loads(raw)
    except ValueError:
        print("raw:", raw[:300])
        return
    if cmd == "ping":
        st = obj.get("state") or {}
        ps = st.get("playerState") or {}
        print(f"playing={bool(st.get('playing'))} progress={float(ps.get('progress') or 0.0):.1f}"
              f" title={ps.get('title') or ''!r} error={obj.get('errorCode') or ''}")
    else:
        print("sent, reply error:", obj.get("errorCode") or "none")


asyncio.run(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else ""))
