"""Настройка Wi-Fi по Bluetooth — для чужих сетей («в гостях»).

Телефон подключается к колонке по профилю Serial Port (SPP,
UUID 00001101-0000-1000-8000-00805F9B34FB), отправляет ОДНУ строку JSON
{"ssid": "...", "password": "..."}; колонка подключается к сети через
wpa_cli (как /_mgmt/wifi/connect) и отвечает {"ok": true, "ip": "..."}.

Так решаются обе проблемы чужих сетей: колонка попадает в гостевой Wi-Fi
без экрана и кнопок, а приложение сразу узнаёт её новый IP.

Работает в фоновом потоке: rfcomm listen блокирующий, ждёт телефон,
после подключения процесс завершается — читаем/пишем /dev/rfcomm0,
затем снова встаём на ожидание. Видимость адаптера (piscan) включаем
на каждом круге: после перезагрузки она сбрасывается.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import select
import subprocess
import threading
import time

from . import mgmt

log = logging.getLogger("bt")

DEV = "/dev/rfcomm0"
CHANNEL = "1"
READ_TIMEOUT_S = 20.0
IP_WAIT_S = 25.0
OPEN_WARN_INTERVAL_S = 60.0
_LAST_OPEN_WARN = 0.0


def parse_request(line: bytes | str) -> tuple[str, str] | None:
    """Строка от телефона -> (ssid, пароль). None — мусор/неполные данные."""
    if isinstance(line, bytes):
        line = line.decode("utf-8", "replace")
    try:
        obj = json.loads(line.strip())
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    ssid = str(obj.get("ssid") or "").strip()
    password = str(obj.get("password") or "")
    if not ssid:
        return None
    return ssid, password


def reply(ok: bool, message: str = "", ip: str = "") -> bytes:
    return (json.dumps({"ok": ok, "message": message, "ip": ip},
                       ensure_ascii=False) + "\n").encode("utf-8")


async def _current_ip() -> str:
    out = await mgmt._run("ip", "-4", "addr", "show", mgmt.IFACE)
    m = re.search(r"inet (\d+\.\d+\.\d+\.\d+)", out)
    return m.group(1) if m else ""


async def _apply(ssid: str, password: str) -> bytes:
    ok, msg = await mgmt.wifi_connect(ssid, password)
    ip = ""
    if ok:
        deadline = time.monotonic() + IP_WAIT_S
        while time.monotonic() < deadline:
            ip = await _current_ip()
            if ip:
                break
            await asyncio.sleep(1.0)
    log.info("bt: сеть «%s»: %s (ip %s)", ssid, msg if not ok else "ok", ip or "—")
    return reply(ok, msg, ip)


def _ensure_visible() -> None:
    subprocess.call(["hciconfig", "hci0", "piscan"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _handle_connection() -> None:
    """Телефон подключился: прочитать строку, применить, ответить."""
    global _LAST_OPEN_WARN
    try:
        fd = os.open(DEV, os.O_RDWR)
    except OSError as e:
        now = time.monotonic()
        if now - _LAST_OPEN_WARN >= OPEN_WARN_INTERVAL_S:
            _LAST_OPEN_WARN = now
            log.warning("bt: не открылся %s: %r", DEV, e)
        return
    try:
        buf = b""
        deadline = time.monotonic() + READ_TIMEOUT_S
        while time.monotonic() < deadline and b"\n" not in buf:
            r, _, _ = select.select([fd], [], [], 1.0)
            if not r:
                continue
            chunk = os.read(fd, 4096)
            if not chunk:
                break
            buf += chunk
        line = buf.split(b"\n", 1)[0]
        req = parse_request(line)
        if req is None:
            log.warning("bt: непонятный запрос %r", line[:80])
            out = reply(False, "нужен JSON: {\"ssid\": ..., \"password\": ...}")
        else:
            out = asyncio.run(_apply(*req))
        os.write(fd, out)
    except (OSError, Exception) as e:  # noqa: BLE001
        log.warning("bt: обмен не удался: %r", e)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _loop() -> None:
    log.info("bt: настройка по Bluetooth запущена (канал %s)", CHANNEL)
    while True:
        try:
            _ensure_visible()
            rc = subprocess.call(["rfcomm", "-S", "listen", DEV, CHANNEL],
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
            if rc != 0:
                time.sleep(5)
                continue
            _handle_connection()
            # rc=0, а узла нет — без паузы цикл жжёт CPU и лог (~60 итераций/с)
            time.sleep(2)
        except Exception as e:  # noqa: BLE001
            log.warning("bt: ошибка цикла: %r", e)
            time.sleep(3)
        finally:
            subprocess.call(["rfcomm", "release", "0"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def start() -> threading.Thread:
    t = threading.Thread(target=_loop, name="bt-provision", daemon=True)
    t.start()
    return t
