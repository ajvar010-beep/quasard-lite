"""API управления колонкой для приложения-пульта (/_mgmt/...).

Доступ — по токену ТОЛЬКО в заголовке X-Token (параметр t= убран: токен в
URL оседал в логах и истории браузера, аудит 22.11): ручки умеют менять
Wi-Fi, включать блютуз и слать команды, поэтому открытыми их держать нельзя
(порт 80/443 слушает всю локальную сеть). Токен создаётся при первом
запуске в LITE_DIR/mgmt_token; без него ручки отвечают 401.

Wi-Fi — wpa_cli (на колонке wpa_supplicant с конфигом
/bkupgrade/data/wpa_supplicant.conf), блютуз — bluetoothctl.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
from pathlib import Path

log = logging.getLogger("mgmt")

IFACE = "wlan0"


def load_or_create_token(lite_dir: Path) -> str:
    p = Path(lite_dir) / "mgmt_token"
    try:
        tok = p.read_text("utf-8").strip()
        if tok:
            return tok
    except OSError:
        pass
    tok = secrets.token_hex(16)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(tok + "\n", "utf-8")
        os.chmod(p, 0o600)
    except OSError:
        log.warning("mgmt: не удалось сохранить токен в %s", p)
    log.info("mgmt: создан токен управления %s...", tok[:6])
    return tok


async def _run(*args: str, timeout: float = 12.0) -> str:
    """Запуск команды на колонке: stdout+stderr, без исключений наружу."""
    try:
        p = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        out, _ = await asyncio.wait_for(p.communicate(), timeout=timeout)
        return (out or b"").decode("utf-8", "replace")
    except FileNotFoundError:
        return f"нет команды {args[0]}"
    except asyncio.TimeoutError:
        return f"таймаут {args[0]}"
    except Exception as e:  # noqa: BLE001
        return f"{args[0]}: {type(e).__name__}"


def parse_status(text: str) -> dict:
    """Разбор `wpa_cli status`: ssid, ip, bssid, freq, id."""
    out = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def parse_scan(text: str) -> list[dict]:
    """Разбор `wpa_cli scan_results` -> [{ssid, signal, flags}], без дублей,
    по убыванию сигнала."""
    rows = []
    for line in text.splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) < 5 or not parts[4].strip():
            continue
        ssid = parts[4].strip()
        try:
            sig = int(parts[2])
        except ValueError:
            sig = -100
        flags = parts[3]
        rows.append({"ssid": ssid, "signal": sig,
                     "secure": "PSK" in flags or "WEP" in flags})
    rows.sort(key=lambda r: -r["signal"])
    seen = set()
    out = []
    for r in rows:
        if r["ssid"] in seen:
            continue
        seen.add(r["ssid"])
        out.append(r)
    return out


async def wifi_status() -> dict:
    st = parse_status(await _run("wpa_cli", "-i", IFACE, "status"))
    ip = ""
    out = await _run("ip", "-4", "addr", "show", IFACE)
    m = re.search(r"inet (\d+\.\d+\.\d+\.\d+)", out)
    if m:
        ip = m.group(1)
    return {"ssid": st.get("ssid", ""), "ip": ip,
            "bssid": st.get("bssid", ""), "freq": st.get("freq", "")}


async def wifi_scan() -> list[dict]:
    await _run("wpa_cli", "-i", IFACE, "scan", timeout=15)
    await asyncio.sleep(3.5)   # сканирование асинхронное, результаты чуть позже
    return parse_scan(await _run("wpa_cli", "-i", IFACE, "scan_results"))


async def wifi_connect(ssid: str, password: str) -> tuple[bool, str]:
    """Подключиться к сети: существующее сохранённое сетевое подключение
    переиспользуем, новое — добавляем и сохраняем (переживёт перезагрузку)."""
    if not ssid:
        return False, "пустой SSID"
    nets = await _run("wpa_cli", "-i", IFACE, "list_networks")
    nid = None
    for line in nets.splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) >= 2 and parts[1] == ssid:
            nid = parts[0]
            break
    if nid is None:
        nid = (await _run("wpa_cli", "-i", IFACE, "add_network")).strip()
        if not nid.isdigit():
            return False, f"add_network: {nid[:80]}"
        await _run("wpa_cli", "-i", IFACE, "set_network", nid, "ssid",
                   f'"{ssid}"')
        if password:
            ok = await _run("wpa_cli", "-i", IFACE, "set_network", nid,
                            "psk", f'"{password}"')
            if "FAIL" in ok:
                return False, "пароль не принят (короче 8 символов?)"
        else:
            await _run("wpa_cli", "-i", IFACE, "set_network", nid, "key_mgmt",
                       "NONE")
        await _run("wpa_cli", "-i", IFACE, "enable_network", nid)
        await _run("wpa_cli", "-i", IFACE, "save_config")
    else:
        # Сеть уже сохранена: новый пароль раньше молча игнорировался —
        # переподключение к той же сети со сменённым паролем (например,
        # сменили в роутере) уходило в FAIL (аудит 22.11).
        if password:
            ok = await _run("wpa_cli", "-i", IFACE, "set_network", nid,
                            "psk", f'"{password}"')
            if "FAIL" in ok:
                return False, "пароль не принят (короче 8 символов?)"
            await _run("wpa_cli", "-i", IFACE, "save_config")
    r = await _run("wpa_cli", "-i", IFACE, "select_network", nid)
    if "FAIL" in r:
        return False, r.strip()[:120]
    # ждём подключения до 20 с
    for _ in range(20):
        await asyncio.sleep(1)
        st = parse_status(await _run("wpa_cli", "-i", IFACE, "status"))
        if st.get("wpa_state") == "COMPLETED" and st.get("ssid") == ssid:
            return True, "подключено"
        if st.get("ssid") == ssid:
            return True, "подключаюсь"
    return False, "сеть не ответила за 20 секунд"


async def bluetooth_set(on: bool) -> dict:
    """Включить/выключить блютуз. Возвращает ошибку, если нет прав/адаптера.

    `bluetoothctl power on` в bluez 5.5x требует начального `show` в той же
    сессии — иначе жалоба «No default controller available»; поэтому шлём
    команды через stdin одной сессии.
    """
    cmd = "power on" if on else "power off"
    try:
        p = await asyncio.create_subprocess_exec(
            "bluetoothctl", stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        p.stdin.write(b"list\n" + cmd.encode() + b"\nquit\n")
        await p.stdin.drain()
        try:
            out, _ = await asyncio.wait_for(p.communicate(), timeout=10)
        except asyncio.TimeoutError:
            p.kill()
            return {"ok": False, "error": "bluetoothctl не ответил"}
        text = (out or b"").decode("utf-8", "replace")
        powered = await bluetooth_status()
        ok = powered == on
        log.info("mgmt: блютуз %s -> %s", "вкл" if on else "выкл", powered)
        return {"ok": ok, "on": powered,
                "output": text.strip()[-200:] if not ok else ""}
    except FileNotFoundError:
        return {"ok": False, "error": "нет bluetoothctl"}


async def bluetooth_status() -> bool | None:
    out = await _run("bluetoothctl", "show", timeout=6)
    m = re.search(r"Powered:\s*(yes|no)", out)
    return (m.group(1) == "yes") if m else None


def recent(history: list, n: int = 60) -> list[dict]:
    """Последние N записей истории команд (свежие сверху)."""
    return list(reversed(history[-n:]))
