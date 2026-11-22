"""Развёртывание quasard-lite на колонку через adb.

Копирует не только код, но и то, без чего автономный режим не работает:
  • device.json  — запись устройства (glagol-сертификат и ключ) из data/devices;
  • certs/       — сертификат для HTTPS/WSS (колонка уже доверяет нашему CA).

Запуск (с ПК, колонка подключена по adb):
  python tools/deploy_lite.py --ip <адрес колонки> [--pc-ip <адрес ПК>]
Серийник берётся из data/devices/*.json (единственный файл) или --device-id.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LITE = ROOT / "column_lite"
DEVICES = ROOT / "data" / "devices"
CERTS = ROOT / "certs"
REMOTE = "/data/opt/column_lite"


def sh(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=False, capture_output=True, text=True, **kw)


def adb(ip: str, *args: str, shell: bool = False) -> subprocess.CompletedProcess:
    base = ["adb", "-s", f"{ip}:5555"]
    if shell:
        return sh(base + ["shell", *args])
    return sh(base + list(args))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ip", default="",
                    help="адрес колонки в вашей сети (обязательно, спросите у роутера)")
    ap.add_argument("--device-id", default="",
                    help="серийник колонки; по умолчанию — из единственного файла data/devices/*.json")
    ap.add_argument("--pc-ip", default="",
                    help="IP компьютера-сервера quasard в той же сети: нужен голосу без ПК "
                         "(микрофон «Олега» шлёт PCM на этот адрес). Необязателен")
    ap.add_argument("--skip-certs", action="store_true")
    args = ap.parse_args()

    if not args.ip:
        print("укажите адрес колонки: --ip <адрес>\n"
              "Его видно в приложении Яндекса или на роутере (DHCP-lease колонки).",
              file=sys.stderr)
        return 2
    if not args.device_id:
        # Серийник живого устройства в репозитории не хранится: берём его из
        # файла устройства, снятого с колонки (data/devices/<серийник>.json).
        found = sorted(DEVICES.glob("*.json")) if DEVICES.exists() else []
        if len(found) == 1:
            args.device_id = found[0].stem
        else:
            print(f"укажите серийник колонки: --device-id <id> "
                  f"(в data/devices найдено файлов: {len(found)})", file=sys.stderr)
            return 2

    if not shutil.which("adb"):
        print("adb не найден в PATH")
        return 1
    out = sh(["adb", "connect", f"{args.ip}:5555"])
    print(out.stdout.strip() or out.stderr.strip())

    # 1) каталоги
    print(adb(args.ip, "mkdir", "-p", f"{REMOTE}/certs", shell=True).stderr.strip())

    # 2) код
    # P1-4: `adb push <dir> <existing_dir>` кладёт каталог ВНУТРЬ удалённого
    # (получался column_lite/column_lite/) — S99zlite потом не мог импортировать
    # пакет. Копируем содержимое каталога: LITE/. -> REMOTE.
    print("копирую код...")
    print(adb(args.ip, "push", str(LITE) + "/.", REMOTE).stderr.strip() or "ok")

    # 3) запись устройства (glagol-сертификат)
    dev_src = DEVICES / f"{args.device_id}.json"
    if dev_src.exists():
        out = adb(args.ip, "push", str(dev_src), f"{REMOTE}/device.json")
        print(out.stderr.strip() or "device.json ok")
    else:
        print(f"ВНИМАНИЕ: нет {dev_src} — без него lite не отдаст glagol-сертификат "
              "и голос на колонке сломается", file=sys.stderr)

    # 4) сертификаты
    if not args.skip_certs:
        for name in ("server.crt", "server.key"):
            src = CERTS / name
            if not src.exists():
                print(f"ВНИМАНИЕ: нет {src}", file=sys.stderr)
                continue
            out = adb(args.ip, "push", str(src), f"{REMOTE}/certs/{name}")
            print(out.stderr.strip() or f"{name} ok")
        adb(args.ip, "chmod", "600", f"{REMOTE}/certs/server.key", shell=True)

    # 5) автозапуск
    out = adb(args.ip, "push", str(LITE / "S99zlite"), "/data/opt/oleg/S99zlite")
    print(out.stderr.strip() or "S99zlite ok")
    adb(args.ip, "chmod", "755", "/data/opt/oleg/S99zlite", shell=True)

    # 5б) автономия: сторож ПК<->lite, lite-конфиг DNS-пинов, исправленный
    # egress-файрвол (DNS 77.88.8.x и uniproxy разрешены до DROP подсетей).
    # IP компьютера и LAN из репозитория не зашиваются: pc_ip кладём файлом,
    # LAN/роутер скрипты определяют сами (wlan0 + default route).
    if args.pc_ip:
        pc_file = LITE.parent / f"pc_ip.{args.device_id}.tmp"
        try:
            pc_file.write_text(args.pc_ip.strip() + "\n", "utf-8")
            out = adb(args.ip, "push", str(pc_file), "/data/opt/cfg/pc_ip")
            print(out.stderr.strip() or "pc_ip ok")
        finally:
            pc_file.unlink(missing_ok=True)
    else:
        print("ВНИМАНИЕ: --pc-ip не задан — голос без ПК не заработает "
              "(DNAT микрофона ставить не на что)", file=sys.stderr)
    for name, dst in (("failover.sh", "/data/opt/oleg/failover.sh"),
                      ("unbound.lite", "/data/opt/cfg/unbound.lite"),
                      ("lock_column_egress.sh", "/data/quasar/lock_column_egress.sh")):
        src = LITE / name
        if not src.exists():
            print(f"ВНИМАНИЕ: нет {src}", file=sys.stderr)
            continue
        out = adb(args.ip, "push", str(src), dst)
        print(out.stderr.strip() or f"{name} ok")
        adb(args.ip, "chmod", "755", dst, shell=True)
    # Голос без ПК: приёмник микрофона слушает UDP :5600 на самой колонке.
    # Нужны пакеты edge-tts (озвучка) и google-stt (распознавание) — они уже в
    # каталоге LITE и уходят вместе с кодом выше.
    adb(args.ip, "sh", "/data/opt/oleg/failover.sh", "start", shell=True)

    # 6) проверим, что питон и pylibs на месте
    check = adb(args.ip, "ls", "/data/opt/python/bin/python3.12", "/data/opt/pylibs", shell=True)
    print(check.stdout.strip() or check.stderr.strip())

    print("\nГотово. Перезапуск: adb shell sh /data/opt/oleg/S99zlite stop && "
          "adb shell sh /data/opt/oleg/S99zlite start")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
