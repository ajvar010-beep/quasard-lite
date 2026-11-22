"""Инжектор синтетической речи в голосовой цикл колонки (диагностика VAD/ASR).

Генерирует речь edge-tts, режет в PCM 16 кГц моно и шлёт пакетами OL-magic
по UDP на :5600 колонки. Окно прослушивания открывает ручкой /_wake через adb
(ручка живёт на колонке, снимать защиту LAN не нужно).

    .venv/Scripts/python tools/voice_inject.py --ip <адрес колонки> "погода в Москве"
    .venv/Scripts/python tools/voice_inject.py --ip <адрес колонки> --rms 95 --fragment "какая погода"

--rms N     тихая подача: клип нормируется под RMS N (в логе 21.11 голос
            давал пики 164–278; порог был 70–71)
--fragment  речь окнами 0.1 с с паузами 0.12 с — реконструкция отказа
            «окно без фразы при живом пике» (VAD терял всплески < 0.45 с)
--via-device  слать пакеты изнутри колонки (adb push PCM + отправитель на
            stdlib): с ПК в UDP :5600 не пролезть — INPUT дропает wlan0
"""
from __future__ import annotations

import argparse
import asyncio
import io
import socket
import subprocess
import sys
import tempfile
import time

import av
import numpy as np

RATE = 16000
FRAME = 320            # 20 мс
MAGIC = b"OL\x00\x00"  # "OL" + 2 байта заголовка (voice.py их пропускает)
VOICE = "ru-RU-SvetlanaNeural"

# Отправитель для колонки: только stdlib (numpy на устройстве не нужен).
SENDER = r'''
import socket, sys, time
path, host, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
data = open(path, "rb").read()
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
FRAME = 640
t0 = time.monotonic()
for i in range(0, len(data), FRAME):
    chunk = data[i:i + FRAME]
    if len(chunk) < FRAME:
        chunk += b"\x00" * (FRAME - len(chunk))
    s.sendto(b"OL\x00\x00" + chunk, (host, port))
    dt = t0 + i / 32000.0 - time.monotonic()
    if dt > 0:
        time.sleep(dt)
'''


async def tts_pcm(text: str) -> np.ndarray:
    import edge_tts
    comm = edge_tts.Communicate(text, VOICE)
    mp3 = bytearray()
    async for chunk in comm.stream():
        if chunk["type"] == "audio":
            mp3.extend(chunk["data"])
    with av.open(io.BytesIO(bytes(mp3))) as c:
        st = c.streams.audio[0]
        rs = av.AudioResampler(format="flt", layout="mono", rate=RATE)
        out = []
        for fr in c.decode(st):
            fr.pts = None
            out += [x.to_ndarray().reshape(-1) for x in rs.resample(fr)]
        out += [x.to_ndarray().reshape(-1) for x in rs.resample(None)]
    data = np.concatenate(out).astype(np.float32)
    peak = float(np.max(np.abs(data))) or 1.0
    return (data / peak * 0.9 * 32767).astype(np.int16)


def shape(data: np.ndarray, rms: int | None, fragment: bool) -> np.ndarray:
    if rms:
        cur = float(np.sqrt(np.mean(data.astype(np.float64) ** 2))) or 1.0
        data = np.clip(data * (rms / cur), -32767, 32767).astype(np.int16)
    if fragment:
        # 0.1 с речи / 0.12 с нуля — всплески короче MIN_SPEECH_S по отдельности
        period = int(0.22 * RATE)
        on = int(0.10 * RATE)
        env = np.zeros_like(data)
        for i in range(0, len(data), period):
            env[i:i + on] = 1
        data = (data * env).astype(np.int16)
    return data


def wake(ip: str, sec: int) -> None:
    cmd = f"wget -qO- 'http://127.0.0.1:80/_wake?sec={sec}'"
    out = subprocess.run(
        ["adb", "-s", f"{ip}:5555", "shell", cmd],
        capture_output=True, text=True)
    print("wake:", out.stdout.strip() or out.stderr.strip())


def send(ip: str, pcm: np.ndarray) -> None:
    pad = np.zeros(int(0.3 * RATE), dtype=np.int16)
    stream = np.concatenate([pad, pcm, pad])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    t0 = time.monotonic()
    for i in range(0, len(stream), FRAME):
        chunk = stream[i:i + FRAME]
        if len(chunk) < FRAME:
            chunk = np.pad(chunk, (0, FRAME - len(chunk)))
        sock.sendto(MAGIC + chunk.astype(np.int16).tobytes(), (ip, 5600))
        # реальный темп: VAD меряет время пакетов часами, а не счётчиком
        dt = t0 + i / RATE - time.monotonic()
        if dt > 0:
            time.sleep(dt)
    sock.close()
    print(f"отправлено {len(stream) / RATE:.1f} с речи на {ip}:5600")


def send_via_device(ip: str, pcm: np.ndarray) -> None:
    pad = np.zeros(int(0.3 * RATE), dtype=np.int16)
    stream = np.concatenate([pad, pcm, pad])
    with tempfile.TemporaryDirectory() as td:
        pcm_path = f"{td}/vad.pcm"
        with open(pcm_path, "wb") as f:
            f.write(stream.tobytes())
        snd_path = f"{td}/vad_send.py"
        with open(snd_path, "w") as f:
            f.write(SENDER)
        dev = ["adb", "-s", f"{ip}:5555"]
        for local, remote in ((pcm_path, "/tmp/vad_inject.pcm"),
                              (snd_path, "/tmp/vad_send.py")):
            out = subprocess.run(dev + ["push", local, remote],
                                 capture_output=True, text=True)
            if out.returncode:
                raise SystemExit(f"adb push: {out.stderr.strip()}")
        out = subprocess.run(dev + ["shell",
                              "/data/opt/python/bin/python3.12 /tmp/vad_send.py "
                              "/tmp/vad_inject.pcm 127.0.0.1 5600"],
                             capture_output=True, text=True)
    print(f"отправлено {len(stream) / RATE:.1f} с речи на 127.0.0.1:5600 (изнутри)",
          (out.stderr.strip() or "")[:200])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("text")
    ap.add_argument("--ip", default="", help="адрес колонки (обязательно)")
    ap.add_argument("--rms", type=int, default=0, help="целевой RMS (0 — как есть)")
    ap.add_argument("--fragment", action="store_true")
    ap.add_argument("--no-wake", action="store_true")
    ap.add_argument("--via-device", action="store_true")
    args = ap.parse_args()
    if not args.ip:
        raise SystemExit("укажите адрес колонки: --ip <адрес>")

    pcm = asyncio.run(tts_pcm(args.text))
    pcm = shape(pcm, args.rms, args.fragment)
    if not args.no_wake:
        wake(args.ip, int(len(pcm) / RATE) + (12 if args.via_device else 8))
        time.sleep(0.3)
    if args.via_device:
        send_via_device(args.ip, pcm)
    else:
        send(args.ip, pcm)
    return 0


if __name__ == "__main__":
    sys.exit(main())
