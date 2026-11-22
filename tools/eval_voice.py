"""Регрессионные проверки голосового тракта quasard (без колонки).

Подключается к серверу как станция (x-uprx-device-id: SIMTEST01), отправляет
Vins.TextInput и разбирает VinsResponse. Это тот же путь, что у живой колонки:
интенты -> LLM/инструменты -> TTS -> директивы.

Запуск:  python tools/eval_voice.py [--only имя]
Код возврата 0 — все проверки прошли.

Зачем: за 06.11 три бага («вопрос включает музыку», «ответ не тот», «модель врёт
про себя») ловились только на слух. Здесь они ловятся за 30 секунд.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid

import websockets

URL = "ws://127.0.0.1:80/uni.ws"
HDR = {"x-uprx-device-id": "SIMTEST01", "x-uprx-ssid": "eval"}

PLAYER_DIRECTIVES = ("audio_play", "player_next_track", "player_previous_track",
                     "sound_set_level", "sound_louder", "sound_quiter")


def mid() -> str:
    return str(uuid.uuid4())


async def ask(text: str, timeout: float = 30.0) -> dict:
    """Отправить реплику, вернуть {"text", "directives", "seconds"}."""
    try:
        ws = await websockets.connect(URL, additional_headers=HDR)
    except TypeError:  # websockets < 13
        ws = await websockets.connect(URL, extra_headers=HDR)
    async with ws:
        await ws.send(json.dumps({"event": {"header": {
            "namespace": "System", "name": "SynchronizeState", "messageId": mid()}, "payload": {}}}))
        await asyncio.sleep(0.3)
        await ws.send(json.dumps({"event": {"header": {
            "namespace": "Vins", "name": "TextInput", "messageId": mid()},
            "payload": {"header": {"request_id": mid(), "sequence_number": 0},
                        "request": {"event": {"type": "text_input", "text": text}}}}}))
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=timeout - (time.monotonic() - t0))
            except asyncio.TimeoutError:
                break
            if isinstance(msg, bytes):
                continue
            d = json.loads(msg)
            ev = d.get("event") or d.get("directive") or {}
            if (ev.get("header") or {}).get("name") == "VinsResponse":
                p = ev.get("payload") or {}
                vr = p.get("voice_response") or {}
                resp = p.get("response") or {}
                return {
                    "text": (vr.get("output_speech") or {}).get("text") or "",
                    "directives": [x.get("name") for x in resp.get("directives") or []],
                    "seconds": time.monotonic() - t0,
                }
        return {"text": "", "directives": [], "seconds": time.monotonic() - t0, "timeout": True}


def played(resp: dict) -> bool:
    return any(d in PLAYER_DIRECTIVES for d in resp["directives"])


# --- сценарии: (имя, реплика, проверка) ---------------------------------------
def _has(resp, *words):
    t = resp["text"].lower()
    return all(w.lower() in t for w in words)


def _answered(resp) -> bool:
    """Ответ по существу, а не заглушка «не расслышал» — иначе сломанный провайдер
    проходит проверку как успех (так и вышло 06.11)."""
    t = resp["text"].lower()
    bad = ("не расслышал", "не уложился", "мозг сейчас недоступен", "не получилось", "перегружена", "занял слишком много времени")
    return bool(t) and not any(b in t for b in bad)


CASES = [
    ("арифметика", "Сколько будет два плюс два?",
     lambda r: ("четыр" in r["text"].lower(), f"ответ {r['text']!r}")),

    ("погода", "Какая сегодня погода в Казани?",
     lambda r: (_answered(r) and not played(r), f"ответ {r['text'][:60]!r}, директивы {r['directives']}")),

    # Регрессия 06.11: «поп» внутри «популярная» уводил вопрос в поиск музыки
    ("вопрос не включает музыку", "Какие сейчас самые популярные новогодние песни?",
     lambda r: (_answered(r) and not played(r), f"ответ {r['text'][:60]!r}, директивы {r['directives']}")),

    ("самоназвание модели", "На какой модели ты работаешь?",
     lambda r: (_has(r, "модели", "провайдер"), f"ответ {r['text']!r}")),

    # Обратная проверка: музыка по прямой просьбе ДОЛЖНА включаться
    ("музыка по просьбе", "Олег, включи группу Кино",
     lambda r: (played(r), f"директивы {r['directives']}")),

    ("быстрый ответ", "Кто такой Пушкин?",
     lambda r: (_answered(r) and r["seconds"] < 20, f"{r['text'][:40]!r} за {r['seconds']:.1f} c")),
]


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="подстрока имени сценария")
    # Пауза между сценариями: шесть запросов подряд упираются в бесплатные лимиты
    # (Groq 8000 токенов/мин) и, судя по всему, провоцируют блокировку по антифроду.
    ap.add_argument("--pause", type=float, default=8.0, help="пауза между запросами, с")
    args = ap.parse_args()

    failed = 0
    for name, text, check in CASES:
        if args.only and args.only not in name:
            continue
        if failed or getattr(main, "_sent", 0):
            await asyncio.sleep(args.pause)
        main._sent = getattr(main, "_sent", 0) + 1
        resp = await ask(text)
        if resp.get("timeout"):
            print(f"  FAIL {name}: нет ответа за отведённое время")
            failed += 1
            continue
        ok, detail = check(resp)
        print(f"  {'OK  ' if ok else 'FAIL'} {name:28} ({resp['seconds']:.1f} c) — {detail}")
        failed += 0 if ok else 1

    print(f"\n{'ВСЁ ПРОШЛО' if not failed else f'ПРОВАЛОВ: {failed}'}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
