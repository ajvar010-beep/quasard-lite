"""Распознавание речи БЕЗ ключа и оплаты — через веб-эндпоинт Google.

Это тот же недокументированный эндпоинт, что использует Chromium для
распознавания в браузере: ключ публичный и вшит в браузер, регистрация не
нужна, лимит — практически «сколько успеешь».

Проверено на колонке 16.11: PCM 16 кГц моно LINEAR16 -> «включаю радио Маяк»,
conf 0.889. Opus этот эндпоинт НЕ принимает (400), а PCM — принимает, и это
удачно: профиль «Олег» уже отдаёт очищенный микрофон сырым PCM 16 кГц.
"""
from __future__ import annotations

import json
import logging

import httpx

log = logging.getLogger("asr_google")

URL = "https://www.google.com/speech-api/v2/recognize"
# Публичный ключ Chromium Speech API (тот же, что в исходниках браузера).
KEY = "AIzaSyBOti4mM-6x9WDnZIjIeyEU21OpBXqWBgw"
RATE = 16000


async def transcribe_pcm(pcm: bytes, lang: str = "ru-RU",
                         timeout: float = 12.0) -> str:
    """PCM int16 mono 16 кГц -> текст. '' если не распознано или сбой.

    Ответ приходит несколькими JSON-строками: первая часто пустая
    {"result":[]}, распознавание — в следующей. Берём первый непустой
    вариант с наибольшей уверенностью.
    """
    if not pcm:
        return ""
    headers = {"Content-Type": f"audio/l16; rate={RATE}"}
    params = {"output": "json", "lang": lang, "key": KEY}
    try:
        async with httpx.AsyncClient(timeout=timeout) as cl:
            r = await cl.post(URL, params=params, headers=headers, content=pcm)
    except Exception as e:  # noqa: BLE001
        log.warning("google stt: %r", e)
        return ""
    if r.status_code != 200:
        log.warning("google stt: HTTP %d", r.status_code)
        return ""
    best, best_conf = "", 0.0
    for line in r.text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except ValueError:
            continue
        for res in data.get("result") or []:
            alts = res.get("alternative") or []
            if not alts:
                continue
            text = (alts[0].get("transcript") or "").strip()
            conf = float(alts[0].get("confidence") or 0.0)
            if text and (conf >= best_conf or not best):
                best, best_conf = text, conf
    if best:
        log.info("распознано (%.2f): %r", best_conf, best[:80])
    return best
