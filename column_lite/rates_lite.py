"""Курс валют на колонке: ЦБ РФ (cbr-xml-daily.ru) + open.er-api.com запасной.

Ключей не нужно. ЦБ отдаёт официальные курсы с номиналом (юань — за 10),
запасной сервис — рыночный курс от рубля. Кэш 30 минут: ЦБ обновляет
курсы раз в день.
"""
from __future__ import annotations

import logging
import time

import httpx

from .tts import _plural, _three, num_words

log = logging.getLogger("rates")

_CBR = "https://www.cbr-xml-daily.ru/daily_json.js"
_ER = "https://open.er-api.com/v6/latest/RUB"
_TIMEOUT = httpx.Timeout(8.0, connect=5.0)
_CACHE_S = 1800

_cache: dict = {"ts": 0.0, "data": None}

# ключ -> (как позвать в ответе, именительный для перечисления)
NAMES = {"USD": "доллар", "EUR": "евро", "CNY": "юань"}
# слова, по которым узнаём валюту в фразе
_WORDS = {"USD": ("доллар", "доллара", "бакс", "баксов", "долл"),
          "EUR": ("евро",), "CNY": ("юан",)}


def rub_words(v: float) -> str:
    """92.3 -> «девяносто два рубля тридцать копеек» (для озвучки)."""
    rub = int(v)
    kop = int(round((float(v) - rub) * 100))
    if kop >= 100:
        rub, kop = rub + 1, 0
    out = f"{num_words(rub)} {_plural(rub, 'рубль', 'рубля', 'рублей')}"
    if kop:
        # копейки — женского рода: «две копейки»
        out += f" {_three(kop, fem=True)} {_plural(kop, 'копейка', 'копейки', 'копеек')}"
    return out


async def fetch() -> dict | None:
    """{"USD": рублей_за_доллар, ...} с кэшем на 30 минут."""
    now = time.monotonic()
    if _cache["data"] is not None and now - _cache["ts"] < _CACHE_S:
        return _cache["data"]
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True,
                                 headers={"User-Agent": "quasard-lite/1.0"}) as cl:
        data = None
        try:
            r = await cl.get(_CBR)
            r.raise_for_status()
            valute = (r.json() or {}).get("Valute") or {}
            data = {}
            for key in NAMES:
                v = valute.get(key)
                if not v:
                    continue
                nominal = float(v.get("Nominal") or 1)
                data[key] = float(v.get("Value") or 0) / nominal
            if data:
                data["_source"] = "cbr"   # откуда курс — для честной подписи в ответе
            log.info("курс: источник ЦБ РФ")
        except Exception as e:  # noqa: BLE001
            log.warning("курс ЦБ не достали: %r", e)
        if not data:
            try:
                r = await cl.get(_ER)
                r.raise_for_status()
                rates = (r.json() or {}).get("rates") or {}
                data = {}
                for key in NAMES:
                    per_rub = float(rates.get(key) or 0)
                    if per_rub > 0:
                        data[key] = round(1.0 / per_rub, 4)
                if data:
                    data["_source"] = "er"
                log.info("курс: источник open.er-api")
            except Exception as e:  # noqa: BLE001
                log.warning("курс (запасной) не достали: %r", e)
        if not data:
            return None
        _cache.update(ts=now, data=data)
        return data


def text(which: str, data: dict) -> str:
    """which: USD | EUR | CNY | all — готовый ответ словами."""
    have = [k for k in (("USD", "EUR", "CNY") if which == "all" else (which,))
            if data.get(k)]
    if not have:
        return "Курс этой валюты сейчас не узнать — связь подвела."
    # Подпись по источнику: запасной open.er-api — это рыночные курсы, а не
    # официальные ЦБ; раньше ответ всегда говорил «Курс ЦБ» (аудит 22.11).
    prefix = "Курс ЦБ" if data.get("_source") == "cbr" else "Курс валют"
    if len(have) == 1:
        k = have[0]
        return f"{prefix}: {NAMES[k]} — {rub_words(data[k])}."
    parts = [f"{NAMES[k][:1].upper() + NAMES[k][1:]} — {rub_words(data[k])}" for k in have]
    return f"{prefix} на сегодня: " + ", ".join(parts) + "."
