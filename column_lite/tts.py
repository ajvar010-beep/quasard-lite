"""Синтез речи на самой колонке через онлайн-сервис Microsoft Edge TTS.

Ключ не нужен: это тот же движок, что читает вслух в браузере Edge. Протокол —
WebSocket к speech.platform.bing.com; библиотеку edge-tts тянуть не нужно
(на колонке нет pip), поэтому клиент написан на голом websockets, а токен
Sec-MS-GEC считается портированным edge_drm (чистый stdlib + hashlib-схема).

Результат — mp3 (audio-24khz-48kbitrate-mono-mp3), колонка его играет
audio_play-директивой как обычный трек; токен станции для этого не нужен.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
import uuid
from datetime import datetime, timezone

import websockets
import httpx

from . import edge_drm

log = logging.getLogger("tts")

# Google Translate TTS: без ключа, из РФ, отдаёт ЦЕЛЫЙ mp3 за ~1.3 с
# (проверено с колонки 17.11). Основной движок: edge-tts периодически рвёт
# соединение (ConnectionClosedError) — тогда ответ вообще оставался без голоса.
# Голос у Google один (женский ru), лимит ~200 символов на запрос — длинные
# фразы режем по предложениям и склеиваем mp3.
GOOGLE_TTS_URL = "https://translate.google.com/translate_tts"
GOOGLE_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/143.0.0.0 Safari/537.36"}

WSS_BASE = ("wss://speech.platform.bing.com/consumer/speech/synthesize"
            "/readaloud/edge/v1")
TRUSTED_CLIENT_TOKEN = "6A5AA1D4EAFF4E9FB37E23D68491D6F4"
SEC_MS_GEC_VERSION = "2-143.0.3650.75"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0")
ORIGIN = "chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold"

# Голоса ru-RU: Светлана — женский, Дмитрий — мужской.
VOICES = {"svetlana": "ru-RU-SvetlanaNeural", "dmitry": "ru-RU-DmitryNeural"}
DEFAULT_VOICE = "svetlana"


def _date_js() -> str:
    now = datetime.now(timezone.utc)
    # JS-формат: "Tue Nov 16 2026 14:35:02 GMT+0000 (Coordinated Universal Time)"
    return f"{now:%a %b %d %Y %H:%M:%S} GMT+0000 (Coordinated Universal Time)"


def _connect_id() -> str:
    return uuid.uuid4().hex


def _clean(text: str) -> str:
    # управляющие символы ломают SSML; & < > экранируем
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text or "")
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .strip())


def _ssml(text: str, voice: str) -> str:
    return (f"<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' "
            f"xml:lang='ru-RU'><voice name='{voice}'>"
            f"<prosody pitch='+0Hz' rate='+0%' volume='+0%'>{_clean(text)}"
            f"</prosody></voice></speak>")


def _headers(extra: dict | None = None) -> dict:
    h = {
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "Origin": ORIGIN,
        "User-Agent": USER_AGENT,
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Cookie": f"muid={edge_drm.DRM.generate_muid()};",
    }
    if extra:
        h.update(extra)
    return h


def _url() -> str:
    return (f"{WSS_BASE}?TrustedClientToken={TRUSTED_CLIENT_TOKEN}"
            f"&ConnectionId={_connect_id()}"
            f"&Sec-MS-GEC={edge_drm.DRM.generate_sec_ms_gec()}"
            f"&Sec-MS-GEC-Version={SEC_MS_GEC_VERSION}")


def _parse_block(head: bytes) -> dict:
    out = {}
    for line in head.split(b"\r\n"):
        if b":" in line:
            k, v = line.split(b":", 1)
            out[k.strip().decode("latin-1").lower()] = v.strip().decode("latin-1")
    return out


def _split_long(text: str, limit: int = 180) -> list[str]:
    """Google TTS принимает ~200 символов на запрос: режем по предложениям."""
    parts = re.split(r"(?<=[.!?…])\s+", text.strip())
    out: list[str] = []
    cur = ""
    for p in parts:
        if len(cur) + len(p) + 1 <= limit:
            cur = (cur + " " + p).strip()
        else:
            if cur:
                out.append(cur)
            while len(p) > limit:
                cut = p.rfind(" ", 0, limit)
                if cut <= 0:
                    cut = limit
                out.append(p[:cut].strip())
                p = p[cut:].strip()
            cur = p
    if cur:
        out.append(cur)
    return out or [text[:limit]]


# ---------------------------------------------------------------- числа словами
# Живой случай 20.11: TTS прочитал «123» как «один, два, три». Числа в тексте
# ответа заранее переводим в слова. Времена («16:06»), десятичные («1.5»),
# дроби и диапазоны не трогаем — их движок читает правильно сам.
_ONES = ("ноль", "один", "два", "три", "четыре", "пять", "шесть", "семь",
         "восемь", "девять")
_FEM = {"один": "одна", "два": "две"}
_TEENS = ("десять", "одиннадцать", "двенадцать", "тринадцать", "четырнадцать",
          "пятнадцать", "шестнадцать", "семнадцать", "восемнадцать", "девятнадцать")
_TENS = ("", "", "двадцать", "тридцать", "сорок", "пятьдесят",
         "шестьдесят", "семьдесят", "восемьдесят", "девяносто")
_HUNDREDS = ("", "сто", "двести", "триста", "четыреста", "пятьсот",
             "шестьсот", "семьсот", "восемьсот", "девятьсот")


def _three(n: int, fem: bool = False) -> str:
    """0..999 словами. fem — женский род для тысяч («одна тысяча»)."""
    out = []
    h, rem = divmod(n, 100)
    if h:
        out.append(_HUNDREDS[h])
    if rem >= 20:
        out.append(_TENS[rem // 10])
        rem %= 10
    elif rem >= 10:
        out.append(_TEENS[rem - 10])
        rem = 0
    if rem or not out:
        w = _ONES[rem]
        if fem and w in _FEM:
            w = _FEM[w]
        out.append(w)
    return " ".join(out)


def _plural(n: int, one: str, few: str, many: str) -> str:
    n %= 100
    if 10 <= n <= 20:
        return many
    n %= 10
    return one if n == 1 else few if 2 <= n <= 4 else many


def num_words(n: int) -> str:
    """123 -> «сто двадцать три», 21000 -> «двадцать одна тысяча»."""
    if n == 0:
        return "ноль"
    parts = []
    millions, rest = divmod(n, 1_000_000)
    thousands, rest = divmod(rest, 1000)
    if millions:
        parts.append(_three(millions) + " "
                     + _plural(millions, "миллион", "миллиона", "миллионов"))
    if thousands:
        parts.append(_three(thousands, fem=True) + " "
                     + _plural(thousands, "тысяча", "тысячи", "тысяч"))
    if rest:
        parts.append(_three(rest))
    return " ".join(parts)


# Число, не входящее в «плотную» группу цифр/знаков: «1,5», «16:06», «1-2»
# не трогаем, а «12,» или «123.» в конце фразы — переводим (запятая там просто
# пунктуация). therefore: перед и после — не цифра и не «. , : / - + цифра».
_DIGITS_RE = re.compile(
    r"(?<![\d.,:/\-])(\d{1,9})(?!\d)(?![.,:/\-+]\d)")


def duration_words(sec: float) -> str:
    """123 -> «две минуты три секунды» — для фраз «место: …».

    Минуты и секунды — женского рода: «две минуты», «одна секунда».
    """
    sec = int(max(0.0, sec))
    m, s = divmod(sec, 60)
    parts = []
    if m:
        parts.append(f"{_three(m, fem=True)} {_plural(m, 'минута', 'минуты', 'минут')}")
    if s or not m:
        parts.append(f"{_three(s, fem=True)} {_plural(s, 'секунда', 'секунды', 'секунд')}")
    return " ".join(parts)


def numbers_to_words(text: str) -> str:
    """Заменяет отдельные числа словами; 007/042 и хвосты после точки/двоеточия
    оставляет как есть (коды и времена звучат правильно по-своему)."""
    def repl(m: re.Match) -> str:
        s = m.group(1)
        if len(s) > 1 and s[0] == "0":
            return s
        return num_words(int(s))
    return _DIGITS_RE.sub(repl, text)


async def synth_google(text: str, timeout: float = 8.0) -> bytes | None:
    """Google Translate TTS: целый mp3 ~1.3 с, без ключа, из РФ."""
    from urllib.parse import quote
    audio = bytearray()
    try:
        async with httpx.AsyncClient(
                timeout=httpx.Timeout(timeout, connect=5),
                follow_redirects=True) as cl:
            for part in _split_long(text):
                url = (f"{GOOGLE_TTS_URL}?ie=UTF-8&client=tw-ob&tl=ru"
                       f"&q={quote(part)}")
                r = await cl.get(url, headers=GOOGLE_UA)
                if r.status_code != 200 or not r.content:
                    log.warning("google-tts: HTTP %d на части %r",
                                r.status_code, part[:30])
                    return None
                audio.extend(r.content)
    except Exception as e:  # noqa: BLE001
        log.warning("google-tts: %r", e)
        return None
    return bytes(audio) if len(audio) > 1000 else None


async def synth(text: str, voice: str = DEFAULT_VOICE,
                timeout: float = 15.0) -> bytes | None:
    """Текст -> mp3 (bytes): Google (быстро и стабильно) -> edge-tts (резерв)."""
    text = numbers_to_words((text or "").strip())
    return await _synth_prepared(text, voice, timeout)


async def _synth_prepared(text: str, voice: str, timeout: float) -> bytes | None:
    """Синтез уже подготовленного текста (numbers_to_words применён ЗАРАНЕЕ).

    speak_to_file конвертирует числа сам (для cache_key), и раньше synth
    прогонял numbers_to_words ещё раз — двойной проход (аудит 22.11).
    """
    if not text:
        return None
    mp3 = await synth_google(text)
    if mp3:
        return mp3
    log.info("edge-tts: Google не ответил, пробую edge")
    audio = bytearray()
    async for chunk in synth_stream(text, voice, timeout):
        audio.extend(chunk)
    return bytes(audio) if audio else None


async def synth_stream(text: str, voice: str = DEFAULT_VOICE,
                       timeout: float = 15.0):
    """Текст -> mp3-чанки ПО МЕРЕ синтеза (async-генератор).

    Edge-tts отдаёт первые байты через ~2–3 с, а всю фразу (длинный ответ) —
    через 10–13 с. Раньше ждали конца и колонка молчала; теперь чанки уходят
    в ответ на /_tts сразу, и воспроизведение начинается с первым из них.
    """
    text = (text or "").strip()
    if not text:
        return
    voice_id = VOICES.get(voice, VOICES[DEFAULT_VOICE])
    rid = _connect_id()
    msg1 = (f"X-Timestamp:{_date_js()}\r\n"
            "Content-Type:application/json; charset=utf-8\r\n"
            "Path:speech.config\r\n\r\n"
            '{"context":{"synthesis":{"audio":{"metadataoptions":'
            '{"sentenceBoundaryEnabled":"false","wordBoundaryEnabled":"false"},'
            '"outputFormat":"audio-24khz-48kbitrate-mono-mp3"}}}}\r\n')
    msg2 = (f"X-RequestId:{rid}\r\n"
            "Content-Type:application/ssml+xml\r\n"
            f"X-Timestamp:{_date_js()}Z\r\n"
            "Path:ssml\r\n\r\n" + _ssml(text, voice_id))
    try:
        async with websockets.connect(
                _url(), additional_headers=_headers(),
                open_timeout=timeout, close_timeout=3,
                max_size=8 * 1024 * 1024) as ws:
            await ws.send(msg1)
            await ws.send(msg2)
            deadline = time.monotonic() + timeout
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                frame = await asyncio.wait_for(ws.recv(), timeout=left)
                if isinstance(frame, str):
                    if "\r\n\r\n" not in frame:
                        continue
                    head, _ = frame.split("\r\n\r\n", 1)
                    h = _parse_block(head.encode("latin-1", "replace"))
                    if h.get("path") == "turn.end":
                        break
                    continue
                if isinstance(frame, (bytes, bytearray)):
                    if len(frame) < 2:
                        continue
                    hlen = int.from_bytes(frame[:2], "big")
                    block = frame[2:2 + hlen]
                    payload = frame[2 + hlen:]
                    h = _parse_block(block)
                    if h.get("path") == "audio" and payload:
                        yield payload
    except asyncio.TimeoutError:
        log.warning("edge-tts: таймаут синтеза")
    except Exception as e:  # noqa: BLE001
        log.warning("edge-tts: синтез не удался: %r", e)


def cache_key(text: str, voice: str) -> str:
    return hashlib.sha256(f"{voice}|{text}".encode("utf-8")).hexdigest()[:24]


async def speak_to_file(text: str, cache_dir, voice: str = DEFAULT_VOICE) -> str | None:
    """Текст -> путь к mp3 в кэше (или None). Повтор той же фразы не синтезируется:
    «Треки закончились», «Сейчас ничего не играет» звучат часто."""
    from pathlib import Path
    text = numbers_to_words((text or "").strip())
    if not text:
        return None
    d = Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{cache_key(text, voice)}.mp3"
    if p.exists() and p.stat().st_size > 1000:
        return str(p)
    # одинаковые фразы долбят сервис параллельно — кэш-файл пишется один раз
    mp3 = await _synth_prepared(text, voice, 15.0)
    if not mp3 or len(mp3) < 1000:
        return None
    tmp = p.with_suffix(".part")
    tmp.write_bytes(mp3)
    tmp.replace(p)
    log.info("edge-tts: %r -> %d байт", text[:40], len(mp3))
    return str(p)
