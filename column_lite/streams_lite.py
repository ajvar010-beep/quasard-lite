"""Источники звука для quasard-lite: музыка, радио, аудиокниги.

Тонкая версия streams.py: без protobuf, без привязки к ПК. Колонка сама стримит
mp3 по прямым ссылкам (audio_play), мы только ищем и отдаём URL.
Популярность: Deezer rank (приходит в поиске) + позиция в выдаче Яндекс Музыки.
"""
from __future__ import annotations

import asyncio
import contextlib
import html as _html
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote

import httpx

log = logging.getLogger("streams")

_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) quasard-lite/1.0"}
_TIMEOUT = httpx.Timeout(20.0, connect=8.0)

# Встроенное радио (мгновенно, без поиска)
# «Энергия» (pub0302.101.ru:8443) убрана 19.11: все её эндпоинты отвергают
# соединение/отдают 403 даже с ПК (проверено) — станция недоступна из сети.
RADIO: dict[str, tuple[str, str]] = {
    "маяк": ("Радио Маяк", "https://icecast-vgtrk.cdnvideo.ru/mayakfm_mp3_192kbps"),
    "вести": ("Вести ФМ", "https://icecast-vgtrk.cdnvideo.ru/vestifm_mp3_192kbps"),
    "русское": ("Русское радио", "https://rusradio.hostingradio.ru/rusradio128.mp3"),
    "русское радио": ("Русское радио", "https://rusradio.hostingradio.ru/rusradio128.mp3"),
    "европа": ("Европа Плюс", "https://europaplus.hostingradio.ru/europaplus128.mp3"),
    "европа плюс": ("Европа Плюс", "https://europaplus.hostingradio.ru/europaplus128.mp3"),
    "dfm": ("DFM", "https://dfm.hostingradio.ru/dfm128.mp3"),
    "хит": ("Хит FM", "https://hitfm.hostingradio.ru/hitfm128.mp3"),
    "хит фм": ("Хит FM", "https://hitfm.hostingradio.ru/hitfm128.mp3"),
    "максимум": ("Радио Максимум", "https://maximum.hostingradio.ru/maximum128.mp3"),
}
RADIO_DEFAULT = "маяк"


def _norm(s: str) -> str:
    return " ".join(re.sub(r"[^\w\s]", " ", (s or "").lower().replace("ё", "е")).split())


@dataclass
class Track:
    url: str
    title: str
    artist: str = ""
    duration_ms: int = 0
    popularity: int = 0  # deezer rank / яндекс-позиция * 30000


@dataclass
class Session:
    kind: str  # "music" | "radio" | "book"
    title: str
    queue: list[Track] = field(default_factory=list)
    pos: int = 0
    identifier: str = ""   # книга: id на archive.org (для «продолжай книгу»)
    fails: int = 0         # подряд неудачных треков (on_failed)
    # Уникальный ключ стрима в плеере колонки. mediad переиспользует
    # кешированного клиента по ключу и делает seek на прошлую позицию; для
    # эфира без Range это ERROR через ~32 КБ (Маяк, проверено 19.11). Новый
    # ключ на каждый запуск радио — свежий плеер с позиции 0.
    key: str = ""
    # радио: время последнего обрыва. Неудачи дальше окна — новый инцидент,
    # счётчик сбрасывается (иначе запоздавший on_failed от старого плеера
    # добил бы рабочую станцию до лимита).
    last_fail: float = 0.0
    # pos — последний ВЫДАННЫЙ колонке трек; started — тот, что реально играет.
    # Они расходятся: колонка просит следующий трек заранее (предзагрузка,
    # через ~1.5 мин после старта текущего — проверено 17.11).
    started: int = 0
    # Пауза: любая остановка (голосом, кнопкой) запоминает секунду трека,
    # и «продолжай» подхватывает ровно оттуда.
    paused: bool = False
    paused_at: float = 0.0       # секунда трека, на которой остановились
    start_ts: float = 0.0        # monotonic начала звучания (оценка позиции, если ping не ответил)
    start_offset: float = 0.0    # с какой секунды начали играть (после «продолжай»)
    # Скорость воспроизведения (1.0 — обычная). Переживает смену треков:
    # следующий audio_play несёт её в playback_speed.
    speed: float = 1.0
    # Повтор текущего трека: по концу (GET_NEXT) трек запускается заново,
    # очередь не двигается.
    loop: bool = False
    # monotonic выдачи последнего audio_play. Фраза ответа глушит поток, трек
    # стартует только после TTS; _watch_stop не должен принять эту паузу
    # за остановку пользователем (гонка, поймана 19.11).
    issued_ts: float = 0.0
    # Когда мы последний раз отправили ответ С ОЗВУЧКОЙ поверх нашей музыки.
    # Только после такого ответа колонка глушит поток фразой, и остановку надо
    # ждать до старта трека. Остановка без свежей фразы (кнопка ▶‖) — пауза
    # сразу (найдено 20.11: кнопка -> «продолжай» через 5 с отвечал «Уже играет»).
    speech_ts: float = 0.0
    # С какой секунды стартует ПОСЛЕДНИЙ выданный audio_play. В start_offset
    # попадает только на on_started: предзагрузка следующего трека не должна
    # затирать отсчёт у того, что ещё играет.
    pending_offset: float = 0.0
    # Растёт при каждой смене трека/паузе: отложенная проверка on_stopped,
    # заставшая чужой epoch, ничего не делает (иначе конец трека принимался
    # за паузу и «продолжай» перезапускало доигранное).
    gen: int = 0

    @property
    def current(self) -> Track | None:
        return self.queue[self.pos] if 0 <= self.pos < len(self.queue) else None

    def next(self) -> Track | None:
        if self.pos + 1 < len(self.queue):
            self.pos += 1
            return self.current
        return None

    def prev(self) -> Track | None:
        if self.pos > 0:
            self.pos -= 1
            return self.current
        return None


# ---------------------------------------------------------------- Deezer: мейнстрим
def _parse_deezer(data: list) -> dict:
    """Структура мейнстрима из выдачи Deezer.

    Deezer — каталог с ранжированием по реальному прослушиванию; самый
    популярный вариант (rank) считаем «каноничным»: его исполнителя и
    название. «кино группа крови» -> артист «Кино», песня «Группа крови» —
    по ним ниже отбираем трек из бесплатных баз.
    """
    pop: dict[str, int] = {}
    best: tuple[str, str, int] | None = None
    artists: set[str] = set()
    for d in data or []:
        rank = int(d.get("rank") or 0)
        title = _norm(d.get("title") or "")
        artist = _norm((d.get("artist") or {}).get("name") or "")
        key = f"{artist} {title}".strip()
        if not key:
            continue
        pop[key] = max(pop.get(key, 0), rank)
        if artist:
            artists.add(artist)
        if best is None or rank > best[2]:
            best = (artist, title, rank)
    return {"pop": pop,
            "best_artist": best[0] if best else "",
            "best_title": best[1] if best else "",
            "artists": artists}


async def deezer_resolve(query: str) -> dict:
    """Канонический вариант песни по Deezer. Пустая структура при сбое."""
    try:
        async with httpx.AsyncClient(timeout=4, follow_redirects=True, headers=_UA) as cl:
            r = await cl.get("https://api.deezer.com/search/track",
                             params={"q": query, "limit": 25})
            return _parse_deezer((r.json() or {}).get("data") or [])
    except Exception as e:  # noqa: BLE001
        log.info("deezer: %r", e)
        return {"pop": {}, "best_artist": "", "best_title": "", "artists": set()}


async def deezer_chart(limit: int = 8) -> list[tuple[str, str]]:
    """Топ треков Deezer [(исполнитель, название)] — основа «моей волны».

    Персональной волны без аккаунта у lite нет: берём чарт и ищем эти же
    песни в бесплатных базах (Streams.wave).
    """
    try:
        async with httpx.AsyncClient(timeout=4, follow_redirects=True, headers=_UA) as cl:
            r = await cl.get("https://api.deezer.com/chart/0/tracks",
                             params={"limit": limit})
            data = (r.json() or {}).get("data") or []
        return [(str((d.get("artist") or {}).get("name") or ""),
                 str(d.get("title") or ""))
                for d in data if isinstance(d, dict) and d.get("title")]
    except Exception as e:  # noqa: BLE001
        log.info("deezer chart: %r", e)
        return []


# ---------------------------------------------------------------- SkySound7
_SKY_BASE = "https://skysound7.net/search?q="
_SKY_ROW = re.compile(
    r'data-url="([^"]+\.mp3[^"]*)"(.{0,1200}?)title="Скачать\s+([^"]+)"', re.S)


async def skysound_fetch(query: str) -> str:
    """HTML выдачи SkySound7 ('' при сбое). Вынесено, чтобы популярность Deezer
    грузилась параллельно с запросом, а не после него.

    504 Gateway Time-out у SkySound — обычное дело на длинных запросах
    (проверено 17.11: «король и шут грохочет гром» то 504, то 200). Один
    повтор через 1.5 с спасает от ложного «Не нашла музыку».
    """
    for _attempt in (0, 1):
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True,
                                         headers=_UA) as cl:
                r = await cl.get(_SKY_BASE + quote(query))
                if r.status_code >= 500:
                    log.warning("skysound: %d, пробую ещё раз", r.status_code)
                    await asyncio.sleep(1.5)
                    continue
                return r.text
        except Exception as e:
            log.warning("skysound: %r", e)
            await asyncio.sleep(1.5)
    return ""


def skysound_score(html: str, query: str, pop_map: dict[str, int] | None = None,
                   limit: int = 25) -> list[Track]:
    """Скоринг выдачи: слова запроса + популярность Deezer.

    Популярность раньше считалась, но НЕ передавалась сюда — deezer_rank_map
    вызывался и результат выбрасывался, поэтому ремиксы и каверы шли наравне с
    оригиналом. Теперь pop_map реально влияет на сортировку.
    """
    pop_map = pop_map or {}
    qn = _norm(query)
    qwords = [w for w in qn.split() if len(w) > 2]
    rows = []
    for m in _SKY_ROW.finditer(html):
        url, _, title = m.group(1), m.group(2), m.group(3).strip()
        tn = _norm(title)
        score = sum(1 for w in qwords if w in tn)
        pop = 0
        for k, v in pop_map.items():
            kwords = [w for w in k.split() if len(w) > 2]
            if kwords and all(w in tn for w in kwords):
                pop = max(pop, v)
        score += int(15 * (pop ** 0.1)) if pop else 0
        rows.append((score, pop, title, url))
    rows.sort(key=lambda r: (-r[0], -r[1]))
    return [Track(url=u, title=t, popularity=p) for s, p, t, u in rows[:limit] if s > 0]


async def skysound_search(query: str, pop_map: dict[str, int] | None = None,
                          limit: int = 25) -> list[Track]:
    """Мейнстрим полными треками (fetch + скоринг)."""
    return skysound_score(await skysound_fetch(query), query, pop_map, limit)


# ---------------------------------------------------------------- muzofond / muzmo
# Дополнительные базы с прямыми mp3 (проверено С КОЛОНКИ 17.11, без VPN):
#   muzofond.fm  — до 110 треков на запрос, полный mp3, Range, есть длительность;
#   muzmo.cc     — ~15 треков, mp3 128 kbps через CDN mzmdl.com.
# Что НЕ годится (проверено там же): mp3party — CDN отдаёт `200 audio/mpeg`
# с текстом «fail» вместо звука; hitmotop/hotmo/mp3uks недоступны; zaycev —
# ссылок в HTML нет; ligaudio — нерелевантная выдача.
_BROWSER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/143.0.0.0 Safari/537.36"}
_SRC_TIMEOUT = httpx.Timeout(8.0, connect=5.0)

_FOND_URL = re.compile(r'data-url="(https?://[^"]+)"')
_FOND_DUR = re.compile(r'data-duration="(\d+)"')
_FOND_ARTIST = re.compile(r'<span class="artist">(.*?)</span>', re.S)
_FOND_TRACK = re.compile(r'<span class="track">(.*?)</span>', re.S)


def _clean(s: str) -> str:
    return " ".join(_html.unescape(re.sub(r"<[^>]+>", " ", s or "")).split())


async def muzofond_search(query: str, limit: int = 60) -> list[Track]:
    """muzofond.fm: [] при любом сбое (вызывается в gather)."""
    try:
        async with httpx.AsyncClient(timeout=_SRC_TIMEOUT, follow_redirects=True,
                                     headers=_BROWSER_UA) as cl:
            r = await cl.get("https://muzofond.fm/search/" + quote(query))
            if r.status_code != 200:
                log.info("muzofond: HTTP %d", r.status_code)
                return []
            text = r.text
    except Exception as e:  # noqa: BLE001
        log.warning("muzofond: %r", e)
        return []
    out: list[Track] = []
    # режем по трекам: общий regex на всю страницу мог бы склеить поля соседей
    for chunk in text.split('<li class="item" data-id=')[1:]:
        mu, mt = _FOND_URL.search(chunk), _FOND_TRACK.search(chunk)
        if not mu or not mt or "/collections/" in mu.group(1):
            continue
        ma, md = _FOND_ARTIST.search(chunk), _FOND_DUR.search(chunk[:200])
        out.append(Track(url=_html.unescape(mu.group(1)), title=_clean(mt.group(1)),
                         artist=_clean(ma.group(1)) if ma else "",
                         duration_ms=int(md.group(1)) * 1000 if md else 0))
        if len(out) >= limit:
            break
    return out


_MUZMO_ROW = re.compile(r'data-file="(/[^"]+\.mp3[^"]*)"\s+data-title="([^"]*)"')


async def muzmo_search(query: str, limit: int = 30) -> list[Track]:
    """muzmo.cc: [] при любом сбое. Название уже «Артист - Трек»."""
    try:
        async with httpx.AsyncClient(timeout=_SRC_TIMEOUT, follow_redirects=True,
                                     headers=_BROWSER_UA) as cl:
            r = await cl.get("https://rmr.muzmo.cc/search", params={"q": query})
            if r.status_code != 200:
                log.info("muzmo: HTTP %d", r.status_code)
                return []
            text = r.text
    except Exception as e:  # noqa: BLE001
        log.warning("muzmo: %r", e)
        return []
    return [Track(url="https://rmr.muzmo.cc" + _html.unescape(m.group(1)),
                  title=_clean(m.group(2)))
            for m in list(_MUZMO_ROW.finditer(text))[:limit]]


# ---------------------------------------------------------------- mp3xa / drivemusic
# Проверено с ПК 22.11: mp3xa.me — поиск GET /search/?story=..., прямые mp3 на
# cdnproxy*.mp3xa.me (206, ID3, полная длительность); ru.drivemusic.me — поиск
# только POST-ом в корень (GET редиректит на главную), выдача короткая (1–9),
# поэтому источник вспомогательный. Что не подошло тогда же: hotplayer отдаёт
# HLS (m3u8, а плееру колонки нужен mp3), Audius отвечает 403, hitmo/zaycev/
# lightaudio/sefon/зеркала muzofond — недоступны или без прямых ссылок.
_MP3XA_DUR = re.compile(r'<span class="duration">(\d+):(\d+)</span>')


def mp3xa_parse(text: str, limit: int = 40) -> list[Track]:
    out: list[Track] = []
    for chunk in text.split('class="plyr-item')[1:]:
        mu = re.search(r'data-url="(https?://[^"]+\.mp3[^"]*)"', chunk)
        mt = re.search(r'<div class="song_name">\s*<a[^>]*>(.*?)</a>', chunk, re.S)
        if not mu or not mt:
            continue
        ma = re.search(r'<div class="name"><a[^>]*>(.*?)</a>', chunk, re.S)
        md = _MP3XA_DUR.search(chunk)
        out.append(Track(url=_html.unescape(mu.group(1)), title=_clean(mt.group(1)),
                         artist=_clean(ma.group(1)) if ma else "",
                         duration_ms=(int(md.group(1)) * 60 + int(md.group(2))) * 1000
                         if md else 0))
        if len(out) >= limit:
            break
    return out


async def mp3xa_search(query: str, limit: int = 40) -> list[Track]:
    """mp3xa.me (rus.mp3xa.me): [] при любом сбое (вызывается в gather)."""
    try:
        async with httpx.AsyncClient(timeout=_SRC_TIMEOUT, follow_redirects=True,
                                     headers=_BROWSER_UA) as cl:
            r = await cl.get("https://mp3xa.me/search/", params={"story": query})
            if r.status_code != 200:
                log.info("mp3xa: HTTP %d", r.status_code)
                return []
            text = r.text
    except Exception as e:  # noqa: BLE001
        log.warning("mp3xa: %r", e)
        return []
    return mp3xa_parse(text, limit)


def drivemusic_parse(text: str, limit: int = 25) -> list[Track]:
    # Только страница результатов: на главной те же блоки с «популярным» — при
    # недоступном поиске сервер отдаёт её, и чужие треки попали бы в очередь.
    if "c-search-results" not in text:
        return []
    out: list[Track] = []
    for chunk in text.split('class="music-popular__item"')[1:]:
        mu = re.search(r'data-url="(https?://[^"]+\.mp3[^"]*)"', chunk)
        if not mu:
            continue
        ma = re.search(r'class="popular-play-author">(.*?)</a>', chunk, re.S)
        out.append(Track(url=_html.unescape(mu.group(1)),
                         title=_clean(ma.group(1)) or "трек"))
        if len(out) >= limit:
            break
    return out


async def drivemusic_search(query: str, limit: int = 25) -> list[Track]:
    """ru.drivemusic.me: поиск POST-ом в корень. [] при любом сбое."""
    try:
        async with httpx.AsyncClient(timeout=_SRC_TIMEOUT, follow_redirects=True,
                                     headers=_BROWSER_UA) as cl:
            await cl.get("https://ru.drivemusic.me/")   # cookie сессии
            r = await cl.post("https://ru.drivemusic.me/",
                              data={"do": "search", "subaction": "search",
                                    "story": query})
            if r.status_code != 200:
                log.info("drivemusic: HTTP %d", r.status_code)
                return []
            text = r.text
    except Exception as e:  # noqa: BLE001
        log.warning("drivemusic: %r", e)
        return []
    return drivemusic_parse(text, limit)


# ---------------------------------------------------------------- слияние и проба
def _is_audio(b: bytes) -> bool:
    """Аудио по первым байтам, а не по Content-Type (mp3party врёт заголовком)."""
    return (b[:3] == b"ID3" or (len(b) > 1 and b[0] == 0xFF and b[1] & 0xE0 == 0xE0)
            or b[4:8] == b"ftyp" or b[:4] in (b"OggS", b"fLaC", b"RIFF"))


async def track_alive(url: str) -> bool:
    if url.startswith("http://127.0.0.1"):
        return True
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=4.0),
                                     follow_redirects=True, headers=_BROWSER_UA) as cl:
            r = await cl.get(url, headers={"Range": "bytes=0-2047"})
            return r.status_code < 400 and _is_audio(r.content)
    except Exception as e:  # noqa: BLE001
        log.info("проба трека: %r", e)
        return False


# Шлак, который нельзя ставить вперёд оригинала: минусы, караоке, каверы,
# ремиксы, «ускоренные» версии и т.п. (живая жалоба 20.11: на популярные
# песни выдавало шлак).
_JUNK = re.compile(
    r"\b(минус|минусовка|караоке|karaoke|cover|кавер|tribute|instrumental|"
    r"инструментал|slowed|sped up|speed up|nightcore|8d|reverb|bassboost|"
    r"bass boost|нейросет\w*|ai cover|пародия|переделка|dance mix|club mix|"
    r"extended mix|dj mix|фонк|phonk)\b")


def _canon_score(full: str, qwords: list[str], pop_map: dict[str, int],
                 dz: dict | None) -> tuple[int, int]:
    """Счёт кандидата: слова запроса + популярность Deezer + совпадение с
    каноничным (мейнстримным) вариантом, со штрафом за шлак."""
    tn = _norm(full)
    score = sum(1 for w in qwords if w in tn)
    pop = 0
    for k, v in pop_map.items():
        kwords = [w for w in k.split() if len(w) > 2]
        if kwords and all(w in tn for w in kwords):
            pop = max(pop, v)
    if score and pop:
        score += int(15 * (pop ** 0.1))
    if dz:
        ba, bt = dz.get("best_artist") or "", dz.get("best_title") or ""
        has_artist = bool(ba) and ba in tn
        if has_artist:
            score += 45          # тот самый исполнитель
        if bt and all(w in tn for w in bt.split() if len(w) > 2):
            score += 35          # то самое название
        if not has_artist:
            for other in dz.get("artists") or ():
                if other and other != ba and other in tn:
                    score -= 30  # другая известная версия — чаще кавер
                    break
        if _JUNK.search(tn):
            score -= 60
    return score, pop


def rank_tracks(query: str, tracks: list["Track"], dz: dict | None = None) -> list["Track"]:
    """Переставить треки: мейнстримный оригинал вперёд, шлак — в хвост."""
    pop_map = (dz or {}).get("pop") or {}
    qwords = [w for w in _norm(query).split() if len(w) > 2]
    rows = []
    for pos, t in enumerate(tracks or []):
        full = f"{t.artist} {t.title}".strip()
        score, pop = _canon_score(full, qwords, pop_map, dz)
        if score > 0:
            rows.append((-score, -pop, pos, t))
    rows.sort(key=lambda r: r[:3])
    return [r[3] for r in rows]


def merge_sources(query: str, pop_map: dict[str, int] | None,
                  *sources: list[Track], dz: dict | None = None,
                  limit: int = 40) -> list[Track]:
    """Одна очередь из нескольких баз: по релеванции, популярности и близости
    к мейнстримному варианту; без дублей. При равном счёте выше та база,
    что передана раньше."""
    pop_map = pop_map or {}
    qwords = [w for w in _norm(query).split() if len(w) > 2]
    seen: set[str] = set()
    rows = []
    for si, tracks in enumerate(sources):
        for pos, t in enumerate(tracks or []):
            full = f"{t.artist} {t.title}".strip()
            key = _norm(full)
            if not key or key in seen:
                continue
            seen.add(key)
            score, pop = _canon_score(full, qwords, pop_map, dz)
            if score > 0:
                rows.append((-score, -pop, si, pos, t))
    rows.sort(key=lambda r: r[:4])
    return [r[4] for r in rows[:limit]]


# ---------------------------------------------------------------- Internet Archive
async def ia_search(query: str, limit: int = 25) -> list[Track]:
    """Свободная музыка и аудиокниги с archive.org."""
    try:
        q = f'title:({query}) AND mediatype:audio'
        async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True,
                                     headers=_UA) as cl:
            r = await cl.get("https://archive.org/advancedsearch.php",
                             params={"q": q, "fl[]": ["identifier", "title"],
                                     "rows": 3, "output": "json"})
            docs = r.json().get("response", {}).get("docs") or []
            if not docs:
                return []
            ident = docs[0]["identifier"]
            r2 = await cl.get(f"https://archive.org/metadata/{quote(ident)}")
            files = (r2.json() or {}).get("files") or []
    except Exception as e:
        log.warning("ia: %r", e)
        return []
    tracks = []
    for f in files:
        name = f.get("name", "")
        if not name.lower().endswith(".mp3"):
            continue
        url = f"https://archive.org/download/{quote(ident)}/{quote(name)}"
        tracks.append(Track(url=url, title=name[:-4]))
        if len(tracks) >= limit:
            break
    return tracks


# ---------------------------------------------------------------- радио
# Стемы названий: ASR отдаёт падежи («энергию», «европу плюс»), а ключи в RADIO —
# именительный, и прямое сравнение не ловило форму — включался Маяк (18.11).
_RADIO_STEMS = (
    ("маяк", "маяк"), ("вести", "вести"), ("русск", "русское"), ("европ", "европа плюс"),
    ("dfm", "dfm"), ("дфм", "dfm"), ("хит", "хит"), ("максим", "максимум"),
)


def radio_pick(query: str) -> tuple[str, str]:
    """(название, url) из встроенного списка; дефолт — Маяк."""
    q = _norm(query or RADIO_DEFAULT)
    for stem, key in _RADIO_STEMS:
        # только левая граница: стем должен ЛОВИТЬ падежи («энергию», «европу»
        # плюс, «русское радио»), хвост слова — любая флексия
        if re.search(rf"(?<!\w){re.escape(stem)}", q):
            name, url = RADIO[key]
            return name, url
    return RADIO[RADIO_DEFAULT]


# ---------------------------------------------------------------- аудиокниги
# Отбор перенесён из ПК-версии (alice_server/streams.py), где он проверен на
# каталоге. Прежний поиск lite брал первую выдачу архива: вместо «Мастера и
# Маргариты» включался альбом мюзикла, вместо «Войны и мира» — «Война за мир»,
# а «Колобок» не находился вовсе (проверено с колонки 17.11).
_BOOK_MUSIC = re.compile(r"\b(опера|опер|балет|оратори|симфонич|оркестр|филармон|концерт"
                         r"|прокофьев|чайковск|римск|мусоргск|шостакович|мюзикл|musical"
                         r"|cast album|soundtrack)\b")
_BOOK_YEAR = re.compile(r"\b(19|20)\d{2}\b")
_BOOK_HINT = re.compile(r"(аудиокниг|audiobook|librivox|читает|сказк)")
_BOOK_STOP = {"книга", "аудиокнига", "сказка", "the", "and"}
# Полки аудиокниг в архиве — отдельные КАТАЛОГИ поверх общего поиска:
# librivoxaudio (22 тыс. свободных книг), audio_bookspoetry (книги и поэзия,
# включает старые LibriVox-записи), audioboo_ru (тысячи русских аудиокниг).
# Без них выдача «(запрос) AND mediatype:audio» тонет в подкастах и музыке.
_BOOK_SHELVES = ("librivoxaudio", "audio_bookspoetry", "audioboo_ru")
_BOOK_SHELF_EN = {"librivoxaudio", "audio_bookspoetry"}
_BOOK_SHELF_RU = {"audioboo_ru", "audioboo"}
_BOOK_SHELF_Q = " OR ".join(f"collection:{c}" for c in _BOOK_SHELVES)
_TRANSLIT = {"а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
             "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
             "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
             "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
             "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya"}


def _translit(s: str) -> str:
    return "".join(_TRANSLIT.get(c, c) for c in s.lower())


async def _ia_query(cl: httpx.AsyncClient, q: str, rows: int = 15) -> list[dict]:
    try:
        r = await cl.get("https://archive.org/advancedsearch.php",
                         params={"q": q, "rows": rows, "output": "json",
                                 "fl[]": ["identifier", "title", "language",
                                          "downloads", "collection"]})
        r.raise_for_status()
        return (r.json().get("response", {}) or {}).get("docs", []) or []
    except Exception as e:  # noqa: BLE001
        log.warning("book: поиск %r: %r", q[:40], e)
        return []


def _collections(d: dict) -> str:
    """collection бывает строкой и списком — всегда возвращаем строку в нижнем регистре."""
    coll = d.get("collection")
    if isinstance(coll, (list, tuple)):
        coll = " ".join(str(c) for c in coll)
    return str(coll or "").lower()


def rank_books(docs: list[dict], q: str) -> list[tuple[str, str]]:
    """Кандидаты-аудиокниги по убыванию качества: [(identifier, title)].

    Точная фраза в названии важнее отдельных слов («война и мир» != «война за
    мир»), оперы/мюзиклы/записи спектаклей штрафуются, русская версия и явные
    признаки аудиокниги — в плюс, число загрузок — слабый тай-брейкер.
    """
    qn = _norm(q)
    q_words = {w for w in qn.split() if len(w) > 2} - _BOOK_STOP
    cyr_q = bool(re.search("[а-яё]", qn))

    def score(d: dict) -> int:
        raw = str(d.get("title", ""))
        title, ident = _norm(raw), _norm(str(d.get("identifier", "")))
        lang = str(d.get("language", "")).lower()
        coll = _collections(d)
        s, matched = 0, 0
        if qn and qn in title:
            s += 150
        for w in q_words:
            stem = w[:5]
            if stem in title:
                s += 120
                matched += 1
            elif stem in ident:
                s += 60
                matched += 1
        if q_words and matched == 0:
            return -1
        if matched < len(q_words):
            s -= 100
        if _BOOK_MUSIC.search(title):
            s -= 200
        if _BOOK_YEAR.search(raw):
            s -= 80
        if "rus" in lang:
            s += 30
        # сербский/македонский/украинский сборник: буквы, которых нет в русском
        # («12 најдобри приказни за Шерлок Холмс» вставал первым)
        if re.search("[јњљџќѓѕіїєґў]", raw.lower()):
            s -= 150
        if "librivox" in ident or "librivox" in coll:
            # английская озвучка русского названия — только если русского
            # источника нет вовсе (иначе «Война и мир» врубалась по-английски)
            s += 20 if ("rus" in lang or not cyr_q) else -60
        if any(c in coll for c in _BOOK_SHELF_RU):
            s += 25        # русская аудиокнижная полка — отдельная база
        if any(c in coll for c in _BOOK_SHELVES):
            s += 10        # книга с книжной полки, а не случайный подкаст
        if _BOOK_HINT.search(title + " " + ident):
            s += 40
        try:
            s += min(int(d.get("downloads", 0) or 0) // 2000, 15)
        except (TypeError, ValueError):
            pass
        return s

    scored = [(score(d), d) for d in docs if d.get("identifier")]
    scored = [x for x in scored if x[0] >= 0]
    scored.sort(key=lambda x: -x[0])
    return [(d["identifier"],
             re.sub(r"\s+", " ", str(d.get("title") or d["identifier"]))[:80])
            for _, d in scored]


async def book_search(query: str) -> list[tuple[str, str]]:
    """Кандидаты с archive.org, лучший первым. [] при сбое.

    Все варианты запроса уходят одной волной: по названию и по всему тексту,
    как сказано и транслитом (классика часто лежит латиницей).
    """
    q = (query or "").strip()
    if not q:
        return []
    # «сказка колобок» — слово-жанр мешает поиску по названию
    bare = re.sub(r"^(сказк\w*|аудиокниг\w*|книг\w*)\s+", "", _norm(q)) or _norm(q)
    variants = list(dict.fromkeys([q, bare]))
    if re.search("[а-яё]", bare):
        variants.append(_translit(bare))
    async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=6.0),
                                 follow_redirects=True, headers=_UA) as cl:
        res = await asyncio.gather(
            *[_ia_query(cl, f"title:({v}) AND mediatype:audio") for v in variants],
            *[_ia_query(cl, f"({v}) AND mediatype:audio") for v in variants],
            # книжные полки отдельными запросами (шире по rows): общий поиск
            # их почти не показывает — подкасты и музыка вытесняют
            *[_ia_query(cl, f"({v}) AND mediatype:audio AND ({_BOOK_SHELF_Q})",
                        rows=25) for v in variants[:2]])
    docs, seen = [], set()
    for lst in res:
        for d in lst:
            ident = d.get("identifier")
            if ident and ident not in seen:
                seen.add(ident)
                docs.append(d)
    return rank_books(docs, bare)


def book_files(files: list[dict]) -> list[dict]:
    """mp3-главы по порядку, без дублей качества (_64kb/_128kb — та же глава)."""
    mp3 = []
    for f in files:
        name = (f.get("name") or "") if isinstance(f, dict) else ""
        if name.lower().endswith(".mp3") and not re.search(r"_(64|128)kb\b", name, re.I):
            mp3.append(f)

    def key(f: dict) -> tuple:
        name = f["name"].lower()
        # расширение отрезаем: в «intro.mp3» цифра 3 — не номер главы
        stem = re.sub(r"\.[a-z0-9]+$", "", name)
        if re.match(r"^(intro|prologue|preface|foreword|dedicat|opening|вступлен)", stem):
            return (-1, [], name)
        # естественная сортировка: «part_10» после «part_2»
        return (0, [int(x) for x in re.findall(r"\d+", stem)] or [10 ** 9], name)

    mp3.sort(key=key)
    return mp3


async def book_tracks(ident: str, title: str, limit: int = 400) -> list[Track]:
    """Главы книги -> очередь."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=6.0),
                                     follow_redirects=True, headers=_UA) as cl:
            r = await cl.get(f"https://archive.org/metadata/{quote(ident)}")
            files = (r.json() or {}).get("files") or []
    except Exception as e:  # noqa: BLE001
        log.warning("book meta: %r", e)
        return []
    out = []
    for i, f in enumerate(book_files(files)[:limit], 1):
        try:
            dur = int(float(f.get("length", 0) or 0) * 1000)
        except (TypeError, ValueError):
            dur = 0   # бывает «12:34» — длительность не критична
        out.append(Track(url=f"https://archive.org/download/{quote(ident)}/{quote(f['name'])}",
                         title=f"{title}, часть {i}", artist=title, duration_ms=dur))
    return out


# ---------------------------------------------------------------- фасад
class Streams:
    """Очереди воспроизведения по устройствам."""

    def __init__(self, cfg=None):
        self.cfg = cfg
        self.sessions: dict[str, Session] = {}
        # место в книге переживает рестарт lite и выключение колонки
        self._books_file = Path(os.environ.get("LITE_DIR", "/data/opt/column_lite")) / "books.json"
        try:
            raw = json.loads(self._books_file.read_text("utf-8"))
        except (OSError, ValueError):
            raw = {}
        # Список/число в books.json ронял classify на каждой «продолжай»:
        # AttributeError: 'list' object has no attribute 'get' (аудит 22.11).
        # Файл не наш — просто живём без сохранённых книг.
        self._books: dict = raw if isinstance(raw, dict) else {}

    async def music(self, device: str, query: str) -> tuple[Session | None, str]:
        """Музыка: SkySound7 + muzofond + mp3xa + muzmo + drivemusic -> IA.

        Архивную задачу гасим в finally: когда источники сработали, ответ
        архива не нужен, а раньше запрос продолжал висеть до своего таймаута
        на КАЖДЫЙ поиск музыки (аудит 22.11).
        """
        self._book_inactive(device)
        # Все базы и мейнстрим Deezer опрашиваем параллельно.
        ia_task = asyncio.create_task(ia_search(query))
        try:
            return await self._music(device, query, ia_task)
        finally:
            if not ia_task.done():
                ia_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await ia_task

    async def _music(self, device: str, query: str,
                     ia_task: "asyncio.Task") -> tuple[Session | None, str]:
        html, dz, fond, xa, muzmo, drive = await asyncio.gather(
            skysound_fetch(query), deezer_resolve(query),
            muzofond_search(query), mp3xa_search(query),
            muzmo_search(query), drivemusic_search(query))
        sky = merge_sources(query, dz["pop"], skysound_score(html, query, dz["pop"]),
                            fond, xa, muzmo, drive, dz=dz)
        if sky:
            log.info("музыка: «%s» -> %d треков (muzofond %d, mp3xa %d, muzmo %d, "
                     "drivemusic %d); мейнстрим: %r — %r, первый: %s", query,
                     len(sky), len(fond), len(xa), len(muzmo), len(drive),
                     dz["best_artist"], dz["best_title"], sky[0].title)
            # Первый трек проверяем ДО «Включаю»: мёртвая ссылка = обещание и тишина.
            # Проба не дольше 2.5 с: зависший CDN не должен задерживать ответ
            # (без ограничения запрос однажды занял 8 с вместо 2).
            head = sky[:3]

            async def _probe(u: str) -> bool:
                try:
                    return await asyncio.wait_for(track_alive(u), timeout=2.5)
                except asyncio.TimeoutError:
                    return False
            ok = await asyncio.gather(*(_probe(t.url) for t in head))
            first = next((i for i, a in enumerate(ok) if a), None)
            if first is None:
                log.warning("музыка: первые %d треков не отвечают", len(head))
            elif first:
                sky = sky[first:]
        if not sky:
            # SkySound по полному запросу пусто/404, а по короткому ищет:
            # «гузель уразова» → 404, «гузель» → есть. Пробуем укоротить,
            # отбрасывая хвост по слову (длинный хвост даёт 404 чаще).
            words = query.split()
            for cut in (2, 1):
                if len(words) > cut:
                    short = " ".join(words[:cut])
                    html2 = await skysound_fetch(short)
                    sky = rank_tracks(short, skysound_score(html2, short, dz["pop"]), dz)
                    if sky:
                        log.info("музыка: «%s» -> короткий запрос «%s»", query, short)
                        break
        if sky:
            s = Session(kind="music", title=query, queue=sky)
            self.sessions[device] = s
            return s, f"Нашла {len(sky)} треков по запросу «{query}». Включаю."
        ia = rank_tracks(query, await ia_task, dz)
        if ia:
            s = Session(kind="music", title=query, queue=ia)
            self.sessions[device] = s
            return s, f"Взяла {len(ia)} записей из архива по запросу «{query}». Включаю."
        return None, f"Не нашла музыку по запросу «{query}»."

    async def wave(self, device: str) -> tuple[Session | None, str]:
        """«Включи музыку / мою волну» без облака.

        Личной волны у lite нет (она живёт в аккаунте), поэтому волна —
        это топ Deezer, найденный в наших бесплатных базах. Раньше фраза
        не доходила сюда вовсе: classify отдавал её облаку/LLM, и колонка
        отвечала текстом вместо музыки (аудит 22.11).
        """
        self._book_inactive(device)
        chart = await deezer_chart()
        tracks: list[Track] = []
        seen: set[str] = set()
        if chart:
            # чарт-песни ищем параллельно: 4 запроса одной волной
            lists = await asyncio.gather(
                *(muzofond_search(f"{a} {t}", limit=6) for a, t in chart[:4]))
            for lst in lists:
                for t in lst:
                    key = _norm(f"{t.artist} {t.title}")
                    if key and key not in seen:
                        seen.add(key)
                        tracks.append(t)
        if not tracks:
            # чарт или Deezer недоступны — хотя бы тематическая подборка
            tracks = await muzofond_search("популярная музыка", limit=30)
        if not tracks:
            return None, "Не смогла подобрать музыку. Скажи исполнителя или включи радио."
        s = Session(kind="music", title="Моя волна", queue=tracks[:40])
        self.sessions[device] = s
        log.info("волна: %d треков (чарт %d)", len(s.queue), len(chart))
        return s, "Включаю музыку."

    def radio(self, device: str, query: str = "") -> Session:
        self._book_inactive(device)
        name, url = radio_pick(query)
        # Через локальный прокси: gstreamer колонки падает на ICY-потоках
        # ("streaming stopped, reason not-linked" в mediad.log), а /_radio
        # вырезает ICY-метаданные и отдаёт чистый audio/mpeg. Порт — из
        # конфига: жёсткий URL без порта ломался при http_port != 80 (аудит 22.11).
        port = int(getattr(self.cfg, "http_port", 80) or 80) if self.cfg is not None else 80
        proxy = f"http://127.0.0.1:{port}/_radio?url={quote(url, safe='')}"
        # НОВЫЙ ключ стрима на каждый запуск (включая авторетрай): mediad
        # переиспользует кешированного клиента по ключу и делает seek
        # (лог 19.11: «Internal pending seek to new position 2.08s» -> ERROR
        # на 32749 байте). Уникальный ключ = свежий плеер с нуля.
        s = Session(kind="radio", title=name,
                    queue=[Track(url=proxy, title=name)],
                    key=name + "@" + uuid.uuid4().hex[:6])
        self.sessions[device] = s
        return s

    def _remember(self, device: str, s: "Session | None") -> None:
        """Запомнить книгу и главу (для «продолжай книгу»)."""
        if s is None or s.kind != "book" or not s.identifier:
            return
        # active — книга сейчас играет: по этому признаку очередь восстанавливается,
        # если lite перезапустился посреди книги (иначе колонка доигрывала часть
        # и останавливалась — спросить следующую было не у кого)
        self._books[device] = {"identifier": s.identifier, "title": s.title,
                               "pos": s.started, "ts": time.time(), "active": True,
                               # секунда внутри главы: «продолжай книгу» после
                               # рестарта lite вернётся туда же, а не в начало главы
                               "offset": round(max(0.0, s.start_offset), 1)}
        self._write_books()

    def _write_books(self) -> None:
        try:
            self._books_file.parent.mkdir(parents=True, exist_ok=True)
            # atomic write, как у таймеров: обрыв питания посреди записи не
            # должен оставить усечённый JSON — тогда разом теряются позиции
            # ВСЕХ книг (аудит 22.11).
            tmp = self._books_file.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._books, ensure_ascii=False), "utf-8")
            os.replace(tmp, self._books_file)
        except OSError as e:
            log.warning("не сохранил books.json: %r", e)

    def book_active(self, device: str) -> bool:
        """Книга играла последней и недавно (не дольше 6 ч назад)."""
        rec = self._books.get(device) or {}
        return bool(rec.get("identifier") and rec.get("active")
                    and time.time() - float(rec.get("ts") or 0) < 6 * 3600)

    def _book_inactive(self, device: str) -> None:
        """Включили музыку или радио — книга больше не «текущая»."""
        rec = self._books.get(device)
        if rec and rec.get("active"):
            rec["active"] = False
            self._write_books()

    def has_saved_book(self, device: str) -> bool:
        # защита в самом методе: _books может прийти битым, если его успел
        # подменить внешний код; на каждой «продолжай» ошибка тут глушит всё
        rec = self._books.get(device) if isinstance(self._books, dict) else None
        return bool(isinstance(rec, dict) and rec.get("identifier"))

    def advance(self, device: str) -> "Session | None":
        """Колонка доиграла трек и просит следующий. None — играть больше нечего."""
        s = self.sessions.get(device)
        if s is None or s.kind == "radio" or s.pos + 1 >= len(s.queue):
            return None
        s.pos += 1
        # новый трек — любая отложенная «пауза» от прошлого больше не в силе
        s.gen += 1
        s.paused = False
        s.paused_at = 0.0
        # start_offset НЕ трогаем: это предзагрузка, текущий трек ещё играет
        s.key = ""   # не переиспользуем id стрима от предыдущего трека
        return s

    def mark_started(self, device: str) -> None:
        """Колонка сообщила, что выданный трек заиграл: фиксируем место в книге."""
        s = self.sessions.get(device)
        if s is None:
            return
        s.started = s.pos
        s.fails = 0
        s.paused = False
        s.paused_at = 0.0
        s.start_ts = time.monotonic()
        s.start_offset = s.pending_offset
        s.gen += 1
        self._remember(device, s)

    def remember_offset(self, device: str, offset: float) -> None:
        """Секунда внутри главы книги: «продолжай книгу» после рестарта."""
        rec = self._books.get(device)
        if not rec or not rec.get("identifier"):
            return
        rec["offset"] = round(float(offset), 1)
        rec["ts"] = time.time()
        self._write_books()

    async def book(self, device: str, query: str) -> tuple[Session | None, str]:
        # 6 кандидатов: с полками IA часть записей без mp3-глав — берём следующих
        for ident, title in (await book_search(query))[:6]:
            queue = await book_tracks(ident, title)
            if not queue:
                continue   # запись без mp3-глав — берём следующего кандидата
            s = Session(kind="book", title=title, queue=queue, identifier=ident)
            self.sessions[device] = s
            self._remember(device, s)
            log.info("книга: «%s» (%s), %d частей", title, ident, len(queue))
            return s, f"Нашла «{title}», {len(queue)} частей. Включаю."
        return None, f"Не нашла аудиокнигу «{query}»."

    async def resume_book(self, device: str) -> tuple[Session | None, str]:
        rec = self._books.get(device) or {}
        if not rec.get("identifier"):
            return None, "Сохранённой книги нет. Скажите, какую включить."
        title = rec.get("title") or "книга"
        queue = await book_tracks(rec["identifier"], title)
        if not queue:
            return None, "Сохранённая книга сейчас недоступна."
        pos = min(max(int(rec.get("pos", 0) or 0), 0), len(queue) - 1)
        offset = max(0.0, float(rec.get("offset") or 0.0))
        s = Session(kind="book", title=title, queue=queue, pos=pos, started=pos,
                    identifier=rec["identifier"], start_offset=offset)
        s.start_ts = time.monotonic()
        # свежий id стрима: mediad кеширует плеера по id и может досекать на
        # старую позицию мимо offset_ms
        s.key = f"{title}:{pos}@{uuid.uuid4().hex[:6]}"
        self.sessions[device] = s
        if offset >= 1.0:
            # время словами: «место: сорок семь секунд» вместо «с 0:47»
            from .tts import duration_words
            return s, f"Продолжаю «{title}», часть {pos + 1}. Место: {duration_words(offset)}."
        return s, f"Продолжаю «{title}» с части {pos + 1}."

    def control(self, device: str, action: str) -> tuple[str, Session | None, bool]:
        """next/prev/stop. Третий элемент — изменилась ли позиция.

        Раньше на «следующий» в конце очереди возвращался тот же session, и
        main.py видел s.current (последний трек) и перезапускал его заново под
        фразу «Треки закончились». Теперь явно сообщаем, что играть нечего.
        """
        s = self.sessions.get(device)
        if not s:
            return "Сейчас ничего не играет.", None, False
        if action == "next":
            # от играющего трека, а не от предзагруженного: иначе «дальше»
            # перескакивало бы через один
            s.pos = s.started
            t = s.next()
            s.started = s.pos
            s.key = ""
            s.paused = False
            s.paused_at = 0.0
            s.start_offset = 0.0
            s.start_ts = 0.0
            self._remember(device, s)
            return (f"Включаю: {t.title}" if t else "Треки закончились."), s, t is not None
        if action == "prev":
            s.pos = s.started
            t = s.prev()
            s.started = s.pos
            s.key = ""
            s.paused = False
            s.paused_at = 0.0
            s.start_offset = 0.0
            s.start_ts = 0.0
            self._remember(device, s)
            return (f"Включаю: {t.title}" if t else "Это первый трек."), s, t is not None
        return "", s, False
