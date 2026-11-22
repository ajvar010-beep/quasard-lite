"""Погода на самой колонке: Open-Meteo (координаты из lite.toml) + wttr.in.

Ключей и регистрации не нужно. open-meteo отвечает за ~0.3 с точными данными
по координатам; wttr.in — запасной на случай недоступности (проверено 30.10).
Сеть это позволяет: egress разрешает HTTPS наружу, а погодные хосты не в
подсетях Яндекса.

Числа сразу словами («минус три») — TTS читать цифры не даём.
"""
from __future__ import annotations

import logging
import re

import httpx

from .tts import _plural, num_words

log = logging.getLogger("weather")

_FORECAST = "https://api.open-meteo.com/v1/forecast"
_GEOCODE = "https://geocoding-api.open-meteo.com/v1/search"
_WTTR = "https://wttr.in/"
_TIMEOUT = httpx.Timeout(8.0, connect=5.0)
_CACHE_S = 600          # дольше — лишние запросы; меньше — устаревшие данные

# Падежи городов, которые голосом звучат чаще всего. Геокодер падежи НЕ
# понимает («Москве» -> городок в Нью-Мексико, проверено 20.11), поэтому
# самые частые формы переводим сами; остальные — эвристикой в _candidates.
_ALIASES = {
    "москве": "Москва", "москва": "Москва", "москвы": "Москва",
    "питере": "Санкт-Петербург", "спб": "Санкт-Петербург", "питер": "Санкт-Петербург",
    "санкт-петербурге": "Санкт-Петербург", "санкт петербурге": "Санкт-Петербург",
    "санкт-петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург",
    "ленинграде": "Санкт-Петербург",
    "казани": "Казань", "казань": "Казань",
    "новосибирске": "Новосибирск", "екатеринбурге": "Екатеринбург",
    "нижнем новгороде": "Нижний Новгород", "нижний новгород": "Нижний Новгород",
    "великом новгороде": "Великий Новгород",
    "самаре": "Самара", "омске": "Омск", "челябинске": "Челябинск",
    "ростове-на-дону": "Ростов-на-Дону", "ростове на дону": "Ростов-на-Дону",
    "ростове": "Ростов-на-Дону", "уфе": "Уфа", "красноярске": "Красноярск",
    "воронеже": "Воронеж", "перми": "Пермь", "волгограде": "Волгоград",
    "краснодаре": "Краснодар", "саратове": "Саратов", "тюмени": "Тюмень",
    "твери": "Тверь", "ижевске": "Ижевск", "барнауле": "Барнаул",
    "ульяновске": "Ульяновск", "иркутске": "Иркутск", "хабаровске": "Хабаровск",
    "владивостоке": "Владивосток", "ярославле": "Ярославль",
    "махачкале": "Махачкала", "тольятти": "Тольятти", "сочи": "Сочи",
    "ставрополе": "Ставрополь", "тюмень": "Тюмень", "пензе": "Пенза",
    "липецке": "Липецк", "кирове": "Киров", "чебоксарах": "Чебоксары",
    "туле": "Тула", "калининграде": "Калининград", "мурманске": "Мурманск",
    "архангельске": "Архангельск", "смоленске": "Смоленск", "курске": "Курск",
}
_city_cache: dict[str, "tuple[float, float, str, str] | None"] = {}

# WMO weather interpretation codes -> разговорное описание
_WMO = {
    0: "ясно", 1: "в основном ясно", 2: "переменная облачность", 3: "пасмурно",
    45: "туман", 48: "изморозь",
    51: "слабая морось", 53: "морось", 55: "сильная морось",
    56: "ледяная морось", 57: "сильная ледяная морось",
    61: "небольшой дождь", 63: "дождь", 65: "сильный дождь",
    66: "ледяной дождь", 67: "сильный ледяной дождь",
    71: "небольшой снег", 73: "снег", 75: "сильный снег", 77: "снежные зёрна",
    80: "небольшой ливень", 81: "ливень", 82: "сильный ливень",
    85: "небольшой снегопад", 86: "снегопад",
    95: "гроза", 96: "гроза с небольшим градом", 99: "гроза с градом",
}
_WET = {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99}
_SNOW = {71, 73, 75, 77, 85, 86}
_WIND_DIRS = ("северный", "северо-восточный", "восточный", "юго-восточный",
              "южный", "юго-западный", "западный", "северо-западный")
_WTTR_DIR = {"N": "северный", "NNE": "северо-восточный", "NE": "северо-восточный",
             "ENE": "восточный", "E": "восточный", "ESE": "юго-восточный",
             "SE": "юго-восточный", "SSE": "южный", "S": "южный",
             "SSW": "юго-западный", "SW": "юго-западный", "WSW": "западный",
             "W": "западный", "WNW": "северо-западный", "NW": "северо-западный",
             "NNW": "северный"}

_cities_cache: dict = {}
# Источник, только что не ответивший, не дёргаем 15 минут: api.open-meteo.com
# с домашней сети колонки таймаутится на connect (проверено 20.11: IP
# 157.90.83.168 молчит), и без паузы каждый вопрос ждал лишние 5 секунд.
_dead: dict = {}


def _key(lat: float, lon: float) -> tuple:
    return round(lat, 3), round(lon, 3)


async def fetch(lat: float, lon: float, tz: str = "Europe/Moscow") -> dict | None:
    """Данные погоды с кэшем на 10 минут (на каждый город свой). None — провал."""
    import time as _time
    key = _key(lat, lon)
    now = _time.monotonic()
    hit = _cities_cache.get(key)
    if hit is not None and now - hit[0] < _CACHE_S:
        return hit[1]
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True,
                                 headers={"User-Agent": "quasard-lite/1.0"}) as cl:
        for name, fn in (("open-meteo", lambda c: _open_meteo(c, lat, lon, tz)),
                         ("wttr.in", lambda c: _wttr(c, lat, lon))):
            if now < _dead.get(name, 0.0):
                continue
            try:
                data = await fn(cl)
                if data and data.get("days"):
                    log.info("погода: источник %s", name)
                    if len(_cities_cache) > 16:     # 238 МБ RAM на колонке
                        _cities_cache.clear()
                    _cities_cache[key] = (now, data)
                    return data
            except (httpx.HTTPError, OSError) as e:
                # Паузу ставим ТОЛЬКО на сетевые сбои: раньше любой разбор
                # ошибки (например, битый JSON) выключал источник на 15 минут
                # (аудит 22.11).
                _dead[name] = now + 900
                log.warning("погода через %s не удалась (пауза 15 мин): %r", name, e)
            except Exception as e:  # noqa: BLE001
                log.warning("погода через %s: %r", name, e)
    return None


def describe(code: int) -> str:
    return _WMO.get(int(code), "облачно")


def temp_words(t: float) -> str:
    """«минус три» / «плюс двадцать два» — для озвучки, без цифр и знаков."""
    n = int(round(abs(t)))
    sign = "минус" if t < 0 and n > 0 else ("плюс" if t > 0 and n > 0 else "")
    return f"{sign} {num_words(n)}".strip()


def wind_words(ms: float) -> str:
    n = int(round(ms))
    return f"{num_words(n)} {_plural(n, 'метр', 'метра', 'метров')} в секунду"


def _wind_dir(deg: float) -> str:
    return _WIND_DIRS[int((deg + 22.5) % 360 // 45)]


def _wttr_code_to_wmo(code: int) -> int:
    if code == 113:
        return 0
    if code == 116:
        return 2
    if code in (119, 122):
        return 3
    if code in (143, 248, 260):
        return 45
    if code in (176, 263, 266, 293, 296, 353):
        return 61
    if code in (299, 302, 305, 308, 356, 359):
        return 63
    if code in (179, 227, 230, 323, 326, 329, 332, 335, 338, 368, 371, 392, 395):
        return 71
    if code in (182, 185, 281, 284, 311, 314, 317, 320, 350, 362, 365, 374, 377):
        return 66
    if code in (200, 386, 389):
        return 95
    return 3


async def _open_meteo(cl, lat: float, lon: float, tz: str) -> dict | None:
    r = await cl.get(_FORECAST, params={
        "latitude": lat, "longitude": lon,
        "current": "temperature_2m,apparent_temperature,weather_code,"
                   "wind_speed_10m,wind_direction_10m",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,"
                 "precipitation_probability_max",
        "forecast_days": 2, "timezone": tz, "wind_speed_unit": "ms",
    })
    r.raise_for_status()
    d = r.json()
    cur = d.get("current") or {}
    daily = d.get("daily") or {}
    times = daily.get("time") or []
    days = []
    for i, day in enumerate(times[:2]):
        days.append({
            "date": day,
            "min": (daily.get("temperature_2m_min") or [0])[i],
            "max": (daily.get("temperature_2m_max") or [0])[i],
            "code": (daily.get("weather_code") or [0])[i],
            "pop": (daily.get("precipitation_probability_max") or [0])[i],
        })
    return {"cur": {"t": cur.get("temperature_2m", 0),
                    "feels": cur.get("apparent_temperature", 0),
                    "code": cur.get("weather_code", 0),
                    "wind": cur.get("wind_speed_10m", 0),
                    "winddir": cur.get("wind_direction_10m", 0)},
            "days": days}


async def _wttr(cl, lat: float, lon: float) -> dict | None:
    r = await cl.get(f"{_WTTR}~{lat},{lon}", params={"format": "j1", "lang": "ru"})
    r.raise_for_status()
    d = r.json()
    cur = (d.get("current_condition") or [{}])[0]
    days = []
    for wd in (d.get("weather") or [])[:2]:
        hourly = wd.get("hourly") or []
        noon = hourly[4] if len(hourly) > 4 else (hourly[0] if hourly else {})
        days.append({
            "date": wd.get("date", ""),
            "min": float(wd.get("mintempC", 0)),
            "max": float(wd.get("maxtempC", 0)),
            # wttr.in кладёт АНГЛИЙСКИЙ текст в lang_ru у current_condition
            # (баг их API) — описываем сами по коду погоды
            "code": _wttr_code_to_wmo(int(noon.get("weatherCode", 0) or 0)),
            "pop": int(noon.get("chanceofrain", 0) or 0),
        })
    return {"cur": {"t": float(cur.get("temp_C", 0)),
                    "feels": float(cur.get("FeelsLikeC", 0)),
                    "code": _wttr_code_to_wmo(int(cur.get("weatherCode", 0) or 0)),
                    "wind": float(cur.get("windspeedKmph", 0) or 0) / 3.6,
                    "winddir": _WTTR_DIR.get(cur.get("winddir16Point", ""), "")},
            "days": days}


def _candidates(name: str) -> list[str]:
    """Варианты написания для геокодера: словарь, оригинал, срез падежных окончаний."""
    n = name.strip().lower().replace("ё", "е")
    n = re.sub(r"^(?:городе|город)\s+", "", n)
    out: list[str] = []
    if n in _ALIASES:
        out.append(_ALIASES[n])
    out.append(n)
    if len(n) > 3:
        for suf in ("е", "и", "у", "ю", "ы", "а", "ой", "ей"):
            if n.endswith(suf) and len(n) - len(suf) >= 3:
                out.append(n[:-len(suf)])
        # «Твери» -> «Тверь», «Перми» -> «Пермь»
        if n.endswith("и"):
            out.append(n[:-1] + "ь")
    return list(dict.fromkeys(out))


async def _geocode(cl, name: str) -> dict | None:
    """Первый результат геокодера В РОССИИ: без фильтра «Москве» находил
    Моcкверо в Нью-Мексико, а «Петербург» — городок в Вирджинии."""
    r = await cl.get(_GEOCODE, params={"name": name, "count": 10,
                                       "language": "ru", "format": "json"})
    res = (r.json() or {}).get("results") or []
    for g in res:
        if str(g.get("country_code") or "").upper() == "RU":
            return g
    return None


async def resolve_city(name: str) -> "tuple[float, float, str, str] | None":
    """«в москве» -> (lat, lon, «Москва», timezone). None — не нашли."""
    key = name.strip().lower().replace("ё", "е")
    if key in _city_cache:
        return _city_cache[key]
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True,
                                 headers={"User-Agent": "quasard-lite/1.0"}) as cl:
        for cand in _candidates(name):
            try:
                g = await _geocode(cl, cand)
            except Exception as e:  # noqa: BLE001
                log.warning("геокодер «%s»: %r", cand, e)
                continue
            if g:
                out = (float(g["latitude"]), float(g["longitude"]),
                       g.get("name") or cand, g.get("timezone") or "auto")
                log.info("погода: город «%s» -> %s", name, out[2])
                _city_cache[key] = out
                return out
    # Промах НЕ кэшируем: раньше здесь стоял _city_cache[key] = None, и
    # случайный сбой геокодера навсегда превращал город в «не найден» —
    # вплоть до перезапуска lite (аудит 22.11).
    return None


def _day_word(days: list[dict], i: int) -> str:
    if i == 0:
        return "Сегодня"
    return "Завтра"


def text(kind: str, city: str, data: dict) -> str:
    """kind: now | tomorrow | rain — готовый ответ словами."""
    cur = data["cur"]
    days = data["days"]
    place = f"в городе {city}"
    if kind == "rain":
        d = days[0]
        if int(d["code"]) in _WET or int(d["code"]) in _SNOW:
            return (f"Сегодня {place} {describe(d['code'])}, "
                    f"вероятность осадков {_pct_words(d['pop'])}.")
        return (f"Дождя не ожидается: сегодня {place} {describe(d['code'])}, "
                f"вероятность осадков {_pct_words(d['pop'])}.")
    if kind == "tomorrow":
        if len(days) < 2:
            return "Прогноза на завтра пока нет."
        d = days[1]
        return (f"Завтра {place} от {_from(d['min'])} до {temp_words(d['max'])}, "
                f"{describe(d['code'])}, вероятность осадков {_pct_words(d['pop'])}.")
    parts = [f"Сейчас {place} {temp_words(cur['t'])}, "
             f"ощущается как {temp_words(cur['feels'])}, {describe(cur['code'])}"]
    wd = cur.get("winddir")
    wdir = wd if isinstance(wd, str) else _wind_dir(wd or 0)
    parts[0] += f", ветер {wdir} {wind_words(cur['wind'])}" if wdir else ""
    d = days[0]
    parts.append(f"Днём от {_from(d['min'])} до {temp_words(d['max'])}, "
                 f"{describe(d['code'])}, осадки {_pct_words(d['pop'])}")
    return ". ".join(parts) + "."


def _from(t: float) -> str:
    """«от …» — родительный падеж нуля: «от нуля», а не «от ноль»."""
    return "нуля" if int(round(abs(t))) == 0 else temp_words(t)


def _pct_words(p) -> str:
    n = int(round(float(p or 0)))
    return f"{num_words(n)} {_plural(n, 'процент', 'процента', 'процентов')}"
