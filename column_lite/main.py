"""quasard-lite: автономное ядро голосового ассистента на самой колонке.

Два режима:

1) relay (по умолчанию) — колонка ходит к настоящему облаку Яндекса через нас.
   Голос (ASR/TTS), погода, таймеры, болтовня — стоковая Алиса. Мы перехватываем
   только музыку/радио/книги и отдаём свой audio_play с прямой mp3-ссылкой.
   ПК не нужен вообще.

2) local — без облака. Отвечаем только на TextInput (текст из приложения или
   glagol sendText). Голосового ввода тут нет: колонка шлёт сырое аудио, а
   распознавать его на самой колонке нечем (нет модели и места под неё).

Запуск на колонке:
  PYTHONHOME=/data/opt/python PYTHONPATH=/data/opt/pylibs \
  SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
  /data/opt/python/bin/python3.12 -m column_lite.main
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import ssl
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

# pylibs (httpx/websockets/protobuf) лежат отдельно от stdlib
if Path("/data/opt/pylibs").exists():
    sys.path.insert(0, "/data/opt/pylibs")
if Path("/data/opt").exists():
    sys.path.insert(0, "/data/opt")

from . import intents
from . import protocol as P
from . import tts as TTS
from . import bt_provision
from .mgmt import (bluetooth_set, bluetooth_status, load_or_create_token,
                   recent, wifi_connect, wifi_scan, wifi_status)
from .config import LITE_DIR, LiteConfig, load_config, load_device
from .httpserver import App, Response, WebSocket
from .llm_local import LocalLLM
from .relay import Relay
from .voice import VoiceLoop
from .streams_lite import Streams
from .sync import (build_no_updates, build_sync_info, build_timezone,
                   build_user_details)
from .tokens import check_jwt, glagol_token, load_or_create_key

import httpx

# Кэш озвученных фраз (edge-tts -> mp3) — на диске колонки, переживает рестарт.
TTS_DIR = Path(os.environ.get("LITE_DIR", "/data/opt/column_lite")) / "tts_cache"

log = logging.getLogger("lite")

SESSION_ID = "lite-" + uuid.uuid4().hex[:12]
CB_PREFIX = "@@mm_stack_engine_"
GET_NEXT = "@@mm_stack_engine_get_next"

# Авторетрай радио: обрывы, попадающие в окно RADIO_RESET_S, считаются одним
# инцидентом; после RADIO_MAX_RETRIES подряд станция считается мёртвой.
RADIO_RESET_S = 90.0
RADIO_MAX_RETRIES = 3

# /_radio: сколько раз переподключаться к upstream внутри одного ответа,
# с какого объёма непрерывной отдачи считать прежний обрыв «старым»
# и сколько накопить до первой отдачи (эфир VGTRK приходит рывками).
RADIO_UPSTREAM_RETRIES = 5
RADIO_STABLE_BYTES = 512 * 1024   # ~30 с эфира 128 kbps
RADIO_PREBUF = 96 * 1024          # ~4-6 с эфира — переживает паузы upstream

# Пауза: через сколько после on_stopped решить, пауза это (кнопка/приложение)
# или естественный конец трека (тогда колонка тут же начнёт следующий).
STOP_CHECK_S = 1.5
# Сколько ждать старта трека после выдачи audio_play, прежде чем считать
# остановку пользовательской: ответ со фразой — сначала TTS, потом трек.
START_GRACE_S = 10.0


async def icy_strip(upstream):
    """Отдаёт аудио-чанки потока, вырезая ICY-метаданные (StreamTitle и пр.).

    Итератор по httpx-ответу; исключение пробрасывается вызывающему, чтобы
    radio_proxy мог переподключиться к upstream и продолжить эфир.
    """
    metaint = upstream.headers.get("icy-metaint")
    agen = upstream.aiter_bytes(16384)
    if not metaint:
        async for chunk in agen:
            yield chunk
        return
    n = int(metaint)
    buf = bytearray()
    audio_left = n
    async for chunk in agen:
        buf.extend(chunk)
        while buf:
            if audio_left > 0:
                take = min(audio_left, len(buf))
                chunk = bytes(buf[:take])
                del buf[:take]
                yield chunk
                audio_left -= take
            else:
                meta_len = buf[0] * 16
                if len(buf) < 1 + meta_len:
                    break
                buf[:1 + meta_len] = b""
                audio_left = n


@dataclass
class Reply:
    speech: str
    directives: list[dict] = field(default_factory=list)
    # Зажечь кольцо колонки (should_listen в ответе): «посвети».
    listen: bool = False


# «стоп», «пауза», «хватит»… — остановить плеер. Раньше такие фразы уходили в
# языковую модель, и та отвечала «Таймер остановлен», а звук продолжался.
_STOP_RE = re.compile(
    r"^(?:алиса\s+)?(?:стоп|хватит|довольно|замолчи|молчи|помолчи|тихо|пауза|паузу|на паузу|"
    r"(?:поставь|сделай)(?: (?:музыку|книгу|радио))? (?:на )?паузу|останови(?:сь)?|"
    r"(?:останови|выключи|выруби)(?: (?:музыку|книгу|аудиокнигу|радио|звук|это))?)"
    r"(?: пожалуйста)?$")


async def glagol_command(cfg: LiteConfig, command: str, guard=None) -> bool:
    """Команда плееру колонки через её локальный glagol (порт 1961): stop / play.

    Проверено на колонке 17.11: «stop» останавливает наш audio_play
    (state.playing -> False). Серверной директивой это сделать нельзя.
    """
    import websockets
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        token = glagol_token(cfg.device_id, cfg.platform,
                             load_or_create_key(LITE_DIR / "glagol_jwt.key"))
        payload = {"conversationToken": token, "id": str(uuid.uuid4()),
                   "payload": {"command": command}, "sentTime": int(time.time() * 1000)}
        async with websockets.connect("wss://127.0.0.1:1961", ssl=ctx, open_timeout=5) as ws:
            # glagol обслуживает одно подключение за раз: наше могло ждать
            # секунды, пока его освободит отправитель СЛЕДУЮЩЕЙ команды. За это
            # время обстановка меняется («стоп» и сразу «включи …» — запоздавший
            # stop глушил только что начатый трек, поймано 20.11). guard()
            # спрашивается в последний момент: False — команда уже не нужна.
            if guard is not None and not guard():
                log.info("glagol: команда %s отменена — уже не актуальна", command)
                return False
            await ws.send(json.dumps(payload))
            try:
                await asyncio.wait_for(ws.recv(), timeout=3)
            except asyncio.TimeoutError:
                pass
        log.info("glagol: команда %s отправлена", command)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("glagol: команда %s не прошла: %r", command, e)
        return False


async def glagol_state(cfg: LiteConfig) -> dict | None:
    """Состояние плеера: {"playing": bool, "progress": секунда трека}.

    glagol отдаёт снимок state.playerState в ответ на любую команду — это
    единственный способ узнать ТОЧНУЮ секунду (по часам сервера расхождение
    до 1-2 с из-за буферизации). None — колонка не ответила, вызывающий
    оценивает позицию сам.
    """
    import websockets
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        token = glagol_token(cfg.device_id, cfg.platform,
                             load_or_create_key(LITE_DIR / "glagol_jwt.key"))
        payload = {"conversationToken": token, "id": str(uuid.uuid4()),
                   "payload": {"command": "ping"}, "sentTime": int(time.time() * 1000)}
        async with websockets.connect("wss://127.0.0.1:1961", ssl=ctx, open_timeout=3) as ws:
            await ws.send(json.dumps(payload))
            raw = await asyncio.wait_for(ws.recv(), timeout=3)
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return None
        st = obj.get("state") or {}
        ps = st.get("playerState") or {}
        return {"playing": bool(st.get("playing")),
                "progress": float(ps.get("progress") or 0.0)}
    except Exception as e:  # noqa: BLE001
        log.warning("glagol: состояние плеера не получено: %r", e)
        return None


async def glagol_sendtext(cfg: LiteConfig, text: str) -> bool:
    """Отправить фразу колонке «как будто сказано» (приложение-пульт).

    Колонка открывает сессию к uniproxy (это мы) и присылает текст; обычный
    путь lite отвечает командами — так пульт умеет всё, что умеет голос.
    """
    import websockets
    if not (text or "").strip():
        return False
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        token = glagol_token(cfg.device_id, cfg.platform,
                             load_or_create_key(LITE_DIR / "glagol_jwt.key"))
        payload = {"conversationToken": token, "id": str(uuid.uuid4()),
                   "payload": {"command": "sendText", "text": text},
                   "sentTime": int(time.time() * 1000)}
        async with websockets.connect("wss://127.0.0.1:1961", ssl=ctx,
                                      open_timeout=5) as ws:
            await ws.send(json.dumps(payload, ensure_ascii=False))
            try:
                await asyncio.wait_for(ws.recv(), timeout=4)
            except asyncio.TimeoutError:
                pass
        log.info("пульт: отправлено %r", text[:60])
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("пульт: sendText не удался: %r", e)
        return False


# ---------------------------------------------------------------- директивы
# Фраза и музыка идут В ОДНОМ VinsResponse: секвенсор колонки выполняет
# директивы последовательно через CONTENT_CHANNEL — audio_play(mp3-фраза),
# затем audio_play(музыка). Проверено на железе: отдельный отложенный ответ
# колонка отбрасывает («hasAliceActivity=0» — сессия уже закрыта).


def tts_audio_play(url: str, request_id: str) -> dict:
    """audio_play для синтезированной фразы (колонка качает mp3 локально)."""
    return {
        "name": "audio_play", "sub_name": "music", "type": "client_action",
        "is_led_silent": True,
        "payload": {
            "stream": {"id": "tts", "type": "Track", "url": url,
                       "format": "MP3", "codec": "Mp3", "bitrate": 48,
                       "offset_ms": 0, "duration_ms": 0},
            "metadata": {"title": "", "subtitle": "", "art_image_url": "",
                         "hide_progress_bar": True,
                         "glagol_metadata": {"music_metadata": {
                             "type": "Radio", "id": "tts"}}},
            "scenario_meta": {"owner": "music"}, "background_mode": "Ducking",
            "screen_type": "Music", "provider_name": "", "set_pause": False,
            "playback_speed": 1, "karaoke_mode": "Undefined",
        },
    }


# ---------------------------------------------------------------- озвучка
# Синтез в файл ДО ответа: Google TTS отдаёт целый mp3 за ~1.3 с (замерено
# на колонке 17.11), edge-tts — резерв.
# Потоковый вариант (_ttsstream) пробовали 17.11: gstreamer колонки начинает
# перезапрашивать URL с backoff 1/2/4/8 с, пока поток не наберёт данные,
# и воспроизведение затягивалось. Целый файл с Content-Length играет сразу.
async def speech_with_followup(speech: str, request_id: str,
                               followup: list[dict], port: int = 80) -> list[dict]:
    """Фраза -> mp3 на колонке, затем followup в той же пачке.

    Синтез не удался (нет сети/сервис лёг) — отдаём followup без реплики:
    музыка важнее.
    """
    followup = list(followup or [])
    if not speech:
        return followup
    try:
        path = await TTS.speak_to_file(speech, TTS_DIR)
    except Exception:
        log.exception("tts: синтез упал")
        path = None
    if not path:
        return followup
    # Порт берём из конфига: раньше жёсткий URL без порта ломался при
    # http_port != 80 (аудит 22.11).
    url = f"http://127.0.0.1:{int(port)}/_tts/" + Path(path).name
    return [tts_audio_play(url, request_id)] + followup


def play_directives(device_id: str, request_id: str, s, offset_ms: int = 0,
                    speed: float | None = None) -> list[dict]:
    """audio_play: колонка сама стримит mp3 по url."""
    track = s.current
    if track is None:
        return []
    nxt = s.queue[s.pos + 1] if s.pos + 1 < len(s.queue) else None
    is_radio = s.kind == "radio"
    if speed is None:
        speed = getattr(s, "speed", 1.0) or 1.0
    # отметка «только что выдали трек»: _watch_stop по ней не примет паузу,
    # вызванную TTS-фразой ответа, за остановку пользователем
    s.issued_ts = time.monotonic()
    s.pending_offset = max(0.0, offset_ms / 1000.0)

    def cb(name: str, ignore: bool = True) -> dict:
        return {"name": CB_PREFIX + name, "payload": {"@request_id": request_id},
                "ignore_answer": ignore, "type": "server_action", "is_led_silent": True}

    audio_play = {
        "name": "audio_play", "sub_name": "music", "type": "client_action",
        "is_led_silent": True,
        "payload": {
            "stream": {"id": s.key or f"{s.title}:{s.pos}", "type": "Track", "url": track.url,
                       "format": "MP3", "codec": "Mp3", "bitrate": 128,
                       "offset_ms": offset_ms, "duration_ms": track.duration_ms},
            "metadata": {"title": track.title, "subtitle": track.artist,
                         "art_image_url": "", "hide_progress_bar": is_radio,
                         "glagol_metadata": {"music_metadata": {
                             "type": "Radio" if is_radio else "Playlist", "id": s.title}}},
            "callbacks": {"on_started": cb("on_started"), "on_stopped": cb("on_stopped"),
                          "on_finished": cb("on_finished"), "on_failed": cb("on_failed", False)},
            "scenario_meta": {"owner": "music"}, "background_mode": "Ducking",
            "screen_type": "Music", "provider_name": "", "set_pause": False,
            "playback_speed": speed, "karaoke_mode": "Undefined",
        },
    }
    directives = [audio_play]
    # GET_NEXT — предзагрузка следующего трека. При повторе нужен и на
    # последнем треке очереди: без него зацикленный трек не перезапустится.
    if not is_radio and (nxt is not None or getattr(s, "loop", False)):
        directives.append({
            "name": GET_NEXT, "type": "server_action", "is_led_silent": True,
            "payload": {"@request_id": request_id, "stack_session_id": request_id,
                        "@scenario_name": "HollywoodMusic",
                        "stack_product_scenario_name": "music",
                        "@recovery_params": {}, "aqw_device": device_id},
        })
    return directives


# ---------------------------------------------------------------- таймеры
class Timers:
    """Таймеры и будильники в автономном режиме.

    Ключевой принцип: ставим их НА КОЛОНКЕ родной директивой (timer_set_directive
    / alarm_add_directive). Тогда колонка отсчитывает и звонит САМА — даже если
    lite упадёт или перезапустится. Список в памяти нужен только для ответа
    на «какие будильники стоят» и для корректного «отмени».

    Список сохраняется на диск: после рестарта lite «отмени будильник» должен
    видеть будильники, поставленные ДО рестарта (сама колонка их помнит).
    """

    def __init__(self, cfg=None):
        self.cfg = cfg          # нужен часовой пояс для будильников (МСК)
        self._timers: list[dict] = []
        self._alarms: list[dict] = []
        self._task: asyncio.Task | None = None
        self._last_timer_id = ""
        self._last_alarm_id = ""
        self._file = Path(os.environ.get("LITE_DIR", "/data/opt/column_lite")) / "timers_state.json"
        self._load()

    # ---------------------------------------------------------------- диск
    def _load(self):
        try:
            data = json.loads(self._file.read_text("utf-8"))
        except (OSError, ValueError):
            return
        # Битый/чужой файл (список вместо объекта) ронял lite ещё в __init__:
        # AttributeError прямо при старте — процесс не поднимался. Состояние
        # таймеров не должно стоить автономности.
        if not isinstance(data, dict):
            log.warning("timers_state.json: не объект (%s) — сбрасываю",
                        type(data).__name__)
            return
        timers = data.get("timers")
        alarms = data.get("alarms")
        if not isinstance(timers, list):
            timers = []
        if not isinstance(alarms, list):
            alarms = []
        now = time.time()
        # Элемент без id/fire раньше ронял lite прямо в __init__ (KeyError
        # «id» на self...[-1]["id"]): одна чужая запись в timers_state.json
        # стоила автономности — процесс не поднимался (аудит 22.11).
        self._timers = [t for t in timers
                        if isinstance(t, dict) and t.get("id")
                        and t.get("fire", 0) > now]
        self._alarms = [a for a in alarms if isinstance(a, dict) and a.get("id")]
        if self._timers:
            self._last_timer_id = self._timers[-1].get("id", "")
        if self._alarms:
            self._last_alarm_id = self._alarms[-1].get("id", "")

    def _save(self):
        try:
            self._file.parent.mkdir(parents=True, exist_ok=True)
            # atomic write: обрыв питания посреди записи не должен оставить
            # усечённый JSON (тогда терялись бы и сработавшие, и нет)
            tmp = self._file.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(
                {"timers": self._timers, "alarms": self._alarms},
                ensure_ascii=False), "utf-8")
            os.replace(tmp, self._file)
        except OSError:
            log.exception("не сохранил состояние таймеров")

    def _active_timers(self) -> list[dict]:
        """Только ещё не сработавшие: «отмени» не должен считать прозвеневшие."""
        now = time.time()
        self._timers = [t for t in self._timers if t.get("fire", 0) > now]
        return self._timers

    def run(self, cmd) -> "Reply | None":
        """TimerCommand -> Reply (что сказать) или None, если это не наша команда."""
        a = getattr(cmd, "action", "")
        if a == "add_timer":
            return self._add_timer(cmd)
        if a == "add_alarm":
            return self._add_alarm(cmd)
        if a == "cancel_timer":
            active = self._active_timers()
            n = len(active)
            # КАЖДОМУ таймеру — своя директива. Раньше слали одну директиву
            # (последний id), а всё чистили: два из трёх таймеров продолжали
            # звенеть после «отмени таймер» (найдено 18.11).
            tids = [t["id"] for t in active]
            self._timers.clear()
            self._last_timer_id = ""
            if not n:
                self._save()
                return Reply("Активных таймеров нет.")
            self._save()
            ds = [{"name": "timer_cancel_directive",
                   "payload": {"timer_id": tid},
                   "id": f"cancelt-{i}-{uuid.uuid4().hex[:8]}"}
                  for i, tid in enumerate(tids)]
            word = ("Таймер отменил." if n == 1
                    else f"Отменил {n} {_plural(n, 'таймер', 'таймера', 'таймеров')}.")
            return Reply(word, ds)
        if a == "cancel_alarm":
            n = len(self._alarms)
            aids = [x["id"] for x in self._alarms] or ([self._last_alarm_id] if self._last_alarm_id else [])
            self._alarms.clear()
            self._last_alarm_id = ""
            if not aids:
                self._save()
                return Reply("Будильников нет.")
            self._save()
            ds = [{"name": "alarm_cancel_directive", "payload": {"alarm_id": aid},
                   "id": f"cancela-{i}-{uuid.uuid4().hex[:8]}"} for i, aid in enumerate(aids)]
            word = ("Будильник отменил." if n <= 1
                    else f"Отменил {n} {_plural(n, 'будильник', 'будильника', 'будильников')}.")
            return Reply(word, ds)
        if a == "cancel_all":
            active = self._active_timers()
            if not active and not self._alarms \
                    and not self._last_timer_id and not self._last_alarm_id:
                return None  # отменять нечего — пусть ответит LLM
            aids = [x["id"] for x in self._alarms] or ([self._last_alarm_id] if self._last_alarm_id else [])
            tids = [t["id"] for t in active] or ([self._last_timer_id] if self._last_timer_id else [])
            self._timers.clear()
            self._alarms.clear()
            self._last_timer_id = ""
            self._last_alarm_id = ""
            self._save()
            ds = [{"name": "alarm_cancel_directive", "payload": {"alarm_id": aid}}
                  for aid in aids]
            ds += [{"name": "timer_cancel_directive", "payload": {"timer_id": tid}}
                   for tid in tids]
            return Reply("Отменил всё.", ds)
        if a == "list":
            return Reply(self._list_str())
        return None

    # ---------------------------------------------------------------- таймер
    def _add_timer(self, cmd) -> "Reply":
        sec = int(getattr(cmd, "seconds", 0) or 0)
        if sec <= 0:
            return Reply("На сколько поставить таймер?")
        label = getattr(cmd, "label", "") or "таймер"
        # uuid-хвост: два таймера в одну секунду получали ОДИН id и делили судьбу
        # при отмене (найдено 18.11)
        tid = f"lite-{int(time.time())}-{uuid.uuid4().hex[:6]}"
        self._last_timer_id = tid
        self._timers.append({"id": tid, "fire": time.time() + sec, "label": label})
        self._save()
        # РОДНАЯ директива: колонка отсчитывает сама и звенит без нас.
        # Отправляем В ТОМ ЖЕ ответе, что и реплику: отдельным сообщением
        # колонка директиву отбрасывает (сессия уже закрыта) — проверено.
        return Reply(f"Таймер на {_dur_words(sec)} поставлен.",
                     [{"name": "timer_set_directive",
                       "payload": {"timer_id": tid, "seconds": sec, "title": label,
                                   "start": int(time.time())},
                       "id": f"timer-{int(time.time())}"}])

    # ---------------------------------------------------------------- будильник
    def _add_alarm(self, cmd) -> "Reply":
        h = int(getattr(cmd, "hour", 0) or 0)
        mi = int(getattr(cmd, "minute", 0) or 0)
        days = tuple(getattr(cmd, "days", ()) or ())
        repeat = bool(getattr(cmd, "repeat", False))
        # uuid-хвост: два будильника в одну миллисекунду получали ОДИН id и
        # делили судьбу при отмене (та же беда, что у таймеров 18.11;
        # будильники не чинили — аудит 22.11)
        aid = f"lite-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
        self._last_alarm_id = aid
        self._alarms.append({"id": aid, "hour": h, "minute": mi, "days": list(days),
                             "repeat": repeat})
        self._save()
        # «каждый день» без дней недели: разворачиваем во все 7 дней, иначе
        # сериализатор делает разовый будильник с датой — и назавтра он молчит.
        if repeat and not days:
            days = tuple(range(7))
        # РОДНАЯ директива. ВАЖНО (проверено на ПК 15.11): время — МЕСТНОЕ
        # (МСК), колонка сама переводит по Timezone.OffsetSeconds; дни в
        # протоколе 1=пн, в intents 0=пн -> +1.
        d = {"name": "alarm_add_directive",
             "payload": {"alarm_id": aid, "hour": h, "minute": mi,
                         "days": [int(x) + 1 for x in days],
                         "repeat": bool(days),
                         "label": "будильник",
                         "tz_name": self.cfg.timezone,
                         "tz_offset": int(self.cfg.timezone_offset_sec)},
             "id": f"alarm-{uuid.uuid4().hex[:8]}"}
        when = f"{h:02d}:{mi:02d}"
        extra = " каждый день" if repeat else ""
        return Reply(f"Будильник на {when}{extra} поставлен.", [d])

    # ---------------------------------------------------------------- служебное
    def _list_str(self) -> str:
        parts = []
        self._active_timers()  # отбросить прозвеневшие — «какие таймеры» не врёт
        for t in self._timers:
            left = int(t["fire"] - time.time())
            if left > 0:
                parts.append(f"таймер на {_dur_words(left)}")
        for a in self._alarms:
            when = f"{a['hour']:02d}:{a['minute']:02d}"
            # repeat без дней (каждый день) после рестарта хранит days=[] —
            # «по дням» терялся; проверяем и repeat тоже
            by_days = a.get("days") or a.get("repeat")
            parts.append(f"будильник на {when}" + (" по дням" if by_days else ""))
        if not parts:
            return "Ничего не поставлено."
        return "У вас: " + ", ".join(parts) + "."


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Склонение по числу: 1 таймер, 2 таймера, 5 таймеров."""
    n10, n100 = n % 10, n % 100
    if n10 == 1 and n100 != 11:
        return one
    if 2 <= n10 <= 4 and not 12 <= n100 <= 14:
        return few
    return many


def _dur_words(sec: int) -> str:
    """«10 минут», «2 часа 5 минут» — для ответа голосом."""
    sec = max(1, int(sec))
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    out = []
    if h:
        out.append(f"{h} {_plural(h, 'час', 'часа', 'часов')}")
    if m:
        out.append(f"{m} {_plural(m, 'минута', 'минуты', 'минут')}")
    if s and not h:
        out.append(f"{s} {_plural(s, 'секунда', 'секунды', 'секунд')}")
    return " ".join(out) or "минуту"


# ---------------------------------------------------------------- маршрутизация
class Router:
    """Решает, наша ли команда, и собирает ответ."""

    def __init__(self, cfg: LiteConfig):
        self.cfg = cfg
        self.streams = Streams(cfg)
        self.timers = Timers(cfg)
        # фоновые задачи (отложенная проверка on_stopped)
        self._tasks: set[asyncio.Task] = set()
        # незавершённая остановка плеера (см. _pause): «продолжай» ждёт её
        self._pause_task: asyncio.Task | None = None
        # текущее свечение кольца (подсветка цветом)
        self._led_task: asyncio.Task | None = None
        # история команд для приложения-пульта (последние 200)
        self.history: list[dict] = []
        # LLM для вопросов без ПК (Pollinations без ключа / Orca / SambaNova).
        # None, если выключено в конфиге.
        self.llm = None
        if getattr(cfg, "llm_enabled", False):
            try:
                self.llm = LocalLLM(cfg)
            except Exception:
                log.exception("LLM не поднялся — вопросы будут без ответа")

    # --- синхронная проверка «наше ли это» (нужна релею до ответа облака)
    def classify(self, text: str) -> bool:
        # Громкость/блютуз — простые client_action, их делаем сами.
        it = intents.match(text)
        if it is not None and it.directives:
            return True
        # Таймеры и будильники: без ПК их ставит lite — родной директивой,
        # чтобы колонка звенела САМА (даже если lite потом упадёт).
        if intents.match_timer(text) is not None:
            return True
        # «стоп/пауза/хватит»: останавливаем плеер через локальный glagol
        # (проверено 17.11). Раньше такие фразы уходили облаку, и наш audio_play
        # продолжал играть (серверная player_pause — no-op). В автономном
        # режиме облако не отвечает вовсе — фраза терялась.
        norm = " ".join(re.sub(r"[^\w\s]", " ", text.lower().replace("ё", "е")).split())
        if _STOP_RE.match(norm):
            return True
        # «посвети/включи свет» — зажигаем кольцо сами (should_listen)
        if intents.match_light(text) is not None:
            return True
        # скорость / повтор / «повтори сначала» / «включи третий трек» — наше,
        # только когда играет НАША очередь (иначе это стоковая Алиса)
        if (intents.match_speed(text) is not None
                or intents.match_loop(text) is not None
                or intents.match_replay(text)
                or intents.match_seek(text) is not None
                or intents.match_jump(text) is not None):
            return self.streams.sessions.get(self.cfg.device_id) is not None
        # «продолжай» — наше, только если есть НАША сессия (пауза) или
        # сохранённая книга: колонка сама помнит, с какого места продолжать.
        # Чужую музыку (стоковая Алиса) не трогаем — она уходит облаку.
        if intents.match_player_resume(text):
            return (self.streams.sessions.get(self.cfg.device_id) is not None
                    or self.streams.has_saved_book(self.cfg.device_id))
        # вопросы о модели и промпте — отвечаем сами: облако/LLM называют
        # себя чужими именами (в логе 06.11 модель называла себя «GPT-4»)
        if (intents.match_model_question(text)
                or intents.match_prompt_question(text)):
            return True
        if intents.match_radio(text) is not None:
            return True
        # погода и курс валют — отвечаем локально ВСЕГДА: при мёртвом токене
        # облака это единственный источник, а данные точные (API, не LLM)
        if intents.match_weather(text) is not None or intents.match_rates(text) is not None:
            return True
        if intents.match_book(text) is not None:
            return True
        m = intents.match_music(text)
        if m is not None:
            action = m[0]
            if action in ("pause", "resume"):
                # пауза/продолжение нашего потока — наше дело (колонка помнит,
                # где остановилась), если наша сессия есть; иначе — облаку
                return self.streams.sessions.get(self.cfg.device_id) is not None
            if action in ("next", "prev", "what"):
                # переключаем, только если играет НАША очередь
                return self.streams.sessions.get(self.cfg.device_id) is not None
            # wave: включаем музыку сами. Раньше отдавали облаку, но в
            # автономном режиме облака нет, а handle без ветки wave уходил
            # в LLM — колонка отвечала текстом вместо музыки (аудит 22.11).
            if action == "wave":
                return True
            # like/dislike — облачные команды: они меняют историю прослушиваний
            # и «мою волну» аккаунта, локально их нечем выполнить. Отдаём
            # облаку, чтобы колонке не молчать.
            if action in ("like", "dislike"):
                return False
            return True
        return False

    async def action(self, name: str, request_id: str) -> Reply:
        """Колбэки плеера колонки. Reply("") — «принято, делать нечего»."""
        device = self.cfg.device_id
        s = self.streams.sessions.get(device)
        if name in (GET_NEXT, "music_thin_client_recovery"):
            if s is not None and s.paused:
                # пауза = glagol stop: колонка считает трек кончившимся и может
                # попросить следующий. Очередь НЕ двигаем — «продолжай» обязан
                # включить тот же трек с того же места.
                log.info("GET_NEXT во время паузы — игнорирую")
                return Reply("")
            if s is not None and s.loop and s.kind != "radio":
                # GET_NEXT — ПРЕДЗАГРУЗКА (приходит задолго до конца трека):
                # подкладываем тот же трек с начала. Текущий играет дальше;
                # его start_offset не трогаем — он нужен для оценки позиции.
                s.pos = s.started
                s.gen += 1
                s.key = f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}"
                log.info("повтор: «%s» подложен заново", s.current.title if s.current else "")
                return Reply("", play_directives(device, request_id, s))
            if s is None and self.streams.book_active(device):
                # lite перезапускался, пока книга играла: очередь восстанавливаем
                # с сохранённой части и выдаём следующую
                await self.streams.resume_book(device)
                log.info("книга: очередь восстановлена после перезапуска")
            nxt = self.streams.advance(device)
            if nxt is not None:
                log.info("следующий трек: %s", nxt.current.title)
                return Reply("", play_directives(device, request_id, nxt))
            return Reply("")
        if name == CB_PREFIX + "on_started":
            if s is not None and s.kind == "radio":
                # Удачный старт эфира. Счётчик обрывов сбрасываем, только если
                # предыдущий обрыв был давно: иначе цикл «старт -> ERROR через
                # 2 с» (как у Маяка) каждый раз сбрасывал бы счётчик и станция
                # перезапускалась бы бесконечно.
                if time.monotonic() - (s.last_fail or 0) > RADIO_RESET_S:
                    s.fails = 0
            else:
                self.streams.mark_started(device)
        elif name == CB_PREFIX + "on_failed":
            if s is not None and s.paused:
                # «обрыв» потока — это наша пауза (glagol stop), а не сбой
                log.info("on_failed во время паузы — игнорирую")
                return Reply("")
            if s is not None and s.kind == "radio":
                # Обрыв эфира. Раньше on_failed для радио игнорировался — станция
                # умирала до новой команды «включи радио» (Mayak падает с
                # PLAYING->ERROR, проверено 18.11). Перезапускаем ту же станцию,
                # но не пулемётом: счётчик обрывов живёт в окне RADIO_RESET_S,
                # после RADIO_MAX_RETRIES подряд сдаёмся с понятной репликой.
                now = time.monotonic()
                if now - (s.last_fail or 0) > RADIO_RESET_S:
                    s.fails = 0
                s.fails += 1
                s.last_fail = now
                log.warning("радио «%s»: обрыв эфира, перезапуск (подряд: %d)",
                            s.title, s.fails)
                if s.fails > RADIO_MAX_RETRIES:
                    self.streams.sessions.pop(device, None)
                    return Reply("Радио не отвечает. Попробуйте другую станцию.")
                # Новый ключ стрима на каждый перезапуск: иначе mediad переиспользует
                # кешированного плеера, делает seek на прошлую позицию, а у эфира
                # нет Range — ERROR через ~32 КБ (проверено 19.11).
                s.key = s.title + "@" + uuid.uuid4().hex[:6]
                s.gen += 1
                return Reply("", play_directives(device, request_id, s))
            if s is not None:
                # мёртвый трек: молча листаем дальше, но не «пулемётом»
                s.fails += 1
                log.warning("колонка не смогла проиграть трек (подряд: %d)", s.fails)
                if s.fails >= 3:
                    self.streams.sessions.pop(device, None)
                    return Reply("Не получается загрузить звук.")
                nxt = self.streams.advance(device)
                if nxt is not None:
                    return Reply("", play_directives(device, request_id, nxt))
        elif name == CB_PREFIX + "on_stopped":
            # Колонка сама остановила поток — либо пауза (кнопка, приложение),
            # либо естественный конец трека (тогда сразу придёт GET_NEXT и
            # gen сессии сменится). Решаем через STOP_CHECK_S.
            if s is not None and not s.paused:
                self._spawn(self._watch_stop(device, s.gen))
        elif name == "@@mm_semantic_frame":
            # Кнопка ▶‖ при нашей паузе: прошивка не умеет возобновлять наш
            # audio_play сама — продолжаем с запомненного места.
            if s is not None and s.paused:
                log.info("кнопка ▶‖: продолжаю «%s» (%s)", s.title, s.kind)
                rep = await self._resume(device, request_id)
                if rep is not None:
                    return rep
        return Reply("")

    def _spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    @staticmethod
    def _pos_now(s) -> float:
        """Текущая секунда трека (с учётом скорости) без запроса к колонке."""
        if s.paused:
            return max(0.0, s.paused_at)
        if s.start_ts > 0:
            return max(0.0, s.start_offset +
                       (time.monotonic() - s.start_ts) * max(0.1, s.speed))
        return max(0.0, s.start_offset)

    # ВАЖНО: пока команда не отвечена, glagol колонки опрашивать нельзя. Он
    # обслуживает одно подключение за раз, а команда пришла через него же:
    # отправитель держит соединение до нашего ответа. Запрос изнутри handle
    # всегда упирается в таймаут (лог 20.11: 3 с «timed out during opening
    # handshake» на каждую команду). Поэтому позиция здесь — по часам.

    async def _say(self, device: str, request_id: str, speech: str) -> Reply:
        """Речевой ответ поверх НАШЕЙ музыки: после фразы трек продолжается.

        Любая TTS-фраза глушит наш аудиопоток. Ответ LLM («Третий трек
        воспроизводится.») поверх играющего трека убивал музыку — найдено
        живьём 20.11. Ответ без наших директив поэтому возвращает audio_play
        с той же секунды.
        """
        s = self.streams.sessions.get(device)
        if s is not None:
            s.speech_ts = time.monotonic()   # фраза сейчас заглушит поток
            return await self._over(device, request_id, s, speech)
        return Reply(speech)

    async def _over(self, device: str, request_id: str, s, speech: str) -> Reply:
        """Фраза поверх НАШЕЙ музыки: после TTS трек продолжается с той же секунды.

        Колонка, проигрывая фразу ответа, глушит наш аудиопоток — если в этом
        же ответе не вернуть audio_play, музыка встанет (найдено 19.11: «зацикли
        этот трек» — фраза сыграла, трек остановился, и lite принял это
        за паузу). Поэтому фраза и музыка едут одной пачкой, как при «включи».
        """
        if s.kind == "radio":
            # эфир вживую: после фразы включаем заново свежим плеером
            s.gen += 1
            s.fails = 0
            s.last_fail = 0.0
            s.key = s.title + "@" + uuid.uuid4().hex[:6]
            return Reply(speech, play_directives(device, request_id, s))
        if s.paused:
            return Reply(speech)   # звук и так стоит — возвращать нечего
        s.speech_ts = time.monotonic()
        pos = self._pos_now(s)
        s.gen += 1
        s.pos = s.started
        s.start_offset = pos
        s.start_ts = time.monotonic()
        s.key = f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}"
        return Reply(speech, play_directives(device, request_id, s, int(pos * 1000)))

    async def _pause(self, device: str, s) -> None:
        """Остановить наш поток, запомнив секунду трека. Возвращается СРАЗУ.

        glagol колонки обслуживает одно подключение за раз, а команда пришла
        именно через него: отправитель (микрофонный цикл) держит соединение,
        пока колонка не получит наш ответ. Останавливать плеер ДО ответа —
        взаимное ожидание на несколько секунд (лог 20.11: «пауза» в 09:09:04,
        stop доставлен в 09:09:10). Поэтому отметка паузы и оценка секунды по
        часам — сразу, а сам stop и точная секунда из плеера — фоном, после
        ответа. Повторная пауза позицию не переписывает.
        """
        first = not s.paused
        if first:
            pos = self._pos_now(s)   # до paused=True: иначе вернулся бы paused_at
            s.paused = True
            s.gen += 1
            if s.kind != "radio":
                s.paused_at = round(pos, 1)
                if s.kind == "book":
                    # место в книге переживает рестарт lite и выключение колонки
                    self.streams.remember_offset(device, s.paused_at)
        prev = self._pause_task
        self._pause_task = asyncio.create_task(self._pause_bg(device, s, first, prev))

    async def _pause_bg(self, device: str, s, first: bool, prev) -> None:
        """Фоновая часть паузы: stop плееру и уточнение секунды."""
        if prev is not None and not prev.done():
            try:
                await prev
            except Exception:  # noqa: BLE001
                pass
        gen = s.gen

        def still_paused() -> bool:
            return (s.paused and s.gen == gen
                    and self.streams.sessions.get(device) is s)

        if not await glagol_command(self.cfg, "stop", guard=still_paused):
            return
        if not first or s.kind == "radio":
            return
        # Остановленный плеер держит позицию (проверено 19.11: после stop
        # progress стоит на месте) — это точная секунда остановки, без
        # погрешности часов и без музыки, доигравшей до доставки stop.
        st = await glagol_state(self.cfg)
        if (st is not None and not st["playing"] and st["progress"] > 0
                and s.paused and s.gen == gen):
            s.paused_at = round(st["progress"], 1)
            if s.kind == "book":
                self.streams.remember_offset(device, s.paused_at)
        log.info("пауза «%s» на %.1f с", s.title, s.paused_at)

    async def _pause_settled(self) -> None:
        """Дождаться незавершённой остановки (коротко): быстрое «пауза» ->
        «продолжай» иначе давало гонку — запоздавший stop глушил уже
        продолженный трек."""
        t = self._pause_task
        if t is not None and not t.done():
            try:
                await asyncio.wait_for(asyncio.shield(t), timeout=4.0)
            except Exception:  # noqa: BLE001
                pass

    async def _resume(self, device: str, request_id: str) -> Reply | None:
        """«Продолжай» из паузы. None — продолжать нечего (пусть отвечает облако)."""
        await self._pause_settled()
        s = self.streams.sessions.get(device)
        if s is None:
            if self.streams.has_saved_book(device):
                b, speech = await self.streams.resume_book(device)
                if b is not None:
                    return Reply(speech, play_directives(device, request_id, b,
                                                         int(b.start_offset * 1000)))
            return None
        if s.kind == "radio":
            if not s.paused:
                st = await glagol_state(self.cfg)
                if st is None or st["playing"]:
                    return Reply("Уже играет.")
            # эфир не перематывается: продолжаем вживую, свежим плеером
            s.paused = False
            s.gen += 1
            s.fails = 0
            s.last_fail = 0.0
            s.start_offset = 0.0
            s.start_ts = 0.0
            s.key = s.title + "@" + uuid.uuid4().hex[:6]
            return Reply("", play_directives(device, request_id, s))
        if s.paused:
            pos = s.paused_at
        else:
            # паузы не было (например, кнопкой остановили раньше, чем мы узнали):
            # спрашиваем колонку, а не верим своим часам
            st = await glagol_state(self.cfg)
            if st is None or st["playing"]:
                return Reply("Уже играет.")
            pos = st["progress"]
        s.paused = False
        s.gen += 1
        s.pos = s.started          # возобновляем игравший трек, не предзагруженный
        s.start_offset = max(0.0, pos)
        s.start_ts = 0.0
        # свежий ключ: mediad кеширует плеера по ключу и делает seek на прошлую
        # позицию мимо offset_ms
        s.key = f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}"
        log.info("продолжаю «%s» с %.1f с", s.title, s.start_offset)
        return Reply("", play_directives(device, request_id, s,
                                         int(s.start_offset * 1000)))

    async def _watch_stop(self, device: str, gen: int) -> None:
        """on_stopped не был нашей паузой: пауза с колонки или конец трека?"""
        await asyncio.sleep(STOP_CHECK_S)
        s = self.streams.sessions.get(device)
        if s is None or s.gen != gen or s.paused:
            return
        # Ждём только если недавно звучала НАША фраза поверх музыки: тогда
        # поток глушит TTS, а трек начнётся после неё. Остановка без свежей
        # фразы (кнопка ▶‖) — настоящая пауза, отмечаем сразу.
        extra = s.speech_ts + START_GRACE_S - time.monotonic()
        if extra > 0:
            await asyncio.sleep(min(extra, START_GRACE_S))
            s = self.streams.sessions.get(device)
            if s is None or s.gen != gen or s.paused:
                return
        st = await glagol_state(self.cfg)
        if st is not None and st["playing"]:
            return
        s = self.streams.sessions.get(device)
        if s is None or s.gen != gen:
            return  # за время ping пришёл GET_NEXT/новый старт
        pos = 0.0
        if st is not None and st["progress"] > 0:
            pos = st["progress"]
        elif s.start_ts > 0:
            pos = self._pos_now(s)
        s.paused = True
        s.gen += 1
        if s.kind != "radio":
            s.paused_at = round(max(0.0, pos), 1)
            if s.kind == "book":
                self.streams.remember_offset(device, s.paused_at)
        log.info("колонка сама остановила поток — пауза «%s» на %.1f с", s.title, pos)

    # границы скорости: ниже 0.5 и выше 3.0 голос/речь почти неразборчивы,
    # а mediad теряет синхронизацию на кастомных значениях
    SPEED_MIN = 0.5
    SPEED_MAX = 3.0
    SPEED_STEP = 0.25

    async def _set_speed(self, device: str, request_id: str, s, spd) -> Reply:
        """Скорость: число, 'faster'/'slower' (шаг) или 0.0 (сброс).

        playback_speed едет прямо в audio_play — от 0.5 до 3.0 проверяем живьём.
        Трек перезапускается с текущей секунды: смена темпа требует нового
        audio_play с тем же offset (плеер не меняет темп на лету).
        """
        if s.kind == "radio":
            return await self._over(device, request_id, s, "В радио нельзя менять скорость.")
        if isinstance(spd, str):
            step = self.SPEED_STEP if spd == "faster" else -self.SPEED_STEP
            new = round(s.speed + step, 2)
        elif spd == 0.0:
            new = 1.0
        else:
            new = round(float(spd), 2)
        new = max(self.SPEED_MIN, min(self.SPEED_MAX, new))
        # секунду берём ДО смены скорости: прошедшее время шло со СТАРЫМ темпом
        pos = self._pos_now(s)
        s.speed = new
        log.info("скорость «%s» = %.2fx", s.title, new)
        s.paused = False
        s.gen += 1
        s.pos = s.started
        s.start_offset = pos
        s.start_ts = 0.0
        s.key = f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}"
        return Reply("", play_directives(device, request_id, s, int(pos * 1000)))

    async def _jump(self, device: str, request_id: str, s, num: int) -> Reply:
        """«Включи третий трек» — запускаем n-й из очереди (1-based)."""
        if s.kind == "radio":
            return await self._over(device, request_id, s, "В эфире только одна станция.")
        if not (1 <= num <= len(s.queue)):
            return await self._over(device, request_id, s, f"В очереди {len(s.queue)} треков.")
        s.pos = num - 1
        s.started = s.pos
        s.gen += 1
        s.paused = False
        s.paused_at = 0.0
        s.start_offset = 0.0
        s.start_ts = 0.0
        s.fails = 0
        s.key = f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}"
        if s.kind == "book":
            # новая глава — с её начала, а не с сохранённой секунды прошлой
            self.streams.remember_offset(device, 0.0)
        self.streams._remember(device, s)
        t = s.current
        label = "часть" if s.kind == "book" else "трек"
        log.info("переход к %d: %s", num, t.title if t else "")
        return Reply(f"Включаю {label} {num}: {t.title}." if t else "",
                     play_directives(device, request_id, s))

    async def _seek(self, device: str, request_id: str, s,
                    target: "tuple[str, float]") -> Reply:
        """Перемотка: ("rel", ±сек) от текущей секунды или ("abs", сек) от начала.

        Позицию считаем по часам (glagol внутри handle занят отправителем
        команды). На паузе меняем запомненное место и молчим; во время
        воспроизведения — новый audio_play с offset.
        """
        if s.kind == "radio":
            return await self._over(device, request_id, s, "В живом эфире перемотки нет.")
        mode, val = target
        pos = val if mode == "abs" else self._pos_now(s) + val
        pos = max(0.0, pos)
        dur = s.current.duration_ms / 1000.0 if s.current and s.current.duration_ms else 0.0
        if dur > 0:
            pos = min(pos, max(0.0, dur - 1.0))
        if s.paused:
            s.paused_at = round(pos, 1)
            if s.kind == "book":
                self.streams.remember_offset(device, s.paused_at)
            log.info("перемотка «%s» на паузе: %.0f с", s.title, pos)
            return Reply("")
        s.gen += 1
        s.pos = s.started
        s.start_offset = pos
        s.start_ts = 0.0
        s.key = f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}"
        log.info("перемотка «%s» на %.0f с", s.title, pos)
        return Reply("", play_directives(device, request_id, s, int(pos * 1000)))

    async def _weather(self, device: str, request_id: str, kind: str,
                       city: str = "") -> Reply:
        """Погода словами. Без города — свой (координаты lite.toml), с городом —
        геокодер Open-Meteo («в москве» -> Москва) и прогноз для него."""
        from . import weather_lite
        lat, lon = self.cfg.latitude, self.cfg.longitude
        tz = self.cfg.timezone
        label = getattr(self.cfg, "city", "") or ""
        if city:
            geo = await weather_lite.resolve_city(city)
            if geo is None:
                return await self._say(
                    device, request_id,
                    f"Не нашёл город «{city}». Назовите точнее — например, Москва.")
            lat, lon, label, tz = geo
        elif not (lat or lon):
            # Координат нет (дефолты репозитория пусты — личные данные):
            # пробуем город из lite.toml, иначе честно просим назвать его.
            geo = await weather_lite.resolve_city(label) if label else None
            if geo is None:
                return await self._say(
                    device, request_id,
                    "Не знаю ваш город. Скажите «погода в Москве» — или пропишите "
                    "город и координаты в lite.toml.")
            lat, lon, label, tz = geo
        data = await weather_lite.fetch(lat, lon, tz)
        if data is None:
            return await self._say(device, request_id,
                                   "Не удалось узнать погоду — связь подвела.")
        speech = weather_lite.text(kind, label, data)
        return await self._say(device, request_id, speech)

    async def _rates(self, device: str, request_id: str, which: str) -> Reply:
        """Курс валют: ЦБ РФ (официальный), запасной open.er-api."""
        from . import rates_lite
        data = await rates_lite.fetch()
        if data is None:
            return await self._say(device, request_id,
                                   "Не удалось узнать курс валют — связь подвела.")
        return await self._say(device, request_id, rates_lite.text(which, data))

    async def _light(self, device: str, request_id: str,
                     cmd: "intents.LightCommand") -> Reply:
        """Подсветка кольца цветом: sysfs RGB-каналы (leds.py).

        Светим ровно столько, сколько попросили (по умолчанию минуту), цвет
        подтверждаем раз в секунду: прошивка перехватывает каналы на время
        своих анимаций (слушаю/говорю). Без sysfs (ПК/другая прошивка) —
        старая механика: ответ с should_listen зажигает кольцо «слушаю».
        """
        from . import leds
        self._light_stop()
        if not cmd.on:
            return Reply("")
        if not leds.available():
            return Reply("", [], listen=True)
        rgb = leds.color(cmd.color)
        leds.set_color(rgb)
        log.info("подсветка: %s на %.0f с", cmd.color or "голубой", cmd.seconds)
        self._led_task = asyncio.create_task(self._hold_led(rgb, cmd.seconds))
        return Reply("")

    def _light_stop(self) -> None:
        t = self._led_task
        self._led_task = None
        if t is not None and not t.done():
            t.cancel()
        from . import leds
        leds.off()

    async def _hold_led(self, rgb, seconds: float) -> None:
        from . import leds
        end = time.monotonic() + seconds
        try:
            while True:
                leds.set_color(rgb)
                if time.monotonic() >= end:
                    break
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        finally:
            leds.off()

    async def handle(self, text: str, request_id: str, seq: int) -> Reply | None:
        device = self.cfg.device_id
        log.info("Команда: %r", text)
        self.history.append({"ts": int(time.time()), "text": text})
        if len(self.history) > 200:
            del self.history[:-200]

        norm = " ".join(re.sub(r"[^\w\s]", " ", text.lower().replace("ё", "е")).split())
        # «стоп», «пауза», «выключи», «хватит» — любая остановка запоминает, где
        # остановились, и «продолжай» подхватывает ровно оттуда. Разницы для
        # пользователя нет: колонка сама знает, с какого места продолжать.
        if _STOP_RE.match(norm):
            s = self.streams.sessions.get(device)
            if s is not None:
                await self._pause(device, s)
            else:
                # фоном: до нашего ответа glagol занят отправителем команды.
                # Если к моменту доставки уже включили новое — не глушим его.
                def nothing_started() -> bool:
                    cur = self.streams.sessions.get(device)
                    return cur is None or cur.paused

                self._spawn(glagol_command(self.cfg, "stop", guard=nothing_started))
            return Reply("")   # молча: остановка сама себе ответ
        if intents.match_player_resume(text):
            rep = await self._resume(device, request_id)
            if rep is not None:
                return rep

        # подсветка: свечение — сам ответ, поэтому молчим (TTS погасил бы)
        light = intents.match_light(text)
        if light is not None:
            return await self._light(device, request_id, light)

        # скорость / повтор / «повтори» / «включи третий трек» — только для
        # НАШЕЙ очереди (у стоковой Алисы свои команды, их не трогаем)
        s = self.streams.sessions.get(device)
        if s is not None:
            spd = intents.match_speed(text)
            if spd is not None:
                return await self._set_speed(device, request_id, s, spd)
            loop = intents.match_loop(text)
            if loop is not None:
                if s.kind == "radio":
                    return await self._over(device, request_id, s, "В радио нельзя включить повтор.")
                s.loop = loop
                return await self._over(device, request_id, s,
                                        "Повтор включён." if loop else "Повтор выключен.")
            if intents.match_replay(text):
                s.pos = s.started
                s.gen += 1
                s.paused = False
                s.paused_at = 0.0
                s.start_offset = 0.0
                s.fails = 0
                s.last_fail = 0.0
                s.key = (s.title + "@" + uuid.uuid4().hex[:6] if s.kind == "radio"
                         else f"{s.title}:{s.pos}@{uuid.uuid4().hex[:6]}")
                return Reply("", play_directives(device, request_id, s))
            jump = intents.match_jump(text)
            if jump is not None:
                return await self._jump(device, request_id, s, jump)
            seek = intents.match_seek(text)
            if seek is not None:
                return await self._seek(device, request_id, s, seek)

        # громкость / блютуз (простые client_action)
        it = intents.match(text)
        if it is not None and it.directives:
            return Reply(it.text, it.directives)

        # таймеры и будильники — ДО музыки: «поставь на десять минут» иначе
        # могло уйти в поиск треков по слову «поставь»
        timer_cmd = intents.match_timer(text)
        if timer_cmd is not None:
            reply = self.timers.run(timer_cmd)
            if reply is not None:
                return reply

        # радио (до музыки: «включи радио маяк» не должно уйти в поиск треков)
        radio_q = intents.match_radio(text)
        if radio_q is not None:
            s = self.streams.radio(device, radio_q)
            return Reply(f"Включаю {s.title}.", play_directives(device, request_id, s))

        # погода и курс валют — точные данные из API, без LLM и облака
        wkind = intents.match_weather(text)
        if wkind is not None:
            return await self._weather(device, request_id, *wkind)
        which = intents.match_rates(text)
        if which is not None:
            return await self._rates(device, request_id, which)

        # книги
        book = intents.match_book(text)
        if book is not None:
            action, query = book
            if action in ("search", "play") and query:
                s, speech = await self.streams.book(device, query)
            elif action == "resume":
                s, speech = await self.streams.resume_book(device)
            else:
                s, speech = None, "Какую книгу включить?"
            if s:
                return Reply(speech, play_directives(device, request_id, s,
                                                     int(s.start_offset * 1000)))
            return await self._say(device, request_id, speech)

        # музыка
        music = intents.match_music(text)
        if music is not None:
            action, query = music
            if action == "pause":
                s = self.streams.sessions.get(device)
                if s is None:
                    return Reply("Сейчас ничего не играет.")
                await self._pause(device, s)
                return Reply("")
            if action == "resume":
                # «сними паузу», «играй дальше» — не поймались голым resume
                rep = await self._resume(device, request_id)
                if rep is not None:
                    return rep
                return Reply("Сейчас ничего не играет.")
            if action in ("next", "prev"):
                speech, s, changed = self.streams.control(device, action)
                if changed and s and s.current:
                    return Reply(speech, play_directives(device, request_id, s))
                return await self._say(device, request_id, speech)
            if action == "what":
                s = self.streams.sessions.get(device)
                t = s.queue[s.started] if s and s.started < len(s.queue) else None
                return await self._say(device, request_id,
                                       f"Сейчас играет: {t.artist} — {t.title}." if t
                                       else "Сейчас ничего не играет.")
            if action == "wave":
                s, speech = await self.streams.wave(device)
                if s:
                    return Reply(speech, play_directives(device, request_id, s))
                return await self._say(device, request_id, speech)
            if action == "search" and query:
                s, speech = await self.streams.music(device, query)
                if s:
                    return Reply(speech, play_directives(device, request_id, s))
                return await self._say(device, request_id, speech)

        # Вопросы про модель и промпт — отвечаем сами и честно, без LLM.
        # Раньше оба matcher'а не вызывались нигде: «на какой модели ты
        # работаешь» уходил в модель, и та называла себя чужим именем.
        if intents.match_model_question(text):
            if self.llm is not None:
                prov, model = self.llm.active_model()
                answer = f"Работаю на модели {model}, провайдер {prov}."
            else:
                answer = "Локальная модель выключена — работаю командами и музыкой."
            return await self._say(device, request_id, answer)
        if intents.match_prompt_question(text):
            from .llm_local import SYSTEM as _SYS
            return await self._say(device, request_id, f"Мой системный промпт: {_SYS}")

        # Вопросы на общие темы: не команда — значит, спрашивают что-то.
        # Без ПК отвечает Pollinations (без ключа) / Orca / SambaNova.
        if self.llm is not None:
            answer = await self.llm.ask(text)
            if answer:
                return await self._say(device, request_id, answer)
        return None  # не наше — пусть отвечает облако (или молчим в local)


# ---------------------------------------------------------------- local-режим
class LiteSession:
    """Без облака: только TextInput (приложение/glagol), без голоса."""

    def __init__(self, ws: WebSocket, router: Router):
        self.ws = ws
        self.router = router
        self.session_id = SESSION_ID
        self._current: asyncio.Task | None = None

    async def send_json(self, obj):
        await self.ws.send_json(obj)

    async def run(self):
        while not self.ws.closed:
            item = await self.ws.recv()
            if item is None:
                return
            kind, data = item
            if kind != "text":
                continue
            try:
                msg = json.loads(data)
            except ValueError:
                continue
            try:
                await self._on_message(msg)
            except Exception:
                log.exception("on_message failed")

    async def _on_message(self, msg: dict):
        ev = msg.get("event") or {}
        header = ev.get("header") or {}
        payload = ev.get("payload") or {}
        name = header.get("name", "")
        if header.get("namespace") == "System" and name == "SynchronizeState":
            await self.send_json(P.sync_state_response(header.get("messageId", ""), self.session_id))
            return
        if header.get("namespace") == "Vins" and name in ("TextInput", "VinsRequest", "CustomInput"):
            await self._on_text_input(header.get("messageId", ""), payload)

    async def _on_text_input(self, ref: str, payload: dict):
        request = payload.get("request") or {}
        event = request.get("event") or {}
        hdr = payload.get("header") or {}
        text = event.get("text") or payload.get("text") or ""
        request_id = hdr.get("request_id") or str(uuid.uuid4())
        seq = int(hdr.get("sequence_number") or 0)
        # Колбэки плеера (get_next/on_started/on_failed) приходят TextInput'ом
        # без текста. Раньше здесь они молча терялись — очередь не доигрывала
        # (найдено 18.11, режим relay_enabled=false).
        is_action = event.get("type") == "server_action" or (not text and event.get("name"))
        if is_action:
            name = event.get("name") or ""
            try:
                logs = await self.router.action(name, request_id)
            except Exception:
                log.exception("server_action %s", name)
                logs = Reply("")
            directives = await speech_with_followup(logs.speech, request_id,
                                                    logs.directives,
                                                    port=self.router.cfg.http_port)
            await self.send_json(P.vins_response(ref, request_id, self.session_id, seq,
                                                 logs.speech, speech="",
                                                 directives=directives,
                                                 should_listen=logs.listen))
            return
        if not text:
            await self.send_json(P.vins_response(ref, request_id, self.session_id, seq, "",
                                                 speech=""))
            return
        if self._current and not self._current.done():
            self._current.cancel()
        self._current = asyncio.create_task(self._answer(ref, request_id, seq, text))

    async def _answer(self, ref: str, request_id: str, seq: int, text: str):
        reply = await self.router.handle(text, request_id, seq)
        if reply is None:
            reply = Reply("Я работаю в автономном режиме: умею музыку, радио, книги. "
                          "Голосовой ввод недоступен — напиши текстом.")
        await self.send_json(P.vins_response(ref, request_id, self.session_id, seq,
                                             reply.speech, speech=reply.speech,
                                             directives=reply.directives,
                                             should_listen=reply.listen))


# ---------------------------------------------------------------- HTTP
def _ssl_context(cfg: LiteConfig) -> ssl.SSLContext | None:
    cert, key = Path(cfg.cert_file), Path(cfg.key_file)
    if not (cert.exists() and key.exists()):
        log.error("HTTPS выключен: нет %s / %s — автономный режим без TLS не работает",
                  cert, key)
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert), str(key))
    return ctx


def build_app(cfg: LiteConfig, device: dict, router: Router) -> App:
    app = App()
    device_id = cfg.device_id
    jwt_key = load_or_create_key(LITE_DIR / "glagol_jwt.key")
    mgmt_token = load_or_create_token(LITE_DIR)

    # ------------------------------------------------ приложение-пульт (/_mgmt)
    # Ручки меняют Wi-Fi и блютуз, поэтому требуют токен (X-Token или ?t=).
    # Порт слушает всю локальную сеть — без токена чужие устройства могли бы
    # перенастроить колонку.
    def _mgmt_ok(req) -> bool:
        # Только заголовок X-Token: токен в URL (?t=) оседал в логах и истории
        # браузера; приложение и так шлёт заголовок (аудит 22.11).
        tok = req.headers.get("x-token")
        return bool(tok) and tok == mgmt_token

    def _deny():
        return Response({"error": "token"}, 403)

    # Служебные ручки без токена были открыты всей LAN (аудит 19.11):
    # /_radio — SSRF-прокси (с колонки вытаскивали админку роутера),
    # /get_sync_info отдавал приватный ключ glagol, /glagol/token — подписанные
    # токены станции, /uni.ws принимал голосовые команды. Легитимные клиенты
    # всех этих ручек — только процессы самой колонки (loopback или её IP),
    # им вход из LAN закрыт.
    def _local_ok(req) -> bool:
        return bool(getattr(req, "local", False))

    def _local_deny():
        return Response({"status": "error", "error": "forbidden"}, 403)

    @app.route("GET", "/_mgmt/status")
    async def mgmt_status(req):
        if not _mgmt_ok(req):
            return _deny()
        return {"wifi": await wifi_status(),
                "bluetooth": await bluetooth_status(),
                "device": device_id}

    @app.route("GET", "/_mgmt/wifi/scan")
    async def mgmt_wifi_scan(req):
        if not _mgmt_ok(req):
            return _deny()
        return {"networks": await wifi_scan()}

    @app.route("POST", "/_mgmt/wifi/connect")
    async def mgmt_wifi_connect(req):
        if not _mgmt_ok(req):
            return _deny()
        body = req.json()
        ok, msg = await wifi_connect(str(body.get("ssid") or ""),
                                     str(body.get("password") or ""))
        return {"ok": ok, "message": msg}

    @app.route("GET", "/_mgmt/history")
    async def mgmt_history(req):
        if not _mgmt_ok(req):
            return _deny()
        return {"items": recent(router.history)}

    @app.route("POST", "/_mgmt/bt")
    async def mgmt_bt(req):
        if not _mgmt_ok(req):
            return _deny()
        return await bluetooth_set(bool(req.json().get("on")))

    @app.route("POST", "/_mgmt/say")
    async def mgmt_say(req):
        if not _mgmt_ok(req):
            return _deny()
        ok = await glagol_sendtext(cfg, str(req.json().get("text") or ""))
        return {"ok": ok}

    @app.ws("/uni.ws")
    async def uni_ws(ws: WebSocket):
        if not getattr(ws, "local", False):
            log.warning("uni.ws: подключение из LAN отклонено (%s)",
                        getattr(ws, "peer", "?"))
            await ws.close(1008)
            return
        log.info("uni.ws подключение")
        if cfg.relay_enabled:
            await Relay(ws, cfg, router.classify, router.handle, router.action).run()
        else:
            await LiteSession(ws, router).run()

    @app.route("GET", "/get_sync_info")
    @app.route("POST", "/get_sync_info")
    async def sync_info(req):
        if not _local_ok(req):
            return _local_deny()
        return build_sync_info(cfg, device, req.q("device_id", device_id),
                               req.q("platform", cfg.platform))

    @app.route("GET", "/get_timezone")
    async def get_timezone(req):
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        return build_timezone(cfg)

    @app.route("GET", "/get_user_details")
    async def get_user_details(req):
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        return build_user_details(cfg)

    @app.route("POST", "/update_device_state")
    async def update_device_state(req):
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        return {"status": "ok"}

    # Прокси радио: gstreamer колонки не умеет ICY-потоки (mediad падает с
    # "streaming stopped, reason not-linked"). Забираем поток сами и вырезаем
    # ICY-метаданные — колонка получает чистый audio/mpeg (как /_radio на ПК).
    # На колонке это локальная ручка (127.0.0.1), сам источник — интернет.
    #
    # АВТОРЕКОННЕКТ (19.11): VGTRK (Маяк) периодически обрывает эфир — gstreamer
    # уходил в ERROR. Теперь при обрыве переподключаемся к upstream ВНУТРИ того
    # же ответа и продолжаем отдавать кадры (MP3 кадровый, склейка безопасна).
    # Если upstream лежит дольше лимита — закрываемся, и колонка пришлёт
    # on_failed, где Router.action перезапустит станцию (второй уровень).
    @app.route("GET", "/_radio")
    async def radio_proxy(req):
        if not _local_ok(req):
            return _local_deny()
        url = req.q("url")
        if not url.startswith(("http://", "https://")):
            return Response({"status": "error", "error": "bad url"}, 400)

        # ПРИЧИНА падения Маяка (лог mediad 19.11): при «gap» в эфире mediad
        # дозапрашивает поток с Range («Internal pending seek»), а мы отвечали
        # 200 без поддержки диапазонов -> gst: «Server does not accept Range …
        # Server does not support seeking» -> ERROR. Для живого эфира честный
        # диапазон не существует: отвечаем 206 + Content-Range и продолжаем
        # отдавать эфир с текущей позиции. gst считает поток сикабельным и
        # переживает gap вместо падения.
        range_start = None
        m = re.match(r"bytes=(\d+)-", req.headers.get("range", "") or "")
        if m:
            range_start = int(m.group(1))
            log.info("radio: range=%s -> 206 (живой эфир)", range_start)

        def _new_client():
            return httpx.AsyncClient(timeout=httpx.Timeout(10, read=None),
                                     follow_redirects=True)

        def _req(client):
            return client.build_request(
                "GET", url, headers={"User-Agent": "Mozilla/5.0",
                                     "Icy-MetaData": "1"})

        client = _new_client()
        try:
            upstream = await client.send(_req(client), stream=True)
        except Exception as e:
            log.warning("radio: upstream %r", e)
            await client.aclose()
            return Response({"status": "error", "error": "upstream"}, 502)
        if upstream.status_code >= 400:
            await upstream.aclose()
            await client.aclose()
            return Response({"status": "error", "error": "upstream %d" % upstream.status_code}, 502)

        async def gen():
            nonlocal client, upstream
            attempts = 0
            pre = bytearray()
            first = True
            while True:
                delivered = 0
                try:
                    async for chunk in icy_strip(upstream):
                        if first:
                            # Предбуфер: VGTRK отдаёт эфир рывками с паузами
                            # 2-4 с; без запаса плеер сразу упирается в gap.
                            # Отдаём первое только когда накоплено достаточно.
                            pre.extend(chunk)
                            if len(pre) < RADIO_PREBUF:
                                continue
                            chunk = bytes(pre)
                            pre.clear()
                            first = False
                        delivered += len(chunk)
                        yield chunk
                    log.info("radio: upstream завершил поток (%d байт)", delivered)
                except Exception as e:
                    log.info("radio: поток оборвался (%d байт): %r", delivered, e)
                finally:
                    try:
                        await upstream.aclose()
                    except Exception:
                        pass
                    await client.aclose()
                # Долгий стабильный участок — это уже новый инцидент, а не
                # продолжение прежнего: счётчик сбрасываем.
                if delivered >= RADIO_STABLE_BYTES:
                    attempts = 0
                while True:
                    attempts += 1
                    if attempts > RADIO_UPSTREAM_RETRIES:
                        log.warning("radio: upstream не отвечает — закрываю поток")
                        return
                    await asyncio.sleep(min(0.5 * (2 ** (attempts - 1)), 3.0))
                    client = _new_client()
                    try:
                        upstream = await client.send(_req(client), stream=True)
                        if upstream.status_code >= 400:
                            raise RuntimeError("upstream %d" % upstream.status_code)
                        log.info("radio: upstream переподключён, продолжаю эфир")
                        break
                    except Exception as e:
                        log.info("radio: переподключение %d не удалось: %r",
                                 attempts, e)
                        await client.aclose()

        try:
            ctype = upstream.headers.get("content-type", "audio/mpeg").split(";")[0].strip()
            if ctype not in ("audio/mpeg", "audio/aac", "audio/mp4", "audio/x-m4a"):
                ctype = "audio/mpeg"
            status = 200
            resp = Response(b"", status, ctype)
            resp.headers["Accept-Ranges"] = "bytes"
            if range_start is not None:
                status = 206
                resp.status = 206
                # end — заведомо дальше всего, что плеер прочитает; total неизвестен
                resp.headers["Content-Range"] = (
                    f"bytes {range_start}-{range_start + 64 * 1024 * 1024}/*")
            resp.stream = gen()
        except Exception:
            # Ошибка ДО старта ленивого генератора: он никогда не запустится,
            # и client/upstream остались бы открытыми навсегда (аудит 22.11).
            try:
                await upstream.aclose()
            except Exception:
                pass
            await client.aclose()
            raise
        return resp

    # Озвучка ответа: синтезированный на колонке mp3 по /_tts/<hash>.mp3.
    # Имя файла — только hex, чтобы нельзя было выйти за каталог кэша.
    @app.route_prefix("GET", "/_tts/")
    @app.route_prefix("HEAD", "/_tts/")
    async def tts_file(req):
        name = req.path.rsplit("/", 1)[-1]
        if not re.fullmatch(r"[0-9a-f]{24}\.mp3", name):
            return Response({"status": "error"}, 404)
        p = TTS_DIR / name
        if not p.exists():
            return Response({"status": "error"}, 404)
        resp = Response(p.read_bytes(), 200, "audio/mpeg")
        resp.headers["Cache-Control"] = "no-store"
        return resp

    @app.route("POST", "/tv/api/v4/updates")
    @app.route("POST", "/tv/api/v5/updates")
    async def tv_updates(req):
        return build_no_updates()

    @app.route("GET", "/allow_update")
    async def allow_update(req):
        return {"status": "ok", "is_allowed": False, "allowed": False}

    @app.route("GET", "/check_updates")
    async def check_updates(req):
        return {"status": "ok", "updates": []}

    @app.route("GET", "/analytics/startup")
    async def analytics_startup(req):
        return {"status": "ok"}

    @app.route("POST", "/report")
    @app.route("POST", "/register")
    @app.route("POST", "/push_subscribe")
    @app.route("POST", "/quasar_metrics/write/json/batch")
    @app.route("POST", "/quasar_metrics/write/batch")
    async def noop(req):
        return {"status": "ok"}

    @app.route("GET", "/glagol/device_list")
    @app.route("GET", "/glagol/family_device_list")
    async def device_list(req):
        return {"status": "ok", "devices": []}

    # Без этого колонка не получит токен для своего glagol (порт 1961):
    # именно им подписаны команды паузы/продолжения и отправка текста.
    @app.route("GET", "/glagol/token")
    async def get_glagol_token(req):
        if not _local_ok(req):
            return _local_deny()
        return {"status": "ok",
                "token": glagol_token(req.q("device_id", device_id),
                                      req.q("platform", cfg.platform), jwt_key)}

    @app.route("POST", "/glagol/check_token")
    @app.route("POST", "/check_token")
    async def post_check_token(req):
        # проверка токена — только колонке/пульту: ручка на всех LAN-адресах
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        return {"status": "ok",
                "valid": check_jwt(req.body.decode("utf-8", "replace"), jwt_key)}

    @app.route("POST", "/glagol/v2.0/check_token")
    async def post_check_token_v2(req):
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        ok = check_jwt(req.body.decode("utf-8", "replace"), jwt_key)
        return {"guest": False, "owner": ok, "status": "ok"}

    @app.route("GET", "/_status")
    async def status(req):
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        voice = getattr(app, "state_voice", None)
        return {"status": "ok", "device": device_id, "relay": cfg.relay_enabled,
                "pin_ip": cfg.pin_ip, "sessions": len(router.streams.sessions),
                "timers": len(router.timers._timers),
                "voice": (voice.stats if voice is not None else None)}

    # Диагностика голоса: /_wake открывает окно прослушивания без микрофона,
    # /_dump_voice отдаёт последние секунды микрофона WAV-ом (что реально слышно).
    @app.route("GET", "/_wake")
    async def wake(req):
        # Микрофонные ручки — тоже не для LAN: дамп голоса — это запись из дома.
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        voice = getattr(app, "state_voice", None)
        if voice is None:
            return {"status": "error", "reason": "voice off"}
        sec = float(req.q("sec", "8") or 8)
        voice.hearing_wake(sec)
        return {"status": "ok", "window": sec}

    @app.route("GET", "/_dump_voice")
    async def dump_voice(req):
        if not (_local_ok(req) or _mgmt_ok(req)):
            return _local_deny()
        voice = getattr(app, "state_voice", None)
        if voice is None:
            return Response({"status": "error"}, 404)
        sec = float(req.q("sec", "15") or 15)
        resp = Response(voice.dump_wav(sec), 200, "audio/wav")
        resp.headers["Cache-Control"] = "no-store"
        return resp

    # Колонка ставит время по этому ответу (иначе «часы из будущего» ломают TLS).
    # HEAD тоже нужен: колонка ходит и им (на ПК-сервере ручка зарегистрирована
    # как GET+HEAD); раньше HEAD уходил в fallback без X-Start-Time и
    # time-over-http через HEAD не работал (найдено 18.11).
    @app.route("GET", "/generate_204")
    @app.route("HEAD", "/generate_204")
    async def generate_204(req):
        resp = Response(b"", 204)
        resp.headers["X-Start-Time"] = str(time.time_ns() // 1000)
        return resp

    return app


async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s")
    cfg = load_config()
    device = load_device(cfg.device_json)
    if device.get("id") and not cfg.device_id:
        cfg.device_id = device["id"]
    router = Router(cfg)
    app = build_app(cfg, device, router)

    ssl_ctx = _ssl_context(cfg)
    servers = [app.serve("0.0.0.0", cfg.http_port)]
    if ssl_ctx is not None:
        servers.append(app.serve("0.0.0.0", cfg.https_port, ssl_ctx))
    log.info("quasard-lite: device=%s relay=%s pin=%s", cfg.device_id,
             cfg.relay_enabled, cfg.pin_ip)

    # Голосовое управление без ПК: микрофон (PCM с профиля «Олег») ->
    # распознавание через asr_google (бесплатно) -> команда колонке через glagol.
    if cfg.voice_enabled:
        voice = VoiceLoop(cfg, cfg.device_id,
                          load_or_create_key(LITE_DIR / "glagol_jwt.key"),
                          require_wake=cfg.voice_require_wake)
        await voice.start()
        app.state_voice = voice  # для /_status и диагностики
    else:
        log.info("голос: выключен в lite.toml (voice.enabled)")

    # Настройка Wi-Fi с телефона по Bluetooth (для чужих сетей): телефон
    # отправляет SSID/пароль по SPP-каналу, колонка входит в сеть и отвечает
    # своим новым IP. Работает в фоновом потоке (rfcomm блокирующий).
    if getattr(cfg, "bt_provision_enabled", True):
        try:
            bt_provision.start()
        except Exception:
            log.exception("bt: настройка по Bluetooth не запустилась")

    await asyncio.gather(*servers)


if __name__ == "__main__":
    asyncio.run(main())
