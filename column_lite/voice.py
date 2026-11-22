"""Голосовое управление в автономном режиме (без ПК, без денег).

Зачем этот модуль: когда ПК выключен, колонка сама по себе слышит только своё
слово-вызов, а речь распознавать нечем — облако Яндекса недоступно. Но профиль
«Олег» отдаёт с микрофона уже очищенный PCM 16 кГц моно по UDP на
$PC_IP:5600 (адрес компьютера из /data/opt/cfg/pc_ip).
В автономном режиме failover перенаправляет этот поток на саму колонку
(127.0.0.1:5600) — здесь мы его принимаем, режем фразы по энергии, распознаём
бесплатным движком Google (asr_google) и отдаём команду колонке через её же
glagol sendText. Дальше работает обычный текстовый путь lite: музыка, радио,
книги + озвучка edge-tts.

Проверено на железе 16.11: PCM -> Google -> «включаю радио Маяк» (conf 0.889).
"""
from __future__ import annotations

import array
import asyncio
import collections
import json
import logging
import re
import ssl
import time
import uuid

log = logging.getLogger("voice")

PACKET_MAGIC = b"OL"
RATE = 16000
FRAME_BYTES = 2  # int16

# Активный экземпляр: релей дёргает hearing_wake(), когда колонка присылает
# VoiceInput (она сама услышала «Алису»).
active: "VoiceLoop | None" = None

# Слово-вызов. Google на «Алиса» может выдать «Алиса», «Алисса», «а лиса»; на
# «Олег» — «Олег/Олек/Алекс». Режем только в НАЧАЛЕ фразы.
WAKE_RE = re.compile(
    r"^\s*(?:(?:эй|слушай|ну)[\s,]+)?"
    r"(?:алис+а?|а\s+лиса|олег|олек|алекс|алег)"
    r"(?:[\s,.!?:—-]|$)", re.I)

# Детектор речи (энергия). Порог ОТНОСИТЕЛЬНЫЙ: шумовой пол комнаты × множитель,
# но не ниже SPEECH_RMS_MIN. Раньше стоял жёсткий 260 — голос без усиления
# (rms в десятках-сотнях) не проходил вообще, и фраза не нарезалась: колонка
# «слышала» вызов, а до распознавания ничего не доходило (диагностика 17.11:
# в тишине rms 14–16, peak 97).
SPEECH_RMS_MIN = 70.0
NOISE_ALPHA = 0.02
NOISE_MULT = 2.2         # порог = max(SPEECH_RMS_MIN, шум×2.2)
END_SILENCE_S = 0.8      # столько тишины -> фраза закончилась
MIN_SPEECH_S = 0.45      # короче — не команда (кашель, щелчок)
MAX_PHRASE_S = 12.0
WIN_BUF_S = 10.0         # сырое окно команды для «спасательного» распознавания
WIN_RESCUE_PEAK = 140    # пик в окне, при котором стоит отправить окно целиком

# После отправки команды колонка сама говорит и играет: столько секунд не слушаем,
# чтобы не распознать собственную озвучку.
MUTE_AFTER_S = 7.0


class VoiceLoop:
    """Приёмник PCM с микрофона + распознавание + отправка команды колонке.

    АКТИВАЦИЯ: по умолчанию ничего не уходит в интернет, пока не позвали.
    У колонки есть свой локальный детектор слова-вызова (он работает без сети
    и без наших моделей): услышав «Алису», колонка присылает нам VoiceInput —
    это и есть «дверь». Открыли дверь -> 8 секунд принимаем речь и только её
    отправляем на распознавание. Всё остальное (телевизор, разговоры) не
    покидает колонку.
    """

    def __init__(self, cfg, device_id: str, jwt_key: bytes, enabled: bool = True,
                 require_wake: bool = True):
        self.cfg = cfg
        self.device_id = device_id
        self.jwt_key = jwt_key
        self.enabled = enabled
        self.require_wake = require_wake
        self.port = 5600
        self.stats = {"packets": 0, "asr_requests": 0, "ignored": 0,
                      "phrases": 0, "recognized": 0, "commands": 0,
                      "armed": False, "last_text": "", "last_cmd": "", "error": "",
                      "rms": 0, "noise": 0, "peak": 0, "speech_s": 0.0,
                      "win_peak": 0, "win_open": False}
        self._noise = 200.0
        self._buf = bytearray()
        self._speech = False
        self._speech_s = 0.0
        self._silence_s = 0.0
        self._mute_until = 0.0
        # Кольцевой буфер последних ~15 с «сырого» PCM для диагностики:
        # GET /_dump_voice отдаёт WAV, чтобы послушать/измерить, что реально
        # приходит с микрофона в проблемной ситуации.
        self._ring: collections.deque[bytes] = collections.deque()
        self._ring_s = 0.0
        self._ring_max_s = 15.0
        # Окно «колонка услышала вызов»: когда микрофон зажигается на «Алису»,
        # колонка присылает VoiceInput — с этого момента речь пользователя
        # принимаем БЕЗ слова-вызова. Google часто его теряет («Алиса, включи
        # радио» -> «включи радио»), и команда без окна терялась.
        self._listen_until = 0.0
        self._task: asyncio.Task | None = None
        self._armed_at_start = False
        self._speech_t0 = 0.0        # monotonic начала текущей «речи»
        self._win_pending = False    # окно открыто, фразы в нём ещё не было
        self._last_loud_t = 0.0      # начало последнего громкого пакета
        self._win_buf = bytearray()  # сырое аудио окна (спасательный путь)

    def hearing_wake(self, seconds: float = 8.0) -> None:
        """Колонка услышала «Алису» — открываем окно команд без вызова."""
        now = time.monotonic()
        self._listen_until = now + seconds
        self._win_pending = True
        # Колонка сама услышала вызов — значит, говорит человек, а не её
        # озвучка: «глухоту» после прошлой команды снимаем. Раньше «Алиса, …»
        # в течение 7 с после команды пропадала целиком (лог 20.11 18:12:36:
        # окно открыто, распознавание даже не запускалось).
        self._mute_until = 0.0
        # «Речь», которая тянется давно, — это фон (музыка, телевизор), а не
        # начало команды: сбрасываем, чтобы фраза началась с вызова.
        if self._speech and now - self._speech_t0 > 2.5:
            self._reset()
        self.stats["win_peak"] = 0
        self._win_buf = bytearray()
        self._last_loud_t = 0.0
        log.info("голос: окно команды открыто на %.0f с (колонка услышала вызов)",
                 seconds)

    def _armed(self) -> bool:
        if not self.require_wake:
            return True
        return time.monotonic() < self._listen_until

    def dump_wav(self, seconds: float = 15.0) -> bytes:
        """Последние секунды микрофона как WAV (диагностика: что реально слышно)."""
        import struct as _st
        need = int(seconds * RATE)
        chunks = []
        total = 0
        for c in reversed(self._ring):
            chunks.append(c)
            total += len(c) // FRAME_BYTES
            if total >= need:
                break
        pcm = b"".join(reversed(chunks))
        if len(pcm) > need * FRAME_BYTES:
            pcm = pcm[-need * FRAME_BYTES:]
        # Формат 13 значений: 4s I 4s 4s I H H I I H H (здесь 11!) — раньше
        # struct.pack падал «expected 13 items, got 11» и /_dump_voice всегда
        # отдавал 500 (найдено 18.11). Заголовок 44 байта: RIFF + fmt(16) + data.
        hdr = (_st.pack("<4sI4s4sIHHIIHH",
                        b"RIFF", 36 + len(pcm), b"WAVE", b"fmt ", 16, 1, 1,
                        RATE, RATE * FRAME_BYTES, FRAME_BYTES, 16)
               + b"data" + _st.pack("<I", len(pcm)))
        return hdr + pcm

    # ------------------------------------------------------------ приём
    async def start(self) -> None:
        global active
        if not self.enabled:
            log.info("голосовой режим выключен в конфиге")
            return
        loop = asyncio.get_running_loop()
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _Proto(self),
                local_addr=("0.0.0.0", self.port),
                reuse_port=False)
        except OSError as e:
            log.warning("голос: UDP :%d занят (%r) — голосовое управление выключено",
                        self.port, e)
            return
        self._transport = transport
        active = self
        log.info("голос: слушаю микрофон UDP :%d", self.port)

    def on_packet(self, data: bytes) -> None:
        if len(data) < 6 or data[:2] != PACKET_MAGIC:
            return
        pcm = data[4:]
        if len(pcm) % 2:
            pcm = pcm[:-1]
        if not pcm:
            return
        self.stats["packets"] += 1
        now = time.monotonic()
        # кольцевой буфер для диагностики — до всех фильтров
        self._ring.append(pcm)
        self._ring_s += len(pcm) / (RATE * FRAME_BYTES)
        while self._ring_s > self._ring_max_s and self._ring:
            self._ring_s -= len(self._ring.popleft()) / (RATE * FRAME_BYTES)
        if now < self._mute_until:
            # колонка говорит/играет — её собственный звук в микрофон не учитываем
            self._reset()
            return
        in_win = now < self._listen_until
        if in_win:
            # сырое окно целиком — если фраза так и не соберётся, отправим окно
            self._win_buf.extend(pcm)
            keep = int(WIN_BUF_S * RATE * FRAME_BYTES)
            if len(self._win_buf) > keep:
                del self._win_buf[:len(self._win_buf) - keep]
        samples = array.array("h")
        samples.frombytes(pcm)
        if not samples:
            return
        n = len(samples)
        dur = n / RATE
        rms = (sum(s * s for s in samples[::4]) /
               max(1, len(samples[::4]))) ** 0.5
        if not self._speech and not in_win:
            # шумовой пол обновляем только в тишине и вне окна команды: в окне
            # порог задирала её же озвучка из динамиков — первые секунды речи
            # оказывались «тише порога» и выпадали (лог 21.11: пик 278, а фраза
            # так и не нарезалась).
            self._noise = (1 - NOISE_ALPHA) * self._noise + NOISE_ALPHA * rms
        elif not self._armed_at_start and now >= self._listen_until:
            # Постоянный фон громче порога (музыка, телевизор) держал детектор
            # в состоянии «речь» бесконечно: шум не обновлялся, конец фразы не
            # находился. Вне окна вызова медленно подтягиваем пол к фону.
            self._noise = 0.9995 * self._noise + 0.0005 * rms
        if in_win:
            # В окне двери порог — базовый: колонка только что услышала вызов,
            # человек рядом; адаптивный пол в этот момент вреден.
            loud = rms > SPEECH_RMS_MIN
        else:
            loud = rms > max(SPEECH_RMS_MIN, self._noise * NOISE_MULT)
        # Диагностика для /_status: видно, доходит ли голос по уровню.
        st = self.stats
        st["rms"] = int(rms)
        st["noise"] = int(self._noise)
        st["peak"] = max(st["peak"], int(rms))
        if time.monotonic() < self._listen_until:
            # пик за время окна после вызова — сразу видно, долетает ли речь
            st["win_open"] = True
            st["win_peak"] = max(st["win_peak"], int(rms))
        else:
            st["win_open"] = False
        if self._speech:
            st["speech_s"] = round(self._speech_s, 1)
        if loud:
            if not self._speech:
                # Фиксируем «нас позвали» В НАЧАЛЕ фразы. Раньше проверка была
                # только в _finish: фраза, начатая под конец 8-секундного окна,
                # завершалась после него и отбрасывалась — колонка молчала.
                self._armed_at_start = self._armed()
                self._speech_t0 = now
            self._speech = True
            self._last_loud_t = now
            self._silence_s = 0.0
            self._speech_s += dur
            self._buf.extend(pcm)
            if self._speech_s >= MAX_PHRASE_S:
                self._finish()
        elif self._speech:
            self._silence_s += dur
            self._buf.extend(pcm)  # хвост тишины помогает распознавателю
            if self._silence_s >= END_SILENCE_S:
                self._finish()
        if self._win_pending and now >= self._listen_until:
            # Окно закрылось, а фразы в нём не было — пишем причину в лог:
            # иначе «Алиса» без реакции никак не диагностируется.
            self._win_pending = False
            if self._speech and self._speech_s >= 4.0:
                log.info("голос: окно закрылось посреди звука (%.1f с) — отправляю как есть",
                         self._speech_s)
                self._armed_at_start = True
                self._finish()
            elif not self._speech:
                if st["win_peak"] >= WIN_RESCUE_PEAK and self._win_buf:
                    # Спасательный путь: звук в окне был, но VAD не собрал фразу
                    # (тихая речь всплесками, короткие куски). Отправляем окно
                    # целиком — человек ведь уже позвал колонку.
                    log.info("голос: окно без фразы, но всплески были (пик %d) — "
                             "отправляю окно на распознавание", st["win_peak"])
                    self._armed_at_start = True
                    self._launch(bytes(self._win_buf), True)
                else:
                    log.info("голос: окно закрылось без фразы (пик %d, порог %d, шум %d)",
                             st["win_peak"],
                             int(max(SPEECH_RMS_MIN, self._noise * NOISE_MULT)),
                             int(self._noise))
            self._win_buf = bytearray()

    def _reset(self) -> None:
        self._buf = bytearray()
        self._speech = False
        self._speech_s = 0.0
        self._silence_s = 0.0
        self._armed_at_start = False
        self._last_loud_t = 0.0

    def _finish(self) -> None:
        pcm = bytes(self._buf)
        spoke = self._speech_s
        # Длительность считаем по всему отрезку (от первого громкого пакета до
        # последнего), а не по сумме громких кусков: тихая речь всплесками
        # давала 0.3–0.4 с «речи» и молча отбрасывалась (лог 21.11 18:43).
        span = (self._last_loud_t - self._speech_t0) if self._last_loud_t else 0.0
        # Окно могло закрыться, пока фраза договаривалась: важно, была ли
        # активация в её НАЧАЛЕ (или окно открылось посреди фразы).
        armed = self._armed_at_start or self._armed()
        self._reset()
        if max(spoke, span) < MIN_SPEECH_S:
            if armed:
                log.info("голос: кандидат отброшен — речь %.2f с, отрезок %.2f с",
                         spoke, span)
            return
        # ФРАЗА-АКТИВАТОР: без того, чтобы колонка сама услышала «Алису», речь
        # из комнаты остаётся на колонке — наружу не уходит ни байта.
        if not armed:
            self.stats["ignored"] += 1
            return
        self.stats["phrases"] += 1
        self.stats["armed"] = True
        self._win_pending = False
        self._launch(pcm, armed)

    def _launch(self, pcm: bytes, via_window: bool) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()  # новая фраза важнее недораспознанной старой
        self._task = asyncio.ensure_future(self._handle(pcm, via_window))

    # ------------------------------------------------------------ команда
    def _command_from_text(self, text: str, via_window: bool) -> str:
        """Текст -> команда ("" — не команда).

        via_window: активация была в окне вызова — команду принимаем без слова-
        вызова, даже если распознавание закончилось уже после закрытия окна.
        Раньше проверялось только текущее время: фраза с хвоста 8-секундного
        окна (ASR медленнее окна) отбрасывалась как «не про вызов».
        """
        m = WAKE_RE.match(text)
        if m:
            return text[m.end():].strip(" ,.!?:—-")
        if via_window or time.monotonic() < self._listen_until:
            log.info("голос: окно вызова — принимаю без слова-вызова")
            return text.strip(" ,.!?:—-")
        if not self.require_wake:
            return text.strip(" ,.!?:—-")
        return ""

    async def _handle(self, pcm: bytes, via_window: bool = False) -> None:
        from . import asr_google
        self.stats["asr_requests"] += 1
        try:
            text = await asr_google.transcribe_pcm(pcm)
        except Exception:
            log.exception("голос: распознавание упало")
            return
        if not text:
            log.info("голос: распознавание ничего не вернуло (%.1f с звука)",
                     len(pcm) / (RATE * FRAME_BYTES))
            return
        self.stats["recognized"] += 1
        self.stats["last_text"] = text[:80]
        command = self._command_from_text(text, via_window)
        if not command:
            if WAKE_RE.match(text):
                log.info("голос: только слово-вызов, жду команду")
            else:
                log.info("голос: не про вызов — %r", text[:60])
            return
        self.stats["commands"] += 1
        self.stats["last_cmd"] = command[:80]
        log.info("голос: команда %r", command[:80])
        self._mute_until = time.monotonic() + MUTE_AFTER_S
        ok = await self._send_to_column(command)
        if not ok:
            self.stats["error"] = "glagol sendText не удался"

    async def _send_to_column(self, text: str) -> bool:
        """Отправляет текст колонке как «будто сказано голосом» (glagol 1961).

        Колонка откроет сессию к uniproxy — а это мы; обычный текстовый путь lite
        ответит музыкой/радио и озвучкой. Токен подписываем нашим ключом.
        """
        import websockets
        from .tokens import glagol_token
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        token = glagol_token(self.device_id, self.cfg.platform, self.jwt_key)
        payload = {"conversationToken": token, "id": str(uuid.uuid4()),
                   "payload": {"command": "sendText", "text": text},
                   "sentTime": int(time.time() * 1000)}
        try:
            async with websockets.connect("wss://127.0.0.1:1961", ssl=ctx,
                                          open_timeout=8) as ws:
                await ws.send(json.dumps(payload, ensure_ascii=False))
                try:
                    await asyncio.wait_for(ws.recv(), timeout=6)
                except asyncio.TimeoutError:
                    log.info("голос: команда ушла, подтверждения нет")
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("голос: glagol sendText не удался: %r", e)
            return False


class _Proto(asyncio.DatagramProtocol):
    def __init__(self, owner: VoiceLoop):
        self.owner = owner

    def datagram_received(self, data, addr):  # noqa: D401
        self.owner.on_packet(data)
