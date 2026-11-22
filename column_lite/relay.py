"""Прозрачный релей к настоящему uniproxy — голос без ПК.

Почему так: колонка НЕ распознаёт речь локально. На «Алису» она открывает
WebSocket и отправляет СЫРОЙ ЗВУК (Vins.VoiceInput + бинарные Ogg/Opus-кадры).
Распознавание и синтез — на стороне облака. Поэтому вариант «перехватить текст
команды» без облака невозможен: текста в трафике просто нет.

Релей делает облако источником голоса, а нас — фильтром:
  колонка --wss--> [lite: 443] --wss--> uniproxy.alice.yandex.net
  • всё, что мы не умеем (погода, таймеры, болтовня), идёт как есть — отвечает
    стоковая Алиса голосом Яндекса;
  • музыку/радио/книги распознаём по ASR-результату, глушим облачный ответ и
    отдаём свой audio_play с прямой mp3-ссылкой.

Важно: DNS-пин уводит uniproxy.alice.yandex.net на 127.0.0.1, поэтому реальный
IP для апстрима берём через публичный DNS (см. net.resolve_a), иначе получим
петлю на самих себя.
"""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import time

import websockets

from .config import LiteConfig
from .net import resolve_a

log = logging.getLogger("relay")


class Upstream:
    """Соединение с настоящим uniproxy (по IP, SNI — настоящий хостнейм)."""

    def __init__(self, cfg: LiteConfig):
        self.cfg = cfg
        self.ws = None
        self.proto: dict | None = None

    async def connect(self, proto: dict):
        self.proto = proto
        ip = await resolve_a(self.cfg.upstream_host)
        if not ip:
            raise RuntimeError(f"не удалось разрезолвить {self.cfg.upstream_host}")
        import socket
        import ssl
        # Сокет создаём и подключаем сами — к РЕАЛЬНОМУ IP (иначе DNS-пин колонки
        # увёл бы нас на 127.0.0.1). При этом в URI остаётся настоящее имя, чтобы
        # SNI и заголовок Host были правильными: с IP вместо Host облако отвечает
        # HTTP 403 на апгрейд.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        try:
            await asyncio.get_running_loop().sock_connect(sock, (ip, 443))
            ctx = ssl.create_default_context()
            self.ws = await websockets.connect(
                f"wss://{self.cfg.upstream_host}/uni.ws", ssl=ctx, sock=sock,
                open_timeout=15, close_timeout=3, max_size=16 * 1024 * 1024,
            )
        except Exception:
            sock.close()
            raise
        log.info("апстрим %s (%s) подключён", self.cfg.upstream_host, ip)

    async def send_json(self, obj):
        await self.ws.send(json.dumps(obj, ensure_ascii=False))

    async def send_bytes(self, b: bytes):
        await self.ws.send(b)

    async def recv(self):
        return await self.ws.recv()

    async def close(self):
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None


def _hdr(obj: dict) -> tuple[dict, dict]:
    ev = obj.get("event") or obj.get("directive") or {}
    return ev.get("header") or {}, ev.get("payload") or {}


def _final_asr_text(payload: dict) -> str | None:
    """Текст финального ASR-результата (None, если это не конец фразы)."""
    if not payload.get("endOfUtt"):
        return None
    rec = payload.get("recognition") or []
    if not rec:
        return None
    return (rec[0].get("normalized") or "").strip() or None


class Relay:
    """Один клиент колонки ↔ одно соединение с облаком."""

    _dead_until = 0.0   # до какого момента (monotonic) облако считаем недоступным

    def __init__(self, ws, cfg: LiteConfig, classify, handle, on_action=None):
        self.ws = ws
        self.cfg = cfg
        self.classify = classify   # (text) -> bool, синхронно: «это наше?»
        self.handle = handle       # async (text, request_id, seq) -> Reply|None
        # async (name, request_id) -> Reply|None: колбэки плеера колонки
        # (get_next, on_failed…). Без ответа на get_next музыка и книги
        # останавливались после первого трека.
        self.on_action = on_action
        self.up = Upstream(cfg)
        self._req_by_msg: dict[str, tuple[str, int]] = {}
        self._suppressed: set[str] = set()
        # rid -> deadline: страховка от «облако не пришлёт VinsResponse».
        # Без неё _suppressed (множество без предела) рос по одному на каждую
        # нашу команду и рос бы бесконечно.
        self._suppressed_ttl: dict[str, float] = {}
        # P1-1: раньше обычные set без предела — при 838 фразах за 4.5 суток
        # память росла (238 МБ RAM на колонке). Держим последние N, как deque.
        self._dropped_msgs: "collections.OrderedDict[str, None]" = collections.OrderedDict()
        self._drop_streams: "collections.OrderedDict[str, None]" = collections.OrderedDict()
        self._tasks: set[asyncio.Task] = set()
        self._up_task: asyncio.Task | None = None
        self._proto: dict | None = None
        self._proto_mode = False   # колонка шлёт AAPI-кадры -> отвечаем бинарём
        self._sync_msg: dict | None = None
        self._sync_ref: str = ""
        self._closed = False
        # Облако отвергло токен: апстрим жив, но любой запрос там — таймаут.
        # В этом состоянии команды обрабатываем ЛОКАЛЬНО, а не шлём в облако.
        # Отказ помним между соединениями (30 мин): колонка переподключается
        # на каждую команду, и без этого первый запрос нового соединения уходил
        # в облако и терялся.
        self._cloud_dead = time.monotonic() < Relay._dead_until
        # последняя команда, пересланная в облако: если оно тут же откажет,
        # обработаем её сами, а не потеряем
        self._pending: tuple | None = None
        # Голосовой цикл приcлал распознанную фразу (sendText -> TextInput).
        # Пендинг-ответ на голосовую сессию ждёт этого, чтобы не гасить LED.
        self._voice_cmd = asyncio.Event()

    # ------------------------------------------------------------------ вход
    async def run(self):
        try:
            await self._pump_client()
        finally:
            self._closed = True
            if self._up_task is not None:
                self._up_task.cancel()
            for t in list(self._tasks):
                t.cancel()
            await self.up.close()

    async def _pump_client(self):
        while not self.ws.closed:
            item = await self.ws.recv()
            if item is None:
                return
            kind, data = item
            if kind == "binary":
                if data[:4] == b"AAPI":
                    # Прошивки 3.x говорят бинарь-протоколом (TClientMessage).
                    # Раньше такой кадр слепо пересылался в апстрим — а без
                    # апстрима колонка оставалась глухой.
                    await self._on_client_proto(data)
                    continue
                if self.up.ws is not None:
                    try:
                        await self.up.send_bytes(data)
                    except Exception as e:
                        log.warning("апстрим (bin): %r", e)
                continue
            try:
                msg = json.loads(data)
            except ValueError:
                continue
            await self._on_client_msg(msg)

    async def _on_client_proto(self, data: bytes):
        """Бинарный кадр прошивки 3.x: AAPI + TClientMessage → JSON 1.56 → общий путь."""
        from . import proto_api
        self._proto_mode = True
        try:
            raw = proto_api.decode_client(data)
        except Exception:
            log.exception("proto: не декодировался кадр %d байт", len(data))
            return
        msg = proto_api.client_to_json(raw)
        if msg is None:
            log.info("proto: кадр без JSON-аналога: %s", list(raw.keys()))
            return
        await self._on_client_msg(msg)

    async def _on_client_msg(self, msg: dict):
        ev_hdr = (msg.get("event") or {}).get("header") or {}
        name = ev_hdr.get("name", "")
        log.info("клиент: %s %s", ev_hdr.get("namespace", "?"), name or "?")
        if name == "SynchronizeState":
            self._sync_ref = ev_hdr.get("messageId", "")
            proto = ((msg.get("event") or {}).get("payload") or {}).get("proto")
            if isinstance(proto, dict):
                self._proto = proto
            if self._cloud_dead:
                # Токен отвергнут — к облаку не ходим: ответ местный.
                await self._send({
                    "directive": {"header": {"namespace": "System",
                                             "name": "SynchronizeStateResponse",
                                             "refMessageId": self._sync_ref},
                                  "payload": {"SessionId": "lite-local"}}})
                return
            if self.up.ws is None:
                try:
                    await self.up.connect(self._proto or {})
                    # читатель запускаем ТОЛЬКО после подключения: раньше он
                    # стартовал до connect, видел up.ws is None и сразу
                    # выходил — ответы облака никто не читал.
                    self._up_task = asyncio.create_task(self._pump_upstream())
                except Exception as e:
                    log.error("апстрим недоступен: %r", e)
                    # Колонка не должна висеть в ожидании синхронизации.
                    await self._send({
                        "directive": {"header": {"namespace": "System",
                                                 "name": "SynchronizeStateResponse",
                                                 "refMessageId": ev_hdr.get("messageId", "")},
                                      "payload": {"SessionId": "lite-relay"}}})
                    # Первый connect упал (DNS/сеть): раньше релей оставался
                    # мёртвым до перезапуска. Повторяем connect с backoff.
                    self._sync_msg = msg
                    if self._up_task is None or self._up_task.done():
                        self._up_task = asyncio.create_task(self._retry_upstream())
                    return
        if name in ("VoiceInput", "TextInput"):
            p = ((msg.get("event") or {}).get("payload") or {}).get("header") or {}
            payload_ev = (((msg.get("event") or {}).get("payload") or {}).get("request") or {}).get("event") or {}
            is_action = payload_ev.get("type") == "server_action" or (
                not (payload_ev.get("text") or "") and payload_ev.get("name"))
            if is_action:
                # Колбэки плеера (get_next/on_started/on_failed) — НАШЕ дело
                # ВСЕГДА, а не только когда облако мертво. Раньше при живом
                # апстриме они уходили в облако, у которого нет нашей stack-сессии,
                # и очередь вставала после первого трека (найдено 18.11).
                task = asyncio.create_task(self._handle_local_text(msg, ev_hdr, p))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                return
            if name == "VoiceInput":
                # Колонка сама услышала слово-вызов и открыла микрофон. Значит,
                # следующая фраза пользователя — команда; даём голосовому циклу
                # окно, чтобы принять её даже без слова-вызова в тексте
                # (Google его часто теряет).
                from . import voice as _voice
                if _voice.active is not None:
                    _voice.active.hearing_wake()
            rid = p.get("request_id")
            if rid:
                self._req_by_msg[ev_hdr.get("messageId", "")] = (rid, int(p.get("sequence_number") or 0))
                # страховка: если облако не пришлёт ASR.Result, запись иначе
                # осталась бы навсегда; держим последние 256 запросов
                while len(self._req_by_msg) > 256:
                    self._req_by_msg.pop(next(iter(self._req_by_msg)))
            local_only = self._cloud_dead or self.up.ws is None
            if local_only and name == "TextInput":
                # Облако недоступно (например, отвергнут токен) — хотя бы
                # текстовые команды из приложения продолжают работать.
                task = asyncio.create_task(self._handle_local_text(msg, ev_hdr, p))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                return
            if local_only and name == "VoiceInput":
                # Облако мёртво — ASR/ответа не будет, колонка ждёт 12 с и сама
                # говорит «Произошла какая-то ошибка». Закрываем сессию молча:
                # сама команда придёт от голосового цикла lite (Google ASR ->
                # glagol sendText) и будет отвечена отдельной сессией.
                task = asyncio.create_task(
                    self._answer_silently(msg, ev_hdr, p))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                return
        if self.up.ws is not None:
            if name in ("VoiceInput", "TextInput"):
                self._pending = (time.monotonic(), name, msg, ev_hdr, p)
            try:
                await self.up.send_json(msg)
            except Exception as e:
                log.warning("апстрим (json): %r", e)

    async def _handle_local_text(self, msg: dict, ev_hdr: dict, payload_hdr: dict):
        from . import main as M
        from . import protocol as P
        payload = (msg.get("event") or {}).get("payload") or {}
        event = (payload.get("request") or {}).get("event") or {}
        text = event.get("text") or payload.get("text") or ""
        rid = payload_hdr.get("request_id") or ""
        seq = int(payload_hdr.get("sequence_number") or 0)
        mid = ev_hdr.get("messageId", "")

        async def empty():
            await self._send(P.vins_response(mid, rid, "lite-local", seq, "",
                                             speech="", directives=[]))

        if event.get("type") == "server_action" or (not text and event.get("name")):
            # Колбэк плеера: колонка шлёт его тем же TextInput, но без текста.
            # Раньше такие запросы оставались без ответа — get_next висел 15 с
            # и завершался FAIL, следующий трек не начинался (лог 17.11 17:19).
            name = event.get("name") or ""
            log.info("server_action от колонки: %s", name)
            reply = None
            if self.on_action is not None:
                try:
                    reply = await self.on_action(name, rid)
                except Exception:
                    log.exception("server_action %s", name)
            if reply is None:
                await empty()
                return
            directives = await M.speech_with_followup(reply.speech, rid, reply.directives,
                                            port=self.cfg.http_port)
            await self._send(P.vins_response(mid, rid, "lite-local", seq, reply.speech,
                                             speech="", directives=directives,
                                             should_listen=getattr(reply, "listen", False)))
            return
        if not text:
            await empty()
            return
        self._voice_cmd.set()   # разбудить ожидающий ответ голосовой сессии
        try:
            reply = await self.handle(text, rid, seq)
        except Exception:
            log.exception("local handle")
            reply = None
        if reply is None:
            # Не знаем, что ответить (например, LLM лёг) — закрываем запрос
            # сразу: иначе колонка ждёт и говорит «Произошла ошибка».
            await empty()
            return
        directives = await M.speech_with_followup(
            reply.speech, rid, reply.directives, port=self.cfg.http_port)
        await self._send(P.vins_response(ev_hdr.get("messageId", ""), rid,
                                         "lite-local", seq, reply.speech,
                                         speech="", directives=directives,
                                         should_listen=getattr(reply, "listen", False)))

    async def _answer_silently(self, msg: dict, ev_hdr: dict, payload_hdr: dict):
        """Облако мёртво: закрыть голосовую сессию БЕЗ реплики — ПОСЛЕ команды.

        Иначе колонка ждёт ответ 12 с и сама произносит «Произошла какая-то
        ошибка». Но ответить СРАЗУ нельзя: колонка по VinsResponse останавливает
        микрофон («stop streaming onVinsResponse») и она гаснет раньше, чем
        человек закончил фразу (проверено 17.11: сессия жила 64 мс). Поэтому
        ждём: голосовой цикл распознает фразу и пришлёт sendText. Тогда
        закрываем старую сессию молча — её содержимое уже прозвучит в новой.
        Страховка: не дольше 11 с, чтобы колонка сама не отвалилась по таймауту.
        """
        from . import protocol as P
        rid = payload_hdr.get("request_id") or ""
        seq = int(payload_hdr.get("sequence_number") or 0)
        from . import voice as _voice
        if _voice.active is None:
            # Голосовой цикл выключен — фраза не придёт никогда, и ждать 11 с
            # бессмысленно: колонка просто прождала бы и сказала «ошибка»
            # (аудит 22.11).
            log.info("голос: цикл выключен — закрываю сессию сразу")
            await self._send(P.vins_response(ev_hdr.get("messageId", ""), rid,
                                             "lite-local", seq, "",
                                             speech="", directives=[]))
            return
        self._voice_cmd.clear()
        try:
            await asyncio.wait_for(self._voice_cmd.wait(), timeout=11.0)
        except asyncio.TimeoutError:
            log.info("голос: команда не распознана за 11 с — закрываю сессию")
        await self._send(P.vins_response(ev_hdr.get("messageId", ""), rid,
                                         "lite-local", seq, "",
                                         speech="", directives=[]))

    # ------------------------------------------------------------------ выход
    async def _pump_upstream(self):
        """Читает облако. P1-3: при обрыве раньше просто выходил — апстрим
        мёртв, колонка молчит до перезапуска. Теперь переподключаемся с
        backoff, пока клиент не закрыт (сбрасывается при явном credential-отказе)."""
        while not self._closed:
            try:
                while not self._closed and self.up.ws is not None:
                    raw = await self.up.recv()
                    if isinstance(raw, bytes):
                        await self._on_binary(raw)
                        continue
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    await self._on_upstream_msg(msg)
            except Exception as e:
                if self._closed or self.up.ws is None:
                    return
                log.warning("апстрим закрылся: %r — переподключаюсь", e)
            if self._closed or self.up.ws is None:
                return
            # reconnect с backoff; credential-отказ обнуляет up.ws и сюда не попадает
            for delay in (2, 5, 10, 20, 30):
                if self._closed:
                    return
                await asyncio.sleep(delay)
                try:
                    await self.up.close()
                    await self.up.connect(self._proto or {})
                    log.info("апстрим переподключён")
                    # ВАЖНО: новое соединение облако считает неавторизованным,
                    # пока не придёт SynchronizeState. Без повтора синка после
                    # блипа сети релей «жив», но глух навсегда (найдено 18.11).
                    await self._resync()
                    break
                except Exception as e:
                    log.warning("reconnect не удался (%d с): %r", delay, e)
            else:
                log.error("апстрим: reconnect исчерпан, работаем локально")
                await self.up.close()
                self.up.ws = None
                return

    async def _retry_upstream(self):
        """Повторяет ПЕРВОЕ подключение, если оно упало (DNS/сеть в момент синка).
        Без этого релей оставался мёртвым до перезапуска lite: _pump_upstream
        стартует только ПОСЛЕ успешного connect."""
        for delay in (2, 5, 10, 20, 30, 60, 60, 60):
            if self._closed:
                return
            await asyncio.sleep(delay)
            if self.up.ws is not None:
                return
            try:
                await self.up.connect(self._proto or {})
            except Exception as e:
                log.warning("первый connect не удался (%d с): %r", delay, e)
                continue
            log.info("апстрим подключён повторно")
            # Повторяем синхронизацию: облако ждёт SynchronizeState на новом
            # соединении, иначе сессия не откроется.
            await self._resync()
            self._up_task = asyncio.create_task(self._pump_upstream())
            return
        log.error("апстрим: начальный connect исчерпан, работаем локально")

    def _prune_suppressed(self) -> None:
        """Убрать протухшие записи _suppressed/_suppressed_ttl.

        Раньше чистка была только в _resync, то есть при переподключении к
        облаку: при долгом соединении без обрывов, где облако часть команд
        так и не подтверждает, обе структуры росли без предела (аудит 20.11).
        """
        if not self._suppressed_ttl:
            return
        now = time.monotonic()
        for rid, ttl in list(self._suppressed_ttl.items()):
            if ttl <= now:
                self._suppressed.discard(rid)
                self._suppressed_ttl.pop(rid, None)

    async def _resync(self):
        """Переотправить SynchronizeState в облако с актуальным токеном.

        Облако привязывает авторизацию к соединению; после connect (первого или
        повторного) без этого запроса сессия остаётся «неавторизованной».
        """
        if self._sync_msg is None:
            return
        # символы протухания очищаем заодно: не копится на 238 МБ
        self._prune_suppressed()
        try:
            await self.up.send_json(self._sync_msg)
        except Exception as e:
            log.warning("повтор синка: %r", e)

    async def _on_upstream_msg(self, msg: dict):
        hdr, payload = _hdr(msg)
        # В протоколе namespace и name — РАЗНЫЕ поля: приходит {"namespace":"ASR",
        # "name":"Result"}, а не "ASR.Result". Раньше сравнение шло с
        # "ASR.Result"/"TTS.Speak" и не совпадало никогда — релей не перехватил
        # бы ни одного ответа и не заглушил бы музыку облака.
        ns = hdr.get("namespace", "")
        name = hdr.get("name", "")

        if ns == "System" and name == "Ping":
            # uniproxy ждёт ответа на ping, иначе рвёт соединение по таймауту.
            # Раньше Ping уходил в хвостовой _send, не находил proto-аналога и
            # молча дропался (найдено 18.11).
            try:
                await self.up.send_json(msg)
            except Exception as e:
                log.warning("pong: %r", e)
            return
        if ns == "System" and name == "GoAway":
            log.warning("облако: GoAway — переподключаюсь")
            try:
                await self.up.close()
            except Exception:
                pass
            self.up.ws = None
            return

        if ns == "System" and name == "EventException":
            err = (payload.get("error") or {}).get("message", "")
            if "credential" in err.lower():
                # Токен колонки облако не принимает. Это не наша ошибка и не
                # лечится в коде: станцию надо заново привязать к аккаунту
                # Яндекса (yandex.ru/device). Пока держим её на связи, чтобы она
                # не висела в ожидании, и работаем в локальном режиме.
                log.error("облако отвергло токен колонки (%s). Нужна повторная "
                          "привязка станции к аккаунту; голос недоступен.", err)
                self._cloud_dead = True
                Relay._dead_until = time.monotonic() + 1800.0
                await self.up.close()
                self.up.ws = None
                await self._send({
                    "directive": {"header": {"namespace": "System",
                                             "name": "SynchronizeStateResponse",
                                             "refMessageId": self._sync_ref},
                                  "payload": {"SessionId": "lite-local"}}})
                # Команда, которую мы только что переслали в облако, осталась
                # без ответа — обрабатываем её локально (раньше первая команда
                # после перезапуска lite/колонки просто пропадала).
                pend, self._pending = self._pending, None
                if pend is not None and time.monotonic() - pend[0] < 10.0:
                    _, kind, pmsg, phdr, pp = pend
                    coro = (self._handle_local_text(pmsg, phdr, pp) if kind == "TextInput"
                            else self._answer_silently(pmsg, phdr, pp))
                    task = asyncio.create_task(coro)
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)
                return
            log.error("облако отказало: %s", err)
            await self._send(msg)
            return

        if ns == "ASR" and name == "Result":
            text = _final_asr_text(payload)
            if text is not None:
                self._maybe_handle(text, hdr.get("refMessageId", ""))
            await self._send(msg)
            return

        if ns == "Vins" and name == "VinsResponse":
            rid = ((payload.get("header") or {}).get("request_id")) or ""
            if rid in self._suppressed:
                # это ответ на команду, которую мы обработали сами
                self._suppressed.discard(rid)
                self._suppressed_ttl.pop(rid, None)
                mid = hdr.get("messageId", "")
                if mid:
                    self._dropped_msgs[mid] = None
                    while len(self._dropped_msgs) > 128:
                        self._dropped_msgs.popitem(last=False)
                log.info("облачный ответ на %s заглушён", rid[:8])
                return
            await self._send(msg)
            return

        if ns == "TTS" and name == "Speak":
            if hdr.get("refMessageId", "") in self._dropped_msgs:
                sid = str(hdr.get("streamId", ""))
                if sid:
                    self._drop_streams[sid] = None
                    while len(self._drop_streams) > 64:
                        self._drop_streams.popitem(last=False)
                return
            await self._send(msg)
            return

        if "streamcontrol" in msg:
            sid = str((msg.get("streamcontrol") or {}).get("streamId", ""))
            if sid in self._drop_streams:
                self._drop_streams.pop(sid, None)
                return
            await self._send(msg)
            return

        # Прочие директивы, порождённые заглушённым ответом, тоже не шлём.
        ref = hdr.get("refMessageId", "")
        if ref and ref in self._dropped_msgs:
            return
        await self._send(msg)

    async def _on_binary(self, raw: bytes):
        if len(raw) >= 4 and str(int.from_bytes(raw[:4], "big")) in self._drop_streams:
            return
        await self.ws.send_bytes(raw)

    async def _send(self, msg: dict):
        if self._proto_mode:
            # Колонка просила бинарь-протокол — JSON-строку она не разберёт.
            # Кодирование отделено от транспорта: раньше KeyError внутри кодека
            # (ASR.Result без "words", сообщение без "directive") падал в общий
            # except и закрывал сессию как «клиент отключился» — читатель
            # апстрима выходил, и lite замолкал до переподключения (18.11).
            # Ошибка конвертации — это пропуск одного сообщения, не обрыв.
            from . import proto_api
            try:
                pm = proto_api.server_to_proto(msg)
                if pm is None:
                    log.info("proto: ответ без proto-аналога: %s.%s",
                             msg.get("directive", {}).get("header", {}).get("namespace"),
                             msg.get("directive", {}).get("header", {}).get("name")
                             or list(msg.keys()))
                    return
                data = proto_api.encode_server(pm)
            except Exception:
                log.exception("proto: ответ не кодируется — пропускаю сообщение")
                return
            try:
                await self.ws.send_bytes(data)
            except Exception as e:
                log.info("клиент отключился: %r", e)
                self._closed = True
            return
        try:
            await self.ws.send_json(msg)
        except Exception as e:
            log.info("клиент отключился: %r", e)
            self._closed = True

    # ------------------------------------------------------------------ логика
    def _maybe_handle(self, text: str, voice_msg_id: str):
        self._prune_suppressed()   # копится только при живом облаке — чистим здесь
        try:
            ours = self.classify(text)
        except Exception:
            log.exception("classify")
            return
        if not ours:
            return
        rid, seq = self._req_by_msg.pop(voice_msg_id, ("", 0))
        if not rid:
            # без request_id нельзя корректно ответить — отдаём облаку
            return
        self._suppressed.add(rid)

        async def job():
            try:
                reply = await self.handle(text, rid, seq)
            except Exception:
                # Раньше тут не было discard: любой сбой (сеть в поиске музыки)
                # оставлял rid в _suppressed НАВСЕГДА — облачный ответ на эту
                # команду глушился вечно, а множество росло (найдено 18.11).
                log.exception("handle")
                self._suppressed.discard(rid)
                return
            if reply is None:
                self._suppressed.discard(rid)
                return
            from . import main as M
            from . import protocol as P
            # Реплику синтезируем НА КОЛОНКЕ (edge-tts -> mp3 -> audio_play);
            # фраза и музыка идут ОДНОЙ пачкой — секвенсор колонки проиграет
            # их по очереди через CONTENT_CHANNEL.
            # Сбой синтеза не должен ронять ответ: отдаём контент без реплики.
            try:
                directives = await M.speech_with_followup(
                    reply.speech, rid, reply.directives,
                    port=self.cfg.http_port)
            except Exception:
                log.exception("tts для ответа не удался — отдаю без озвучки")
                directives = reply.directives
            await self._send(P.vins_response(
                voice_msg_id, rid, "lite-relay", seq, reply.speech,
                speech="", directives=directives, should_listen=getattr(reply, "listen", False)))
            # rid остаётся в _suppressed до прихода облачного VinsResponse
            # (его глушим). Страховка от «облако не пришлёт»: не копится —
            # чистим запись, если облако так и не ответило за 60 с.
            self._suppressed_ttl[rid] = time.monotonic() + 60.0

        t = asyncio.create_task(job())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
