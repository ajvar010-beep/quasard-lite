"""Минимальный HTTP-сервер на чистом asyncio для колонки (без FastAPI/starlette).

Только то, что нужно quasard-lite: GET/POST, JSON, WebSocket-upgrade для uni.ws.
Не тянет uvicorn/click/h11 — их нет в glibc-wheels для armv7.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
from urllib.parse import urlsplit, parse_qs

log = logging.getLogger("http")

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Ограничения размеров: порт слушает всю локальную сеть без авторизации,
# и без потолков любой в сети мог положить lite (238 МБ RAM) одним запросом
# с огромным Content-Length или WebSocket-фреймом (аудит 20.11).
MAX_BODY = 4 * 1024 * 1024      # HTTP-тело (команды/токены — килобайты)
MAX_FRAME = 8 * 1024 * 1024     # WS-фрейм и накопленная фрагментация


def _peer_is_local(peer, sockname) -> bool:
    """Соединение с самой колонкой: loopback или её же адрес интерфейса.

    При accept на сокете, привязанном к 0.0.0.0, getsockname отдаёт конкретный
    адрес, на который пришло соединение (не wildcard), поэтому «клиент с того
    же IP, что и сервер» = локальный процесс. Соединение из LAN имеет другой
    peer и локальным не считается.
    """
    try:
        ip = str(peer[0])
    except Exception:
        return False
    if ip.startswith("127.") or ip in ("::1", "::ffff:127.0.0.1"):
        return True
    try:
        return ip == str(sockname[0])
    except Exception:
        return False


class Request:
    def __init__(self, method: str, path: str, query: dict, headers: dict, body: bytes):
        self.method = method
        self.path = path
        self.query = query  # {name: [values]}
        self.headers = headers  # lowercase keys
        self.body = body

    def q(self, name: str, default: str = "") -> str:
        v = self.query.get(name)
        return v[0] if v else default

    def json(self) -> dict:
        try:
            return json.loads(self.body.decode("utf-8", "replace"))
        except Exception:
            return {}


class Response:
    def __init__(self, body=b"", status: int = 200, content_type: str = "application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
            content_type = "application/json; charset=utf-8"
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.body = body
        self.status = status
        self.content_type = content_type
        self.headers: dict[str, str] = {}
        # Потоковый ответ (радио): async-итератор чанков вместо единого тела.
        # Content-Length тогда не пишем — длину эфира заранее не знаем.
        self.stream = None


_STATUS = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found",
           500: "Internal Server Error", 502: "Bad Gateway"}


class App:
    """Маршруты: app.route("GET", "/path")(handler). WebSocket: app.ws("/uni.ws")(handler)."""

    def __init__(self):
        self._routes: dict[tuple[str, str], object] = {}
        self._ws: dict[str, object] = {}
        # Префиксные маршруты: GET /_tts/<hash>.mp3 — имя файла динамическое.
        self._prefix: dict[tuple[str, str], object] = {}
        # Что отвечать на неизвестный путь. У колонки десятки служебных ручек
        # (метрики, репорты, апдейты); отвечать 404 — значит провоцировать её
        # на повторы и висеть в ретраях. Сервер ПК в таких случаях отвечает «ok».
        self.fallback: dict | None = {"status": "ok"}

    def route(self, method: str, path: str):
        def deco(fn):
            self._routes[(method, path)] = fn
            return fn
        return deco

    def ws(self, path: str):
        def deco(fn):
            self._ws[path] = fn
            return fn
        return deco

    def route_prefix(self, method: str, prefix: str):
        """Маршрут с динамическим хвостом: GET /_media/<name>.mp3."""
        def deco(fn):
            self._prefix[(method, prefix)] = fn
            return fn
        return deco

    async def _read_request(self, reader: asyncio.StreamReader) -> Request | None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            return None
        line, *rest = head.decode("latin-1").split("\r\n")
        try:
            method, target, _ = line.split(" ", 2)
        except ValueError:
            return None
        headers = {}
        for h in rest:
            if not h or ":" not in h:
                continue
            k, v = h.split(":", 1)
            headers[k.strip().lower()] = v.strip()
        u = urlsplit(target)
        query = parse_qs(u.query)
        body = b""
        raw_cl = headers.get("content-length", "0") or "0"
        try:
            cl = int(raw_cl)
        except ValueError:
            # «Content-Length: abc» раньше бросал ValueError наружу: обработчик
            # соединения умирал, писатель не закрывался — клиент висел без
            # ответа (аудит 22.11). Битый запрос просто закрываем.
            log.warning("http: плохой Content-Length %r", raw_cl[:20])
            return None
        if cl < 0 or cl > MAX_BODY:
            log.warning("http: тело %d байт отклонено (лимит %d)", cl, MAX_BODY)
            return None
        if cl:
            try:
                body = await reader.readexactly(cl)
            except (asyncio.IncompleteReadError, ConnectionError):
                # оборванное тело: обещанных байт не будет — закрываемся
                return None
        return Request(method, u.path or "/", query, headers, body)

    def _is_ws(self, req: Request) -> bool:
        return (req.headers.get("upgrade", "").lower() == "websocket"
                and req.path in self._ws)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        req = await self._read_request(reader)
        if req is None:
            writer_close(reader, writer)
            return
        # Кто пришёл: сервисные ручки решают по req.local, пускать ли (аудит 19.11).
        req.peer = ""
        req.local = False
        try:
            req.peer = str(writer.get_extra_info("peername")[0])
            req.local = _peer_is_local(writer.get_extra_info("peername"),
                                       writer.get_extra_info("sockname"))
        except Exception:
            pass
        if self._is_ws(req):
            await self._do_ws(req, reader, writer)
            return
        log.info("http: %s %s", req.method, req.path)
        fn = self._routes.get((req.method, req.path)) or self._routes.get(("*", req.path))
        if fn is None:
            for (m, pref), pfn in self._prefix.items():
                if req.path.startswith(pref) and (m == req.method or m == "*"):
                    fn = pfn
                    break
        if fn is None:
            resp = Response(self.fallback if self.fallback is not None
                            else {"error": "not found"})
        else:
            try:
                resp = await fn(req)
                if not isinstance(resp, Response):
                    resp = Response(resp)
            except Exception:
                log.exception("handler %s %s failed", req.method, req.path)
                resp = Response({"error": "internal"}, 500)
        if getattr(resp, "stream", None) is not None:
            # Потоковый ответ (радио): отдаём чанки, пока источник жив или пока
            # колонка не отвалилась. Без Content-Length — эфир бесконечен.
            hdr = [f"HTTP/1.1 {resp.status} {_STATUS.get(resp.status, 'OK')}",
                   f"Content-Type: {resp.content_type}", "Connection: close"]
            for k, v in resp.headers.items():
                hdr.append(f"{k}: {v}")
            try:
                writer.write(("\r\n".join(hdr) + "\r\n\r\n").encode("latin-1"))
                await writer.drain()
                async for chunk in resp.stream:
                    writer.write(chunk)
                    await writer.drain()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                log.info("радио: колонка закрыла соединение")
            except Exception:
                log.exception("радио: обрыв потока")
            finally:
                writer_close(reader, writer)
            return
        hdr = [f"HTTP/1.1 {resp.status} {_STATUS.get(resp.status, 'OK')}",
               f"Content-Type: {resp.content_type}",
               f"Content-Length: {len(resp.body)}", "Connection: close"]
        for k, v in resp.headers.items():
            hdr.append(f"{k}: {v}")
        writer.write(("\r\n".join(hdr) + "\r\n\r\n").encode("latin-1") + resp.body)
        await writer.drain()
        writer_close(reader, writer)

    async def _do_ws(self, req: Request, reader, writer):
        key = req.headers.get("sec-websocket-key", "")
        accept = base64.b64encode(
            hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()
        writer.write((f"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                      f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                      ).encode("latin-1"))
        await writer.drain()
        ws = WebSocket(reader, writer)
        # Заголовки хендшейка нужны релею: в них приходит токен колонки
        # (Authorization), без которого облако отвергает апстрим.
        ws.headers = req.headers
        ws.local = req.local   # канал голоса принимаем только с самой колонки
        ws.peer = req.peer
        log.info("ws upgrade: %s", {k: (v[:20] + "…" if len(v) > 20 else v)
                                   for k, v in req.headers.items()
                                   if k in ("authorization", "user-agent", "x-yandex-")})
        try:
            await self._ws[req.path](ws)
        except Exception:
            log.exception("ws handler %s failed", req.path)

    async def serve(self, host: str, port: int, ssl_ctx=None):
        srv = await asyncio.start_server(self._handle, host, port, ssl=ssl_ctx)
        log.info("%s на %s:%d", "HTTPS+WSS" if ssl_ctx else "HTTP+WS", host, port)
        async with srv:
            await srv.serve_forever()


def writer_close(reader, writer):
    try:
        writer.close()
    except Exception:
        pass


class WebSocket:
    """Минимальный WS: текст/бинарь/ping/pong/close. Серверная сторона."""

    def __init__(self, reader, writer):
        self._r = reader
        self._w = writer
        self.closed = False

    async def recv(self) -> tuple[str, bytes | str] | None:
        """-> ('text', str) | ('binary', bytes) | None при close.

        Поддерживаем фрагментацию: большие сообщения (аудио колонки бывает
        десятками килобайт) приходят несколькими фреймами с FIN=0, последний —
        continuation (op=0x0). Без склейки JSON обрезался бы и терялся.
        """
        frag_op: int | None = None
        frag = bytearray()
        self._frames = getattr(self, "_frames", 0)
        while True:
            try:
                head = await self._r.readexactly(2)
            except asyncio.IncompleteReadError:
                self.closed = True
                return None
            self._frames += 1
            if self._frames <= 5:
                log.info("ws frame #%d head=%s", self._frames, head.hex())
            fin = head[0] & 0x80
            op = head[0] & 0x0F
            masked = head[1] & 0x80
            ln = head[1] & 0x7F
            if ln == 126:
                ln = int.from_bytes(await self._r.readexactly(2), "big")
            elif ln == 127:
                ln = int.from_bytes(await self._r.readexactly(8), "big")
            if ln > MAX_FRAME:
                log.warning("ws: фрейм %d байт отклонён (лимит %d)", ln, MAX_FRAME)
                try:
                    await self._send(0x8, (1009).to_bytes(2, "big"))
                except Exception:
                    pass
                self.closed = True
                return None
            mask = await self._r.readexactly(4) if masked else b""
            data = await self._r.readexactly(ln) if ln else b""
            if masked:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            if op == 0x8:  # close — отвечаем close и выходим
                try:
                    await self._send(0x8, data[:2])
                except Exception:
                    pass
                self.closed = True
                return None
            if op == 0x9:  # ping
                await self._send(0xA, data)
                continue
            if op == 0xA:  # pong
                continue
            if op == 0x0:  # continuation
                if frag_op is None:
                    continue  # продолжение без начала — мусор
                frag += data
                if len(frag) > MAX_FRAME:
                    # фрагменты копились без предела: каждый сам по себе в
                    # лимите, сумма — нет (аудит 20.11)
                    log.warning("ws: собранный фрейм %d байт отклонён", len(frag))
                    try:
                        await self._send(0x8, (1009).to_bytes(2, "big"))
                    except Exception:
                        pass
                    self.closed = True
                    return None
            else:
                frag_op, frag = op, bytearray(data)
            if not fin:
                continue
            op, data = frag_op, bytes(frag)
            frag_op, frag = None, bytearray()
            if op == 0x1:
                return ("text", data.decode("utf-8", "replace"))
            if op == 0x2:
                return ("binary", data)

    async def _send(self, op: int, data: bytes):
        if self.closed:
            return
        ln = len(data)
        head = bytes([0x80 | op])
        if ln < 126:
            head += bytes([ln])
        elif ln < 65536:
            head += bytes([126]) + ln.to_bytes(2, "big")
        else:
            head += bytes([127]) + ln.to_bytes(8, "big")
        self._w.write(head + data)
        await self._w.drain()

    async def send_text(self, s: str):
        await self._send(0x1, s.encode("utf-8"))

    async def close(self, code: int = 1000):
        """Отправить close-кадр и закрыть соединение.

        Кадр — ДО флага: _send при closed молчит, и колонка не получала
        close (найдено 19.11). Метод раньше был объявлен ДВАЖДЫ (ruff F811):
        версия без параметра перетирала первую, и await ws.close(1008) из
        uni_ws падал TypeError — колонке не уходил ни кадр, ни код.
        """
        if not self.closed:
            try:
                await self._send(0x8, int(code).to_bytes(2, "big"))
            except Exception:
                pass
            self.closed = True
        try:
            self._w.close()
        except Exception:
            pass

    async def send_json(self, obj):
        await self.send_text(json.dumps(obj, ensure_ascii=False))

    async def send_bytes(self, b: bytes):
        await self._send(0x2, b)
