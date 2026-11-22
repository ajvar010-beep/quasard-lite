"""Сетевые помощники для автономного режима.

Зачем свой DNS: в автономном режиме колонка пиннит uniproxy.alice.yandex.net
на СЕБЯ (local-data в unbound). Значит, обычный getaddrinfo внутри нашего же
процесса вернёт 127.0.0.1 — и релей к настоящему облаку уйдёт в петлю на самого
себя. Поэтому реальный IP спрашиваем напрямую у публичного резолвера (77.88.8.8),
минуя unbound колонки.
"""
from __future__ import annotations

import asyncio
import logging
import random
import struct

log = logging.getLogger("net")

DNS_SERVERS = ("77.88.8.8", "77.88.8.1")  # Яндекс DNS — те же, что в forwarders колонки


def _encode_qname(host: str) -> bytes:
    out = b""
    for part in host.rstrip(".").split("."):
        b = part.encode("idna") if any(ord(c) > 127 for c in part) else part.encode()
        if len(b) > 63:
            raise ValueError("слишком длинная метка в имени")
        out += bytes([len(b)]) + b
    return out + b"\x00"


def _build_query(host: str, qid: int, qtype: int = 1) -> bytes:
    # Заголовок: id, flags=RD, qd=1, an/ns/ar=0
    return (struct.pack(">HHHHHH", qid, 0x0100, 1, 0, 0, 0)
            + _encode_qname(host) + struct.pack(">HH", qtype, 1))


def _skip_name(data: bytes, off: int) -> int:
    while True:
        if off >= len(data):
            raise ValueError("обрезано имя")
        ln = data[off]
        if ln == 0:
            return off + 1
        if ln & 0xC0 == 0xC0:  # указатель сжатия — конец имени
            return off + 2
        off += 1 + ln


def parse_a(data: bytes, qid: int) -> str | None:
    """IP из ответа DNS A-запроса (или None). Вынесено отдельно — тестируется."""
    if len(data) < 12:
        return None
    rid, flags, qd, an, _ns, _ar = struct.unpack(">HHHHHH", data[:12])
    if rid != qid or (flags & 0x8000) == 0:  # не наш ответ
        return None
    if (flags & 0x000F) != 0:  # rcode != NOERROR
        return None
    off = 12
    for _ in range(qd):
        off = _skip_name(data, off) + 4
    for _ in range(an):
        off = _skip_name(data, off)
        if off + 10 > len(data):
            return None
        rtype, rclass, _ttl, rdlen = struct.unpack(">HHIH", data[off:off + 10])
        off += 10
        if rtype == 1 and rclass == 1 and rdlen == 4 and off + 4 <= len(data):
            return ".".join(str(b) for b in data[off:off + 4])
        off += rdlen
    return None


class _QueryProtocol(asyncio.DatagramProtocol):
    """Ждёт ответ на конкретный запрос на том же сокете, куда его отправили."""

    def __init__(self, query: bytes, qid: int):
        self.query = query
        self.qid = qid
        self.fut: asyncio.Future = asyncio.get_running_loop().create_future()

    def connection_made(self, transport):
        transport.sendto(self.query)

    def datagram_received(self, data, addr):
        if self.fut.done():
            return
        ip = parse_a(data, self.qid)
        if ip:
            self.fut.set_result(ip)

    def error_received(self, exc):
        if not self.fut.done():
            self.fut.set_exception(exc)


async def resolve_a(host: str, servers: tuple[str, ...] = DNS_SERVERS,
                    timeout: float = 4.0) -> str | None:
    """A-запись host через публичный DNS (в обход пинов колонки).

    Возвращает IP или None. Не бросает: сеть может быть недоступна, и тогда
    вызывающий код должен просто отказаться от релея, а не падать.
    """
    qid = random.randint(0, 0xFFFF)
    query = _build_query(host, qid)
    loop = asyncio.get_running_loop()
    for server in servers:
        transport = None
        try:
            transport, proto = await asyncio.wait_for(
                loop.create_datagram_endpoint(lambda: _QueryProtocol(query, qid),
                                              remote_addr=(server, 53)),
                timeout=timeout)
            return await asyncio.wait_for(asyncio.shield(proto.fut), timeout=timeout)
        except (asyncio.TimeoutError, OSError, ValueError) as e:
            log.info("DNS %s через %s: %r", host, server, e)
        finally:
            if transport is not None:
                transport.close()
    return None
