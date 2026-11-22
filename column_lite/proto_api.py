"""Бинарный protobuf-протокол uniproxy («proto Alice API») — им говорят прошивки 3.x.

Кадр: бинарный WS-фрейм "AAPI" + сериализованный NAlice.NAliceApi.TClientMessage / TServerMessage.
Аудио по-прежнему идёт кадрами [streamId BE32][данные].

Схемы (aliceapi.desc) восстановлены из встроенных дескрипторов libquasar_daemons.so прошивки
3.367.1.28. Сервер внутри работает с JSON-сообщениями (protocol.py), а здесь они переводятся в proto
и обратно — так один конвейер обслуживает обе версии прошивки.
"""
from __future__ import annotations

import datetime
import time
from functools import lru_cache
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory

MAGIC = b"AAPI"

# streamcontrol.reason JSON <-> EReason proto
_REASON_TO_JSON = {"OK": 0, "INTERNAL_ERROR": 2, "STOP_RECORDING": 5, "AUDIO_SOURCE_STOPPED": 6, "CANCEL": 7,
                   "NO_SPEECH": 8}
_JSON_TO_REASON = {v: k for k, v in _REASON_TO_JSON.items()}


@lru_cache(maxsize=1)
def _classes():
    fds = descriptor_pb2.FileDescriptorSet.FromString((Path(__file__).parent / "aliceapi.desc").read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for f in fds.file:
        pool.Add(f)
    get = lambda n: message_factory.GetMessageClass(pool.FindMessageTypeByName(n))  # noqa: E731
    return get("NAlice.NAliceApi.TServerMessage"), get("NAlice.NAliceApi.TClientMessage")


def decode_client(frame: bytes) -> dict:
    _, client_cls = _classes()
    msg = client_cls.FromString(frame[len(MAGIC):])
    return json_format.MessageToDict(msg, preserving_proto_field_name=True)


def encode_server(msg: dict) -> bytes:
    server_cls, _ = _classes()
    return MAGIC + json_format.ParseDict(msg, server_cls(), ignore_unknown_fields=True).SerializeToString()


# для имитатора станции (tools/station_sim.py --proto)
def encode_client(msg: dict) -> bytes:
    _, client_cls = _classes()
    return MAGIC + json_format.ParseDict(msg, client_cls(), ignore_unknown_fields=True).SerializeToString()


def decode_server(frame: bytes) -> dict:
    server_cls, _ = _classes()
    return json_format.MessageToDict(server_cls.FromString(frame[len(MAGIC):]), preserving_proto_field_name=True)


# ------------------------------------------------------------------ proto -> JSON (входящие)

def client_to_json(m: dict) -> dict | None:
    """TClientMessage (dict) -> JSON-сообщение в формате прошивки 1.56 (event / streamcontrol)."""
    if "StreamControl" in m:
        sc = m["StreamControl"]
        if "SpotterEnd" in sc:
            action, reason = 2, 0
        elif "Close" in sc:
            action, reason = 0, _REASON_TO_JSON.get(sc["Close"].get("Reason", "OK"), 0)
        else:
            return None  # LazyTTS — не используем
        return {"streamcontrol": {"action": action, "reason": reason, "streamId": int(sc.get("StreamId", 0)),
                                  "messageId": sc.get("MessageId", "")}}
    ev = m.get("Event")
    if not ev:
        return None
    h = ev.get("Header", {})
    header = {"messageId": h.get("MessageId", ""), "refMessageId": h.get("RefMessageId", "")}
    if "StreamId" in h:
        header["streamId"] = int(h["StreamId"])
    kind = next((k for k in ev if k != "Header"), None)
    body = ev.get(kind) or {}
    names = {
        "SynchronizeStateRequest": ("System", "SynchronizeState"),
        "VoiceInput": ("Vins", "VoiceInput"),
        "TextInput": ("Vins", "TextInput"),
        "LogSpotter": ("Log", "Spotter"),
        "RequestStat": ("Log", "RequestStat"),
        "Ack": ("System", "Ack"),
        "Ping": ("System", "Ping"),
        "CancelRequest": ("Vins", "CancelRequest"),
    }
    ns, name = names.get(kind, ("Proto", kind or "Unknown"))
    payload: dict = {"proto": body}
    if kind in ("VoiceInput", "TextInput"):
        rh = body.get("Header", {})
        event = (body.get("Request") or {}).get("Event") or {}
        payload = {
            "header": {"request_id": rh.get("RequestId"), "sequence_number": rh.get("SequenceNumber", 0)},
            "enable_spotter_validation": bool(body.get("EnableSpotterValidation")),
            "request": {"event": {"type": event.get("Type"), "text": event.get("Text"), "name": event.get("Name"),
                                  "payload": event.get("Payload") or {}}},
        }
        if kind == "TextInput" and event.get("Type") in (None, "text_input"):
            payload["request"]["event"]["type"] = "text_input"
    return {"event": {"header": {"namespace": ns, "name": name, **header}, "payload": payload}}


# ------------------------------------------------------------------ JSON -> proto (исходящие)

def _varint(n: int) -> bytes:
    # Отрицательные — двухкомплементарным int64 (10 байт, как protobuf). Раньше
    # `n >>= 7` у отрицательного в Python стремится к -1 вечно: `_varint(-10800)`
    # (любой пояс западнее UTC в lite.toml) блокировал event loop — весь lite
    # вставал (проверено исполнением 18.11).
    if n < 0:
        n += 1 << 64
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _f_str(f: int, s: str) -> bytes:
    b = s.encode("utf-8")
    return _varint((f << 3) | 2) + _varint(len(b)) + b


def _f_msg(f: int, m: bytes) -> bytes:
    return _varint((f << 3) | 2) + _varint(len(m)) + m


def _f_var(f: int, v: int) -> bytes:
    return _varint((f << 3) | 0) + _varint(v)


def _alarm_add_directive(p: dict) -> bytes:
    """Сериализация typed TAlarmAddDirective по РЕАЛЬНОЙ схеме прошивки.

    Источник: tools_silero/_ref/quasar-protos/alice/protos/data/scenario/alarm/
    alarm.proto и .../endpoint/capabilities/alarm/capability.proto:
      TAlarmAddDirective { Name=1, Alarm=2 }
      TAlarm { AlarmId=1, LocalTime=2{Hours,Minutes,Seconds}, Timezone=7{...},
               ActivationDays=5[{Weekday=1, Date=2}], Disabled=9, Title=10,
               EditTimestamp=11 }

    Две прошлые ошибки, из-за которых будильник принимался, но НЕ звенел:
      1) field 9 трактовался как «enabled» и туда писали 1 — а это Disabled.
         Значение 1 = будильник выключен inmediatamente после создания.
      2) field 11 считался «временем срабатывания», а это EditTimestamp.
    """
    import uuid
    alarm_id = str(p.get("alarm_id") or str(uuid.uuid4()))
    h = int(p.get("hour", 0))
    mi = int(p.get("minute", 0))
    tz_off = int(p.get("tz_offset", 10800))

    # TAlarm.LocalTime = 2 {Hours=1, Minutes=2, Seconds=3}
    #
    # ВРЕМЯ ПЕРЕДАЁМ В МЕСТНОМ (МСК), НЕ В UTC. Проверено на живой колонке 15.11
    # разбором event_scheduler: отправленное 16:52 дало
    # «when: 1794761520+3:0:0 2026-11-15 16:52:00+3:0:0», nearest=1794750720
    # (13:52 UTC) — то есть колонка САМА переводит местное время в UTC,
    # применяя Timezone.OffsetSeconds. И звонок реально прозвучал в 13:52:00
    # («Need volume approvement» -> «Approve volume change» в maind.log).
    # Попытка «отнять сдвиг заранее» сдвигала звонок на 3 часа раньше.
    alarm = _f_str(1, alarm_id)
    alarm += _f_msg(2, _f_var(1, h) + _f_var(2, mi) + _f_var(3, 0))

    days = p.get("days") or []
    if days:
        # повтор: ActivationDays = 5 [{Weekday=1}] по каждому дню
        for wd in days:
            alarm += _f_msg(5, _f_var(1, int(wd)))
    else:
        # разовый: ActivationDays = 5 [{Date=2{Year,Month,Day}}]
        # ВАЖНО: TActivationDay — это oneof {Weekday=1, Date=2}. Писать ОБА поля
        # нельзя: protobuf оставляет последнее, и weekday затирался — в логе
        # колонки появлялось «Got empty activation days». Для разового — ТОЛЬКО дата.
        now_local = datetime.datetime.utcnow() + datetime.timedelta(seconds=tz_off)
        when_local = now_local.replace(hour=h, minute=mi, second=0, microsecond=0)
        if when_local <= now_local:
            when_local += datetime.timedelta(days=1)
        date = _f_var(1, when_local.year) + _f_var(2, when_local.month) + _f_var(3, when_local.day)
        alarm += _f_msg(5, _f_msg(2, date))

    # Timezone = 7 {TimezoneName=1, OffsetSeconds=2}
    tz_name = str(p.get("tz_name", "Europe/Moscow"))
    alarm += _f_msg(7, _f_str(1, tz_name) + _f_var(2, tz_off))

    # Disabled=9 НЕ выставляем (0 = активен). Раньше тут стояло 1 — и это
    # выключало будильник сразу после создания. Именно поэтому «пустой id» и
    # «already exists» в логах были ложным следом: директива принималась,
    # сохранялась в alarms.dat и просто никогда не срабатывала.
    if p.get("label"):
        alarm += _f_str(10, str(p["label"]))          # Title=10
    alarm += _f_var(11, int(time.time()))             # EditTimestamp=11

    # ВНИМАНИЕ: возвращаем ТОЛЬКО TAlarm. Обёртку TAlarmAddDirective
    # {Name=1, Alarm=2} добавляет _directive(). Двойная обёртка здесь давала
    # в логе колонки «Got empty activation days» (InvalidActivationDay):
    # колонка читала внешний Alarm как TAlarm, видела там строку Name вместо
    # AlarmId — и будильник не звенел, хотя alarms.dat рос.
    return alarm


# Номера typed-директив (TDirective.DirectiveType, поле 20; enum
# EDirectiveType из alice/protos/endpoint/capability.proto). Без этого поля
# maind видит PayloadRaw, но не знает, каким сообщением его парсить:
# в логе колонки «Try to convert directive to proto in no reflection IOSDK»,
# будильник молча принимается и НЕ звенит.
DIRECTIVE_TYPES = {
    "alarm_add_directive": 32,
    "timer_set_directive": 112,
    "timer_cancel_directive": 113,
    "timer_pause_directive": 114,
    "timer_resume_directive": 115,
    "timer_stop_directive": 276,
    "alarm_cancel_directive": 132,
    "alarm_snooze_directive": 116,
    "set_do_not_disturb_directive": 224,
}


def _timer_set_directive(p: dict) -> bytes:
    """Сериализация typed TTimerSetDirective по схеме прошивки.

    Источник: alice/protos/endpoint/capabilities/alarm/capability.proto:
      TTimerSetDirective { Name=1, Timer=2 }
      TTimerDescription { TimerId=1, StartTimestamp=2, Duration=3,
                          Title=10, Disabled=11 }

    На ПК этот путь проверен на живой колонке: таймер отсчитывает САМА и
    звенит без сервера. Без поля DirectiveType в _directive() колонка видит
    PayloadRaw, но не знает, чем парсить, и молча игнорирует.
    """
    timer_id = str(p.get("timer_id") or f"lite-{int(time.time())}")
    inner = _f_str(1, timer_id)                       # TimerId=1
    inner += _f_var(2, int(p.get("start") or time.time()))  # StartTimestamp=2
    inner += _f_var(3, int(p.get("seconds", 0)))      # Duration=3
    title = str(p.get("title") or "таймер")
    if title:
        inner += _f_str(10, title)                    # Title=10
    # Disabled=11 не выставляем: 0 = активен (та же ловушка, что у будильника —
    # единица там означала «выключен», и таймер молча не звенел).
    return inner


def _cancel_directive(name: str, value: str) -> bytes:
    """TTimerCancelDirective / TAlarmCancelDirective { Name=1, <Id>=2 }.

    Как и set-директивы, они typed: в JSON-payload колонка их не разбирает
    (нет reflection) и молча игнорирует — отмена «не срабатывала».
    Id — обычное string-поле 2 прямо в директиве, вложенности нет.
    """
    return _f_str(1, name) + _f_str(2, value)


def _directive(d: dict) -> dict:
    name = d.get("name", "")
    # typed-директивы (alarm_add и др.): поле 15 PayloadRaw (bytes) вместо Payload (Struct).
    # В реальном трафике Яндекса — NFDirective{..., 15: <сырые AlarmAddDirective>}.
    typed = d.get("typed")
    if typed is None and name == "alarm_add_directive":
        inner = _alarm_add_directive(d.get("payload") or {})
        # WARP: f1 = directive name (колонка IOSDK читает field 1 как Name, не alarm_id).
        # Реальный шаблон: {f1: "alarm_add_directive", f2: <AlarmAddDirective>}
        typed = _f_str(1, name) + _f_msg(2, inner)
    elif typed is None and name == "timer_set_directive":
        inner = _timer_set_directive(d.get("payload") or {})
        typed = _f_str(1, name) + _f_msg(2, inner)
    elif typed is None and name == "timer_cancel_directive":
        p = d.get("payload") or {}
        typed = _cancel_directive(name, str(p.get("timer_id") or ""))
    elif typed is None and name == "alarm_cancel_directive":
        p = d.get("payload") or {}
        typed = _cancel_directive(name, str(p.get("alarm_id") or ""))
    out = {"Type": d.get("type", "client_action"), "Name": name,
           "AnalyticsType": d.get("sub_name") or name}
    if d.get("id"):
        out["Id"] = d["id"]
    if typed is not None:
        import base64
        out["PayloadRaw"] = base64.b64encode(typed).decode("ascii")
        # oneof PayloadType: Payload и PayloadRaw взаимоисключающие — только PayloadRaw
        dt = d.get("directive_type") or DIRECTIVE_TYPES.get(name)
        if dt:
            out["DirectiveType"] = int(dt)  # иначе maind не знает, чем парсить PayloadRaw
    else:
        out["Payload"] = d.get("payload") or {}
    if d.get("is_led_silent"):
        out["IsLedSilent"] = True
    return out


def server_to_proto(msg: dict) -> dict | None:
    """JSON-сообщение сервера -> TServerMessage (dict). None — у этого сообщения нет proto-аналога."""
    if "streamcontrol" in msg:
        sc = msg["streamcontrol"]
        return {"StreamControl": {"StreamId": sc["streamId"], "MessageId": sc.get("messageId", ""),
                                  "Close": {"Reason": _JSON_TO_REASON.get(sc.get("reason", 0), "OK")}}}
    d = msg["directive"]
    h, p = d["header"], d["payload"]
    header = {"MessageId": h.get("messageId", ""), "RefMessageId": h.get("refMessageId", "")}
    if "streamId" in h:
        header["StreamId"] = h["streamId"]
    name = f"{h['namespace']}.{h['name']}"
    if name == "System.SynchronizeStateResponse":
        body = {"SynchronizeStateResponse": {"SessionId": p.get("SessionId", "")}}
    elif name == "System.Push":
        header["Ack"] = h.get("ack", 0)
        body = {"Push": {"Directives": [_directive(x) for x in p.get("directives", [])]}}
    elif name == "System.InputStartAck":
        body = {"InputStartAck": {"RequestStartTime": p["input_start_ack"]["request_start_time"]}}
    elif name == "ASR.Result":
        rec = [{"Normalized": r["normalized"], "Confidence": 0.999,
                "Words": [{"Value": w["value"], "Confidence": 0.999} for w in r["words"]]}
               for r in p["recognition"] if r.get("normalized")]
        body = {"AsrResult": {"EndOfUtt": p["endOfUtt"], "MessagesCount": 1, "Recognition": rec}}
    elif name == "Spotter.Validation":
        body = {"SpotterValidation": {"Result": "Ok" if p.get("valid") else "SpottingRejected"}}
    elif name == "Log.Ack":
        body = {"LogAck": {}}
    elif name == "Log.RequestStatAck":
        body = {"RequestStatAck": {}}
    elif name == "TTS.Speak":
        body = {"TtsSpeak": {"Format": p.get("format", "audio/opus"), "LazyTtsStreaming": False}}
    elif name == "Vins.VinsResponse":
        r, vr, ph = p.get("response", {}), p.get("voice_response", {}), p.get("header", {})
        voice = {"HasVoiceResponse": bool(vr.get("has_voice_response")), "ShouldListen": bool(vr.get("should_listen"))}
        if vr.get("output_speech"):
            voice["OutputSpeech"] = {"Text": vr["output_speech"].get("text", "")}
        body = {"AliceResponse": {
            "Header": {"RequestId": ph.get("request_id") or "", "ResponseId": ph.get("response_id", ""),
                       "SequenceNumber": ph.get("sequence_number") or 0, "ParentRequestId": ph.get("request_id") or ""},
            "Response": {"Cards": [{"Type": c.get("type", "simple_text"), "Text": c.get("text", ""),
                                    "CardId": c.get("card_id", "")} for c in r.get("cards", [])],
                         "Directives": [_directive(x) for x in r.get("directives", [])]},
            "VoiceResponse": voice,
        }}
    else:
        return None
    return {"Event": {"Header": header, **body}}
