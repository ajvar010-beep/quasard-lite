"""Тесты quasard-lite: sync_info, DNS-парсер, фильтрация релея, маршрутизация."""
from __future__ import annotations

import asyncio
import struct
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from column_lite.config import LiteConfig  # noqa: E402
from column_lite.net import _build_query, parse_a  # noqa: E402
from column_lite.relay import Relay  # noqa: E402
from column_lite.sync import build_sync_info  # noqa: E402

DEVICE = {"glagol": {"security": {"server_certificate": "CERT", "server_private_key": "KEY"}},
          "id": "TESTDEVICE01"}


# ---------------------------------------------------------------- sync_info
def test_sync_info_has_glagol_cert_and_full_config():
    cfg = LiteConfig(pin_ip="127.0.0.1")
    info = build_sync_info(cfg, DEVICE, "TESTDEVICE01", "yandexmicro")
    assert info["glagol"]["security"]["server_certificate"] == "CERT"
    sysc = info["config"]["system_config"]
    assert sysc["onlineSpotterEnabled"] is False
    assert sysc["uniProxyUrl"] == "wss://uniproxy.alice.yandex.net/uni.ws"
    assert info["config"]["account_config"]["spotter"] == "alisa"
    assert info["config"]["device_config"]["locale"] == "ru-RU"
    assert sysc["com.yandex.launcher.updaterapp"]["settings"]["allowFirmwareUpdate"]["ota"] is False


def test_sync_info_pins_do_not_break_oauth():
    """oauth/login должны остаться у настоящего Яндекса, иначе колонка не обновит токен."""
    cfg = LiteConfig(pin_ip="127.0.0.1")
    info = build_sync_info(cfg, DEVICE)
    params = info["config"]["system_config"]["unbound"]["serverParams"]
    pins = " ".join(p for p in params if p.startswith("local-data"))
    assert "quasar.yandex.net" in pins
    assert "uniproxy.alice.yandex.net" in pins
    assert "oauth.yandex.ru" not in pins
    assert "login.yandex.ru" not in pins
    assert "mobileproxy.passport.yandex.net" not in pins


def test_sync_info_without_device_does_not_send_empty_glagol():
    """Пустой сертификат ломает glagol колонки — лучше не отдавать блок вовсе."""
    info = build_sync_info(LiteConfig(), {})
    assert info["glagol"] == {}


# ---------------------------------------------------------------- DNS
def _dns_response(qid: int, ip: str) -> bytes:
    header = struct.pack(">HHHHHH", qid, 0x8180, 1, 1, 0, 0)
    question = b"\x08uniproxy\x05alice\x06yandex\x03net\x00" + struct.pack(">HH", 1, 1)
    answer = b"\xc0\x0c" + struct.pack(">HHIH", 1, 1, 60, 4) + bytes(int(x) for x in ip.split("."))
    return header + question + answer


def test_dns_parse_a():
    qid = 0x1234
    assert parse_a(_dns_response(qid, "1.2.3.4"), qid) == "1.2.3.4"
    assert parse_a(_dns_response(qid + 1, "1.2.3.4"), qid) is None  # чужой id
    assert parse_a(b"\x00" * 3, qid) is None                         # мусор


def test_dns_query_has_id_and_question():
    q = _build_query("uniproxy.alice.yandex.net", 0xABCD)
    assert struct.unpack(">H", q[:2])[0] == 0xABCD
    assert b"uniproxy" in q


# ---------------------------------------------------------------- релей
class FakeWS:
    def __init__(self):
        self.sent = []
        self.bin = []
        self.closed = False

    async def send_json(self, obj):
        self.sent.append(obj)

    async def send_bytes(self, b):
        self.bin.append(b)


class FakeUpstream:
    def __init__(self):
        self.ws = object()
        self.sent = []
        self.bin = []

    async def send_json(self, obj):
        self.sent.append(obj)

    async def send_bytes(self, b):
        self.bin.append(b)

    async def close(self):
        self.ws = None


def _voice_input(msg_id: str, rid: str, text_hint: str = "") -> dict:
    return {"event": {"header": {"namespace": "Vins", "name": "VoiceInput",
                                 "messageId": msg_id, "streamId": 1},
                      "payload": {"header": {"request_id": rid, "sequence_number": 1},
                                  "request": {"event": {"type": "voice_input", "text": None}}}}}


def _asr_final(ref: str, text: str) -> dict:
    return {"directive": {"header": {"namespace": "ASR", "name": "Result", "refMessageId": ref},
                          "payload": {"endOfUtt": True,
                                      "recognition": [{"normalized": text}]}}}


def _cloud_response(ref: str, rid: str, mid: str = "cloud-mid") -> dict:
    return {"directive": {"header": {"namespace": "Vins", "name": "VinsResponse",
                                     "messageId": mid, "refMessageId": ref},
                          "payload": {"header": {"request_id": rid}, "voice_response": {}}}}


async def _make_relay(ours: bool, reply):
    ws = FakeWS()
    calls = []

    def classify(text):
        calls.append(text)
        return ours

    async def handle(text, rid, seq):
        return reply

    r = Relay(ws, LiteConfig(), classify, handle)
    r.up = FakeUpstream()
    return r, ws, calls


@pytest.mark.asyncio
async def test_relay_suppresses_cloud_answer_for_our_command(monkeypatch):
    # Ответ теперь озвучивается на колонке (edge-tts по сети). В тесте сети нет:
    # синтез «не удался» — ответ должен уйти без реплики (fallback-путь).
    from column_lite import tts as lite_tts

    async def _no_tts(*a, **kw):
        return None
    monkeypatch.setattr(lite_tts, "speak_to_file", _no_tts)
    reply = type("R", (), {"speech": "Включаю Кино.", "directives": [{"name": "audio_play"}]})()
    r, ws, calls = await _make_relay(True, reply)
    # колонка прислала голосовой запрос
    r._req_by_msg["m1"] = ("rid1", 1)
    await r._on_upstream_msg(_asr_final("m1", "включи кино"))
    await asyncio.sleep(0.01)   # даём задаче ответа выполниться
    # ASR ушёл клиенту, а наш VinsResponse — тоже
    names = [m["directive"]["header"]["name"] for m in ws.sent]
    assert "Result" in names
    assert "VinsResponse" in names
    # облачный ответ на тот же request_id глушится
    before = len(ws.sent)
    await r._on_upstream_msg(_cloud_response("m1", "rid1"))
    assert len(ws.sent) == before
    assert calls == ["включи кино"]


@pytest.mark.asyncio
async def test_relay_passes_cloud_answer_for_foreign_command():
    r, ws, calls = await _make_relay(False, None)
    r._req_by_msg["m1"] = ("rid1", 1)
    await r._on_upstream_msg(_asr_final("m1", "какая погода"))
    await asyncio.sleep(0.01)
    await r._on_upstream_msg(_cloud_response("m1", "rid1"))
    names = [m["directive"]["header"]["name"] for m in ws.sent]
    assert names.count("VinsResponse") == 1  # ровно облачный, без нашего


@pytest.mark.asyncio
async def test_relay_drops_tts_and_audio_of_suppressed_answer():
    reply = type("R", (), {"speech": "Включаю.", "directives": []})()
    r, ws, _ = await _make_relay(True, reply)
    r._req_by_msg["m1"] = ("rid1", 1)
    await r._on_upstream_msg(_asr_final("m1", "включи кино"))
    await asyncio.sleep(0.01)
    await r._on_upstream_msg(_cloud_response("m1", "rid1", mid="cloud-mid"))
    # TTS, порождённый заглушённым ответом, не идёт клиенту
    tts = {"directive": {"header": {"namespace": "TTS", "name": "Speak",
                                    "refMessageId": "cloud-mid", "streamId": 2}, "payload": {}}}
    before = len(ws.sent)
    await r._on_upstream_msg(tts)
    assert len(ws.sent) == before
    # и аудио этого потока тоже
    frame = struct.pack(">I", 2) + b"AUDIO"
    await r._on_binary(frame)
    assert ws.bin == []


@pytest.mark.asyncio
async def test_relay_passes_foreign_audio():
    r, ws, _ = await _make_relay(False, None)
    frame = struct.pack(">I", 7) + b"AUDIO"
    await r._on_binary(frame)
    assert ws.bin and ws.bin[0] == frame


# ---------------------------------------------------------------- маршрутизация
def test_router_classify_does_not_steal_foreign_commands():
    from column_lite.main import Router
    r = Router(LiteConfig())
    assert r.classify("включи кино группа крови") is True
    assert r.classify("включи радио маяк") is True
    # Таймеры и будильники теперь СВОИ: без ПК их ставит lite родной директивой,
    # чтобы колонка звенела сама (было: «таймеры — облаку»).
    assert r.classify("поставь таймер на 5 минут") is True
    assert r.classify("поставь будильник на 7 утра") is True
    # погода и курс валют теперь отвечаются локально (с 20.11), а не облаком
    assert r.classify("какая погода в Москве") is True
    assert r.classify("какой курс доллара") is True
    assert r.classify("включи свет") is True   # подсветка колонки, с 20.11
    assert r.classify("включи телевизор") is False
    assert r.classify("завтра будет дождь") is False
    # медиа-фраза не должна становиться таймером
    assert r.classify("поставь музыку на 5 минут") is True  # это поиск музыки


def test_timer_command_builds_native_directives():
    """Таймер/будильник ставятся РОДНОЙ директивой: колонка звенит без lite."""
    from column_lite.main import Router
    from column_lite import intents
    r = Router(LiteConfig())

    cmd = intents.match_timer("поставь таймер на 10 минут")
    reply = r.timers.run(cmd)
    assert reply is not None and "10 минут" in reply.speech
    names = [d["name"] for d in reply.directives]
    assert names == ["timer_set_directive"]
    d = reply.directives[0]
    assert d["payload"]["seconds"] == 600
    assert d["payload"]["timer_id"]

    cmd = intents.match_timer("поставь будильник на 7 утра")
    reply = r.timers.run(cmd)
    assert reply is not None and "7:00" in reply.speech
    names = [d["name"] for d in reply.directives]
    assert names == ["alarm_add_directive"]
    p = reply.directives[0]["payload"]
    assert (p["hour"], p["minute"]) == (7, 0)
    # МСК по умолчанию: колонка сама переведёт местное время в UTC
    assert p["tz_offset"] == 10800
    assert p["tz_name"] == "Europe/Moscow"


def test_daily_alarm_repeats_every_day():
    """«каждый день» разворачивается в 7 дней недели, а не в разовую дату."""
    from column_lite.main import Router
    from column_lite import intents
    r = Router(LiteConfig())
    cmd = intents.match_timer("поставь будильник на семь утра каждый день")
    reply = r.timers.run(cmd)
    p = reply.directives[0]["payload"]
    assert sorted(p["days"]) == [1, 2, 3, 4, 5, 6, 7]
    assert p["repeat"] is True


def test_alarm_colon_time_not_glued():
    """«будильник на 16:06» — это 16:06, а не 22:00 (16+6 от _norm)."""
    from column_lite import intents
    cmd = intents.match_timer("поставь будильник на 16:06")
    assert (cmd.hour, cmd.minute) == (16, 6)
    cmd = intents.match_timer("поставь будильник на 18:05")
    assert (cmd.hour, cmd.minute) == (18, 5)
    # «двадцать три» по-прежнему склеивается в 23
    cmd = intents.match_timer("поставь будильник на двадцать три тридцать")
    assert (cmd.hour, cmd.minute) == (23, 30)


# ------------------------------------------------ регрессии баг-чека 19.11
def test_daily_alarm_not_shifted_to_pm():
    """«каждый ДЕНЬ в 7:30» — это 7:30, а не 19:30: слово «день» из повтора
    не должно трактоваться как «дня» (pm). Живой баг 18.11."""
    from column_lite import intents
    for phrase in ("будильник на каждый день в 7:30",
                   "будильник каждый день на 6:30",
                   "будильник на 7:30 каждый день"):
        cmd = intents.match_timer(phrase)
        assert cmd is not None, phrase
        assert cmd.action == "add_alarm"
        assert (cmd.hour, cmd.minute) in ((7, 30), (6, 30)), (phrase, cmd.hour, cmd.minute)
        assert cmd.repeat is True
    # «дня» как часть суток по-прежнему работает
    cmd = intents.match_timer("будильник на семь тридцать вечера")
    assert (cmd.hour, cmd.minute) == (19, 30)


def test_alarm_hours_and_minutes():
    """«на 7 часов 30 минут» -> 07:30 (раньше минуты терялись)."""
    from column_lite import intents
    cmd = intents.match_timer("будильник на 7 часов 30 минут")
    assert (cmd.hour, cmd.minute) == (7, 30)


def test_relative_alarm():
    """«через 10 минут» / «на 10 минут» у будильника — относительное время,
    а не 10:00/02:00 (живой баг 18.11)."""
    import time as _t
    from column_lite import intents
    cmd = intents.match_timer("разбуди меня через 10 минут")
    assert cmd.action == "add_alarm"
    now = _t.localtime()
    want_min = (now.tm_hour * 60 + now.tm_min + 10) % (24 * 60)
    assert cmd.hour * 60 + cmd.minute == want_min
    cmd = intents.match_timer("поставь будильник на 10 минут")
    assert cmd.action == "add_alarm"
    # «будильник на 10 часов вечера» — по-прежнему абсолютное 22:00
    cmd = intents.match_timer("будильник на 10 часов вечера")
    assert (cmd.hour, cmd.minute) == (22, 0)


def test_tens_in_durations():
    """«шестьдесят/девяносто минут» -> часы, а не 60 секунд."""
    from column_lite import intents
    assert intents.match_timer("поставь таймер на шестьдесят минут").seconds == 3600
    assert intents.match_timer("поставь таймер на девяносто минут").seconds == 5400
    assert intents.match_timer("поставь таймер на полчаса").seconds == 1800


def test_book_and_music_do_not_misfire():
    """«включи книгу» без названия — переспрос книги, а не поиск музыки «книгу»."""
    from column_lite import intents
    assert intents.match_book("включи книгу") == ("play", "")
    assert intents.match_book("включи аудиокнигу") == ("play", "")
    assert intents.match_book("почитай мне") == ("play", "")
    assert intents.match_book("включи книгу война и мир") == ("play", "война и мир")
    assert intents.match_music("включи книгу") is None
    # не музыка — не ищем треки
    assert intents.match_music("включи чайник") is None
    assert intents.match_music("включи фильм интерстеллар") is None
    assert intents.match_music("поставь громкость на пятьдесят процентов") is None
    # а настоящая музыка работает
    assert intents.match_music("включи кино группа крови") == ("search", "кино группа крови")


def test_player_resume_accepts_politeness():
    from column_lite import intents
    for phrase in ("продолжай", "продолжай, пожалуйста", "продолжай музыку", "возобнови"):
        assert intents.match_player_resume(phrase) is True, phrase


def test_radio_cases():
    """Падежи ASR: «европу плюс», «дфм», «хит фм» — правильная станция.
    «Энергия» убрана 19.11 (все её эндпоинты мертвы — 403/refused даже с ПК)."""
    from column_lite.streams_lite import radio_pick
    assert radio_pick("европу плюс")[0] == "Европа Плюс"
    assert radio_pick("дфм")[0] == "DFM"
    assert radio_pick("вести фм")[0] == "Вести ФМ"
    assert radio_pick("русское радио")[0] == "Русское радио"
    assert radio_pick("хит фм")[0] == "Хит FM"
    assert radio_pick("максимум")[0] == "Радио Максимум"
    assert radio_pick("")[0] == "Радио Маяк"


def test_timer_ids_unique_and_cancel_sends_each(monkeypatch, tmp_path):
    """Два таймера в одну секунду получают разные id; «отмени» шлёт директиву
    на КАЖДЫЙ таймер (раньше — одну на все)."""
    from column_lite import main as M
    from column_lite import intents
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    r = M.Router(LiteConfig())
    r.timers.run(intents.match_timer("поставь таймер на 10 минут"))
    r.timers.run(intents.match_timer("поставь таймер на 15 минут"))
    ids = [t["id"] for t in r.timers._timers]
    assert len(ids) == 2 and ids[0] != ids[1]
    rep = r.timers.run(intents.match_timer("отмени таймер"))
    assert sorted(ids) == sorted(d["payload"]["timer_id"] for d in rep.directives)
    assert r.timers._timers == []
    # повторная отмена не шлёт мёртвые директивы
    rep2 = r.timers.run(intents.match_timer("отмени таймер"))
    assert rep2.directives == []


def test_timers_state_file_garbage_does_not_crash(monkeypatch, tmp_path):
    """Битый timers_state.json (список вместо объекта) не должен ронять lite."""
    from column_lite import main as M
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    (tmp_path / "timers_state.json").write_text("[1, 2, 3]", "utf-8")
    r = M.Router(LiteConfig())   # раньше падало AttributeError прямо в __init__
    assert r.timers._timers == [] and r.timers._alarms == []
    r2 = M.Router(LiteConfig())  # {"timers": null} — тоже не падает
    assert r2.timers._timers == []


def test_varint_negative_terminates():
    """_varint(-n) — двухкомплементарный int64; раньше вечный цикл (зависание)."""
    from column_lite.proto_api import _varint
    b = _varint(-10800)
    # int64(-10800) = 0xFFFFFFFFFFFFD5D0 -> 10 групп по 7 бит, последняя 0x01
    assert len(b) == 10 and b[-1] == 0x01
    assert _varint(300) == b"\xac\x02"


def test_dump_wav_header_valid():
    """/_dump_voice: struct.pack падал (13 vs 11) — заголовок WAV должен быть."""
    import struct as _s
    from column_lite.voice import VoiceLoop
    v = VoiceLoop(cfg=None, device_id="x", jwt_key=b"k")
    v._ring.append(b"\x00\x01" * 1600)
    wav = v.dump_wav(0.1)
    assert wav[:4] == b"RIFF" and wav[8:12] == b"WAVE"
    (size,) = _s.unpack("<I", wav[40:44])
    assert size == len(wav) - 44


def test_stop_is_ours():
    """«стоп/пауза» должны обрабатываться локально (glagol), а не уходить облаку."""
    from column_lite.main import Router
    r = Router(LiteConfig())
    for phrase in ("стоп", "пауза", "хватит", "замолчи", "выключи музыку"):
        assert r.classify(phrase) is True, phrase


def test_egress_allows_radio_8443():
    """«Энергия» живёт на :8443 — egress обязан её пускать."""
    text = (ROOT / "column_lite" / "lock_column_egress.sh").read_text("utf-8")
    assert "8000:8500" in text


def test_unbound_lite_pins_match_config():
    """unbound.lite (файл для failover) не должен пинить oauth/login: у lite нет
    /token — колонка не смогла бы обновить токен (регрессия закрытого P0)."""
    text = (ROOT / "column_lite" / "unbound.lite").read_text("utf-8")
    # проверяем только действующие директивы, не комментарии (в комментариях
    # слова oauth/mobileproxy/77.88.8.88 упоминаются намеренно — пояснение)
    directives = [ln.strip() for ln in text.splitlines()
                  if ln.strip().startswith(("local-data", "forward-addr"))]
    body = " ".join(directives)
    assert "quasar.yandex.net" in body
    assert "uniproxy.alice.yandex.net" in body
    assert "oauth.yandex.ru" not in body
    assert "login.yandex.ru" not in body
    assert "mobileproxy" not in body
    # заблокированные egress-ом форвардеры не упоминаем в forward-addr
    assert "77.88.8.88" not in body and "77.88.8.2" not in body


def test_sync_forwarders_are_allowed():
    cfg = LiteConfig(pin_ip="127.0.0.1")
    fw = build_sync_info(cfg, DEVICE)["config"]["system_config"]["unbound"]["forwarders"]
    assert "77.88.8.88" not in fw and "77.88.8.2" not in fw


# ------------------------------------------------ авторетрай радио (19.11)
def _radio_router(monkeypatch, tmp_path):
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    r = M.Router(LiteConfig())
    s = r.streams.radio(r.cfg.device_id, "маяк")
    return M, r, s


@pytest.mark.asyncio
async def test_radio_on_failed_restarts_station(monkeypatch, tmp_path):
    """Обрыв эфира (on_failed) перезапускает ту же станцию, а не молчит."""
    M, r, s = _radio_router(monkeypatch, tmp_path)
    rep = await r.action(M.CB_PREFIX + "on_failed", "rid1")
    assert rep is not None
    assert rep.directives and rep.directives[0]["name"] == "audio_play"
    url = rep.directives[0]["payload"]["stream"]["url"]
    # порт берётся из конфига (http_port), по умолчанию 80 (аудит 22.11)
    assert url.startswith("http://127.0.0.1:80/_radio")
    assert s.fails == 1


@pytest.mark.asyncio
async def test_radio_on_failed_gives_up_after_limit(monkeypatch, tmp_path):
    """Станция, падающая сразу после старта, не крутится бесконечно."""
    M, r, s = _radio_router(monkeypatch, tmp_path)
    s.last_fail = __import__("time").monotonic()   # считаем серию обрывов
    for _ in range(M.RADIO_MAX_RETRIES):
        rep = await r.action(M.CB_PREFIX + "on_failed", "rid")
        assert rep.directives          # пока лимит не исчерпан — перезапуск
        await r.action(M.CB_PREFIX + "on_started", "rid")   # плеер ответил «играю»
    rep = await r.action(M.CB_PREFIX + "on_failed", "rid")
    assert "не отвечает" in rep.speech.lower()
    assert r.cfg.device_id not in r.streams.sessions


@pytest.mark.asyncio
async def test_radio_stable_playback_resets_fail_counter(monkeypatch, tmp_path):
    """Долгая стабильная работа обнуляет счётчик: единичные обрывы не копятся."""
    import time as _t
    M, r, s = _radio_router(monkeypatch, tmp_path)
    await r.action(M.CB_PREFIX + "on_failed", "r1")
    assert s.fails == 1
    s.last_fail = _t.monotonic() - (M.RADIO_RESET_S + 5)   # была долгая тишина
    await r.action(M.CB_PREFIX + "on_started", "r2")
    assert s.fails == 0
    await r.action(M.CB_PREFIX + "on_failed", "r3")
    assert s.fails == 1


@pytest.mark.asyncio
async def test_relay_handles_playback_callbacks_locally_even_with_cloud():
    """Колбэки плеера при ЖИВОМ облаке не уходят в апстрим (иначе очередь
    встаёт после первого трека: у облака нет нашей stack-сессии)."""
    from column_lite.main import CB_PREFIX
    seen = []

    async def on_action(name, rid):
        seen.append(name)
        return None

    ws = FakeWS()
    relay = Relay(ws, LiteConfig(), lambda t: False, lambda *a: None, on_action)
    relay.up = FakeUpstream()
    relay._proto_mode = False
    msg = {"event": {"header": {"namespace": "Vins", "name": "TextInput", "messageId": "m"},
                     "payload": {"header": {"request_id": "rid", "sequence_number": 1},
                                 "request": {"event": {"type": "server_action",
                                                       "name": CB_PREFIX + "on_failed"}}}}}
    await relay._on_client_msg(msg)
    await asyncio.sleep(0.02)
    assert seen == [CB_PREFIX + "on_failed"]
    # в облако колбэк не пошёл
    assert not any(isinstance(x, dict) and x.get("event", {}).get("header", {}).get("name") == "TextInput"
                   for x in relay.up.sent)


@pytest.mark.asyncio
async def test_icy_strip_removes_metadata():
    """icy_strip вырезает ICY-метаданные и отдаёт только аудио."""
    from column_lite.main import icy_strip

    class Up:
        headers = {"icy-metaint": "8"}
        def aiter_bytes(self, n):
            async def g():
                # 8 байт аудио, затем 1 байт длины (2 блока=32 байта) и 32 байта мета
                yield b"AUDIO000"
                yield bytes([2]) + b"StreamTitle='x';".ljust(32, b"\x00")
                yield b"AUDIO111" + bytes([0])  # мета-блок нулевой длины
            return g()

    out = b"".join([c async for c in icy_strip(Up())])
    assert out == b"AUDIO000AUDIO111"
    assert b"StreamTitle" not in out


def test_timers_survive_restart(tmp_path, monkeypatch):
    """После рестарта lite «отмени будильник» видит будильники, поставленные раньше."""
    from column_lite import main as M
    from column_lite.config import LiteConfig
    from column_lite import intents
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    r1 = M.Router(LiteConfig())
    r1.timers.run(intents.match_timer("поставь будильник на семь утра"))
    r1.timers.run(intents.match_timer("поставь будильник на восемь утра"))
    r2 = M.Router(LiteConfig())          # «рестарт»: новый Router, тот же LITE_DIR
    assert len(r2.timers._alarms) == 2
    rep = r2.timers.run(intents.match_timer("отмени будильник"))
    assert len(rep.directives) == 2
    assert all(d["name"] == "alarm_cancel_directive" for d in rep.directives)
    assert "нет" not in rep.speech.lower()


def test_rank_books_prefers_exact_audiobook():
    """Аудиокнига выше альбома мюзикла; «война и мир» != «война за мир»."""
    from column_lite.streams_lite import rank_books
    docs = [
        {"identifier": "mm-2014-cast",
         "title": "Master and Margarita 2014 Original Russian Cast Album (Мастер и Маргарита)"},
        {"identifier": "bulgakov_master_audiobook",
         "title": "Булгаков - Мастер и Маргарита (аудиокнига)", "language": "rus"},
        {"identifier": "voina-za-mir", "title": "Война за мир"},
    ]
    got = rank_books(docs, "мастер и маргарита")
    assert got[0][0] == "bulgakov_master_audiobook"
    assert all(i != "voina-za-mir" for i, _ in got)
    docs = [{"identifier": "a", "title": "Жидков - Война за мир"},
            {"identifier": "b", "title": "Толстой Война и мир"}]
    assert rank_books(docs, "война и мир")[0][0] == "b"


def test_book_files_order_and_dupes():
    from column_lite.streams_lite import book_files
    files = [{"name": "part_10.mp3"}, {"name": "part_2.mp3"}, {"name": "part_2_64kb.mp3"},
             {"name": "intro.mp3"}, {"name": "cover.jpg"}, {}]
    assert [f["name"] for f in book_files(files)] == ["intro.mp3", "part_2.mp3", "part_10.mp3"]


def test_rank_books_russian_shelf_beats_english_librivox():
    """«Война и мир» по-русски не должна проигрывать английской LibriVox-версии:
    коллекция audioboo_ru (русские аудиокниги) — отдельная база, раньше её
    выдачу топили подкасты и английские озвучки (21.11)."""
    from column_lite.streams_lite import rank_books
    docs = [
        {"identifier": "war_and_peace_librivox", "title": "Война и мир (War and Peace)",
         "language": "eng", "collection": ["librivoxaudio"], "downloads": 500000},
        {"identifier": "voina_i_mir_rus", "title": "Толстой - Война и мир",
         "language": "rus", "collection": ["audioboo_ru"]},
    ]
    got = rank_books(docs, "война и мир")
    assert got[0][0] == "voina_i_mir_rus"


def test_rank_books_latin_query_prefers_librivox():
    """Английский запрос — LibriVox впереди случайных сборников."""
    from column_lite.streams_lite import rank_books
    docs = [
        {"identifier": "random_mix", "title": "Sherlock Holmes mix tape",
         "language": "eng"},
        {"identifier": "sherlock_holmes_1008_librivox",
         "title": "The Adventures of Sherlock Holmes",
         "language": "eng", "collection": "librivoxaudio", "downloads": 900000},
    ]
    assert rank_books(docs, "sherlock holmes")[0][0].startswith("sherlock_holmes")


@pytest.mark.asyncio
async def test_book_search_queries_audiobook_shelves(monkeypatch):
    """Поиск книг обязан опрашивать полки аудиокниг, а не только общий поиск по audio."""
    from column_lite import streams_lite as S
    seen: list[str] = []

    async def fake(cl, q, rows=15):
        seen.append(q)
        return []

    monkeypatch.setattr(S, "_ia_query", fake)
    await S.book_search("война и мир")
    joined = " | ".join(seen)
    assert "collection:librivoxaudio" in joined
    assert "collection:audio_bookspoetry" in joined
    assert "collection:audioboo_ru" in joined
    assert "медиатека" not in joined


# ------------------------------------------------ новые музыкальные базы (22.11)
def test_mp3xa_parse():
    """mp3xa: артист/название/длительность/прямой mp3 из блока plyr-item."""
    from column_lite.streams_lite import mp3xa_parse
    html = '''
    <div class="plyr-item plyr-item--rating" data-id="1"><span class="status">2</span></div>
    <div class="plyr-item" data-id="2">
      <span class="status fixed">2</span>
      <div class="name"><a href="/singer/kino/">Кино</a></div>
      <div class="song_name"><a href="/hits/1-x.html">Группа крови</a></div>
      <div class="time-keep rating"><span class="duration">04:21</span></div>
      <div class="maudio"><a class="play"
        data-url="https://cdnproxy3.mp3xa.me/abc/1795380282/x.mp3"
        data-url_lq="https://cdnproxy3.mp3xa.me/q/x.mp3"></a></div>
    </div>'''
    got = mp3xa_parse(html)
    assert len(got) == 1
    t = got[0]
    assert (t.artist, t.title) == ("Кино", "Группа крови")
    assert t.url.startswith("https://cdnproxy3.mp3xa.me/") and t.duration_ms == 261000


def test_drivemusic_parse_requires_search_page():
    """drivemusic: главная (без c-search-results) — чужое «популярное», не берём."""
    from column_lite.streams_lite import drivemusic_parse
    item = ('<div class="music-popular__item"><div class="btn_player">'
            '<button data-url="https://ru-cdn1.drivemusic.me/dl/online/x/y.mp3">'
            '</button></div><div class="popular-play-name">'
            '<a href="/p/1.html" class="popular-play-author">Беспечный ангел</a></div></div>')
    assert drivemusic_parse(item) == []
    got = drivemusic_parse('<div class="c-search-results">' + item + '</div>')
    assert len(got) == 1 and got[0].title == "Беспечный ангел"
    assert got[0].url.endswith("y.mp3")


# ------------------------------------------------ wave / вопросы о модели (22.11)
class _FakeLLM:
    def __init__(self):
        self.calls: list[str] = []

    def active_model(self) -> tuple[str, str]:
        return "mistral", "mistral-small-latest"

    async def ask(self, text: str, timeout: float = 20.0) -> str:
        self.calls.append(text)
        return "Текстовый ответ"


def _wave_router(monkeypatch, tmp_path):
    """Router с подставной волной/базой и фейковым LLM (без сети)."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite.streams_lite import Session, Track
    r = M.Router(LiteConfig())
    s = Session(kind="music", title="Кино",
                queue=[Track(url=f"http://x/{i}.mp3", title=f"Трек {i}",
                             duration_ms=200_000) for i in range(3)])
    started = []

    async def fake_wave(device):
        r.streams.sessions[device] = s
        started.append(device)
        return s, "Включаю музыку."

    async def fake_music(device, q):
        r.streams.sessions[device] = s
        return s, "Включаю."

    async def fake_book(device, q):
        return s, "Включаю."

    async def fake_resume(device):
        return s, "Продолжаю."

    async def fake_cmd(cfg, cmd, guard=None):
        return True

    async def fake_state(cfg):
        return {"playing": True, "progress": 0.0}

    async def fake_weather(device, rid, *kind):
        return M.Reply("Погода.")

    async def fake_rates(device, rid, which):
        return M.Reply("Курс.")

    monkeypatch.setattr(r.streams, "wave", fake_wave)
    monkeypatch.setattr(r.streams, "music", fake_music)
    monkeypatch.setattr(r.streams, "book", fake_book)
    monkeypatch.setattr(r.streams, "resume_book", fake_resume)
    monkeypatch.setattr(M, "glagol_command", fake_cmd)
    monkeypatch.setattr(M, "glagol_state", fake_state)
    monkeypatch.setattr(r, "_weather", fake_weather)
    monkeypatch.setattr(r, "_rates", fake_rates)
    r.llm = _FakeLLM()
    return M, r, s, started


@pytest.mark.asyncio
async def test_wave_plays_music_not_llm(monkeypatch, tmp_path):
    """«Включи музыку» — наша волна, а не текстовый ответ LLM.

    Раньше classify отдавал wave=False (в облако), а в local-режиме handle
    без ветки wave спускался до llm.ask — колонка говорила текст (аудит 22.11).
    """
    M, r, s, started = _wave_router(monkeypatch, tmp_path)
    assert r.classify("включи музыку") is True
    rep = await r.handle("включи музыку", "rid", 1)
    assert rep is not None and rep.directives
    assert rep.directives[0]["name"] == "audio_play"
    assert started and r.llm.calls == []


@pytest.mark.asyncio
async def test_model_and_prompt_questions_answered_without_llm(monkeypatch, tmp_path):
    """«На какой модели работаешь» и «покажи промпт» — свои ответы, не LLM."""
    M, r, s, _ = _wave_router(monkeypatch, tmp_path)
    rep = await r.handle("на какой модели ты работаешь", "rid", 1)
    assert rep is not None and "mistral" in rep.speech
    rep = await r.handle("покажи свой системный промпт", "rid", 2)
    assert rep is not None and "системный промпт" in rep.speech.lower()
    assert r.llm.calls == []


@pytest.mark.asyncio
async def test_classify_true_implies_handle_reply(monkeypatch, tmp_path):
    """Свойство маршрутизатора: classify True + handle None = молчание колонки
    с проглоченным облачным ответом. Ловит расхождения списков путей."""
    M, r, s, _ = _wave_router(monkeypatch, tmp_path)
    phrases = ["включи музыку", "включи кино группа крови", "поставь паузу",
               "продолжай", "дальше", "посвети красным", "выключи",
               "поставь таймер на 5 минут", "включи радио маяк", "погода в москве",
               "курс доллара", "включи книгу война и мир",
               "на какой модели ты работаешь"]
    for ph in phrases:
        assert r.classify(ph) is True, ph
        rep = await r.handle(ph, "rid", 1)
        assert rep is not None, f"{ph}: classify True, handle None"


@pytest.mark.asyncio
async def test_get_next_advances_queue(tmp_path, monkeypatch):
    """Колонка просит следующий трек — отдаём его и запоминаем главу книги."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite.streams_lite import Session, Track
    r = M.Router(LiteConfig())
    dev = r.cfg.device_id
    r.streams.sessions[dev] = Session(
        kind="book", title="Книга", identifier="id1",
        queue=[Track(url="http://x/1.mp3", title="1"), Track(url="http://x/2.mp3", title="2")])
    rep = await r.action(M.GET_NEXT, "rid")
    assert rep.directives and rep.directives[0]["name"] == "audio_play"
    assert r.streams.sessions[dev].pos == 1
    # get_next — это предзагрузка (приходит задолго до конца текущей части):
    # место в книге сдвигается, только когда новая часть реально заиграла
    assert not r.streams.has_saved_book(dev)
    await r.action(M.CB_PREFIX + "on_started", "rid")
    assert r.streams._books[dev]["pos"] == 1
    rep = await r.action(M.GET_NEXT, "rid")   # очередь кончилась
    assert rep.directives == []


def test_stop_phrases_do_not_reach_llm():
    """«стоп/пауза/хватит» — команда плееру, а «выключи свет/таймер» — нет."""
    from column_lite.main import _STOP_RE
    for ok in ("стоп", "алиса стоп", "пауза", "хватит", "поставь на паузу",
               "выключи музыку", "останови книгу", "стоп пожалуйста"):
        assert _STOP_RE.match(ok), ok
    for no in ("выключи свет", "останови таймер", "выключи будильник",
               "стоп кран это что", "поставь таймер на паузу"):
        assert not _STOP_RE.match(no), no


# ------------------------------------------------ пауза / продолжение (19.11)
def _music_router(monkeypatch, tmp_path, kind="music"):
    """Router с нашей сессией и подставным glagol (без колонки)."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite.streams_lite import Session, Track
    r = M.Router(LiteConfig())
    s = Session(kind=kind, title="Кино",
                queue=[Track(url=f"http://x/{i}.mp3", title=f"Трек {i}", artist="Кино",
                             duration_ms=200_000) for i in range(3)])
    r.streams.sessions[r.cfg.device_id] = s

    cmds = []
    state = {"playing": True, "progress": 42.5}

    async def fake_cmd(cfg, cmd, guard=None):
        if guard is not None and not guard():
            return False
        cmds.append(cmd)
        state["playing"] = False
        return True

    async def fake_state(cfg):
        return dict(state)
    monkeypatch.setattr(M, "glagol_command", fake_cmd)
    monkeypatch.setattr(M, "glagol_state", fake_state)
    return M, r, s, cmds, state


def _offset_of(rep):
    for d in rep.directives:
        if d.get("name") == "audio_play":
            return d["payload"]["stream"]["offset_ms"]
    return None


def _stream_id_of(rep):
    for d in rep.directives:
        if d.get("name") == "audio_play":
            return d["payload"]["stream"]["id"]
    return None


@pytest.mark.asyncio
async def test_pause_remembers_position_and_resume_uses_offset(monkeypatch, tmp_path):
    """«Пауза» запоминает секунду из playerState, «продолжай» включает её же."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    rep = await r.handle("пауза", "rid", 0)
    assert rep.speech == ""
    assert s.paused is True          # отметка паузы — сразу, до остановки плеера
    await r._pause_settled()         # stop и точная секунда — фоном, после ответа
    assert cmds == ["stop"]
    assert s.paused is True and s.paused_at == 42.5

    rep = await r.handle("продолжай", "rid2", 0)
    assert _offset_of(rep) == 42500, _offset_of(rep)
    assert s.paused is False
    # свежий id стрима: mediad не должен досекать старого плеера
    assert _stream_id_of(rep) and "@" in _stream_id_of(rep)


@pytest.mark.asyncio
async def test_repeat_pause_does_not_lose_position(monkeypatch, tmp_path):
    """Вторая «пауза» не перезаписывает позицию (трек уже стоит, прогресс 0)."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    await r.handle("пауза", "rid", 0)
    await r._pause_settled()
    state["progress"] = 0.0      # после stop плеер стоит на нуле
    await r.handle("ещё пауза", "rid", 0)
    await r.handle("пауза", "rid", 0)
    await r._pause_settled()
    assert s.paused_at == 42.5


@pytest.mark.asyncio
async def test_stop_also_remembers(monkeypatch, tmp_path):
    """«Стоп»/«выключи» — та же память места: «продолжай» вернёт на секунду."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    await r.handle("стоп", "rid", 0)
    await r._pause_settled()
    assert cmds == ["stop"] and s.paused_at == 42.5
    rep = await r.handle("продолжай", "rid2", 0)
    assert _offset_of(rep) == 42500


@pytest.mark.asyncio
async def test_get_next_during_pause_is_ignored(monkeypatch, tmp_path):
    """on pause glagol stop makes the column ask for the next track — don't advance."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    await r.handle("пауза", "rid", 0)
    gen = s.gen
    rep = await r.action(M.GET_NEXT, "rid")
    assert rep.directives == []
    assert s.pos == 0 and s.gen == gen


@pytest.mark.asyncio
async def test_on_failed_during_pause_is_ignored(monkeypatch, tmp_path):
    """Пауза обрывает поток — это не сбой: авторетрай не должен поднимать звук."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path, kind="radio")
    await r.handle("пауза", "rid", 0)
    assert s.paused is True
    rep = await r.action(M.CB_PREFIX + "on_failed", "rid2")
    assert rep.directives == []
    assert s.fails == 0


@pytest.mark.asyncio
async def test_radio_resume_goes_live_with_fresh_key(monkeypatch, tmp_path):
    """После паузы радио включается заново вживую: offset 0 и новый ключ."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path, kind="radio")
    old_key = s.key
    await r.handle("пауза", "rid", 0)
    rep = await r.handle("продолжай", "rid2", 0)
    assert _offset_of(rep) == 0
    assert _stream_id_of(rep) != old_key
    assert s.fails == 0


@pytest.mark.asyncio
async def test_resume_without_our_session_goes_to_cloud(monkeypatch, tmp_path):
    """Чужую музыку (стоковая Алиса) не трогаем: «продолжай» уходит облаку.

    В релее фраза с classify=False просто не доходит до handle — её отвечает
    облако. Если handle всё же позовут (local-режим), максимум — честная
    реплика «нечего продолжать», но никакого audio_play.
    """
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    r = M.Router(LiteConfig(llm_enabled=False))
    assert r.classify("продолжай") is False
    rep = await r.handle("продолжай", "rid", 0)
    assert rep is None or not rep.directives


@pytest.mark.asyncio
async def test_on_stopped_from_button_becomes_pause(monkeypatch, tmp_path):
    """Кнопка ▶‖: on_stopped без GET_NEXT — через STOP_CHECK_S это пауза."""
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    state["playing"] = False
    state["progress"] = 17.0
    s.start_ts = _t.monotonic()
    await r.action(M.CB_PREFIX + "on_stopped", "rid")
    await asyncio.sleep(M.STOP_CHECK_S + 0.3)
    assert s.paused is True and s.paused_at == 17.0
    rep = await r.handle("продолжай", "rid2", 0)
    assert _offset_of(rep) == 17000


@pytest.mark.asyncio
async def test_on_stopped_before_get_next_is_not_pause(monkeypatch, tmp_path):
    """Естественный конец трека: on_stopped, затем GET_NEXT — паузы нет."""
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    state["playing"] = False
    s.start_ts = _t.monotonic()
    await r.action(M.CB_PREFIX + "on_stopped", "rid")
    await asyncio.sleep(0.05)
    await r.action(M.GET_NEXT, "rid")   # трек кончился — колонка просит следующий
    await asyncio.sleep(M.STOP_CHECK_S + 0.3)
    assert s.paused is False


@pytest.mark.asyncio
async def test_on_stopped_right_after_audio_play_is_not_pause(monkeypatch, tmp_path):
    """Фраза ответа глушит поток, трек стартует после TTS — не пауза (гонка 19.11)."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    # окно ожидания старта коротко: тест не спит 10 с
    monkeypatch.setattr(M, "START_GRACE_S", 0.5)
    monkeypatch.setattr(M, "STOP_CHECK_S", 0.05)
    s.speech_ts = __import__("time").monotonic()   # только что отвечали ФРАЗОЙ
    state["playing"] = False                       # фраза ещё звучит, трек не начался

    async def late_start():
        await asyncio.sleep(0.2)
        state["playing"] = True                    # трек заиграл после фразы
    task = asyncio.create_task(late_start())
    await r.action(M.CB_PREFIX + "on_stopped", "rid")
    await asyncio.sleep(0.9)
    await task
    assert s.paused is False, "старт после фразы принят за паузу"


@pytest.mark.asyncio
async def test_button_pause_is_detected_quickly_without_speech(monkeypatch, tmp_path):
    """Кнопка ▶‖: остановка без свежей фразы — пауза за STOP_CHECK_S, без
    десятисекундного ожидания (иначе быстрый «продолжай» отвечал «Уже играет»)."""
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    monkeypatch.setattr(M, "STOP_CHECK_S", 0.05)
    s.issued_ts = _t.monotonic()       # audio_play выдали только что…
    s.speech_ts = 0.0                  # …но ФРАЗЫ не было — ждать нечего
    state["playing"] = False
    state["progress"] = 55.5
    await r.action(M.CB_PREFIX + "on_stopped", "rid")
    await asyncio.sleep(0.4)
    assert s.paused is True, "пауза с кнопки не отметилась"
    assert s.paused_at == 55.5
    rep = await r.handle("продолжай", "rid2", 0)
    assert _offset_of(rep) == 55500


@pytest.mark.asyncio
async def test_book_resume_keeps_second_offset(monkeypatch, tmp_path):
    """Секунда внутри главы книги сохраняется и отдаётся при «продолжай книгу»."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite import streams_lite as S
    r = M.Router(LiteConfig())
    dev = r.cfg.device_id
    s = S.Session(kind="book", title="Книга", identifier="id1",
                  queue=[S.Track(url="http://x/1.mp3", title="1")], started=0)
    r.streams.sessions[dev] = s
    r.streams._remember(dev, s)
    r.streams.remember_offset(dev, 123.4)

    async def fake_tracks(ident, title):
        return [S.Track(url="http://x/1.mp3", title="1")]
    monkeypatch.setattr(S, "book_tracks", fake_tracks)
    r.streams.sessions.clear()          # «lite перезапустился»
    rep = await r.handle("продолжай книгу", "rid", 0)
    assert _offset_of(rep) == 123400, _offset_of(rep)
    assert "две минуты три секунды" in rep.speech, rep.speech


# ------------------------------------------------ скорость / повтор / номер (19.11)
def _speed_of(rep):
    for d in rep.directives:
        if d.get("name") == "audio_play":
            return d["payload"]["playback_speed"]
    return None


def test_speed_parser_phrases():
    from column_lite import intents
    cases = {
        "быстрее": "faster", "побыстрее пожалуйста": "faster",
        "медленнее": "slower", "замедли": "slower",
        "скорость 1.5": 1.5, "поставь скорость 2": 2.0, "скорость полтора": 1.5,
        "включи скорость 0,75": 0.75,
        "в два раза быстрее": 2.0, "в два раза медленнее": 0.5,
        "нормальная скорость": 0.0, "сбрось скорость": 0.0,
    }
    for phrase, want in cases.items():
        got = intents.match_speed(phrase)
        assert got == want, (phrase, got, want)
    for phrase in ("включи кино", "поставь таймер на 5 минут", "громче"):
        assert intents.match_speed(phrase) is None, phrase


@pytest.mark.asyncio
async def test_speed_sets_playback_speed_and_keeps_position(monkeypatch, tmp_path):
    """«Скорость 1.5» -> новый audio_play с playback_speed=1.5 с той же секунды."""
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    s.start_offset = 10.0
    s.start_ts = _t.monotonic()          # играем с 10-й секунды

    # колонка не ответила на ping — позиция оценивается по часам
    async def no_state(cfg):
        return None
    monkeypatch.setattr(M, "glagol_state", no_state)
    rep = await r.handle("скорость 1.5", "rid", 0)
    assert _speed_of(rep) == 1.5
    off = _offset_of(rep) / 1000.0
    assert 9.5 <= off <= 12.0, off          # не с начала, примерно та же секунда

    # до ответа на команду glagol опрашивать нельзя (он занят отправителем
    # команды — 3 с таймаута на каждую фразу, лог 20.11): позиция по часам
    async def live_state(cfg):
        raise AssertionError("glagol опрошен до ответа на команду")
    monkeypatch.setattr(M, "glagol_state", live_state)
    rep = await r.handle("скорость 1.5", "rid", 0)
    assert 9.5 <= _offset_of(rep) / 1000.0 <= 12.0, _offset_of(rep)
    assert s.speed == 1.5
    # «быстрее» — шаг +0.25, новое значение едет в следующий audio_play
    rep = await r.handle("быстрее", "rid", 0)
    assert _speed_of(rep) == 1.75
    # «нормальная скорость» — сброс
    rep = await r.handle("нормальная скорость", "rid", 0)
    assert _speed_of(rep) == 1.0


@pytest.mark.asyncio
async def test_speed_clamped(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    rep = await r.handle("скорость 10", "rid", 0)
    assert _speed_of(rep) == M.Router.SPEED_MAX
    rep = await r.handle("скорость 0.1", "rid", 0)
    assert _speed_of(rep) == M.Router.SPEED_MIN


@pytest.mark.asyncio
async def test_speed_not_for_radio(monkeypatch, tmp_path):
    """Скорость радио не меняется, но эфир возвращается: фраза глушит поток."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path, kind="radio")
    rep = await r.handle("быстрее", "rid", 0)
    assert rep is not None and "радио" in rep.speech.lower()
    assert s.speed == 1.0
    # фраза поверх эфира глушит его — в том же ответе эфир запускается заново
    assert any(x.get("name") == "audio_play" for x in rep.directives)


@pytest.mark.asyncio
async def test_loop_replays_track_on_get_next(monkeypatch, tmp_path):
    """Повтор: трек кончился (GET_NEXT) — играем его же заново, очередь стоит."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    rep = await r.handle("зациклить этот трек", "rid", 0)
    assert s.loop is True and "повтор" in rep.speech.lower()
    s.started = 0
    rep = await r.action(M.GET_NEXT, "rid")
    d = [x for x in rep.directives if x.get("name") == "audio_play"]
    assert d and d[0]["payload"]["stream"]["id"].startswith("Кино:0")
    assert s.pos == 0                       # очередь не сдвинулась
    assert _offset_of(rep) == 0             # с начала
    rep = await r.handle("выключи повтор", "rid", 0)
    assert s.loop is False
    rep = await r.action(M.GET_NEXT, "rid")
    assert s.pos == 1                       # без повтора — следующий


@pytest.mark.asyncio
async def test_loop_not_for_radio(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path, kind="radio")
    rep = await r.handle("зацикли", "rid", 0)
    assert rep is not None and s.loop is False
    # отказ не обрывает эфир: после фразы радио запускается заново
    assert any(x.get("name") == "audio_play" for x in rep.directives)


@pytest.mark.asyncio
async def test_replay_restarts_current_track(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    s.pos = s.started = 1
    rep = await r.handle("повтори", "rid", 0)
    assert _offset_of(rep) == 0
    assert s.pos == 1


def test_jump_parser():
    from column_lite import intents
    cases = {
        "включи третий трек": 3, "поставь пятую часть": 5, "включи трек номер 7": 7,
        "переключи на 12": 12, "включи второй": 2,
        # голосом глагол теряется — ASR отдал голое «третий трек» (лог 20.11)
        "третий трек": 3, "вторую часть": 2, "пятый трек": 5, "трек 7": 7,
    }
    for phrase, want in cases.items():
        got = intents.match_jump(phrase)
        assert got == want, (phrase, got, want)
    for phrase in ("включи кино", "включи группу кино", "включи двадцать два",
                   "поставь будильник на семь", "включи радио маяк",
                   "третий", "следующий трек", "предыдущий трек"):
        assert intents.match_jump(phrase) is None, phrase


@pytest.mark.asyncio
async def test_bare_third_track_switches_queue_not_llm(monkeypatch, tmp_path):
    """«третий трек» без глагола — переключение очереди, а не вопрос к LLM."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)

    class Boom:
        async def ask(self, text, timeout=20.0):
            raise AssertionError("фраза ушла в LLM")
    r.llm = Boom()
    assert r.classify("третий трек") is True
    rep = await r.handle("третий трек", "rid", 0)
    assert s.started == 2
    assert _offset_of(rep) == 0


@pytest.mark.asyncio
async def test_llm_answer_does_not_kill_music(monkeypatch, tmp_path):
    """Ответ LLM поверх играющей музыки возвращает трек с той же секунды.

    Живьём 20.11: «третий трек» ушёл в LLM, ответ «Третий трек
    воспроизводится.» озвучился и заглушил поток.
    """
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)

    class Stub:
        async def ask(self, text, timeout=20.0):
            return "Третий трек воспроизводится."
    r.llm = Stub()
    s.start_offset = 20.0
    s.start_ts = _t.monotonic()          # играем с 20-й секунды
    rep = await r.handle("расскажи анекдот про кота", "rid", 0)
    assert rep.speech == "Третий трек воспроизводится."
    d = [x for x in rep.directives if x.get("name") == "audio_play"]
    assert d, "ответ без audio_play заглушил бы музыку"
    assert 19.5 <= _offset_of(rep) / 1000.0 <= 22.0


@pytest.mark.asyncio
async def test_jump_plays_nth_track(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    rep = await r.handle("включи третий трек", "rid", 0)
    assert s.pos == 2 and s.started == 2
    assert _offset_of(rep) == 0
    assert "3" in rep.speech
    # за пределами очереди — отказ, но фраза не должна глушить играющую музыку:
    # в ответе едет audio_play того же трека (с той же секунды)
    rep = await r.handle("включи десятый трек", "rid", 0)
    assert "3" in rep.speech
    d = [x for x in rep.directives if x.get("name") == "audio_play"]
    assert d and d[0]["payload"]["stream"]["id"].startswith("Кино:2@")
    assert s.pos == 2 and s.started == 2


def test_new_commands_do_not_steal_foreign_phrases():
    """Без нашей сессии «быстрее/повтори/третий трек» уходят облаку."""
    from column_lite.main import Router
    r = Router(LiteConfig(llm_enabled=False))
    for phrase in ("быстрее", "повтори", "включи третий трек", "зацикли",
                   "перемотай на минуту вперёд"):
        assert r.classify(phrase) is False, phrase


# ------------------------------------------------ провайдеры LLM (20.11)
def test_llm_providers_keys_from_config(monkeypatch):
    """Провайдер без ключа пропускается; ключ из lite.toml [llm] включается.
    Геоблокированные с сети колонки провайдеры (groq/cerebras/gemini/openrouter)
    в цепочку не добавлялись вовсе."""
    from column_lite.llm_local import LocalLLM
    for env in ("ALICE_MISTRAL_API_KEY", "ALICE_ZAI_API_KEY",
                "ALICE_SAMBANOVA_API_KEY", "ALICE_ORCA_API_KEY"):
        monkeypatch.delenv(env, raising=False)
    llm = LocalLLM(LiteConfig())
    names = {p["name"] for p in llm._providers()}
    assert {"llm7", "pollinations"} <= names
    assert "mistral" not in names and "zai" not in names
    assert not {"groq", "cerebras", "gemini", "openrouter"} & names
    llm2 = LocalLLM(LiteConfig(llm_mistral_key="m-test", llm_zai_key="z-test"))
    provs = {p["name"]: p["key"] for p in llm2._providers()}
    assert provs.get("mistral") == "m-test"
    assert provs.get("zai") == "z-test"


def test_lite_toml_llm_section_reads(tmp_path):
    """Раньше секция [llm] lite.toml не читалась вовсе — ключи игнорировались."""
    from column_lite.config import load_config
    p = tmp_path / "lite.toml"
    p.write_text('[device]\ncity = "Омск"\n[llm]\nmistral_key = "abc"\n'
                 'zai_key = "def"\napi_key = "one"\n', encoding="utf-8")
    cfg = load_config(p)
    assert cfg.llm_mistral_key == "abc"
    assert cfg.llm_zai_key == "def"
    assert cfg.llm_api_key == "one"
    assert cfg.city == "Омск"


# ------------------------------------------------ аудит 20.11: регрессии
def test_alarm_without_the_word_alarm():
    """«поставь на шесть утра» без слова «будильник» — будильник, не NameError.

    Функция _looks_like_alarm была потеряна при правке (остался мёртвый
    хвост), и ровно этот случай падал исключением: колонка не получала
    ответа вовсе (аудит 20.11).
    """
    from column_lite import intents
    cmd = intents.match_timer("поставь на шесть утра")
    assert cmd is not None and cmd.action == "add_alarm"
    assert (cmd.hour, cmd.minute) == (6, 0)
    cmd = intents.match_timer("заведи на семь тридцать")
    assert cmd is not None and cmd.action == "add_alarm"
    assert (cmd.hour, cmd.minute) == (7, 30)
    # таймеры при этом не путаются с будильниками
    cmd = intents.match_timer("поставь на десять минут")
    assert cmd is not None and cmd.action == "add_timer" and cmd.seconds == 600


def test_check_jwt_rejects_expired():
    """Подпись без проверки exp делала токен вечным (аудит 20.11)."""
    import time as _t
    from column_lite.tokens import check_jwt, jwt_hs256
    key = b"k" * 32
    good = jwt_hs256({"exp": int(_t.time()) + 60}, key)
    assert check_jwt(good, key)
    expired = jwt_hs256({"exp": int(_t.time()) - 120}, key)
    assert not check_jwt(expired, key)
    assert not check_jwt(good, b"x" * 32)      # чужая подпись
    assert not check_jwt("мусор", key)


def test_relay_prunes_expired_suppressed():
    """Чистка _suppressed работала только при реконнекте — теперь и при командах."""
    import time as _t
    from column_lite.relay import Relay
    r = Relay(FakeWS(), LiteConfig(), lambda t: False, lambda *a: None)
    r._suppressed.add("old")
    r._suppressed_ttl["old"] = _t.monotonic() - 1
    r._suppressed.add("new")
    r._suppressed_ttl["new"] = _t.monotonic() + 60
    r._prune_suppressed()
    assert "old" not in r._suppressed and "old" not in r._suppressed_ttl
    assert "new" in r._suppressed and "new" in r._suppressed_ttl


@pytest.mark.asyncio
async def test_http_body_size_limit():
    """Гигантский Content-Length не должен вызывать огромную аллокацию (аудит)."""
    import asyncio as _a
    from column_lite import httpserver as H
    rd = _a.StreamReader()
    rd.feed_data(b"POST /x HTTP/1.1\r\nContent-Length: "
                 + str(H.MAX_BODY + 1).encode() + b"\r\n\r\n")
    rd.feed_eof()
    assert await H.App()._read_request(rd) is None
    # нормальное тело проходит
    rd2 = _a.StreamReader()
    rd2.feed_data(b"POST /_mgmt/say HTTP/1.1\r\nContent-Length: 5\r\n\r\n{\"a\"}")
    rd2.feed_eof()
    req = await H.App()._read_request(rd2)
    assert req is not None and req.body == b'{"a"}'


class _FakeWriter:
    def __init__(self):
        self.data = bytearray()

    def write(self, b):
        self.data += b

    async def drain(self):
        pass


@pytest.mark.asyncio
async def test_ws_rejects_oversized_frame(monkeypatch):
    """Заявленная длина фрейма больше MAX_FRAME — отказ с close 1009 (аудит 20.11)."""
    from column_lite import httpserver as H
    monkeypatch.setattr(H, "MAX_FRAME", 64)
    rd = asyncio.StreamReader()
    rd.feed_data(bytes([0x81, 126]) + (65).to_bytes(2, "big"))
    rd.feed_eof()
    w = _FakeWriter()
    ws = H.WebSocket(rd, w)
    assert await ws.recv() is None
    assert ws.closed
    assert w.data[:4] == bytes([0x88, 0x02]) + (1009).to_bytes(2, "big")


@pytest.mark.asyncio
async def test_ws_rejects_oversized_fragmented_message(monkeypatch):
    """Сумма фрагментов тоже лимитирована: 40+40 при MAX_FRAME=64 — отказ."""
    from column_lite import httpserver as H
    monkeypatch.setattr(H, "MAX_FRAME", 64)
    rd = asyncio.StreamReader()
    rd.feed_data(bytes([0x01, 40]) + b"a" * 40)   # text, FIN=0
    rd.feed_data(bytes([0x00, 40]) + b"b" * 40)   # continuation, FIN=0
    rd.feed_eof()
    ws = H.WebSocket(rd, _FakeWriter())
    assert await ws.recv() is None
    assert ws.closed


class _FakeConnWriter(_FakeWriter):
    """StreamWriter-заглушка с адресами соединения для проверки req.local."""

    def __init__(self, peer, sock):
        super().__init__()
        self._peer, self._sock = peer, sock

    def get_extra_info(self, name, default=None):
        if name == "peername":
            return self._peer
        if name == "sockname":
            return self._sock
        return default

    def close(self):
        pass


@pytest.mark.asyncio
async def test_service_guard_sees_lan_peer_as_remote():
    """Служебные ручки (/_radio, /get_sync_info...) пускают только самоустройство.

    Аудит 19.11: из LAN /_radio возвращал админку роутера, /get_sync_info — ключ
    glagol. req.local должен быть False для чужого IP и True для loopback и для
    соединения с собственного адреса колонки.
    """
    from column_lite import httpserver as H

    async def handler(req):
        return {"local": req.local, "peer": req.peer}

    async def call(peer, sock) -> bytes:
        app = H.App()
        app.route("GET", "/priv")(handler)
        rd = asyncio.StreamReader()
        rd.feed_data(b"GET /priv HTTP/1.1\r\n\r\n")
        rd.feed_eof()
        w = _FakeConnWriter(peer, sock)
        await app._handle(rd, w)
        return bytes(w.data)

    lan = await call(("192.0.2.20", 41000), ("192.0.2.10", 80))
    assert b'"local": false' in lan and b'"peer": "192.0.2.20"' in lan
    lo = await call(("127.0.0.1", 41001), ("127.0.0.1", 80))
    assert b'"local": true' in lo
    own = await call(("192.0.2.10", 41002), ("192.0.2.10", 80))
    assert b'"local": true' in own


def test_peer_is_local_variants():
    from column_lite.httpserver import _peer_is_local
    assert _peer_is_local(("127.0.0.1", 1), ("127.0.0.1", 80))
    assert _peer_is_local(("127.0.0.5", 1), ("0.0.0.0", 80))
    assert _peer_is_local(("::1", 1, 0, 0), ("::1", 80, 0, 0))
    assert _peer_is_local(("192.0.2.10", 1), ("192.0.2.10", 80))
    assert not _peer_is_local(("192.0.2.20", 1), ("192.0.2.10", 80))
    assert not _peer_is_local(None, ("192.0.2.10", 80))


# ------------------------------------------------ настройка Wi-Fi по Bluetooth (20.11)
def test_bt_request_parsing():
    from column_lite import bt_provision as bt
    assert bt.parse_request(b'{"ssid":"Dom","password":"pass1234"}\n') == ("Dom", "pass1234")
    assert bt.parse_request('{"ssid": "Кафе", "password": ""}') == ("Кафе", "")
    assert bt.parse_request(b"garbage") is None
    assert bt.parse_request(b'{"password":"x"}') is None
    assert bt.parse_request(b"[]") is None
    out = bt.reply(True, "подключено", "192.0.2.20")
    import json as _j
    assert _j.loads(out.decode()) == {"ok": True, "message": "подключено",
                                      "ip": "192.0.2.20"}


# ------------------------------------------------ API пульта (20.11)
def test_mgmt_parse_status_and_scan():
    from column_lite import mgmt
    st = mgmt.parse_status("bssid=AA:BB\nssid=Keenetic\nwpa_state=COMPLETED\n")
    assert st["ssid"] == "Keenetic" and st["wpa_state"] == "COMPLETED"
    text = ("bssid / frequency / signal level / flags / ssid\n"
            "aa\t2437\t-70\t[WPA2-PSK][ESS]\tNet1\n"
            "bb\t2437\t-50\t[WPA2-PSK][ESS]\tNet2\n"
            "cc\t5200\t-60\t[ESS]\tNet2\n"
            "dd\t2437\t-80\t[ESS]\t\n")
    nets = mgmt.parse_scan(text)
    assert [n["ssid"] for n in nets] == ["Net2", "Net1"]   # по сигналу, без дублей
    assert nets[0]["secure"] is True and nets[1]["secure"] is True


def test_mgmt_token_persists(tmp_path):
    from column_lite import mgmt
    tok = mgmt.load_or_create_token(tmp_path)
    assert len(tok) == 32
    assert mgmt.load_or_create_token(tmp_path) == tok   # не пересоздаётся
    assert (tmp_path / "mgmt_token").exists()


def test_mgmt_recent_newest_first():
    from column_lite import mgmt
    items = [{"ts": i, "text": f"c{i}"} for i in range(5)]
    got = mgmt.recent(items, 3)
    assert [x["text"] for x in got] == ["c4", "c3", "c2"]


# ------------------------------------------------ мейнстрим-отбор песен (20.11)
def test_parse_deezer_canonical():
    """Самый популярный вариант из Deezer — каноничный исполнитель и название."""
    from column_lite.streams_lite import _parse_deezer
    dz = _parse_deezer([
        {"rank": 99000, "title": "Группа крови", "artist": {"name": "Кино"}},
        {"rank": 5000, "title": "Группа крови (Cover)", "artist": {"name": "Пикник"}},
    ])
    assert dz["best_artist"] == "кино"
    assert dz["best_title"] == "группа крови"
    assert "пикник" in dz["artists"]
    assert dz["pop"]["кино группа крови"] == 99000


def test_rank_tracks_prefers_mainstream_original():
    """Оригинал мейнстримного исполнителя впереди; минусы/караоке/каверы — в хвост."""
    from column_lite.streams_lite import Track, _parse_deezer, rank_tracks
    dz = _parse_deezer([
        {"rank": 98000, "title": "Группа крови", "artist": {"name": "Кино"}},
        {"rank": 3000, "title": "Группа крови", "artist": {"name": "Пикник"}},
    ])
    tracks = [
        Track(url="http://x/1.mp3", title="Группа крови (минус)", artist="Караоке"),
        Track(url="http://x/2.mp3", title="Группа крови (cover)", artist="Пикник"),
        Track(url="http://x/3.mp3", title="Группа крови", artist="Неизвестный"),
        Track(url="http://x/4.mp3", title="Группа крови", artist="Кино"),
    ]
    got = rank_tracks("кино группа крови", tracks, dz)
    assert got[0].artist == "Кино"
    assert got[0].url == "http://x/4.mp3"
    # шлак отфильтрован целиком (минус/караоке/кавер получают отрицательный счёт)
    assert all("минус" not in t.title.lower() and "cover" not in t.title.lower()
               for t in got)
    assert "Караоке" not in [t.artist for t in got]
    # версия без каноничного исполнителя — после оригинала
    assert got[-1].artist == "Неизвестный"


def test_junk_markers():
    from column_lite.streams_lite import _JUNK
    for s in ("группа крови минус", "караоке хит", "song (cover)",
              "nightcore mix", "slowed ремикс", "трек нейросеть"):
        assert _JUNK.search(s) or "ремикс" in s, s
    for s in ("группа крови", "кино и виктор цой группа крови"):
        assert not _JUNK.search(s), s


def test_rank_tracks_without_deezer_still_words():
    """Deezer не ответил — работает прежняя логика по словам запроса."""
    from column_lite.streams_lite import Track, rank_tracks
    tracks = [Track(url="http://x/1.mp3", title="Другая песня", artist="А"),
              Track(url="http://x/2.mp3", title="Группа крови", artist="Кино")]
    got = rank_tracks("группа крови", tracks, None)
    assert got[0].title == "Группа крови"


# ------------------------------------------------ перемотка (20.11)
def test_seek_parser():
    from column_lite import intents
    cases = {
        "перемотай на минуту вперёд": ("rel", 60.0),
        "отмотай тридцать секунд": ("rel", -30.0),
        "назад на полторы минуты": ("rel", -90.0),
        "промотай на две минуты": ("rel", 120.0),
        "перемотай на 2:30": ("abs", 150.0),
        "в начало трека": ("abs", 0.0),
        "с начала песни": ("abs", 0.0),
        "на десять минут назад": ("rel", -600.0),
    }
    for phrase, want in cases.items():
        got = intents.match_seek(phrase)
        assert got == want, (phrase, got, want)
    for phrase in ("поставь таймер на 10 минут", "включи третий трек",
                   "включи кино", "дальше", "назад"):
        assert intents.match_seek(phrase) is None, phrase


@pytest.mark.asyncio
async def test_seek_forward_keeps_playing_from_new_position(monkeypatch, tmp_path):
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    s.start_offset = 100.0
    s.start_ts = _t.monotonic()          # играем со 100-й секунды
    rep = await r.handle("отмотай тридцать секунд", "rid", 0)
    off = _offset_of(rep) / 1000.0
    assert 69.0 <= off <= 71.5, off      # назад на 30 с от ~100 с
    assert s.start_offset == off
    rep = await r.handle("перемотай на 2:30", "rid", 0)
    assert _offset_of(rep) == 150000     # абсолютная позиция 2:30


@pytest.mark.asyncio
async def test_seek_on_pause_moves_saved_position_only(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    await r.handle("пауза", "rid", 0)
    await r._pause_settled()
    rep = await r.handle("отмотай двадцать секунд", "rid", 0)
    assert rep.directives == []          # звук стоит — новые директивы не нужны
    assert s.paused_at == 22.5           # 42.5 - 20
    rep = await r.handle("продолжай", "rid", 0)
    assert _offset_of(rep) == 22500


@pytest.mark.asyncio
async def test_seek_not_for_radio(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path, kind="radio")
    rep = await r.handle("перемотай на минуту назад", "rid", 0)
    assert "эфир" in rep.speech.lower()
    assert any(x.get("name") == "audio_play" for x in rep.directives)  # эфир жив


@pytest.mark.asyncio
async def test_seek_clamped_by_duration(monkeypatch, tmp_path):
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)  # трек 200 с
    s.start_offset = 10.0
    s.start_ts = _t.monotonic()
    rep = await r.handle("перемотай вперёд на час", "rid", 0)
    assert 198_000 <= _offset_of(rep) <= 199_000


# ------------------------------------------------ числа словами (20.11)
def test_num_words():
    from column_lite.tts import num_words, duration_words
    assert num_words(0) == "ноль"
    assert num_words(7) == "семь"
    assert num_words(21) == "двадцать один"
    assert num_words(123) == "сто двадцать три"
    assert num_words(1000) == "одна тысяча"
    assert num_words(2001) == "две тысячи один"
    assert num_words(21000) == "двадцать одна тысяча"
    assert num_words(1000000) == "один миллион"
    assert duration_words(123) == "две минуты три секунды"
    assert duration_words(47) == "сорок семь секунд"


def test_numbers_to_words_in_tts_text():
    from column_lite.tts import numbers_to_words
    assert numbers_to_words("Твой код 123") == "Твой код сто двадцать три"
    assert numbers_to_words("часть 12, трек 3") == "часть двенадцать, трек три"
    # времена, дроби и коды не трогаем
    assert numbers_to_words("будильник на 16:06") == "будильник на 16:06"
    assert numbers_to_words("скорость 1.5") == "скорость 1.5"
    assert numbers_to_words("код 007") == "код 007"


# ------------------------------------------------ подсветка (20.11)
def test_light_parser():
    from column_lite import intents
    for p in ("посвети", "посвети мне", "включи подсветку", "включи свет",
              "светись", "зажгись", "подсвети", "посвети красным",
              "включи подсветку зелёным"):
        cmd = intents.match_light(p)
        assert cmd is not None and cmd.on is True, p
        assert 5 <= cmd.seconds <= 3600, p
    for p in ("выключи свет", "погаси", "не свети", "выключи подсветку"):
        cmd = intents.match_light(p)
        assert cmd is not None and cmd.on is False, p
    for p in ("включи кино", "какая погода", "включи телевизор"):
        assert intents.match_light(p) is None, p
    # цвет и длительность
    assert intents.match_light("посвети красным").color == "красн"
    assert intents.match_light("посвети синим").color == "син"
    assert intents.match_light("посвети синим на полминуты").seconds == 30
    assert intents.match_light("посвети жёлтым на две минуты").seconds == 120
    assert intents.match_light("посвети").color == ""
    assert intents.match_light("посвети салатовым").color == "салатов"
    # живые голосовые формы 20.11: ASR «посвятим», приветствия, «цвет/свет»
    assert intents.match_light("Алиса привет посвятим красным").color == "красн"
    assert intents.match_light("Включи красный цвет").color == "красн"
    assert intents.match_light("включи синий свет").color == "син"
    assert intents.match_light("сделай свет зелёным").color == "зелен"
    assert intents.match_light("Алиса, выключи свет").on is False
    # музыка со «светом» в названии — не подсветка
    assert intents.match_light("включи песню свет") is None
    assert intents.match_light("включи трек белый цвет") is None
    # цвет без глагола (живой случай 20.11: «зелёный цвет» ушёл в LLM)
    assert intents.match_light("зелёный цвет").color == "зелен"
    assert intents.match_light("а теперь синий").color == "син"
    assert intents.match_light("красный").color == "красн"
    assert intents.match_light("поменяй цвет на жёлтый").color == "желт"
    assert intents.match_light("красная шапочка") is None
    assert intents.match_light("расскажи про синий кит") is None


def test_wake_clears_mute_and_background_lock():
    """«Алиса» сразу после команды: глухота 7 с снимается, фоновая «речь»
    сбрасывается (лог 20.11: окно открыто, распознавание не запускалось)."""
    import time as _t
    from column_lite.voice import VoiceLoop
    v = VoiceLoop(cfg=None, device_id="x", jwt_key=b"k")
    v._mute_until = _t.monotonic() + 7
    v._speech = True
    v._speech_t0 = _t.monotonic() - 10      # «речь» тянется 10 с — это фон
    v._buf.extend(b"\x00\x01" * 100)
    v.hearing_wake()
    assert v._mute_until == 0.0
    assert v._speech is False and len(v._buf) == 0
    # свежее начало фразы («Алиса вклю…») при открытии окна не теряется
    v._speech = True
    v._speech_t0 = _t.monotonic() - 0.5
    v._buf.extend(b"\x00\x01" * 100)
    v.hearing_wake()
    assert v._speech is True and len(v._buf) == 200


def test_leds_set_color_writes_sysfs(monkeypatch):
    """Цвет пишется в RGB-каналы ядра: три зоны x три канала."""
    from column_lite import leds
    wrote = []
    monkeypatch.setattr(leds, "_write", lambda name, v: wrote.append((name, v)) or True)
    assert leds.set_color((255, 10, 0))
    assert ("micro:red:ring2", 255) in wrote
    assert ("micro:green:ring3", 10) in wrote
    assert ("micro:blue:ring1", 0) in wrote
    assert len(wrote) == 9
    wrote.clear()
    leds.off()
    assert all(v == 0 for _, v in wrote) and len(wrote) == 9


def test_leds_colors():
    from column_lite import leds
    assert leds.color("красн") == (255, 0, 0)
    assert leds.color("син") == (0, 0, 255)
    assert leds.color("") == leds.DEFAULT
    assert leds.find_color("посвети красным") == "красн"
    assert leds.find_color("включи подсветку голубым") == "голуб"
    assert leds.find_color("посвети") == ""


@pytest.mark.asyncio
async def test_light_color_feeds_leds(monkeypatch, tmp_path):
    """«Посвети красным» -> заливка красным сразу, «выключи свет» -> гасит."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    from column_lite import leds
    calls = []
    monkeypatch.setattr(leds, "available", lambda: True)
    monkeypatch.setattr(leds, "set_color", lambda rgb: calls.append(rgb) or True)
    monkeypatch.setattr(leds, "off", lambda: calls.append(("off",)))
    rep = await r.handle("посвети красным", "rid", 0)
    assert rep.speech == "" and not rep.directives and rep.listen is False
    # сначала гасим прошлое свечение, затем заливаем красный
    assert calls[0] == ("off",)
    assert (255, 0, 0) in calls, "цвет не залился"
    r._light_stop()
    await asyncio.sleep(0.05)          # фоновая задача гасит в finally
    assert ("off",) in calls[1:]


@pytest.mark.asyncio
async def test_light_command_sets_should_listen(monkeypatch, tmp_path):
    """«Посвети» -> ответ без речи с should_listen (только он зажигает кольцо)."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite.config import LiteConfig
    from column_lite.relay import Relay
    router = M.Router(LiteConfig(llm_enabled=False))
    rep = await router.handle("посвети", "rid", 0)
    assert rep.listen is True and rep.speech == "" and not rep.directives
    off = await router.handle("выключи свет", "rid", 0)
    assert off.listen is False and off.speech == ""

    # полный путь: релей собирает VinsResponse с should_listen
    ws = FakeWS()
    relay = Relay(ws, LiteConfig(), router.classify, router.handle)
    relay.up = FakeUpstream()
    relay._proto_mode = False
    msg = {"event": {"header": {"namespace": "Vins", "name": "TextInput", "messageId": "m"},
                     "payload": {"header": {"request_id": "rid", "sequence_number": 1},
                                 "request": {"event": {"type": "text_input", "text": "посвети"}}}}}
    await relay._handle_local_text(msg, msg["event"]["header"],
                                   msg["event"]["payload"]["header"])
    vs = [x for x in ws.sent if isinstance(x, dict)
          and (x.get("directive") or {}).get("header", {}).get("name") == "VinsResponse"]
    assert vs, "ответ не ушёл"
    vr = vs[-1]["directive"]["payload"]["voice_response"]
    assert vr["should_listen"] is True
    assert vr["has_voice_response"] is False     # молчим: свечение — сам ответ


# ------------------------------------------------ погода / курс валют (20.11)
def test_weather_parser():
    from column_lite import intents
    cases = {
        "какая погода": ("now", ""), "погода": ("now", ""),
        "сколько градусов": ("now", ""),
        "какая температура на улице": ("now", ""),
        "погода на завтра": ("tomorrow", ""),
        "какая завтра погода": ("tomorrow", ""),
        "будет ли завтра дождь": ("tomorrow", ""),
        "будет ли дождь": ("rain", ""), "нужен ли зонт": ("rain", ""),
        # города: падежи сохраняем как услышали, разбирает weather_lite
        "какая погода в москве": ("now", "москве"),
        "погода в москве на завтра": ("tomorrow", "москве"),
        "какая в питере погода": ("now", "питере"),
        "погода в нижнем новгороде": ("now", "нижнем новгороде"),
        "погода в городе сочи": ("now", "сочи"),
    }
    for phrase, want in cases.items():
        assert intents.match_weather(phrase) == want, phrase
    for phrase in ("включи песню погода", "поставь погоду в плейлист",
                   "почему погода меняется", "включи кино"):
        assert intents.match_weather(phrase) is None, phrase


def test_city_candidates():
    """Падежи -> именительный: словарь и эвристика (геокодер падежи не знает)."""
    from column_lite.weather_lite import _candidates

    def cands(name):
        return [c.lower() for c in _candidates(name)]

    assert cands("москве")[0] == "москва"
    assert cands("питере")[0] == "санкт-петербург"
    assert "нижний новгород" in cands("нижнем новгороде")
    assert "тверь" in cands("твери")
    assert "ижевск" in cands("ижевске")
    assert "ростов-на-дону" in cands("ростове")


@pytest.mark.asyncio
async def test_city_weather_uses_resolved_label(monkeypatch, tmp_path):
    """«Погода в Москве» отвечает про Москву, а не про свой город."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    from column_lite import weather_lite

    async def fake_resolve(name):
        assert name == "москве"
        return (55.75, 37.62, "Москва", "Europe/Moscow")

    async def fake_fetch(lat, lon, tz="Europe/Moscow"):
        assert abs(lat - 55.75) < 0.01    # спросили именно Москву
        return _WEATHER_DATA
    monkeypatch.setattr(weather_lite, "resolve_city", fake_resolve)
    monkeypatch.setattr(weather_lite, "fetch", fake_fetch)
    rep = await r.handle("какая погода в москве", "rid", 0)
    assert "в городе Москва" in rep.speech


@pytest.mark.asyncio
async def test_unknown_city_gets_honest_answer(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    from column_lite import weather_lite

    async def none(name):
        return None
    monkeypatch.setattr(weather_lite, "resolve_city", none)
    rep = await r.handle("какая погода в нью-васюках", "rid", 0)
    assert "не нашёл город" in rep.speech.lower()
    assert any(x.get("name") == "audio_play" for x in rep.directives)  # музыка жива


def test_rates_parser():
    from column_lite import intents
    cases = {
        "какой курс доллара": "USD", "курс доллара": "USD",
        "сколько стоит доллар": "USD", "почём бакс": "USD",
        "курс евро": "EUR", "какой курс юаня": "CNY",
        "какой курс валют": "all", "курс": "all",
    }
    for phrase, want in cases.items():
        assert intents.match_rates(phrase) == want, phrase
    for phrase in ("включи песню доллар", "поставь курс", "какая погода"):
        assert intents.match_rates(phrase) is None, phrase


_WEATHER_DATA = {
    "cur": {"t": -3.2, "feels": -6.0, "code": 73, "wind": 2.2, "winddir": 225.0},
    "days": [{"date": "2026-11-20", "min": -6.4, "max": -1.2, "code": 73, "pop": 40},
             {"date": "2026-11-21", "min": -8.0, "max": -2.0, "code": 3, "pop": 10}],
}


def test_weather_text_words():
    """Погода словами: без цифр и знаков, минус/плюс проговариваются."""
    from column_lite import weather_lite
    now = weather_lite.text("now", "Москва", _WEATHER_DATA)
    assert "Сейчас в городе Москва минус три" in now
    assert "ощущается как минус шесть" in now
    assert "юго-западный" in now
    assert "два метра в секунду" in now
    assert "снег" in now
    assert not any(ch.isdigit() for ch in now), now
    tom = weather_lite.text("tomorrow", "Москва", _WEATHER_DATA)
    assert "Завтра" in tom and "минус восемь" in tom and "минус два" in tom
    rain = weather_lite.text("rain", "Москва", _WEATHER_DATA)
    assert "осадков" in rain and "сорок процентов" in rain


def test_rates_text_words():
    from column_lite import rates_lite
    t = rates_lite.text("USD", {"USD": 92.3, "_source": "cbr"})
    assert t == "Курс ЦБ: доллар — девяносто два рубля тридцать копеек."
    t = rates_lite.text("all", {"USD": 92.3, "EUR": 101.05, "CNY": 12.84,
                                "_source": "cbr"})
    assert "Доллар — девяносто два рубля тридцать копеек" in t
    assert "Евро — сто один рубль пять копеек" in t
    assert "Юань — двенадцать рублей восемьдесят четыре копейки" in t
    assert rates_lite.rub_words(1.01) == "один рубль одна копейка"
    assert rates_lite.rub_words(2.02) == "два рубля две копейки"
    assert rates_lite.rub_words(100.0) == "сто рублей"


@pytest.mark.asyncio
async def test_weather_answer_keeps_music(monkeypatch, tmp_path):
    """Погода поверх играющей музыки: ответ и аудио трека в одной пачке."""
    import time as _t
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    from column_lite import weather_lite

    async def fake_fetch(lat, lon, tz="Europe/Moscow"):
        return _WEATHER_DATA
    monkeypatch.setattr(weather_lite, "fetch", fake_fetch)
    # дефолты репозитория обезличены (city/координаты пусты) — задаём тесту свои
    r.cfg.latitude, r.cfg.longitude, r.cfg.city = 55.0, 49.0, "Тест"
    s.start_offset = 30.0
    s.start_ts = _t.monotonic()
    rep = await r.handle("какая погода", "rid", 0)
    assert "минус три" in rep.speech
    assert any(x.get("name") == "audio_play" for x in rep.directives)
    assert 29_000 <= _offset_of(rep) <= 32_000


@pytest.mark.asyncio
async def test_rates_answer_and_failure(monkeypatch, tmp_path):
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    from column_lite import rates_lite

    async def ok():
        return {"USD": 92.3, "EUR": 101.05, "CNY": 12.84}
    monkeypatch.setattr(rates_lite, "fetch", ok)
    rep = await r.handle("какой курс доллара", "rid", 0)
    assert "девяносто два рубля тридцать копеек" in rep.speech

    async def broken():
        return None
    monkeypatch.setattr(rates_lite, "fetch", broken)
    rep = await r.handle("какой курс доллара", "rid", 0)
    assert "связь" in rep.speech.lower()
    # музыка не умерла от отказа — трек в той же пачке
    assert any(x.get("name") == "audio_play" for x in rep.directives)


@pytest.mark.asyncio
async def test_late_stop_does_not_kill_newly_started_playback(monkeypatch, tmp_path):
    """«Пауза» и сразу «включи радио»: запоздавший stop не глушит новое (20.11)."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    await r.handle("пауза", "rid", 0)                  # stop уйдёт фоном, после ответа
    r.streams.radio(r.cfg.device_id, "маяк")           # за это время включили другое
    await r._pause_settled()
    assert cmds == [], "запоздавший stop заглушил бы только что включённое радио"


@pytest.mark.asyncio
async def test_stop_without_session_skipped_if_playback_started(monkeypatch, tmp_path):
    """«Стоп» без нашей сессии, затем «включи …»: фоновый stop отменяется."""
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    dev = r.cfg.device_id
    r.streams.sessions.clear()
    await r.handle("стоп", "rid", 0)
    r.streams.radio(dev, "маяк")
    await asyncio.sleep(0.05)
    assert cmds == []
    # а если ничего не включили — stop доходит
    r.streams.sessions.clear()
    await r.handle("стоп", "rid", 0)
    await asyncio.sleep(0.05)
    assert cmds == ["stop"]


@pytest.mark.asyncio
async def test_preload_does_not_reset_position_of_playing_track(monkeypatch, tmp_path):
    """GET_NEXT — предзагрузка: отсчёт позиции играющего трека не сбивается.

    Раньше advance() обнулял start_offset, и после «продолжай» с 42-й секунды
    оценка позиции по часам уезжала к началу трека (найдено 20.11).
    """
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    await r.handle("пауза", "rid", 0)
    await r.handle("продолжай", "rid", 0)          # audio_play с 42.5 с
    await r.action(M.CB_PREFIX + "on_started", "rid")
    assert s.start_offset == 42.5
    await r.action(M.GET_NEXT, "rid")              # колонка заранее просит следующий
    assert s.pos == 1 and s.started == 0
    assert s.start_offset == 42.5, "предзагрузка сбила отсчёт играющего трека"
    await r.action(M.CB_PREFIX + "on_started", "rid")   # следующий реально заиграл
    assert s.started == 1 and s.start_offset == 0.0


# ------------------------------------------------ VAD окна команды (21.11)
class _VadClock:
    """Пакеты «идут» с шагом 20 мс без реальных пауз: time.monotonic подменён."""

    def __init__(self, t: float = 1000.0):
        self.t = t

    def monotonic(self) -> float:
        return self.t

    def time(self) -> float:
        return self.t


def _vad_packet(amp: int, n: int = 320) -> bytes:
    import array as _a
    return b"OL" + b"\x00\x00" + _a.array("h", [amp] * n).tobytes()


def _vad_loop(monkeypatch, **kw):
    from column_lite import voice as V
    clock = _VadClock()
    monkeypatch.setattr(V, "time", clock)
    v = V.VoiceLoop(cfg=None, device_id="x", jwt_key=b"k", **kw)
    calls: list[tuple[int, bool]] = []

    async def fake_handle(pcm, via_window=False):
        calls.append((len(pcm), via_window))

    v._handle = fake_handle
    return v, clock, calls


def _vad_feed(v, clock, amp: int, packets: int) -> None:
    for _ in range(packets):
        v.on_packet(_vad_packet(amp))
        clock.t += 0.02


@pytest.mark.asyncio
async def test_vad_fragmented_quiet_speech_in_window(monkeypatch):
    """Тихая речь всплесками (< 0.45 с громкого суммарно) должна приниматься:
    длительность считается по отрезку, а не по сумме громких кусков.
    (Лог 21.11 18:43:22: окно команды, пик 278, «без фразы».)"""
    v, clock, calls = _vad_loop(monkeypatch)
    v.hearing_wake(8.0)
    _vad_feed(v, clock, 20, 10)               # фон
    for _ in range(4):                        # 4 всплеска по 0.1 с, паузы 0.2 с
        _vad_feed(v, clock, 95, 5)
        _vad_feed(v, clock, 20, 10)
    _vad_feed(v, clock, 15, 50)               # тишина -> фраза закрывается
    await asyncio.sleep(0)
    assert calls, "фрагментированная тихая фраза не дошла до распознавания"
    assert calls[0][1] is True, "активация была в окне — via_window обязателен"
    assert v.stats["phrases"] == 1


@pytest.mark.asyncio
async def test_vad_window_noise_not_inflated_by_background(monkeypatch):
    """В окне команды шумовой пол не учится: ровный фон 60 раньше задирал
    порог до 130+, и речь в 100 rms терялась целиком."""
    v, clock, calls = _vad_loop(monkeypatch)
    v.hearing_wake(8.0)
    _vad_feed(v, clock, 60, 100)              # 2 с «телевизор/озвучка» 60
    _vad_feed(v, clock, 100, 50)              # 1 с речь
    _vad_feed(v, clock, 15, 50)               # тишина
    await asyncio.sleep(0)
    assert calls, "речь 100 rms в окне должна приниматься при пороге 70"


@pytest.mark.asyncio
async def test_vad_window_rescue_sends_whole_window(monkeypatch):
    """Окно закрылось без фразы, но всплески были — отправляем окно целиком,
    а не молчим (страховка для самых тихих фраз)."""
    v, clock, calls = _vad_loop(monkeypatch)
    v.hearing_wake(1.0)
    _vad_feed(v, clock, 200, 3)               # один короткий всплеск 0.06 с
    _vad_feed(v, clock, 15, 60)               # тишина до конца окна (1.2 с)
    await asyncio.sleep(0)
    assert len(calls) == 1, "окно с всплесками должно уйти на распознавание"
    assert calls[0][0] > 0 and calls[0][1] is True
    assert not v._win_buf


def test_vad_window_phrase_acceptance():
    """Фраза из окна, чьё ASR закончилось после закрытия окна, не должна
    отбрасываться как «не про вызов»; болтовня вне окна — должна."""
    from column_lite.voice import VoiceLoop
    v = VoiceLoop(cfg=None, device_id="x", jwt_key=b"k")
    v._listen_until = 0.0                     # окно уже закрыто
    assert v._command_from_text("включи песню", True) == "включи песню"
    assert v._command_from_text("включи песню", False) == ""
    assert v._command_from_text("Алиса", True) == ""          # только вызов
    assert v._command_from_text("Олег, включи песню", False) == "включи песню"
    v2 = VoiceLoop(cfg=None, device_id="x", jwt_key=b"k", require_wake=False)
    assert v2._command_from_text("включи песню", False) == "включи песню"


# ------------------------------------------------ WebSocket.close (22.11)
class _CloseWriter:
    """Писатель только для close: запоминает кадры, помечает закрытие."""
    def __init__(self):
        self.buf = bytearray()
        self.closed = False

    def write(self, data):
        self.buf.extend(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_websocket_close_accepts_code():
    """await ws.close(1008) не падает и шлёт кадр close 0x88.

    Раньше close был объявлен дважды: версия без параметра перетирала
    первую, и uni_ws падал TypeError — колонке не уходил close-кадр.
    """
    from column_lite import httpserver as H
    w = _CloseWriter()
    ws = H.WebSocket(None, w)
    await ws.close(1008)
    assert ws.closed is True and w.closed is True
    assert w.buf[0] == 0x88                      # FIN + close, без маски
    assert bytes(w.buf[2:4]) == (1008).to_bytes(2, "big")


# ------------------------------------------------ lite.toml: [net] (22.11)
def test_defaults_have_no_personal_data():
    """Дефолты репозитория обезличены: серийник и город задаются при развёртывании.

    Серийник берётся из device.json (deploy_lite.sh), город/координаты — из
    lite.toml [device]. В коде публичного репозитория их быть не должно.
    """
    from column_lite.config import LiteConfig
    cfg = LiteConfig()
    assert cfg.device_id == ""
    assert cfg.city == "" and cfg.latitude == 0.0 and cfg.longitude == 0.0


def test_load_config_reads_net_section(tmp_path):
    """[net] relay = false должен выключать релей: секцию не читали — no-op."""
    from column_lite.config import load_config
    p = tmp_path / "lite.toml"
    p.write_text('[net]\nrelay = false\nupstream = "proxy.local"\n'
                 'http_port = 8080\n', "utf-8")
    cfg = load_config(p)
    assert cfg.relay_enabled is False
    assert cfg.upstream_host == "proxy.local"
    assert cfg.http_port == 8080


def test_load_config_all_sections(tmp_path):
    """Все секции lite.toml доходят до полей конфига."""
    from column_lite.config import load_config
    p = tmp_path / "lite.toml"
    p.write_text('[device]\ndevice_id = "DEV1"\nplatform = "yandexstation"\n'
                 '[server]\nhttps_port = 8443\n'
                 '[voice]\nport = 6000\n'
                 '[llm]\nmistral_key = "k1"\nmodel = "m1"\n'
                 '[net]\nrelay = false\n', "utf-8")
    cfg = load_config(p)
    assert cfg.device_id == "DEV1"
    assert cfg.platform == "yandexstation"
    assert cfg.https_port == 8443
    assert cfg.voice_port == 6000
    assert cfg.llm_mistral_key == "k1"
    assert cfg.llm_model == "m1"
    assert cfg.relay_enabled is False


def test_match_book_stop_words_after_object():
    """«включи книгу что-нибудь» — пустой запрос, «включи что-нибудь» — не книга.

    Раньше стоп-слова сверялись до откусывания объекта «книгу», и поиск шёл
    по запросу «что нибудь» (аудит 22.11).
    """
    from column_lite import intents
    assert intents.match_book("включи книгу что-нибудь") == ("play", "")
    assert intents.match_book("включи что-нибудь") is None
    assert intents.match_book("почитай что-нибудь") == ("play", "")
    assert intents.match_book("включи книгу война и мир") == ("play", "война и мир")
    # _nominative приводит форму к именительному: «маргариту» -> «маргарита»
    assert intents.match_book("почитай мастера и маргариту") == \
        ("play", "мастера и маргарита")


def test_seek_decimal_minutes_not_clock():
    """«На 2.5 минуты» — 150 с, не «2:05» = 125 с; двоеточие остаётся мм:сс."""
    from column_lite.intents import match_seek
    assert match_seek("перемотай на 2.5 минуты") == ("rel", 150.0)
    assert match_seek("отмотай на 1.5 минуты") == ("rel", -90.0)
    assert match_seek("перемотай на 2:30") == ("abs", 150.0)
    assert match_seek("перемотай на 2.5 часа") == ("rel", 9000.0)


def test_match_jump_number_word():
    """«Трек номер семь» — номер семь: работали только «седьмой» и цифра."""
    from column_lite.intents import match_jump
    assert match_jump("включи трек номер семь") == 7
    assert match_jump("трек номер один") == 1
    assert match_jump("включи седьмой трек") == 7
    assert match_jump("включи двадцать два") is None
    assert match_jump("включи трек сто") is None      # многозначные не гадаем


# ------------------------------------------------ P1: битые state-файлы (22.11)
def test_timers_load_survives_records_without_id(monkeypatch, tmp_path):
    """{"alarms":[{"foo":1}]} и таймер без id роняли Timers.__init__ KeyError."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    import json as _j
    future = 9e12   # 285 год: живой таймер по fire, но без id
    (tmp_path / "timers_state.json").write_text(_j.dumps(
        {"alarms": [{"foo": 1}, {"id": "a1", "hour": 7, "minute": 30}],
         "timers": [{"fire": future}, {"id": "t1", "fire": future, "label": "x"}],
         "мусор": True}), "utf-8")
    from column_lite.main import Timers
    t = Timers(LiteConfig())
    assert [x["id"] for x in t._timers] == ["t1"]
    assert [x["id"] for x in t._alarms] == ["a1"]
    assert t._last_timer_id == "t1" and t._last_alarm_id == "a1"


def test_streams_survives_list_books_json(monkeypatch, tmp_path):
    """books.json-список ронял «продолжай»: AttributeError list.get (аудит 22.11)."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    (tmp_path / "books.json").write_text("[1, 2, 3]", "utf-8")
    from column_lite.streams_lite import Streams
    st = Streams()
    assert st._books == {}
    assert st.has_saved_book("dev") is False
    assert st.book_active("dev") is False
    # валидный объект по-прежнему читается
    (tmp_path / "books.json").write_text(
        '{"dev": {"identifier": "id1", "title": "Книга", "active": true, '
        '"ts": 9999999999}}', "utf-8")
    st2 = Streams()
    assert st2.has_saved_book("dev") is True


@pytest.mark.asyncio
async def test_http_bad_content_length_does_not_raise():
    """Content-Length: abc и оборванное тело — закрытие, а не висение/исключение."""
    from column_lite import httpserver as H
    app = H.App()

    rd = asyncio.StreamReader()
    rd.feed_data(b"POST /x HTTP/1.1\r\nHost: t\r\nContent-Length: abc\r\n\r\n")
    rd.feed_eof()
    assert await app._read_request(rd) is None

    rd = asyncio.StreamReader()
    rd.feed_data(b"POST /x HTTP/1.1\r\nContent-Length: 10\r\n\r\nshort")
    rd.feed_eof()
    assert await app._read_request(rd) is None

    # валидный запрос по-прежнему читается
    rd = asyncio.StreamReader()
    rd.feed_data(b"POST /x HTTP/1.1\r\nContent-Length: 2\r\n\r\nok")
    rd.feed_eof()
    req = await app._read_request(rd)
    assert req is not None and req.body == b"ok"


def test_two_alarms_same_ms_get_different_ids(monkeypatch, tmp_path):
    """Два будильника в одну миллисекунду не должны получить один id."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite.intents import TimerCommand
    t = M.Timers(LiteConfig())
    monkeypatch.setattr(M.time, "time", lambda: 1700000000.0)   # одна и та же мс
    r1 = t.run(TimerCommand(action="add_alarm", hour=7, minute=0))
    r2 = t.run(TimerCommand(action="add_alarm", hour=8, minute=0))
    id1 = r1.directives[0]["payload"]["alarm_id"]
    id2 = r2.directives[0]["payload"]["alarm_id"]
    assert id1 != id2 and id1.startswith("lite-") and id2.startswith("lite-")


@pytest.mark.asyncio
async def test_music_cancels_archive_task_when_sources_hit(monkeypatch, tmp_path):
    """После удачного поиска в базах запрос к архиву отменяется, а не висит."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import streams_lite as S
    cancelled = []

    async def ia(q):
        try:
            await asyncio.sleep(30)
            return []
        finally:
            cancelled.append(True)

    async def sky(q):
        return ""

    async def dz(q):
        return {"pop": {}, "best_artist": "", "best_title": "", "artists": set()}

    async def empty(q, limit=0):
        return []

    def score(html, q, pop, limit=25):
        return [S.Track(url="http://x/1.mp3", title="Трек", artist="Кино")]

    async def alive(u):
        return True

    monkeypatch.setattr(S, "ia_search", ia)
    monkeypatch.setattr(S, "skysound_fetch", sky)
    monkeypatch.setattr(S, "deezer_resolve", dz)
    monkeypatch.setattr(S, "muzofond_search", empty)
    monkeypatch.setattr(S, "mp3xa_search", empty)
    monkeypatch.setattr(S, "muzmo_search", empty)
    monkeypatch.setattr(S, "drivemusic_search", empty)
    monkeypatch.setattr(S, "skysound_score", score)
    monkeypatch.setattr(S, "track_alive", alive)
    st = S.Streams()
    s, speech = await st.music("dev", "кино группа крови")
    assert s is not None and s.queue
    await asyncio.sleep(0)
    assert cancelled, "запрос к архиву не отменён после успешных источников"


def test_write_books_atomic(monkeypatch, tmp_path):
    """Позиции книг пишутся через tmp+rename: обрыв питания не съест файл."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import streams_lite as S
    calls = []
    orig = S.os.replace

    def spy(a, b):
        calls.append((str(a), str(b)))
        return orig(a, b)

    monkeypatch.setattr(S.os, "replace", spy)
    st = S.Streams()
    st._books["dev"] = {"identifier": "id1", "title": "Книга"}
    st._write_books()
    assert calls and calls[0][1].endswith("books.json")
    assert calls[0][0].endswith(".json.tmp")
    import json as _j
    data = _j.loads((tmp_path / "books.json").read_text("utf-8"))
    assert data["dev"]["identifier"] == "id1"
    assert not (tmp_path / "books.json.tmp").exists()


# ------------------------------------------------ P2: гигиена (22.11)
def test_plural_declension():
    from column_lite.main import _plural
    assert _plural(1, "таймер", "таймера", "таймеров") == "таймер"
    assert _plural(3, "таймер", "таймера", "таймеров") == "таймера"
    assert _plural(5, "таймер", "таймера", "таймеров") == "таймеров"
    assert _plural(11, "таймер", "таймера", "таймеров") == "таймеров"


def test_timers_cancel_five_says_timerov(monkeypatch, tmp_path):
    """«Отменил 5 таймеров», а не «5 таймера» (аудит 22.11)."""
    monkeypatch.setenv("LITE_DIR", str(tmp_path))
    from column_lite import main as M
    from column_lite.intents import TimerCommand
    t = M.Timers(LiteConfig())
    for i in range(5):
        t._timers.append({"id": f"t{i}", "fire": __import__("time").time() + 60,
                          "label": "x"})
    rep = t.run(TimerCommand(action="cancel_timer"))
    assert rep.speech == "Отменил 5 таймеров."


def test_rates_fallback_does_not_claim_central_bank():
    """Запасной open.er-api — «Курс валют», не «Курс ЦБ» (аудит 22.11)."""
    from column_lite import rates_lite as R
    assert R.text("USD", {"USD": 90.5, "_source": "er"}).startswith("Курс валют")
    assert R.text("USD", {"USD": 90.5, "_source": "cbr"}).startswith("Курс ЦБ")


@pytest.mark.asyncio
async def test_weather_city_miss_not_cached(monkeypatch):
    """Промах геокодера не кэшируется навсегда (аудит 22.11)."""
    from column_lite import weather_lite as W
    W._city_cache.clear()
    monkeypatch.setattr(W, "_candidates", lambda name: [])
    assert await W.resolve_city("абракадабра") is None
    assert "абракадабра" not in W._city_cache


@pytest.mark.asyncio
async def test_weather_dead_only_on_network_error(monkeypatch):
    """Пауза источнику — только на сетевой сбой, не на ошибку разбора."""
    from column_lite import weather_lite as W
    W._dead.clear()
    W._cities_cache.clear()

    async def parse_bug(c, lat, lon, tz):
        raise RuntimeError("битый JSON")

    async def no_wttr(c, lat, lon):
        return None
    monkeypatch.setattr(W, "_open_meteo", parse_bug)
    monkeypatch.setattr(W, "_wttr", no_wttr)
    assert await W.fetch(1.0, 2.0) is None
    assert "open-meteo" not in W._dead

    async def net_fail(c, lat, lon):
        raise W.httpx.ConnectError("нет сети")
    monkeypatch.setattr(W, "_wttr", net_fail)
    await W.fetch(1.0, 2.0)
    assert "wttr.in" in W._dead


@pytest.mark.asyncio
async def test_tts_converts_numbers_once(monkeypatch, tmp_path):
    """speak_to_file не прогоняет numbers_to_words повторно через synth."""
    from column_lite import tts as T
    calls = []
    orig = T.numbers_to_words
    monkeypatch.setattr(T, "numbers_to_words",
                        lambda s: calls.append(s) or orig(s))

    async def fake_google(text):
        return b"ID3" + b"x" * 2000
    monkeypatch.setattr(T, "synth_google", fake_google)
    p = await T.speak_to_file("12 минут", tmp_path)
    assert p and len(calls) == 1


@pytest.mark.asyncio
async def test_wifi_reuses_saved_network_and_updates_psk(monkeypatch):
    """Новый пароль сохранённой сети не должен молча игнорироваться."""
    from column_lite import mgmt
    calls = []

    async def fake(*args, timeout=12.0):
        calls.append(args)
        cmd = args[3] if len(args) > 3 else ""
        if cmd == "list_networks":
            return "network id / ssid / bssid / flags\n0\tHome\tany\t[CURRENT]"
        if cmd == "status":
            return "wpa_state=COMPLETED\nssid=Home"
        return "OK"

    async def no_sleep(_):
        return None
    monkeypatch.setattr(mgmt, "_run", fake)
    monkeypatch.setattr(mgmt.asyncio, "sleep", no_sleep)
    ok, _msg = await mgmt.wifi_connect("Home", "newpass123")
    assert ok
    assert ("wpa_cli", "-i", "wlan0", "set_network", "0", "psk",
            '"newpass123"') in calls
    assert any(c[-1] == "save_config" for c in calls)


@pytest.mark.asyncio
async def test_bare_vykluchi_stops_music_not_light(monkeypatch, tmp_path):
    """Голое «выключи» — остановка музыки, а не «выключи подсветку».

    _STOP_RE проверяется раньше match_light и в classify, и в handle, поэтому
    безобъектное «выключи» до света не доходит (аудит 22.11 — уже так).
    Тест фиксирует приоритет, чтобы правки порядка не сломали его молча.
    """
    M, r, s, cmds, state = _music_router(monkeypatch, tmp_path)
    from column_lite import leds
    led_calls = []
    monkeypatch.setattr(leds, "set_color", lambda rgb: led_calls.append(rgb) or True)
    monkeypatch.setattr(leds, "off", lambda: led_calls.append(("off",)))
    assert r.classify("выключи") is True
    rep = await r.handle("выключи", "rid", 0)
    assert rep is not None
    assert led_calls == [], "голое «выключи» не должно трогать подсветку"

