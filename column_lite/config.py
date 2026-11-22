"""Настройки quasard-lite: устройство, порты, сертификаты, пины.

Всё, что раньше было захардкожено в main.py, читается отсюда. Значения можно
переопределить файлом /data/opt/column_lite/lite.toml (если он есть) — так
конфиг на колонке правится без переустановки кода.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

LITE_DIR = Path(os.environ.get("LITE_DIR", "/data/opt/column_lite"))


@dataclass
class LiteConfig:
    # --- устройство ---
    # Серийник колонки в репозитории НЕ хранится: это идентификатор живого
    # устройства. Если в lite.toml пусто — берём id из device.json, который
    # кладёт deploy_lite.sh при развёртывании (см. load_config).
    device_id: str = ""
    platform: str = "yandexmicro"
    device_name: str = "Яндекс Станция Лайт"
    display_name: str = "Хозяин"
    # Город/координаты — личные данные: в репозитории пусто, задаются в
    # lite.toml [device] своего развёртывания (без них lite попросит город).
    latitude: float = 0.0
    longitude: float = 0.0
    city: str = ""
    timezone: str = "Europe/Moscow"
    timezone_offset_sec: int = 10800
    uid: str = ""

    # --- сеть ---
    http_port: int = 80
    https_port: int = 443
    # Куда unbound колонки отправляет домены Яндекса. 127.0.0.1 — сама колонка
    # (автономный режим). Если понадобится вернуться на ПК — тут его IP.
    pin_ip: str = "127.0.0.1"
    # Адрес uniproxy, который отдаём колонке. Хостнейм настоящий: так проверка
    # сертификата работает, а DNS-пин уводит его на нас.
    uniproxy_url: str = "wss://uniproxy.alice.yandex.net/uni.ws"
    # Релей к настоящему облаку (ASR/TTS). Выключатель — на случай отладки.
    relay_enabled: bool = True
    upstream_host: str = "uniproxy.alice.yandex.net"
    # Голосовое управление без ПК: микрофон (PCM 16 кГц с профиля «Олег»,
    # перенаправленный на себя) -> бесплатное распознавание Google -> команда.
    voice_enabled: bool = True
    voice_port: int = 5600
    # Настройка Wi-Fi по Bluetooth (передача сети с телефона в гостях).
    # Включает видимость адаптера и SPP-сервер на канале 1.
    bt_provision_enabled: bool = True
    # Фраза-активатор: True — аудио уходит на распознавание ТОЛЬКО после того,
    # как колонка сама услышала «Алису» (её локальный детектор, без сети).
    # False — распознавать любую речь (больше запросов наружу, но и без
    # необходимости в активации). Детектор колонки — тот же, что у штатной Алисы.
    voice_require_wake: bool = True
    # LLM для вопросов без ПК: Pollinations (без ключа) / Orca / SambaNova.
    # Без него lite отвечает только музыкой/радио/книгами, а на «кто такой
    # Пушкин» молчит.
    llm_enabled: bool = True
    llm_api_key: str = ""   # один ключ для Orca/SambaNova (можно не задавать)
    llm_model: str = ""     # переопределить модель провайдера
    # Ключи по провайдерам (lite.toml [llm]): провайдер включается только
    # тогда, когда ключ есть. Проверены с сети колонки (20.11): mistral,
    # z.ai, sambanova отвечают; groq/cerebras/gemini/openrouter — геоблок РФ.
    llm_orca_key: str = ""
    llm_sambanova_key: str = ""
    llm_mistral_key: str = ""
    llm_zai_key: str = ""

    # --- файлы ---
    cert_file: str = "/data/opt/column_lite/certs/server.crt"
    key_file: str = "/data/opt/column_lite/certs/server.key"
    device_json: str = "/data/opt/column_lite/device.json"

    # Домены, которые колонка резолвит на себя. OAuth/login/mobileproxy здесь
    # НАМЕРЕННО отсутствуют: колонка должна иметь возможность обновить токен у
    # настоящего Яндекса, иначе после его истечения голос умрёт навсегда.
    pin_hosts: tuple[str, ...] = field(default_factory=lambda: (
        "quasar.yandex.net", "quasar.yandex.ru",
        "uniproxy.alice.yandex.net", "uniproxy-quic.alice.yandex.net",
        "scbh.yandex.net", "updater.mobile.yandex.net",
    ))


def load_config(path: str | Path | None = None) -> LiteConfig:
    """Конфиг из TOML, если файл есть; иначе дефолты."""
    cfg = LiteConfig()
    p = Path(path) if path else (LITE_DIR / "lite.toml")
    if not p.exists():
        return cfg
    try:
        import tomllib
        data = tomllib.loads(p.read_text(encoding="utf-8"))
    except Exception:
        # Битый конфиг не должен ронять ядро: работаем на дефолтах.
        return cfg
    for k, v in (data.get("device") or {}).items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    for k, v in (data.get("server") or {}).items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    for k, v in (data.get("voice") or {}).items():
        attr = k if hasattr(cfg, k) else f"voice_{k}"
        if hasattr(cfg, attr):
            setattr(cfg, attr, v)
    # [llm]: api_key/model читаются как есть, остальные ключи — с префиксом
    # llm_ (mistral_key -> llm_mistral_key и т.п.). Раньше секция не читалась
    # вовсе — ключи из lite.toml молча игнорировались.
    for k, v in (data.get("llm") or {}).items():
        attr = k if hasattr(cfg, k) else f"llm_{k}"
        if hasattr(cfg, attr):
            setattr(cfg, attr, v)
    # [net]: выключатель релея и сетевые адреса. Секция не читалась вовсе:
    # «[net] relay = false» из lite.toml был no-op, и колонка продолжала
    # ходить в облако (аудит 22.11). Имена key->атрибут: relay/upstream
    # исторические, остальные совпадают (http_port, https_port, pin_ip).
    for k, v in (data.get("net") or {}).items():
        attr = {"relay": "relay_enabled", "upstream": "upstream_host"}.get(k, k)
        if hasattr(cfg, attr):
            setattr(cfg, attr, v)
    if isinstance(data.get("pin_hosts"), list):
        cfg.pin_hosts = tuple(data["pin_hosts"])
    if not cfg.device_id:
        # Серийник живого устройства в репозитории не хранится. device.json
        # кладёт deploy_lite.sh — оттуда и берём id. Нет файла — работаем с
        # пустым id (в PCRouter это валидный ключ сессии), но голос/привязка
        # так работать не будут: пишем в лог подсказку.
        dev = load_device(cfg.device_json)
        if dev.get("id"):
            cfg.device_id = str(dev["id"])
        else:
            logging.getLogger("config").warning(
                "device_id не задан: нет [device] device_id в lite.toml и нет "
                "id в %s — разверните через deploy_lite.sh", cfg.device_json)
    return cfg


def load_device(path: str | Path | None = None) -> dict:
    """Запись устройства, снятая с колонки: glagol-сертификат и приватный ключ.

    Колонка ожидает их в sync_info.glagol.security и поднимает на них свой
    локальный HTTPS-сервер glagol (порт 1961). Пустой сертификат = сломанный
    голос, поэтому без этого файла автономный режим запускать нельзя.
    """
    p = Path(path) if path else Path(LITE_DIR / "device.json")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
