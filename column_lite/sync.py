"""Ответы quasar-API для колонки: sync_info, timezone, user_details, обновления.

Раньше в main.py лежала самописная заглушка sync_info из четырёх полей. Колонка
на такой конфиг реагирует плохо: пропадает account_config (spotter «alisa»),
device_config (локаль, имя, координаты, standby) и onlineSpotterEnabled, а в
glagol.security уезжает пустой сертификат — её локальный HTTPS-сервер glagol
перестаёт подниматься, и голос умирает целиком.

Здесь полный конфиг, повторяющий alice_server/store.py::_default_sync_info, плюс
glagol-сертификат из снятой с колонки записи устройства.
"""
from __future__ import annotations

import copy

from .config import LiteConfig


def _default_sync_info(cfg: LiteConfig) -> dict:
    return {
        "alice_pro_subscription": {"enabled": False},
        "config": {
            "account_config": {
                "spotter": "alisa", "jingle": False, "smartActivation": True,
                "contentAccess": "without", "childContentAccess": "children",
                "aliceProactivity": False, "saveHistoryUsage": False,
            },
            "device_config": {
                "locale": "ru-RU", "name": cfg.device_name, "beta": False, "tv_beta": False,
                "location": {"latitude": cfg.latitude, "longitude": cfg.longitude},
                "dndMode": {"enabled": False, "features": {"allowIncomingCalls": False}},
                "standby": {"deepStandbyEnabled": True, "deepStandbyTimeoutMinutes": 240},
            },
            "system_config": {
                "uniProxyUrl": cfg.uniproxy_url,
                "voiceDialogSettings": {"useProtoAliceApi": False},
                # Без этого колонка может включить облачный споттер и слушать
                # «Алиса» сама — тогда наши пины и музыка ей не нужны, а трафик
                # уходит в облако мимо нас.
                "onlineSpotterEnabled": False,
            },
        },
        "subscription": {"mode": "transaction"},
        "tags": [],
        "status": "ok",
    }


def _unbound_block(cfg: LiteConfig) -> dict:
    return {
        "enable": True,
        "checkHosts": ["quasar.yandex.net", "uniproxy.alice.yandex.net", "scbh.yandex.net"],
        # только .8/.1: egress пускает DNS на эти два адреса до DROP 77.88/18;
        # .88/.2 в конфиге означали таймауты каждого третьего запроса (18.11)
        "forwarders": ["77.88.8.1", "77.88.8.8"],
        "serverParams": [
            "msg-cache-size: 2m",
            "rrset-cache-size: 2m",
            'username: "nobody"',
            *[f'local-data: "{h} A {cfg.pin_ip}"' for h in cfg.pin_hosts],
        ],
    }


def build_sync_info(cfg: LiteConfig, device: dict, device_id: str = "",
                    platform: str = "") -> dict:
    """Полный ответ /get_sync_info.

    device — запись из device.json (glagol.security = сертификат + ключ станции).
    device_id/platform из запроса важнее дефолтов конфига: колонка присылает свои.
    """
    info = copy.deepcopy(_default_sync_info(cfg))
    sysc = info["config"]["system_config"]

    glagol_sec = (device.get("glagol") or {}).get("security") or {}
    if glagol_sec.get("server_certificate") and glagol_sec.get("server_private_key"):
        info["glagol"] = {"security": glagol_sec}
    else:
        # Не подменяем пустым сертификатом: лучше не отдавать glagol вовсе,
        # чем отдать пустой и сломать станции её локальный HTTPS-сервер.
        info["glagol"] = {}

    if cfg.pin_ip:
        sysc["unbound"] = _unbound_block(cfg)

    # Обновления прошивки выключаем: новая прошивка может сломать совместимость.
    sysc["com.yandex.launcher.updaterapp"] = {
        "settings": {
            "allowFirmwareUpdate": {"ota": False},
            "allowFirmwareUpdateV2": {"ota": False},
        }
    }

    if device_id:
        info["config"].setdefault("device_config", {})["device_id"] = device_id
    if platform:
        info["config"].setdefault("device_config", {})["platform"] = platform
    if device.get("name"):
        info["config"]["device_config"]["name"] = device["name"]
    return info


def build_timezone(cfg: LiteConfig) -> dict:
    return {"timezone": cfg.timezone, "status": "ok",
            "offset_sec": cfg.timezone_offset_sec}


def build_user_details(cfg: LiteConfig) -> dict:
    return {"status": "ok", "user_details": {
        "display_name": {"avatar": {"default": "0/0-0", "empty": True},
                         "name": cfg.display_name}}}


def build_no_updates() -> dict:
    return {"apps": [], "rom_update": None, "ab_update": None,
            "sys_recovery_update": None, "on_boot_update": None, "batches": []}
