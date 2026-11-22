"""Цвет подсветки кольца колонки — напрямую через sysfs ядра.

На Лайт кольцо — три зоны (ring1..3), у каждой канал R/G/B, яркость 0–255
(/sys/class/leds/micro:{red,green,blue}:ringN/brightness). Проверено живьём
20.11: запись держится, красная заливка видна. Прошивка на время своих
анимаций (слушаю/говорю/громкость) перехватывает каналы, поэтому цвет
повторно подтверждаем раз в секунду, пока светим (см. Router._hold_led).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger("leds")

LEDS_ROOT = Path(os.environ.get("LITE_LEDS_DIR", "/sys/class/leds"))
RINGS = (1, 2, 3)

# основа слова -> RGB. Сопоставление по основе («син» поймает «синим/синий»),
# поэтому длинные основы проверяются первыми (сиренев раньше син и т.п.)
COLORS = {
    "красн": (255, 0, 0),
    "зелен": (0, 255, 0),
    "син": (0, 0, 255),
    "желт": (255, 255, 0),
    "оранж": (255, 102, 0),
    "фиолет": (140, 0, 255),
    "розов": (255, 105, 180),
    "голуб": (0, 190, 255),
    "бирюзов": (0, 255, 200),
    "бел": (255, 255, 255),
    "салатов": (170, 255, 80),
    "сиренев": (200, 150, 255),
    "изумруд": (0, 255, 120),
    "золот": (255, 200, 40),
}
DEFAULT = (0, 160, 255)     # фирменный голубой, если цвет не назвали

_STEMS = sorted(COLORS, key=len, reverse=True)


def color(word: str) -> tuple[int, int, int]:
    """Основа из фразы -> RGB; неизвестный/пустой — фирменный голубой."""
    return COLORS.get(word, DEFAULT)


def find_color(text: str) -> str:
    """Основа цвета в нормализованной фразе ('' — не назвали)."""
    for stem in _STEMS:
        if stem in text:
            return stem
    return ""


def available() -> bool:
    return (LEDS_ROOT / "micro:red:ring1" / "brightness").exists()


def _write(name: str, value: int) -> bool:
    """Одна запись в канал. Вынесено отдельно: имена с «:» в sysfs, на Windows
    такой каталог не создать — тесты подменяют именно эту функцию."""
    try:
        (LEDS_ROOT / name / "brightness").write_text(str(int(value)))
        return True
    except OSError:
        return False


def set_color(rgb: tuple[int, int, int]) -> bool:
    """Залить кольцо цветом. False — sysfs недоступен/не пишется."""
    ok = False
    r, g, b = rgb
    for ring in RINGS:
        for ch, val in (("red", r), ("green", g), ("blue", b)):
            ok = _write(f"micro:{ch}:ring{ring}", val) or ok
    if not ok:
        log.warning("leds: не удалось записать цвет в %s", LEDS_ROOT)
    return ok


def off() -> None:
    set_color((0, 0, 0))
