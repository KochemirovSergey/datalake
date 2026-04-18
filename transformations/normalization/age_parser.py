"""
Парсер возрастных строк → (age_min, age_max, age_category).

Источник: BUGFIXES_AUDIT.md §4.1-4.4; ЧТЗ §4.4

Входные форматы:
  "от 2 до 3 лет"     → (2,  3,    "range")
  "16 лет"            → (16, 16,   "exact")
  "40 лет и старше"   → (40, None, "open_end")
  "14 лет и моложе"   → (None, 14, "open_start")
  "моложе 15 лет"     → (None, 14, "open_start")
  "всего"             → (None, None, "total")
  "возраст неизвестен"→ (None, None, "unknown")
  "25-29" / "25–29"   → (25, 29,  "range")
  "5" / "5 лет"       → (5,  5,   "exact")
  "80 и старше"       → (80, None, "open_end")
  "80+" / "80 и более"→ (80, None, "open_end")
  ": 26" суффикс      → (26, 26,  "exact")   # BUGFIXES_AUDIT §4.2
"""

import re

_DASH_NORM = re.compile(r"[–—‐\u00ad]")  # нормализация тире (BUGFIXES_AUDIT §4.4)


def parse_age(s: str) -> tuple[int | None, int | None, str]:
    """
    Парсит строку с описанием возраста.
    Возвращает (age_min, age_max, age_category).
    """
    if not s:
        return None, None, "unknown"

    raw = str(s).strip()
    norm = _DASH_NORM.sub("-", raw).lower().strip()

    # Агрегаты
    if norm in ("всего", "итого", "total"):
        return None, None, "total"
    if "возраст неизвестен" in norm or "неизвестен" in norm:
        return None, None, "unknown"

    # "от X до Y лет"
    m = re.search(r"от\s+(\d+)\s+до\s+(\d+)", norm)
    if m:
        return int(m.group(1)), int(m.group(2)), "range"

    # "X лет и старше" / "X и старше" / "X+" / "80 и более"
    m = re.search(r"(\d+)(?:\s+(?:лет?|года?))?\s+и\s+(?:старше|более)", norm)
    if m:
        return int(m.group(1)), None, "open_end"
    m = re.fullmatch(r"(\d+)\+", norm)
    if m:
        return int(m.group(1)), None, "open_end"

    # "моложе X лет" / "X лет и моложе"
    m = re.search(r"моложе\s+(\d+)", norm)
    if m:
        return None, int(m.group(1)) - 1, "open_start"
    m = re.search(r"(\d+)(?:\s+(?:лет?|года?))?\s+и\s+моложе", norm)
    if m:
        return None, int(m.group(1)), "open_start"

    # "X-Y" диапазон (BUGFIXES_AUDIT §4.4 — нормализация тире уже выше)
    m = re.fullmatch(r"(\d{1,3})-(\d{1,3})(?:\s+(?:лет?|года?))?", norm)
    if m:
        return int(m.group(1)), int(m.group(2)), "range"

    # "в том числе в возрасте ...: N" (BUGFIXES_AUDIT §4.2, ВПО и СПО)
    m = re.search(r":\s*(\d+)\s*$", norm)
    if m:
        age = int(m.group(1))
        return age, age, "exact"

    # "X лет" / "X год[а]" / "X года"
    m = re.search(r"(\d+)\s+(?:лет?|год[а]?)", norm)
    if m:
        return int(m.group(1)), int(m.group(1)), "exact"

    # Просто число
    m = re.fullmatch(r"(\d+)", norm)
    if m:
        age = int(m.group(1))
        return age, age, "exact"

    return None, None, "unknown"
