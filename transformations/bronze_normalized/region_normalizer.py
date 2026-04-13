"""
Общая библиотека нормализации названий регионов.

Перенесено из silver_doshkolka.py + дополнено из silver_naselenie.py.
После создания этого модуля silver-трансформации должны импортировать
функции отсюда, а не дублировать логику.

Публичный API:
  normalize_region_name(name)  — строка → нормализованный ключ
  build_region_index(cat)      — SqlCatalog → dict[key → region_code]
  lookup_region(name, index)   — строка × индекс → region_code | None
"""

import re
from pyiceberg.catalog.sql import SqlCatalog


def normalize_region_name(name: str) -> str:
    """
    Приводит название региона к единому виду для сравнения с region_lookup.

    Шаги:
      1. Замена латинских букв, часто встречающихся как артефакт OCR/копипасты
      2. Приведение к нижнему регистру
      3. Удаление префиксов «г.», «город», «город федерального значения»
      4. Раскрытие скобок — содержимое сохраняется («Якутия»)
      5. Тире → пробел (Кабардино-Балкарская, Северная Осетия-Алания)
      6. Раскрытие «авт.» → «автономный»
      7. Схлопывание пробелов
    """
    if not name:
        return ""
    s = name.strip()

    # Латинская H → кириллическая Н (артефакт в 2018: "Hижегородская")
    s = s.replace("H", "Н").replace("h", "н")
    # Латинская O → кириллическая О (артефакт в Население: "Oмская")
    s = s.replace("O", "О").replace("o", "о")

    s = s.lower()

    # "Город Санкт-Петербург город федерального значения" → "санкт-петербург"
    s = re.sub(r"город федерального значения\s*", "", s)
    s = re.sub(r"^город\s+", "", s)

    # "г. Санкт-Петербург" / "г Санкт-Петербург" → убрать префикс
    s = re.sub(r"^г\.\s*", "", s)
    s = re.sub(r"^г\s+", "", s)

    # Скобки: контент сохраняем («(Якутия)» → «Якутия»)
    s = re.sub(r"\(([^)]+)\)", r" \1", s)

    # Тире → пробел
    s = s.replace("-", " ").replace("–", " ")

    # «авт.» → «автономный »
    s = s.replace("авт.", "автономный ")

    # Схлопывание пробелов
    s = re.sub(r"\s+", " ", s).strip()

    return s


def build_region_index(cat: SqlCatalog) -> dict[str, str]:
    """
    Строит индекс нормализованное_название → region_code из bronze.region_lookup.
    Включает:
      1. Все name_variant (нормализованные)
      2. Все region_code (в нижнем регистре)
    """
    df = cat.load_table("bronze.region_lookup").scan().to_pandas()
    index: dict[str, str] = {}
    for _, row in df.iterrows():
        # Регистрация по названию
        key = normalize_region_name(row["name_variant"])
        index[key] = row["region_code"]

        # Регистрация по ISO коду (RU-MOW -> ru mow)
        code = row["region_code"]
        if code and code != SKIP:
            index[normalize_region_name(code)] = code

    return index


SKIP = "SKIP"  # Сентинель: агрегат нескольких субъектов, не должен попасть в данные


def lookup_region(name: str, index: dict[str, str]) -> str | None:
    """
    Ищет region_code по названию.

    Стратегия:
      1. Точное совпадение нормализованного ключа.
         Если найден «SKIP» — возвращает SKIP (агрегат, идёт в quarantine).
      2. Последовательно убирать последнее слово (до 4 попыток).
         Покрывает случаи:
           «кемеровская область кузбасс»  → «кемеровская область»
           «чувашская республика чувашия» → «чувашская республика»

    Возвращает None если регион не найден.
    """
    key = normalize_region_name(name)

    # Точное совпадение — включая quarantine-записи (SKIP)
    if key in index:
        return index[key]

    # Стратегия trim: НЕ применяем к quarantine, только к обычным вариантам
    parts = key.split()
    for trim in range(1, min(4, len(parts))):
        shorter = " ".join(parts[: len(parts) - trim])
        if shorter in index:
            code = index[shorter]
            if code == SKIP:
                # Укороченный вариант совпал с quarantine — не возвращаем
                continue
            return code

    return None
