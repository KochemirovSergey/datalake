"""
Row Gate: фильтрация строк-агрегатов из bronze-данных.

Источник: BUGFIXES_AUDIT.md §5, §6; ЧТЗ §4.1

Удаляются строки, где регион является:
  - "Российская Федерация" / "Всего по РФ"  (дублируют сумму регионов)
  - Федеральным округом                      (агрегат нескольких субъектов)
  - Агрегатами с НАО/ХМАО/ЯНАО              (двойной счёт)
  - region_code = RU-FED или SKIP
"""

import re

_QUARANTINE_NAMES: frozenset[str] = frozenset({
    "российская федерация",
    "всего по рф",
    "всего по российской федерации",
    "архангельская область включая ненецкий автономный округ",
    "тюменская область включая ханты мансийский автономный округ "
    "югра и ямало ненецкий автономный округ",
    # Дополнительно: регионы с «(включая ...)» — агрегаты
    "тюменская область включая хмао и янао",
})

QUARANTINE_CODES: frozenset[str] = frozenset({"RU-FED", "SKIP"})

_FED_DISTRICT = re.compile(r"федеральный\s+округ", re.IGNORECASE)


def is_aggregate(region_raw: str | None, region_code: str | None = None) -> bool:
    """True если строка является региональным агрегатом и должна быть удалена."""
    if region_code in QUARANTINE_CODES:
        return True
    if not region_raw:
        return False
    s = str(region_raw).strip().lower()
    if s in _QUARANTINE_NAMES:
        return True
    if _FED_DISTRICT.search(s):
        return True
    return False
