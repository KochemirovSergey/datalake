"""
Библиотека нормализации и валидации года.

Для текущих источников (doshkolka, naselenie) год уже записан
в bronze.excel_tables.year экселевским лоадером — либо из имени папки,
либо из паттерна _YYYY.xlsx.

Данный модуль валидирует, что год находится в ожидаемом диапазоне,
и оформляет записи для bronze_normalized.year / year_error.

Публичный API:
  validate_year(year) → bool
  normalize_year_from_field(row, year_type) → (year_int | None, error_type | None)
"""

VALID_YEAR_MIN = 2000
VALID_YEAR_MAX = 2030


def validate_year(year: int) -> bool:
    """True если год в допустимом диапазоне для статистических данных."""
    return VALID_YEAR_MIN <= year <= VALID_YEAR_MAX


def normalize_year_from_field(raw_year, year_type: str) -> tuple[int | None, str | None]:
    """
    Нормализует значение поля year из bronze.excel_tables.

    raw_year  — значение поля year (обычно int или строка)
    year_type — «period» или «exact_date»

    Возвращает (year_int, None) при успехе или (None, error_type) при ошибке.
    error_type: «empty» | «unparseable» | «out_of_range»
    """
    if raw_year is None:
        return None, "empty"

    try:
        year_int = int(raw_year)
    except (ValueError, TypeError):
        return None, "unparseable"

    if not validate_year(year_int):
        return None, "out_of_range"

    return year_int, None
