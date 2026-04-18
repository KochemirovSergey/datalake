"""
OCR-артефакты: замена латинских символов на кириллические.

Источник: BUGFIXES_AUDIT.md §3, §8; ЧТЗ §4.2
Применяется только к полям с именами регионов ПЕРЕД маппингом.
"""

# Точечный маппинг Latin → Cyrillic (только визуальные двойники)
_LATIN_TO_CYR: dict[int, int] = {
    ord("c"): ord("с"),
    ord("C"): ord("С"),
    ord("o"): ord("о"),
    ord("O"): ord("О"),
    ord("a"): ord("а"),
    ord("A"): ord("А"),
    ord("p"): ord("р"),
    ord("P"): ord("Р"),
    ord("x"): ord("х"),
    ord("X"): ord("Х"),
    ord("e"): ord("е"),
    ord("E"): ord("Е"),
    # Дополнительно из region_normalizer (BUGFIXES_AUDIT §3)
    ord("H"): ord("Н"),
    ord("h"): ord("н"),
    ord("B"): ord("В"),
    ord("M"): ord("М"),
    ord("K"): ord("К"),
    ord("T"): ord("Т"),
}

# Нормализация дефис-подобных символов → обычный дефис (BUGFIXES_AUDIT §4.4)
_DASH_VARIANTS = str.maketrans({
    "\u2013": "-",  # EN DASH –
    "\u2014": "-",  # EM DASH —
    "\u2010": "-",  # HYPHEN ‐
    "\u00ad": "-",  # SOFT HYPHEN
})


def fix_ocr(s: str) -> str:
    """
    Применяет OCR-фиксы к строке с именем региона:
      1. Latin lookalikes → Кириллица
      2. Нестандартные дефисы → -
      3. strip() + lower()
    """
    if not s:
        return s
    s = str(s).translate(_LATIN_TO_CYR).translate(_DASH_VARIANTS)
    return s.strip().lower()


def fix_ocr_preserve_case(s: str) -> str:
    """Применяет OCR-фиксы без изменения регистра (для хранения region_name_raw)."""
    if not s:
        return s
    return str(s).translate(_LATIN_TO_CYR).translate(_DASH_VARIANTS).strip()
