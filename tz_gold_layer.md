# ТЗ: Gold-слой — 3 таблицы

## Общие принципы


- Слой: `gold`
- Хранилище: Apache Iceberg (аналогично Silver)
- Dagster-ассет: `goldpipeline.py`, группа `gold`
- Гранулярность и ключи указаны для каждой таблицы отдельно
- Теги (гос/негос, город/село) агрегируются — суммируются по всем комбинациям тегов перед записью в Gold
- Паттерн агрегации брать из существующих примеров в `transformations/staffshortagepipeline.py`

Типы данных: При вычислениях и джойнах обязательно оборачиваем сырые строковые колонки из слоя Bronze в CAST(... AS FLOAT) или INTEGER, чтобы DuckDB не упал с ошибкой.

Агрегация: Сначала делаем GROUP BY (схлопываем теги внутри каждого источника), и только потом делаем LEFT JOIN, чтобы избежать геометрического разрастания строк.

---

## Таблица 1: `gold.students` — Обучающиеся

### Источник
`silver.educationpopulationwideannual` (`datalake.main.silver_education_population_wide_annual`)

### Описание
Таблица уже содержит нужный формат: одна строка = регион × год × однолетний возраст.
Колонки по уровням образования — широкий формат (wide).
Колонка `region_name_raw` не несёт аналитической ценности на Gold-слое — убрать.

Берём нормальный возраст (0–80 лет) из сборной витрины education_population_wide_annual_silver, чтобы студенты и школьники не схлопнулись в категорию «7+».

### Ключи
`region_code`, `year`, `age`

### Колонки

| Колонка | Тип | Источник | Описание |
|---|---|---|---|
| `region_code` | VARCHAR | `region_code` | ISO-код региона |
| `year` | INTEGER | `year` | Учебный год |
| `age` | INTEGER | `age` | Возраст (однолетний, 0–80) |
| `population_total` | FLOAT | `population_total` | Численность населения данного возраста |
| `level_1_1` | FLOAT | `level_1_1` | Уровень 1.1 — дошкольное образование |
| `level_1_2` | FLOAT | `level_1_2` | Уровень 1.2 — начальное общее |
| `level_1_3` | FLOAT | `level_1_3` | Уровень 1.3 — основное общее |
| `level_1_4` | FLOAT | `level_1_4` | Уровень 1.4 — среднее общее |
| `level_2_5_1` | FLOAT | `level_2_5_1` | Уровень 2.5.1 — СПО (подготовка квалиф. рабочих) |
| `level_2_5_2` | FLOAT | `level_2_5_2` | Уровень 2.5.2 — СПО (специалисты среднего звена) |
| `level_2_6` | FLOAT | `level_2_6` | Уровень 2.6 — бакалавриат |
| `level_2_7` | FLOAT | `level_2_7` | Уровень 2.7 — специалитет |
| `level_2_8` | FLOAT | `level_2_8` | Уровень 2.8 — магистратура |
| `level_4_8b_1` | FLOAT | `level_4_8b_1` | Уровень 4.8b.1 — аспирантура |
| `level_4_8b_2` | FLOAT | `level_4_8b_2` | Уровень 4.8b.2 — докторантура |
| `education_total` | FLOAT | `education_total` | Всего обучающихся (сумма по всем уровням) |
| `education_share` | FLOAT | `education_share` | Доля охваченных образованием (education_total / population_total) |

### Трансформации
- Убрать `region_name_raw`
- Тип `age` привести к INTEGER (в source — строка)
- Остальные колонки переносятся без изменений (select + cast)

---

## Таблица 2: `gold.staff_load` — Педагогическая нагрузка

### Источники
- `silver.staffshortagetriggers` — нагрузка, вакансии (доли), score
- `bronze.oo134230` (`public.oo134230`) — ставки по штату, численность по факту
- `bronze.oo131218` (`public.oo131218`) — квалификация, FTE
- `bronze.oo132221` (`public.oo132221`) — стаж педагогов

### Ключи
`region_code`, `year`, `level`

### Уровни образования (`level`)
- `1.2` — основное общее (одна группа педработников)
- `1.3+1.4` — среднее общее (вторая группа педработников)
- `2.5_bachelor`, `2.5_specialist`, `2.5_master` — СПО/ВПО (три подгруппы)

### Колонки

| Колонка | Тип | Источник | Описание |
|---|---|---|---|
| `region_code` | VARCHAR | silver / bronze | ISO-код региона |
| `year` | INTEGER | silver / bronze | Учебный год |
| `level` | VARCHAR | silver | Уровень образования |
| `student_count` | FLOAT | silver `student_count` | Численность обучающихся по факту |
| `positions_total` | FLOAT | bronze_230 `Число ставок по штату, единиц` | Ставок по штату (агрегировано по тегам) |
| `staff_headcount` | FLOAT | bronze_230 `Численность работников на начало отчетного учебного года` | Занято по факту (без совместителей) |
| `vacancies_unfilled` | FLOAT | `positions_total - staff_headcount` | Незаполненных вакансий, штук |
| `staff_fte` | FLOAT | bronze_218 `Численность работников в пересчете на полную занятость, единиц` | FTE (полная занятость) |
| `qual_higher_edu` | FLOAT | bronze_218 `Высшее` | Имеют высшее образование |
| `qual_higher_cat` | FLOAT | bronze_218 `высшую` | Имеют высшую категорию |
| `qual_first_cat` | FLOAT | bronze_218 `первую` | Имеют первую категорию |
| `qual_ped_higher` | FLOAT | bronze_218 `из них (графы 4) педагогическое` | Из высшего — педагогическое |
| `qual_spe_mid` | FLOAT | bronze_218 `среднее профессиональное образование по программам подготовки специалистов среднего звена` | СПО специалисты среднего звена |
| `qual_ped_mid` | FLOAT | bronze_218 `из них (графы 10) педагогическое` | Из СПО — педагогическое |
| `qual_candidate` | FLOAT | bronze_218 `кандидата наук` | Кандидатов наук |
| `qual_doctor` | FLOAT | bronze_218 `доктора наук` | Докторов наук |
| `exp_total` | FLOAT | bronze_221 `Всего (сумма граф 4-9)` | Всего со стажем |
| `exp_none` | FLOAT | bronze_221 `Не имеют стажа педагогической работы` | Без педагогического стажа |
| `exp_lt3` | FLOAT | bronze_221 `до 3` | Стаж до 3 лет |
| `exp_3_5` | FLOAT | bronze_221 `от 3 до 5` | Стаж 3–5 лет |
| `exp_5_10` | FLOAT | bronze_221 `от 5 до 10` | Стаж 5–10 лет |
| `exp_10_15` | FLOAT | bronze_221 `от 10 до 15` | Стаж 10–15 лет |
| `exp_15_20` | FLOAT | bronze_221 `от 15 до 20` | Стаж 15–20 лет |
| `exp_gt20` | FLOAT | bronze_221 `20 и более` | Стаж 20+ лет |
| `load_ratio` | FLOAT | silver `trig1_val` | Коэффициент нагрузки (учеников / ставку) |
| `vacancy_unfilled_share` | FLOAT | silver `trig2_val` | Доля незакрытых вакансий (0–1) |
| `avg_hours_per_teacher` | FLOAT | silver `bonus_score` | Средняя нагрузка в часах на педагога |
| `shortage_score` | FLOAT | silver `score` | Итоговый индекс дефицита кадров (0–5) |

---

## Таблица 3: `gold.dormitory` — Общежития

### Источники
- `silver.dormitoryinfrastructure` — все метрики
- (bronze.2138 и bronze.21410 уже агрегированы в Silver)

### Ключи
`region_code`, `year`

### Колонки

| Колонка | Тип | Источник | Описание |
|---|---|---|---|
| `region_code` | VARCHAR | silver | ISO-код региона |
| `year` | INTEGER | silver | Год |
| `area_total` | FLOAT | silver `area_total` | Всего площадей (кв.м) |
| `area_need_repair` | FLOAT | silver `area_need_repair` | Площадь, нуждающаяся в капремонте (кв.м) |
| `repair_share` | FLOAT | silver `metric_1` | Доля площади под ремонт (area_need_repair / area_total) |
| `area_emergency` | FLOAT | silver `area_emergency` | Аварийная площадь (кв.м) |
| `dorm_shortage_abs` | FLOAT | silver `dorm_shortage_abs` | Нехватка мест (абс., = dorm_need − dorm_live) |
| `dorm_shortage_share` | FLOAT | silver `metric_3` | Доля нехватки (dorm_shortage_abs / dorm_need) |
| `dorm_need` | FLOAT | silver `dorm_need` | Потребность в местах |
| `dorm_live` | FLOAT | silver `dorm_live` | Мест по факту |
| `is_forecast` | BOOLEAN | silver `is_forecast` | Признак прогнозных данных |
| `alert_flag` | INTEGER | silver `alert_flag` | Флаг аномалии (sigma-alert, 0/1) |

---

## Dagster-ассеты (новые)

| Ассет | Зависимости | Выход |
|---|---|---|
| `students_gold` | `educationpopulationwideannualsilver` | `gold.students` |
| `staff_load_gold` | `staffshortagesilver`, `staffshortagesourcesbronze` | `gold.staff_load` |
| `dormitory_gold` | `dormitorysilver` | `gold.dormitory` |

## Файлы для создания

- `transformations/gold_students.py`
- `transformations/gold_staff_load.py`
- `transformations/gold_dormitory.py`
- `dagsterproject/assets/gold_assets.py`
- `validation/validate_gold.py`