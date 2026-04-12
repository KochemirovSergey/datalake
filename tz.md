# ТЗ: bronze_normalized (education_level) + silver (образовательный контингент)

## 1. Контекст

Источники — 5 PostgreSQL-таблиц, загруженных в bronze через `postgres_loader.py`.
Все значения в bronze хранятся как string. Bronze не изменяется.

Этапы:
1. **bronze_normalized.education_level** — разметка уровня/программы на каждой строке
2. **silver.oo / silver.spo / silver.vpo / silver.dpo** — агрегированные витрины

---

## 2. Справочник уровней образования

| code | label |
|------|-------|
| 1.2 | Начальное общее образование |
| 1.3 | Основное общее образование |
| 1.4 | Среднее общее образование |
| 2.5 | Среднее профессиональное образование |
| 2.5.1 | СПО — квалифицированные рабочие, служащие |
| 2.5.2 | СПО — специалисты среднего звена |
| 2.6 | Высшее образование — бакалавриат |
| 2.7 | Высшее образование — специалитет |
| 2.8 | Высшее образование — магистратура |
| 4.8b | Дополнительное профессиональное образование |
| 4.8b.1 | ДПО — программы повышения квалификации |
| 4.8b.2 | ДПО — программы профессиональной переподготовки |

---

## 3. bronze_normalized.education_level

### 3.1 Файлы

- Справочник: `data/education_level_lookup.csv`
- Конфиг: `ingestion/normalization_config.yaml`
- Пайплайн: `transformations/bronze_normalized/education_level_pipeline.py`

### 3.2 Обновление education_level_lookup.csv

```csv
source_table,match_field,match_value,level_code,level_label,program_code,program_label
oo_1_2_7_1_209,column_name,начального общего образования - всего (1–4 классы),1.2,Начальное общее образование,,
oo_1_2_7_1_209,column_name,основного общего образования - всего (5–9 классы),1.3,Основное общее образование,,
oo_1_2_7_1_209,column_name,среднего общего образования - всего (10–11(12) классы,1.4,Среднее общее образование,,
oo_1_2_7_2_211,column_name,начального общего образования – всего,1.2,Начальное общее образование,,
oo_1_2_7_2_211,column_name,основного общего образования – всего,1.3,Основное общее образование,,
oo_1_2_7_2_211,column_name,среднего общего образования – всего,1.4,Среднее общее образование,,
спо_1_р2_101_43,column_metadata_2,"Программы подготовки квалифицированных рабочих, служащих",2.5,Среднее профессиональное образование,2.5.1,"Программы подготовки квалифицированных рабочих, служащих"
спо_1_р2_101_43,column_metadata_2,Программы подготовки специалистов среднего звена,2.5,Среднее профессиональное образование,2.5.2,Программы подготовки специалистов среднего звена
впо_1_р2_13_54,column_metadata_1,Программы бакалавриата,2.6,Высшее образование — бакалавриат,,
впо_1_р2_13_54,column_metadata_1,Программы специалитета,2.7,Высшее образование — специалитет,,
впо_1_р2_13_54,column_metadata_1,Программы магистратуры,2.8,Высшее образование — магистратура,,
пк_1_2_4_180,row_name,в том числе обученных по программам:          повышения квалификации,4.8b,Дополнительное профессиональное образование,4.8b.1,Программы повышения квалификации
пк_1_2_4_180,row_name,профессиональной переподготовки,4.8b,Дополнительное профессиональное образование,4.8b.2,Программы профессиональной переподготовки
```

### 3.3 Обновление normalization_config.yaml

```yaml
sources:
  - source_table: oo_1_2_7_1_209
    required: true
    location: column_name
    match_field: column_name

  - source_table: oo_1_2_7_2_211
    required: true
    location: column_name
    match_field: column_name

  - source_table: спо_1_р2_101_43
    required: true
    location: column_metadata_2
    match_field: column_metadata_2
    filters:
      column_name:
        - "Числен-ность студентов"

  - source_table: впо_1_р2_13_54
    required: true
    location: column_metadata_1
    match_field: column_metadata_1

  - source_table: пк_1_2_4_180
    required: true
    location: row_name
    match_field: row_name
```

### 3.4 Логика пайплайна (education_level_pipeline.py)

Для каждой строки из bronze PostgreSQL-источника:

1. Определить `source_table`
2. Найти конфиг источника в `normalization_config.yaml`
3. Если источник не описан → error `unsupported_source`
4. Применить фильтры из конфига (если есть)
   - Строка не проходит фильтр → error `filter_not_matched`
5. Прочитать значение поля `match_field`
   - Поле отсутствует → error `match_field_missing`
   - Поле пустое/null → error `match_value_empty`
6. Найти строку в `education_level_lookup.csv` по `source_table + match_field + match_value`
   - Не найдено → error `lookup_not_found`
7. При успехе → записать в `bronze_normalized.education_level`

### 3.5 Схема bronze_normalized.education_level (ok)

```
row_id          string
source_table    string
level_code      string
level_label     string
program_code    string   (nullable)
program_label   string   (nullable)
match_field     string
match_value     string
status          string   = 'ok'
```

### 3.6 Схема bronze_normalized.education_level_error

```
row_id          string
source_table    string
match_field     string
match_value     string   (nullable)
error_type      string   (unsupported_source | filter_not_matched | match_field_missing | match_value_empty | lookup_not_found)
error_details   string
```

### 3.7 Типы ошибок

| error_type | Причина |
|---|---|
| unsupported_source | source_table не описан в конфиге |
| filter_not_matched | строка не прошла обязательный фильтр по полю |
| match_field_missing | в bronze нет колонки match_field |
| match_value_empty | колонка есть, но значение пустое / null |
| lookup_not_found | значение есть, но в справочнике не найдено |

### 3.8 Статистика в Dagster UI (normalized_education asset)

```
ok_count       — строк успешно размечено
error_count    — строк с ошибкой
coverage       — ok / (ok + error), %
breakdown:
  oo_1_2_7_1_209:  ok=N error=M
  oo_1_2_7_2_211:  ok=N error=M
  спо_1_р2_101_43: ok=N error=M
  впо_1_р2_13_54:  ok=N error=M
  пк_1_2_4_180:    ok=N error=M
```

---

## 4. Silver: агрегированные витрины

Silver читает строки, у которых в `bronze_normalized.row_gate` стоит `ready_for_silver = true`.
Это означает, что у строки нормализованы регион, год и education_level.

### 4.1 Общий алгоритм построения любой silver-витрины

1. Join bronze.<source_table> с bronze_normalized.row_gate по row_id, фильтр ready_for_silver=true
2. Join с bronze_normalized.region по row_id → получить region_code, region_name_raw
3. Join с bronze_normalized.year по row_id → получить year
4. Join с bronze_normalized.education_level по row_id → получить level_code, program_code
5. Применить фильтры источника (оставить только целевые row_name / column_name)
6. Привести значение value к числу (CAST, nullable при невозможности привести)
7. Агрегировать sum(value) по целевому ключу
8. Записать результат в silver-таблицу

---

### 4.2 silver.oo

**Источники:** bronze.oo_1_2_7_1_209 + bronze.oo_1_2_7_2_211

**Что агрегируем (схлопываем через SUM):**
- Государственные + Негосударственные (тег_1)
- Городские + Сельские (тег_2)
- Строки из обеих исходных таблиц (объединяются через UNION)

**Целевой ключ агрегации:**
```
region_code, year, age, level_code
```

**Фильтры при отборе строк:**
- `row_name` должен быть из списка возрастных значений (см. исходный WHERE из запроса ОО)
- `column_name` должен совпасть с одним из трёх уровней (обеспечивается через education_level)

**Схема silver.oo:**
```
region_code      string
region_name_raw  string
year             int
age              string   (нормализованное значение из row_name)
level_code       string   (1.2 | 1.3 | 1.4)
level_label      string
value            int      (sum, количество обучающихся)
```

**Файл трансформации:** `transformations/silver_oo.py`

---

### 4.3 silver.spo

**Источник:** bronze.спо_1_р2_101_43

**Что агрегируем (схлопываем через SUM):**
- Государственные + Негосударственные
- Очная + Очно-заочная + Заочная (тег_3 или аналог в таблице)
- `column_metadata_1`: на базе основного / на базе среднего — схлопываем

**Целевой ключ агрегации:**
```
region_code, year, age, level_code, program_code
```

**Фильтры при отборе строк:**
- `column_name = 'Числен-ность студентов'`
- `row_name` из списка возрастных значений СПО

**Схема silver.spo:**
```
region_code      string
region_name_raw  string
year             int
age              string
level_code       string   = '2.5'
program_code     string   (2.5.1 | 2.5.2)
program_label    string
value            int      (sum)
```

**Файл трансформации:** `transformations/silver_spo.py`

---

### 4.4 silver.vpo

**Источник:** bronze.впо_1_р2_13_54

**Что агрегируем (схлопываем через SUM):**
- Государственные + Негосударственные (тег_1)
- Очная + Очно-заочная + Заочная + Аттестация экстернов (тег_3)

**Целевой ключ агрегации:**
```
region_code, year, age, level_code
```

**Фильтры при отборе строк:**
- `row_name` из списка возрастных значений ВПО

**Схема silver.vpo:**
```
region_code      string
region_name_raw  string
year             int
age              string
level_code       string   (2.6 | 2.7 | 2.8)
level_label      string
value            int      (sum)
```

**Файл трансформации:** `transformations/silver_vpo.py`

---

### 4.5 silver.dpo

**Источник:** bronze.пк_1_2_4_180

**Агрегация:** не требуется, данные уже в целевом виде.
Только: привести к целевому формату, нормализовать age из column_name.

**Фильтры при отборе строк:**
- `row_name` из двух значений программ ДПО (обеспечивается через education_level)
- `column_name` — возрастные корзины (40–44, 35–39, 25–29, 30–34, 45–49, моложе 25, 50–54, 55–59, 60–64, 65 и более)

**Схема silver.dpo:**
```
region_code      string
region_name_raw  string
year             int
age_band         string   (нормализованная возрастная корзина из column_name)
level_code       string   = '4.8b'
program_code     string   (4.8b.1 | 4.8b.2)
program_label    string
value            int
```

**Файл трансформации:** `transformations/silver_dpo.py`

---

## 5. Dagster assets (новые)

### Группа silver (дополнение)

| Asset | Зависимости | Пишет в |
|---|---|---|
| oo_silver | normalized_validation | silver.oo |
| spo_silver | normalized_validation | silver.spo |
| vpo_silver | normalized_validation | silver.vpo |
| dpo_silver | normalized_validation | silver.dpo |
| education_silver_validation | oo_silver, spo_silver, vpo_silver, dpo_silver | reports/silver_education_YYYY-MM-DD.md |

---

## 6. Валидация silver (education_silver_validation)

**Файл:** `validation/validate_silver_education.py`
**Отчёт:** `reports/silver_education_YYYY-MM-DD.md`

### 6.1 Структура отчёта

**Секция 1. Наличие возрастов по уровням**

Таблица: `level_code × age` — есть хотя бы одна запись в этой комбинации или нет.

Цель: убедиться, что после агрегации ни один ожидаемый возраст не исчез.

**Секция 2. Покрытие по регионам**

Для каждого `level_code + age`:
- сколько из 89 регионов имеют хотя бы одну запись

Формат: `level_code | age | регионов заполнено | регионов нет`

**Секция 3. Матрица покрытия по годам**

Для каждого `level_code + age + region_code`:
- какие годы заполнены из ожидаемого диапазона
- отображать как матрицу `region × year` с маркером ✓/✗

**Секция 4. Error summary**

- Топ-20 регионов с наибольшим количеством пропущенных комбинаций
- Распределение по `level_code`: сколько строк ok, сколько пропусков

### 6.2 Статистика в Dagster UI

```
report_path    — путь к отчёту
per_level:
  1.2:  regions=N, ages=M, years_min=X years_max=Y
  1.3:  ...
  1.4:  ...
  2.5.1: ...
  2.5.2: ...
  2.6:  ...
  2.7:  ...
  2.8:  ...
  4.8b.1: ...
  4.8b.2: ...
```

---

## 7. Порядок внедрения

```
1. Обновить education_level_lookup.csv
2. Обновить normalization_config.yaml (убрать stub для oo, vo, pk; уточнить spo)
3. Доработать education_level_pipeline.py (добавить location=column_name, column_metadata_1/2, row_name)
4. Перезапустить: normalized_education → normalized_row_gate → normalized_validation
5. Реализовать transformations/silver_oo.py
6. Реализовать transformations/silver_spo.py
7. Реализовать transformations/silver_vpo.py
8. Реализовать transformations/silver_dpo.py
9. Добавить assets в dagster_project/assets/silver_assets.py
10. Реализовать validation/validate_silver_education.py
11. Запустить все silver assets → education_silver_validation
12. Проверить отчёт: покрытие регионов/возрастов/лет равномерно
```

---

## 8. Что НЕ входит в silver на этом этапе

- Сопоставление годов между источниками — следующий этап
- Gold-агрегаты (охват образованием, динамика YoY) — после Silver validation
- Нормализация возрастных значений в единый справочник — нужна отдельная задача перед Gold

---

## 9. Открытые вопросы для следующего этапа

- Единый справочник возрастов: ОО даёт поштучные года (7 лет, 8 лет...), ДПО даёт корзины (25–29, 30–34...). Нужно ли их приводить к общему виду уже на silver или оставить as-is до Gold?
- Нормализация регионов для PostgreSQL-источников пока не описана — необходимо расширить `region_pipeline.py` или добавить отдельный конфиг для postgres-источников.
