# ТЗ: Интеграция coverage_dashboard в Dagster-узел с выгрузкой HTML в S3

## 1. Контекст

В пайплайне Dagster существует asset `educationpopulationwideannualsilver`, который:

- Генерирует таблицы `silver.educationpopulationwide` и `silver.educationpopulationwideannual` в Apache Iceberg;
- Вызывает `validate_coverage_analysis.run(cat)`, сохраняя Markdown-отчёт в `reports/`;
- Возвращает в Dagster UI metadata: `totalrows`, `reportpath`, `coveragereportpath`.

Существует отдельный скрипт `coverage_dashboard.py`, который принимает два CSV-файла (wide и tidy) и генерирует интерактивный HTML-дашборд на Plotly.

Задача: интегрировать генерацию HTML-дашборда в Dagster-узел, загружать HTML в локальный S3/MinIO и возвращать URL в metadata узла.

---

## 2. Цель

После выполнения asset `educationpopulationwideannualsilver` в Dagster UI в разделе **Metadata** должен появляться кликабельный URL вида:

```
http://<minio-host>:<port>/<bucket>/dashboards/coverage/YYYY-MM-DD_<run_id>.html
```

---

## 3. Изменения в коде

### 3.1 Рефакторинг `coverage_dashboard.py`

Выделить публичную функцию, пригодную для вызова из Dagster:

```python
def build_dashboard(
    wide_df: pd.DataFrame,
    tidy_df: pd.DataFrame,
    output_path: str,
) -> str:
    """
    Принимает DataFrames вместо CSV-файлов,
    записывает HTML в output_path,
    возвращает output_path.
    """
    ...
    return output_path
```

- Убрать жёсткие константы `WIDECSV`, `TIDYCSV`, `OUTPUT` из модульного уровня — перенести в `main()`.
- Оставить `main()` рабочим как CLI-скрипт.
- `build_dashboard` не делает никаких `sys.exit` и не читает файлы с диска — только принимает DataFrames.

**Где находится:** `coverage_dashboard.py` в корне или в `validation/` (уточнить по структуре проекта).

---

### 3.2 Новый модуль `utils/s3_utils.py`

```python
import boto3
from botocore.client import Config

def get_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )

def upload_html(
    client,
    bucket: str,
    key: str,
    local_path: str,
    public_base_url: str,
) -> str:
    """
    Загружает HTML-файл в S3, возвращает публичный URL.
    ContentType выставляется text/html; charset=utf-8.
    """
    with open(local_path, "rb") as f:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=f,
            ContentType="text/html; charset=utf-8",
        )
    return f"{public_base_url.rstrip('/')}/{bucket}/{key}"
```

---

### 3.3 Конфигурация S3 через Dagster `EnvVar` / ресурс

Добавить в `dagsterproject/resources.py` (или `definitions.py`) ресурс:

```python
from dagster import ConfigurableResource, EnvVar

class S3Config(ConfigurableResource):
    endpoint_url: str = EnvVar("S3_ENDPOINT_URL")       # http://localhost:9000
    access_key: str   = EnvVar("S3_ACCESS_KEY")
    secret_key: str   = EnvVar("S3_SECRET_KEY")
    bucket: str       = EnvVar("S3_DASHBOARD_BUCKET")   # например: dashboards
    public_base_url: str = EnvVar("S3_PUBLIC_BASE_URL") # http://localhost:9000
```

Переменные окружения добавить в `.env` (не в коммит) и прокинуть в `docker-compose.yml` при необходимости.

---

### 3.4 Изменение asset `educationpopulationwideannualsilver`

Файл: `dagsterproject/assets/silverassets.py`

#### Добавить импорты

```python
import tempfile
import os
from datetime import date
from dagster import MetadataValue
from coverage_dashboard import build_dashboard
from utils.s3_utils import get_s3_client, upload_html
```

#### Добавить S3Config как зависимость ресурса

```python
@asset(
    ...
    required_resource_keys={"s3_config"},
)
def educationpopulationwideannualsilver(context, ...):
```

или через аргумент ресурса (современный стиль):

```python
def educationpopulationwideannualsilver(context, s3_config: S3Config, ...):
```

#### Добавить генерацию дашборда в тело asset (после записи таблиц)

```python
# --- Coverage dashboard ---
# 1. Подготовить DataFrames для дашборда
wide_df  = ...   # запрос к silver.educationpopulationwideannual (wide-формат: регион × возраст)
tidy_df  = ...   # delta-версия или тidy-формат для второго графика

# 2. Собрать HTML
run_id    = context.run_id[:8]
date_str  = date.today().isoformat()
html_name = f"coverage_{date_str}_{run_id}.html"

with tempfile.TemporaryDirectory() as tmpdir:
    local_html = os.path.join(tmpdir, html_name)
    build_dashboard(wide_df, tidy_df, local_html)

    # 3. Загрузить в S3
    s3 = get_s3_client(
        s3_config.endpoint_url,
        s3_config.access_key,
        s3_config.secret_key,
    )
    s3_key = f"dashboards/coverage/{date_str}/{html_name}"
    dashboard_url = upload_html(
        s3, s3_config.bucket, s3_key, local_html, s3_config.public_base_url
    )

# 4. Вернуть metadata
return MaterializeResult(
    metadata={
        "totalrows":          MetadataValue.int(total_rows),
        "reportpath":         MetadataValue.path(report_path),
        "coveragereportpath": MetadataValue.path(coverage_report_path),
        "dashboard_url":      MetadataValue.url(dashboard_url),
        "dashboard_s3_key":   MetadataValue.text(s3_key),
    }
)
```

---

## 4. Требования к S3/MinIO

| Параметр | Значение |
|---|---|
| Бакет | `dashboards` (или по конфигурации) |
| Policy | публичный read (anonymous GET) или presigned URL |
| ContentType | `text/html; charset=utf-8` |
| Структура ключей | `dashboards/coverage/YYYY-MM-DD/coverage_YYYY-MM-DD_<run_id_8>.html` |

Если бакет private — заменить `upload_html` на функцию, генерирующую presigned URL с TTL (например, 7 дней):

```python
url = client.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": key},
    ExpiresIn=604800,  # 7 дней
)
```

---

## 5. Переменные окружения

Добавить в `.env`:

```dotenv
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_DASHBOARD_BUCKET=dashboards
S3_PUBLIC_BASE_URL=http://localhost:9000
```

---

## 6. Зависимости

Добавить в `requirements.txt`:

```
boto3>=1.34
```

---

## 7. Как это выглядит в Dagster UI

После успешного запуска asset-а в разделе **Asset details → Materializations → Metadata**:

| Ключ | Тип | Значение |
|---|---|---|
| `totalrows` | int | 993 |
| `reportpath` | path | reports/educationpopulationwideannual-2026-04-15.md |
| `coveragereportpath` | path | reports/coverageanalysis-2026-04-15.md |
| `dashboard_url` | **url** | http://localhost:9000/dashboards/coverage/... ← кликабельная ссылка |
| `dashboard_s3_key` | text | dashboards/coverage/2026-04-15/coverage_2026-04-15_a1b2c3d4.html |

Ключ типа `MetadataValue.url` Dagster рендерит как гиперссылку, по которой открывается HTML-дашборд в браузере.

---

## 8. Порядок реализации

1. Рефакторинг `coverage_dashboard.py` — выделить `build_dashboard(wide_df, tidy_df, output_path)`.
2. Создать `utils/s3_utils.py`.
3. Добавить `S3Config` ресурс в `definitions.py`.
4. Прокинуть ресурс в `educationpopulationwideannualsilver`.
5. Добавить логику генерации и загрузки HTML в тело asset-а.
6. Обновить `requirements.txt`.
7. Проверить MinIO: бакет существует, политика позволяет GET по URL.
8. Тест: запустить asset, проверить metadata в Dagster UI, открыть ссылку.

---

## 9. Что НЕ меняется

- Логика трансформации таблиц `silver.educationpopulationwide` и `silver.educationpopulationwideannual` — без изменений.
- `validate_coverage_analysis.py` — без изменений.
- Markdown-отчёты продолжают генерироваться как раньше.
- CLI-запуск `python coverage_dashboard.py` продолжает работать.

