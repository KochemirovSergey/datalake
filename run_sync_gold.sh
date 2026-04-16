#!/bin/bash
#
# Скрипт для синхронизации Gold-слоя: DuckDB → PostgreSQL + KG.md
#
# Использование:
#   ./run_sync_gold.sh [опции]
#
# Примеры:
#   ./run_sync_gold.sh                    # Полная синхронизация
#   ./run_sync_gold.sh --skip-postgres    # Только обновить KG.md
#   ./run_sync_gold.sh --skip-kg          # Только мигрировать в PostgreSQL
#   ./run_sync_gold.sh --help             # Справка

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ============================================================================
# ПАРАМЕТРЫ ПОДКЛЮЧЕНИЯ К БАЗАМ ДАННЫХ
# ============================================================================

# DuckDB
DUCKDB_PATH="${DUCKDB_PATH:-datalake.duckdb}"

# PostgreSQL
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-etl_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-etl_password}"
POSTGRES_DB="${POSTGRES_DB:-kg_db}"

# KG.md
KG_PATH="${KG_PATH:-KG.md}"

# ============================================================================
# ЦВЕТА ДЛЯ ВЫВОДА
# ============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Функция для вывода
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Проверка Python
if ! command -v python3 &> /dev/null; then
    log_error "Python 3 не найден"
    exit 1
fi

log_info "Версия Python: $(python3 --version)"

# Проверка зависимостей
log_info "Проверка зависимостей..."

python3 << 'EOF'
import sys
try:
    import duckdb
    print("  ✓ duckdb")
except ImportError:
    print("  ✗ duckdb НЕ установлен", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg2
    print("  ✓ psycopg2")
except ImportError:
    print("  ✗ psycopg2 НЕ установлен", file=sys.stderr)
    sys.exit(1)
EOF

if [ $? -ne 0 ]; then
    log_error "Не все зависимости установлены"
    echo ""
    echo "Установите их:"
    echo "  pip install duckdb psycopg2-binary"
    exit 1
fi

log_success "Все зависимости установлены"

# Проверка файлов
log_info "Проверка файлов модуля..."

if [ ! -f "sync_gold_to_postgres_and_kg.py" ]; then
    log_error "sync_gold_to_postgres_and_kg.py не найден"
    exit 1
fi

if [ ! -d "gold_sync" ]; then
    log_error "Директория gold_sync не найдена"
    exit 1
fi

log_success "Все файлы на месте"

# Запуск основного скрипта
log_info "Запуск синхронизации Gold-слоя..."
log_info "Параметры подключения:"
log_info "  DuckDB: $DUCKDB_PATH"
log_info "  PostgreSQL: $POSTGRES_USER@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
log_info "  KG.md: $KG_PATH"
echo ""

python3 sync_gold_to_postgres_and_kg.py \
  --duckdb-path "$DUCKDB_PATH" \
  --postgres-host "$POSTGRES_HOST" \
  --postgres-port "$POSTGRES_PORT" \
  --postgres-user "$POSTGRES_USER" \
  --postgres-password "$POSTGRES_PASSWORD" \
  --postgres-db "$POSTGRES_DB" \
  --kg-path "$KG_PATH" \
  "$@"

if [ $? -eq 0 ]; then
    echo ""
    log_success "Синхронизация завершена!"
else
    log_error "Ошибка при синхронизации"
    exit 1
fi
