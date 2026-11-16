#!/usr/bin/env bash
set -euo pipefail
umask 077

# ================= ПАРАМЕТРЫ ТЕСТА =================
USERS=30                       # Виртуальные пользователи, по одному запросу на каждого
DURATION_SECONDS=300           # Окно запуска запросов, включая постепенный набор нагрузки
BASE_URL="https://wuxu.ru"      # Open WebUI, НЕ llm.wuxu.ru
MODEL="gpt-6.1-sol"             # Точный ID из: bash run.sh --list-models
RAMP_UP_SECONDS=30             # Время подключения всех пользователей
THINK_MIN_SECONDS=10           # Пауза пользователя после ответа
THINK_MAX_SECONDS=30
MAX_OUTPUT_TOKENS=512          # Для reasoning-моделей может включать токены рассуждений
TOKEN_LIMIT_PARAM="max_completion_tokens"  # Или max_tokens, зависит от модели
REQUEST_TIMEOUT_SECONDS=120    # Полный таймаут одного запроса, включая streaming
MAX_REQUESTS=0                 # 0 = до конца времени; иначе лимит POST ВКЛЮЧАЯ пробный
TURNS_PER_CHAT=3               # Затем начинается новый диалог без старой истории
STOP_AFTER_ERRORS=10           # Остановить новые запросы после N ошибок подряд; 0 = выключено
STREAM=true                   # Streaming как в чате; false = обычный JSON-ответ
INCLUDE_USAGE=true             # Запрос статистики токенов в конце stream
KEYS_FILE=""                   # Необязательно: файл с ключами разных пользователей, по строке
PROMPTS_FILE=""                # Необязательно: JSON-массив своих запросов
# Ключ НЕ вписывайте сюда. Скрипт запросит его скрыто или прочитает WUXU_API_KEY.
# ====================================================

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
PYTHON="${WUXU_PYTHON:-$SCRIPT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON" ]]; then
    echo "Сначала установите окружение:"
    echo "python3 -m venv .venv"
    echo ".venv/bin/python -m pip install -r requirements.txt"
    exit 2
fi

args=(
    --base-url "$BASE_URL" --model "$MODEL"
    --users "$USERS" --duration "$DURATION_SECONDS" --ramp-up "$RAMP_UP_SECONDS"
    --think-min "$THINK_MIN_SECONDS" --think-max "$THINK_MAX_SECONDS"
    --max-output-tokens "$MAX_OUTPUT_TOKENS" --token-limit-param "$TOKEN_LIMIT_PARAM"
    --timeout "$REQUEST_TIMEOUT_SECONDS" --max-requests "$MAX_REQUESTS"
    --turns "$TURNS_PER_CHAT" --stop-after-errors "$STOP_AFTER_ERRORS"
)
[[ "$STREAM" == true ]] || args+=(--no-stream)
[[ "$INCLUDE_USAGE" == true ]] || args+=(--no-usage)
[[ -z "$KEYS_FILE" ]] || args+=(--keys-file "$KEYS_FILE")
[[ -z "$PROMPTS_FILE" ]] || args+=(--prompts-file "$PROMPTS_FILE")
# Аргументы командной строки могут переопределить параметры выше.
exec "$PYTHON" "$SCRIPT_DIR/wuxu_loadtest.py" "${args[@]}" "$@"
