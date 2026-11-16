#!/usr/bin/env bash
set -euo pipefail
umask 077

# ================= ПАРАМЕТРЫ ТЕСТА =================
USERS=30                       # Виртуальные пользователи, по одному запросу на каждого
DURATION_SECONDS=300           # Окно запуска запросов, включая постепенный набор нагрузки
BASE_URL="https://wuxu.ru"      # Open WebUI, НЕ llm.wuxu.ru
MODEL="pilot-model"             # Точный ID из: bash run.sh --list-models
RAMP_UP_SECONDS=30             # Время подключения всех пользователей
THINK_MIN_SECONDS=10           # Пауза пользователя после ответа
THINK_MAX_SECONDS=30
MAX_OUTPUT_TOKENS=512          # Для reasoning-моделей может включать токены рассуждений
TOKEN_LIMIT_PARAM="max_completion_tokens"  # Или max_tokens, зависит от модели
REQUEST_TIMEOUT_SECONDS=120    # Полный таймаут одного запроса, включая streaming
MAX_REQUESTS=0                 # 0 = по времени; иначе лимит сценариев + 1 пробный chat POST
TURNS_PER_CHAT=3               # Затем начинается новый диалог без старой истории
STOP_AFTER_ERRORS=10           # Остановить новые запросы после N ошибок подряд; 0 = выключено
STREAM=true                   # Streaming как в чате; false = обычный JSON-ответ
INCLUDE_USAGE=true             # Запрос статистики токенов в конце stream
KEYS_FILE=""                   # Необязательно: файл с ключами разных пользователей, по строке
PROMPTS_FILE=""                # Необязательно: JSON-массив своих запросов

# ФАЙЛЫ: 1 MB = 1 000 000 байт. Можно указать "1,10,100" или "300".
UPLOAD_LOGS=true
LOG_FILE_SIZES_MB="1"          # Размеры сгенерированных .log, чередуются между пользователями/диалогами
LOG_FILES=()                   # Свои файлы: ("/home/kvg/app.log" "/home/kvg/other.log") вместо генерации
LOG_UPLOAD_EVERY_DIALOGS=1     # Прикреплять новый файл в каждом N-м диалоге, начиная с первого
MAX_UPLOADS=30                 # Общий лимит файлов на запуск; 0 = без ограничения
UPLOAD_TIMEOUT_SECONDS=300
FILE_PROCESSING_TIMEOUT_SECONDS=600
FILE_POLL_SECONDS=2
CLEANUP_UPLOADED_FILES=true    # Удалить только файлы ЭТОГО запуска после завершения; pending оставить
VERIFY_FILE_CONTENT=true       # Проверить контрольную метку в первом ответе по синтетическому логу
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
    --log-sizes-mb "$LOG_FILE_SIZES_MB" --log-every-dialogs "$LOG_UPLOAD_EVERY_DIALOGS"
    --max-uploads "$MAX_UPLOADS" --upload-timeout "$UPLOAD_TIMEOUT_SECONDS"
    --processing-timeout "$FILE_PROCESSING_TIMEOUT_SECONDS" --file-poll-interval "$FILE_POLL_SECONDS"
)
[[ "$UPLOAD_LOGS" == true ]] || args+=(--no-upload-logs)
[[ "$CLEANUP_UPLOADED_FILES" == true ]] || args+=(--no-cleanup-files)
[[ "$VERIFY_FILE_CONTENT" == true ]] || args+=(--no-verify-file)
if (( ${#LOG_FILES[@]} > 0 )); then args+=(--log-files "${LOG_FILES[@]}"); fi
[[ "$STREAM" == true ]] || args+=(--no-stream)
[[ "$INCLUDE_USAGE" == true ]] || args+=(--no-usage)
[[ -z "$KEYS_FILE" ]] || args+=(--keys-file "$KEYS_FILE")
[[ -z "$PROMPTS_FILE" ]] || args+=(--prompts-file "$PROMPTS_FILE")
# Аргументы командной строки могут переопределить параметры выше.
exec "$PYTHON" "$SCRIPT_DIR/wuxu_loadtest.py" "${args[@]}" "$@"
