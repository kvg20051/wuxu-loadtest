#!/usr/bin/env python3
"""WUXU Open WebUI inference load test. Python 3.11+, httpx 0.28.1."""
from __future__ import annotations

# Можно менять здесь, если запускаете Python напрямую, без run.sh.
DEFAULT_USERS = 30
DEFAULT_DURATION_SECONDS = 300
DEFAULT_BASE_URL = "https://wuxu.ru"
DEFAULT_MODEL = "pilot-model"

import argparse
import asyncio
import csv
import getpass
import json
import math
import os
from pathlib import Path
import random
import re
import signal
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from uuid import uuid4
from log_files import FileManager, generate_only, parse_sizes

try:
    import httpx
except ImportError:
    raise SystemExit("Установите зависимости: python -m pip install -r requirements.txt")

PROMPTS = [
    "Составь краткий план проверки релиза веб-приложения с PostgreSQL и REST API. Дай 6 пунктов.",
    "Проанализируй синтетические события: 10:00 HTTP 200 80ms; 10:01 HTTP 200 95ms; "
    "10:02 DB pool timeout; 10:02 HTTP 503 5000ms; 10:03 HTTP 200 90ms. "
    "Предложи гипотезу и три проверки, отделяя факты от предположений.",
    "Напиши короткую функцию Python, которая читает JSONL построчно, считает записи уровня ERROR "
    "и отдельно считает строки с некорректным JSON. Поясни обработку ошибок.",
    "Данные за три дня: запросов 1000, 1200, 1500; ошибок 10, 24, 15. "
    "Посчитай долю ошибок по дням и за весь период. Покажи расчёт.",
    "Переработай требование в критерии приёмки: пользователь создаёт заказ, выбирает склад, "
    "добавляет позиции, система проверяет остатки и сохраняет заказ. Ответ краткий.",
    "Сделай краткое резюме записки: запуск пилота в понедельник; 30 сотрудников; "
    "ответственные за доступы — администраторы; результаты собираются неделю; "
    "ошибки регистрируются с временем и идентификатором запроса.",
]
FOLLOWUPS = [
    "Выдели два наиболее важных пункта из своего ответа и объясни почему. Кратко.",
    "Как проверить результат на практике? Дай три конкретных шага.",
]
FIELDS = [
    "request_id", "user_id", "credential_id", "scenario", "started_utc", "offset_s",
    "http_status", "ok", "elapsed_ms", "headers_ms", "ttft_ms", "output_chars",
    "prompt_tokens", "completion_tokens", "total_tokens", "finish_reason",
    "error_kind", "error", "retry_after_s",
    "chat_sent", "file_id", "file_bytes", "generation_ms", "upload_ms", "processing_ms",
    "workflow_ms", "attachment_verified",
]
MAX_RESPONSE_BYTES = 4 * 1024 * 1024


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def redact(value, secrets):
    value = str(value)
    for secret in sorted(set(secrets), key=len, reverse=True):
        if secret:
            value = value.replace(secret, "[KEY HIDDEN]")
    value = re.sub(r"(?i)Bearer\s+[^\s\"',}]+", "Bearer [HIDDEN]", value)
    value = re.sub(r"sk-[A-Za-z0-9_-]+", "[KEY HIDDEN]", value)
    value = re.sub(r"[\x00-\x1f\x7f]", " ", value)
    return value[:600]


def retry_after(value):
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            seconds = 5.0
    return max(0.0, seconds) if math.isfinite(seconds) else 5.0


def usage_counts(data):
    def number(name, fallback=None):
        value = data.get(name, data.get(fallback))
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None
    prompt = number("input_tokens", "prompt_tokens")
    completion = number("output_tokens", "completion_tokens")
    total = number("total_tokens")
    if total is None and prompt is not None and completion is not None:
        total = prompt + completion
    return dict(prompt_tokens=prompt, completion_tokens=completion, total_tokens=total)


def text_content(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(x.get("text", "") for x in value if isinstance(x, dict) and isinstance(x.get("text"), str))
    return ""


class CompletionError(Exception):
    def __init__(self, kind, message):
        self.kind = kind
        super().__init__(message)


async def read_limited(response, limit=MAX_RESPONSE_BYTES):
    chunks = bytearray()
    async for chunk in response.aiter_bytes():
        chunks.extend(chunk)
        if len(chunks) > limit:
            raise CompletionError("response_too_large", "Response exceeded the client size limit")
    return bytes(chunks)


async def sse_events(response):
    """Parse SSE frames, including comments, multiline data and CRLF."""
    parts = []
    size = 0
    async for line in response.aiter_lines():
        size += len(line.encode("utf-8")) + 1
        if size > MAX_RESPONSE_BYTES:
            raise CompletionError("response_too_large", "SSE stream exceeded the client size limit")
        if line == "":
            if parts:
                yield "\n".join(parts)
                parts = []
        elif line.startswith("data:"):
            value = line[5:]
            parts.append(value[1:] if value.startswith(" ") else value)
    if parts:
        yield "\n".join(parts)


async def completion(client, cfg, key, payload, identity, secrets):
    start = time.perf_counter()
    row = {field: None for field in FIELDS}
    row.update(identity, started_utc=utcnow(), ok=False, output_chars=0,
               error_kind="", error="", retry_after_s=0)
    answer = []
    finished = False
    done = False
    try:
        # Absolute deadline, separate from HTTPX connect/read inactivity timeouts.
        async with asyncio.timeout(cfg.timeout):
            async with client.stream(
                "POST", cfg.base_url + "/api/chat/completions", json=payload,
                headers={"Authorization": "Bearer " + key,
                         "User-Agent": "wuxu-loadtest/1.1", "Accept": "text/event-stream, application/json"},
            ) as response:
                row["headers_ms"] = round((time.perf_counter() - start) * 1000, 3)
                row["http_status"] = response.status_code
                if response.status_code != 200:
                    if response.status_code in (429, 503):
                        row["retry_after_s"] = retry_after(response.headers.get("Retry-After"))
                    body = (await read_limited(response, 64 * 1024)).decode("utf-8", errors="replace")
                    raise CompletionError("http_error", f"HTTP {response.status_code}: {body}")
                if cfg.stream:
                    if "text/event-stream" not in response.headers.get("content-type", "").lower():
                        body = (await read_limited(response, 64 * 1024)).decode("utf-8", errors="replace")
                        raise CompletionError("protocol_error", "Expected SSE stream, received: " + body)
                    async for event in sse_events(response):
                        if event.strip() == "[DONE]":
                            done = True
                            break
                        data = json.loads(event)
                        if not isinstance(data, dict):
                            raise CompletionError("protocol_error", "Non-object SSE event")
                        if data.get("error"):
                            raise CompletionError("provider_error", json.dumps(data["error"], ensure_ascii=False))
                        if isinstance(data.get("usage"), dict):
                            row.update(usage_counts(data["usage"]))
                        choices = data.get("choices") or []
                        if choices:
                            choice = choices[0]
                            delta = choice.get("delta") or {}
                            content = text_content(delta.get("content"))
                            if content:
                                if row["ttft_ms"] is None:
                                    row["ttft_ms"] = round((time.perf_counter() - start) * 1000, 3)
                                answer.append(content)
                            if choice.get("finish_reason") is not None:
                                row["finish_reason"] = choice["finish_reason"]
                                finished = True
                    if not (done or finished):
                        raise CompletionError("incomplete_stream", "Connection ended without DONE or finish_reason")
                else:
                    data = json.loads(await read_limited(response))
                    if not isinstance(data, dict):
                        raise CompletionError("protocol_error", "Expected a completion object")
                    if data.get("error"):
                        raise CompletionError("provider_error", json.dumps(data["error"], ensure_ascii=False))
                    choices = data.get("choices") or []
                    if not choices:
                        raise CompletionError("protocol_error", "No choices in completion response")
                    answer.append(text_content((choices[0].get("message") or {}).get("content")))
                    row["finish_reason"] = choices[0].get("finish_reason")
                    if isinstance(data.get("usage"), dict):
                        row.update(usage_counts(data["usage"]))
                if not "".join(answer).strip():
                    raise CompletionError("empty_response", "No visible answer; check output limit, reasoning tokens or model tools")
                row["ok"] = True
    except CompletionError as exc:
        row.update(error_kind=exc.kind, error=redact(exc, secrets))
    except (TimeoutError, httpx.TimeoutException) as exc:
        row.update(error_kind="timeout", error=f"{type(exc).__name__}: request timeout")
    except httpx.HTTPError as exc:
        row.update(error_kind="network_error", error=redact(exc, secrets))
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        row.update(error_kind="protocol_error", error=redact(exc, secrets))
    except asyncio.CancelledError:
        row.update(error_kind="cancelled", error="Interrupted by operator")
    finally:
        row["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 3)
        row["output_chars"] = sum(map(len, answer))
    return row, "".join(answer)


def make_payload(cfg, messages):
    payload = {"model": cfg.model, "messages": messages, "stream": cfg.stream,
               cfg.token_limit_param: cfg.max_output_tokens}
    if cfg.stream and cfg.include_usage:
        payload["stream_options"] = {"include_usage": True}
    return payload


async def models_for_key(client, cfg, key, secrets):
    async with asyncio.timeout(cfg.timeout):
        async with client.stream("GET", cfg.base_url + "/api/models",
                                 headers={"Authorization": "Bearer " + key}) as response:
            raw = await read_limited(response)
            if response.status_code != 200:
                raise ValueError(redact(f"GET /api/models: HTTP {response.status_code}: "
                                       + raw.decode("utf-8", errors="replace"), secrets))
            data = json.loads(raw)
            items = data.get("data") if isinstance(data, dict) else data
            if not isinstance(items, list):
                raise ValueError("GET /api/models вернул неожиданный формат")
            return sorted({item["id"] for item in items if isinstance(item, dict) and isinstance(item.get("id"), str)})


def distribution(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    def p(q):
        return round(values[max(0, math.ceil(len(values) * q) - 1)], 3)
    return {"count": len(values), "mean": round(statistics.mean(values), 3),
            "p50": p(.5), "p95": p(.95), "p99": p(.99), "max": round(values[-1], 3)}


def summarize(rows, elapsed, credentials, users, peak, stop_reason):
    good = [r for r in rows if r["ok"]]
    failures = [r for r in rows if not r["ok"]]
    return {
        "requests": len(rows), "successful": len(good), "failed": len(failures),
        "error_rate_percent": round(100 * len(failures) / len(rows), 3) if rows else None,
        "elapsed_seconds_including_drain": round(elapsed, 3),
        "successful_requests_per_second": round(len(good) / elapsed, 4) if elapsed else 0,
        "configured_virtual_users": users, "distinct_credentials": credentials,
        "users_with_requests": len({r["user_id"] for r in rows}), "peak_inflight_requests": peak,
        "stop_reason": stop_reason,
        "latency_success_ms": distribution([r["elapsed_ms"] for r in good]),
        "latency_all_ms": distribution([r["elapsed_ms"] for r in rows]),
        "ttft_success_ms": distribution([r["ttft_ms"] for r in good]),
        "http_statuses": dict(Counter(str(r["http_status"]) for r in rows)),
        "error_kinds": dict(Counter(r["error_kind"] for r in failures)),
        "usage_reported_requests": sum(r["total_tokens"] is not None for r in rows),
        "reported_tokens_only": {name: sum(r[name] or 0 for r in rows)
                                 for name in ("prompt_tokens", "completion_tokens", "total_tokens")},
        "length_limited_responses": sum(r["finish_reason"] == "length" for r in rows),
        "per_user": {user: {"requests": len(items), "successful": sum(r["ok"] for r in items),
                             "credential_id": items[0]["credential_id"]}
                     for user in sorted({r["user_id"] for r in rows})
                     if (items := [r for r in rows if r["user_id"] == user])},
    }


class Recorder:
    def __init__(self, directory):
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.rows = []
        self.jsonl = (directory / "requests.jsonl").open("x", encoding="utf-8")
        self.csvfile = (directory / "requests.csv").open("x", encoding="utf-8-sig", newline="")
        self.csv = csv.DictWriter(self.csvfile, fieldnames=FIELDS)
        self.csv.writeheader()

    def add(self, row):
        self.rows.append(row)
        self.jsonl.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.jsonl.flush()
        # Do not let spreadsheet software execute an upstream error as a formula.
        csv_row = {k: ("'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@")) else v)
                   for k, v in row.items()}
        self.csv.writerow(csv_row)
        self.csvfile.flush()

    def close(self):
        self.jsonl.close()
        self.csvfile.close()


async def run_load(client, cfg, keys, prompts, run_id, record, stop, files=None):
    start = time.perf_counter()
    deadline = start + cfg.duration
    state = dict(issued=0, inflight=0, peak=0, consecutive_errors=0, reason="duration")

    async def pause(seconds):
        seconds = min(seconds, max(0, deadline - time.perf_counter()))
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def user(index):
        rng = random.Random(cfg.seed + index)
        key_index = index % len(keys)
        identity = {"user_id": f"vu-{index + 1:03}", "credential_id": f"key-{key_index + 1:03}"}
        history = []
        attachment, marker, dialogs = None, None, 0
        await pause(cfg.ramp_up * index / max(1, cfg.users - 1))
        while not stop.is_set() and time.perf_counter() < deadline:
            # No await between checking and reserving: atomic in this event loop.
            if cfg.max_requests and state["issued"] + 1 >= cfg.max_requests:
                state["reason"] = "max_requests"  # +1 accounts for the preflight POST
                stop.set()
                break
            state["issued"] += 1
            sequence = state["issued"]
            if cfg.max_requests and state["issued"] + 1 >= cfg.max_requests:
                state["reason"] = "max_requests"
                stop.set()  # Finish this last reserved request and drain other in-flight ones.
            if len(history) >= cfg.turns * 2:
                history = []
            workflow_start = time.perf_counter()
            new_file = None
            if not history:
                attachment, marker = None, None
                if files and dialogs % cfg.log_every_dialogs == 0 and files.available():
                    new_file, marker = await files.prepare(key_index, identity["user_id"], index + dialogs)
                    if new_file["ok"]:
                        attachment = new_file
                dialogs += 1
            turn = len(history) // 2
            scenario = rng.randrange(len(prompts)) if not history else -1
            prompt = prompts[scenario] if not history else FOLLOWUPS[(turn - 1) % len(FOLLOWUPS)]
            if attachment and not history:
                prompt = ("Проанализируй прикреплённый log-файл. Найди примеры DB_POOL_TIMEOUT, HTTP 503 "
                          "и UPSTREAM_RETRY. Кратко опиши наблюдения и предложи проверки. "
                          "Не выдавай найденные фрагменты за статистику всего файла.")
                if marker and cfg.verify_file:
                    prompt += " Найди в файле поле WUXU_CHECK и обязательно приведи его значение целиком, без изменений."
            request_id = f"{run_id}-{identity['user_id']}-{sequence:06}"
            prompt += f"\nИдентификатор тестового запроса: {request_id}. Не включай его в ответ."
            message = {"role": "user", "content": prompt}
            payload = make_payload(cfg, history + [message])
            if attachment:
                payload["files"] = [{"type": "file", "id": attachment["file_id"]}]
            current = dict(identity, request_id=request_id, scenario=f"prompt-{scenario + 1}" if scenario >= 0 else "followup",
                           offset_s=round(workflow_start - start, 3))
            if new_file and not new_file["ok"]:
                row = dict.fromkeys(FIELDS)
                row.update(current, started_utc=utcnow(), ok=False, output_chars=0,
                           http_status=new_file["http_status"], error_kind=new_file["error_kind"],
                           error=new_file["error"], retry_after_s=new_file["retry_after_s"], chat_sent=False)
                answer = ""
            else:
                state["inflight"] += 1
                state["peak"] = max(state["peak"], state["inflight"])
                try:
                    row, answer = await completion(client, cfg, keys[key_index], payload, current, keys)
                    row["chat_sent"] = True
                finally:
                    state["inflight"] -= 1
            if new_file or attachment:
                attached = new_file or attachment
                row.update(file_id=attached["file_id"], file_bytes=attached["bytes"], scenario="file-chat" if not history else "file-followup")
                if new_file:
                    row.update({name: new_file[name] for name in ("generation_ms", "upload_ms", "processing_ms")})
                if new_file and new_file["ok"] and marker and cfg.verify_file and row["ok"]:
                    row["attachment_verified"] = marker in answer
                    if not row["attachment_verified"]:
                        row.update(ok=False, error_kind="file_content_not_verified",
                                   error="Модель не вернула WUXU_CHECK из файла. Проверьте RAG/Preload Files у модели.")
            row["workflow_ms"] = round((time.perf_counter() - workflow_start) * 1000, 3)
            record.add(row)
            if row["ok"]:
                # Bound retained history even if a provider ignores max_output_tokens.
                history += [message, {"role": "assistant", "content": answer[:16000]}]
                state["consecutive_errors"] = 0
            else:
                state["consecutive_errors"] += 1
                if row["error_kind"] == "cancelled":
                    stop.set()
                if row["http_status"] in (401, 403, 413):
                    state["reason"] = "authentication_or_permission_error"
                    if row["http_status"] == 413:
                        state["reason"] = "file_too_large"
                    stop.set()
                elif cfg.stop_after_errors and state["consecutive_errors"] >= cfg.stop_after_errors:
                    state["reason"] = "consecutive_errors"
                    stop.set()
            await pause(max(rng.uniform(cfg.think_min, cfg.think_max), row["retry_after_s"] or 0))

    tasks = [asyncio.create_task(user(index)) for index in range(cfg.users)]
    try:
        while any(not task.done() for task in tasks):
            _, pending = await asyncio.wait(tasks, timeout=10, return_when=asyncio.ALL_COMPLETED)
            if pending:
                ok = sum(r["ok"] for r in record.rows)
                print(f"{time.perf_counter()-start:6.1f}s  requests={len(record.rows)}  "
                      f"ok={ok}  errors={len(record.rows)-ok}  active_requests={state['inflight']}", flush=True)
                if files:
                    print(f"  files={len(files.entries)}  accepted={sum(e['upload_accepted'] for e in files.entries)}  "
                          f"processed={sum(e['ok'] for e in files.entries)}", flush=True)
            # Propagate unexpected programming errors instead of silently losing a worker.
            for task in tasks:
                if task.done() and not task.cancelled() and task.exception():
                    raise task.exception()
        await asyncio.gather(*tasks)
    except BaseException:
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    elapsed = time.perf_counter() - start
    if stop.is_set() and state["reason"] == "duration":
        state["reason"] = "operator_stop"
    summary = summarize(record.rows, elapsed, len(keys), cfg.users, state["peak"], state["reason"])
    summary.update(chat_posts=sum(bool(r.get("chat_sent")) for r in record.rows),
                   workflow_success_ms=distribution([r.get("workflow_ms") for r in record.rows if r["ok"]]),
                   verified_file_answers=sum(r.get("attachment_verified") is True for r in record.rows))
    return summary


def write_report(directory, summary, cfg, probe):
    document = dict(summary, preflight=probe)
    (directory / "summary.json").write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    def metric(value):
        return "нет данных" if value is None else str(value)
    latency, ttft = summary["latency_success_ms"], summary["ttft_success_ms"]
    lines = ["# WUXU — результат нагрузочного теста", "",
             f"- Endpoint: {cfg.base_url}/api/chat/completions",
             f"- Модель: `{cfg.model}`", f"- Пользователи: {cfg.users}; разных ключей: {summary['distinct_credentials']}",
             f"- Плановое окно начала сценариев: {cfg.duration} с; с ожиданием их завершения: {summary['elapsed_seconds_including_drain']} с",
             f"- Завершение: {summary['stop_reason']}",
             f"- Сценарии нагрузки: {summary['requests']}; успешно: {summary['successful']}; ошибок: {summary['failed']}",
             f"- Ошибок: {metric(summary['error_rate_percent'])}%",
             f"- Пиковое число одновременных запросов: {summary['peak_inflight_requests']}",
             f"- Успешных запросов/с (включая ожидание последних ответов): {summary['successful_requests_per_second']}", "",
             "| Метрика, мс — успешные запросы | p50 | p95 | p99 | max |",
             "|---|---:|---:|---:|---:|"]
    for title, values in (("Полный ответ от chat POST", latency), ("Первый видимый текст (TTFT)", ttft),
                          ("Сценарий целиком", summary.get("workflow_success_ms", distribution([])))):
        lines.append("| " + title + " | " + " | ".join(metric(values[x]) for x in ("p50", "p95", "p99", "max")) + " |")
    lines += ["", f"Токены получены для {summary['usage_reported_requests']} из {summary['requests']} запросов нагрузки.",
              "Отсутствующие usage не считаются нулевым расходом. Известные суммы: " + json.dumps(summary["reported_tokens_only"]),
              f"Ответов с finish_reason=length: {summary['length_limited_responses']}.",
              "Пробный POST до нагрузки приведён отдельно в summary.json; он также расходует токены.",
              "Расход денег проверяйте в LiteLLM: оценки стоимости в этом отчёте нет.", "",
              f"Chat POST нагрузки: {summary.get('chat_posts', summary['requests'])}. Ошибка загрузки не запускает chat POST.",
              "Это тест API с паузами, историей и опциональной загрузкой файлов/RAG. Он не проверяет браузер, вход/2FA "
              "или сохранение чатов. Один ключ означает одну учётную запись.", "",
              "При росте задержек пользователи этого теста ждут ответа, поэтому поток новых запросов снижается "
              "(closed-loop). Результат не определяет максимальный RPS системы.", ""]
    errors = Counter((r["error_kind"], r["error"]) for r in getattr(cfg, "error_rows", []))
    for (kind, message), count in errors.most_common(5):
        lines.append(f"- {count} × {kind}: {message}")
    if "files" in summary:
        lines += ["", "## Файлы", "", "```json", json.dumps(summary["files"], ensure_ascii=False, indent=2), "```",
                  "Время workflow включает генерацию, загрузку, ожидание обработки и ответ модели. "
                  "Обычные latency/TTFT отсчитываются от chat POST. files.json содержит ID и результат очистки.",
                  "Проверка WUXU_CHECK подтверждает получение фрагмента файла, а не прочтение всех строк. "
                  "Токены embeddings в статистику ответов чата не входят."]
    (directory / "report.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    parser.add_argument("--duration", type=float, default=DEFAULT_DURATION_SECONDS)
    parser.add_argument("--ramp-up", type=float, default=30)
    parser.add_argument("--think-min", type=float, default=10)
    parser.add_argument("--think-max", type=float, default=30)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--max-output-tokens", type=int, default=512)
    parser.add_argument("--token-limit-param", choices=["max_tokens", "max_completion_tokens"], default="max_completion_tokens")
    parser.add_argument("--max-requests", type=int, default=0)
    parser.add_argument("--turns", type=int, default=3)
    parser.add_argument("--stop-after-errors", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keys-file", type=Path)
    parser.add_argument("--prompts-file", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-stream", action="store_false", dest="stream", default=True)
    parser.add_argument("--no-usage", action="store_false", dest="include_usage", default=True)
    parser.add_argument("--list-models", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--smoke", action="store_true", help="2 пользователя, 20 с, максимум 3 сценария + 1 пробный chat POST")
    parser.add_argument("--upload-logs", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--log-sizes-mb", default="1")
    parser.add_argument("--log-files", nargs="+", type=Path, default=[])
    parser.add_argument("--log-every-dialogs", type=int, default=1)
    parser.add_argument("--max-uploads", type=int, default=30, help="0 = без ограничения; иначе общий лимит файлов")
    parser.add_argument("--upload-timeout", type=float, default=300)
    parser.add_argument("--processing-timeout", type=float, default=600)
    parser.add_argument("--file-poll-interval", type=float, default=2)
    parser.add_argument("--cleanup-files", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--verify-file", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--generate-only", action="store_true", help="Создать реальные .log локально; без ключа/сети")
    parser.add_argument("--log-dir", type=Path, default=Path("generated-logs"))
    args = parser.parse_args(argv)
    args.base_url = args.base_url.rstrip("/")
    url = httpx.URL(args.base_url)
    if url.scheme not in ("https", "http") or not url.host or url.username or url.password or url.query or url.fragment:
        parser.error("base-url должен быть URL без логина, пароля, query и fragment")
    if url.scheme == "http" and url.host not in ("localhost", "127.0.0.1", "::1"):
        parser.error("Для удалённого сервера требуется HTTPS")
    if args.smoke:
        args.users, args.duration, args.ramp_up = 2, 20, 1
        args.think_min, args.think_max, args.max_requests = 1, 2, 4
    if not 1 <= args.users <= 1000:
        parser.error("users: 1..1000")
    if not all(math.isfinite(v) for v in (args.duration, args.ramp_up, args.think_min, args.think_max, args.timeout)):
        parser.error("Временные параметры должны быть конечными числами")
    if args.duration <= 0 or not 0 <= args.ramp_up < args.duration:
        parser.error("duration > 0; 0 <= ramp-up < duration")
    if not 0 <= args.think_min <= args.think_max or args.timeout <= 0:
        parser.error("0 <= think-min <= think-max; timeout > 0")
    if args.max_requests < 0 or args.max_requests == 1 or args.stop_after_errors < 0 or args.turns < 1 or args.max_output_tokens < 1:
        parser.error("max-requests: 0 или >=2; stop-after-errors >=0; turns и max-output-tokens >=1")
    if not args.model.strip() and not args.list_models:
        parser.error("Укажите --model (точный ID модели Open WebUI)")
    try:
        args.log_sizes_bytes = parse_sizes(args.log_sizes_mb)
    except ValueError as exc:
        parser.error(str(exc))
    if args.log_every_dialogs < 1 or args.max_uploads < 0:
        parser.error("log-every-dialogs >= 1; max-uploads >= 0")
    if any(not math.isfinite(v) or v <= 0 for v in (args.upload_timeout, args.processing_timeout, args.file_poll_interval)):
        parser.error("Таймауты файлов и file-poll-interval должны быть конечными числами > 0")
    for path in args.log_files:
        if not path.is_file() or path.stat().st_size == 0 or path.suffix.lower() != ".log":
            parser.error(f"Нужен существующий непустой .log: {path}")
    return args


def read_keys(cfg):
    if cfg.keys_file:
        raw = cfg.keys_file.read_text(encoding="utf-8-sig").splitlines()
        keys = [line.strip() for line in raw if line.strip() and not line.lstrip().startswith("#")]
    else:
        value = os.environ.get("WUXU_API_KEY")
        if not value:
            if not sys.stdin.isatty():
                raise ValueError("Нет терминала для скрытого ввода. Задайте WUXU_API_KEY или --keys-file.")
            value = getpass.getpass("Ключ Open WebUI (ввод скрыт): ")
        keys = [value.strip()]
    if not keys or any(not key or any(c.isspace() or ord(c) < 32 for c in key) for key in keys):
        raise ValueError("Пустой ключ или пробельные символы внутри ключа")
    return list(dict.fromkeys(keys))


async def main_async(cfg, keys):
    prompts = PROMPTS
    if cfg.prompts_file:
        prompts = json.loads(cfg.prompts_file.read_text(encoding="utf-8-sig"))
        if not isinstance(prompts, list) or not prompts or any(not isinstance(p, str) or not p.strip() for p in prompts):
            raise ValueError("prompts-file: нужен непустой JSON-массив строк")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    timeout = httpx.Timeout(cfg.timeout, connect=min(10, cfg.timeout))
    limits = httpx.Limits(max_connections=cfg.users, max_keepalive_connections=cfg.users)
    # TLS verification on; no redirects, no HTTP transport retries, no implicit proxy from env.
    async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=False, trust_env=False) as client:
        for index, key in enumerate(keys):
            models = await models_for_key(client, cfg, key, keys)
            if cfg.list_models:
                print(f"key-{index+1:03}:\n" + "\n".join(models))
            elif cfg.model not in models:
                raise ValueError(f"Модель {cfg.model!r} недоступна key-{index+1:03}. Запустите --list-models.")
        if cfg.list_models:
            return 0
        directory = cfg.output or Path("results") / run_id
        record = Recorder(directory)
        stop = asyncio.Event()
        previous = {}
        def on_signal(signum, frame):
            if not stop.is_set():
                print("\nОстановка новых запросов; ожидаю текущие до их таймаута...", flush=True)
            stop.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, on_signal)
        probe = None
        files = FileManager(client, cfg, keys, directory, redact) if cfg.upload_logs else None
        try:
            settings = {k: getattr(cfg, k) for k in (
                "base_url", "model", "users", "duration", "ramp_up", "think_min", "think_max", "timeout",
                "max_output_tokens", "token_limit_param", "max_requests", "turns", "stop_after_errors", "stream", "include_usage", "seed",
                "upload_logs", "log_sizes_bytes", "log_every_dialogs", "max_uploads", "upload_timeout", "processing_timeout",
                "file_poll_interval", "cleanup_files", "verify_file")}
            settings["log_files"] = [str(p) for p in cfg.log_files]
            settings.update(run_id=run_id, distinct_credentials=len(keys), created_utc=utcnow())
            (directory / "settings.json").write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"Endpoint: {cfg.base_url}/api/chat/completions\nModel: {cfg.model}\n"
                  f"Users: {cfg.users}; credentials: {len(keys)}; duration: {cfg.duration}s\n"
                  f"Results: {directory.resolve()}", flush=True)
            if len(keys) == 1:
                print("Все виртуальные пользователи работают от ОДНОЙ учётной записи Open WebUI.")
            elif len(keys) != cfg.users:
                print("Ключи распределяются по пользователям циклически; часть ключей может не использоваться.")
            print("Проверяю один реальный ответ модели перед нагрузкой...", flush=True)
            probe, _ = await completion(client, cfg, keys[0], make_payload(cfg, [
                {"role": "user", "content": "Ответь одним коротким предложением: соединение работает. " + run_id}
            ]), {"request_id": run_id + "-preflight", "user_id": "preflight", "credential_id": "key-001",
                 "scenario": "preflight", "offset_s": None}, keys)
            (directory / "preflight.json").write_text(json.dumps(probe, ensure_ascii=False, indent=2), encoding="utf-8")
            if not probe["ok"]:
                print("Пробный запрос не прошёл; нагрузка не запущена: " + probe["error"], file=sys.stderr)
                return 2
            if files:
                print(f"Files: sizes={cfg.log_sizes_mb} MB; own_files={len(cfg.log_files)}; max_uploads={cfg.max_uploads}; "
                      f"cleanup={cfg.cleanup_files}. Начатый сценарий завершается целиком, даже после окна нагрузки.", flush=True)
            summary = await run_load(client, cfg, keys, prompts, run_id, record, stop, files)
            if files:
                print("Завершаю обработку отчёта и очистку файлов этого запуска...", flush=True)
                await files.cleanup()
                entries = files.entries
                summary["files"] = {
                    "attempts": len(entries), "accepted": sum(e["upload_accepted"] for e in entries),
                    "processed": sum(e["ok"] for e in entries), "failed": sum(not e["ok"] for e in entries),
                    "accepted_file_bytes": sum(e["bytes"] for e in entries if e["upload_accepted"]),
                    "upload_ms": distribution([e["upload_ms"] for e in entries if e["upload_accepted"]]),
                    "processing_ms": distribution([e["processing_ms"] for e in entries if e["ok"]]),
                    "cleanup": dict(Counter(e["cleanup"] for e in entries)),
                    "cleanup_errors": sum(e["cleanup"] == "failed" for e in entries),
                }
            cfg.error_rows = [r for r in record.rows if not r["ok"]]
            write_report(directory, summary, cfg, probe)
            print(json.dumps({k: summary[k] for k in ("requests", "successful", "failed", "stop_reason",
                                                    "peak_inflight_requests", "latency_success_ms", "ttft_success_ms")}, ensure_ascii=False, indent=2))
            print(f"Отчёт: {(directory / 'report.md').resolve()}")
            if summary["stop_reason"] == "operator_stop":
                return 130
            return 0 if summary["requests"] > 0 and summary["failed"] == 0 and not summary.get("files", {}).get("cleanup_errors") else 1
        finally:
            if files:
                files.close()
            record.close()
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def main(argv=None):
    if sys.version_info < (3, 11):
        raise SystemExit("Нужен Python 3.11 или новее")
    os.umask(0o077)
    cfg = parse_args(argv)
    if cfg.dry_run:
        print(json.dumps(vars(cfg), ensure_ascii=False, indent=2, default=str))
        print("Dry run: сетевых запросов не было; ключ не требуется.")
        return 0
    keys = []
    try:
        if cfg.generate_only:
            return generate_only(cfg)
        keys = read_keys(cfg)
        return asyncio.run(main_async(cfg, keys))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print("Ошибка: " + redact(f"{type(exc).__name__}: {exc}", keys), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
