"""Real log generation, multipart upload and processing for Open WebUI."""
from __future__ import annotations

import asyncio
import csv
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import time
from urllib.parse import quote
from uuid import uuid4

import httpx


def parse_sizes(value):
    try:
        numbers = [Decimal(part.strip()) * 1_000_000 for part in value.split(",")]
        if not numbers or any(not n.is_finite() or n < 256 or n != int(n) for n in numbers):
            raise ValueError
        return [int(n) for n in numbers]
    except (InvalidOperation, ValueError, OverflowError):
        raise ValueError("log-sizes-mb: размеры через запятую, минимум 0.000256 MB; 1 MB = 1 000 000 байт") from None


def generate_log(path, size, marker):
    """Write exactly size bytes; bounded ~1 MiB buffer, unique log lines, ASCII/UTF-8."""
    if size < 256:
        raise ValueError("Минимальный размер: 256 байт")
    rows, tail = divmod(size, 256)
    with Path(path).open("xb") as target:
        for start in range(0, rows, 4096):
            block = bytearray()
            for number in range(start, min(start + 4096, rows)):
                level, event = ("ERROR", "DB_POOL_TIMEOUT HTTP=503 duration_ms=5000") if number % 17 == 0 else (
                    ("WARN", "UPSTREAM_RETRY HTTP=429 duration_ms=800") if number % 11 == 0 else
                    ("INFO", "REQUEST_OK HTTP=200 duration_ms=85"))
                line = (f"2026-11-16T12:{number // 60 % 60:02}:{number % 60:02}Z {level} "
                        f"service=wuxu-test event_id={number:012} WUXU_CHECK={marker} {event} synthetic=true")
                block.extend(line.encode("ascii").ljust(255, b" ") + b"\n")
            target.write(block)
        if tail:
            target.write(b" " * (tail - 1) + b"\n")
    return Path(path)


def generate_only(cfg):
    directory = cfg.log_dir / ("logs-" + uuid4().hex[:12])
    directory.mkdir(parents=True, exist_ok=False)
    for index, size in enumerate(cfg.log_sizes_bytes):
        path = generate_log(directory / f"sample-{index+1}-{size}bytes.log", size, uuid4().hex)
        print(f"{path.resolve()}  {path.stat().st_size} bytes")
    return 0


class FileError(Exception):
    def __init__(self, kind, message, status=None, retry=0):
        super().__init__(message)
        self.kind, self.status, self.retry = kind, status, retry


class FileManager:
    def __init__(self, client, cfg, keys, directory, redact):
        self.client, self.cfg, self.keys = client, cfg, keys
        self.directory, self.redact = directory, redact
        self.entries = []
        self.generated = directory / "generated"
        self.generated.mkdir()
        self.journal = (directory / "files.jsonl").open("x", encoding="utf-8")

    def checkpoint(self, entry):
        self.journal.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self.journal.flush()

    async def json_request(self, method, path, key_index, timeout, **kwargs):
        async with asyncio.timeout(timeout):
            async with self.client.stream(method, self.cfg.base_url + path,
                    headers={"Authorization": "Bearer " + self.keys[key_index]},
                    timeout=httpx.Timeout(timeout, connect=min(10, timeout)), **kwargs) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 65536:
                        raise FileError("file_protocol_error", "Ответ API файлов превышает 64 KiB", response.status_code)
                if not 200 <= response.status_code < 300:
                    retry = 5 if response.status_code in (429, 503) else 0
                    try:
                        retry = max(retry, min(3600, float(response.headers.get("Retry-After", "0"))))
                    except ValueError:
                        pass
                    raise FileError("file_http_error", f"{method} {path}: HTTP {response.status_code}: "
                                    + body.decode("utf-8", errors="replace"), response.status_code, retry)
                try:
                    return json.loads(body), response.status_code
                except (ValueError, UnicodeDecodeError):
                    raise FileError("file_protocol_error", "API файлов вернул не JSON", response.status_code) from None

    def available(self):
        return self.cfg.max_uploads == 0 or len(self.entries) < self.cfg.max_uploads

    async def prepare(self, key_index, user_id, selection):
        """Reserve before first await, upload a real file, then poll to a terminal state."""
        tag = uuid4().hex
        generated = not self.cfg.log_files
        path = self.generated / (tag + ".log") if generated else self.cfg.log_files[selection % len(self.cfg.log_files)]
        size = self.cfg.log_sizes_bytes[selection % len(self.cfg.log_sizes_bytes)] if generated else path.stat().st_size
        entry = dict(upload_id=tag, user_id=user_id, credential_id=f"key-{key_index+1:03}",
                     file_id=None, filename=f"wuxu-{tag}.log", bytes=size, generated=generated,
                     upload_accepted=False, processing_status="not_started", ok=False,
                     generation_ms=0, upload_ms=None, processing_ms=None, http_status=None,
                     error_kind="", error="", retry_after_s=0, cleanup="not_uploaded")
        self.entries.append(entry)
        marker = uuid4().hex if generated else None
        stage, started = "generation", time.perf_counter()
        self.checkpoint(entry)
        try:
            if generated:
                await asyncio.to_thread(generate_log, path, size, marker)
                entry["generation_ms"] = round((time.perf_counter() - started) * 1000, 3)
            stage, started = "upload", time.perf_counter()
            # HTTPX reads file objects in bounded chunks; never read the whole log into RAM.
            with path.open("rb") as source:
                data, status = await self.json_request("POST", "/api/v1/files/?process=true&process_in_background=true",
                    key_index, self.cfg.upload_timeout, files={"file": (entry["filename"], source, "text/plain")})
            entry["upload_ms"] = round((time.perf_counter() - started) * 1000, 3)
            entry["http_status"] = status
            file_id = data.get("id") if isinstance(data, dict) else None
            if not isinstance(file_id, str) or not file_id or file_id == "all" or len(file_id) > 200:
                raise FileError("file_protocol_error", "Upload не вернул корректный file id", status)
            entry.update(file_id=file_id, upload_accepted=True, processing_status="pending", cleanup="pending")
            self.checkpoint(entry)  # Persist the ID before polling; useful even if the process is killed.
            stage, started = "processing", time.perf_counter()
            async with asyncio.timeout(self.cfg.processing_timeout):
                while True:
                    data, status = await self.json_request("GET", f"/api/v1/files/{quote(file_id, safe='')}/process/status",
                                                         key_index, min(30, self.cfg.processing_timeout))
                    entry["http_status"] = status
                    state = data.get("status") if isinstance(data, dict) else None
                    entry["processing_status"] = state
                    if state == "completed":
                        entry["ok"] = True
                        break
                    if state == "failed":
                        raise FileError("file_processing_failed", data.get("error") or
                                        "Обработка файла завершилась failed; подробности в логах Open WebUI", status)
                    if state not in ("pending", "processing"):
                        raise FileError("file_protocol_error", f"Неизвестный статус обработки: {state!r}", status)
                    await asyncio.sleep(self.cfg.file_poll_interval)
        except FileError as exc:
            entry.update(error_kind=exc.kind, error=self.redact(str(exc), self.keys),
                         http_status=exc.status, retry_after_s=exc.retry)
        except (TimeoutError, httpx.TimeoutException) as exc:
            entry.update(error_kind=f"file_{stage}_timeout", error=self.redact(str(exc) or f"Таймаут: {stage}", self.keys))
        except (OSError, httpx.HTTPError, ValueError) as exc:
            entry.update(error_kind=f"file_{stage}_error", error=self.redact(str(exc), self.keys))
        finally:
            entry[stage + "_ms"] = round((time.perf_counter() - started) * 1000, 3)
            if generated:
                path.unlink(missing_ok=True)
            if not entry["file_id"] and stage == "upload" and entry["error_kind"] in (
                    "file_upload_timeout", "file_upload_error", "file_protocol_error"):
                # A timed-out upload might have reached the server without returning an ID.
                entry["cleanup"] = "unknown_no_id"
            self.checkpoint(entry)
        return entry, marker

    async def cleanup(self):
        semaphore = asyncio.Semaphore(min(self.cfg.users, 5))

        async def remove(entry):
            if not entry["file_id"]:
                return
            if not self.cfg.cleanup_files:
                entry["cleanup"] = "retained_by_setting"
            elif entry["processing_status"] not in ("completed", "failed"):
                entry["cleanup"] = "retained_processing_unfinished"
            else:
                async with semaphore:
                    try:
                        index = int(entry["credential_id"].split("-")[1]) - 1
                        await self.json_request("DELETE", f"/api/v1/files/{quote(entry['file_id'], safe='')}", index, 30)
                        entry["cleanup"] = "deleted"
                    except FileError as exc:
                        if exc.status == 404:
                            entry["cleanup"] = "already_absent"
                        else:
                            entry.update(cleanup="failed", cleanup_error=self.redact(str(exc), self.keys))
                    except (TimeoutError, httpx.HTTPError, ValueError) as exc:
                        entry.update(cleanup="failed", cleanup_error=self.redact(str(exc), self.keys))
            self.checkpoint(entry)

        await asyncio.gather(*(remove(entry) for entry in self.entries))
        (self.directory / "files.json").write_text(json.dumps(self.entries, ensure_ascii=False, indent=2), encoding="utf-8")
        if self.entries:
            fields = sorted(set().union(*(entry.keys() for entry in self.entries)))
            with (self.directory / "files.csv").open("x", encoding="utf-8-sig", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=fields)
                writer.writeheader()
                writer.writerows({k: "'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@")) else v
                                 for k, v in entry.items()} for entry in self.entries)

    def close(self):
        self.journal.close()
