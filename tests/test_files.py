"""Offline file workflow tests: real multipart bodies, no provider traffic."""
import asyncio
from email.parser import BytesParser
from email.policy import default
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import wuxu_loadtest as app
from log_files import FileManager, generate_log, parse_sizes


def config(*extra):
    return app.parse_args(["--base-url", "http://127.0.0.1:9999", "--users", "2", "--duration", "3",
                          "--ramp-up", "0", "--think-min", "0", "--think-max", "0", "--timeout", "1",
                          "--log-sizes-mb", ".004096,.008192", "--file-poll-interval", ".001",
                          "--processing-timeout", "1", "--max-requests", "7", "--no-stream"] + list(extra))


def multipart(request):
    raw = ("Content-Type: " + request.headers["content-type"] + "\r\nMIME-Version: 1.0\r\n\r\n").encode() + request.content
    message = BytesParser(policy=default).parsebytes(raw)
    parts = list(message.iter_parts())
    assert len(parts) == 1 and parts[0].get_param("name", header="content-disposition") == "file"
    return parts[0].get_payload(decode=True)


class GeneratorTests(unittest.TestCase):
    def test_exact_size_real_utf8_and_unique_content(self):
        with tempfile.TemporaryDirectory() as temp:
            outputs = []
            for index, size in enumerate((256, 1025, 1_000_000)):
                path = generate_log(Path(temp) / f"{index}.log", size, f"marker{index}")
                data = path.read_bytes()
                self.assertEqual(len(data), size)
                self.assertIn(f"WUXU_CHECK=marker{index}", data.decode("utf-8"))
                self.assertTrue(data.endswith(b"\n"))
                outputs.append(hashlib.sha256(data).hexdigest())
            self.assertEqual(len(set(outputs)), 3)

    def test_size_parsing(self):
        self.assertEqual(parse_sizes("1,10,300,.004096"), [1_000_000, 10_000_000, 300_000_000, 4096])
        for value in ("nan", "inf", "0", "-1", "", "1,", "0.0000001"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_sizes(value)


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, cfg, failure=None, wrong_marker=False):
        uploaded, chats, deleted, polls = {}, [], [], {}
        async def handler(request):
            key = request.headers["Authorization"]
            if request.method == "POST" and request.url.path == "/api/v1/files/":
                data = multipart(request)
                if failure == "upload":
                    return httpx.Response(413, json={"detail": "too large sk-one"})
                fid = str(len(uploaded) + 1)
                uploaded[fid] = (data, key)
                polls[fid] = 0
                return httpx.Response(200, json={"id": fid})
            if request.method == "GET":
                fid = request.url.path.split("/")[4]
                self.assertEqual(uploaded[fid][1], key)
                polls[fid] += 1
                status = "failed" if failure == "processing" else "pending" if failure == "timeout" or polls[fid] == 1 else "completed"
                return httpx.Response(200, json={"status": status})
            if request.method == "DELETE":
                fid = request.url.path.split("/")[4]
                self.assertEqual(uploaded[fid][1], key)
                deleted.append(fid)
                if failure == "cleanup":
                    return httpx.Response(403, json={"error": "denied"})
                return httpx.Response(200, json={"message": "deleted"})
            payload = json.loads(request.content)
            chats.append(payload)
            answer = "OK"
            if payload.get("files"):
                fid = payload["files"][0]["id"]
                data, owner = uploaded[fid]
                self.assertEqual(owner, key)
                self.assertGreaterEqual(polls[fid], 2)
                marker = re.search(rb"WUXU_CHECK=(\w+)", data)
                if marker and not wrong_marker:
                    answer += " " + marker[1].decode()
            return httpx.Response(200, json={"choices": [{"message": {"content": answer}, "finish_reason": "stop"}]})

        with tempfile.TemporaryDirectory() as temp:
            record = app.Recorder(Path(temp) / "result")
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                files = FileManager(client, cfg, ["sk-one", "sk-two"], record.directory, app.redact)
                try:
                    summary = await app.run_load(client, cfg, ["sk-one", "sk-two"], ["plain"], "run", record, asyncio.Event(), files)
                    await files.cleanup()
                    persisted = (record.directory / "files.json").read_text(encoding="utf-8")
                    self.assertNotIn("sk-one", persisted)
                    self.assertEqual(list(files.generated.iterdir()), [])
                    return summary, list(record.rows), list(files.entries), uploaded, chats, deleted
                finally:
                    files.close()
                    record.close()

    async def test_real_upload_poll_chat_history_owner_and_cleanup(self):
        summary, rows, entries, uploaded, chats, deleted = await self.exercise(config())
        self.assertEqual(summary["successful"], 6)
        self.assertTrue(all(p.get("files") for p in chats))
        self.assertEqual({len(data) for data, _ in uploaded.values()}, {4096, 8192})
        self.assertEqual(set(deleted), set(uploaded))
        self.assertTrue(any(len(p["messages"]) > 1 for p in chats))
        self.assertTrue(all(e["ok"] and e["cleanup"] == "deleted" for e in entries))
        self.assertTrue(any(r["attachment_verified"] for r in rows))
        for r in rows:
            self.assertGreaterEqual(r["workflow_ms"], r["elapsed_ms"] - .01)

    async def test_upload_413_stops_and_never_calls_model(self):
        summary, rows, entries, uploaded, chats, deleted = await self.exercise(config(), failure="upload")
        self.assertEqual(summary["stop_reason"], "file_too_large")
        self.assertEqual(chats, [])
        self.assertEqual(deleted, [])
        self.assertTrue(all(not r["chat_sent"] and not r["ok"] for r in rows))

    async def test_failed_processing_not_sent_to_chat_and_can_be_deleted(self):
        summary, rows, entries, uploaded, chats, deleted = await self.exercise(config("--max-requests", "3"), failure="processing")
        self.assertEqual(chats, [])
        self.assertEqual(len(deleted), len(uploaded))
        self.assertTrue(all(r["error_kind"] == "file_processing_failed" for r in rows))

    async def test_processing_timeout_retains_file(self):
        summary, rows, entries, uploaded, chats, deleted = await self.exercise(config("--max-requests", "3", "--processing-timeout", ".03"), failure="timeout")
        self.assertEqual(chats, [])
        self.assertEqual(deleted, [])
        self.assertTrue(all(e["cleanup"] == "retained_processing_unfinished" for e in entries))
        self.assertTrue(all(r["error_kind"] == "file_processing_timeout" for r in rows))

    async def test_missing_marker_is_failure_despite_http_200(self):
        summary, rows, entries, uploaded, chats, deleted = await self.exercise(config("--max-requests", "3"), wrong_marker=True)
        self.assertEqual(summary["successful"], 0)
        self.assertTrue(all(r["error_kind"] == "file_content_not_verified" for r in rows))

    async def test_max_uploads_cap_then_plain_dialogues(self):
        summary, rows, entries, uploaded, chats, deleted = await self.exercise(config("--turns", "1", "--max-uploads", "1"))
        self.assertEqual(len(entries), 1)
        self.assertTrue(any("files" not in p for p in chats))
        self.assertEqual(summary["chat_posts"], 6)

    async def test_original_file_is_uploaded_exactly_and_never_changed(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "original.log"
            data = b"2026-11-16 ERROR actual test content\n" * 50
            path.write_bytes(data)
            summary, rows, entries, uploaded, chats, deleted = await self.exercise(config("--log-files", str(path), "--max-requests", "3"))
            self.assertEqual(path.read_bytes(), data)
            self.assertTrue(all(body == data for body, _ in uploaded.values()))
            self.assertTrue(all(r["attachment_verified"] is None for r in rows))

    async def test_keep_files_setting_and_cleanup_error(self):
        result = await self.exercise(config("--no-cleanup-files", "--max-requests", "3"))
        self.assertEqual(result[-1], [])
        self.assertTrue(all(e["cleanup"] == "retained_by_setting" for e in result[2]))
        result = await self.exercise(config("--max-requests", "3"), failure="cleanup")
        self.assertTrue(all(e["cleanup"] == "failed" for e in result[2]))

    async def test_admitted_file_workflows_drain_past_window(self):
        result = await self.exercise(config("--users", "1", "--duration", ".1", "--file-poll-interval", ".2"))
        self.assertEqual(len(result[3]), 1)
        self.assertEqual(len(result[4]), 1)
        self.assertGreater(result[0]["elapsed_seconds_including_drain"], .1)


if __name__ == "__main__":
    unittest.main()
