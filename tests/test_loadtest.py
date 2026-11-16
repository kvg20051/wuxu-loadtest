"""Offline tests: no real keys, no WUXU traffic and no provider charges."""
import asyncio
import contextlib
import csv
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import sys
import unittest

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("wuxu_loadtest", ROOT / "wuxu_loadtest.py")
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks, delay=0):
        self.chunks, self.delay = chunks, delay

    async def __aiter__(self):
        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield chunk


def event(data):
    return ("data: " + json.dumps(data, ensure_ascii=False) + "\r\n\r\n").encode()


def good_chunks(text="Работает", usage=True):
    chunks = [b": keepalive\r\n\r\n", event({"choices": [{"delta": {"role": "assistant"}}]}),
              event({"choices": [{"delta": {"reasoning_content": "Thinking"}}]}),
              event({"choices": [{"delta": {"content": text}, "finish_reason": None}]}),
              event({"choices": [{"delta": {}, "finish_reason": "stop"}]})]
    if usage:
        chunks.append(event({"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}))
    chunks.append(b"data: [DONE]\n\n")
    return chunks


def config(*args):
    return app.parse_args(["--base-url", "http://127.0.0.1:9999", "--users", "3", "--duration", "0.15",
                           "--ramp-up", "0", "--think-min", "0", "--think-max", "0", "--timeout", "1"] + list(args))


class CompletionTests(unittest.IsolatedAsyncioTestCase):
    async def request(self, response, cfg=None):
        cfg = cfg or config()
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
            return await app.completion(client, cfg, "sk-fake-key", app.make_payload(cfg, []),
                                        {"request_id": "test", "user_id": "vu-001", "credential_id": "key-001",
                                         "scenario": "test", "offset_s": 0}, ["sk-fake-key"])

    async def test_stream_success_usage_unicode_and_ttft(self):
        row, answer = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"},
                                                       stream=Stream(good_chunks(), .001)))
        self.assertTrue(row["ok"], row)
        self.assertEqual(answer, "Работает")
        self.assertEqual(row["total_tokens"], 15)
        self.assertGreater(row["ttft_ms"], row["headers_ms"])
        self.assertGreater(row["elapsed_ms"], row["ttft_ms"])

    async def test_http_200_stream_error_after_partial_answer(self):
        chunks = [event({"choices": [{"delta": {"content": "partial"}}]}),
                  event({"error": {"message": "Failed with sk-fake-key"}})]
        row, _ = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(chunks)))
        self.assertFalse(row["ok"])
        self.assertEqual(row["error_kind"], "provider_error")
        self.assertNotIn("sk-fake-key", json.dumps(row))
        self.assertEqual(row["output_chars"], 7)

    async def test_partial_stream_is_failure(self):
        row, _ = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream([
            event({"choices": [{"delta": {"content": "partial"}}]})])))
        self.assertFalse(row["ok"])
        self.assertEqual(row["error_kind"], "incomplete_stream")

    async def test_absolute_timeout_catches_slow_stream(self):
        row, _ = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"},
                                                  stream=Stream(good_chunks(), .02)), config("--timeout", ".035"))
        self.assertFalse(row["ok"])
        self.assertEqual(row["error_kind"], "timeout")
        self.assertLess(row["elapsed_ms"], 500)

    async def test_429_and_retry_after(self):
        row, _ = await self.request(httpx.Response(429, headers={"Retry-After": "17"}, json={"error": "limit"}))
        self.assertEqual(row["http_status"], 429)
        self.assertEqual(row["retry_after_s"], 17)
        self.assertFalse(row["ok"])

    async def test_missing_usage_remains_unknown(self):
        row, _ = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(good_chunks(usage=False))))
        self.assertTrue(row["ok"])
        self.assertIsNone(row["total_tokens"])

    async def test_no_visible_content_is_failure(self):
        row, _ = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(good_chunks(text=""))))
        self.assertEqual(row["error_kind"], "empty_response")

    async def test_json_mode_does_not_claim_ttft(self):
        row, answer = await self.request(httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}, "finish_reason": "length"}],
            "usage": {"input_tokens": 20, "output_tokens": 10, "prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 30},
        }), config("--no-stream"))
        self.assertTrue(row["ok"])
        self.assertEqual(answer, "ok")
        self.assertIsNone(row["ttft_ms"])
        self.assertEqual(row["prompt_tokens"], 20)

    async def test_non_sse_response_rejected_in_stream_mode(self):
        row, _ = await self.request(httpx.Response(200, json={"task_id": "123"}))
        self.assertEqual(row["error_kind"], "protocol_error")

    async def test_multiline_sse_event(self):
        chunks = [b'data: {"choices":\n', b'data: [{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n', b'data: [DONE]\n\n']
        row, answer = await self.request(httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(chunks)))
        self.assertTrue(row["ok"])
        self.assertEqual(answer, "ok")


class LoadTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, cfg, handler, keys=None, stop=None):
        with tempfile.TemporaryDirectory() as temp:
            recorder = app.Recorder(Path(temp) / "result")
            try:
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                    result = await app.run_load(client, cfg, keys or ["sk-fake-key"], ["test"], "run", recorder, stop or asyncio.Event())
                app.write_report(recorder.directory, result, cfg, {"ok": True})
                saved = json.loads((recorder.directory / "summary.json").read_text())
                self.assertEqual(saved["requests"], result["requests"])
                self.assertNotIn("sk-fake-key", (recorder.directory / "summary.json").read_text())
                return result, recorder.rows.copy()
            finally:
                recorder.close()

    async def test_global_cap_counts_preflight_and_keeps_inflight_bounded(self):
        calls = []
        async def handler(request):
            calls.append(request)
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(good_chunks(), .001))
        summary, rows = await self.exercise(config("--duration", "3", "--max-requests", "8"), handler)
        self.assertEqual(len(calls), 7)
        self.assertEqual(summary["requests"], 7)
        self.assertEqual(summary["successful"], 7)
        self.assertEqual(summary["stop_reason"], "max_requests")
        self.assertLessEqual(summary["peak_inflight_requests"], 3)
        self.assertEqual(len({r["request_id"] for r in rows}), 7)

    async def test_full_duration_and_round_robin_credentials(self):
        seen = set()
        starts = []
        async def handler(request):
            seen.add(request.headers["Authorization"])
            starts.append(asyncio.get_running_loop().time())
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(good_chunks(), .001))
        summary, _ = await self.exercise(config("--ramp-up", ".02", "--think-min", ".005", "--think-max", ".005"),
                                         handler, ["sk-one", "sk-two"])
        self.assertEqual(summary["stop_reason"], "duration")
        self.assertEqual(summary["users_with_requests"], 3)
        self.assertEqual(summary["distinct_credentials"], 2)
        self.assertEqual(seen, {"Bearer sk-one", "Bearer sk-two"})
        self.assertGreaterEqual(summary["elapsed_seconds_including_drain"], .14)
        self.assertLess(starts[-1] - starts[0], .15)

    async def test_401_stops_scheduling_without_retries(self):
        calls = 0
        async def handler(request):
            nonlocal calls
            calls += 1
            return httpx.Response(401, json={"error": "invalid"})
        summary, _ = await self.exercise(config(), handler)
        self.assertLessEqual(calls, 3)
        self.assertEqual(summary["stop_reason"], "authentication_or_permission_error")
        self.assertEqual(summary["successful"], 0)

    async def test_error_streak_stops(self):
        async def handler(request):
            return httpx.Response(500, json={"error": "failure"})
        summary, _ = await self.exercise(config("--users", "1", "--stop-after-errors", "2"), handler)
        self.assertEqual(summary["requests"], 2)
        self.assertEqual(summary["stop_reason"], "consecutive_errors")

    async def test_models_are_checked_per_key(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"data": [{"id": "model-a"}]}))) as client:
            self.assertEqual(await app.models_for_key(client, config(), "fake", ["fake"]), ["model-a"])

    async def test_stop_event_drains_current_requests(self):
        stop = asyncio.Event()
        async def handler(request):
            asyncio.get_running_loop().call_later(.003, stop.set)
            return httpx.Response(200, headers={"Content-Type": "text/event-stream"}, stream=Stream(good_chunks(), .003))
        summary, _ = await self.exercise(config(), handler, stop=stop)
        self.assertEqual(summary["stop_reason"], "operator_stop")
        self.assertLessEqual(summary["requests"], 3)
        self.assertEqual(summary["failed"], 0)


class UtilityTests(unittest.TestCase):
    def test_dry_run_needs_no_key_and_uses_requested_defaults(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(app.main(["--dry-run"]), 0)
        self.assertIn('"users": 30', output.getvalue())
        self.assertIn('"duration": 300', output.getvalue())

    def test_parameter_validation(self):
        for args in (["--users", "0"], ["--duration", "nan"], ["--ramp-up", "300"],
                     ["--think-min", "31"], ["--timeout", "0"], ["--max-requests", "1"],
                     ["--base-url", "http://example.com"], ["--base-url", "https://user:secret@example.com"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                app.parse_args(args)

    def test_percentiles_and_missing_usage(self):
        self.assertEqual(app.distribution(list(range(1, 101)))["p95"], 95)
        self.assertIsNone(app.distribution([])["p95"])
        self.assertEqual(app.usage_counts({"prompt_tokens": 3, "completion_tokens": 4})["total_tokens"], 7)
        self.assertIsNone(app.usage_counts({})["total_tokens"])

    def test_csv_error_formula_escaped(self):
        with tempfile.TemporaryDirectory() as temp:
            record = app.Recorder(Path(temp) / "result")
            row = {field: None for field in app.FIELDS}
            row["error"] = "=SUM(1,2)"
            record.add(row)
            record.close()
            with (record.directory / "requests.csv").open(encoding="utf-8-sig", newline="") as file:
                self.assertEqual(next(csv.DictReader(file))["error"], "'=SUM(1,2)")

    def test_key_file_deduplicates_without_echoing(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "keys.txt"
            path.write_text("# comment\nsk-one\nsk-two\nsk-one\n")
            self.assertEqual(app.read_keys(config("--keys-file", str(path))), ["sk-one", "sk-two"])


if __name__ == "__main__":
    unittest.main()
