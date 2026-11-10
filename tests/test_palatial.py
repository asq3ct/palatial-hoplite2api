import asyncio
import copy
import json
import unittest
import tempfile
from pathlib import Path
from decimal import Decimal

import httpx

from hoplite2api.admission import Limits, RateLimit
from hoplite2api.bridge import Bridge, Conflict, Capacity
from hoplite2api.protocol import parse_request
from hoplite2api.server import create_app
from hoplite2api.usage import UsageSnapshot, UsageError
from hoplite2api.acp import ACPClient
from hoplite2api.journal import DispatchJournal, ReplayConflict
from test_bridge import FakeACP, TOOL


BODY = {"model": "m", "messages": [{"role": "user", "content": "hello"}], "tools": [TOOL]}


class UsageTests(unittest.TestCase):
    def test_rest_counters_use_tokens_and_exact_microdollar_cost(self):
        value = UsageSnapshot.parse_rest({"inputTokens": 123, "outputTokens": 20, "runCount": 1,
                                          "topModel": "m", "totalCostMicros": 100001, "latestContextTokens": 55})
        self.assertEqual(value.input_tokens, 123)
        self.assertEqual(value.cost_usd, Decimal("0.100001"))
    def test_context_occupancy_is_not_billable_usage(self):
        with self.assertRaises(UsageError):
            UsageSnapshot.parse({"used": 9000, "size": 32000})

    def test_reported_cumulative_counters_and_exact_cost(self):
        first = UsageSnapshot.parse({"inputTokens": 101, "outputTokens": 12, "runCount": 1, "modelId": "m"})
        last = UsageSnapshot.parse({"_meta": {"hoplite": {"inputTokens": 900, "outputTokens": 33, "runCount": 2, "modelId": "m"}}, "cost": {"currency": "USD", "amount": "0.1000001"}})
        self.assertEqual(last.cost_usd, Decimal("0.1000001"))
        self.assertEqual(last.delta(first), UsageSnapshot(799, 21, 1, "m"))
        self.assertEqual(last.delta(last), UsageSnapshot(0, 0, 0, "m"))
        self.assertEqual(last.public()["scope"], "hoplite_thread")

    def test_invalid_or_regressing_counters_fail_closed(self):
        first = UsageSnapshot(100, 10, 1, "m")
        for value in (UsageSnapshot(99, 10, 1, "m"), UsageSnapshot(100, 9, 1, "m"), UsageSnapshot(100, 10, 0, "m"), UsageSnapshot(200, 20, 2, "other")):
            with self.assertRaises(UsageError):
                value.delta(first)
        for bad in (None, True, -1, 1.5, 2**54):
            with self.assertRaises(UsageError):
                UsageSnapshot.parse({"inputTokens": bad, "outputTokens": 1, "runCount": 1, "modelId": "m"})


class TenantTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clients = []
        async def factory(*args):
            c = FakeACP()
            self.clients.append(c)
            return c
        self.bridge = Bridge("upstream-secret", "project", client_factory=factory)

    async def asyncTearDown(self):
        await self.bridge.close()

    def request(self, tenant, body=None):
        r = parse_request(copy.deepcopy(body or BODY))
        r["_tenant"] = tenant
        return r

    async def test_identical_customer_conversations_get_separate_sessions(self):
        a = await self.bridge.acquire(self.request("a"))
        b = await self.bridge.acquire(self.request("b"))
        self.assertIsNot(a, b)
        self.assertEqual(len(self.clients), 2)

    async def test_cross_tenant_tool_result_is_rejected_without_dispatch(self):
        a = await self.bridge.acquire(self.request("a"))
        call = self.bridge.publish_tool(a, {"rpc_id": "rpc", "call_id": "shared-id", "name": "read", "arguments": {}, "deadline": asyncio.get_running_loop().time() + 60}, "")
        self.bridge.commit_tool(a)
        body = copy.deepcopy(BODY)
        body["messages"] += [{"role": "assistant", "content": None, "tool_calls": [call]}, {"role": "tool", "tool_call_id": call["id"], "content": "secret file"}]
        with self.assertRaises(Conflict):
            await self.bridge.acquire(self.request("b", body))
        self.assertEqual(a.client.replies, [])
        self.assertEqual(len(self.clients), 1)
        self.assertIs(await self.bridge.acquire(self.request("a", body)), a)

    async def test_identical_call_ids_are_scoped_by_tenant(self):
        for tenant in ("a", "b"):
            s = await self.bridge.acquire(self.request(tenant))
            self.bridge.publish_tool(s, {"rpc_id": "rpc", "call_id": "same", "name": "read", "arguments": {}, "deadline": asyncio.get_running_loop().time() + 60}, "")
            self.bridge.commit_tool(s)
        self.assertEqual(len(self.bridge.pending), 2)

    async def test_per_tenant_capacity_and_cleanup(self):
        self.bridge.max_tenant_sessions = 1
        s = await self.bridge.acquire(self.request("a"))
        body = copy.deepcopy(BODY)
        body["messages"][0]["content"] = "different"
        with self.assertRaises(Capacity):
            await self.bridge.acquire(self.request("a", body))
        self.assertEqual(len(self.clients), 1)
        await self.bridge.close_session(s)
        await self.bridge.acquire(self.request("a", body))
        self.assertEqual(len(self.clients), 2)

    async def test_finished_idle_sessions_do_not_block_sequential_new_chats(self):
        self.bridge.max_tenant_sessions = 1
        first = await self.bridge.acquire(self.request("a"))
        self.bridge.commit_done(first, "done")
        body = copy.deepcopy(BODY)
        body["messages"][0]["content"] = "different fresh conversation"
        second = await self.bridge.acquire(self.request("a", body))
        self.assertTrue(first.client.closed)
        self.assertFalse(second.closed)
        self.assertEqual(len(self.bridge.sessions), 1)

    async def test_turn_deadline_fails_and_does_not_replay(self):
        self.bridge.turn_timeout = 0.01
        s = await self.bridge.acquire(self.request("a"))
        with self.assertRaisesRegex(Exception, "deadline"):
            _ = [e async for e in self.bridge.events(s)]
        self.assertEqual(len(s.client.started), 1)

    async def test_tool_error_is_delivered_as_error(self):
        class ErrorACP(FakeACP):
            async def reply_tool(self, rpc_id, text, is_error=False):
                self.replies.append((rpc_id, text, is_error))
        async def factory(*args):
            return ErrorACP()
        self.bridge.client_factory = factory
        s = await self.bridge.acquire(self.request("a"))
        call = self.bridge.publish_tool(s, {"rpc_id": "rpc", "call_id": "err", "name": "read", "arguments": {}, "deadline": asyncio.get_running_loop().time() + 60}, "")
        self.bridge.commit_tool(s)
        body = copy.deepcopy(BODY)
        body["messages"] += [{"role": "assistant", "content": None, "tool_calls": [call]}, {"role": "tool", "tool_call_id": call["id"], "content": "Permission denied", "is_error": True}]
        await self.bridge.acquire(self.request("a", body))
        self.assertEqual(s.client.replies, [("rpc", "Permission denied", True)])

    async def test_usage_snapshot_for_other_model_is_not_returned(self):
        client = object.__new__(ACPClient)
        client.session_id = "session"
        client.selected_model = "wanted"
        client.expected_run_count = 1
        client.reported_usage = None
        client.usage_error = None
        async def request(*args):
            return {"used": 9999, "inputTokens": 0, "outputTokens": 0, "runCount": 0, "modelId": "other"}
        client._request = request
        self.assertIsNone(await client.read_usage())
        self.assertIsNone(client.reported_usage)

    async def test_pre_run_usage_is_not_a_completed_run_counter(self):
        client = object.__new__(ACPClient)
        client.session_id = "session"
        client.selected_model = "wanted"
        client.expected_run_count = 1
        client.reported_usage = None
        client.usage_error = None
        async def request(*args):
            return {"inputTokens": 0, "outputTokens": 0, "runCount": 0, "modelId": "wanted"}
        client._request = request
        self.assertIsNone(await client.read_usage())


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clients = []
        async def factory(*args):
            c = FakeACP()
            self.clients.append(c)
            await c.events.put({"type": "text", "text": "OK"})
            await c.events.put({"type": "done", "stop_reason": "end_turn"})
            return c
        self.factory = factory
        self.http = httpx.AsyncClient(base_url="https://unused.test", transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"models": [{"id": "m"}]})))
        self.apps = []

    def app(self, **kw):
        app = create_app("project", "upstream-secret", bridge_key="separate-private-bridge-secret", require_tenant=True,
                         client_factory=self.factory, http_client=self.http, **kw)
        self.apps.append(app)
        return app

    async def asyncTearDown(self):
        for app in self.apps:
            await app.state.bridge.close()
        await self.http.aclose()

    async def send(self, app, body=None, **kw):
        headers = {"Authorization": "Bearer separate-private-bridge-secret", "X-Palatial-Tenant": "a" * 32}
        headers.update(kw.pop("headers", {}))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
            return await client.post("/v1/chat/completions", json=body or {"model": "m", "messages": [{"role": "user", "content": "hello"}]}, headers=headers, **kw)

    async def test_upstream_key_cannot_authenticate_client(self):
        res = await self.send(self.app(), headers={"Authorization": "Bearer upstream-secret"})
        self.assertEqual(res.status_code, 401)
        self.assertEqual(self.clients, [])

    async def test_missing_authenticated_tenant_rejected(self):
        res = await self.send(self.app(), headers={"X-Palatial-Tenant": "untrusted customer name"})
        self.assertEqual(res.status_code, 400)
        self.assertEqual(self.clients, [])

    async def test_body_limit_before_dispatch(self):
        res = await self.send(self.app(limits=Limits(body_bytes=50)))
        self.assertEqual(res.status_code, 413)
        self.assertEqual(self.clients, [])

    async def test_rate_limit_is_per_authenticated_tenant(self):
        app = self.app(limits=Limits(requests_per_minute=1))
        self.assertEqual((await self.send(app)).status_code, 200)
        second = await self.send(app)
        self.assertEqual(second.status_code, 429)
        self.assertEqual(second.headers["Retry-After"], "60")
        self.assertEqual((await self.send(app, headers={"X-Palatial-Tenant": "b" * 32})).status_code, 200)

    async def test_pending_tool_result_is_not_blocked_by_new_prompt_rate_limit(self):
        async def factory(*args):
            c = FakeACP()
            async def reply(*args):
                await c.events.put({"type": "text", "text": "OK"})
                await c.events.put({"type": "done", "stop_reason": "end_turn"})
            c.reply_tool = reply
            await c.events.put({"type": "tool_call", "rpc_id": "rpc", "call_id": "known-call", "name": "read", "arguments": {}, "deadline": asyncio.get_running_loop().time() + 60})
            return c
        self.factory = factory
        app = self.app(limits=Limits(requests_per_minute=1))
        body = copy.deepcopy(BODY)
        first = await self.send(app, body)
        self.assertEqual(first.status_code, 200)
        message = first.json()["choices"][0]["message"]
        body["messages"] += [message, {"role": "tool", "tool_call_id": "known-call", "content": "file contents"}]
        resumed = await self.send(app, body)
        self.assertEqual(resumed.status_code, 200, resumed.text)
        self.assertEqual(resumed.json()["choices"][0]["message"]["content"], "OK")

    async def test_stream_and_nonstream_have_no_fabricated_token_usage(self):
        app = self.app()
        res = await self.send(app)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["choices"][0]["message"]["content"], "OK")
        self.assertNotIn("usage", res.json())
        self.assertIsNone(res.json()["hoplite_usage"])
        res = await self.send(app, {"model": "m", "messages": [{"role": "user", "content": "different"}], "stream": True})
        frames = [json.loads(s[6:]) for s in res.text.splitlines() if s.startswith("data: ") and s != "data: [DONE]"]
        self.assertTrue(res.text.endswith("data: [DONE]\n\n"))
        self.assertEqual(frames[-1]["choices"][0]["finish_reason"], "stop")
        self.assertTrue(all("usage" not in f for f in frames))
        self.assertEqual(res.headers["X-Accel-Buffering"], "no")

    async def test_output_overflow_is_error_not_success(self):
        app = self.app(limits=Limits(output_bytes=1))
        res = await self.send(app, {"model": "m", "messages": [{"role": "user", "content": "hello"}], "stream": True})
        self.assertIn("output byte limit", res.text)
        self.assertNotIn('"finish_reason": "stop"', res.text)
        self.assertTrue(self.clients[0].closed)

    async def test_json_object_is_validated_before_streaming_content(self):
        async def factory(*args):
            c = FakeACP()
            await c.events.put({"type": "text", "text": '{"total":'})
            await c.events.put({"type": "text", "text": '42}'})
            await c.events.put({"type": "done", "stop_reason": "end_turn"})
            return c
        self.factory = factory
        res = await self.send(self.app(), {"model": "m", "messages": [{"role": "user", "content": "json"}], "response_format": {"type": "json_object"}, "stream": True})
        self.assertEqual(res.status_code, 200)
        frames = [json.loads(s[6:]) for s in res.text.splitlines() if s.startswith("data: ") and s != "data: [DONE]"]
        text = "".join(c.get("delta", {}).get("content", "") for f in frames for c in f.get("choices", []))
        self.assertEqual(json.loads(text), {"total": 42})

    async def test_bad_json_objects_never_become_successes(self):
        for content in ('not json', '[1,2]', '{"a":1,"a":2}', '{"a":NaN}'):
            async def factory(*args):
                c = FakeACP()
                await c.events.put({"type": "text", "text": content})
                await c.events.put({"type": "done", "stop_reason": "end_turn"})
                return c
            self.factory = factory
            res = await self.send(self.app(), {"model": "m", "messages": [{"role": "user", "content": "json"}], "response_format": {"type": "json_object"}})
            self.assertEqual(res.status_code, 502)
            self.assertIn("valid JSON object", res.json()["error"]["message"])

    async def test_heartbeat_is_only_a_comment(self):
        async def factory(*args):
            c = FakeACP()
            self.clients.append(c)
            return c
        self.factory = factory
        app = self.app(limits=Limits(heartbeat_seconds=0.01))
        body = json.dumps({"model": "m", "messages": [{"role": "user", "content": "hi"}], "stream": True}).encode()
        first = True
        chunks = []
        never = asyncio.Event()
        async def receive():
            nonlocal first
            if first:
                first = False
                return {"type": "http.request", "body": body, "more_body": False}
            await never.wait()
        async def send(message):
            if message["type"] == "http.response.body":
                chunk = message.get("body", b"")
                chunks.append(chunk)
                if b": keep-alive" in chunk:
                    await self.clients[0].events.put({"type": "text", "text": "OK"})
                    await self.clients[0].events.put({"type": "done", "stop_reason": "end_turn"})
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.0"}, "method": "POST", "scheme": "http", "path": "/v1/chat/completions", "raw_path": b"/v1/chat/completions", "query_string": b"", "headers": [(b"authorization", b"Bearer separate-private-bridge-secret"), (b"x-palatial-tenant", b"a" * 32), (b"content-type", b"application/json")], "server": ("127.0.0.1", 8091), "client": ("127.0.0.1", 1234)}
        await asyncio.wait_for(app(scope, receive, send), 3)
        output = b"".join(chunks)
        self.assertIn(b": keep-alive\n\n", output)
        self.assertTrue(output.endswith(b"data: [DONE]\n\n"))


class AdmissionTests(unittest.TestCase):
    def test_rate_window_releases_capacity(self):
        rate = RateLimit(1)
        self.assertTrue(rate.admit("a", now=10))
        self.assertFalse(rate.admit("a", now=69))
        self.assertTrue(rate.admit("b", now=69))
        self.assertTrue(rate.admit("a", now=70))

    def test_journal_survives_restart_and_is_tenant_scoped(self):
        with tempfile.TemporaryDirectory() as folder:
            filename = Path(folder) / "dispatch.db"
            journal = DispatchJournal(filename)
            journal.claim("a", "attempt-123", b"private synthetic prompt")
            journal.close()
            journal = DispatchJournal(filename)
            try:
                with self.assertRaises(ReplayConflict):
                    journal.claim("a", "attempt-123", b"private synthetic prompt")
                with self.assertRaises(ReplayConflict):
                    journal.claim("a", "attempt-123", b"changed")
                journal.claim("b", "attempt-123", b"private synthetic prompt")
                self.assertNotIn(b"private synthetic prompt", filename.read_bytes())
            finally:
                journal.close()
