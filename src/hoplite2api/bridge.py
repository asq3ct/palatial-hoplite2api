"""Process-local ownership of ACP sessions and native tool callbacks."""

import asyncio
import json
import time
import uuid
import hashlib
from dataclasses import dataclass, field

from .acp import ACPClient, UpstreamError
from .protocol import assistant_message, history_key, prompt_text, registered_tools, text_content


class Conflict(Exception):
    pass


class Capacity(Exception):
    pass


@dataclass(eq=False)
class Session:
    client: object
    request: dict
    history: list
    busy: bool = True
    pending: dict | None = None
    active_key: str | None = None
    timer: asyncio.Task | None = None
    had_tool: bool = False
    closed: bool = False
    tenant: str = "local"
    deadline: float = 0


class Bridge:
    def __init__(self, key, project, base_url="https://api.hoplite.sh", client_factory=ACPClient.open, idle_timeout=300,
                 *, max_sessions=16, max_tenant_sessions=2, turn_timeout=600):
        self.key, self.project, self.base_url = key, project, base_url
        self.client_factory, self.idle_timeout = client_factory, idle_timeout
        self.sessions = set()
        self.pending = {}
        self.idle = {}
        self.active_keys = set()
        self.opening = {}
        self.max_sessions, self.max_tenant_sessions, self.turn_timeout = max_sessions, max_tenant_sessions, turn_timeout

    def tenant_for(self, request):
        return request.get("_tenant", "local")

    def key_for(self, request, messages):
        key = history_key(request["model"], messages, request["tools"], request["reasoning_effort"])
        return hashlib.sha256((self.tenant_for(request) + "\0" + key).encode()).hexdigest()

    def valid_continuation(self, request):
        """Existing callbacks use their session lease, not a fresh-prompt RPM slot."""
        messages = request["messages"]
        if messages[-1]["role"] != "tool":
            return False
        session = self.pending.get((self.tenant_for(request), messages[-1]["tool_call_id"]))
        if not session or session.closed or session.busy:
            return False
        return (self.key_for(request, messages[:-1]) == self.key_for(session.request, session.history)
                and registered_tools(request) == registered_tools(session.request)
                and all(request.get(field) == session.request.get(field) for field in
                        ("tool_choice", "parallel_tool_calls", "max_tokens", "max_completion_tokens", "response_format")))

    async def close_session(self, session):
        if session.closed:
            return
        session.closed = True
        self.sessions.discard(session)
        for mapping in (self.pending, self.idle):
            for key, value in list(mapping.items()):
                if value is session:
                    del mapping[key]
        if session.active_key:
            self.active_keys.discard(session.active_key)
        if session.timer and session.timer is not asyncio.current_task():
            session.timer.cancel()
        await session.client.close()

    async def close(self):
        await asyncio.gather(*(self.close_session(s) for s in list(self.sessions)), return_exceptions=True)

    def schedule_expiry(self, session, delay):
        if session.timer:
            session.timer.cancel()
        async def expire():
            try:
                await asyncio.sleep(max(0, delay))
                await self.close_session(session)
            except asyncio.CancelledError:
                pass
        session.timer = asyncio.create_task(expire())

    async def acquire(self, request):
        messages = request["messages"]
        if messages[-1]["role"] == "tool":
            call_id = messages[-1]["tool_call_id"]
            tenant = self.tenant_for(request)
            session = self.pending.get((tenant, call_id))
            if not session or session.closed or session.busy:
                raise Conflict("Unknown, expired, or already claimed tool call")
            if self.key_for(request, messages[:-1]) != self.key_for(session.request, session.history):
                raise Conflict("Tool result does not match the pending conversation")
            if registered_tools(request) != registered_tools(session.request):
                raise Conflict("Registered tools changed during the pending call")
            for field in ("tool_choice", "parallel_tool_calls", "max_tokens", "max_completion_tokens", "response_format"):
                if request.get(field) != session.request.get(field):
                    raise Conflict("Prompt controls changed during the pending call")
            event = session.pending
            if asyncio.get_running_loop().time() >= event["deadline"]:
                await self.close_session(session)
                raise Conflict("Tool call expired")
            result = text_content(messages[-1].get("content"))
            is_error = messages[-1].get("is_error", False)
            payload = {"content": [{"type": "text", "text": result}], "isError": is_error}
            if len(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")) > session.client.max_result_bytes:
                raise Conflict("Tool result exceeds upstream limit")
            session.busy = True
            self.pending.pop((tenant, call_id), None)
            if session.timer:
                session.timer.cancel()
                session.timer = None
            try:
                if is_error:
                    await session.client.reply_tool(event["rpc_id"], result, is_error=True)
                else:
                    await session.client.reply_tool(event["rpc_id"], result)
            except BaseException:
                await self.close_session(session)
                raise
            session.pending = None
            session.request = request
            session.history = list(messages)
            return session

        # Only a user turn can follow an idle assistant response.
        match = None
        if messages[-1]["role"] == "user":
            for index in range(len(messages) - 1, 0, -1):
                candidate = self.key_for(request, messages[:index])
                if candidate in self.active_keys:
                    raise Conflict("Conversation already has an active request")
                session = self.idle.get(candidate)
                if session is not None:
                    if registered_tools(request) != registered_tools(session.request):
                        continue
                    match = (candidate, index, session)
                    break
        if match:
            key, index, session = match
            del self.idle[key]
            self.active_keys.add(key)
            session.active_key = key
            session.busy = True
            if session.timer:
                session.timer.cancel()
                session.timer = None
            session.had_tool = False
            session.deadline = asyncio.get_running_loop().time() + self.turn_timeout
            session.request, session.history = request, list(messages)
            try:
                await session.client.start_prompt(prompt_text(request, messages[index:]))
            except BaseException:
                await self.close_session(session)
                raise
            return session

        key = self.key_for(request, messages)
        if key in self.active_keys:
            raise Conflict("Conversation already has an active request")
        tenant = self.tenant_for(request)
        # Finished warm sessions must not make sequential new chats look busy.
        # Evict only completed idle sessions, never active/pending tool work.
        while True:
            count = sum(s.tenant == tenant for s in self.sessions) + self.opening.get(tenant, 0)
            tenant_full = count >= self.max_tenant_sessions
            global_full = len(self.sessions) + sum(self.opening.values()) >= self.max_sessions
            if not tenant_full and not global_full:
                break
            idle = next((s for s in self.idle.values() if not s.busy and not s.pending and (not tenant_full or s.tenant == tenant)), None)
            if idle is None:
                raise Capacity("Experimental bridge session capacity reached; retry after completing an existing turn")
            await self.close_session(idle)
        self.active_keys.add(key)
        self.opening[tenant] = self.opening.get(tenant, 0) + 1
        opening = True
        session = None
        try:
            client = await self.client_factory(self.base_url, self.key, self.project)
            session = Session(client, request, list(messages), active_key=key, tenant=tenant,
                              deadline=asyncio.get_running_loop().time() + self.turn_timeout)
            self.sessions.add(session)
            self.opening[tenant] -= 1
            if not self.opening[tenant]:
                del self.opening[tenant]
            opening = False
            await client.set_model(request["model"], request["reasoning_effort"])
            await client.register_tools(registered_tools(request))
            await client.start_prompt(prompt_text(request))
            return session
        except BaseException:
            if session is None:
                self.active_keys.discard(key)
            else:
                await self.close_session(session)
            raise
        finally:
            if opening:
                self.opening[tenant] -= 1
                if not self.opening[tenant]:
                    del self.opening[tenant]

    def publish_tool(self, session, event, text):
        event = dict(event)
        timeout = session.client.tool_timeout
        event["deadline"] -= max(0, timeout - 60)
        call_id = event["call_id"]
        if not call_id or (session.tenant, call_id) in self.pending or asyncio.get_running_loop().time() >= event["deadline"]:
            raise UpstreamError("Invalid, duplicate, or expired upstream tool call")
        call = {"id": call_id, "type": "function", "function": {"name": event["name"], "arguments": json.dumps(event["arguments"], ensure_ascii=False, separators=(",", ":"), allow_nan=False)}}
        session.history.append(assistant_message(text, call))
        session.pending = event
        session.had_tool = True
        return call

    def commit_tool(self, session):
        session.busy = False
        self.pending[(session.tenant, session.pending["call_id"])] = session
        self.schedule_expiry(session, session.pending["deadline"] - asyncio.get_running_loop().time())

    def commit_done(self, session, text):
        session.history.append(assistant_message(text))
        key = self.key_for(session.request, session.history)
        previous = self.idle.get(key)
        session.busy = False
        self.idle[key] = session
        self.schedule_expiry(session, self.idle_timeout)
        if session.active_key:
            self.active_keys.discard(session.active_key)
            session.active_key = None
        if previous is not None and previous is not session:
            asyncio.create_task(self.close_session(previous))

    async def events(self, session):
        while True:
            remaining = session.deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise UpstreamError("Experimental agent turn deadline exceeded; no automatic replay", "turn_timeout")
            try:
                event = await asyncio.wait_for(session.client.events.get(), remaining)
            except TimeoutError:
                raise UpstreamError("Experimental agent turn deadline exceeded; no automatic replay", "turn_timeout") from None
            kind = event.get("type")
            if kind == "text":
                yield event
            elif kind in ("tool_call", "done"):
                yield event
                return
            elif kind == "error":
                raise event["error"]


def completion_id():
    return "chatcmpl-" + uuid.uuid4().hex


def completion_base(model):
    return {"id": completion_id(), "object": "chat.completion", "created": int(time.time()), "model": model}
