"""Hoplite ACP v1 transport and local-tool request lifecycle."""

import asyncio
import json
import logging
import re
import httpx
from urllib.parse import urlencode, urlsplit, urlunsplit

from websockets.asyncio.client import connect
from .usage import UsageSnapshot, UsageError
from . import __version__


_MAX_FRAME = 1024 * 1024
_REQUEST_TIMEOUT = 30.0
_SEND_TIMEOUT = 10.0
_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}\Z")
_LOGGER = logging.Logger("hoplite2api.acp.websocket")
_LOGGER.disabled = True


class UpstreamError(Exception):
    def __init__(self, message: str, code=None):
        super().__init__(message)
        self.code = code


class ACPClient:
    def __init__(self, ws, key: str):
        self._ws = ws
        self._key = key
        self._pending = {}
        self._tool_calls = {}
        self._tools = set()
        self._next_id = 0
        self._prompt = None
        self._closing = False
        self._closed = False
        self._close_lock = asyncio.Lock()
        self.session_id = ""
        self.tool_timeout = 60.0
        self.max_result_bytes = 524288
        self.events = asyncio.Queue(maxsize=256)
        self.config_options = []
        self.reported_usage = None
        self.usage_error = None
        self.selected_model = None
        self.expected_run_count = 0
        self._rest_base = None
        self._reader = asyncio.create_task(self._read_loop())

    @classmethod
    async def open(cls, base_url: str, key: str, project_id: str):
        parts = urlsplit(base_url)
        schemes = {"http": "ws", "https": "wss", "ws": "ws", "wss": "wss"}
        if parts.scheme not in schemes or not parts.netloc:
            raise UpstreamError("Invalid upstream URL")
        url = urlunsplit((schemes[parts.scheme], parts.netloc, "/acp", urlencode({"projectId": project_id}), ""))
        try:
            ws = await connect(url, additional_headers={"X-Api-Key": key}, max_size=_MAX_FRAME, close_timeout=2, logger=_LOGGER)
        except Exception:
            raise UpstreamError("Unable to connect to ACP upstream") from None
        client = cls(ws, key)
        client._rest_base = base_url.rstrip("/")
        try:
            init = await client._request("initialize", {"protocolVersion": 1, "clientCapabilities": {}, "clientInfo": {"name": "palatial-hoplite2api", "version": __version__}})
            if init.get("protocolVersion") != 1:
                raise UpstreamError("Upstream did not negotiate ACP protocol version 1")
            await client._request("authenticate", {"methodId": "hoplite-api-key"})
            session = await client._request("session/new", {"cwd": "/tmp", "mcpServers": []})
            client.session_id = session.get("sessionId")
            if not isinstance(client.session_id, str) or not client.session_id:
                raise UpstreamError("Upstream returned an invalid session ID")
            client.config_options = session.get("configOptions", [])
            usage = session.get("_meta", {}).get("hoplite", {}).get("usage")
            if usage is not None:
                client.accept_usage(usage)
            return client
        except BaseException:
            await client.close()
            raise

    def _provider_error(self, error):
        if not isinstance(error, dict):
            return UpstreamError("Invalid upstream error")
        message = error.get("message")
        if not isinstance(message, str):
            message = "Upstream request failed"
        if self._key:
            message = message.replace(self._key, "[redacted]")
        message = re.sub(r"(?:hop_|pa_|sk-|ksk_)[A-Za-z0-9_-]{16,}", "[redacted]", message)
        data = error.get("data")
        code = data.get("code") if isinstance(data, dict) else None
        if not isinstance(code, (str, int)):
            code = error.get("code")
        if not isinstance(code, (str, int)):
            code = None
        if isinstance(code, str):
            code = code.replace(self._key, "[redacted]")[:128] if self._key else code[:128]
        return UpstreamError(message[:512], code)

    def _emit(self, event):
        try:
            self.events.put_nowait(event)
        except asyncio.QueueFull:
            while not self.events.empty():
                self.events.get_nowait()
            failure = UpstreamError("Client is not consuming agent events; run cancelled", "event_backlog")
            self.events.put_nowait({"type": "error", "error": failure})
            raise failure

    async def _send(self, message):
        try:
            data = json.dumps(message, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            if len(data.encode("utf-8")) > _MAX_FRAME:
                raise UpstreamError("ACP message exceeds the 1 MiB limit", "body_too_large")
            await asyncio.wait_for(self._ws.send(data), timeout=_SEND_TIMEOUT)
        except UpstreamError:
            raise
        except TimeoutError as exc:
            raise UpstreamError("ACP send timed out", "timeout") from exc
        except (TypeError, ValueError, UnicodeError) as exc:
            raise UpstreamError("ACP message is not valid JSON") from exc
        except Exception as exc:
            raise UpstreamError("ACP connection is closed") from exc

    async def _begin_request(self, method, params):
        if self._closing or self._closed:
            raise UpstreamError("ACP connection is closed")
        self._next_id += 1
        rpc_id = self._next_id
        future = asyncio.get_running_loop().create_future()
        self._pending[rpc_id] = future
        try:
            await self._send({"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params})
        except BaseException:
            self._pending.pop(rpc_id, None)
            future.cancel()
            raise
        return rpc_id, future

    async def _request(self, method, params):
        rpc_id = None
        try:
            async with asyncio.timeout(_REQUEST_TIMEOUT):
                rpc_id, future = await self._begin_request(method, params)
                result = await future
                if not isinstance(result, dict):
                    raise UpstreamError("Invalid ACP response")
                return result
        except TimeoutError as exc:
            raise UpstreamError("ACP request timed out", "timeout") from exc
        finally:
            if rpc_id is not None:
                self._pending.pop(rpc_id, None)

    async def set_model(self, model: str, reasoning: str | None = None):
        for option_id, requested in (("model", model), ("reasoning", reasoning)):
            if requested is None:
                continue
            option = next((o for o in self.config_options if isinstance(o, dict) and o.get("id") == option_id), None)
            if not option:
                raise UpstreamError(f"Upstream does not advertise {option_id} options")
            values = set()
            for item in option.get("options", []):
                if isinstance(item, dict):
                    if isinstance(item.get("value"), str):
                        values.add(item["value"])
                    for nested in item.get("options", []):
                        if isinstance(nested, dict) and isinstance(nested.get("value"), str):
                            values.add(nested["value"])
            value = "none" if option_id == "reasoning" and requested == "off" and "off" not in values and "none" in values else requested
            if value not in values:
                raise UpstreamError(f"Requested {option_id} is not advertised by upstream", "invalid_" + option_id)
            response = await self._request("session/set_config_option", {"sessionId": self.session_id, "configId": option_id, "value": value})
            options = response.get("configOptions")
            if not isinstance(options, list):
                raise UpstreamError("Upstream returned invalid configuration options")
            self.config_options = options
            selected = next((o for o in options if isinstance(o, dict) and o.get("id") == option_id), {})
            if selected.get("currentValue") != value:
                raise UpstreamError(f"Upstream did not confirm requested {option_id}", "configuration_mismatch")
        self.selected_model = model
        if self.reported_usage and self.reported_usage.model != model:
            self.reported_usage = None

    def accept_usage(self, value):
        try:
            snapshot = UsageSnapshot.parse(value)
            if self.selected_model and snapshot.model != self.selected_model:
                raise UsageError("Reported usage model differs from selected model")
            # Never combine counters across silent model changes or resets.
            snapshot.delta(self.reported_usage)
            self.reported_usage = snapshot
            self.usage_error = None
        except UsageError:
            self.usage_error = "reported_usage_unavailable"

    async def read_usage(self):
        try:
            async with asyncio.timeout(3):
                result = await self._request("_hoplite/thread/usage", {"sessionId": self.session_id})
                self.accept_usage(result.get("usage", result))
        except (UpstreamError, TimeoutError):
            self.usage_error = "reported_usage_unavailable"
        # ACP can return its pre-run snapshot just after stop. The official REST
        # usage endpoint exposes persisted thread counters; it is not inference.
        if not self._usage_current() and getattr(self, "_rest_base", None):
            try:
                async with asyncio.timeout(5):
                    async with httpx.AsyncClient(timeout=2, trust_env=False, follow_redirects=False) as http:
                        for attempt in range(3):
                            response = await http.get(self._rest_base + "/api/threads/" + self.session_id + "/usage",
                                                      headers={"X-Api-Key": self._key})
                            response.raise_for_status()
                            value = UsageSnapshot.parse_rest(response.json().get("usage"))
                            if value.model == self.selected_model and value.run_count >= self.expected_run_count:
                                value.delta(self.reported_usage)
                                self.reported_usage, self.usage_error = value, None
                                break
                            await asyncio.sleep(0.25 * (attempt + 1))
            except (httpx.HTTPError, ValueError, UsageError, TimeoutError):
                self.usage_error = "reported_usage_unavailable"
        if not self._usage_current():
            return None
        return self.reported_usage

    def _usage_current(self):
        if self.usage_error or not self.reported_usage or self.reported_usage.run_count < self.expected_run_count:
            return False
        return self.reported_usage.model == self.selected_model

    async def register_tools(self, tools: list[dict]) -> None:
        names = [tool.get("name") if isinstance(tool, dict) else None for tool in tools]
        if len(tools) > 64 or any(not isinstance(name, str) or not _NAME.fullmatch(name) for name in names) or len(set(names)) != len(names):
            raise UpstreamError("Invalid local tool names or count")
        servers = [{"name": "omp", "writes": True, "trusted": True, "tools": tools}] if tools else []
        response = await self._request("_hoplite/client_tools/register", {"sessionId": self.session_id, "servers": servers})
        self._tools = set()
        accepted = response.get("servers")
        if not isinstance(accepted, list) or response.get("dropped") != []:
            raise UpstreamError("Upstream did not accept local tools")
        if tools:
            if len(accepted) != 1 or not isinstance(accepted[0], dict) or accepted[0].get("name") != "omp" or accepted[0].get("hiddenWriteTools", 0) != 0:
                raise UpstreamError("Upstream did not accept local tools")
            received = accepted[0].get("tools")
            if not isinstance(received, list) or any(not isinstance(name, str) for name in received) or len(received) != len(names) or set(received) != set(names):
                raise UpstreamError("Upstream did not accept all local tools")
        elif accepted:
            raise UpstreamError("Upstream did not clear local tools")
        limits = response.get("limits", {})
        if not isinstance(limits, dict):
            raise UpstreamError("Invalid upstream local tool limits")
        timeout = limits.get("callTimeoutMs", 60000)
        size = limits.get("maxResultBytes", 524288)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout < float("inf") or isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise UpstreamError("Invalid upstream local tool limits")
        self.tool_timeout = timeout / 1000
        self.max_result_bytes = size
        self._tools = set(names)

    async def start_prompt(self, text: str) -> None:
        async with self._close_lock:
            if self._prompt is not None:
                raise UpstreamError("A prompt is already active")
            self.expected_run_count = max(self.expected_run_count, self.reported_usage.run_count if self.reported_usage else 0) + 1
            rpc_id, future = await self._begin_request("session/prompt", {"sessionId": self.session_id, "prompt": [{"type": "text", "text": text}]})
            self._prompt = asyncio.create_task(self._finish_prompt(rpc_id, future))

    async def _finish_prompt(self, rpc_id, future):
        try:
            result = await future
            if not isinstance(result, dict) or not isinstance(result.get("stopReason"), str):
                raise UpstreamError("Invalid upstream prompt result")
            self._emit({"type": "done", "stop_reason": result["stopReason"]})
        except UpstreamError as exc:
            self._emit({"type": "error", "error": exc})
        finally:
            self._pending.pop(rpc_id, None)
            self._prompt = None

    async def reply_tool(self, rpc_id: str | int, text: str, is_error: bool = False) -> None:
        if rpc_id not in self._tool_calls:
            raise UpstreamError("Local tool request is no longer pending")
        result = {"content": [{"type": "text", "text": text}], "isError": is_error}
        try:
            size = len(json.dumps(result, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
        except (TypeError, ValueError, UnicodeError) as exc:
            raise UpstreamError("Invalid local tool result") from exc
        if size > self.max_result_bytes:
            raise UpstreamError("Local tool result exceeds upstream limit", "result_too_large")
        await self._send({"jsonrpc": "2.0", "id": rpc_id, "result": result})
        self._tool_calls.pop(rpc_id, None)

    async def _server_error(self, rpc_id, code, message):
        await self._send({"jsonrpc": "2.0", "id": rpc_id, "error": {"code": code, "message": message}})

    async def _handle_server_request(self, message):
        rpc_id = message["id"]
        method = message.get("method")
        params = message.get("params")
        if method == "_hoplite/client_tools/call":
            if not isinstance(params, dict) or params.get("sessionId") != self.session_id or params.get("server") != "omp" or params.get("tool") not in self._tools or not isinstance(params.get("callId"), str) or not 1 <= len(params["callId"]) <= 128 or not isinstance(params.get("arguments"), dict) or rpc_id in self._tool_calls or len(self._tool_calls) >= 64:
                await self._server_error(rpc_id, -32602, "Invalid or unregistered local tool call")
                return
            self._tool_calls[rpc_id] = True
            self._emit({"type": "tool_call", "rpc_id": rpc_id, "call_id": params["callId"], "name": params["tool"], "arguments": params["arguments"], "server": "omp", "deadline": asyncio.get_running_loop().time() + self.tool_timeout})
        elif method == "session/request_permission":
            await self._server_error(rpc_id, -32000, "Interactive permission is unavailable")
            self._emit({"type": "error", "error": UpstreamError("Upstream requires interactive permission", "permission_required")})
        else:
            await self._server_error(rpc_id, -32601, "Method not supported")

    async def _read_loop(self):
        failure = UpstreamError("ACP upstream disconnected")
        try:
            async for frame in self._ws:
                try:
                    message = json.loads(frame)
                except (ValueError, UnicodeError, TypeError):
                    raise UpstreamError("Invalid ACP message from upstream")
                if not isinstance(message, dict):
                    raise UpstreamError("Invalid ACP message from upstream")
                if "method" in message:
                    if "id" in message:
                        await self._handle_server_request(message)
                    elif message.get("method") == "session/update":
                        params = message.get("params", {})
                        update = params.get("update", {}) if isinstance(params, dict) else {}
                        if not isinstance(update, dict) or params.get("sessionId") != self.session_id:
                            continue
                        if update.get("sessionUpdate") == "agent_message_chunk":
                            content = update.get("content", {})
                            if isinstance(content, dict) and content.get("type") == "text" and isinstance(content.get("text"), str):
                                self._emit({"type": "text", "text": content["text"]})
                        elif update.get("sessionUpdate") == "config_option_update" and isinstance(update.get("configOptions"), list):
                            self.config_options = update["configOptions"]
                        elif update.get("sessionUpdate") == "usage_update":
                            self.accept_usage(update)
                elif "id" in message:
                    future = self._pending.pop(message["id"], None)
                    if future is not None and not future.done():
                        if "error" in message:
                            future.set_exception(self._provider_error(message["error"]))
                        elif "result" in message:
                            future.set_result(message["result"])
                        else:
                            future.set_exception(UpstreamError("Invalid ACP response"))
        except asyncio.CancelledError:
            raise
        except UpstreamError as exc:
            failure = exc
        except Exception:
            pass
        finally:
            self._closed = True
            self._tool_calls.clear()
            pending = list(self._pending.values())
            self._pending.clear()
            for future in pending:
                if not future.done():
                    future.set_exception(failure)
            if not self._closing and self._prompt is None:
                self._emit({"type": "error", "error": failure})

    async def close(self) -> None:
        async with self._close_lock:
            if self._closing:
                return
            self._closing = True
            if not self._closed:
                if self._prompt is not None:
                    try:
                        await self._send({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": self.session_id}})
                    except UpstreamError:
                        pass
                for rpc_id in list(self._tool_calls):
                    try:
                        await self._server_error(rpc_id, -32000, "Client connection closing")
                    except UpstreamError:
                        break
                    self._tool_calls.pop(rpc_id, None)
                if self._prompt is not None:
                    try:
                        await asyncio.wait_for(asyncio.shield(self._prompt), timeout=2)
                    except (asyncio.TimeoutError, UpstreamError):
                        pass
            await self._ws.close()
            await self._reader
            if self._prompt is not None:
                await self._prompt
