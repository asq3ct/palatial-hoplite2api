"""Loopback OpenAI-compatible HTTP facade for Hoplite ACP."""

import argparse
import asyncio
from contextlib import asynccontextmanager, suppress
import hmac
import json
import os
import re
import subprocess
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn

from .acp import ACPClient, UpstreamError
from .bridge import Bridge, Conflict, completion_base
from .bridge import Capacity
from .admission import Limits, RateLimit
from .journal import DispatchJournal, ReplayConflict
from .protocol import RequestError, assistant_message, parse_request


UPSTREAM = "https://api.hoplite.sh"


def load_key():
    key = os.environ.get("HOPLITE_API_KEY")
    if key is None:
        try:
            key = subprocess.run(["omp", "token", "hoplite"], check=True, capture_output=True, text=True).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            raise RuntimeError("Unable to load Hoplite credential from omp") from None
    if not key or "\n" in key or "\r" in key:
        raise RuntimeError("Hoplite credential is empty or invalid")
    return key


def model_rows(data):
    if not isinstance(data, dict) or not isinstance(data.get("models"), list):
        raise UpstreamError("Invalid upstream model roster")
    rows = []
    for model in data["models"]:
        if not isinstance(model, dict) or not isinstance(model.get("id"), str) or not model["id"]:
            continue
        row = {"id": model["id"], "object": "model", "owned_by": model.get("provider", "hoplite"),
               "input_modalities": ["text"], "output_modalities": ["text"],
               "architecture": {"input_modalities": ["text"], "output_modalities": ["text"]}}
        if isinstance(model.get("name"), str):
            row["name"] = model["name"]
        capabilities = model.get("capabilities")
        if isinstance(capabilities, dict):
            for source, target in (("contextWindowTokens", "context_length"), ("maxOutputTokens", "max_output_tokens")):
                if source in capabilities:
                    row[target] = capabilities[source]
            levels = capabilities.get("reasoningLevels")
            if isinstance(levels, list):
                levels = [level for level in levels if isinstance(level, str)]
                if levels:
                    row["supported_reasoning_levels"] = levels
                    config = model.get("runConfig")
                    if not isinstance(config, dict):
                        config = data.get("runConfig")
                    default = config.get("defaultReasoning") if isinstance(config, dict) else None
                    row["default_reasoning_level"] = default if default in levels else levels[0]
        pricing = model.get("pricing")
        if isinstance(pricing, dict):
            cost = {}
            for source, target in (("inputMicrosPerMTok", "input_usd_per_million_tokens"), ("outputMicrosPerMTok", "output_usd_per_million_tokens")):
                if isinstance(pricing.get(source), (int, float)) and not isinstance(pricing[source], bool):
                    cost[target] = pricing[source] / 1_000_000
            if cost:
                row["pricing"] = cost
        rows.append(row)
    return rows


def create_app(project, key, *, base_url=UPSTREAM, client_factory=ACPClient.open, http_client=None, idle_timeout=300, roster_ttl=60,
               bridge_key=None, require_tenant=False, limits=None, journal_path=None, allowed_models=None):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", project):
        raise ValueError("project must be a safe plaintext project ID")
    if not key:
        raise ValueError("credential must not be empty")
    limits = limits or Limits()
    if allowed_models is not None and (not isinstance(allowed_models, list) or not 1 <= len(allowed_models) <= 64 or any(not isinstance(m, str) or not 1 <= len(m) <= 256 for m in allowed_models)):
        raise ValueError("Supply 1-64 exact allowed model IDs")
    local_key = key if bridge_key is None else bridge_key
    if not local_key or (bridge_key is not None and hmac.compare_digest(local_key, key)):
        raise ValueError("Bridge credential must be nonempty and separate from the upstream key")
    bridge = Bridge(key, project, base_url, client_factory, idle_timeout,
                    max_sessions=limits.sessions, max_tenant_sessions=limits.tenant_sessions, turn_timeout=limits.turn_seconds)
    rate = RateLimit(limits.requests_per_minute)
    journal = DispatchJournal(journal_path) if journal_path else None
    http = http_client or httpx.AsyncClient(base_url=base_url, timeout=20)
    roster = {"rows": None, "until": 0.0}
    roster_lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        yield
        await bridge.close()
        await http.aclose()
        if journal:
            journal.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.bridge = bridge

    def authorized(request):
        header = request.headers.get("authorization", "")
        scheme, sep, token = header.partition(" ")
        return bool(sep and scheme.lower() == "bearer" and hmac.compare_digest(token.encode(), local_key.encode()))

    def error(message, status, code=None):
        return JSONResponse({"error": {"message": message, "type": "invalid_request_error" if status < 500 else "server_error", "code": code}}, status_code=status)

    async def models():
        async with roster_lock:
            now = time.monotonic()
            if roster["rows"] is None or now >= roster["until"]:
                try:
                    response = await http.get("/api/model-providers", headers={"X-Api-Key": key})
                    response.raise_for_status()
                    rows = model_rows(response.json())
                    if allowed_models is not None:
                        rows = [row for row in rows if row["id"] in allowed_models]
                except (httpx.HTTPError, ValueError):
                    raise UpstreamError("Unable to fetch upstream model roster") from None
                roster.update(rows=rows, until=now + roster_ttl)
            return roster["rows"]

    @app.get("/health")
    async def health():
        return {"status": "ok", "scope": "process_liveness", "generation_verified": False}

    @app.get("/v1/capabilities")
    async def capabilities(request: Request):
        if not authorized(request):
            return error("Invalid API key", 401)
        return {"experimental": True, "protocols": ["chat_completions"], "modalities": ["text"],
                "adapter_validated_json_object": True, "structured_stream_buffered_until_validated": True,
                "local_tools": True, "native_strict_json_schema": False, "hard_output_token_cap": False,
                "max_body_bytes": limits.body_bytes, "max_tools": 64, "max_tool_result_bytes": 524288,
                "tool_result_deadline_seconds": 60, "turn_deadline_seconds": limits.turn_seconds,
                "usage_scope": "hoplite_thread", "paid_customer_qualified": False,
                "note": "Cloud agent harness; local tools execute only in the client. Thread counters are not per-request token usage."}

    @app.get("/v1/models")
    async def list_models(request: Request):
        if not authorized(request):
            return error("Invalid API key", 401)
        try:
            return {"object": "list", "data": await models()}
        except UpstreamError as exc:
            return error(str(exc), 502)

    async def disconnected(request):
        while True:
            if await request.is_disconnected():
                return
            await asyncio.sleep(0.1)

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        if not authorized(request):
            return error("Invalid API key", 401)
        try:
            tenant = request.headers.get("x-palatial-tenant", "") if require_tenant else "local"
            if require_tenant and not re.fullmatch(r"[a-f0-9]{32,64}", tenant):
                return error("Authenticated gateway tenant context is required", 400, "tenant_required")
            raw = bytearray()
            async for chunk in request.stream():
                if len(raw) + len(chunk) > limits.body_bytes:
                    return error("Experimental bridge request body exceeds its limit", 413, "body_too_large")
                raw.extend(chunk)
            body = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Non-finite JSON is unsupported")))
            parsed = parse_request(body)
            parsed["_tenant"] = tenant
            if not bridge.valid_continuation(parsed) and not rate.admit(tenant):
                response = error("Experimental bridge new-prompt limit reached", 429, "rate_limit_exceeded")
                response.headers["Retry-After"] = "60"
                return response
            if parsed["messages"][-1]["role"] != "tool":
                if parsed["model"] not in {row["id"] for row in await models()}:
                    return error("Model is not in the authorized upstream roster", 400, "model_not_found")
        except (ValueError, RequestError) as exc:
            return error(str(exc), 400)
        except UpstreamError as exc:
            return error(str(exc), 502)

        session = None
        attempt_key = request.headers.get("idempotency-key")
        if attempt_key is not None:
            if not re.fullmatch(r"[A-Za-z0-9_.:-]{8,128}", attempt_key):
                return error("Invalid idempotency key", 400, "invalid_idempotency_key")
            if journal is None:
                return error("Durable idempotency is not configured on this experimental instance", 503, "journal_required")
            try:
                journal.claim(tenant, attempt_key, bytes(raw))
            except ReplayConflict as exc:
                return error(str(exc), 409, "attempt_already_claimed")
        acquire_task = asyncio.create_task(bridge.acquire(parsed))
        disconnect_task = asyncio.create_task(disconnected(request))
        try:
            done, _ = await asyncio.wait({acquire_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED)
            if disconnect_task in done:
                acquire_task.cancel()
                with suppress(asyncio.CancelledError):
                    session = await acquire_task
                if session:
                    await bridge.close_session(session)
                return error("Client disconnected", 499)
            session = await acquire_task
        except Conflict as exc:
            return error(str(exc), 409)
        except Capacity as exc:
            response = error(str(exc), 429, "session_capacity")
            response.headers["Retry-After"] = "5"
            return response
        except UpstreamError as exc:
            return error(str(exc), 502)
        except RequestError as exc:
            return error(str(exc), 400)
        finally:
            if not acquire_task.done():
                acquire_task.cancel()
                with suppress(asyncio.CancelledError):
                    await acquire_task
            if session is None and acquire_task.done() and not acquire_task.cancelled() and acquire_task.exception() is None:
                await bridge.close_session(acquire_task.result())
            disconnect_task.cancel()
            with suppress(asyncio.CancelledError):
                await disconnect_task
        base = completion_base(parsed["model"])

        async def generate():
            text = ""
            json_mode = parsed["response_format"]["type"] == "json_object"
            output_bytes = 0
            committed = False
            try:
                yield ("role", {"role": "assistant"})
                async for event in bridge.events(session):
                    if event["type"] == "text":
                        output_bytes += len(event["text"].encode("utf-8"))
                        if output_bytes > limits.output_bytes:
                            raise UpstreamError("Experimental bridge output byte limit reached; run cancelled", "output_too_large")
                        text += event["text"]
                        if not json_mode:
                            yield ("text", event["text"])
                    elif event["type"] == "tool_call":
                        if hasattr(session.client, "read_usage"):
                            snapshot = await session.client.read_usage()
                            yield ("reported_usage", snapshot.public() if snapshot else None)
                        call = bridge.publish_tool(session, event, text)
                        yield ("tool", call)
                        bridge.commit_tool(session)
                        yield ("finish", "tool_calls")
                        committed = True
                    else:
                        if event.get("stop_reason") != "end_turn":
                            raise UpstreamError("Upstream prompt ended without completing the response")
                        if json_mode:
                            try:
                                def pairs(items):
                                    result = {}
                                    for k, v in items:
                                        if k in result:
                                            raise ValueError("Duplicate JSON key")
                                        result[k] = v
                                    return result
                                value = json.loads(text, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                                if not isinstance(value, dict):
                                    raise ValueError("JSON object required")
                            except (ValueError, RecursionError):
                                raise UpstreamError("Upstream did not produce a valid JSON object; no automatic regeneration was attempted", "structured_output_invalid") from None
                            yield ("text", text)
                        if (parsed["tool_choice"] == "required" or isinstance(parsed["tool_choice"], dict)) and not session.had_tool:
                            raise UpstreamError("Upstream finished without making the required local tool call")
                        if hasattr(session.client, "read_usage"):
                            snapshot = await session.client.read_usage()
                            yield ("reported_usage", snapshot.public() if snapshot else None)
                        bridge.commit_done(session, text)
                        yield ("finish", "stop")
                        committed = True
            finally:
                if not committed:
                    await bridge.close_session(session)

        if parsed["stream"]:
            async def stream():
                iterator = generate()
                pending = None
                try:
                    while True:
                        if pending is None:
                            pending = asyncio.create_task(anext(iterator))
                        done, _ = await asyncio.wait({pending}, timeout=limits.heartbeat_seconds)
                        if not done:
                            yield ": keep-alive\n\n"
                            continue
                        try:
                            kind, value = pending.result()
                        except StopAsyncIteration:
                            break
                        pending = None
                        if kind == "reported_usage":
                            yield "data: " + json.dumps({**base, "object": "chat.completion.chunk", "choices": [], "hoplite_usage": value}) + "\n\n"
                            continue
                        if kind == "role":
                            delta, finish = value, None
                        elif kind == "text":
                            delta, finish = {"content": value}, None
                        elif kind == "tool":
                            delta, finish = {"tool_calls": [{"index": 0, **value}]}, None
                        else:
                            delta, finish = {}, value
                        chunk = {**base, "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
                        yield "data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n"
                except (UpstreamError, Conflict) as exc:
                    yield "data: " + json.dumps({"error": {"message": str(exc), "type": "server_error"}}) + "\n\n"
                except Exception:
                    yield "data: " + json.dumps({"error": {"message": "Proxy response failed", "type": "server_error"}}) + "\n\n"
                finally:
                    if pending and not pending.done():
                        pending.cancel()
                        with suppress(asyncio.CancelledError):
                            await pending
                    await iterator.aclose()
                yield "data: [DONE]\n\n"
            return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no", "X-Palatial-Experimental": "hoplite"})

        async def collect():
            text, call, finish = "", None, None
            reported_usage = None
            iterator = generate()
            try:
                async for kind, value in iterator:
                    if kind == "text":
                        text += value
                    elif kind == "tool":
                        call = value
                    elif kind == "finish":
                        finish = value
                    elif kind == "reported_usage":
                        reported_usage = value
                return {**base, "choices": [{"index": 0, "message": assistant_message(text, call), "finish_reason": finish}], "hoplite_usage": reported_usage}
            finally:
                await iterator.aclose()

        task = asyncio.create_task(collect())
        watcher = asyncio.create_task(disconnected(request))
        try:
            done, _ = await asyncio.wait({task, watcher}, return_when=asyncio.FIRST_COMPLETED)
            if watcher in done:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
                return error("Client disconnected", 499)
            return await task
        except UpstreamError as exc:
            return error(str(exc), 502)
        except Exception:
            return error("Proxy response failed", 500)
        finally:
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    return app


def main():
    parser = argparse.ArgumentParser(description="Serve Hoplite ACP on loopback")
    parser.add_argument("--project", required=True)
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--gateway-mode", action="store_true", help="Require authenticated gateway tenant context")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", args.project):
        parser.error("--project must be a safe plaintext project ID")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    try:
        bridge_key = os.environ.get("HOPLITE_BRIDGE_KEY")
        if not bridge_key or len(bridge_key) < 32:
            raise RuntimeError("Set a separate random HOPLITE_BRIDGE_KEY of at least 32 characters")
        journal_path = os.environ.get("HOPLITE_DISPATCH_JOURNAL")
        if args.gateway_mode and not journal_path:
            raise RuntimeError("Gateway mode requires a durable HOPLITE_DISPATCH_JOURNAL path")
        allowed_models = os.environ.get("HOPLITE_ALLOWED_MODELS", "gpt-6.1-sol,claude-opus-5-5").split(",")
        app = create_app(args.project, load_key(), bridge_key=bridge_key, require_tenant=args.gateway_mode, journal_path=journal_path, allowed_models=allowed_models)
    except RuntimeError as exc:
        parser.error(str(exc))
    uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
