"""Explicit limits for the experimental private bridge."""

from dataclasses import dataclass
from collections import deque
import time


@dataclass(frozen=True)
class Limits:
    body_bytes: int = 524288
    output_bytes: int = 1048576
    sessions: int = 16
    tenant_sessions: int = 2
    requests_per_minute: int = 60
    turn_seconds: float = 600
    heartbeat_seconds: float = 10

    def __post_init__(self):
        for name in ("body_bytes", "output_bytes", "sessions", "tenant_sessions", "requests_per_minute"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError("Bridge limits must be positive integers")
        if self.body_bytes > 1048576 or self.output_bytes > 16 * 1048576 or self.sessions > 64 or self.tenant_sessions > self.sessions:
            raise ValueError("Bridge limits exceed supported bounds")
        if not 0 < self.turn_seconds <= 1800 or not 0 < self.heartbeat_seconds <= 30:
            raise ValueError("Invalid bridge deadlines")


class RateLimit:
    def __init__(self, limit):
        self.limit = limit
        self.rows = {}

    def admit(self, tenant, now=None):
        now = time.monotonic() if now is None else now
        # Entries are keyed by authenticated principals, never untrusted headers.
        if len(self.rows) > 4096:
            self.rows = {k: v for k, v in self.rows.items() if v and v[-1] > now - 60}
        row = self.rows.setdefault(tenant, deque())
        while row and row[0] <= now - 60:
            row.popleft()
        if len(row) >= self.limit:
            return False
        row.append(now)
        return True
