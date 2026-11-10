"""Durable, content-free dispatch tombstones; never replay a local edit."""

import hashlib
from pathlib import Path
import sqlite3
import time


class ReplayConflict(Exception):
    pass


class DispatchJournal:
    def __init__(self, filename):
        self.db = sqlite3.connect(str(filename), isolation_level=None)
        self.db.execute("PRAGMA busy_timeout=2000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("CREATE TABLE IF NOT EXISTS dispatch (tenant TEXT, key TEXT, digest TEXT NOT NULL, created INTEGER NOT NULL, PRIMARY KEY(tenant,key))")

    def claim(self, tenant, key, body):
        # Persist hashes only, no prompts, user files, results, or credentials.
        tenant = hashlib.sha256(tenant.encode()).hexdigest()
        key = hashlib.sha256(key.encode()).hexdigest()
        digest = hashlib.sha256(body).hexdigest()
        try:
            self.db.execute("INSERT INTO dispatch VALUES(?,?,?,?)", (tenant, key, digest, int(time.time())))
        except sqlite3.IntegrityError:
            raise ReplayConflict("This idempotency key already claimed an attempt; automatic replay is blocked to prevent duplicate agent work") from None

    def close(self):
        self.db.close()
