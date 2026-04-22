"""
MongoDB utilities for raw ETL payloads and crawl logs.

This layer is optional by design. If MongoDB is unavailable, the ETL
pipeline continues to run against TimescaleDB without failing.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
except Exception:  # pragma: no cover - keeps project usable without pymongo
    MongoClient = None
    PyMongoError = Exception


class MongoManager:
    """Store raw API payloads and ETL crawl logs in MongoDB."""

    def __init__(
        self,
        uri: str | None = None,
        database: str | None = None,
        enabled: bool | None = None,
    ) -> None:
        self.enabled = self._resolve_enabled(enabled)
        self.client = None
        self.db = None
        self.available = False

        if not self.enabled:
            print("[MongoDB] Disabled by configuration.")
            return

        if MongoClient is None:
            print("[MongoDB] pymongo is not installed. Skipping MongoDB integration.")
            return

        if uri is None:
            uri = self._default_uri()
        if database is None:
            database = os.getenv("MONGO_DATABASE", "vnstock_raw")

        try:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            self.client.admin.command("ping")
            self.db = self.client[database]
            self._ensure_indexes()
            self.available = True
            print(f"[MongoDB] Connected: {database}")
        except Exception as exc:
            print(f"[MongoDB] Unavailable, continuing without it: {exc}")

    def _resolve_enabled(self, enabled: bool | None) -> bool:
        if enabled is not None:
            return enabled
        return os.getenv("MONGO_ENABLED", "true").strip().lower() not in {"0", "false", "no"}

    def _default_uri(self) -> str:
        if os.path.exists("/.dockerenv"):
            host = os.getenv("MONGO_HOST", "mongodb")
        else:
            host = os.getenv("MONGO_HOST", "localhost")
        port = os.getenv("MONGO_PORT", "27017")
        username = os.getenv("MONGO_USERNAME")
        password = os.getenv("MONGO_PASSWORD")

        if username and password:
            return f"mongodb://{username}:{password}@{host}:{port}/"
        return f"mongodb://{host}:{port}/"

    def _ensure_indexes(self) -> None:
        if not self.db:
            return
        self.db.raw_payloads.create_index(
            [("run_id", 1), ("dataset", 1), ("symbol", 1), ("fetched_at", -1)]
        )
        self.db.crawl_logs.create_index(
            [("run_id", 1), ("stage", 1), ("symbol", 1), ("logged_at", -1)]
        )

    def _normalize(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, dict):
            return {str(k): self._normalize(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._normalize(v) for v in value]
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        if hasattr(value, "item"):
            try:
                return self._normalize(value.item())
            except Exception:
                pass
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        return value

    def save_raw_payload(
        self,
        dataset: str,
        symbol: str | None,
        source: str,
        payload: Any,
        run_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        if not self.available or not self.db:
            return False
        document = {
            "run_id": run_id,
            "dataset": dataset,
            "symbol": symbol,
            "source": source,
            "payload": self._normalize(payload),
            "metadata": self._normalize(metadata or {}),
            "fetched_at": datetime.utcnow(),
        }
        try:
            self.db.raw_payloads.insert_one(document)
            return True
        except PyMongoError as exc:
            print(f"[MongoDB] Failed to store raw payload '{dataset}': {exc}")
            return False

    def log_crawl(
        self,
        run_id: str,
        stage: str,
        symbol: str | None,
        status: str,
        message: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> bool:
        if not self.available or not self.db:
            return False
        document = {
            "run_id": run_id,
            "stage": stage,
            "symbol": symbol,
            "status": status,
            "message": message,
            "extra": self._normalize(extra or {}),
            "logged_at": datetime.utcnow(),
        }
        try:
            self.db.crawl_logs.insert_one(document)
            return True
        except PyMongoError as exc:
            print(f"[MongoDB] Failed to write crawl log '{stage}': {exc}")
            return False
