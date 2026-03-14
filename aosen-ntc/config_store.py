#!/usr/bin/env python3
"""
Shared MongoDB-backed config storage.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.errors import PyMongoError

MONGO_URI_DEFAULT = "mongodb://admin:MongoAdmin2026!@1.94.238.248:8443/?authSource=admin"
MONGO_URI = os.getenv("MONGO_URI", MONGO_URI_DEFAULT)
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "ticket_bpt")
CONFIG_COLLECTION = "config"
LEGACY_CONFIG_COLLECTION = "cron_configs"
CONFIG_DOC_ID = "default"

_mongo_client: MongoClient | None = None
_mongo_db: Any | None = None
log = logging.getLogger("config_store")


def get_mongo_db() -> Any | None:
    global _mongo_client, _mongo_db
    if _mongo_db is not None:
        return _mongo_db
    try:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        _mongo_client.admin.command("ping")
        _mongo_db = _mongo_client[MONGO_DB_NAME]
        _mongo_db["user_cookies"].create_index("mobile", unique=True)
        return _mongo_db
    except Exception as exc:  # noqa: BLE001
        log.warning("MongoDB unavailable: %s", exc)
        _mongo_client = None
        _mongo_db = None
        return None


def _strip_meta(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in doc.items() if k not in {"_id", "updated_at"}}


def _find_config_doc(db: Any, collection: str) -> Dict[str, Any] | None:
    try:
        doc = db[collection].find_one({"_id": CONFIG_DOC_ID})
    except PyMongoError as exc:
        log.warning("Load config from %s failed: %s", collection, exc)
        return None
    if not isinstance(doc, dict):
        return None
    return _strip_meta(doc)


def load_config() -> Dict[str, Any]:
    db = get_mongo_db()
    if db is None:
        raise RuntimeError("MongoDB is unavailable, cannot load config.")

    cfg = _find_config_doc(db, CONFIG_COLLECTION)
    if isinstance(cfg, dict):
        return cfg

    # One-time migration path: read old collection and seed new one.
    legacy_cfg = _find_config_doc(db, LEGACY_CONFIG_COLLECTION)
    if isinstance(legacy_cfg, dict):
        save_config(legacy_cfg)
        return legacy_cfg

    return {}


def load_all_user_credentials() -> List[Dict[str, Any]]:
    """Load all user accounts from user_cookies collection."""
    db = get_mongo_db()
    if db is None:
        return []
    try:
        return list(db["user_cookies"].find({}, {"_id": 0}))
    except PyMongoError as exc:
        log.warning("Load user credentials failed: %s", exc)
        return []


def save_config(cfg: Dict[str, Any]) -> None:
    db = get_mongo_db()
    if db is None:
        raise RuntimeError("MongoDB is unavailable, cannot save config.")

    doc = {
        "_id": CONFIG_DOC_ID,
        **cfg,
        "updated_at": datetime.utcnow(),
    }
    try:
        db[CONFIG_COLLECTION].update_one({"_id": CONFIG_DOC_ID}, {"$set": doc}, upsert=True)
    except PyMongoError as exc:
        raise RuntimeError(f"Save config to MongoDB failed: {exc}") from exc
