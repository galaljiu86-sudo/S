#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Atomic, flock-protected state management for streams, live statuses, and bot config."""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
import threading
import time
from typing import Callable, List

try:
    import fcntl
except ImportError:  # Windows test compatibility
    fcntl = None

DATA_DIR = os.path.abspath(os.environ.get("DATA_DIR", "data"))
STREAMS_PATH = os.path.join(DATA_DIR, "streams.json")
STATUS_PATH = os.path.join(DATA_DIR, "status.json")
CONFIG_PATH = os.path.join(DATA_DIR, "global.json")

_PROCESS_LOCK = threading.RLock()
_LAST_VALID_STREAMS: list = []


def ensure_data_dir() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)


@contextmanager
def _locked_file(path: str):
    """Acquire both an in-process RLock and a POSIX flock for cross-process safety."""
    ensure_data_dir()
    lock_path = path + ".lock"
    with _PROCESS_LOCK:
        with open(lock_path, "a+", encoding="utf-8") as lock_handle:
            if fcntl is not None:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _atomic_write_json(path: str, payload) -> None:
    ensure_data_dir()
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def _read_json_unlocked(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def normalize_stream_entries(data: list) -> list:
    """Ensure every stream dictionary has a stable id and clean schema."""
    normalized: List[dict] = []
    seen_ids = set()
    for idx, entry in enumerate(data):
        if not isinstance(entry, dict):
            continue
        item = dict(entry)
        sid = str(item.get("id") or "").strip()
        if not sid or sid in seen_ids:
            user = str(item.get("tiktok_user") or "stream").strip()
            sid = f"{user}_{idx}"
            while sid in seen_ids:
                sid = f"{sid}_{idx}"
        item["id"] = sid
        seen_ids.add(sid)
        item.setdefault("enabled", True)
        item.setdefault("layout", "portrait_copy")
        item.setdefault("platform", "YouTube")
        # Keep runtime live state solely in status.json
        item.pop("is_live", None)
        normalized.append(item)
    return normalized


def load_statuses() -> dict:
    with _locked_file(STATUS_PATH):
        data = _read_json_unlocked(STATUS_PATH, {})
        return data if isinstance(data, dict) else {}


def update_stream_status(
    stream_id: str,
    tiktok_user: str,
    is_live: bool,
    extra: dict | None = None,
) -> None:
    """Atomically update runtime live state and diagnostics in status.json."""
    with _locked_file(STATUS_PATH):
        statuses = _read_json_unlocked(STATUS_PATH, {})
        if not isinstance(statuses, dict):
            statuses = {}

        current = statuses.get(stream_id, {})
        new_entry = {
            "is_live": bool(is_live),
            "tiktok_user": tiktok_user,
            "updated_at": round(time.time(), 3),
        }
        if extra:
            new_entry.update(extra)

        comparable_cur = {k: v for k, v in current.items() if k != "updated_at"}
        comparable_new = {k: v for k, v in new_entry.items() if k != "updated_at"}
        if comparable_cur == comparable_new:
            return

        statuses[stream_id] = new_entry
        _atomic_write_json(STATUS_PATH, statuses)


def prune_statuses(active_ids: set[str]) -> None:
    """Drop stale entries from status.json for deleted streams."""
    with _locked_file(STATUS_PATH):
        statuses = _read_json_unlocked(STATUS_PATH, {})
        if not isinstance(statuses, dict):
            return
        pruned = {sid: info for sid, info in statuses.items() if sid in active_ids}
        if len(pruned) != len(statuses):
            _atomic_write_json(STATUS_PATH, pruned)


def load_streams(include_status: bool = False) -> list:
    """Safely read streams.json, falling back to last valid snapshot on read error."""
    global _LAST_VALID_STREAMS
    with _locked_file(STREAMS_PATH):
        raw = _read_json_unlocked(STREAMS_PATH, None)
        if isinstance(raw, list):
            streams = normalize_stream_entries(raw)
            _LAST_VALID_STREAMS = [dict(s) for s in streams]
        else:
            streams = [dict(s) for s in _LAST_VALID_STREAMS]

    if include_status:
        statuses = load_statuses()
        for item in streams:
            st = statuses.get(item["id"], {})
            item["is_live"] = bool(st.get("is_live", False))
            item["runtime_status"] = st
    return streams


def save_streams(streams: list, touch_reload: bool = True) -> list:
    """Atomically save streams.json under lock and optionally signal reload."""
    global _LAST_VALID_STREAMS
    cleaned = normalize_stream_entries(streams)
    with _locked_file(STREAMS_PATH):
        _atomic_write_json(STREAMS_PATH, cleaned)
        _LAST_VALID_STREAMS = [dict(s) for s in cleaned]

    if touch_reload:
        try:
            with open("/tmp/reload_restreamer", "w", encoding="utf-8") as handle:
                handle.write(str(time.time()))
        except OSError:
            pass
    return cleaned


def mutate_streams(mutator: Callable[[list], object], touch_reload: bool = True):
    """Read-modify-write streams.json atomically under an exclusive lock."""
    global _LAST_VALID_STREAMS
    with _locked_file(STREAMS_PATH):
        raw = _read_json_unlocked(STREAMS_PATH, [])
        current = normalize_stream_entries(raw if isinstance(raw, list) else [])
        result = mutator(current)
        cleaned = normalize_stream_entries(current)
        _atomic_write_json(STREAMS_PATH, cleaned)
        _LAST_VALID_STREAMS = [dict(s) for s in cleaned]

    if touch_reload:
        try:
            with open("/tmp/reload_restreamer", "w", encoding="utf-8") as handle:
                handle.write(str(time.time()))
        except OSError:
            pass
    return result, cleaned


def load_global_config() -> dict:
    with _locked_file(CONFIG_PATH):
        data = _read_json_unlocked(CONFIG_PATH, {})
        return data if isinstance(data, dict) else {}


def save_global_config(updates: dict) -> dict:
    with _locked_file(CONFIG_PATH):
        current = _read_json_unlocked(CONFIG_PATH, {})
        if not isinstance(current, dict):
            current = {}
        merged = dict(current)
        merged.update(updates)
        if merged != current:
            _atomic_write_json(CONFIG_PATH, merged)
        return merged
