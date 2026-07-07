from __future__ import annotations

import html
import io
import os
import json
import re
import secrets
import sqlite3
import threading
import time
import zipfile
from datetime import datetime
from typing import Any

import requests
from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE = os.path.join(BASE_DIR, "discord_manager.db")
EXPORT_DIR = os.path.join(BASE_DIR, "exports")
API_BASE_URL = os.getenv("DISCORD_API_BASE_URL", "https://discord.com/api/v9").rstrip("/")
REQUEST_TIMEOUT = float(os.getenv("DISCORD_REQUEST_TIMEOUT", "10"))
MAX_RETRIES = max(1, int(os.getenv("DISCORD_MAX_RETRIES", "3")))
REQUEST_SPACING = float(os.getenv("DISCORD_REQUEST_SPACING", "0.25"))
TEXT_CHANNEL_TYPES = {0, 5, 10, 11, 12}
MESSAGE_FETCH_PAGE_SIZE = 100
SEARCH_PAGE_SIZE = 25
SEARCH_MAX_PAGES = max(0, int(os.getenv("DISCORD_SEARCH_MAX_PAGES", "0")))
SEARCH_INDEX_RETRIES = max(0, int(os.getenv("DISCORD_SEARCH_INDEX_RETRIES", "3")))
CHANNEL_MESSAGE_PAGE_SIZE = max(1, int(os.getenv("CHANNEL_MESSAGE_PAGE_SIZE", "20")))
TASK_MESSAGE_LOG_LIMIT = max(0, int(os.getenv("TASK_MESSAGE_LOG_LIMIT", "1000")))
DM_EXPORT_MAX_PAGES = max(0, int(os.getenv("DM_EXPORT_MAX_PAGES", "0")))
DM_EXPORT_ASSET_TIMEOUT = float(os.getenv("DM_EXPORT_ASSET_TIMEOUT", "20"))
DM_EXPORT_MAX_ASSET_BYTES = max(0, int(os.getenv("DM_EXPORT_MAX_ASSET_BYTES", str(25 * 1024 * 1024))))

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)

deletion_tasks: dict[str, dict[str, Any]] = {}
task_lock = threading.Lock()
discord_request_lock = threading.Lock()
next_discord_request_at = 0.0


TASK_DEFAULTS: dict[str, Any] = {
    "profile_id": None,
    "guild_id": None,
    "selected_channels": [],
    "channel_names": {},
    "delay": 1.5,
    "batch_size": 30,
    "batch_delay": 30.0,
    "dry_run": False,
    "export_before_delete": False,
    "export_status": "not_requested",
    "export_files": [],
    "export_errors": [],
    "found_message_log": [],
    "deleted_message_log": [],
    "failed_message_log": [],
    "message_log_limited": False,
    "status": "pending",
    "total_messages": 0,
    "deleted_messages": 0,
    "failed_messages": 0,
    "skipped_messages": 0,
    "current_channel": None,
    "current_channel_name": None,
    "current_message": None,
    "start_time": None,
    "end_time": None,
    "error": None,
}


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            user_id TEXT,
            username TEXT,
            discriminator TEXT,
            avatar_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS guilds (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            icon_url TEXT,
            owner_id TEXT,
            member_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            id TEXT PRIMARY KEY,
            guild_id TEXT,
            name TEXT NOT NULL,
            type INTEGER,
            position INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (guild_id) REFERENCES guilds (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            channel_id TEXT,
            author_id TEXT,
            content TEXT,
            timestamp TIMESTAMP,
            FOREIGN KEY (channel_id) REFERENCES channels (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS direct_messages (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type INTEGER,
            icon_url TEXT,
            recipients TEXT,
            recipient_count INTEGER DEFAULT 0,
            last_message_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS deletion_tasks (
            id TEXT PRIMARY KEY,
            profile_id INTEGER,
            guild_id TEXT,
            selected_channels TEXT,
            channel_names TEXT,
            delay REAL DEFAULT 1.5,
            batch_size INTEGER DEFAULT 30,
            batch_delay REAL DEFAULT 30,
            dry_run INTEGER DEFAULT 0,
            export_before_delete INTEGER DEFAULT 0,
            export_status TEXT DEFAULT 'not_requested',
            export_files TEXT DEFAULT '[]',
            export_errors TEXT DEFAULT '[]',
            found_message_log TEXT DEFAULT '[]',
            deleted_message_log TEXT DEFAULT '[]',
            failed_message_log TEXT DEFAULT '[]',
            message_log_limited INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            total_messages INTEGER DEFAULT 0,
            deleted_messages INTEGER DEFAULT 0,
            failed_messages INTEGER DEFAULT 0,
            skipped_messages INTEGER DEFAULT 0,
            current_channel TEXT,
            current_channel_name TEXT,
            current_message TEXT,
            start_time TEXT,
            end_time TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    ensure_deletion_task_columns(conn)

    conn.commit()
    conn.close()


def ensure_deletion_task_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(deletion_tasks)").fetchall()
    }
    required_columns = {
        "guild_id": "TEXT",
        "selected_channels": "TEXT",
        "channel_names": "TEXT",
        "delay": "REAL DEFAULT 1.5",
        "batch_size": "INTEGER DEFAULT 30",
        "batch_delay": "REAL DEFAULT 30",
        "dry_run": "INTEGER DEFAULT 0",
        "export_before_delete": "INTEGER DEFAULT 0",
        "export_status": "TEXT DEFAULT 'not_requested'",
        "export_files": "TEXT DEFAULT '[]'",
        "export_errors": "TEXT DEFAULT '[]'",
        "found_message_log": "TEXT DEFAULT '[]'",
        "deleted_message_log": "TEXT DEFAULT '[]'",
        "failed_message_log": "TEXT DEFAULT '[]'",
        "message_log_limited": "INTEGER DEFAULT 0",
        "skipped_messages": "INTEGER DEFAULT 0",
        "current_channel": "TEXT",
        "current_channel_name": "TEXT",
        "current_message": "TEXT",
        "start_time": "TEXT",
        "end_time": "TEXT",
        "error": "TEXT",
        "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    }

    for column, definition in required_columns.items():
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE deletion_tasks ADD COLUMN {column} {definition}")


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def task_from_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None

    task = dict(TASK_DEFAULTS)
    task.update(
        {
            "profile_id": row["profile_id"],
            "guild_id": row["guild_id"],
            "delay": row["delay"] if row["delay"] is not None else TASK_DEFAULTS["delay"],
            "batch_size": row["batch_size"] if row["batch_size"] is not None else TASK_DEFAULTS["batch_size"],
            "batch_delay": row["batch_delay"] if row["batch_delay"] is not None else TASK_DEFAULTS["batch_delay"],
            "dry_run": bool(row["dry_run"]),
            "export_before_delete": bool(row["export_before_delete"]),
            "export_status": row["export_status"] or TASK_DEFAULTS["export_status"],
            "message_log_limited": bool(row["message_log_limited"]),
            "status": row["status"] or TASK_DEFAULTS["status"],
            "total_messages": row["total_messages"] or 0,
            "deleted_messages": row["deleted_messages"] or 0,
            "failed_messages": row["failed_messages"] or 0,
            "skipped_messages": row["skipped_messages"] or 0,
            "current_channel": row["current_channel"],
            "current_channel_name": row["current_channel_name"],
            "current_message": row["current_message"],
            "start_time": row["start_time"],
            "end_time": row["end_time"] or row["completed_at"],
            "error": row["error"],
        }
    )

    try:
        task["selected_channels"] = json.loads(row["selected_channels"] or "[]")
    except (TypeError, ValueError):
        task["selected_channels"] = []

    try:
        task["channel_names"] = json.loads(row["channel_names"] or "{}")
    except (TypeError, ValueError):
        task["channel_names"] = {}

    try:
        task["export_files"] = json.loads(row["export_files"] or "[]")
    except (TypeError, ValueError):
        task["export_files"] = []

    try:
        task["export_errors"] = json.loads(row["export_errors"] or "[]")
    except (TypeError, ValueError):
        task["export_errors"] = []

    for log_key in ("found_message_log", "deleted_message_log", "failed_message_log"):
        try:
            task[log_key] = json.loads(row[log_key] or "[]")
        except (TypeError, ValueError):
            task[log_key] = []

    return task


def load_task(task_id: str) -> dict[str, Any] | None:
    with task_lock:
        task = deletion_tasks.get(task_id)
        if task:
            return dict(task)

    conn = get_db_connection()
    row = conn.execute("SELECT * FROM deletion_tasks WHERE id = ?", (task_id,)).fetchone()
    conn.close()
    task = task_from_row(row)
    if not task:
        return None

    if task["status"] in {"pending", "running"}:
        task["status"] = "interrupted"
        task["error"] = "Tache interrompue par un redemarrage du processus."
        task["end_time"] = task["end_time"] or datetime.now().isoformat(timespec="seconds")
        persist_task(task_id, task)

    return task


def persist_new_task(task_id: str, task: dict[str, Any]) -> None:
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO deletion_tasks (
            id, profile_id, guild_id, selected_channels, channel_names, delay, batch_size,
            batch_delay, dry_run, export_before_delete, export_status, export_files,
            export_errors, found_message_log, deleted_message_log, failed_message_log,
            message_log_limited, status, total_messages, deleted_messages,
            failed_messages, skipped_messages, current_channel, current_channel_name,
            current_message, start_time, end_time, error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        task_record(task_id, task),
    )
    conn.commit()
    conn.close()


def persist_task(task_id: str, task: dict[str, Any]) -> None:
    conn = get_db_connection()
    record = task_record(task_id, task)
    conn.execute(
        """
        UPDATE deletion_tasks
        SET
            profile_id = ?,
            guild_id = ?,
            selected_channels = ?,
            channel_names = ?,
            delay = ?,
            batch_size = ?,
            batch_delay = ?,
            dry_run = ?,
            export_before_delete = ?,
            export_status = ?,
            export_files = ?,
            export_errors = ?,
            found_message_log = ?,
            deleted_message_log = ?,
            failed_message_log = ?,
            message_log_limited = ?,
            status = ?,
            total_messages = ?,
            deleted_messages = ?,
            failed_messages = ?,
            skipped_messages = ?,
            current_channel = ?,
            current_channel_name = ?,
            current_message = ?,
            start_time = ?,
            end_time = ?,
            completed_at = ?,
            error = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (*record[1:-1], task.get("end_time"), record[-1], task_id),
    )
    conn.commit()
    conn.close()


def persist_task_snapshot(task_id: str) -> None:
    with task_lock:
        task = deletion_tasks.get(task_id)
        if not task:
            return
        snapshot = dict(task)
    persist_task(task_id, snapshot)


def task_record(task_id: str, task: dict[str, Any]) -> tuple[Any, ...]:
    return (
        task_id,
        task.get("profile_id"),
        task.get("guild_id"),
        json.dumps(task.get("selected_channels", [])),
        json.dumps(task.get("channel_names", {})),
        task.get("delay", TASK_DEFAULTS["delay"]),
        task.get("batch_size", TASK_DEFAULTS["batch_size"]),
        task.get("batch_delay", TASK_DEFAULTS["batch_delay"]),
        1 if task.get("dry_run") else 0,
        1 if task.get("export_before_delete") else 0,
        task.get("export_status", TASK_DEFAULTS["export_status"]),
        json.dumps(task.get("export_files", [])),
        json.dumps(task.get("export_errors", [])),
        json.dumps(task.get("found_message_log", [])),
        json.dumps(task.get("deleted_message_log", [])),
        json.dumps(task.get("failed_message_log", [])),
        1 if task.get("message_log_limited") else 0,
        task.get("status", TASK_DEFAULTS["status"]),
        task.get("total_messages", 0),
        task.get("deleted_messages", 0),
        task.get("failed_messages", 0),
        task.get("skipped_messages", 0),
        task.get("current_channel"),
        task.get("current_channel_name"),
        task.get("current_message"),
        task.get("start_time"),
        task.get("end_time"),
        task.get("error"),
    )


def token_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
    }


def retry_after(response: requests.Response) -> float:
    header = response.headers.get("Retry-After")
    if header:
        try:
            return float(header) + 1.0
        except ValueError:
            pass

    try:
        payload = response.json()
    except ValueError:
        return 10.0

    try:
        return float(payload.get("retry_after", 10)) + 1.0
    except (TypeError, ValueError):
        return 10.0


def discord_request(token: str, method: str, path: str, **kwargs: Any) -> requests.Response | None:
    global next_discord_request_at

    url = f"{API_BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with discord_request_lock:
                wait_time = next_discord_request_at - time.monotonic()
                if wait_time > 0:
                    time.sleep(wait_time)

                response = requests.request(
                    method,
                    url,
                    headers=token_headers(token),
                    timeout=REQUEST_TIMEOUT,
                    **kwargs,
                )

                next_discord_request_at = max(
                    next_discord_request_at,
                    time.monotonic() + REQUEST_SPACING,
                )

                remaining = response.headers.get("X-RateLimit-Remaining")
                reset_after = response.headers.get("X-RateLimit-Reset-After")
                if remaining == "0" and reset_after:
                    try:
                        next_discord_request_at = max(
                            next_discord_request_at,
                            time.monotonic() + float(reset_after),
                        )
                    except ValueError:
                        pass
        except requests.RequestException as error:
            print(f"Erreur reseau Discord ({attempt}/{MAX_RETRIES}): {error}")
            if attempt < MAX_RETRIES:
                time.sleep(2)
            continue

        if response.status_code == 429 and attempt < MAX_RETRIES:
            delay = retry_after(response)
            with discord_request_lock:
                next_discord_request_at = max(next_discord_request_at, time.monotonic() + delay)
            time.sleep(delay)
            continue

        return response

    return None


def verify_discord_token(token: str) -> dict[str, Any]:
    response = discord_request(token, "GET", "/users/@me")
    if not response or response.status_code != 200:
        return {"valid": False}

    try:
        user_data = response.json()
    except ValueError:
        return {"valid": False}

    avatar_url = None
    if user_data.get("avatar"):
        avatar_url = f"https://cdn.discordapp.com/avatars/{user_data['id']}/{user_data['avatar']}.png"

    return {
        "valid": True,
        "user_id": user_data.get("id"),
        "username": user_data.get("username"),
        "discriminator": user_data.get("discriminator"),
        "avatar_url": avatar_url,
    }


def get_user_guilds(token: str, user_id: str | None = None) -> list[dict[str, Any]]:
    response = discord_request(token, "GET", "/users/@me/guilds")
    if not response or response.status_code != 200:
        return []

    try:
        guilds = response.json()
    except ValueError:
        return []

    result = []
    for guild in guilds:
        icon_url = None
        if guild.get("icon"):
            icon_url = f"https://cdn.discordapp.com/icons/{guild['id']}/{guild['icon']}.png"

        result.append(
            {
                "id": guild.get("id"),
                "name": guild.get("name") or "Serveur sans nom",
                "icon_url": icon_url,
                "owner_id": user_id if guild.get("owner") else guild.get("owner_id"),
                "member_count": guild.get("approximate_member_count"),
                "permissions": guild.get("permissions"),
            }
        )
    return result


def get_guild_channels(token: str, guild_id: str) -> list[dict[str, Any]]:
    response = discord_request(token, "GET", f"/guilds/{guild_id}/channels")
    if not response or response.status_code != 200:
        return []

    try:
        channels = response.json()
    except ValueError:
        return []

    text_channels = []
    for channel in channels:
        if channel.get("type") not in TEXT_CHANNEL_TYPES:
            continue

        text_channels.append(
            {
                "id": channel.get("id"),
                "guild_id": guild_id,
                "name": channel.get("name") or "salon-sans-nom",
                "type": channel.get("type"),
                "position": channel.get("position", 0),
                "nsfw": channel.get("nsfw", False),
                "topic": channel.get("topic"),
            }
        )

    return sorted(text_channels, key=lambda item: (item["position"], item["name"].lower()))


def user_avatar_url(user: dict[str, Any]) -> str | None:
    if not user.get("id") or not user.get("avatar"):
        return None
    return f"https://cdn.discordapp.com/avatars/{user['id']}/{user['avatar']}.png"


def direct_message_name(dm: dict[str, Any]) -> str:
    recipients = dm.get("recipients") or []
    if dm.get("type") == 1:
        recipient = recipients[0] if recipients else {}
        return f"MP avec {recipient.get('username', 'inconnu')}"

    name = dm.get("name") or ", ".join(user.get("username", "inconnu") for user in recipients)
    return f"Groupe: {name or 'Groupe MP'}"


def direct_message_icon_url(dm: dict[str, Any]) -> str | None:
    recipients = dm.get("recipients") or []
    if dm.get("type") == 1 and recipients:
        return user_avatar_url(recipients[0])
    if dm.get("icon") and dm.get("id"):
        return f"https://cdn.discordapp.com/channel-icons/{dm['id']}/{dm['icon']}.png"
    return None


def normalize_direct_message(dm: dict[str, Any]) -> dict[str, Any]:
    recipients = dm.get("recipients") or []
    return {
        "id": dm.get("id"),
        "name": direct_message_name(dm),
        "type": dm.get("type"),
        "icon_url": direct_message_icon_url(dm),
        "recipients": recipients,
        "recipient_count": len(recipients),
        "last_message_id": dm.get("last_message_id"),
    }


def get_user_direct_messages(token: str) -> list[dict[str, Any]]:
    response = discord_request(token, "GET", "/users/@me/channels")
    if not response or response.status_code != 200:
        return []

    try:
        channels = response.json()
    except ValueError:
        return []

    direct_messages = []
    for channel in channels:
        if channel.get("type") not in (1, 3):
            continue
        normalized = normalize_direct_message(channel)
        if normalized["id"]:
            direct_messages.append(normalized)
    return direct_messages


def search_messages(
    token: str,
    path: str,
    params: dict[str, Any],
    max_pages: int = SEARCH_MAX_PAGES,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    offset = 0
    meta: dict[str, Any] = {
        "total_results": 0,
        "pages_fetched": 0,
        "messages_seen": 0,
        "limited": False,
        "last_status": None,
        "error": None,
    }

    while True:
        if max_pages and meta["pages_fetched"] >= max_pages:
            meta["limited"] = True
            break

        request_params = {
            **params,
            "offset": offset,
            "include_nsfw": "true",
            "sort_by": "timestamp",
            "sort_order": "desc",
        }

        response = None
        for index_attempt in range(SEARCH_INDEX_RETRIES + 1):
            response = discord_request(token, "GET", path, params=request_params)
            if response is None:
                meta["error"] = "Aucune reponse Discord."
                break

            meta["last_status"] = response.status_code
            if response.status_code != 202 or index_attempt >= SEARCH_INDEX_RETRIES:
                break

            time.sleep(retry_after(response))

        if not response:
            break

        if response.status_code == 202:
            meta["error"] = "Index de recherche Discord temporairement indisponible."
            break

        if response.status_code != 200:
            meta["error"] = f"Recherche Discord interrompue avec le statut HTTP {response.status_code}."
            break

        try:
            payload = response.json()
        except ValueError:
            meta["error"] = "Reponse Discord illisible."
            break

        pages = payload.get("messages", [])
        if not pages:
            break

        meta["total_results"] = int(payload.get("total_results", 0) or 0)
        meta["pages_fetched"] += 1

        for page in pages:
            for message in page:
                message_id = message.get("id") if isinstance(message, dict) else None
                if not message_id or message_id in seen_ids:
                    continue
                seen_ids.add(message_id)
                messages.append(message)
                meta["messages_seen"] += 1

        offset += SEARCH_PAGE_SIZE
        if meta["total_results"] <= offset:
            break

    return messages, meta


def normalize_user_messages(
    raw_messages: list[dict[str, Any]],
    user_id: str,
    fallback_channel_id: str | None = None,
) -> list[dict[str, Any]]:
    messages = []
    for message in raw_messages:
        if message.get("author", {}).get("id") != user_id:
            continue
        messages.append(
            {
                "id": message.get("id"),
                "content": message.get("content", ""),
                "timestamp": message.get("timestamp"),
                "channel_id": message.get("channel_id") or fallback_channel_id,
                "attachments": message.get("attachments") or [],
                "embeds": message.get("embeds") or [],
                "call": message.get("call"),
            }
        )
    return messages


def get_channel_messages_with_meta(
    token: str,
    channel_id: str,
    user_id: str,
    guild_id: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = f"/guilds/{guild_id}/messages/search" if guild_id else f"/channels/{channel_id}/messages/search"
    params = {"author_id": user_id}
    if guild_id:
        params["channel_id"] = channel_id

    raw_messages, meta = search_messages(
        token,
        path,
        params,
    )
    messages = normalize_user_messages(raw_messages, user_id, channel_id)
    messages.sort(key=lambda message: parse_discord_timestamp(message.get("timestamp")), reverse=True)
    meta["messages_filtered"] = len(messages)
    return messages, meta


def get_channel_messages(token: str, channel_id: str, user_id: str, guild_id: str | None = None) -> list[dict[str, Any]]:
    messages, _ = get_channel_messages_with_meta(token, channel_id, user_id, guild_id)
    return messages


def get_guild_user_messages(token: str, guild_id: str, user_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw_messages, meta = search_messages(
        token,
        f"/guilds/{guild_id}/messages/search",
        {"author_id": user_id},
    )
    messages = normalize_user_messages(raw_messages, user_id)
    messages.sort(key=lambda message: parse_discord_timestamp(message.get("timestamp")), reverse=True)
    meta["messages_filtered"] = len(messages)
    return messages, meta


def build_guild_analysis(
    token: str,
    guild_id: str,
    user_id: str,
    channels_list: list[dict[str, Any]],
) -> dict[str, Any]:
    channels_by_id = {channel["id"]: channel for channel in channels_list}
    channel_order = {channel["id"]: index for index, channel in enumerate(channels_list)}
    totals = {"messages": 0, "text": 0, "media": 0, "link": 0, "other": 0}
    messages, search_meta = get_guild_user_messages(token, guild_id, user_id)
    analysis_by_channel: dict[str, dict[str, Any]] = {}

    for message in messages:
        channel_id = message.get("channel_id") or "unknown"
        channel = channels_by_id.get(channel_id)
        if not channel:
            channel = {
                "id": channel_id,
                "name": f"Salon {channel_id}",
                "type": None,
                "position": len(channels_list) + len(analysis_by_channel),
                "nsfw": False,
            }
            channels_by_id[channel_id] = channel
            channel_order[channel_id] = len(channel_order)

        if channel_id not in analysis_by_channel:
            analysis_by_channel[channel_id] = {
                "channel": channel,
                "total_messages": 0,
                "message_types": {"text": 0, "media": 0, "link": 0, "other": 0},
                "messages": [],
                "page_count": 1,
            }

        message_type = classify_message(message)
        totals[message_type] += 1
        totals["messages"] += 1
        analysis_by_channel[channel_id]["total_messages"] += 1
        analysis_by_channel[channel_id]["message_types"][message_type] += 1
        analysis_by_channel[channel_id]["messages"].append(
            {
                "id": message.get("id"),
                "content": message.get("content") or "",
                "timestamp": message.get("timestamp"),
                "timestamp_label": format_discord_timestamp(message.get("timestamp")),
                "type": message_type,
                "type_label": message_type_label(message_type),
                "attachments": len(message.get("attachments") or []),
                "embeds": len(message.get("embeds") or []),
                "jump_url": discord_jump_url(guild_id, channel_id, message.get("id")),
            }
        )

    analysis_results = sorted(
        analysis_by_channel.values(),
        key=lambda result: channel_order.get(result["channel"]["id"], 999999),
    )
    for result in analysis_results:
        result["page_count"] = max(1, (len(result["messages"]) + CHANNEL_MESSAGE_PAGE_SIZE - 1) // CHANNEL_MESSAGE_PAGE_SIZE)

    return {
        "analysis_results": analysis_results,
        "search_meta": search_meta,
        "totals": totals,
        "message_page_size": CHANNEL_MESSAGE_PAGE_SIZE,
    }


def build_direct_message_analysis(token: str, dm: dict[str, Any], user_id: str) -> dict[str, Any]:
    messages, search_meta = get_channel_messages_with_meta(token, dm["id"], user_id)
    message_types = {"text": 0, "media": 0, "link": 0, "other": 0}
    rendered_messages = []

    for message in messages:
        message_type = classify_message(message)
        message_types[message_type] += 1
        rendered_messages.append(
            {
                "id": message.get("id"),
                "content": message.get("content") or "",
                "timestamp": message.get("timestamp"),
                "timestamp_label": format_discord_timestamp(message.get("timestamp")),
                "type": message_type,
                "type_label": message_type_label(message_type),
                "attachments": len(message.get("attachments") or []),
                "embeds": len(message.get("embeds") or []),
                "jump_url": discord_jump_url(None, dm["id"], message.get("id")),
            }
        )

    total_messages = len(rendered_messages)
    return {
        "direct_message": dm,
        "total_messages": total_messages,
        "message_types": message_types,
        "messages": rendered_messages,
        "page_count": max(1, (total_messages + CHANNEL_MESSAGE_PAGE_SIZE - 1) // CHANNEL_MESSAGE_PAGE_SIZE),
        "search_meta": search_meta,
        "message_page_size": CHANNEL_MESSAGE_PAGE_SIZE,
    }


def get_direct_message_history(token: str, channel_id: str, max_pages: int = DM_EXPORT_MAX_PAGES) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    before = None
    page_count = 0

    while True:
        params = {"limit": MESSAGE_FETCH_PAGE_SIZE}
        if before:
            params["before"] = before

        response = discord_request(token, "GET", f"/channels/{channel_id}/messages", params=params)
        if not response or response.status_code != 200:
            break

        try:
            page = response.json()
        except ValueError:
            break

        if not isinstance(page, list) or not page:
            break

        for message in page:
            message_id = message.get("id") if isinstance(message, dict) else None
            if not message_id or message_id in seen_ids:
                continue
            seen_ids.add(message_id)
            messages.append(message)

        before = page[-1].get("id")
        page_count += 1
        if len(page) < MESSAGE_FETCH_PAGE_SIZE:
            break
        if max_pages and page_count >= max_pages:
            break

    return list(reversed(messages))


def delete_discord_message(token: str, channel_id: str, message_id: str) -> bool:
    response = discord_request(token, "DELETE", f"/channels/{channel_id}/messages/{message_id}")
    return response is not None and response.status_code == 204


def upsert_guilds(conn: sqlite3.Connection, guilds: list[dict[str, Any]]) -> None:
    for guild in guilds:
        conn.execute(
            """
            INSERT OR REPLACE INTO guilds (id, name, icon_url, owner_id, member_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                guild["id"],
                guild["name"],
                guild.get("icon_url"),
                guild.get("owner_id"),
                guild.get("member_count"),
            ),
        )


def upsert_channels(conn: sqlite3.Connection, guild_id: str, channels: list[dict[str, Any]]) -> None:
    for channel in channels:
        conn.execute(
            """
            INSERT OR REPLACE INTO channels (id, guild_id, name, type, position)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                channel["id"],
                guild_id,
                channel["name"],
                channel.get("type", 0),
                channel.get("position", 0),
            ),
        )


def upsert_direct_messages(conn: sqlite3.Connection, direct_messages: list[dict[str, Any]]) -> None:
    for dm in direct_messages:
        conn.execute(
            """
            INSERT OR REPLACE INTO direct_messages (
                id, name, type, icon_url, recipients, recipient_count, last_message_id, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                dm["id"],
                dm["name"],
                dm.get("type"),
                dm.get("icon_url"),
                json.dumps(dm.get("recipients", [])),
                dm.get("recipient_count", 0),
                dm.get("last_message_id"),
            ),
        )


def parse_float(value: str | None, default: float, minimum: float, maximum: float) -> float:
    try:
        number = float(value or default)
    except (TypeError, ValueError):
        return default
    return min(max(number, minimum), maximum)


def parse_int(value: str | None, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value or default)
    except (TypeError, ValueError):
        return default
    return min(max(number, minimum), maximum)


def classify_message(message: dict[str, Any]) -> str:
    content = (message.get("content") or "").strip()
    attachments = message.get("attachments") or []
    embeds = message.get("embeds") or []

    if attachments:
        return "media"
    if "http://" in content or "https://" in content or embeds:
        return "link"
    if content:
        return "text"
    return "other"


def truncate_log_content(content: Any, max_length: int = 500) -> str:
    value = str(content or "")
    if len(value) <= max_length:
        return value
    return f"{value[: max_length - 1]}…"


def discord_jump_url(guild_id: str | None, channel_id: str | None, message_id: str | None) -> str | None:
    if not channel_id or not message_id:
        return None
    if guild_id:
        return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    return f"https://discord.com/channels/@me/{channel_id}/{message_id}"


def message_log_entry(
    message: dict[str, Any],
    channel_name: str,
    guild_id: str | None,
    state: str,
) -> dict[str, Any]:
    channel_id = message.get("channel_id")
    message_id = message.get("id")
    message_type = classify_message(message)
    return {
        "id": message_id,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "content": truncate_log_content(message.get("content")),
        "timestamp": message.get("timestamp"),
        "type": message_type,
        "type_label": message_type_label(message_type),
        "attachments": len(message.get("attachments") or []),
        "embeds": len(message.get("embeds") or []),
        "state": state,
        "jump_url": discord_jump_url(guild_id, channel_id, message_id),
        "logged_at": datetime.now().isoformat(timespec="seconds"),
    }


def append_task_log(task: dict[str, Any], key: str, entry: dict[str, Any]) -> None:
    if TASK_MESSAGE_LOG_LIMIT and len(task.get(key, [])) >= TASK_MESSAGE_LOG_LIMIT:
        task["message_log_limited"] = True
        return
    task.setdefault(key, []).append(entry)


def safe_filename(value: str, fallback: str = "discord_export") -> str:
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", value.strip())
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return (cleaned or fallback)[:120]


def escape_html(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def parse_discord_timestamp(value: str | None) -> float:
    if not value:
        return 0
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return 0


def format_discord_timestamp(value: str | None) -> str:
    if not value:
        return "Date inconnue"
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def author_display_name(message: dict[str, Any]) -> str:
    author = message.get("author") or {}
    return author.get("global_name") or author.get("username") or author.get("id") or "Utilisateur inconnu"


def author_avatar_url(message: dict[str, Any]) -> str | None:
    author = message.get("author") or {}
    if not author.get("id") or not author.get("avatar"):
        return None
    extension = "gif" if str(author["avatar"]).startswith("a_") else "png"
    return f"https://cdn.discordapp.com/avatars/{author['id']}/{author['avatar']}.{extension}?size=64"


def attachment_name(attachment: dict[str, Any], fallback: str = "attachment") -> str:
    return attachment.get("filename") or attachment.get("name") or attachment.get("id") or fallback


def attachment_url(attachment: dict[str, Any]) -> str | None:
    return attachment.get("proxy_url") or attachment.get("proxyURL") or attachment.get("url")


def collect_export_assets(messages: list[dict[str, Any]]) -> list[dict[str, str]]:
    assets: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    def add(url: str | None, path: str, label: str) -> None:
        if not url or url in seen_urls:
            return
        seen_urls.add(url)
        assets.append({"url": url, "path": path, "label": label})

    for message in messages:
        message_id = message.get("id") or "unknown"
        avatar_url = author_avatar_url(message)
        author = message.get("author") or {}
        if avatar_url:
            add(
                avatar_url,
                f"avatars/{safe_filename(author.get('id') or message_id)}_{safe_filename(os.path.basename(avatar_url.split('?')[0]), 'avatar.png')}",
                f"avatar {author_display_name(message)}",
            )

        for index, attachment in enumerate(message.get("attachments") or []):
            url = attachment_url(attachment)
            name = safe_filename(attachment_name(attachment, f"attachment_{index}"), f"attachment_{index}")
            add(url, f"attachments/{message_id}/{index}_{name}", name)

        for embed_index, embed in enumerate(message.get("embeds") or []):
            if not isinstance(embed, dict):
                continue
            urls = [
                (embed.get("image") or {}).get("proxy_url") or (embed.get("image") or {}).get("url"),
                (embed.get("thumbnail") or {}).get("proxy_url") or (embed.get("thumbnail") or {}).get("url"),
            ]
            for url_index, url in enumerate(url for url in urls if url):
                add(
                    url,
                    f"embeds/{message_id}/{embed_index}_{url_index}_{safe_filename(os.path.basename(str(url).split('?')[0]), 'embed_media')}",
                    "embed media",
                )

    return assets


def download_export_asset(url: str) -> bytes | None:
    try:
        response = requests.get(url, timeout=DM_EXPORT_ASSET_TIMEOUT, stream=True)
        if response.status_code != 200:
            return None

        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if not chunk:
                continue
            total += len(chunk)
            if DM_EXPORT_MAX_ASSET_BYTES and total > DM_EXPORT_MAX_ASSET_BYTES:
                return None
            chunks.append(chunk)
        return b"".join(chunks)
    except requests.RequestException:
        return None


def render_export_content(content: str | None) -> str:
    raw_content = str(content or "")
    if not raw_content.strip():
        return '<span class="dm-empty">(sans texte)</span>'

    rendered = []
    last_index = 0
    for match in re.finditer(r"https?://[^\s<]+", raw_content):
        rendered.append(escape_html(raw_content[last_index : match.start()]))
        url = match.group(0)
        rendered.append(f'<a href="{escape_html(url)}" target="_blank" rel="noreferrer noopener">{escape_html(url)}</a>')
        last_index = match.end()

    rendered.append(escape_html(raw_content[last_index:]))
    return "".join(rendered)


def render_export_attachments(message: dict[str, Any], asset_paths: dict[str, str]) -> str:
    rendered = []
    for attachment in message.get("attachments") or []:
        url = attachment_url(attachment)
        if not url:
            continue
        name = attachment_name(attachment)
        src = asset_paths.get(url, url)
        content_type = (attachment.get("content_type") or "").lower()
        extension = os.path.splitext(name)[1].lower()
        detail = f" ({attachment.get('size')} octets)" if attachment.get("size") else ""

        if content_type.startswith("image/") or extension in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif"}:
            rendered.append(
                f'<figure class="dm-attachment"><a href="{escape_html(src)}"><img src="{escape_html(src)}" alt="{escape_html(name)}" loading="lazy"></a><figcaption>{escape_html(name)}{escape_html(detail)}</figcaption></figure>'
            )
        elif content_type.startswith("video/") or extension in {".mp4", ".webm", ".mov"}:
            rendered.append(
                f'<figure class="dm-attachment"><video src="{escape_html(src)}" controls preload="metadata"></video><figcaption>{escape_html(name)}{escape_html(detail)}</figcaption></figure>'
            )
        elif content_type.startswith("audio/") or extension in {".mp3", ".ogg", ".wav", ".m4a", ".flac"}:
            rendered.append(
                f'<figure class="dm-attachment"><audio src="{escape_html(src)}" controls preload="metadata"></audio><figcaption>{escape_html(name)}{escape_html(detail)}</figcaption></figure>'
            )
        else:
            rendered.append(f'<div class="dm-file"><a href="{escape_html(src)}">{escape_html(name)}</a>{escape_html(detail)}</div>')

    if not rendered:
        return ""
    return f'<div class="dm-attachments">{"".join(rendered)}</div>'


def render_export_embeds(message: dict[str, Any], asset_paths: dict[str, str]) -> str:
    rendered = []
    for embed in message.get("embeds") or []:
        if not isinstance(embed, dict):
            continue
        title = escape_html(embed.get("title") or "")
        description = render_export_content(embed.get("description") or "")
        raw_image = (embed.get("image") or {}).get("proxy_url") or (embed.get("image") or {}).get("url")
        image_src = asset_paths.get(raw_image, raw_image) if raw_image else None
        image = f'<img class="dm-embed-image" src="{escape_html(image_src)}" loading="lazy">' if image_src else ""
        rendered.append(f'<div class="dm-embed"><strong>{title}</strong><p>{description}</p>{image}</div>')

    if not rendered:
        return ""
    return f'<div class="dm-embeds">{"".join(rendered)}</div>'


def generate_dm_transcript_html(dm: dict[str, Any], messages: list[dict[str, Any]], asset_paths: dict[str, str]) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for message in sorted(messages, key=lambda item: parse_discord_timestamp(item.get("timestamp"))):
        avatar_url = author_avatar_url(message)
        avatar_src = asset_paths.get(avatar_url, avatar_url) if avatar_url else None
        avatar = (
            f'<img src="{escape_html(avatar_src)}" alt="" loading="lazy">'
            if avatar_src
            else f"<span>{escape_html(author_display_name(message)[:1].upper() or '?')}</span>"
        )
        message_url = f"https://discord.com/channels/@me/{message.get('channel_id') or dm['id']}/{message.get('id')}"
        rows.append(
            f"""
            <article class="dm-message" id="message-{escape_html(message.get('id'))}">
                <div class="dm-avatar">{avatar}</div>
                <div class="dm-message-main">
                    <header>
                        <span class="dm-author">{escape_html(author_display_name(message))}</span>
                        <a class="dm-time" href="{escape_html(message_url)}">{escape_html(format_discord_timestamp(message.get('timestamp')))}</a>
                    </header>
                    <div class="dm-content">{render_export_content(message.get('content'))}</div>
                    {render_export_attachments(message, asset_paths)}
                    {render_export_embeds(message, asset_paths)}
                </div>
            </article>
            """
        )

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape_html(dm['name'])} - transcript</title>
<style>
:root{{color-scheme:dark;--bg:#101114;--panel:#16181d;--text:#f2f3f5;--muted:#a3a8b3;--line:#2a2d35;--accent:#5865f2;--link:#8ea1e1}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
a{{color:var(--link);text-decoration:none}}a:hover{{text-decoration:underline}}
.dm-wrap{{max-width:1100px;margin:0 auto;padding:32px 18px 48px}}.dm-head{{position:sticky;top:0;z-index:1;margin:-32px -18px 20px;padding:24px 18px 16px;background:linear-gradient(var(--bg) 70%,rgba(16,17,20,.88));border-bottom:1px solid var(--line)}}
h1{{margin:0 0 8px;font-size:24px;line-height:1.2}}.dm-meta{{display:flex;flex-wrap:wrap;gap:8px 16px;color:var(--muted);font-size:13px}}
.dm-log{{display:grid;gap:2px}}.dm-message{{display:grid;grid-template-columns:42px minmax(0,1fr);gap:12px;padding:10px 8px;border-radius:6px}}.dm-message:hover{{background:rgba(255,255,255,.035)}}
.dm-avatar{{width:42px;height:42px;border-radius:50%;overflow:hidden;background:#30343c;display:grid;place-items:center;color:#fff;font-weight:700}}.dm-avatar img{{width:100%;height:100%;object-fit:cover}}
header{{display:flex;align-items:baseline;gap:8px;min-width:0}}.dm-author{{font-weight:700;color:#fff}}.dm-time{{color:var(--muted);font-size:12px}}.dm-content{{white-space:pre-wrap;overflow-wrap:anywhere}}.dm-empty{{color:var(--muted);font-style:italic}}
.dm-attachments,.dm-embeds{{display:grid;gap:8px;margin-top:8px}}.dm-attachment{{margin:0}}.dm-attachment img,.dm-attachment video{{max-width:min(520px,100%);max-height:420px;border-radius:6px;border:1px solid var(--line);background:#0b0c0f}}.dm-attachment audio{{width:min(520px,100%)}}figcaption{{color:var(--muted);font-size:12px}}.dm-file{{max-width:520px;padding:10px 12px;border:1px solid var(--line);border-radius:6px;background:var(--panel)}}
.dm-embed{{max-width:560px;padding:10px 12px;border-left:4px solid var(--accent);border-radius:4px;background:var(--panel)}}.dm-embed p{{white-space:pre-wrap;overflow-wrap:anywhere}}.dm-embed-image{{display:block;max-width:min(420px,100%);max-height:320px;margin-top:8px;border-radius:6px}}
</style>
</head>
<body>
<main class="dm-wrap">
    <section class="dm-head">
        <h1>{escape_html(dm['name'])}</h1>
        <div class="dm-meta">
            <span><b>Conversation ID:</b> {escape_html(dm['id'])}</span>
            <span><b>Genere:</b> {escape_html(generated_at)}</span>
            <span><b>Messages:</b> {len(messages)}</span>
            <span><b>Participants:</b> {escape_html(dm.get('recipient_count', 0))}</span>
        </div>
    </section>
    <section class="dm-log">
        {"".join(rows)}
    </section>
</main>
</body>
</html>"""


def build_dm_export_zip(dm: dict[str, Any], messages: list[dict[str, Any]]) -> bytes:
    assets = collect_export_assets(messages)
    asset_paths = {}
    downloaded = 0
    failed = 0
    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for asset in assets:
            data = download_export_asset(asset["url"])
            if data is None:
                failed += 1
                continue
            archive.writestr(asset["path"], data)
            asset_paths[asset["url"]] = asset["path"]
            downloaded += 1

        html_transcript = generate_dm_transcript_html(dm, messages, asset_paths)
        archive.writestr("transcript.html", html_transcript)
        archive.writestr("messages.json", json.dumps(messages, indent=2, ensure_ascii=False))
        archive.writestr(
            "metadata.json",
            json.dumps(
                {
                    "conversation": dm,
                    "message_count": len(messages),
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "assets_total": len(assets),
                    "assets_downloaded": downloaded,
                    "assets_failed": failed,
                },
                indent=2,
                ensure_ascii=False,
            ),
        )

    buffer.seek(0)
    return buffer.getvalue()


def task_export_dir(task_id: str) -> str:
    return os.path.join(EXPORT_DIR, safe_filename(task_id, "task"))


def save_task_dm_export(task_id: str, dm: dict[str, Any], messages: list[dict[str, Any]]) -> dict[str, Any]:
    os.makedirs(task_export_dir(task_id), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_filename(dm['name'], 'conversation')}_{dm['id']}_{timestamp}.zip"
    path = os.path.join(task_export_dir(task_id), filename)
    zip_data = build_dm_export_zip(dm, messages)

    with open(path, "wb") as export_file:
        export_file.write(zip_data)

    return {
        "filename": filename,
        "name": dm["name"],
        "channel_id": dm["id"],
        "message_count": len(messages),
        "size": os.path.getsize(path),
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


def generate_channel_export_html(guild_name: str, channel_name: str, messages: list[dict[str, Any]]) -> str:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for message in sorted(messages, key=lambda item: parse_discord_timestamp(item.get("timestamp"))):
        message_type = classify_message(message)
        rows.append(
            f"""
            <article class="message">
                <header>
                    <strong>{escape_html(format_discord_timestamp(message.get("timestamp")))}</strong>
                    <span>{escape_html(message_type_label(message_type))}</span>
                </header>
                <p>{render_export_content(message.get("content"))}</p>
            </article>
            """
        )

    return f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape_html(guild_name)} - {escape_html(channel_name)}</title>
<style>
body{{margin:0;background:#101114;color:#f2f3f5;font:15px/1.45 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
main{{max-width:1000px;margin:0 auto;padding:28px 18px 42px}}
h1{{margin:0 0 6px;font-size:24px}}.meta{{color:#a3a8b3;margin-bottom:20px}}
.message{{padding:10px 12px;border-bottom:1px solid #2a2d35}}.message header{{display:flex;gap:10px;color:#a3a8b3;font-size:12px}}
.message p{{white-space:pre-wrap;overflow-wrap:anywhere;margin:5px 0 0}}
a{{color:#8ea1e1}}
</style>
</head>
<body>
<main>
<h1>{escape_html(channel_name)}</h1>
<div class="meta">{escape_html(guild_name)} · Genere le {escape_html(generated_at)} · {len(messages)} message(s)</div>
{"".join(rows)}
</main>
</body>
</html>"""


def build_channel_export_zip(guild: dict[str, Any], channel_name: str, channel_id: str, messages: list[dict[str, Any]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("transcript.html", generate_channel_export_html(guild.get("name") or "Serveur", channel_name, messages))
        archive.writestr("messages.json", json.dumps(messages, indent=2, ensure_ascii=False))
        archive.writestr(
            "metadata.json",
            json.dumps(
                {
                    "guild": {"id": guild.get("id"), "name": guild.get("name")},
                    "channel": {"id": channel_id, "name": channel_name},
                    "message_count": len(messages),
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                },
                indent=2,
                ensure_ascii=False,
            ),
        )
    buffer.seek(0)
    return buffer.getvalue()


def save_task_channel_export(
    task_id: str,
    guild: dict[str, Any],
    channel_name: str,
    channel_id: str,
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    os.makedirs(task_export_dir(task_id), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_filename(guild.get('name') or 'serveur')}_{safe_filename(channel_name)}_{channel_id}_{timestamp}.zip"
    path = os.path.join(task_export_dir(task_id), filename)
    zip_data = build_channel_export_zip(guild, channel_name, channel_id, messages)

    with open(path, "wb") as export_file:
        export_file.write(zip_data)

    return {
        "filename": filename,
        "name": f"{guild.get('name') or 'Serveur'} / {channel_name}",
        "channel_id": channel_id,
        "message_count": len(messages),
        "size": os.path.getsize(path),
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


def export_status_label(status: str | None) -> str:
    return {
        "not_requested": "Non demande",
        "pending": "En attente",
        "running": "Export en cours",
        "completed": "Export termine",
        "error": "Export en erreur",
    }.get(status or "not_requested", "Inconnu")


def export_status_badge_class(status: str | None) -> str:
    return {
        "not_requested": "text-bg-secondary",
        "pending": "text-bg-secondary",
        "running": "text-bg-primary",
        "completed": "text-bg-success",
        "error": "text-bg-danger",
    }.get(status or "not_requested", "text-bg-secondary")


def export_file_payload(task_id: str, export_file: dict[str, Any]) -> dict[str, Any]:
    filename = export_file.get("filename") or ""
    payload = dict(export_file)
    payload["download_url"] = url_for("download_task_export", task_id=task_id, filename=filename) if filename else None
    return payload


def load_profile_and_guild(profile_id: int, guild_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    conn = get_db_connection()
    profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone())
    guild = None
    if guild_id:
        guild = row_to_dict(conn.execute("SELECT * FROM guilds WHERE id = ?", (guild_id,)).fetchone())
    conn.close()
    return profile, guild


def load_profile(profile_id: int) -> dict[str, Any] | None:
    conn = get_db_connection()
    profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone())
    conn.close()
    return profile


def discord_display_name(profile: dict[str, Any] | None) -> str:
    if not profile:
        return "Utilisateur inconnu"
    username = profile.get("username") or profile.get("name") or "Utilisateur"
    discriminator = profile.get("discriminator")
    if discriminator and discriminator != "0":
        return f"{username}#{discriminator}"
    return username


def channel_type_label(channel_type: int | None) -> str:
    labels = {
        0: "Texte",
        5: "Annonce",
        10: "Thread annonce",
        11: "Thread public",
        12: "Thread prive",
    }
    return labels.get(channel_type, "Texte")


def status_label(status: str) -> str:
    return {
        "pending": "En attente",
        "running": "En cours",
        "completed": "Terminee",
        "interrupted": "Interrompue",
        "error": "Erreur",
    }.get(status, status)


def message_type_label(message_type: str) -> str:
    return {
        "text": "Texte",
        "media": "Medias",
        "link": "Liens",
        "other": "Autres",
    }.get(message_type, "Autres")


def status_badge_class(status: str) -> str:
    return {
        "pending": "text-bg-secondary",
        "running": "text-bg-primary",
        "completed": "text-bg-success",
        "interrupted": "text-bg-warning",
        "error": "text-bg-danger",
    }.get(status, "text-bg-secondary")


def create_deletion_task(
    profile_id: int,
    guild_id: str | None,
    selected_channels: list[str],
    channel_names: dict[str, str],
    delay: float,
    batch_size: int,
    batch_delay: float,
    dry_run: bool,
    export_before_delete: bool = False,
) -> str:
    task_id = secrets.token_hex(8)
    with task_lock:
        deletion_tasks[task_id] = {
            "profile_id": profile_id,
            "guild_id": guild_id,
            "selected_channels": selected_channels,
            "channel_names": channel_names,
            "delay": delay,
            "batch_size": batch_size,
            "batch_delay": batch_delay,
            "dry_run": dry_run,
            "export_before_delete": export_before_delete,
            "export_status": "pending" if export_before_delete else "not_requested",
            "export_files": [],
            "export_errors": [],
            "found_message_log": [],
            "deleted_message_log": [],
            "failed_message_log": [],
            "message_log_limited": False,
            "status": "pending",
            "total_messages": 0,
            "deleted_messages": 0,
            "failed_messages": 0,
            "skipped_messages": 0,
            "current_channel": None,
            "current_channel_name": None,
            "current_message": None,
            "start_time": None,
            "end_time": None,
            "error": None,
        }

    persist_new_task(task_id, deletion_tasks[task_id])

    thread = threading.Thread(target=run_deletion_task, args=(task_id,), daemon=True)
    thread.start()
    return task_id


@app.context_processor
def template_helpers() -> dict[str, Any]:
    return {
        "discord_display_name": discord_display_name,
        "channel_type_label": channel_type_label,
        "status_label": status_label,
        "message_type_label": message_type_label,
        "status_badge_class": status_badge_class,
        "export_status_label": export_status_label,
        "export_status_badge_class": export_status_badge_class,
        "export_file_payload": export_file_payload,
        "format_discord_timestamp": format_discord_timestamp,
    }


@app.route("/")
def index():
    conn = get_db_connection()
    profiles = rows_to_dicts(conn.execute("SELECT * FROM profiles ORDER BY created_at DESC").fetchall())
    conn.close()
    return render_template("index.html", profiles=profiles)


@app.route("/add_profile", methods=["GET", "POST"])
def add_profile():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        token = (request.form.get("token") or "").strip()

        if not name or not token:
            flash("Nom et token sont requis.", "error")
            return redirect(url_for("add_profile"))

        token_info = verify_discord_token(token)
        if not token_info["valid"]:
            flash("Token invalide ou impossible a verifier.", "error")
            return redirect(url_for("add_profile"))

        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO profiles (name, token, user_id, username, discriminator, avatar_url)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    token,
                    token_info["user_id"],
                    token_info["username"],
                    token_info["discriminator"],
                    token_info["avatar_url"],
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            flash("Ce token est deja utilise.", "error")
            return redirect(url_for("add_profile"))
        finally:
            conn.close()

        flash("Profil ajoute avec succes.", "success")
        return redirect(url_for("index"))

    return render_template("add_profile.html")


@app.route("/delete_profile/<int:profile_id>", methods=["POST"])
def delete_profile(profile_id: int):
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
        conn.commit()
    finally:
        conn.close()

    flash("Profil supprime avec succes.", "success")
    return redirect(url_for("index"))


@app.route("/servers/<int:profile_id>")
def servers(profile_id: int):
    conn = get_db_connection()
    profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone())
    if not profile:
        conn.close()
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    guilds = get_user_guilds(profile["token"], profile.get("user_id"))
    upsert_guilds(conn, guilds)
    conn.commit()
    conn.close()

    return render_template("servers.html", profile=profile, guilds=guilds)


@app.route("/scan_servers/<int:profile_id>")
def scan_servers(profile_id: int):
    conn = get_db_connection()
    profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone())
    if not profile:
        conn.close()
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    guilds = get_user_guilds(profile["token"], profile.get("user_id"))
    upsert_guilds(conn, guilds)
    conn.commit()
    conn.close()

    flash(f"{len(guilds)} serveur(s) scanne(s) avec succes.", "success")
    return redirect(url_for("servers", profile_id=profile_id))


@app.route("/channels/<int:profile_id>/<guild_id>")
def channels(profile_id: int, guild_id: str):
    profile, guild = load_profile_and_guild(profile_id, guild_id)
    if not profile or not guild:
        flash("Profil ou serveur non trouve. Actualisez d'abord la liste des serveurs.", "error")
        return redirect(url_for("servers", profile_id=profile_id))

    channels_list = get_guild_channels(profile["token"], guild_id)
    conn = get_db_connection()
    upsert_channels(conn, guild_id, channels_list)
    conn.commit()
    conn.close()

    analysis = build_guild_analysis(profile["token"], guild_id, profile["user_id"], channels_list)
    return render_template(
        "channels.html",
        profile=profile,
        guild=guild,
        channels=analysis["analysis_results"],
        search_meta=analysis["search_meta"],
        totals=analysis["totals"],
        message_page_size=analysis["message_page_size"],
    )


@app.route("/direct_messages/<int:profile_id>")
def direct_messages(profile_id: int):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    dms = get_user_direct_messages(profile["token"])
    conn = get_db_connection()
    upsert_direct_messages(conn, dms)
    conn.commit()
    conn.close()

    return render_template("direct_messages.html", profile=profile, direct_messages=dms)


@app.route("/direct_messages/<int:profile_id>/<dm_id>")
def direct_message_detail(profile_id: int, dm_id: str):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    dms = get_user_direct_messages(profile["token"])
    conn = get_db_connection()
    upsert_direct_messages(conn, dms)
    conn.commit()
    conn.close()

    dm = next((item for item in dms if item["id"] == dm_id), None)
    if not dm:
        flash("Conversation privee non trouvee.", "error")
        return redirect(url_for("direct_messages", profile_id=profile_id))

    analysis = build_direct_message_analysis(profile["token"], dm, profile["user_id"])
    return render_template(
        "direct_message_detail.html",
        profile=profile,
        direct_message=dm,
        result=analysis,
        search_meta=analysis["search_meta"],
        message_page_size=analysis["message_page_size"],
    )


@app.route("/scan_direct_messages/<int:profile_id>")
def scan_direct_messages(profile_id: int):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    dms = get_user_direct_messages(profile["token"])
    conn = get_db_connection()
    upsert_direct_messages(conn, dms)
    conn.commit()
    conn.close()

    flash(f"{len(dms)} conversation(s) privee(s) scannee(s) avec succes.", "success")
    return redirect(url_for("direct_messages", profile_id=profile_id))


@app.route("/analyze_direct_messages/<int:profile_id>")
def analyze_direct_messages(profile_id: int):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    dms = get_user_direct_messages(profile["token"])
    analysis_results = []
    totals = {"messages": 0, "text": 0, "media": 0, "link": 0, "other": 0}

    for dm in dms:
        messages = get_channel_messages(profile["token"], dm["id"], profile["user_id"])
        message_types = {"text": 0, "media": 0, "link": 0, "other": 0}

        for message in messages:
            message_type = classify_message(message)
            message_types[message_type] += 1
            totals[message_type] += 1

        totals["messages"] += len(messages)
        analysis_results.append(
            {
                "direct_message": dm,
                "total_messages": len(messages),
                "message_types": message_types,
            }
        )

    return render_template(
        "analyze_direct_messages.html",
        profile=profile,
        analysis_results=analysis_results,
        totals=totals,
    )


@app.route("/analyze_messages/<int:profile_id>/<guild_id>")
def analyze_messages(profile_id: int, guild_id: str):
    return redirect(url_for("channels", profile_id=profile_id, guild_id=guild_id))


@app.route("/delete_messages/<int:profile_id>/<guild_id>", methods=["GET", "POST"])
def delete_messages(profile_id: int, guild_id: str):
    profile, guild = load_profile_and_guild(profile_id, guild_id)
    if not profile or not guild:
        flash("Profil ou serveur non trouve. Actualisez d'abord la liste des serveurs.", "error")
        return redirect(url_for("servers", profile_id=profile_id))

    if request.method != "POST":
        return redirect(url_for("channels", profile_id=profile_id, guild_id=guild_id))

    channels_list = get_guild_channels(profile["token"], guild_id)
    selected_channels = request.form.getlist("channels")
    known_channel_ids = {channel["id"] for channel in channels_list}
    channel_names = {channel["id"]: channel["name"] for channel in channels_list}
    selected_channels = [
        channel_id
        for channel_id in selected_channels
        if channel_id in known_channel_ids or re.fullmatch(r"\d+", channel_id)
    ]

    delay = parse_float(request.form.get("delay"), default=1.5, minimum=0.2, maximum=60.0)
    batch_size = parse_int(request.form.get("batch_size"), default=30, minimum=1, maximum=500)
    batch_delay = parse_float(request.form.get("batch_delay"), default=30.0, minimum=0.0, maximum=600.0)
    export_before_delete = request.form.get("export_before_delete") == "on"

    if not selected_channels:
        flash("Veuillez selectionner au moins un salon.", "error")
        return redirect(url_for("channels", profile_id=profile_id, guild_id=guild_id))

    task_id = create_deletion_task(
        profile_id,
        guild_id,
        selected_channels,
        channel_names,
        delay,
        batch_size,
        batch_delay,
        False,
        export_before_delete,
    )

    return redirect(url_for("deletion_status", task_id=task_id))


@app.route("/delete_direct_messages/<int:profile_id>", methods=["GET", "POST"])
def delete_direct_messages(profile_id: int):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    dms = get_user_direct_messages(profile["token"])

    if request.method == "POST":
        selected_dms = request.form.getlist("channels")
        known_dm_ids = {dm["id"] for dm in dms}
        selected_dms = [dm_id for dm_id in selected_dms if dm_id in known_dm_ids]
        dm_names = {dm["id"]: dm["name"] for dm in dms}

        delay = parse_float(request.form.get("delay"), default=1.5, minimum=0.2, maximum=60.0)
        batch_size = parse_int(request.form.get("batch_size"), default=30, minimum=1, maximum=500)
        batch_delay = parse_float(request.form.get("batch_delay"), default=30.0, minimum=0.0, maximum=600.0)
        export_before_delete = request.form.get("export_before_delete") == "on"

        if not selected_dms:
            flash("Veuillez selectionner au moins une conversation.", "error")
            return redirect(url_for("direct_messages", profile_id=profile_id))

        task_id = create_deletion_task(
            profile_id,
            None,
            selected_dms,
            dm_names,
            delay,
            batch_size,
            batch_delay,
            False,
            export_before_delete,
        )

        return redirect(url_for("deletion_status", task_id=task_id))

    selected_channel = request.args.get("selected")
    if selected_channel:
        return redirect(url_for("direct_message_detail", profile_id=profile_id, dm_id=selected_channel))

    return render_template(
        "delete_direct_messages.html",
        profile=profile,
        direct_messages=dms,
        selected_channel=selected_channel,
    )


@app.route("/export_direct_message/<int:profile_id>/<dm_id>")
def export_direct_message(profile_id: int, dm_id: str):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    dms = get_user_direct_messages(profile["token"])
    dm = next((item for item in dms if item["id"] == dm_id), None)
    if not dm:
        flash("Conversation privee non trouvee.", "error")
        return redirect(url_for("direct_messages", profile_id=profile_id))

    messages = get_direct_message_history(profile["token"], dm_id)
    zip_data = build_dm_export_zip(dm, messages)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"discord_transcript_{safe_filename(dm['name'])}_{timestamp}.zip"

    return send_file(
        io.BytesIO(zip_data),
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download_task_export/<task_id>/<filename>")
def download_task_export(task_id: str, filename: str):
    task = load_task(task_id)
    if not task:
        abort(404)

    allowed_files = {export_file.get("filename") for export_file in task.get("export_files", [])}
    if filename not in allowed_files or "/" in filename or "\\" in filename:
        abort(404)

    path = os.path.join(task_export_dir(task_id), filename)
    if not os.path.isfile(path):
        abort(404)

    return send_file(
        path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/deletion_status/<task_id>")
def deletion_status(task_id: str):
    task_snapshot = load_task(task_id)
    if not task_snapshot:
        flash("Tache non trouvee.", "error")
        return redirect(url_for("index"))

    profile, guild = load_profile_and_guild(task_snapshot["profile_id"], task_snapshot["guild_id"])
    return render_template(
        "deletion_status.html",
        profile=profile,
        guild=guild,
        task=task_snapshot,
        task_id=task_id,
    )


@app.route("/api/task_status/<task_id>")
def api_task_status(task_id: str):
    task = load_task(task_id)
    if not task:
        return jsonify({"error": "Tache non trouvee"}), 404

    processed = task["deleted_messages"] + task["failed_messages"] + task["skipped_messages"]
    progress = processed / max(1, task["total_messages"]) * 100
    return jsonify(
        {
            "status": task["status"],
            "status_label": status_label(task["status"]),
            "status_badge_class": status_badge_class(task["status"]),
            "total_messages": task["total_messages"],
            "deleted_messages": task["deleted_messages"],
            "failed_messages": task["failed_messages"],
            "skipped_messages": task["skipped_messages"],
            "current_channel": task["current_channel"],
            "current_channel_name": task["current_channel_name"],
            "current_message": task["current_message"],
            "progress": min(progress, 100),
            "error": task["error"],
            "start_time": task["start_time"],
            "end_time": task["end_time"],
            "dry_run": task["dry_run"],
            "export_before_delete": task["export_before_delete"],
            "export_status": task["export_status"],
            "export_status_label": export_status_label(task["export_status"]),
            "export_status_badge_class": export_status_badge_class(task["export_status"]),
            "export_files": [export_file_payload(task_id, export_file) for export_file in task.get("export_files", [])],
            "export_errors": task.get("export_errors", []),
            "found_message_log": task.get("found_message_log", []),
            "deleted_message_log": task.get("deleted_message_log", []),
            "failed_message_log": task.get("failed_message_log", []),
            "message_log_limited": task.get("message_log_limited", False),
        }
    )


def run_deletion_task(task_id: str) -> None:
    with task_lock:
        task = deletion_tasks[task_id]
        task["status"] = "running"
        task["start_time"] = datetime.now().isoformat(timespec="seconds")
    persist_task_snapshot(task_id)

    try:
        conn = get_db_connection()
        profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (task["profile_id"],)).fetchone())
        conn.close()

        if not profile:
            raise RuntimeError("Profil non trouve")

        guild = None
        guild_messages_by_channel: dict[str, list[dict[str, Any]]] = {}
        if task["guild_id"]:
            conn = get_db_connection()
            guild = row_to_dict(conn.execute("SELECT * FROM guilds WHERE id = ?", (task["guild_id"],)).fetchone())
            conn.close()
            guild_messages, _ = get_guild_user_messages(profile["token"], task["guild_id"], profile["user_id"])
            selected_channel_ids = set(task["selected_channels"])
            for message in guild_messages:
                channel_id = message.get("channel_id")
                if channel_id in selected_channel_ids:
                    guild_messages_by_channel.setdefault(channel_id, []).append(message)

        if task.get("export_before_delete"):
            with task_lock:
                task["export_status"] = "running"
            persist_task_snapshot(task_id)

        for channel_id in task["selected_channels"]:
            channel_name = task["channel_names"].get(channel_id, channel_id)
            with task_lock:
                task["current_channel"] = channel_id
                task["current_channel_name"] = channel_name
                task["current_message"] = None
            persist_task_snapshot(task_id)

            if task.get("export_before_delete") and not task.get("guild_id"):
                with task_lock:
                    task["current_message"] = "Export ZIP en cours"
                persist_task_snapshot(task_id)

                dm = {
                    "id": channel_id,
                    "name": channel_name,
                    "type": 1,
                    "recipient_count": 0,
                }
                try:
                    export_messages = get_direct_message_history(profile["token"], channel_id)
                    export_file = save_task_dm_export(task_id, dm, export_messages)
                except Exception as export_error:
                    with task_lock:
                        task["export_status"] = "error"
                        task["export_errors"].append(
                            {
                                "channel_id": channel_id,
                                "name": channel_name,
                                "error": str(export_error),
                                "created_at": datetime.now().isoformat(timespec="seconds"),
                            }
                        )
                        task["status"] = "error"
                        task["error"] = f"Export ZIP impossible pour {channel_name}: {export_error}"
                        task["end_time"] = datetime.now().isoformat(timespec="seconds")
                    persist_task_snapshot(task_id)
                    return

                with task_lock:
                    task["export_files"].append(export_file)
                    task["current_message"] = None
                persist_task_snapshot(task_id)

            messages = (
                guild_messages_by_channel.get(channel_id, [])
                if task["guild_id"]
                else get_channel_messages(profile["token"], channel_id, profile["user_id"], task["guild_id"])
            )

            if task.get("export_before_delete") and task.get("guild_id"):
                with task_lock:
                    task["current_message"] = "Export ZIP en cours"
                persist_task_snapshot(task_id)
                try:
                    export_file = save_task_channel_export(
                        task_id,
                        guild or {"id": task["guild_id"], "name": task["guild_id"]},
                        channel_name,
                        channel_id,
                        messages,
                    )
                except Exception as export_error:
                    with task_lock:
                        task["export_status"] = "error"
                        task["export_errors"].append(
                            {
                                "channel_id": channel_id,
                                "name": channel_name,
                                "error": str(export_error),
                                "created_at": datetime.now().isoformat(timespec="seconds"),
                            }
                        )
                        task["status"] = "error"
                        task["error"] = f"Export ZIP impossible pour {channel_name}: {export_error}"
                        task["end_time"] = datetime.now().isoformat(timespec="seconds")
                    persist_task_snapshot(task_id)
                    return

                with task_lock:
                    task["export_files"].append(export_file)
                    task["current_message"] = None
                persist_task_snapshot(task_id)

            found_entries = [
                message_log_entry(message, channel_name, task["guild_id"], "found")
                for message in messages
            ]

            with task_lock:
                for entry in found_entries:
                    append_task_log(task, "found_message_log", entry)
                task["total_messages"] += len(messages)
            persist_task_snapshot(task_id)

            for index, message in enumerate(messages, 1):
                with task_lock:
                    task["current_message"] = f"{index}/{len(messages)}"

                if task["dry_run"]:
                    with task_lock:
                        task["skipped_messages"] += 1
                    persist_task_snapshot(task_id)
                    continue

                if delete_discord_message(profile["token"], message["channel_id"], message["id"]):
                    with task_lock:
                        task["deleted_messages"] += 1
                        append_task_log(
                            task,
                            "deleted_message_log",
                            message_log_entry(message, channel_name, task["guild_id"], "deleted"),
                        )
                else:
                    with task_lock:
                        task["failed_messages"] += 1
                        append_task_log(
                            task,
                            "failed_message_log",
                            message_log_entry(message, channel_name, task["guild_id"], "failed"),
                        )
                persist_task_snapshot(task_id)

                if index < len(messages):
                    time.sleep(task["delay"])

                if task["batch_size"] > 0 and index % task["batch_size"] == 0 and index < len(messages):
                    time.sleep(task["batch_delay"])

        with task_lock:
            if task.get("export_before_delete"):
                task["export_status"] = "completed" if not task.get("export_errors") else "error"
            task["status"] = "completed"
            task["current_message"] = None
            task["end_time"] = datetime.now().isoformat(timespec="seconds")
        persist_task_snapshot(task_id)
    except Exception as error:
        with task_lock:
            if task.get("export_before_delete") and task.get("export_status") == "running":
                task["export_status"] = "error"
            task["status"] = "error"
            task["error"] = str(error)
            task["end_time"] = datetime.now().isoformat(timespec="seconds")
        persist_task_snapshot(task_id)


@app.route("/favicon.svg")
def favicon():
    return send_file(os.path.join(BASE_DIR, "static", "favicon.svg"), mimetype="image/svg+xml")


init_db()


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1") != "0"
    app.run(debug=debug, host=host, port=port)
