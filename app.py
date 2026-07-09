from __future__ import annotations

import csv
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
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

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
AUTOBUMPER_ENABLED = os.getenv("AUTOBUMPER_ENABLED", "1") != "0"
AUTOBUMPER_DISBOARD_APP_ID = os.getenv("AUTOBUMPER_DISBOARD_APP_ID", "302050872383242240")
AUTOBUMPER_CHECK_INTERVAL = max(10.0, float(os.getenv("AUTOBUMPER_CHECK_INTERVAL", "30")))
AUTOBUMPER_BUMP_INTERVAL = max(7200.0, float(os.getenv("AUTOBUMPER_BUMP_INTERVAL", "7200")))
AUTOBUMPER_RETRY_DELAY = max(60.0, float(os.getenv("AUTOBUMPER_RETRY_DELAY", "60")))
AUTOBUMPER_MAX_RETRIES = max(0, int(os.getenv("AUTOBUMPER_MAX_RETRIES", "3")))
AUTOBUMPER_MAX_DUE_PER_CYCLE = max(1, int(os.getenv("AUTOBUMPER_MAX_DUE_PER_CYCLE", "1")))
AUTOBUMPER_COMMAND_CACHE_TTL = max(60.0, float(os.getenv("AUTOBUMPER_COMMAND_CACHE_TTL", "900")))
AUTOBUMPER_RESPONSE_POLL_SECONDS = max(0.0, float(os.getenv("AUTOBUMPER_RESPONSE_POLL_SECONDS", "8")))
AUTOBUMPER_RESPONSE_POLL_INTERVAL = max(1.0, float(os.getenv("AUTOBUMPER_RESPONSE_POLL_INTERVAL", "1.5")))
AUTOBUMPER_COOLDOWN_MARGIN = max(0.0, float(os.getenv("AUTOBUMPER_COOLDOWN_MARGIN", "5")))
AUTOBUMPER_DELAY_SLOTS = max(1, int(os.getenv("AUTOBUMPER_DELAY_SLOTS", "12")))
AUTOBUMPER_DELAY_STEP_SECONDS = max(60, int(os.getenv("AUTOBUMPER_DELAY_STEP_MINUTES", "10")) * 60)
AUTOBUMPER_MAX_DELAY_SECONDS = AUTOBUMPER_DELAY_SLOTS * AUTOBUMPER_DELAY_STEP_SECONDS
DISCORD_SNOWFLAKE_EPOCH_MS = 1420070400000
MESSAGE_SCRAPER_DEFAULT_MAX_PAGES = max(0, int(os.getenv("MESSAGE_SCRAPER_DEFAULT_MAX_PAGES", "0")))
MESSAGE_SCRAPER_PREVIEW_LIMIT = max(20, int(os.getenv("MESSAGE_SCRAPER_PREVIEW_LIMIT", "300")))
ACCOUNT_BACKUP_SCHEDULER_ENABLED = os.getenv("ACCOUNT_BACKUP_SCHEDULER_ENABLED", "1") != "0"
ACCOUNT_BACKUP_SCHEDULER_CHECK_INTERVAL = max(30.0, float(os.getenv("ACCOUNT_BACKUP_SCHEDULER_CHECK_INTERVAL", "60")))
SELFBOT_TOOLS_WORKER_ENABLED = os.getenv("SELFBOT_TOOLS_WORKER_ENABLED", "1") != "0"
SELFBOT_TOOLS_SPY_INTERVAL = max(5.0, float(os.getenv("SELFBOT_TOOLS_SPY_INTERVAL", "15")))
SELFBOT_TOOLS_ANTIGROUP_INTERVAL = max(10.0, float(os.getenv("SELFBOT_TOOLS_ANTIGROUP_INTERVAL", "30")))
SELFBOT_SPY_MAX_CHANNELS = max(1, int(os.getenv("SELFBOT_SPY_MAX_CHANNELS", "50")))
SELFBOT_SPY_MESSAGE_LOOKBACK = max(1, int(os.getenv("SELFBOT_SPY_MESSAGE_LOOKBACK", "25")))

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY") or secrets.token_hex(32)

deletion_tasks: dict[str, dict[str, Any]] = {}
task_lock = threading.Lock()
discord_request_lock = threading.Lock()
next_discord_request_at = 0.0
autobumper_lock = threading.Lock()
autobumper_active_targets: set[int] = set()
autobumper_stop_event = threading.Event()
autobumper_thread: threading.Thread | None = None
autobumper_command_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
message_scraper_lock = threading.Lock()
message_scraper_active_runs: set[str] = set()
mutual_guild_lock = threading.Lock()
mutual_guild_active_runs: set[str] = set()
account_backup_lock = threading.Lock()
account_backup_active_runs: set[str] = set()
account_backup_scheduler_lock = threading.Lock()
account_backup_scheduler_stop_event = threading.Event()
account_backup_scheduler_thread: threading.Thread | None = None
selfbot_tool_lock = threading.Lock()
selfbot_tool_active_runs: set[str] = set()
selfbot_tools_stop_event = threading.Event()
selfbot_tools_thread: threading.Thread | None = None


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
    conn = sqlite3.connect(DATABASE, timeout=30)
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

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS autobumper_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            guild_id TEXT NOT NULL,
            guild_name TEXT,
            channel_id TEXT NOT NULL,
            channel_name TEXT,
            enabled INTEGER DEFAULT 1,
            next_bump_at REAL,
            last_bump_at REAL,
            delay_seconds INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            total_attempts INTEGER DEFAULT 0,
            last_status TEXT DEFAULT 'queued',
            last_error TEXT,
            custom_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(profile_id, guild_id),
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS autobumper_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id INTEGER,
            profile_id INTEGER,
            guild_id TEXT,
            channel_id TEXT,
            status TEXT NOT NULL,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (target_id) REFERENCES autobumper_targets (id),
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mutual_guild_runs (
            id TEXT PRIMARY KEY,
            target_user_id TEXT NOT NULL,
            profile_count INTEGER DEFAULT 0,
            completed_profiles INTEGER DEFAULT 0,
            guild_count INTEGER DEFAULT 0,
            mutual_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'completed',
            error TEXT,
            target_user_payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mutual_guild_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            guild_id TEXT NOT NULL,
            guild_name TEXT,
            guild_icon_url TEXT,
            owner_id TEXT,
            member_count INTEGER,
            guild_banner_url TEXT,
            vanity_url TEXT,
            invite_count INTEGER DEFAULT 0,
            role_count INTEGER DEFAULT 0,
            target_nick TEXT,
            target_joined_at TEXT,
            selected_profile_id INTEGER,
            found_by_profiles TEXT DEFAULT '[]',
            member_payload TEXT,
            guild_payload TEXT,
            user_roles_payload TEXT DEFAULT '[]',
            invites_payload TEXT DEFAULT '[]',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, guild_id),
            FOREIGN KEY (run_id) REFERENCES mutual_guild_runs (id),
            FOREIGN KEY (selected_profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS message_scraper_runs (
            id TEXT PRIMARY KEY,
            mutual_run_id TEXT,
            target_user_id TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            total_guilds INTEGER DEFAULT 0,
            completed_guilds INTEGER DEFAULT 0,
            total_messages INTEGER DEFAULT 0,
            total_pages INTEGER DEFAULT 0,
            current_guild_id TEXT,
            current_guild_name TEXT,
            current_phase TEXT,
            current_detail TEXT,
            last_status_code INTEGER,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (mutual_run_id) REFERENCES mutual_guild_runs (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS message_scraper_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            profile_id INTEGER,
            guild_id TEXT NOT NULL,
            guild_name TEXT,
            channel_id TEXT,
            channel_name TEXT,
            message_id TEXT NOT NULL,
            timestamp TEXT,
            content TEXT,
            attachment_count INTEGER DEFAULT 0,
            embed_count INTEGER DEFAULT 0,
            jump_url TEXT,
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, message_id),
            FOREIGN KEY (run_id) REFERENCES message_scraper_runs (id),
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_backup_runs (
            id TEXT PRIMARY KEY,
            profile_id INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            source TEXT DEFAULT 'manual',
            create_invites INTEGER DEFAULT 1,
            friend_count INTEGER DEFAULT 0,
            guild_count INTEGER DEFAULT 0,
            processed_guilds INTEGER DEFAULT 0,
            invite_count INTEGER DEFAULT 0,
            failed_invite_count INTEGER DEFAULT 0,
            current_phase TEXT,
            current_detail TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_backup_friends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT,
            global_name TEXT,
            discriminator TEXT,
            display_name TEXT,
            avatar_url TEXT,
            relationship_type INTEGER,
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, user_id),
            FOREIGN KEY (run_id) REFERENCES account_backup_runs (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_backup_guilds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            guild_id TEXT NOT NULL,
            guild_name TEXT,
            icon_url TEXT,
            owner INTEGER DEFAULT 0,
            permissions TEXT,
            channel_id TEXT,
            channel_name TEXT,
            invite_code TEXT,
            invite_url TEXT,
            invite_max_age INTEGER,
            invite_status TEXT,
            invite_error TEXT,
            raw_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, guild_id),
            FOREIGN KEY (run_id) REFERENCES account_backup_runs (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_backup_schedule_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            enabled INTEGER DEFAULT 0,
            mode TEXT DEFAULT 'daily',
            interval_days INTEGER DEFAULT 1,
            daily_time TEXT DEFAULT '03:00',
            create_invites INTEGER DEFAULT 0,
            last_check_at TEXT,
            last_run_started_at TEXT,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_backup_schedule_profiles (
            profile_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 1,
            last_run_id TEXT,
            last_run_at TEXT,
            next_run_at TEXT,
            last_status TEXT,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id),
            FOREIGN KEY (last_run_id) REFERENCES account_backup_runs (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_backup_schedule_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER,
            run_id TEXT,
            status TEXT NOT NULL,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id),
            FOREIGN KEY (run_id) REFERENCES account_backup_runs (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS selfbot_tool_runs (
            id TEXT PRIMARY KEY,
            profile_id INTEGER,
            tool TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            total_count INTEGER DEFAULT 0,
            processed_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            current_detail TEXT,
            result_json TEXT DEFAULT '{}',
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS selfbot_spy_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            guild_id TEXT NOT NULL,
            guild_name TEXT,
            channel_id TEXT,
            channel_name TEXT,
            keywords TEXT NOT NULL,
            notify_channel_id TEXT,
            enabled INTEGER DEFAULT 1,
            last_message_id TEXT,
            match_count INTEGER DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS selfbot_spy_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER,
            profile_id INTEGER,
            guild_id TEXT,
            channel_id TEXT,
            message_id TEXT,
            author_id TEXT,
            author_name TEXT,
            keyword TEXT,
            content TEXT,
            jump_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (rule_id) REFERENCES selfbot_spy_rules (id),
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS selfbot_antigroup_settings (
            profile_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            last_scan_at TEXT,
            left_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            last_error TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS server_backups (
            id TEXT PRIMARY KEY,
            profile_id INTEGER,
            guild_id TEXT,
            guild_name TEXT,
            name TEXT,
            payload_json TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles (id)
        )
        """
    )

    cursor.execute("INSERT OR IGNORE INTO account_backup_schedule_settings (id) VALUES (1)")

    ensure_deletion_task_columns(conn)
    ensure_autobumper_columns(conn)
    ensure_tool_columns(conn)
    mark_interrupted_tool_runs(conn)

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


def ensure_autobumper_columns(conn: sqlite3.Connection) -> None:
    existing_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(autobumper_targets)").fetchall()
    }
    required_columns = {
        "delay_seconds": "INTEGER DEFAULT 0",
    }

    for column, definition in required_columns.items():
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE autobumper_targets ADD COLUMN {column} {definition}")
    conn.execute("UPDATE autobumper_targets SET delay_seconds = 0 WHERE COALESCE(delay_seconds, 0) != 0")
    conn.execute(
        """
        UPDATE autobumper_targets
        SET last_status = 'queued', last_error = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE enabled = 1
          AND last_error LIKE '%closed database%'
        """
    )


def ensure_table_columns(
    conn: sqlite3.Connection,
    table_name: str,
    required_columns: dict[str, str],
) -> None:
    existing_columns = {
        row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column, definition in required_columns.items():
        if column not in existing_columns:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {definition}")


def ensure_tool_columns(conn: sqlite3.Connection) -> None:
    ensure_table_columns(
        conn,
        "mutual_guild_runs",
        {
            "completed_profiles": "INTEGER DEFAULT 0",
            "updated_at": "TIMESTAMP",
            "target_user_payload": "TEXT",
        },
    )
    ensure_table_columns(
        conn,
        "mutual_guild_results",
        {
            "guild_banner_url": "TEXT",
            "vanity_url": "TEXT",
            "invite_count": "INTEGER DEFAULT 0",
            "role_count": "INTEGER DEFAULT 0",
            "guild_payload": "TEXT",
            "user_roles_payload": "TEXT DEFAULT '[]'",
            "invites_payload": "TEXT DEFAULT '[]'",
        },
    )
    ensure_table_columns(
        conn,
        "message_scraper_runs",
        {
            "current_guild_id": "TEXT",
            "current_guild_name": "TEXT",
            "current_phase": "TEXT",
            "current_detail": "TEXT",
            "last_status_code": "INTEGER",
        },
    )
    ensure_table_columns(
        conn,
        "account_backup_runs",
        {
            "source": "TEXT DEFAULT 'manual'",
            "create_invites": "INTEGER DEFAULT 1",
            "friend_count": "INTEGER DEFAULT 0",
            "guild_count": "INTEGER DEFAULT 0",
            "processed_guilds": "INTEGER DEFAULT 0",
            "invite_count": "INTEGER DEFAULT 0",
            "failed_invite_count": "INTEGER DEFAULT 0",
            "current_phase": "TEXT",
            "current_detail": "TEXT",
            "completed_at": "TIMESTAMP",
        },
    )
    ensure_table_columns(
        conn,
        "account_backup_schedule_settings",
        {
            "enabled": "INTEGER DEFAULT 0",
            "mode": "TEXT DEFAULT 'daily'",
            "interval_days": "INTEGER DEFAULT 1",
            "daily_time": "TEXT DEFAULT '03:00'",
            "create_invites": "INTEGER DEFAULT 0",
            "last_check_at": "TEXT",
            "last_run_started_at": "TEXT",
            "last_error": "TEXT",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
    )
    ensure_table_columns(
        conn,
        "account_backup_schedule_profiles",
        {
            "enabled": "INTEGER DEFAULT 1",
            "last_run_id": "TEXT",
            "last_run_at": "TEXT",
            "next_run_at": "TEXT",
            "last_status": "TEXT",
            "last_error": "TEXT",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
    )
    ensure_table_columns(
        conn,
        "selfbot_tool_runs",
        {
            "total_count": "INTEGER DEFAULT 0",
            "processed_count": "INTEGER DEFAULT 0",
            "success_count": "INTEGER DEFAULT 0",
            "fail_count": "INTEGER DEFAULT 0",
            "current_detail": "TEXT",
            "result_json": "TEXT DEFAULT '{}'",
            "completed_at": "TIMESTAMP",
        },
    )
    ensure_table_columns(
        conn,
        "selfbot_spy_rules",
        {
            "guild_name": "TEXT",
            "channel_name": "TEXT",
            "notify_channel_id": "TEXT",
            "last_message_id": "TEXT",
            "match_count": "INTEGER DEFAULT 0",
            "last_error": "TEXT",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
    )
    ensure_table_columns(
        conn,
        "selfbot_antigroup_settings",
        {
            "last_scan_at": "TEXT",
            "left_count": "INTEGER DEFAULT 0",
            "fail_count": "INTEGER DEFAULT 0",
            "last_error": "TEXT",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        },
    )


def mark_interrupted_tool_runs(conn: sqlite3.Connection) -> None:
    interrupted = "Run interrompu par un redemarrage du processus."
    conn.execute(
        """
        UPDATE mutual_guild_runs
        SET status = 'failed',
            error = COALESCE(error, ?),
            updated_at = CURRENT_TIMESTAMP
        WHERE status IN ('pending', 'running')
        """,
        (interrupted,),
    )
    conn.execute(
        """
        UPDATE message_scraper_runs
        SET status = 'failed',
            error = COALESCE(error, ?),
            current_phase = 'Interrompu',
            current_detail = ?,
            updated_at = CURRENT_TIMESTAMP,
            completed_at = CURRENT_TIMESTAMP
        WHERE status IN ('pending', 'running')
        """,
        (interrupted, interrupted),
    )
    conn.execute(
        """
        UPDATE account_backup_runs
        SET status = 'failed',
            error = COALESCE(error, ?),
            current_phase = 'Interrompu',
            current_detail = ?,
            updated_at = CURRENT_TIMESTAMP,
            completed_at = CURRENT_TIMESTAMP
        WHERE status IN ('pending', 'running')
        """,
        (interrupted, interrupted),
    )
    conn.execute(
        """
        UPDATE selfbot_tool_runs
        SET status = 'failed',
            error = COALESCE(error, ?),
            current_detail = ?,
            updated_at = CURRENT_TIMESTAMP,
            completed_at = CURRENT_TIMESTAMP
        WHERE status IN ('pending', 'running')
        """,
        (interrupted, interrupted),
    )


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def current_timestamp() -> float:
    return time.time()


def format_epoch_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "Jamais"

    try:
        return datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return str(value)


def format_local_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "Date inconnue"
    if isinstance(value, (int, float)):
        return format_epoch_timestamp(value)

    raw_value = str(value)
    try:
        if raw_value.endswith("Z"):
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        elif re.fullmatch(r".*[+-]\d{2}:\d{2}$", raw_value):
            parsed = datetime.fromisoformat(raw_value)
        else:
            parsed = datetime.fromisoformat(raw_value).replace(tzinfo=timezone.utc)
        return parsed.astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return raw_value


def parse_local_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value)).astimezone()
        except (TypeError, ValueError, OSError):
            return None

    raw_value = str(value)
    try:
        if raw_value.endswith("Z"):
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        elif re.fullmatch(r".*[+-]\d{2}:\d{2}$", raw_value):
            parsed = datetime.fromisoformat(raw_value)
        else:
            parsed = datetime.fromisoformat(raw_value).replace(tzinfo=timezone.utc)
        return parsed.astimezone()
    except ValueError:
        return None


def format_time_until(value: Any) -> str:
    if value in (None, ""):
        return "Non planifie"

    try:
        remaining = float(value) - current_timestamp()
    except (TypeError, ValueError):
        return "Non planifie"

    if remaining <= 0:
        return "Pret"

    hours, remainder = divmod(int(remaining), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


def format_bytes(value: Any) -> str:
    try:
        size = float(value or 0)
    except (TypeError, ValueError):
        size = 0

    units = ["B", "KB", "MB", "GB"]
    unit = units[0]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            break
        size /= 1024

    if unit == "B":
        return f"{int(size)} {unit}"
    return f"{size:.1f} {unit}"


def autobumper_status_label(status: str | None) -> str:
    return {
        "queued": "Planifie",
        "running": "En cours",
        "success": "OK",
        "cooldown": "Cooldown",
        "unknown": "A verifier",
        "failed": "Erreur",
        "paused": "Pause",
    }.get(status or "queued", status or "Planifie")


def autobumper_status_badge_class(status: str | None, enabled: bool = True) -> str:
    if not enabled:
        return "text-bg-secondary"
    return {
        "queued": "text-bg-primary",
        "running": "text-bg-primary",
        "success": "text-bg-success",
        "cooldown": "text-bg-warning",
        "unknown": "text-bg-warning",
        "failed": "text-bg-danger",
        "paused": "text-bg-secondary",
    }.get(status or "queued", "text-bg-secondary")


def clamp_autobumper_delay_seconds(value: Any) -> int:
    return 0


def autobumper_duration_label(seconds: Any) -> str:
    try:
        raw_seconds = max(0, int(seconds or 0))
    except (TypeError, ValueError):
        raw_seconds = 0
    minutes = max(0, int(round(raw_seconds / 60)))
    hours, rest_minutes = divmod(minutes, 60)
    if hours and rest_minutes:
        return f"{hours}h{rest_minutes:02d}"
    if hours:
        return f"{hours}h"
    return f"{minutes} min"


def autobumper_delay_range(seconds: Any) -> tuple[int, int]:
    return 0, 0


def autobumper_delay_label(seconds: Any) -> str:
    return "Aucun delai"


def autobumper_delay_options() -> list[dict[str, Any]]:
    return [{"value": 0, "label": "Aucun delai"}]


def roll_autobumper_delay_seconds(target: dict[str, Any]) -> int:
    return 0


def next_autobumper_schedule_at(target: dict[str, Any], now: float | None = None) -> float:
    current = current_timestamp() if now is None else now
    try:
        last_bump_at = float(target.get("last_bump_at") or 0)
    except (TypeError, ValueError):
        last_bump_at = 0

    base_at = last_bump_at + AUTOBUMPER_BUMP_INTERVAL if last_bump_at else current
    return max(current, base_at) + roll_autobumper_delay_seconds(target)


def autobumper_target_delay_seconds(target: dict[str, Any]) -> int:
    return clamp_autobumper_delay_seconds(target.get("delay_seconds"))


def apply_autobumper_delay_to_result(result: dict[str, Any] | None, target: dict[str, Any]) -> dict[str, Any] | None:
    return result


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
                "owner": bool(guild.get("owner")),
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


def user_avatar_from_user(user: dict[str, Any]) -> str | None:
    user_id = user.get("id")
    avatar_hash = user.get("avatar")
    if not user_id or not avatar_hash:
        return None
    extension = "gif" if str(avatar_hash).startswith("a_") else "png"
    return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.{extension}?size=128"


def normalize_relationship_type(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        relation_name = str(value or "").strip().lower()
        return {
            "friend": 1,
            "friends": 1,
            "ami": 1,
            "blocked": 2,
            "block": 2,
            "bloque": 2,
            "incoming": 3,
            "entrant": 3,
            "outgoing": 4,
            "sortant": 4,
        }.get(relation_name, 0)


def relationship_type_label(value: Any) -> str:
    relation_type = normalize_relationship_type(value)
    return {
        1: "ami",
        2: "bloque",
        3: "entrant",
        4: "sortant",
    }.get(relation_type, f"type {relation_type}" if relation_type else "inconnu")


def get_user_relationships(token: str) -> tuple[list[dict[str, Any]], str | None]:
    response = discord_request(token, "GET", "/users/@me/relationships")
    if response is None:
        return [], "Aucune reponse Discord pendant la lecture des amis."
    if response.status_code != 200:
        return [], discord_response_error(response)
    try:
        payload = response.json()
    except ValueError:
        return [], "Reponse Discord illisible pendant la lecture des amis."
    if not isinstance(payload, list):
        return [], "Reponse Discord inattendue pendant la lecture des amis."

    relationships = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        user = item.get("user") if isinstance(item.get("user"), dict) else item
        if not user.get("id"):
            continue
        relation_type = item.get("type", item.get("relationship_type"))
        if relation_type is None:
            relation_type = 1
        relation_type = normalize_relationship_type(relation_type)
        display_name = user.get("global_name") or user.get("username") or user.get("id")
        relationships.append(
            {
                "user_id": str(user.get("id")),
                "username": user.get("username"),
                "global_name": user.get("global_name"),
                "discriminator": user.get("discriminator"),
                "display_name": display_name,
                "avatar_url": user_avatar_from_user(user),
                "relationship_type": relation_type,
                "relationship_label": relationship_type_label(relation_type),
                "raw": item,
            }
        )
    return relationships, None


CREATE_INSTANT_INVITE_PERMISSION = 1 << 0


def guild_can_create_invite(guild: dict[str, Any]) -> bool:
    if guild.get("owner"):
        return True
    try:
        permissions = int(guild.get("permissions") or 0)
    except (TypeError, ValueError):
        return False
    return bool(permissions & CREATE_INSTANT_INVITE_PERMISSION)


def invite_limit_error_message(existing_error: str | None = None) -> str:
    if existing_error and "50013" in existing_error:
        return (
            "Maximum d'invitations atteint. Discord bloque aussi la lecture des invitations "
            "existantes pour ce compte; impossible de reutiliser un lien sans permission Gerer le serveur."
        )
    return "Maximum d'invitations atteint et aucun lien existant lisible pour ce compte."


def create_channel_invite(token: str, channel_id: str, max_age: int = 0) -> tuple[dict[str, Any] | None, str | None, int | None]:
    payload = {
        "max_age": max(0, int(max_age or 0)),
        "max_uses": 0,
        "target_type": None,
        "temporary": False,
        "flags": 0,
    }
    response = discord_request(token, "POST", f"/channels/{channel_id}/invites", json=payload)
    if response is None:
        return None, "Aucune reponse Discord pendant la creation d'invitation.", None
    if response.status_code not in {200, 201}:
        error = discord_response_error(response)
        error_code = discord_error_code(response)
        if error_code == 50109:
            fallback_payload = {
                "max_age": payload["max_age"],
                "max_uses": 0,
                "temporary": False,
            }
            fallback_response = discord_request(
                token,
                "POST",
                f"/channels/{channel_id}/invites",
                json=fallback_payload,
            )
            if fallback_response is None:
                return None, "Aucune reponse Discord pendant le retry d'invitation.", error_code
            if fallback_response.status_code in {200, 201}:
                try:
                    invite = fallback_response.json()
                except ValueError:
                    return None, "Invitation creee, mais reponse Discord illisible.", None
                return invite if isinstance(invite, dict) else None, None, None
            fallback_error = discord_response_error(fallback_response)
            return None, f"{error}; retry sans champs optionnels: {fallback_error}", discord_error_code(fallback_response) or error_code
        return None, error, error_code
    try:
        invite = response.json()
    except ValueError:
        return None, "Invitation creee, mais reponse Discord illisible.", None
    return invite if isinstance(invite, dict) else None, None, None


def get_channel_detail(token: str, channel_id: str) -> dict[str, Any] | None:
    if not channel_id:
        return None
    payload, _, _ = discord_api_json(token, f"/channels/{channel_id}")
    if not isinstance(payload, dict):
        return None
    return {
        "id": payload.get("id") or channel_id,
        "guild_id": payload.get("guild_id"),
        "name": payload.get("name") or f"Salon {channel_id}",
        "type": payload.get("type", 0),
        "position": payload.get("position", 0),
        "nsfw": payload.get("nsfw", False),
        "topic": payload.get("topic"),
    }


def resolve_channel_name(token: str, channel_id: str, channel_cache: dict[str, str]) -> str:
    if not channel_id:
        return "Salon inconnu"
    if channel_id in channel_cache:
        return channel_cache[channel_id]

    channel = get_channel_detail(token, channel_id)
    if channel and channel.get("name"):
        channel_cache[channel_id] = channel["name"]
        if channel.get("guild_id"):
            conn = get_db_connection()
            upsert_channels(conn, str(channel["guild_id"]), [channel])
            conn.commit()
            conn.close()
        return channel["name"]

    fallback = f"Salon {channel_id}"
    channel_cache[channel_id] = fallback
    return fallback


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


def load_profiles(include_tokens: bool = True) -> list[dict[str, Any]]:
    conn = get_db_connection()
    columns = "*" if include_tokens else "id, name, user_id, username, discriminator, avatar_url, created_at"
    profiles = rows_to_dicts(
        conn.execute(f"SELECT {columns} FROM profiles ORDER BY name COLLATE NOCASE").fetchall()
    )
    conn.close()
    return profiles


def profile_public_summary(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": profile.get("id"),
        "name": profile.get("name"),
        "user_id": profile.get("user_id"),
        "username": profile.get("username"),
        "display_name": discord_display_name(profile),
        "avatar_url": profile.get("avatar_url"),
    }


def discord_api_json(
    token: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> tuple[Any, int | None, str | None]:
    kwargs = {"params": params} if params else {}
    response = discord_request(token, "GET", path, **kwargs)
    if response is None:
        return None, None, "Aucune reponse Discord."
    if response.status_code != 200:
        return None, response.status_code, discord_response_error(response)
    try:
        return response.json(), response.status_code, None
    except ValueError:
        return None, response.status_code, "Reponse Discord illisible."


def fetch_target_profile_payload(token: str, target_user_id: str) -> dict[str, Any] | None:
    payload, _, _ = discord_api_json(
        token,
        f"/users/{target_user_id}/profile",
        params={
            "with_mutual_guilds": "true",
            "with_mutual_friends": "false",
        },
    )
    return payload if isinstance(payload, dict) else None


def fetch_target_user_info(token: str, target_user_id: str) -> dict[str, Any] | None:
    payload = fetch_target_profile_payload(token, target_user_id)
    if isinstance(payload, dict):
        user_data = payload.get("user") if isinstance(payload.get("user"), dict) else payload
        if isinstance(user_data, dict) and user_data.get("id"):
            profile_data = payload.get("user_profile") or {}
            if isinstance(profile_data, dict):
                user_data = dict(user_data)
                user_data["bio"] = profile_data.get("bio")
                user_data["pronouns"] = profile_data.get("pronouns")
            return enrich_target_user_profile(enrich_discord_user_media(user_data), target_user_id)

    payload, _, _ = discord_api_json(token, f"/users/{target_user_id}")
    if isinstance(payload, dict) and payload.get("id"):
        return enrich_target_user_profile(enrich_discord_user_media(payload), target_user_id)
    return None


def enrich_target_user_profile(user_data: dict[str, Any], fallback_user_id: str) -> dict[str, Any]:
    enriched = dict(user_data)
    user_id = str(enriched.get("id") or fallback_user_id)
    enriched["created_at"] = snowflake_created_iso(user_id)
    badges = public_flag_badges(enriched.get("public_flags"))
    premium_label = premium_type_label(enriched.get("premium_type"))
    if premium_label:
        badges.append(premium_label)
    enriched["badges"] = badges
    enriched["display_label"] = target_user_label(enriched, user_id)
    return enriched


def enrich_discord_user_media(user_data: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(user_data)
    user_id = str(enriched.get("id") or "")
    if user_id and enriched.get("avatar"):
        enriched["avatar_url"] = f"https://cdn.discordapp.com/avatars/{user_id}/{enriched['avatar']}.png"
    if user_id and enriched.get("banner"):
        enriched["banner_url"] = f"https://cdn.discordapp.com/banners/{user_id}/{enriched['banner']}.png"
    return enriched


def get_guild_member_info(token: str, guild_id: str, user_id: str) -> tuple[dict[str, Any] | None, str | None]:
    payload, status, error = discord_api_json(token, f"/guilds/{guild_id}/members/{user_id}")
    if isinstance(payload, dict):
        return payload, None
    if status == 404:
        return None, None
    return None, error


def guild_media_url(guild_id: str, field: str, media_hash: str | None) -> str | None:
    if not guild_id or not media_hash:
        return None
    return f"https://cdn.discordapp.com/{field}/{guild_id}/{media_hash}.png"


def enrich_guild_media(guild_id: str, guild_data: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(guild_data)
    enriched["icon_url"] = guild_media_url(guild_id, "icons", enriched.get("icon"))
    enriched["banner_url"] = guild_media_url(guild_id, "banners", enriched.get("banner"))
    return enriched


def get_guild_detail_info(token: str, guild_id: str) -> dict[str, Any] | None:
    payload, _, _ = discord_api_json(token, f"/guilds/{guild_id}")
    if isinstance(payload, dict):
        return enrich_guild_media(guild_id, payload)
    return None


def get_guild_roles(token: str, guild_id: str) -> list[dict[str, Any]]:
    payload, _, _ = discord_api_json(token, f"/guilds/{guild_id}/roles")
    return payload if isinstance(payload, list) else []


def get_guild_invites(token: str, guild_id: str) -> tuple[list[dict[str, Any]], str | None]:
    payload, _, error = discord_api_json(token, f"/guilds/{guild_id}/invites")
    if not isinstance(payload, list):
        return [], error or "Invitations existantes illisibles."
    return [invite for invite in payload if isinstance(invite, dict)], None


def get_guild_permanent_invites(token: str, guild_id: str) -> list[dict[str, Any]]:
    invites, _ = get_guild_invites(token, guild_id)
    return [invite for invite in invites if invite.get("max_age") == 0]


def get_guild_vanity_url(token: str, guild_id: str) -> str | None:
    payload, _, _ = discord_api_json(token, f"/guilds/{guild_id}/vanity-url")
    if isinstance(payload, dict) and payload.get("code"):
        return f"https://discord.gg/{payload['code']}"
    return None


def guild_member_avatar_url(guild_id: str, user_id: str, avatar_hash: str | None) -> str | None:
    if not guild_id or not user_id or not avatar_hash:
        return None
    return f"https://cdn.discordapp.com/guilds/{guild_id}/users/{user_id}/avatars/{avatar_hash}.png"


def role_color_hex(value: Any) -> str | None:
    try:
        color = int(value or 0)
    except (TypeError, ValueError):
        return None
    if color <= 0:
        return None
    return f"#{color:06x}"


def build_member_roles(member_info: dict[str, Any], roles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    roles_by_id = {str(role.get("id")): role for role in roles if isinstance(role, dict)}
    result = []
    for role_id in member_info.get("roles") or []:
        role = roles_by_id.get(str(role_id))
        if not role:
            continue
        result.append(
            {
                "id": str(role.get("id")),
                "name": role.get("name") or str(role.get("id")),
                "color": role.get("color", 0),
                "color_hex": role_color_hex(role.get("color")),
                "position": role.get("position", 0),
            }
        )
    return sorted(result, key=lambda item: item.get("position") or 0, reverse=True)


def important_member_permissions(member_info: dict[str, Any]) -> list[str]:
    try:
        permissions = int(member_info.get("permissions") or 0)
    except (TypeError, ValueError):
        return []
    flags = [
        (1 << 3, "Administrateur"),
        (1 << 5, "Gerer le serveur"),
        (1 << 13, "Gerer les messages"),
        (1 << 28, "Gerer les roles"),
        (1 << 2, "Bannir"),
        (1 << 1, "Expulser"),
    ]
    return [label for bit, label in flags if permissions & bit]


def public_flag_badges(flags_value: Any) -> list[str]:
    try:
        flags = int(flags_value or 0)
    except (TypeError, ValueError):
        return []
    known_flags = [
        (1 << 0, "Staff Discord"),
        (1 << 1, "Partenaire Discord"),
        (1 << 2, "HypeSquad Events"),
        (1 << 6, "HypeSquad Bravery"),
        (1 << 7, "HypeSquad Brilliance"),
        (1 << 8, "HypeSquad Balance"),
        (1 << 9, "Early Supporter"),
        (1 << 14, "Bug Hunter 1"),
        (1 << 16, "Bug Hunter 2"),
        (1 << 22, "Active Developer"),
    ]
    return [label for bit, label in known_flags if flags & bit]


def premium_type_label(value: Any) -> str | None:
    try:
        premium_type = int(value or 0)
    except (TypeError, ValueError):
        return None
    return {1: "Nitro Classic", 2: "Nitro", 3: "Nitro Basic"}.get(premium_type)


def verification_level_label(value: Any) -> str:
    try:
        level = int(value or 0)
    except (TypeError, ValueError):
        return "Inconnu"
    return {
        0: "Aucune",
        1: "Faible",
        2: "Moyenne",
        3: "Elevee",
        4: "Tres elevee",
    }.get(level, "Inconnu")


def snowflake_created_iso(value: Any) -> str | None:
    created_at = snowflake_created_at(value)
    if not created_at:
        return None
    return datetime.fromtimestamp(created_at).isoformat(timespec="seconds")


def build_mutual_guild_payload(
    token: str,
    guild_id: str,
    base_guild: dict[str, Any],
    member_info: dict[str, Any],
    target_user_id: str,
) -> dict[str, Any]:
    guild_info = get_guild_detail_info(token, guild_id) or {}
    roles = get_guild_roles(token, guild_id)
    permanent_invites = get_guild_permanent_invites(token, guild_id)
    vanity_url = get_guild_vanity_url(token, guild_id)
    user_roles = build_member_roles(member_info, roles)

    guild_payload = {
        **base_guild,
        **guild_info,
        "id": guild_id,
        "name": guild_info.get("name") or base_guild.get("name") or guild_id,
        "icon_url": guild_info.get("icon_url") or base_guild.get("icon_url") or guild_icon_url(guild_id, base_guild.get("icon")),
        "banner_url": guild_info.get("banner_url"),
        "vanity_url": vanity_url,
        "permanent_invites": permanent_invites,
        "created_at": snowflake_created_iso(guild_id),
        "verification_label": verification_level_label(guild_info.get("verification_level")),
    }

    member_payload = dict(member_info)
    member_payload["server_avatar_url"] = guild_member_avatar_url(guild_id, target_user_id, member_info.get("avatar"))
    member_payload["user_roles"] = user_roles
    member_payload["important_permissions"] = important_member_permissions(member_info)

    return {
        "guild": guild_payload,
        "member": member_payload,
        "roles": roles,
        "user_roles": user_roles,
        "permanent_invites": permanent_invites,
        "vanity_url": vanity_url,
    }


def guild_icon_url(guild_id: str, icon_hash: str | None) -> str | None:
    if not guild_id or not icon_hash:
        return None
    return f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.png"


def target_user_label(user_data: dict[str, Any] | None, fallback_user_id: str) -> str:
    if not user_data:
        return fallback_user_id
    display_name = user_data.get("global_name") or user_data.get("username")
    username = user_data.get("username")
    discriminator = user_data.get("discriminator")
    if username and discriminator and discriminator != "0":
        return f"{display_name or username} ({username}#{discriminator})"
    if username:
        return f"{display_name or username} (@{username})"
    return display_name or fallback_user_id


def create_mutual_guild_run(target_user_id: str) -> str:
    run_id = secrets.token_hex(8)
    profiles = load_profiles(include_tokens=False)
    target_user_id = str(target_user_id).strip()

    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO mutual_guild_runs (id, target_user_id, profile_count, completed_profiles, status)
        VALUES (?, ?, ?, 0, 'pending')
        """,
        (run_id, target_user_id, len(profiles)),
    )
    conn.commit()
    conn.close()

    thread = threading.Thread(
        target=run_mutual_guild_task,
        args=(run_id,),
        daemon=True,
        name=f"mutual-guild-{run_id}",
    )
    thread.start()
    return run_id


def run_mutual_guild_task(run_id: str) -> None:
    with mutual_guild_lock:
        if run_id in mutual_guild_active_runs:
            return
        mutual_guild_active_runs.add(run_id)

    try:
        conn = get_db_connection()
        run = row_to_dict(conn.execute("SELECT * FROM mutual_guild_runs WHERE id = ?", (run_id,)).fetchone())
        if not run:
            conn.close()
            return
        target_user_id = str(run["target_user_id"])
        conn.execute(
            """
            UPDATE mutual_guild_runs
            SET status = 'running', completed_profiles = 0, guild_count = 0,
                mutual_count = 0, error = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (run_id,),
        )
        conn.execute("DELETE FROM mutual_guild_results WHERE run_id = ?", (run_id,))
        conn.commit()
        conn.close()

        profiles = load_profiles(include_tokens=True)
        guild_index: dict[str, dict[str, Any]] = {}
        first_token = profiles[0]["token"] if profiles else ""
        target_info = fetch_target_user_info(first_token, target_user_id) if first_token else None
        errors: list[str] = []
        fast_profile_count = 0

        for profile in profiles:
            guilds = get_user_guilds(profile["token"], profile.get("user_id"))
            conn = get_db_connection()
            upsert_guilds(conn, guilds)
            conn.commit()
            conn.close()

            for guild in guilds:
                guild_id = str(guild.get("id") or "")
                if not guild_id:
                    continue
                entry = guild_index.setdefault(
                    guild_id,
                    {
                        "guild": guild,
                        "profiles": [],
                        "mutual_profiles": [],
                    },
                )
                entry["profiles"].append(profile)

            profile_payload = fetch_target_profile_payload(profile["token"], target_user_id)
            if isinstance(profile_payload, dict):
                if not target_info and isinstance(profile_payload.get("user"), dict):
                    user_data = enrich_discord_user_media(profile_payload["user"])
                    profile_data = profile_payload.get("user_profile") or {}
                    if isinstance(profile_data, dict):
                        user_data["bio"] = profile_data.get("bio")
                        user_data["pronouns"] = profile_data.get("pronouns")
                    target_info = enrich_target_user_profile(user_data, target_user_id)

                mutual_guilds = profile_payload.get("mutual_guilds")
                if isinstance(mutual_guilds, list):
                    fast_profile_count += 1
                    for mutual_guild in mutual_guilds:
                        if isinstance(mutual_guild, dict):
                            guild_id = str(mutual_guild.get("id") or "")
                            mutual_payload = mutual_guild
                        else:
                            guild_id = str(mutual_guild or "")
                            mutual_payload = {"id": guild_id}
                        if guild_id and guild_id in guild_index:
                            guild_index[guild_id]["mutual_profiles"].append((profile, mutual_payload))

            conn = get_db_connection()
            conn.execute(
                """
                UPDATE mutual_guild_runs
                SET completed_profiles = completed_profiles + 1,
                    guild_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (len(guild_index), run_id),
            )
            conn.commit()
            conn.close()

        mutual_count = 0
        for guild_id, entry in guild_index.items():
            member_info = None
            selected_profile = None
            last_error = None
            fast_matches = entry.get("mutual_profiles") or []
            if fast_matches:
                selected_profile, mutual_payload = fast_matches[0]
                member_info, last_error = get_guild_member_info(selected_profile["token"], guild_id, target_user_id)
                if not member_info:
                    member_info = {
                        "nick": mutual_payload.get("nick"),
                        "joined_at": mutual_payload.get("joined_at"),
                        "user": target_info or {},
                        "source": "profile_mutual_guilds",
                    }
            elif fast_profile_count < len(profiles):
                for profile in entry["profiles"]:
                    member_info, last_error = get_guild_member_info(profile["token"], guild_id, target_user_id)
                    if member_info:
                        selected_profile = profile
                        break

            if not member_info or not selected_profile:
                if last_error:
                    errors.append(f"{entry['guild'].get('name') or guild_id}: {last_error}")
                continue

            if not target_info and isinstance(member_info.get("user"), dict):
                target_info = enrich_target_user_profile(enrich_discord_user_media(member_info["user"]), target_user_id)

            guild = entry["guild"]
            detail_payload = build_mutual_guild_payload(
                selected_profile["token"],
                guild_id,
                guild,
                member_info,
                target_user_id,
            )
            guild_payload = detail_payload["guild"]
            member_payload = detail_payload["member"]
            user_roles = detail_payload["user_roles"]
            permanent_invites = detail_payload["permanent_invites"]
            found_by = [profile_public_summary(profile) for profile in entry["profiles"]]
            member_user = member_info.get("user") if isinstance(member_info.get("user"), dict) else {}
            nick = member_info.get("nick") or member_user.get("global_name") or member_user.get("username")

            conn = get_db_connection()
            conn.execute(
                """
                INSERT OR REPLACE INTO mutual_guild_results (
                    run_id, guild_id, guild_name, guild_icon_url, owner_id, member_count,
                    guild_banner_url, vanity_url, invite_count, role_count,
                    target_nick, target_joined_at, selected_profile_id, found_by_profiles,
                    member_payload, guild_payload, user_roles_payload, invites_payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    guild_id,
                    guild_payload.get("name") or guild.get("name") or guild_id,
                    guild_payload.get("icon_url") or guild.get("icon_url") or guild_icon_url(guild_id, guild.get("icon")),
                    guild_payload.get("owner_id") or guild.get("owner_id"),
                    guild_payload.get("approximate_member_count") or guild_payload.get("member_count") or guild.get("member_count"),
                    guild_payload.get("banner_url"),
                    detail_payload.get("vanity_url"),
                    len(permanent_invites),
                    len(user_roles),
                    nick,
                    member_payload.get("joined_at"),
                    selected_profile["id"],
                    json.dumps(found_by, ensure_ascii=False),
                    json.dumps(member_payload, ensure_ascii=False),
                    json.dumps(guild_payload, ensure_ascii=False),
                    json.dumps(user_roles, ensure_ascii=False),
                    json.dumps(permanent_invites, ensure_ascii=False),
                ),
            )
            conn.commit()
            conn.close()
            mutual_count += 1

        conn = get_db_connection()
        conn.execute(
            """
            UPDATE mutual_guild_runs
            SET guild_count = ?, mutual_count = ?, status = 'completed',
                completed_profiles = profile_count, error = ?, target_user_payload = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                len(guild_index),
                mutual_count,
                "\n".join(errors[:8]) if errors else None,
                json.dumps(target_info, ensure_ascii=False) if target_info else None,
                run_id,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as error:
        conn = get_db_connection()
        conn.execute(
            """
            UPDATE mutual_guild_runs
            SET status = 'failed', error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (str(error)[:1000], run_id),
        )
        conn.commit()
        conn.close()
    finally:
        with mutual_guild_lock:
            mutual_guild_active_runs.discard(run_id)


def mutual_guild_status_label(status: str | None) -> str:
    return {
        "pending": "En attente",
        "running": "Scan en cours",
        "completed": "Termine",
        "failed": "Erreur",
    }.get(status or "pending", status or "En attente")


def mutual_guild_status_badge_class(status: str | None) -> str:
    return {
        "pending": "text-bg-secondary",
        "running": "text-bg-primary",
        "completed": "text-bg-success",
        "failed": "text-bg-danger",
    }.get(status or "pending", "text-bg-secondary")


def mutual_guild_progress(run: dict[str, Any] | None) -> float:
    if not run:
        return 0
    try:
        total = int(run.get("profile_count") or 0)
        done = int(run.get("completed_profiles") or 0)
    except (TypeError, ValueError):
        return 0
    if total <= 0:
        return 100 if run.get("status") == "completed" else 0
    return min(100, max(0, (done / total) * 100))


def load_mutual_run(run_id: str | None) -> dict[str, Any] | None:
    if not run_id:
        return None
    conn = get_db_connection()
    run = row_to_dict(conn.execute("SELECT * FROM mutual_guild_runs WHERE id = ?", (run_id,)).fetchone())
    conn.close()
    if run:
        try:
            run["target_user_payload"] = json.loads(run.get("target_user_payload") or "null")
        except (TypeError, ValueError):
            run["target_user_payload"] = None
    return run


def list_mutual_runs(limit: int = 12) -> list[dict[str, Any]]:
    conn = get_db_connection()
    runs = rows_to_dicts(
        conn.execute(
            """
            SELECT *
            FROM mutual_guild_runs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    )
    conn.close()
    return [reconcile_message_scraper_run(run) or run for run in runs]


def list_mutual_results(run_id: str | None) -> list[dict[str, Any]]:
    if not run_id:
        return []
    conn = get_db_connection()
    results = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, p.name AS selected_profile_name, p.username AS selected_profile_username
            FROM mutual_guild_results r
            LEFT JOIN profiles p ON p.id = r.selected_profile_id
            WHERE r.run_id = ?
            ORDER BY r.guild_name COLLATE NOCASE
            """,
            (run_id,),
        ).fetchall()
    )
    conn.close()
    for result in results:
        try:
            result["found_by_profiles"] = json.loads(result.get("found_by_profiles") or "[]")
        except (TypeError, ValueError):
            result["found_by_profiles"] = []
        for field, fallback in (
            ("member_payload", {}),
            ("guild_payload", {}),
            ("user_roles_payload", []),
            ("invites_payload", []),
        ):
            try:
                result[field] = json.loads(result.get(field) or json.dumps(fallback))
            except (TypeError, ValueError):
                result[field] = fallback
        if not result.get("guild_banner_url") and isinstance(result.get("guild_payload"), dict):
            result["guild_banner_url"] = result["guild_payload"].get("banner_url")
        if not result.get("vanity_url") and isinstance(result.get("guild_payload"), dict):
            result["vanity_url"] = result["guild_payload"].get("vanity_url")
        if not result.get("role_count"):
            result["role_count"] = len(result.get("user_roles_payload") or [])
        if not result.get("invite_count"):
            result["invite_count"] = len(result.get("invites_payload") or [])
    return results


def message_scraper_status_label(status: str | None) -> str:
    return {
        "pending": "En attente",
        "running": "Scan en cours",
        "completed": "Termine",
        "failed": "Erreur",
    }.get(status or "pending", status or "En attente")


def message_scraper_status_badge_class(status: str | None) -> str:
    return {
        "pending": "text-bg-secondary",
        "running": "text-bg-primary",
        "completed": "text-bg-success",
        "failed": "text-bg-danger",
    }.get(status or "pending", "text-bg-secondary")


def search_messages(
    token: str,
    path: str,
    params: dict[str, Any],
    max_pages: int = SEARCH_MAX_PAGES,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
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

    def notify(phase: str, detail: str | None = None) -> None:
        if not progress_callback:
            return
        payload = dict(meta)
        payload["offset"] = offset
        payload["phase"] = phase
        payload["detail"] = detail
        progress_callback(payload)

    while True:
        if max_pages and meta["pages_fetched"] >= max_pages:
            meta["limited"] = True
            notify("Limite atteinte", f"{meta['pages_fetched']} page(s) recuperee(s)")
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
                notify("Erreur reseau", meta["error"])
                break

            meta["last_status"] = response.status_code
            if response.status_code != 202 or index_attempt >= SEARCH_INDEX_RETRIES:
                break

            delay = retry_after(response)
            notify("Index Discord", f"Indexation en cours, nouvelle tentative dans {delay:.1f}s")
            time.sleep(delay)

        if not response:
            break

        if response.status_code == 202:
            meta["error"] = "Index de recherche Discord temporairement indisponible."
            notify("Index Discord", meta["error"])
            break

        if response.status_code != 200:
            meta["error"] = f"Recherche Discord interrompue avec le statut HTTP {response.status_code}."
            notify("Erreur Discord", meta["error"])
            break

        try:
            payload = response.json()
        except ValueError:
            meta["error"] = "Reponse Discord illisible."
            notify("Erreur Discord", meta["error"])
            break

        pages = payload.get("messages", [])
        if not pages:
            notify("Recherche terminee", f"{meta['pages_fetched']} page(s), {meta['messages_seen']} message(s) vus")
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

        notify("Recherche Discord", f"{meta['pages_fetched']} page(s), {meta['messages_seen']} message(s) vus")
        offset += SEARCH_PAGE_SIZE
        if meta["total_results"] <= offset:
            notify("Recherche terminee", f"{meta['pages_fetched']} page(s), {meta['messages_seen']} message(s) vus")
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
                "author_id": message.get("author", {}).get("id") or user_id,
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


def create_message_scraper_run(
    mutual_run_id: str | None,
    target_user_id: str,
    selected_guild_ids: list[str],
    max_pages: int,
) -> str:
    run_id = secrets.token_hex(8)
    normalized_guild_ids = []
    seen = set()
    for guild_id in selected_guild_ids:
        guild_id = str(guild_id or "").strip()
        if guild_id and guild_id not in seen:
            normalized_guild_ids.append(guild_id)
            seen.add(guild_id)

    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO message_scraper_runs (
            id, mutual_run_id, target_user_id, status, total_guilds, completed_guilds,
            total_messages, total_pages
        )
        VALUES (?, ?, ?, 'pending', ?, 0, 0, ?)
        """,
        (run_id, mutual_run_id or None, target_user_id, len(normalized_guild_ids), 0),
    )
    conn.commit()
    conn.close()

    thread = threading.Thread(
        target=run_message_scraper_task,
        args=(run_id, normalized_guild_ids, max_pages),
        daemon=True,
        name=f"message-scraper-{run_id}",
    )
    thread.start()
    return run_id


def load_message_scraper_run(run_id: str | None) -> dict[str, Any] | None:
    if not run_id:
        return None
    conn = get_db_connection()
    run = row_to_dict(conn.execute("SELECT * FROM message_scraper_runs WHERE id = ?", (run_id,)).fetchone())
    conn.close()
    return reconcile_message_scraper_run(run)


def list_message_scraper_runs(limit: int = 12, mutual_run_id: str | None = None) -> list[dict[str, Any]]:
    conn = get_db_connection()
    where_clause = "WHERE mutual_run_id = ?" if mutual_run_id else ""
    params: tuple[Any, ...] = (mutual_run_id, limit) if mutual_run_id else (limit,)
    runs = rows_to_dicts(
        conn.execute(
            f"""
            SELECT *
            FROM message_scraper_runs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    )
    conn.close()
    return runs


def message_scraper_progress(run: dict[str, Any] | None) -> float:
    if not run:
        return 0
    try:
        total = int(run.get("total_guilds") or 0)
        done = int(run.get("completed_guilds") or 0)
    except (TypeError, ValueError):
        return 0
    if total <= 0:
        return 100 if run.get("status") == "completed" else 0
    return min(100, max(0, (done / total) * 100))


def update_message_scraper_run_fields(run_id: str, **fields: Any) -> None:
    allowed = {
        "status",
        "completed_guilds",
        "total_messages",
        "total_pages",
        "current_guild_id",
        "current_guild_name",
        "current_phase",
        "current_detail",
        "last_status_code",
        "error",
        "completed_at",
    }
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return

    assignments = [f"{key} = ?" for key in updates]
    assignments.append("updated_at = CURRENT_TIMESTAMP")
    values = list(updates.values())
    values.append(run_id)
    conn = get_db_connection()
    conn.execute(
        f"UPDATE message_scraper_runs SET {', '.join(assignments)} WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()


def inactive_tool_run_age_seconds(run: dict[str, Any]) -> float:
    raw_value = run.get("updated_at") or run.get("created_at")
    if not raw_value:
        return 0
    try:
        parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0, current_timestamp() - parsed.timestamp())
    except ValueError:
        return 0


def reconcile_message_scraper_run(run: dict[str, Any] | None) -> dict[str, Any] | None:
    if not run or run.get("status") not in {"pending", "running"}:
        return run
    with message_scraper_lock:
        active = run["id"] in message_scraper_active_runs
    if active or inactive_tool_run_age_seconds(run) < 30:
        return run

    error = "Run interrompu: aucun worker actif ne suit ce scrape."
    update_message_scraper_run_fields(
        run["id"],
        status="failed",
        error=error,
        current_phase="Interrompu",
        current_detail=error,
        completed_at=datetime.now().isoformat(timespec="seconds"),
    )
    run = dict(run)
    run["status"] = "failed"
    run["error"] = error
    run["current_phase"] = "Interrompu"
    run["current_detail"] = error
    return run


def list_message_scraper_results(run_id: str | None, limit: int | None = None) -> list[dict[str, Any]]:
    if not run_id:
        return []
    query = """
        SELECT *
        FROM message_scraper_results
        WHERE run_id = ?
        ORDER BY COALESCE(timestamp, '') DESC, id DESC
    """
    params: tuple[Any, ...]
    if limit:
        query += " LIMIT ?"
        params = (run_id, limit)
    else:
        params = (run_id,)

    conn = get_db_connection()
    rows = rows_to_dicts(conn.execute(query, params).fetchall())
    conn.close()
    for row in rows:
        raw_payload = {}
        try:
            raw_payload = json.loads(row.get("raw_json") or "{}")
        except (TypeError, ValueError):
            raw_payload = {}
        attachments = raw_payload.get("attachments") if isinstance(raw_payload, dict) else []
        embeds = raw_payload.get("embeds") if isinstance(raw_payload, dict) else []
        content = row.get("content") or ""
        row["attachments"] = attachments if isinstance(attachments, list) else []
        row["embeds"] = embeds if isinstance(embeds, list) else []
        row["has_links"] = bool(re.search(r"https?://|discord\.gg/", content, re.IGNORECASE))
        row["has_attachments"] = bool(row["attachments"])
        row["has_embeds"] = bool(row["embeds"])
        row["has_images"] = any(
            str(item.get("content_type") or item.get("filename") or item.get("url") or "").lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif"))
            or str(item.get("content_type") or "").lower().startswith("image/")
            for item in row["attachments"]
            if isinstance(item, dict)
        )
        row["is_empty"] = not content.strip()
    return rows


def grouped_message_scraper_results(run_id: str | None, limit: int = MESSAGE_SCRAPER_PREVIEW_LIMIT) -> list[dict[str, Any]]:
    rows = list_message_scraper_results(run_id, limit)
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        guild_id = row.get("guild_id") or "unknown"
        guild = grouped.setdefault(
            guild_id,
            {
                "guild_id": guild_id,
                "guild_name": row.get("guild_name") or guild_id,
                "channels": {},
                "total_messages": 0,
            },
        )
        guild["total_messages"] += 1
        channel_id = row.get("channel_id") or "unknown"
        channel = guild["channels"].setdefault(
            channel_id,
            {
                "channel_id": channel_id,
                "channel_name": row.get("channel_name") or channel_id,
                "messages": [],
            },
        )
        channel["messages"].append(row)

    result = []
    for guild in grouped.values():
        guild["channels"] = sorted(
            guild["channels"].values(),
            key=lambda item: item["channel_name"].lower(),
        )
        result.append(guild)
    return sorted(result, key=lambda item: item["guild_name"].lower())


def message_scraper_view_data(run_id: str | None) -> dict[str, Any]:
    messages = list_message_scraper_results(run_id, MESSAGE_SCRAPER_PREVIEW_LIMIT)
    channels: dict[str, dict[str, Any]] = {}
    guilds: dict[str, dict[str, Any]] = {}
    stats = {
        "messages": len(messages),
        "channels": 0,
        "guilds": 0,
        "attachments": 0,
        "links": 0,
        "images": 0,
        "empty": 0,
        "limited": False,
    }

    for message in messages:
        guild_id = message.get("guild_id") or "unknown"
        channel_id = message.get("channel_id") or "unknown"
        guild = guilds.setdefault(
            guild_id,
            {
                "id": guild_id,
                "name": message.get("guild_name") or guild_id,
                "count": 0,
            },
        )
        channel = channels.setdefault(
            channel_id,
            {
                "id": channel_id,
                "name": message.get("channel_name") or channel_id,
                "guild_id": guild_id,
                "guild_name": guild["name"],
                "count": 0,
            },
        )
        guild["count"] += 1
        channel["count"] += 1
        stats["attachments"] += int(message.get("attachment_count") or 0)
        if message.get("has_links"):
            stats["links"] += 1
        if message.get("has_images"):
            stats["images"] += 1
        if message.get("is_empty"):
            stats["empty"] += 1

    stats["channels"] = len(channels)
    stats["guilds"] = len(guilds)
    stats["limited"] = len(messages) >= MESSAGE_SCRAPER_PREVIEW_LIMIT
    return {
        "messages": messages,
        "channels": sorted(channels.values(), key=lambda item: (item["guild_name"].lower(), item["name"].lower())),
        "guilds": sorted(guilds.values(), key=lambda item: item["name"].lower()),
        "stats": stats,
    }


def selected_scraper_targets(mutual_run_id: str | None, guild_ids: list[str]) -> list[dict[str, Any]]:
    if mutual_run_id:
        mutual_results = {
            result["guild_id"]: result
            for result in list_mutual_results(mutual_run_id)
        }
        return [
            mutual_results[guild_id]
            for guild_id in guild_ids
            if guild_id in mutual_results and mutual_results[guild_id].get("selected_profile_id")
        ]

    return []


def run_message_scraper_task(run_id: str, guild_ids: list[str], max_pages: int) -> None:
    with message_scraper_lock:
        if run_id in message_scraper_active_runs:
            return
        message_scraper_active_runs.add(run_id)

    try:
        conn = get_db_connection()
        run = row_to_dict(conn.execute("SELECT * FROM message_scraper_runs WHERE id = ?", (run_id,)).fetchone())
        if not run:
            conn.close()
            return
        conn.close()
        update_message_scraper_run_fields(
            run_id,
            status="running",
            current_phase="Preparation",
            current_detail="Chargement des comptes et serveurs selectionnes.",
        )

        target_user_id = str(run["target_user_id"])
        mutual_run_id = run.get("mutual_run_id")
        targets = selected_scraper_targets(mutual_run_id, guild_ids)
        profiles = {profile["id"]: profile for profile in load_profiles(include_tokens=True)}
        total_messages = 0
        total_pages = 0

        for target in targets:
            profile = profiles.get(target.get("selected_profile_id"))
            if not profile:
                continue

            guild_id = str(target["guild_id"])
            guild_name = target.get("guild_name") or guild_id
            update_message_scraper_run_fields(
                run_id,
                current_guild_id=guild_id,
                current_guild_name=guild_name,
                current_phase="Resolution salons",
                current_detail=f"Lecture des salons de {guild_name}.",
            )
            channels = get_guild_channels(profile["token"], guild_id)
            channel_names = {channel["id"]: channel["name"] for channel in channels}
            conn = get_db_connection()
            upsert_channels(conn, guild_id, channels)
            conn.commit()
            conn.close()

            base_pages = total_pages

            def scraper_progress(meta: dict[str, Any]) -> None:
                update_message_scraper_run_fields(
                    run_id,
                    total_pages=base_pages + int(meta.get("pages_fetched") or 0),
                    current_phase=meta.get("phase") or "Recherche Discord",
                    current_detail=meta.get("detail") or f"{meta.get('messages_seen') or 0} message(s) vus.",
                    last_status_code=meta.get("last_status"),
                )

            raw_messages, meta = search_messages(
                profile["token"],
                f"/guilds/{guild_id}/messages/search",
                {"author_id": target_user_id},
                max_pages=max_pages,
                progress_callback=scraper_progress,
            )
            update_message_scraper_run_fields(
                run_id,
                current_phase="Insertion",
                current_detail=f"{len(raw_messages)} message(s) bruts a normaliser pour {guild_name}.",
                last_status_code=meta.get("last_status"),
            )
            messages = normalize_user_messages(raw_messages, target_user_id)
            messages.sort(key=lambda message: parse_discord_timestamp(message.get("timestamp")), reverse=True)
            unknown_channel_ids = sorted(
                {
                    str(message.get("channel_id") or "")
                    for message in messages
                    if message.get("channel_id") and str(message.get("channel_id")) not in channel_names
                }
            )
            if unknown_channel_ids:
                update_message_scraper_run_fields(
                    run_id,
                    current_phase="Resolution salons",
                    current_detail=f"Resolution de {len(unknown_channel_ids)} salon(s) manquant(s) pour {guild_name}.",
                )
                for channel_id in unknown_channel_ids:
                    resolve_channel_name(profile["token"], channel_id, channel_names)
            update_message_scraper_run_fields(
                run_id,
                current_phase="Insertion",
                current_detail=f"Insertion locale de {len(messages)} message(s) pour {guild_name}.",
            )

            conn = get_db_connection()
            inserted = 0
            for message in messages:
                channel_id = str(message.get("channel_id") or "")
                channel_name = channel_names.get(channel_id) or ("Salon inconnu" if not channel_id else f"Salon {channel_id}")
                attachment_count = len(message.get("attachments") or [])
                embed_count = len(message.get("embeds") or [])
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO message_scraper_results (
                        run_id, profile_id, guild_id, guild_name, channel_id, channel_name,
                        message_id, timestamp, content, attachment_count, embed_count, jump_url, raw_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        profile["id"],
                        guild_id,
                        guild_name,
                        channel_id,
                        channel_name,
                        message.get("id"),
                        message.get("timestamp"),
                        message.get("content") or "",
                        attachment_count,
                        embed_count,
                        discord_jump_url(guild_id, channel_id, message.get("id")),
                        json.dumps(message, ensure_ascii=False),
                    ),
                )
                inserted += cursor.rowcount

            total_messages += inserted
            total_pages += int(meta.get("pages_fetched") or 0)
            conn.execute(
                """
                UPDATE message_scraper_runs
                SET completed_guilds = completed_guilds + 1,
                    total_messages = ?,
                    total_pages = ?,
                    current_phase = ?,
                    current_detail = ?,
                    last_status_code = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    total_messages,
                    total_pages,
                    "Serveur termine",
                    f"{inserted} nouveau(x) message(s) inseres pour {guild_name}.",
                    meta.get("last_status"),
                    run_id,
                ),
            )
            conn.commit()
            conn.close()

        conn = get_db_connection()
        conn.execute(
            """
            UPDATE message_scraper_runs
            SET status = 'completed', total_messages = ?, total_pages = ?,
                completed_guilds = total_guilds, updated_at = CURRENT_TIMESTAMP,
                current_guild_id = NULL, current_guild_name = NULL,
                current_phase = 'Termine', current_detail = ?,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (total_messages, total_pages, f"{total_messages} message(s) collectes.", run_id),
        )
        conn.commit()
        conn.close()
    except Exception as error:
        conn = get_db_connection()
        conn.execute(
            """
            UPDATE message_scraper_runs
            SET status = 'failed', error = ?, current_phase = 'Erreur',
                current_detail = ?, updated_at = CURRENT_TIMESTAMP,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (str(error)[:1000], str(error)[:1000], run_id),
        )
        conn.commit()
        conn.close()
    finally:
        with message_scraper_lock:
            message_scraper_active_runs.discard(run_id)


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


def discord_error_payload(response: requests.Response | None) -> dict[str, Any]:
    if response is None:
        return {}
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def discord_error_code(response: requests.Response | None) -> int | None:
    payload = discord_error_payload(response)
    try:
        return int(payload.get("code"))
    except (TypeError, ValueError):
        return None


def discord_response_error(response: requests.Response | None) -> str:
    if response is None:
        return "Aucune reponse Discord."

    payload = discord_error_payload(response)

    if payload:
        message = payload.get("message")
        code = payload.get("code")
        if message and code:
            return f"HTTP {response.status_code} - {message} ({code})"
        if message:
            return f"HTTP {response.status_code} - {message}"

    return f"HTTP {response.status_code}"


def extract_disboard_bump_command(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        commands = payload
    elif isinstance(payload, dict):
        commands = payload.get("application_commands") or payload.get("commands") or []
    else:
        commands = []

    for command in commands:
        if not isinstance(command, dict):
            continue
        if command.get("name") != "bump":
            continue
        application_id = str(command.get("application_id") or command.get("application", {}).get("id") or "")
        if application_id and application_id != AUTOBUMPER_DISBOARD_APP_ID:
            continue
        if not command.get("id"):
            continue
        normalized = dict(command)
        normalized["id"] = str(normalized["id"])
        normalized["application_id"] = AUTOBUMPER_DISBOARD_APP_ID
        if normalized.get("version"):
            normalized["version"] = str(normalized["version"])
        return normalized

    return None


def get_cached_bump_command(cache_key: tuple[str, str, str]) -> dict[str, Any] | None:
    cached = autobumper_command_cache.get(cache_key)
    if not cached:
        return None
    if cached["expires_at"] <= current_timestamp():
        autobumper_command_cache.pop(cache_key, None)
        return None
    return dict(cached["command"])


def cache_bump_command(cache_key: tuple[str, str, str], command: dict[str, Any]) -> None:
    autobumper_command_cache[cache_key] = {
        "command": dict(command),
        "expires_at": current_timestamp() + AUTOBUMPER_COMMAND_CACHE_TTL,
    }


def fetch_disboard_bump_command(token: str, guild_id: str, channel_id: str) -> tuple[dict[str, Any] | None, str | None]:
    cache_key = (guild_id, channel_id, AUTOBUMPER_DISBOARD_APP_ID)
    cached = get_cached_bump_command(cache_key)
    if cached:
        return cached, None

    endpoints = [
        (
            f"/channels/{channel_id}/application-commands/search",
            {
                "type": 1,
                "query": "bump",
                "limit": 25,
                "include_applications": "true",
            },
        ),
        (f"/applications/{AUTOBUMPER_DISBOARD_APP_ID}/guilds/{guild_id}/commands", None),
        (f"/guilds/{guild_id}/application-command-index", None),
    ]
    last_error = None

    for path, params in endpoints:
        kwargs = {"params": params} if params else {}
        response = discord_request(token, "GET", path, **kwargs)
        if not response:
            last_error = "Aucune reponse Discord pendant la resolution de /bump."
            continue
        if response.status_code != 200:
            last_error = discord_response_error(response)
            continue

        try:
            payload = response.json()
        except ValueError:
            last_error = "Reponse Discord illisible pendant la resolution de /bump."
            continue

        command = extract_disboard_bump_command(payload)
        if command:
            cache_bump_command(cache_key, command)
            return command, None

    return None, last_error or "Commande /bump Disboard introuvable pour ce salon."


def invoke_disboard_bump(token: str, guild_id: str, channel_id: str) -> tuple[bool, str]:
    command, command_error = fetch_disboard_bump_command(token, guild_id, channel_id)
    if not command:
        return False, command_error or "Commande /bump Disboard introuvable."

    command_type = int(command.get("type") or 1)
    data: dict[str, Any] = {
        "id": command["id"],
        "name": command.get("name") or "bump",
        "type": command_type,
        "options": [],
        "application_command": command,
        "attachments": [],
    }
    if command.get("version"):
        data["version"] = command["version"]

    payload = {
        "type": 2,
        "application_id": AUTOBUMPER_DISBOARD_APP_ID,
        "guild_id": guild_id,
        "channel_id": channel_id,
        "session_id": secrets.token_hex(16),
        "data": data,
        "nonce": str(int(current_timestamp() * 1000)),
    }

    response = discord_request(token, "POST", "/interactions", json=payload)
    if response is not None and response.status_code in {200, 204}:
        return True, "Bump envoye."

    if response is not None and response.status_code == 400:
        slim_payload = dict(payload)
        slim_payload["data"] = {key: value for key, value in data.items() if key != "application_command"}
        retry_response = discord_request(token, "POST", "/interactions", json=slim_payload)
        if retry_response is not None and retry_response.status_code in {200, 204}:
            return True, "Bump envoye."
        response = retry_response or response

    return False, discord_response_error(response)


def send_channel_message(token: str, channel_id: str, content: str) -> tuple[bool, str]:
    text = content.strip()
    if not text:
        return True, "Aucun message personnalise a envoyer."

    response = discord_request(
        token,
        "POST",
        f"/channels/{channel_id}/messages",
        json={"content": text[:1900]},
    )
    if response is not None and response.status_code in {200, 201}:
        return True, "Message personnalise envoye."
    return False, discord_response_error(response)


def discord_json_request(
    token: str,
    method: str,
    path: str,
    json_payload: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | list[Any] | None, int | None, str | None]:
    kwargs: dict[str, Any] = {}
    if json_payload is not None:
        kwargs["json"] = json_payload
    if params:
        kwargs["params"] = params
    response = discord_request(token, method, path, **kwargs)
    if response is None:
        return None, None, "Aucune reponse Discord."
    if response.status_code not in {200, 201, 204}:
        return None, response.status_code, discord_response_error(response)
    if response.status_code == 204:
        return {}, response.status_code, None
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    return payload, response.status_code, None


SELF_TOOL_LABELS = {
    "dmall": "DM all",
    "delallfriends": "Supprimer amis",
    "bcreate": "Backup serveur",
    "bload": "Deploy backup",
}


def selfbot_tool_label(tool: str | None) -> str:
    return SELF_TOOL_LABELS.get(tool or "", tool or "Outil")


def selfbot_tool_status_label(status: str | None) -> str:
    return {
        "pending": "En attente",
        "running": "En cours",
        "completed": "Termine",
        "failed": "Erreur",
    }.get(status or "pending", status or "En attente")


def selfbot_tool_status_badge_class(status: str | None) -> str:
    return {
        "pending": "text-bg-secondary",
        "running": "text-bg-primary",
        "completed": "text-bg-success",
        "failed": "text-bg-danger",
    }.get(status or "pending", "text-bg-secondary")


def create_selfbot_tool_run(profile_id: int | None, tool: str, result: dict[str, Any] | None = None) -> str:
    run_id = secrets.token_hex(8)
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO selfbot_tool_runs (id, profile_id, tool, status, result_json, current_detail)
        VALUES (?, ?, ?, 'pending', ?, 'En attente.')
        """,
        (run_id, profile_id, tool, json.dumps(result or {}, ensure_ascii=False)),
    )
    conn.commit()
    conn.close()
    return run_id


def update_selfbot_tool_run(run_id: str, **fields: Any) -> None:
    allowed = {
        "status",
        "total_count",
        "processed_count",
        "success_count",
        "fail_count",
        "current_detail",
        "result_json",
        "error",
        "completed_at",
    }
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return
    assignments = [f"{key} = ?" for key in updates]
    assignments.append("updated_at = CURRENT_TIMESTAMP")
    values = list(updates.values()) + [run_id]
    conn = get_db_connection()
    conn.execute(
        f"UPDATE selfbot_tool_runs SET {', '.join(assignments)} WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()


def append_selfbot_tool_result(run_id: str, key: str, entry: dict[str, Any], limit: int = 500) -> None:
    conn = get_db_connection()
    row = row_to_dict(conn.execute("SELECT result_json FROM selfbot_tool_runs WHERE id = ?", (run_id,)).fetchone())
    result = {}
    if row:
        try:
            result = json.loads(row.get("result_json") or "{}")
        except ValueError:
            result = {}
    entries = result.get(key)
    if not isinstance(entries, list):
        entries = []
    if len(entries) < limit:
        entries.append(entry)
    else:
        result[f"{key}_limited"] = True
    result[key] = entries
    conn.execute(
        "UPDATE selfbot_tool_runs SET result_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (json.dumps(result, ensure_ascii=False), run_id),
    )
    conn.commit()
    conn.close()


def load_selfbot_tool_run(run_id: str | None) -> dict[str, Any] | None:
    if not run_id:
        return None
    conn = get_db_connection()
    run = row_to_dict(
        conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username
            FROM selfbot_tool_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            WHERE r.id = ?
            """,
            (run_id,),
        ).fetchone()
    )
    conn.close()
    return run


def list_selfbot_tool_runs(limit: int = 20) -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username
            FROM selfbot_tool_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    )
    conn.close()
    return rows


def selfbot_tool_result_payload(run: dict[str, Any] | None) -> dict[str, Any]:
    if not run:
        return {}
    try:
        payload = json.loads(run.get("result_json") or "{}")
    except (TypeError, ValueError):
        payload = {}
    return payload if isinstance(payload, dict) else {}


def selfbot_tool_progress(run: dict[str, Any] | None) -> float:
    if not run:
        return 0
    if run.get("status") == "completed":
        return 100
    return bounded_progress(run.get("processed_count"), run.get("total_count"))


def selfbot_tool_public_payload(run: dict[str, Any] | None) -> dict[str, Any] | None:
    if not run:
        return None
    payload = dict(run)
    payload["result"] = selfbot_tool_result_payload(run)
    payload.pop("result_json", None)
    return payload


def server_backup_export_payload(backup: dict[str, Any] | None) -> dict[str, Any] | None:
    if not backup:
        return None
    payload = backup.get("payload") or {}
    channels = payload.get("channels") if isinstance(payload.get("channels"), list) else []
    roles = payload.get("roles") if isinstance(payload.get("roles"), list) else []
    return {
        "schema": "server_backup_export.v1",
        "exported_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "backup": {
            "id": backup.get("id"),
            "name": backup.get("name"),
            "source_profile": backup.get("profile_name"),
            "source_guild_id": backup.get("guild_id"),
            "source_guild_name": backup.get("guild_name"),
            "created_at": backup.get("created_at"),
            "counts": {
                "channels": len(channels),
                "roles": len(roles),
            },
        },
        "guild": payload.get("guild") or {},
        "channels": channels,
        "roles": roles,
        "raw": payload,
    }


def parse_spy_keywords(raw_value: str | None) -> list[str]:
    raw_parts = re.split(r"[\n,;]+", raw_value or "")
    keywords: list[str] = []
    seen: set[str] = set()
    for part in raw_parts:
        keyword = part.strip()
        normalized = keyword.casefold()
        if not keyword or normalized in seen:
            continue
        seen.add(normalized)
        keywords.append(keyword[:80])
    return keywords[:40]


def keyword_match(text: str, keywords: list[str]) -> str | None:
    lower = text.casefold()
    for keyword in keywords:
        if keyword.casefold() in lower:
            return keyword
    return None


def list_selfbot_spy_rules() -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username
            FROM selfbot_spy_rules r
            LEFT JOIN profiles p ON p.id = r.profile_id
            ORDER BY r.enabled DESC, r.updated_at DESC, r.id DESC
            """
        ).fetchall()
    )
    conn.close()
    for row in rows:
        try:
            row["keyword_list"] = json.loads(row.get("keywords") or "[]")
        except ValueError:
            row["keyword_list"] = []
    return rows


def list_selfbot_spy_events(limit: int = 30) -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT e.*, r.guild_name, r.channel_name, p.name AS profile_name
            FROM selfbot_spy_events e
            LEFT JOIN selfbot_spy_rules r ON r.id = e.rule_id
            LEFT JOIN profiles p ON p.id = e.profile_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    )
    conn.close()
    return rows


def list_selfbot_antigroup_settings() -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT p.id AS profile_id, p.name, p.username, p.user_id, p.avatar_url,
                   COALESCE(s.enabled, 0) AS enabled, s.last_scan_at, s.left_count,
                   s.fail_count, s.last_error, s.updated_at
            FROM profiles p
            LEFT JOIN selfbot_antigroup_settings s ON s.profile_id = p.id
            ORDER BY p.name COLLATE NOCASE
            """
        ).fetchall()
    )
    conn.close()
    return rows


def create_dm_channel(token: str, recipient_id: str) -> tuple[str | None, str | None]:
    payload, _, error = discord_json_request(
        token,
        "POST",
        "/users/@me/channels",
        json_payload={"recipient_id": str(recipient_id)},
    )
    if isinstance(payload, dict) and payload.get("id"):
        return str(payload["id"]), None
    return None, error or "DM impossible."


def delete_discord_relationship(token: str, user_id: str) -> tuple[bool, str | None]:
    _, status, error = discord_json_request(token, "DELETE", f"/users/@me/relationships/{user_id}")
    return status in {200, 204}, error


def leave_discord_channel(token: str, channel_id: str) -> tuple[bool, str | None]:
    _, status, error = discord_json_request(
        token,
        "DELETE",
        f"/channels/{channel_id}",
        params={"silent": "true"},
    )
    return status in {200, 204}, error


def change_hypesquad_house(token: str, house: str) -> tuple[bool, str]:
    house_map = {
        "bravery": 1,
        "brilliance": 2,
        "balance": 3,
    }
    if house == "leave":
        _, status, error = discord_json_request(token, "DELETE", "/hypesquad/online")
        return status in {200, 204}, error or "HypeSquad retire."
    if house not in house_map:
        return False, "Maison HypeSquad inconnue."
    _, status, error = discord_json_request(
        token,
        "POST",
        "/hypesquad/online",
        json_payload={"house_id": house_map[house]},
    )
    return status in {200, 204}, error or "HypeSquad mis a jour."


def get_user_direct_channels_raw(token: str) -> list[dict[str, Any]]:
    response = discord_request(token, "GET", "/users/@me/channels")
    if response is None or response.status_code != 200:
        return []
    try:
        payload = response.json()
    except ValueError:
        return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def get_guild_channels_raw(token: str, guild_id: str) -> list[dict[str, Any]]:
    response = discord_request(token, "GET", f"/guilds/{guild_id}/channels")
    if response is None or response.status_code != 200:
        return []
    try:
        payload = response.json()
    except ValueError:
        return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def sanitize_channel_backup(channel: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "id",
        "name",
        "type",
        "position",
        "parent_id",
        "topic",
        "nsfw",
        "rate_limit_per_user",
        "bitrate",
        "user_limit",
        "permission_overwrites",
    )
    return {key: channel.get(key) for key in keys if key in channel}


def build_server_backup_payload(token: str, guild_id: str) -> tuple[dict[str, Any] | None, str | None]:
    guild = get_guild_detail_info(token, guild_id) or {}
    if not guild:
        guilds = get_user_guilds(token)
        guild = next((item for item in guilds if str(item.get("id")) == str(guild_id)), {})
    if not guild:
        return None, "Serveur introuvable ou inaccessible."
    channels = get_guild_channels_raw(token, guild_id)
    roles = get_guild_roles(token, guild_id)
    payload = {
        "schema": "server_backup.v1",
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "guild": {
            "id": str(guild_id),
            "name": guild.get("name") or guild.get("guild_name") or str(guild_id),
            "icon_url": guild.get("icon_url"),
            "verification_level": guild.get("verification_level"),
            "default_message_notifications": guild.get("default_message_notifications"),
            "explicit_content_filter": guild.get("explicit_content_filter"),
        },
        "channels": [sanitize_channel_backup(channel) for channel in channels],
        "roles": [
            {
                "id": role.get("id"),
                "name": role.get("name"),
                "color": role.get("color"),
                "hoist": role.get("hoist"),
                "permissions": role.get("permissions"),
                "mentionable": role.get("mentionable"),
                "position": role.get("position"),
            }
            for role in roles
            if isinstance(role, dict) and role.get("name") != "@everyone"
        ],
    }
    return payload, None


def save_server_backup(profile_id: int, guild_id: str, payload: dict[str, Any], name: str | None = None) -> str:
    backup_id = secrets.token_hex(8)
    guild_name = ((payload.get("guild") or {}).get("name") or guild_id)[:200]
    backup_name = (name or f"{guild_name} - {datetime.now().strftime('%Y-%m-%d %H:%M')}").strip()[:200]
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO server_backups (id, profile_id, guild_id, guild_name, name, payload_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (backup_id, profile_id, guild_id, guild_name, backup_name, json.dumps(payload, ensure_ascii=False)),
    )
    conn.commit()
    conn.close()
    return backup_id


def load_server_backup(backup_id: str | None) -> dict[str, Any] | None:
    if not backup_id:
        return None
    conn = get_db_connection()
    backup = row_to_dict(
        conn.execute(
            """
            SELECT b.*, p.name AS profile_name
            FROM server_backups b
            LEFT JOIN profiles p ON p.id = b.profile_id
            WHERE b.id = ?
            """,
            (backup_id,),
        ).fetchone()
    )
    conn.close()
    if backup:
        try:
            backup["payload"] = json.loads(backup.get("payload_json") or "{}")
        except ValueError:
            backup["payload"] = {}
    return backup


def list_server_backups(limit: int = 30) -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT b.*, p.name AS profile_name
            FROM server_backups b
            LEFT JOIN profiles p ON p.id = b.profile_id
            ORDER BY b.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    )
    conn.close()
    for row in rows:
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except ValueError:
            payload = {}
        row["channel_count"] = len(payload.get("channels") or [])
        row["role_count"] = len(payload.get("roles") or [])
    return rows


def create_role_from_backup(token: str, guild_id: str, role: dict[str, Any]) -> tuple[str | None, str | None]:
    payload: dict[str, Any] = {
        "name": str(role.get("name") or "role"),
    }
    for key in ("permissions", "color", "hoist", "mentionable"):
        if role.get(key) is not None:
            payload[key] = role.get(key)
    created, _, error = discord_json_request(token, "POST", f"/guilds/{guild_id}/roles", json_payload=payload)
    if isinstance(created, dict) and created.get("id"):
        return str(created["id"]), None
    return None, error or "Creation role impossible."


def remap_permission_overwrites(
    overwrites: Any,
    source_guild_id: str | None,
    target_guild_id: str,
    role_id_map: dict[str, str],
) -> list[dict[str, Any]]:
    if not isinstance(overwrites, list):
        return []
    mapped: list[dict[str, Any]] = []
    for overwrite in overwrites:
        if not isinstance(overwrite, dict):
            continue
        item = dict(overwrite)
        overwrite_id = str(item.get("id") or "")
        overwrite_type = item.get("type")
        if source_guild_id and overwrite_id == str(source_guild_id):
            item["id"] = str(target_guild_id)
        elif str(overwrite_type) == "0" and overwrite_id in role_id_map:
            item["id"] = role_id_map[overwrite_id]
        mapped.append(item)
    return mapped


def create_channel_from_backup(
    token: str,
    guild_id: str,
    channel: dict[str, Any],
    parent_id: str | None = None,
    role_id_map: dict[str, str] | None = None,
    source_guild_id: str | None = None,
) -> tuple[str | None, str | None]:
    payload: dict[str, Any] = {
        "name": str(channel.get("name") or "salon"),
        "type": int(channel.get("type") or 0),
    }
    for key in ("topic", "nsfw", "rate_limit_per_user", "bitrate", "user_limit"):
        if channel.get(key) not in (None, ""):
            payload[key] = channel.get(key)
    permission_overwrites = remap_permission_overwrites(
        channel.get("permission_overwrites"),
        source_guild_id,
        guild_id,
        role_id_map or {},
    )
    if permission_overwrites:
        payload["permission_overwrites"] = permission_overwrites
    if parent_id:
        payload["parent_id"] = parent_id
    created, _, error = discord_json_request(token, "POST", f"/guilds/{guild_id}/channels", json_payload=payload)
    if isinstance(created, dict) and created.get("id"):
        return str(created["id"]), None
    return None, error or "Creation salon impossible."


def start_selfbot_tool_thread(run_id: str, target: Callable[..., None], *args: Any) -> None:
    thread = threading.Thread(target=target, args=(run_id, *args), daemon=True, name=f"selfbot-tool-{run_id}")
    thread.start()


def run_dmall_task(run_id: str, message: str, include_friends: bool, include_dms: bool, delay: float) -> None:
    with selfbot_tool_lock:
        selfbot_tool_active_runs.add(run_id)
    try:
        run = load_selfbot_tool_run(run_id)
        profile = load_profile_token(run.get("profile_id") if run else None)
        if not profile:
            raise RuntimeError("Profil introuvable.")
        recipients: dict[str, dict[str, Any]] = {}

        if include_friends:
            relationships, error = get_user_relationships(profile["token"])
            if error:
                append_selfbot_tool_result(run_id, "errors", {"stage": "friends", "error": error})
            for relation in relationships:
                if normalize_relationship_type(relation.get("relationship_type")) == 1:
                    recipients[f"user:{relation['user_id']}"] = {
                        "type": "friend",
                        "user_id": relation["user_id"],
                        "label": relation.get("display_name") or relation.get("username") or relation["user_id"],
                    }

        if include_dms:
            for channel in get_user_direct_messages(profile["token"]):
                recipients[f"channel:{channel['id']}"] = {
                    "type": "dm",
                    "channel_id": channel["id"],
                    "label": channel.get("name") or channel["id"],
                }

        items = list(recipients.values())
        update_selfbot_tool_run(
            run_id,
            status="running",
            total_count=len(items),
            current_detail=f"0/{len(items)} destinataire(s).",
        )

        success_count = 0
        fail_count = 0
        for index, item in enumerate(items, 1):
            channel_id = item.get("channel_id")
            error = None
            if not channel_id and item.get("user_id"):
                channel_id, error = create_dm_channel(profile["token"], str(item["user_id"]))
            if channel_id:
                ok, error = send_channel_message(profile["token"], str(channel_id), message)
            else:
                ok = False
            if ok:
                success_count += 1
            else:
                fail_count += 1
            append_selfbot_tool_result(
                run_id,
                "recipients",
                {
                    "label": item.get("label"),
                    "type": item.get("type"),
                    "status": "sent" if ok else "failed",
                    "error": error,
                },
            )
            update_selfbot_tool_run(
                run_id,
                processed_count=index,
                success_count=success_count,
                fail_count=fail_count,
                current_detail=f"{index}/{len(items)} destinataire(s).",
            )
            if index < len(items) and delay > 0:
                time.sleep(delay)

        update_selfbot_tool_run(
            run_id,
            status="completed",
            current_detail=f"{success_count}/{len(items)} message(s) envoye(s).",
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as error:
        update_selfbot_tool_run(
            run_id,
            status="failed",
            error=str(error)[:1000],
            current_detail=str(error)[:1000],
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    finally:
        with selfbot_tool_lock:
            selfbot_tool_active_runs.discard(run_id)


def run_delallfriends_task(run_id: str, delay: float) -> None:
    with selfbot_tool_lock:
        selfbot_tool_active_runs.add(run_id)
    try:
        run = load_selfbot_tool_run(run_id)
        profile = load_profile_token(run.get("profile_id") if run else None)
        if not profile:
            raise RuntimeError("Profil introuvable.")
        relationships, error = get_user_relationships(profile["token"])
        if error:
            raise RuntimeError(error)
        friends = [relation for relation in relationships if normalize_relationship_type(relation.get("relationship_type")) == 1]
        update_selfbot_tool_run(
            run_id,
            status="running",
            total_count=len(friends),
            result_json=json.dumps({"backup_friends": friends}, ensure_ascii=False),
            current_detail=f"0/{len(friends)} ami(s).",
        )
        success_count = 0
        fail_count = 0
        for index, friend in enumerate(friends, 1):
            ok, delete_error = delete_discord_relationship(profile["token"], friend["user_id"])
            if ok:
                success_count += 1
            else:
                fail_count += 1
            append_selfbot_tool_result(
                run_id,
                "deleted",
                {
                    "user_id": friend["user_id"],
                    "display_name": friend.get("display_name"),
                    "status": "deleted" if ok else "failed",
                    "error": delete_error,
                },
            )
            update_selfbot_tool_run(
                run_id,
                processed_count=index,
                success_count=success_count,
                fail_count=fail_count,
                current_detail=f"{index}/{len(friends)} ami(s).",
            )
            if index < len(friends) and delay > 0:
                time.sleep(delay)
        update_selfbot_tool_run(
            run_id,
            status="completed",
            current_detail=f"{success_count}/{len(friends)} ami(s) supprime(s). Backup dans result_json.",
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as error:
        update_selfbot_tool_run(
            run_id,
            status="failed",
            error=str(error)[:1000],
            current_detail=str(error)[:1000],
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    finally:
        with selfbot_tool_lock:
            selfbot_tool_active_runs.discard(run_id)


def run_bcreate_task(run_id: str, guild_id: str, backup_name: str | None) -> None:
    with selfbot_tool_lock:
        selfbot_tool_active_runs.add(run_id)
    try:
        run = load_selfbot_tool_run(run_id)
        profile = load_profile_token(run.get("profile_id") if run else None)
        if not profile:
            raise RuntimeError("Profil introuvable.")
        update_selfbot_tool_run(run_id, status="running", current_detail="Lecture du serveur.")
        payload, error = build_server_backup_payload(profile["token"], guild_id)
        if not payload:
            raise RuntimeError(error or "Backup impossible.")
        backup_id = save_server_backup(profile["id"], guild_id, payload, backup_name)
        result = {
            "backup_id": backup_id,
            "guild_id": guild_id,
            "guild_name": (payload.get("guild") or {}).get("name"),
            "channels": len(payload.get("channels") or []),
            "roles": len(payload.get("roles") or []),
        }
        update_selfbot_tool_run(
            run_id,
            status="completed",
            total_count=result["channels"],
            processed_count=result["channels"],
            success_count=1,
            result_json=json.dumps(result, ensure_ascii=False),
            current_detail=f"Backup {backup_id} cree.",
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as error:
        update_selfbot_tool_run(
            run_id,
            status="failed",
            error=str(error)[:1000],
            current_detail=str(error)[:1000],
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    finally:
        with selfbot_tool_lock:
            selfbot_tool_active_runs.discard(run_id)


def run_bload_task(run_id: str, backup_id: str, target_guild_id: str, clear_channels: bool, restore_guild_name: bool) -> None:
    with selfbot_tool_lock:
        selfbot_tool_active_runs.add(run_id)
    try:
        run = load_selfbot_tool_run(run_id)
        profile = load_profile_token(run.get("profile_id") if run else None)
        backup = load_server_backup(backup_id)
        if not profile:
            raise RuntimeError("Profil introuvable.")
        if not backup:
            raise RuntimeError("Backup introuvable.")
        payload = backup.get("payload") or {}
        channels = [item for item in payload.get("channels") or [] if isinstance(item, dict)]
        roles = [
            item
            for item in payload.get("roles") or []
            if isinstance(item, dict) and item.get("name") and item.get("name") != "@everyone"
        ]
        total_items = len(roles) + len(channels)
        update_selfbot_tool_run(
            run_id,
            status="running",
            total_count=total_items,
            current_detail="Preparation du serveur cible.",
        )
        source_guild_id = str((payload.get("guild") or {}).get("id") or backup.get("guild_id") or "")
        if restore_guild_name and (payload.get("guild") or {}).get("name"):
            discord_json_request(
                profile["token"],
                "PATCH",
                f"/guilds/{target_guild_id}",
                json_payload={"name": (payload.get("guild") or {}).get("name")},
            )

        if clear_channels:
            existing_channels = get_guild_channels_raw(profile["token"], target_guild_id)
            for channel in sorted(existing_channels, key=lambda item: 1 if item.get("type") == 4 else 0):
                if channel.get("id"):
                    discord_json_request(profile["token"], "DELETE", f"/channels/{channel['id']}")
                    time.sleep(0.4)

        role_id_map: dict[str, str] = {}
        category_map: dict[str, str] = {}
        success_count = 0
        fail_count = 0
        processed = 0
        for role in sorted(roles, key=lambda item: item.get("position") or 0):
            created_id, error = create_role_from_backup(profile["token"], target_guild_id, role)
            processed += 1
            if created_id:
                success_count += 1
                if role.get("id"):
                    role_id_map[str(role["id"])] = created_id
            else:
                fail_count += 1
            append_selfbot_tool_result(
                run_id,
                "roles",
                {
                    "name": role.get("name"),
                    "created_id": created_id,
                    "status": "created" if created_id else "failed",
                    "error": error,
                },
            )
            update_selfbot_tool_run(
                run_id,
                processed_count=processed,
                success_count=success_count,
                fail_count=fail_count,
                current_detail=f"{processed}/{total_items} element(s) · roles.",
            )
            time.sleep(0.4)

        categories = sorted([item for item in channels if item.get("type") == 4], key=lambda item: item.get("position") or 0)
        others = sorted([item for item in channels if item.get("type") != 4], key=lambda item: item.get("position") or 0)
        for channel in categories + others:
            parent_id = category_map.get(str(channel.get("parent_id"))) if channel.get("parent_id") else None
            created_id, error = create_channel_from_backup(
                profile["token"],
                target_guild_id,
                channel,
                parent_id,
                role_id_map,
                source_guild_id,
            )
            processed += 1
            if created_id:
                success_count += 1
                if channel.get("type") == 4 and channel.get("id"):
                    category_map[str(channel["id"])] = created_id
            else:
                fail_count += 1
            append_selfbot_tool_result(
                run_id,
                "channels",
                {
                    "name": channel.get("name"),
                    "type": channel.get("type"),
                    "created_id": created_id,
                    "status": "created" if created_id else "failed",
                    "error": error,
                },
            )
            update_selfbot_tool_run(
                run_id,
                processed_count=processed,
                success_count=success_count,
                fail_count=fail_count,
                current_detail=f"{processed}/{total_items} element(s) · salons.",
            )
            time.sleep(0.5)
        update_selfbot_tool_run(
            run_id,
            status="completed",
            current_detail=f"{success_count}/{total_items} element(s) deploye(s).",
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as error:
        update_selfbot_tool_run(
            run_id,
            status="failed",
            error=str(error)[:1000],
            current_detail=str(error)[:1000],
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    finally:
        with selfbot_tool_lock:
            selfbot_tool_active_runs.discard(run_id)


def process_selfbot_spy_once() -> None:
    rules = [rule for rule in list_selfbot_spy_rules() if rule.get("enabled")]
    for rule in rules:
        profile = load_profile_token(rule.get("profile_id"))
        if not profile:
            continue
        try:
            keywords = rule.get("keyword_list") or []
            if not keywords:
                continue
            channels = []
            if rule.get("channel_id"):
                channels = [{"id": str(rule["channel_id"]), "name": rule.get("channel_name") or str(rule["channel_id"])}]
            else:
                channels = get_guild_channels(profile["token"], str(rule["guild_id"]))[:SELFBOT_SPY_MAX_CHANNELS]
            max_seen = rule.get("last_message_id")
            first_scan = not max_seen
            matches = 0
            for channel in channels:
                messages = list(reversed(get_recent_channel_messages(profile["token"], str(channel["id"]), SELFBOT_SPY_MESSAGE_LOOKBACK)))
                for message in messages:
                    message_id = str(message.get("id") or "")
                    if not message_id:
                        continue
                    if max_seen is None or message_id_is_newer(message_id, max_seen):
                        max_seen = message_id
                    if first_scan or not message_id_is_newer(message_id, rule.get("last_message_id")):
                        continue
                    text = message_text_blob(message)
                    keyword = keyword_match(text, keywords)
                    if not keyword:
                        continue
                    author = message.get("author") or {}
                    jump_url = discord_jump_url(str(rule["guild_id"]), str(channel["id"]), message_id)
                    conn = get_db_connection()
                    conn.execute(
                        """
                        INSERT INTO selfbot_spy_events (
                            rule_id, profile_id, guild_id, channel_id, message_id,
                            author_id, author_name, keyword, content, jump_url
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            rule["id"],
                            profile["id"],
                            str(rule["guild_id"]),
                            str(channel["id"]),
                            message_id,
                            author.get("id"),
                            author.get("global_name") or author.get("username"),
                            keyword,
                            truncate_log_content(text, 800),
                            jump_url,
                        ),
                    )
                    conn.execute(
                        "UPDATE selfbot_spy_rules SET match_count = match_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (rule["id"],),
                    )
                    conn.commit()
                    conn.close()
                    matches += 1
                    if rule.get("notify_channel_id"):
                        send_channel_message(
                            profile["token"],
                            str(rule["notify_channel_id"]),
                            f"[Spy] `{keyword}` dans <#{channel['id']}> par {author.get('global_name') or author.get('username') or author.get('id')}.\n{jump_url or ''}\n{text[:1200]}",
                        )
            conn = get_db_connection()
            conn.execute(
                """
                UPDATE selfbot_spy_rules
                SET last_message_id = COALESCE(?, last_message_id),
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (max_seen, rule["id"]),
            )
            conn.commit()
            conn.close()
        except Exception as error:
            conn = get_db_connection()
            conn.execute(
                "UPDATE selfbot_spy_rules SET last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (str(error)[:1000], rule["id"]),
            )
            conn.commit()
            conn.close()


def process_selfbot_antigroup_once() -> None:
    settings = [item for item in list_selfbot_antigroup_settings() if item.get("enabled")]
    for setting in settings:
        profile = load_profile_token(setting.get("profile_id"))
        if not profile:
            continue
        left_count = 0
        fail_count = 0
        last_error = None
        for channel in get_user_direct_channels_raw(profile["token"]):
            if channel.get("type") != 3:
                continue
            if str(channel.get("owner_id") or "") == str(profile.get("user_id") or ""):
                continue
            ok, error = leave_discord_channel(profile["token"], str(channel["id"]))
            if ok:
                left_count += 1
            else:
                fail_count += 1
                last_error = error
            time.sleep(0.5)
        conn = get_db_connection()
        conn.execute(
            """
            INSERT INTO selfbot_antigroup_settings (
                profile_id, enabled, last_scan_at, left_count, fail_count, last_error, updated_at
            )
            VALUES (?, 1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(profile_id) DO UPDATE SET
                last_scan_at = excluded.last_scan_at,
                left_count = left_count + excluded.left_count,
                fail_count = fail_count + excluded.fail_count,
                last_error = excluded.last_error,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                profile["id"],
                datetime.now().astimezone().isoformat(timespec="seconds"),
                left_count,
                fail_count,
                last_error,
            ),
        )
        conn.commit()
        conn.close()


def selfbot_tools_worker_loop() -> None:
    last_spy_at = 0.0
    last_antigroup_at = 0.0
    while not selfbot_tools_stop_event.is_set():
        now = current_timestamp()
        try:
            if now - last_spy_at >= SELFBOT_TOOLS_SPY_INTERVAL:
                process_selfbot_spy_once()
                last_spy_at = now
            if now - last_antigroup_at >= SELFBOT_TOOLS_ANTIGROUP_INTERVAL:
                process_selfbot_antigroup_once()
                last_antigroup_at = now
        except Exception as error:
            print(f"Erreur selfbot tools: {error}")
        selfbot_tools_stop_event.wait(1.0)


def should_start_selfbot_tools_worker() -> bool:
    if not SELFBOT_TOOLS_WORKER_ENABLED:
        return False
    if os.getenv("FLASK_DEBUG", "1") != "0" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return False
    return True


def start_selfbot_tools_worker() -> None:
    global selfbot_tools_thread
    if not should_start_selfbot_tools_worker():
        return
    if selfbot_tools_thread and selfbot_tools_thread.is_alive():
        return
    selfbot_tools_thread = threading.Thread(target=selfbot_tools_worker_loop, daemon=True, name="selfbot-tools-worker")
    selfbot_tools_thread.start()


def snowflake_created_at(value: Any) -> float:
    try:
        return ((int(value) >> 22) + DISCORD_SNOWFLAKE_EPOCH_MS) / 1000
    except (TypeError, ValueError):
        return 0.0


def get_recent_channel_messages(token: str, channel_id: str, limit: int = 10) -> list[dict[str, Any]]:
    response = discord_request(
        token,
        "GET",
        f"/channels/{channel_id}/messages",
        params={"limit": max(1, min(limit, 50))},
    )
    if not response or response.status_code != 200:
        return []

    try:
        messages = response.json()
    except ValueError:
        return []

    return messages if isinstance(messages, list) else []


def message_text_blob(message: dict[str, Any]) -> str:
    parts = [str(message.get("content") or "")]
    for embed in message.get("embeds") or []:
        if not isinstance(embed, dict):
            continue
        for key in ("title", "description"):
            if embed.get(key):
                parts.append(str(embed[key]))
        footer = embed.get("footer") or {}
        if isinstance(footer, dict) and footer.get("text"):
            parts.append(str(footer["text"]))
        author = embed.get("author") or {}
        if isinstance(author, dict) and author.get("name"):
            parts.append(str(author["name"]))
        for field in embed.get("fields") or []:
            if not isinstance(field, dict):
                continue
            if field.get("name"):
                parts.append(str(field["name"]))
            if field.get("value"):
                parts.append(str(field["value"]))
    return "\n".join(part for part in parts if part).strip()


def message_is_disboard_response(message: dict[str, Any], text: str) -> bool:
    author = message.get("author") or {}
    if str(author.get("id") or "") == AUTOBUMPER_DISBOARD_APP_ID:
        return True
    if str(message.get("application_id") or "") == AUTOBUMPER_DISBOARD_APP_ID:
        return True
    return "disboard" in text.lower()


def parse_cooldown_seconds(text: str) -> int | None:
    cleaned = re.sub(r"[*_`~]", " ", text or "")
    lower = cleaned.lower()
    cooldown_terms = (
        "wait",
        "cooldown",
        "again",
        "another",
        "bumped recently",
        "can be bumped",
        "remaining",
        "reste",
        "attend",
        "attendre",
        "prochain",
        "disponible",
    )
    if not any(term in lower for term in cooldown_terms):
        return None

    timestamp_match = re.search(r"<t:(\d{10,}):?[a-zA-Z]?>", cleaned)
    if timestamp_match:
        seconds = int(timestamp_match.group(1)) - int(current_timestamp())
        return max(0, seconds)

    total = 0
    unit_patterns = (
        (3600, r"(\d+)\s*(?:h|hr|hrs|hour|hours|heure|heures)\b"),
        (60, r"(\d+)\s*(?:m|min|mins|minute|minutes)\b"),
        (1, r"(\d+)\s*(?:s|sec|secs|second|seconds|seconde|secondes)\b"),
    )
    for multiplier, pattern in unit_patterns:
        for match in re.finditer(pattern, lower):
            total += int(match.group(1)) * multiplier

    if total:
        return total

    colon_match = re.search(r"\b(\d{1,2}):(\d{2})(?::(\d{2}))?\b", lower)
    if colon_match:
        first = int(colon_match.group(1))
        second = int(colon_match.group(2))
        third = int(colon_match.group(3) or 0)
        if colon_match.group(3):
            return first * 3600 + second * 60 + third
        return first * 60 + second

    return None


def detect_disboard_bump_state(
    messages: list[dict[str, Any]],
    started_at: float | None = None,
) -> dict[str, Any] | None:
    for message in sorted(messages, key=lambda item: snowflake_created_at(item.get("id")), reverse=True):
        created_at = snowflake_created_at(message.get("id")) or parse_discord_timestamp(message.get("timestamp"))
        if started_at is not None and created_at and created_at < started_at - 8:
            continue

        text = message_text_blob(message)
        if not text or not message_is_disboard_response(message, text):
            continue

        cooldown_seconds = parse_cooldown_seconds(text)
        if cooldown_seconds is not None:
            next_bump_at = current_timestamp() + cooldown_seconds + AUTOBUMPER_COOLDOWN_MARGIN
            return {
                "outcome": "cooldown",
                "message": f"Cooldown Disboard detecte: prochain bump dans {format_time_until(next_bump_at)}.",
                "next_bump_at": next_bump_at,
                "last_bump_at": None,
            }

        lower = text.lower()
        success_terms = (
            "bumped",
            "server bumped",
            "has been bumped",
            "bump done",
            "bump successful",
            "bump effectué",
            "bump effectue",
            "bump réussi",
            "bump reussi",
            "bump valide",
            "bump fait",
            "serveur bump",
        )
        if any(term in lower for term in success_terms):
            last_bump_at = created_at or current_timestamp()
            return {
                "outcome": "success",
                "message": "Bump confirme par Disboard.",
                "next_bump_at": last_bump_at + AUTOBUMPER_BUMP_INTERVAL,
                "last_bump_at": last_bump_at,
            }

    return None


def detect_disboard_bump_response(
    messages: list[dict[str, Any]],
    started_at: float,
) -> dict[str, Any] | None:
    return detect_disboard_bump_state(messages, started_at)


def wait_for_disboard_bump_response(token: str, channel_id: str, started_at: float) -> dict[str, Any] | None:
    deadline = current_timestamp() + AUTOBUMPER_RESPONSE_POLL_SECONDS
    while True:
        messages = get_recent_channel_messages(token, channel_id, 25)
        result = detect_disboard_bump_response(messages, started_at)
        if result:
            return result

        if current_timestamp() >= deadline:
            return None
        time.sleep(AUTOBUMPER_RESPONSE_POLL_INTERVAL)


def inspect_recent_disboard_state(token: str, target: dict[str, Any]) -> dict[str, Any] | None:
    messages = get_recent_channel_messages(token, target["channel_id"], 25)
    result = detect_disboard_bump_state(messages)
    result = apply_autobumper_delay_to_result(result, target)
    if not result:
        return None

    next_bump_at = result.get("next_bump_at")
    if not next_bump_at or next_bump_at <= current_timestamp():
        return None

    return result


def execute_disboard_bump(token: str, target: dict[str, Any]) -> dict[str, Any]:
    started_at = current_timestamp()
    invoked, message = invoke_disboard_bump(token, target["guild_id"], target["channel_id"])
    if not invoked:
        return {
            "outcome": "failed",
            "message": message,
            "next_bump_at": None,
            "last_bump_at": None,
        }

    response_result = wait_for_disboard_bump_response(token, target["channel_id"], started_at)
    if response_result:
        return apply_autobumper_delay_to_result(response_result, target)

    fallback_next_bump_at = started_at + AUTOBUMPER_BUMP_INTERVAL
    try:
        last_bump_at = float(target.get("last_bump_at") or 0)
    except (TypeError, ValueError):
        last_bump_at = 0
    if last_bump_at and last_bump_at + AUTOBUMPER_BUMP_INTERVAL > started_at:
        fallback_next_bump_at = last_bump_at + AUTOBUMPER_BUMP_INTERVAL

    return {
        "outcome": "unknown",
        "message": f"{message} Reponse Disboard non detectee; prochaine tentative cale sur le meilleur cooldown connu.",
        "next_bump_at": fallback_next_bump_at,
        "last_bump_at": None,
    }


def normalize_autobumper_target(target: dict[str, Any]) -> dict[str, Any]:
    if not target:
        return target
    target["delay_seconds"] = autobumper_target_delay_seconds(target)
    target["enabled"] = bool(target.get("enabled"))
    target["success_count"] = int(target.get("success_count") or 0)
    target["fail_count"] = int(target.get("fail_count") or 0)
    target["total_attempts"] = int(target.get("total_attempts") or 0)
    return target


def list_autobumper_targets(profile_id: int) -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT *
            FROM autobumper_targets
            WHERE profile_id = ?
            ORDER BY enabled DESC, COALESCE(next_bump_at, 9999999999), guild_name COLLATE NOCASE
            """,
            (profile_id,),
        ).fetchall()
    )
    conn.close()
    return [normalize_autobumper_target(row) for row in rows]


def list_autobumper_events(profile_id: int, limit: int = 20) -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT e.*, t.guild_name, t.channel_name
            FROM autobumper_events e
            LEFT JOIN autobumper_targets t ON t.id = e.target_id
            WHERE e.profile_id = ?
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT ?
            """,
            (profile_id, limit),
        ).fetchall()
    )
    conn.close()
    return rows


def autobumper_stats(profile_id: int) -> dict[str, Any]:
    conn = get_db_connection()
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_targets,
            COALESCE(SUM(enabled), 0) AS enabled_targets,
            COALESCE(SUM(success_count), 0) AS success_count,
            COALESCE(SUM(fail_count), 0) AS fail_count,
            COALESCE(SUM(total_attempts), 0) AS total_attempts,
            MIN(CASE WHEN enabled = 1 THEN next_bump_at END) AS next_bump_at
        FROM autobumper_targets
        WHERE profile_id = ?
        """,
        (profile_id,),
    ).fetchone()
    conn.close()
    stats = row_to_dict(row) or {}
    for key in ("total_targets", "enabled_targets", "success_count", "fail_count", "total_attempts"):
        stats[key] = int(stats.get(key) or 0)
    return stats


def load_autobumper_target(profile_id: int, target_id: int) -> dict[str, Any] | None:
    conn = get_db_connection()
    target = row_to_dict(
        conn.execute(
            "SELECT * FROM autobumper_targets WHERE id = ? AND profile_id = ?",
            (target_id, profile_id),
        ).fetchone()
    )
    conn.close()
    return normalize_autobumper_target(target) if target else None


def record_autobumper_event(
    conn: sqlite3.Connection,
    target: dict[str, Any],
    status: str,
    message: str,
) -> None:
    conn.execute(
        """
        INSERT INTO autobumper_events (target_id, profile_id, guild_id, channel_id, status, message)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            target["id"],
            target["profile_id"],
            target["guild_id"],
            target["channel_id"],
            status,
            message[:1000],
        ),
    )


def save_autobumper_target(
    profile_id: int,
    guild: dict[str, Any],
    channel: dict[str, Any],
    custom_message: str,
    enabled: bool,
    delay_seconds: int,
) -> None:
    now = current_timestamp()
    delay_seconds = clamp_autobumper_delay_seconds(delay_seconds)
    conn = get_db_connection()
    existing = conn.execute(
        "SELECT * FROM autobumper_targets WHERE profile_id = ? AND guild_id = ?",
        (profile_id, guild["id"]),
    ).fetchone()

    if existing:
        existing_target = normalize_autobumper_target(dict(existing))
        existing_target["delay_seconds"] = delay_seconds
        next_bump_at = next_autobumper_schedule_at(existing_target, now) if enabled else existing["next_bump_at"] or now
        last_status = "queued" if enabled else "paused"
        conn.execute(
            """
            UPDATE autobumper_targets
            SET
                guild_name = ?,
                channel_id = ?,
                channel_name = ?,
                enabled = ?,
                next_bump_at = ?,
                delay_seconds = ?,
                custom_message = ?,
                last_status = ?,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                guild["name"],
                channel["id"],
                channel["name"],
                1 if enabled else 0,
                next_bump_at,
                delay_seconds,
                custom_message or None,
                last_status,
                existing["id"],
            ),
        )
    else:
        next_bump_at = next_autobumper_schedule_at({"delay_seconds": delay_seconds}, now)
        conn.execute(
            """
            INSERT INTO autobumper_targets (
                profile_id, guild_id, guild_name, channel_id, channel_name, enabled,
                next_bump_at, delay_seconds, custom_message, last_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile_id,
                guild["id"],
                guild["name"],
                channel["id"],
                channel["name"],
                1 if enabled else 0,
                next_bump_at,
                delay_seconds,
                custom_message or None,
                "queued" if enabled else "paused",
            ),
        )

    conn.commit()
    conn.close()


def message_id_is_newer(message_id: str | None, last_message_id: str | None) -> bool:
    if not message_id:
        return False
    if not last_message_id:
        return True
    try:
        return int(message_id) > int(last_message_id)
    except (TypeError, ValueError):
        return message_id != last_message_id


def load_profile_token(profile_id: int | None) -> dict[str, Any] | None:
    if not profile_id:
        return None
    conn = get_db_connection()
    profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone())
    conn.close()
    return profile


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


def run_autobumper_target(target_id: int, forced: bool = False) -> None:
    with autobumper_lock:
        if target_id in autobumper_active_targets:
            return
        autobumper_active_targets.add(target_id)

    try:
        conn = get_db_connection()
        row = conn.execute(
            """
            SELECT t.*, p.token
            FROM autobumper_targets t
            JOIN profiles p ON p.id = t.profile_id
            WHERE t.id = ?
            """,
            (target_id,),
        ).fetchone()

        if not row:
            conn.close()
            return

        target = normalize_autobumper_target(dict(row))
        now = current_timestamp()
        if not forced:
            if not target["enabled"]:
                conn.close()
                return
            if target.get("next_bump_at") and float(target["next_bump_at"]) > now:
                conn.close()
                return

        conn.close()
        recent_state = inspect_recent_disboard_state(target["token"], target)
        if recent_state:
            conn = get_db_connection()
            conn.execute(
                """
                UPDATE autobumper_targets
                SET
                    next_bump_at = ?,
                    last_bump_at = COALESCE(?, last_bump_at),
                    fail_count = 0,
                    last_status = ?,
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    recent_state["next_bump_at"],
                    recent_state.get("last_bump_at"),
                    recent_state["outcome"],
                    target_id,
                ),
            )
            record_autobumper_event(
                conn,
                target,
                recent_state["outcome"],
                f"Sync auto: {recent_state['message']}",
            )
            conn.commit()
            conn.close()
            return

        conn = get_db_connection()
        conn.execute(
            """
            UPDATE autobumper_targets
            SET last_status = 'running', last_error = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (target_id,),
        )
        conn.commit()
        conn.close()

        bump_result = execute_disboard_bump(target["token"], target)

        conn = get_db_connection()
        latest = row_to_dict(conn.execute("SELECT * FROM autobumper_targets WHERE id = ?", (target_id,)).fetchone())
        if not latest:
            conn.close()
            return
        latest = normalize_autobumper_target(latest)
        now = current_timestamp()
        outcome = bump_result["outcome"]
        message = bump_result["message"]

        if outcome == "success":
            next_bump_at = bump_result.get("next_bump_at") or now + AUTOBUMPER_BUMP_INTERVAL
            last_bump_at = bump_result.get("last_bump_at") or now
            conn.execute(
                """
                UPDATE autobumper_targets
                SET
                    next_bump_at = ?,
                    last_bump_at = ?,
                    success_count = success_count + 1,
                    fail_count = 0,
                    total_attempts = total_attempts + 1,
                    last_status = 'success',
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (next_bump_at, last_bump_at, target_id),
            )
            record_autobumper_event(conn, latest, "success", message)

            if latest.get("custom_message"):
                message_ok, message_result = send_channel_message(target["token"], latest["channel_id"], latest["custom_message"])
                if not message_ok:
                    record_autobumper_event(
                        conn,
                        latest,
                        "failed",
                        f"Bump envoye, message personnalise non envoye: {message_result}",
                    )
        elif outcome in {"cooldown", "unknown"}:
            next_bump_at = bump_result.get("next_bump_at") or now + AUTOBUMPER_BUMP_INTERVAL
            conn.execute(
                """
                UPDATE autobumper_targets
                SET
                    next_bump_at = ?,
                    fail_count = 0,
                    total_attempts = total_attempts + 1,
                    last_status = ?,
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (next_bump_at, outcome, target_id),
            )
            record_autobumper_event(conn, latest, outcome, message)
        else:
            fail_count = latest["fail_count"] + 1
            retry_delay = (
                AUTOBUMPER_RETRY_DELAY
                if fail_count <= AUTOBUMPER_MAX_RETRIES
                else AUTOBUMPER_BUMP_INTERVAL
            )
            conn.execute(
                """
                UPDATE autobumper_targets
                SET
                    next_bump_at = ?,
                    fail_count = ?,
                    total_attempts = total_attempts + 1,
                    last_status = 'failed',
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (now + retry_delay, fail_count, message[:1000], target_id),
            )
            record_autobumper_event(conn, latest, "failed", message)

        conn.commit()
        conn.close()
    except Exception as error:
        conn = get_db_connection()
        target = row_to_dict(conn.execute("SELECT * FROM autobumper_targets WHERE id = ?", (target_id,)).fetchone())
        if target:
            now = current_timestamp()
            target = normalize_autobumper_target(target)
            conn.execute(
                """
                UPDATE autobumper_targets
                SET
                    next_bump_at = ?,
                    fail_count = fail_count + 1,
                    total_attempts = total_attempts + 1,
                    last_status = 'failed',
                    last_error = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (now + AUTOBUMPER_RETRY_DELAY, str(error)[:1000], target_id),
            )
            record_autobumper_event(conn, target, "failed", str(error))
            conn.commit()
        conn.close()
    finally:
        with autobumper_lock:
            autobumper_active_targets.discard(target_id)


def process_due_autobumps() -> None:
    now = current_timestamp()
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT id
            FROM autobumper_targets
            WHERE enabled = 1
              AND (next_bump_at IS NULL OR CAST(next_bump_at AS REAL) <= ?)
            ORDER BY COALESCE(CAST(next_bump_at AS REAL), 0), updated_at
            LIMIT ?
            """,
            (now, AUTOBUMPER_MAX_DUE_PER_CYCLE),
        ).fetchall()
    )
    conn.close()

    for row in rows:
        run_autobumper_target(int(row["id"]))


def autobumper_worker_loop() -> None:
    while not autobumper_stop_event.is_set():
        try:
            process_due_autobumps()
        except Exception as error:
            print(f"Erreur AutoBumper: {error}")
        autobumper_stop_event.wait(AUTOBUMPER_CHECK_INTERVAL)


def should_start_autobumper_worker() -> bool:
    if not AUTOBUMPER_ENABLED:
        return False
    if os.getenv("FLASK_DEBUG", "1") != "0" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return False
    return True


def start_autobumper_worker() -> None:
    global autobumper_thread
    if not should_start_autobumper_worker():
        return
    if autobumper_thread and autobumper_thread.is_alive():
        return

    autobumper_thread = threading.Thread(target=autobumper_worker_loop, daemon=True, name="autobumper-worker")
    autobumper_thread.start()


@app.context_processor
def template_helpers() -> dict[str, Any]:
    return {
        "discord_display_name": discord_display_name,
        "target_user_label": target_user_label,
        "channel_type_label": channel_type_label,
        "status_label": status_label,
        "message_type_label": message_type_label,
        "relationship_type_label": relationship_type_label,
        "status_badge_class": status_badge_class,
        "export_status_label": export_status_label,
        "export_status_badge_class": export_status_badge_class,
        "export_file_payload": export_file_payload,
        "format_discord_timestamp": format_discord_timestamp,
        "format_epoch_timestamp": format_epoch_timestamp,
        "format_local_timestamp": format_local_timestamp,
        "format_time_until": format_time_until,
        "format_bytes": format_bytes,
        "autobumper_delay_label": autobumper_delay_label,
        "autobumper_status_label": autobumper_status_label,
        "autobumper_status_badge_class": autobumper_status_badge_class,
        "mutual_guild_status_label": mutual_guild_status_label,
        "mutual_guild_status_badge_class": mutual_guild_status_badge_class,
        "mutual_guild_progress": mutual_guild_progress,
        "message_scraper_status_label": message_scraper_status_label,
        "message_scraper_status_badge_class": message_scraper_status_badge_class,
        "message_scraper_progress": message_scraper_progress,
        "account_backup_status_label": account_backup_status_label,
        "account_backup_status_badge_class": account_backup_status_badge_class,
        "account_backup_progress": account_backup_progress,
        "invite_status_label": invite_status_label,
        "invite_status_badge_class": invite_status_badge_class,
        "account_backup_schedule_mode_label": account_backup_schedule_mode_label,
        "account_backup_schedule_status_label": account_backup_schedule_status_label,
        "account_backup_schedule_status_badge_class": account_backup_schedule_status_badge_class,
        "selfbot_tool_label": selfbot_tool_label,
        "selfbot_tool_status_label": selfbot_tool_status_label,
        "selfbot_tool_status_badge_class": selfbot_tool_status_badge_class,
        "selfbot_tool_progress": selfbot_tool_progress,
    }


@app.route("/")
def index():
    conn = get_db_connection()
    profiles = rows_to_dicts(conn.execute("SELECT * FROM profiles ORDER BY created_at DESC").fetchall())
    conn.close()
    return render_template("index.html", profiles=profiles)


@app.route("/main/servers")
def server_profiles():
    return render_template(
        "profile_select.html",
        profiles=load_profiles(include_tokens=False),
        mode="servers",
        title="Serveurs",
        subtitle="Choisis le compte a ouvrir pour analyser les serveurs.",
    )


@app.route("/main/dms")
def dm_profiles():
    return render_template(
        "profile_select.html",
        profiles=load_profiles(include_tokens=False),
        mode="dms",
        title="DM",
        subtitle="Choisis le compte a ouvrir pour analyser les messages prives.",
    )


@app.route("/tools/auto-bump")
def autobumper_profiles():
    return render_template(
        "profile_select.html",
        profiles=load_profiles(include_tokens=False),
        mode="autobumper",
        title="Auto Bump",
        subtitle="Choisis le compte qui doit gerer ses serveurs et salons de bump.",
    )


@app.route("/tools/account-backup")
def account_backup_profiles():
    return render_template(
        "profile_select.html",
        profiles=load_profiles(include_tokens=False),
        mode="account_backup",
        title="Backup compte",
        subtitle="Choisis le compte a sauvegarder: amis, serveurs et invitations possibles.",
    )


@app.route("/tools/account-backup/settings", methods=["GET", "POST"])
def account_backup_settings():
    profiles = load_profiles(include_tokens=False)
    profile_ids = {int(profile["id"]) for profile in profiles}

    if request.method == "POST":
        action = request.form.get("action")
        if action == "save_schedule":
            enabled = request.form.get("enabled") == "on"
            mode = request.form.get("mode") if request.form.get("mode") in {"daily", "interval"} else "daily"
            interval_days = parse_int(request.form.get("interval_days"), default=1, minimum=1, maximum=365)
            daily_time = normalize_backup_daily_time(request.form.get("daily_time"))
            create_invites = request.form.get("create_invites") == "on"
            selected_profile_ids = []
            for raw_id in request.form.getlist("profile_ids"):
                profile_id = parse_int(raw_id, default=0, minimum=0, maximum=999999999)
                if profile_id in profile_ids:
                    selected_profile_ids.append(profile_id)
            selected_profile_ids = sorted(set(selected_profile_ids))

            if enabled and not selected_profile_ids:
                flash("Selectionne au moins un compte avant d'activer la routine backup.", "error")
                return redirect(url_for("account_backup_settings"))

            conn = get_db_connection()
            conn.execute(
                """
                UPDATE account_backup_schedule_settings
                SET enabled = ?,
                    mode = ?,
                    interval_days = ?,
                    daily_time = ?,
                    create_invites = ?,
                    last_error = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                """,
                (1 if enabled else 0, mode, interval_days, daily_time, 1 if create_invites else 0),
            )
            conn.execute("UPDATE account_backup_schedule_profiles SET enabled = 0, updated_at = CURRENT_TIMESTAMP")
            for profile_id in selected_profile_ids:
                conn.execute(
                    """
                    INSERT INTO account_backup_schedule_profiles (profile_id, enabled, last_status, updated_at)
                    VALUES (?, 1, COALESCE((SELECT last_status FROM account_backup_schedule_profiles WHERE profile_id = ?), 'waiting'), CURRENT_TIMESTAMP)
                    ON CONFLICT(profile_id) DO UPDATE SET
                        enabled = 1,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (profile_id, profile_id),
                )
            record_account_backup_schedule_event(
                conn,
                None,
                None,
                "waiting" if enabled else "skipped",
                f"Routine backup {'activee' if enabled else 'desactivee'} pour {len(selected_profile_ids)} compte(s).",
            )
            conn.commit()
            conn.close()
            flash("Parametres de routine backup enregistres.", "success")
            return redirect(url_for("account_backup_settings"))

        if action == "run_missing_now":
            launched = process_account_backup_schedule_once(force_missing=True)
            if launched:
                flash(f"{len(launched)} backup(s) manquant(s) lance(s).", "success")
            else:
                flash("Aucun compte selectionne n'a besoin d'un nouveau backup sur la periode courante.", "info")
            return redirect(url_for("account_backup_settings"))

    settings = load_account_backup_schedule_settings()
    scheduled_profiles = list_account_backup_schedule_profiles(settings)
    return render_template(
        "account_backup_settings.html",
        settings=settings,
        profiles=scheduled_profiles,
        events=list_account_backup_schedule_events(),
        scheduler_enabled=ACCOUNT_BACKUP_SCHEDULER_ENABLED,
        scheduler_interval=int(ACCOUNT_BACKUP_SCHEDULER_CHECK_INTERVAL),
    )


@app.route("/tools/account-backup/<int:profile_id>", methods=["GET", "POST"])
def account_backup(profile_id: int):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("account_backup_profiles"))

    if request.method == "POST":
        create_invites = request.form.get("create_invites") == "on"
        run_id = create_account_backup_run(profile_id, create_invites)
        flash("Backup compte lance en arriere-plan.", "success")
        return redirect(url_for("account_backup", profile_id=profile_id, run_id=run_id))

    run_id = (request.args.get("run_id") or "").strip()
    run = load_account_backup_run(run_id)
    if run and int(run.get("profile_id") or 0) != profile_id:
        run = None
    friends = list_account_backup_friends(run["id"]) if run else []
    guilds = list_account_backup_guilds(run["id"]) if run else []
    return render_template(
        "account_backup.html",
        profile=profile,
        run=run,
        friends=friends,
        guilds=guilds,
        runs=list_account_backup_runs(profile_id),
    )


@app.route("/tools/mutual-guild", methods=["GET", "POST"])
def mutual_guild_tool():
    if request.method == "POST":
        target_user_id = (request.form.get("target_user_id") or "").strip()
        if not re.fullmatch(r"\d{15,25}", target_user_id):
            flash("ID utilisateur invalide.", "error")
            return redirect(url_for("mutual_guild_tool"))

        profiles = load_profiles(include_tokens=False)
        if not profiles:
            flash("Ajoute au moins un compte avant de lancer Mutual Guild.", "error")
            return redirect(url_for("mutual_guild_tool"))

        run_id = create_mutual_guild_run(target_user_id)
        flash("Scan Mutual Guild lance en arriere-plan.", "success")
        return redirect(url_for("mutual_guild_tool", run_id=run_id))

    run_id = (request.args.get("run_id") or "").strip()
    run = load_mutual_run(run_id)
    results = list_mutual_results(run_id) if run else []
    return render_template(
        "mutual_guild.html",
        profiles=load_profiles(include_tokens=False),
        runs=list_mutual_runs(),
        run=run,
        results=results,
    )


@app.route("/tools/message-scraper", methods=["GET", "POST"])
def message_scraper_tool():
    if request.method == "POST":
        mutual_run_id = (request.form.get("mutual_run_id") or "").strip()
        target_user_id = (request.form.get("target_user_id") or "").strip()
        guild_ids = request.form.getlist("guild_ids")
        max_pages = parse_int(request.form.get("max_pages"), default=MESSAGE_SCRAPER_DEFAULT_MAX_PAGES, minimum=0, maximum=200)

        mutual_run = load_mutual_run(mutual_run_id)
        if not mutual_run:
            flash("Lance ou selectionne d'abord un resultat Mutual Guild.", "error")
            return redirect(url_for("message_scraper_tool"))
        if mutual_run.get("status") != "completed":
            flash("Attends la fin du scan Mutual Guild avant de lancer Message Scrapper.", "error")
            return redirect(url_for("message_scraper_tool", mutual_run_id=mutual_run_id))
        if not re.fullmatch(r"\d{15,25}", target_user_id):
            flash("ID utilisateur invalide.", "error")
            return redirect(url_for("message_scraper_tool", mutual_run_id=mutual_run_id))
        if not guild_ids:
            flash("Selectionne au moins un serveur a scrapper.", "error")
            return redirect(url_for("message_scraper_tool", mutual_run_id=mutual_run_id))

        run_id = create_message_scraper_run(mutual_run_id, target_user_id, guild_ids, max_pages)
        flash("Message Scrapper lance en arriere-plan.", "success")
        return redirect(url_for("message_scraper_tool", mutual_run_id=mutual_run_id, run_id=run_id))

    mutual_run_id = (request.args.get("mutual_run_id") or "").strip()
    scraper_run_id = (request.args.get("run_id") or "").strip()
    mutual_run = load_mutual_run(mutual_run_id)
    mutual_results = list_mutual_results(mutual_run_id) if mutual_run else []
    scraper_run = load_message_scraper_run(scraper_run_id)
    if scraper_run and mutual_run_id and scraper_run.get("mutual_run_id") != mutual_run_id:
        scraper_run = None
    scraper_view = message_scraper_view_data(scraper_run_id) if scraper_run else {"messages": [], "channels": [], "guilds": [], "stats": {}}

    return render_template(
        "message_scraper.html",
        mutual_run=mutual_run,
        mutual_results=mutual_results,
        mutual_runs=list_mutual_runs(),
        scraper_run=scraper_run,
        scraper_runs=list_message_scraper_runs(mutual_run_id=mutual_run_id or None),
        scraper_view=scraper_view,
        preview_limit=MESSAGE_SCRAPER_PREVIEW_LIMIT,
        default_max_pages=MESSAGE_SCRAPER_DEFAULT_MAX_PAGES,
    )


def bounded_progress(done: Any, total: Any) -> float:
    try:
        total_value = int(total or 0)
        done_value = int(done or 0)
    except (TypeError, ValueError):
        return 0
    if total_value <= 0:
        return 0
    return min(100, max(0, (done_value / total_value) * 100))


def active_activity_items() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    with task_lock:
        active_deletions = [
            (task_id, dict(task))
            for task_id, task in deletion_tasks.items()
            if task.get("status") in {"pending", "running"}
        ]

    for task_id, task in active_deletions:
        processed = int(task.get("deleted_messages") or 0) + int(task.get("failed_messages") or 0) + int(task.get("skipped_messages") or 0)
        total = int(task.get("total_messages") or 0)
        current = task.get("current_channel_name") or task.get("current_channel") or "Preparation"
        items.append(
            {
                "id": task_id,
                "service": "Suppression messages",
                "status": task.get("status"),
                "status_label": status_label(task.get("status")),
                "detail": f"{processed}/{total} message(s) · {current}" if total else str(current),
                "progress": bounded_progress(processed, total),
                "current": current,
                "url": url_for("deletion_status", task_id=task_id),
                "updated_at": task.get("start_time"),
            }
        )

    with mutual_guild_lock:
        active_mutual_ids = sorted(mutual_guild_active_runs)
    with message_scraper_lock:
        active_scraper_ids = sorted(message_scraper_active_runs)
    with account_backup_lock:
        active_backup_ids = sorted(account_backup_active_runs)
    with selfbot_tool_lock:
        active_selfbot_ids = sorted(selfbot_tool_active_runs)
    with autobumper_lock:
        active_bump_ids = sorted(autobumper_active_targets)

    conn = get_db_connection()
    mutual_runs: list[dict[str, Any]] = []
    if active_mutual_ids:
        placeholders = ",".join("?" for _ in active_mutual_ids)
        mutual_runs = rows_to_dicts(
            conn.execute(
                f"""
                SELECT *
                FROM mutual_guild_runs
                WHERE id IN ({placeholders})
                ORDER BY updated_at DESC
                """,
                tuple(active_mutual_ids),
            ).fetchall()
        )
    scraper_runs: list[dict[str, Any]] = []
    if active_scraper_ids:
        placeholders = ",".join("?" for _ in active_scraper_ids)
        scraper_runs = rows_to_dicts(
            conn.execute(
                f"""
                SELECT *
                FROM message_scraper_runs
                WHERE id IN ({placeholders})
                ORDER BY updated_at DESC
                """,
                tuple(active_scraper_ids),
            ).fetchall()
        )
    backup_runs: list[dict[str, Any]] = []
    if active_backup_ids:
        placeholders = ",".join("?" for _ in active_backup_ids)
        backup_runs = rows_to_dicts(
            conn.execute(
                f"""
                SELECT r.*, p.name AS profile_name, p.username AS profile_username
                FROM account_backup_runs r
                LEFT JOIN profiles p ON p.id = r.profile_id
                WHERE r.id IN ({placeholders})
                ORDER BY r.updated_at DESC
                """,
                tuple(active_backup_ids),
            ).fetchall()
        )
    selfbot_runs: list[dict[str, Any]] = []
    if active_selfbot_ids:
        placeholders = ",".join("?" for _ in active_selfbot_ids)
        selfbot_runs = rows_to_dicts(
            conn.execute(
                f"""
                SELECT r.*, p.name AS profile_name, p.username AS profile_username
                FROM selfbot_tool_runs r
                LEFT JOIN profiles p ON p.id = r.profile_id
                WHERE r.id IN ({placeholders})
                ORDER BY r.updated_at DESC
                """,
                tuple(active_selfbot_ids),
            ).fetchall()
        )
    autobump_targets: list[dict[str, Any]] = []
    if active_bump_ids:
        placeholders = ",".join("?" for _ in active_bump_ids)
        autobump_targets = rows_to_dicts(
            conn.execute(
                f"""
                SELECT t.*, p.name AS profile_name, p.username AS profile_username
                FROM autobumper_targets t
                LEFT JOIN profiles p ON p.id = t.profile_id
                WHERE t.id IN ({placeholders})
                ORDER BY t.updated_at DESC
                """,
                tuple(active_bump_ids),
            ).fetchall()
        )
    conn.close()

    for run in mutual_runs:
        done = int(run.get("completed_profiles") or 0)
        total = int(run.get("profile_count") or 0)
        items.append(
            {
                "id": run["id"],
                "service": "Mutual Guild",
                "status": run["status"],
                "status_label": mutual_guild_status_label(run["status"]),
                "detail": f"{done}/{total} compte(s) · {run.get('mutual_count') or 0} mutual · {run.get('guild_count') or 0} serveur(s)",
                "progress": mutual_guild_progress(run),
                "current": run.get("target_user_id"),
                "url": url_for("mutual_guild_tool", run_id=run["id"]),
                "updated_at": run.get("updated_at") or run.get("created_at"),
            }
        )

    for run in scraper_runs:
        done = int(run.get("completed_guilds") or 0)
        total = int(run.get("total_guilds") or 0)
        items.append(
            {
                "id": run["id"],
                "service": "Message Scrapper",
                "status": run["status"],
                "status_label": message_scraper_status_label(run["status"]),
                "detail": (
                    f"{done}/{total} serveur(s) · {run.get('total_messages') or 0} message(s) · "
                    f"{run.get('total_pages') or 0} page(s) · {run.get('current_phase') or 'En cours'}"
                ),
                "progress": message_scraper_progress(run),
                "current": run.get("current_detail") or run.get("target_user_id"),
                "url": url_for(
                    "message_scraper_tool",
                    mutual_run_id=run.get("mutual_run_id") or "",
                    run_id=run["id"],
                ),
                "updated_at": run.get("updated_at") or run.get("created_at"),
            }
        )

    for run in backup_runs:
        done = int(run.get("processed_guilds") or 0)
        total = int(run.get("guild_count") or 0)
        profile_name = run.get("profile_name") or run.get("profile_username") or f"Profil {run.get('profile_id')}"
        items.append(
            {
                "id": run["id"],
                "service": "Backup compte",
                "status": run["status"],
                "status_label": account_backup_status_label(run["status"]),
                "detail": (
                    f"{profile_name} · {run.get('friend_count') or 0} ami(s) · "
                    f"{done}/{total} serveur(s) · {run.get('invite_count') or 0} invitation(s)"
                ),
                "progress": account_backup_progress(run),
                "current": run.get("current_detail") or run.get("current_phase"),
                "url": url_for("account_backup", profile_id=run["profile_id"], run_id=run["id"]),
                "updated_at": run.get("updated_at") or run.get("created_at"),
            }
        )

    for run in selfbot_runs:
        processed = int(run.get("processed_count") or 0)
        total = int(run.get("total_count") or 0)
        profile_name = run.get("profile_name") or run.get("profile_username") or f"Profil {run.get('profile_id')}"
        items.append(
            {
                "id": run["id"],
                "service": f"Selfbot · {selfbot_tool_label(run.get('tool'))}",
                "status": run["status"],
                "status_label": selfbot_tool_status_label(run["status"]),
                "detail": f"{profile_name} · {processed}/{total} traite(s) · {run.get('current_detail') or ''}",
                "progress": selfbot_tool_progress(run),
                "current": run.get("current_detail") or run.get("tool"),
                "url": url_for("selfbot_suite", tab="runs", run_id=run["id"]),
                "updated_at": run.get("updated_at") or run.get("created_at"),
            }
        )

    for target in autobump_targets:
        guild_name = target.get("guild_name") or target.get("guild_id") or "Serveur inconnu"
        channel_name = target.get("channel_name") or target.get("channel_id") or "Salon inconnu"
        profile_name = target.get("profile_name") or target.get("profile_username") or f"Profil {target.get('profile_id')}"
        items.append(
            {
                "id": str(target["id"]),
                "service": "Auto Bump",
                "status": target.get("last_status") or "running",
                "status_label": autobumper_status_label(target.get("last_status") or "running"),
                "detail": f"{profile_name} · {guild_name} · #{channel_name}",
                "progress": None,
                "current": guild_name,
                "url": url_for("autobumper", profile_id=target["profile_id"], guild_id=target.get("guild_id") or ""),
                "updated_at": target.get("updated_at"),
            }
        )

    return items


@app.route("/api/activity")
def api_activity():
    items = active_activity_items()
    return jsonify({"count": len(items), "items": items})


@app.route("/api/mutual_guild/<run_id>")
def api_mutual_guild_status(run_id: str):
    run = load_mutual_run(run_id)
    if not run:
        abort(404)
    return jsonify(
        {
            "id": run["id"],
            "status": run["status"],
            "status_label": mutual_guild_status_label(run["status"]),
            "status_badge_class": mutual_guild_status_badge_class(run["status"]),
            "progress": mutual_guild_progress(run),
            "profile_count": run["profile_count"],
            "completed_profiles": run["completed_profiles"],
            "guild_count": run["guild_count"],
            "mutual_count": run["mutual_count"],
            "error": run["error"],
        }
    )


@app.route("/api/message_scraper/<run_id>")
def api_message_scraper_status(run_id: str):
    run = load_message_scraper_run(run_id)
    if not run:
        abort(404)
    return jsonify(
        {
            "id": run["id"],
            "status": run["status"],
            "status_label": message_scraper_status_label(run["status"]),
            "status_badge_class": message_scraper_status_badge_class(run["status"]),
            "progress": message_scraper_progress(run),
            "total_guilds": run["total_guilds"],
            "completed_guilds": run["completed_guilds"],
            "total_messages": run["total_messages"],
            "total_pages": run["total_pages"],
            "current_guild_id": run.get("current_guild_id"),
            "current_guild_name": run.get("current_guild_name"),
            "current_phase": run.get("current_phase"),
            "current_detail": run.get("current_detail"),
            "last_status_code": run.get("last_status_code"),
            "error": run["error"],
        }
    )


@app.route("/api/account_backup/<run_id>")
def api_account_backup_status(run_id: str):
    run = load_account_backup_run(run_id)
    if not run:
        abort(404)
    return jsonify(
        {
            "id": run["id"],
            "status": run["status"],
            "status_label": account_backup_status_label(run["status"]),
            "status_badge_class": account_backup_status_badge_class(run["status"]),
            "progress": account_backup_progress(run),
            "friend_count": run.get("friend_count") or 0,
            "guild_count": run.get("guild_count") or 0,
            "processed_guilds": run.get("processed_guilds") or 0,
            "invite_count": run.get("invite_count") or 0,
            "failed_invite_count": run.get("failed_invite_count") or 0,
            "current_phase": run.get("current_phase"),
            "current_detail": run.get("current_detail"),
            "error": run.get("error"),
        }
    )


@app.route("/api/selfbot_tools/runs/<run_id>")
def api_selfbot_tool_status(run_id: str):
    run = load_selfbot_tool_run(run_id)
    if not run:
        abort(404)
    return jsonify(
        {
            "id": run["id"],
            "tool": run["tool"],
            "tool_label": selfbot_tool_label(run["tool"]),
            "status": run["status"],
            "status_label": selfbot_tool_status_label(run["status"]),
            "status_badge_class": selfbot_tool_status_badge_class(run["status"]),
            "progress": selfbot_tool_progress(run),
            "total_count": run.get("total_count") or 0,
            "processed_count": run.get("processed_count") or 0,
            "success_count": run.get("success_count") or 0,
            "fail_count": run.get("fail_count") or 0,
            "current_detail": run.get("current_detail"),
            "error": run.get("error"),
        }
    )


@app.route("/tools/selfbot-suite/runs/<run_id>/result.json")
def export_selfbot_tool_run(run_id: str):
    run = load_selfbot_tool_run(run_id)
    if not run:
        abort(404)
    data = json.dumps(selfbot_tool_public_payload(run), ensure_ascii=False, indent=2).encode("utf-8")
    return send_file(
        io.BytesIO(data),
        mimetype="application/json",
        as_attachment=True,
        download_name=f"selfbot_{run.get('tool') or 'tool'}_{run_id}.json",
    )


@app.route("/tools/selfbot-suite/backups/<backup_id>.json")
def export_server_backup_json(backup_id: str):
    payload = server_backup_export_payload(load_server_backup(backup_id))
    if not payload:
        abort(404)
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    return send_file(
        io.BytesIO(data),
        mimetype="application/json",
        as_attachment=True,
        download_name=f"server_backup_{backup_id}.json",
    )


@app.route("/tools/message-scraper/<run_id>/export.json")
def export_message_scraper_run(run_id: str):
    run = load_message_scraper_run(run_id)
    if not run:
        abort(404)
    rows = list_message_scraper_results(run_id)
    payload = {
        "run": run,
        "messages": rows,
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    return send_file(
        io.BytesIO(data),
        mimetype="application/json",
        as_attachment=True,
        download_name=f"message_scraper_{run_id}.json",
    )


@app.route("/tools/account-backup/<run_id>/export.json")
def export_account_backup_json(run_id: str):
    payload = account_backup_payload(run_id)
    if not payload:
        abort(404)
    data = json.dumps(account_backup_export_payload(payload), ensure_ascii=False, indent=2).encode("utf-8")
    return send_file(
        io.BytesIO(data),
        mimetype="application/json",
        as_attachment=True,
        download_name=f"account_backup_{run_id}.json",
    )


@app.route("/tools/account-backup/<run_id>/export.zip")
def export_account_backup_zip(run_id: str):
    payload = account_backup_payload(run_id)
    if not payload:
        abort(404)
    return send_file(
        io.BytesIO(build_account_backup_zip(payload)),
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"account_backup_{run_id}.zip",
    )


def account_backup_progress(run: dict[str, Any] | None) -> float:
    if not run:
        return 0
    if run.get("status") == "completed":
        return 100
    try:
        total = max(1, int(run.get("guild_count") or 0))
        done = int(run.get("processed_guilds") or 0)
    except (TypeError, ValueError):
        return 0
    return min(100, max(0, (done / total) * 100))


def update_account_backup_run_fields(run_id: str, **fields: Any) -> None:
    allowed = {
        "status",
        "friend_count",
        "guild_count",
        "processed_guilds",
        "invite_count",
        "failed_invite_count",
        "current_phase",
        "current_detail",
        "error",
        "completed_at",
    }
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return

    assignments = [f"{key} = ?" for key in updates]
    assignments.append("updated_at = CURRENT_TIMESTAMP")
    values = list(updates.values())
    values.append(run_id)
    conn = get_db_connection()
    conn.execute(
        f"UPDATE account_backup_runs SET {', '.join(assignments)} WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()


def create_account_backup_run(profile_id: int, create_invites: bool, source: str = "manual") -> str:
    run_id = secrets.token_hex(8)
    source = source if source in {"manual", "scheduled"} else "manual"
    conn = get_db_connection()
    conn.execute(
        """
        INSERT INTO account_backup_runs (id, profile_id, status, source, create_invites, current_phase, current_detail)
        VALUES (?, ?, 'pending', ?, ?, 'Preparation', 'Backup en attente.')
        """,
        (run_id, profile_id, source, 1 if create_invites else 0),
    )
    conn.commit()
    conn.close()

    thread = threading.Thread(
        target=run_account_backup_task,
        args=(run_id,),
        daemon=True,
        name=f"account-backup-{run_id}",
    )
    thread.start()
    return run_id


def update_account_backup_schedule_profile_from_run(run_id: str) -> None:
    conn = get_db_connection()
    run = row_to_dict(conn.execute("SELECT * FROM account_backup_runs WHERE id = ?", (run_id,)).fetchone())
    if not run or run.get("source") != "scheduled":
        conn.close()
        return

    finished_at = run.get("completed_at") or run.get("updated_at") or datetime.now().astimezone().isoformat(timespec="seconds")
    status = run.get("status") or "completed"
    error = run.get("error")
    conn.execute(
        """
        INSERT INTO account_backup_schedule_profiles (
            profile_id, enabled, last_run_id, last_run_at, last_status, last_error, updated_at
        )
        VALUES (?, 1, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(profile_id) DO UPDATE SET
            last_run_id = excluded.last_run_id,
            last_run_at = excluded.last_run_at,
            last_status = excluded.last_status,
            last_error = excluded.last_error,
            updated_at = CURRENT_TIMESTAMP
        """,
        (run["profile_id"], run_id, finished_at, status, error),
    )
    record_account_backup_schedule_event(
        conn,
        run["profile_id"],
        run_id,
        status,
        f"Backup planifie {account_backup_status_label(status).lower()} pour ce compte.",
    )
    conn.commit()
    conn.close()


def process_account_backup_schedule_once(force_missing: bool = False) -> list[str]:
    if not account_backup_scheduler_lock.acquire(blocking=False):
        return []

    launched: list[str] = []
    now = datetime.now().astimezone()
    now_iso = now.isoformat(timespec="seconds")

    try:
        settings = load_account_backup_schedule_settings()
        if not settings.get("enabled"):
            conn = get_db_connection()
            conn.execute(
                """
                UPDATE account_backup_schedule_settings
                SET last_check_at = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                """,
                (now_iso,),
            )
            conn.commit()
            conn.close()
            return launched

        profile_rows = [
            row for row in list_account_backup_schedule_profiles(settings)
            if row.get("schedule_enabled")
        ]

        conn = get_db_connection()
        conn.execute(
            """
            UPDATE account_backup_schedule_settings
            SET last_check_at = ?, last_error = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (now_iso,),
        )
        conn.commit()
        conn.close()

        for profile in profile_rows:
            profile_id = int(profile["profile_id"])
            if account_backup_profile_has_active_run(profile_id):
                conn = get_db_connection()
                conn.execute(
                    """
                    UPDATE account_backup_schedule_profiles
                    SET last_status = 'waiting',
                        last_error = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE profile_id = ?
                    """,
                    (profile_id,),
                )
                conn.commit()
                conn.close()
                continue

            due_info = account_backup_schedule_due_info(
                settings,
                profile,
                now,
                ignore_daily_time=force_missing,
            )
            conn = get_db_connection()
            conn.execute(
                """
                UPDATE account_backup_schedule_profiles
                SET next_run_at = ?,
                    last_status = COALESCE(last_status, 'waiting'),
                    updated_at = CURRENT_TIMESTAMP
                WHERE profile_id = ?
                """,
                (due_info["next_run_at"], profile_id),
            )
            conn.commit()
            conn.close()

            if not due_info["due"]:
                continue

            run_id = create_account_backup_run(
                profile_id,
                bool(settings.get("create_invites")),
                source="scheduled",
            )
            launched.append(run_id)

            conn = get_db_connection()
            conn.execute(
                """
                UPDATE account_backup_schedule_settings
                SET last_run_started_at = ?, last_error = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                """,
                (now_iso,),
            )
            conn.execute(
                """
                INSERT INTO account_backup_schedule_profiles (
                    profile_id, enabled, last_run_id, last_run_at, last_status, last_error, next_run_at, updated_at
                )
                VALUES (?, 1, ?, ?, 'started', NULL, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(profile_id) DO UPDATE SET
                    enabled = 1,
                    last_run_id = excluded.last_run_id,
                    last_run_at = excluded.last_run_at,
                    last_status = excluded.last_status,
                    last_error = NULL,
                    next_run_at = excluded.next_run_at,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (profile_id, run_id, now_iso, due_info["next_run_at"]),
            )
            record_account_backup_schedule_event(
                conn,
                profile_id,
                run_id,
                "started",
                "Backup planifie lance automatiquement.",
            )
            conn.commit()
            conn.close()
    except Exception as error:
        conn = get_db_connection()
        conn.execute(
            """
            UPDATE account_backup_schedule_settings
            SET last_check_at = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (now_iso, str(error)[:1000]),
        )
        record_account_backup_schedule_event(conn, None, None, "failed", f"Routine backup: {error}")
        conn.commit()
        conn.close()
    finally:
        account_backup_scheduler_lock.release()

    return launched


def account_backup_scheduler_loop() -> None:
    while not account_backup_scheduler_stop_event.is_set():
        try:
            process_account_backup_schedule_once()
        except Exception as error:
            print(f"Erreur routine backup compte: {error}")
        account_backup_scheduler_stop_event.wait(ACCOUNT_BACKUP_SCHEDULER_CHECK_INTERVAL)


def should_start_account_backup_scheduler_worker() -> bool:
    if not ACCOUNT_BACKUP_SCHEDULER_ENABLED:
        return False
    if os.getenv("FLASK_DEBUG", "1") != "0" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return False
    return True


def start_account_backup_scheduler_worker() -> None:
    global account_backup_scheduler_thread
    if not should_start_account_backup_scheduler_worker():
        return
    if account_backup_scheduler_thread and account_backup_scheduler_thread.is_alive():
        return

    account_backup_scheduler_thread = threading.Thread(
        target=account_backup_scheduler_loop,
        daemon=True,
        name="account-backup-scheduler",
    )
    account_backup_scheduler_thread.start()


def load_account_backup_run(run_id: str | None) -> dict[str, Any] | None:
    if not run_id:
        return None
    conn = get_db_connection()
    run = row_to_dict(
        conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username, p.user_id AS profile_user_id,
                   p.avatar_url AS profile_avatar_url
            FROM account_backup_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            WHERE r.id = ?
            """,
            (run_id,),
        ).fetchone()
    )
    conn.close()
    return run


def list_account_backup_runs(profile_id: int | None = None, limit: int = 20) -> list[dict[str, Any]]:
    conn = get_db_connection()
    if profile_id:
        rows = conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username, p.user_id AS profile_user_id
            FROM account_backup_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            WHERE r.profile_id = ?
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (profile_id, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username, p.user_id AS profile_user_id
            FROM account_backup_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    runs = rows_to_dicts(rows)
    conn.close()
    return runs


def list_account_backup_friends(run_id: str | None) -> list[dict[str, Any]]:
    if not run_id:
        return []
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT *
            FROM account_backup_friends
            WHERE run_id = ?
            ORDER BY COALESCE(display_name, username, user_id) COLLATE NOCASE
            """,
            (run_id,),
        ).fetchall()
    )
    conn.close()
    return rows


def list_account_backup_guilds(run_id: str | None) -> list[dict[str, Any]]:
    if not run_id:
        return []
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT *
            FROM account_backup_guilds
            WHERE run_id = ?
            ORDER BY COALESCE(guild_name, guild_id) COLLATE NOCASE
            """,
            (run_id,),
        ).fetchall()
    )
    conn.close()
    return rows


def normalize_backup_daily_time(value: Any) -> str:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{2})\s*", str(value or ""))
    if not match:
        return "03:00"
    hour = min(23, max(0, int(match.group(1))))
    minute = min(59, max(0, int(match.group(2))))
    return f"{hour:02d}:{minute:02d}"


def account_backup_schedule_mode_label(mode: str | None) -> str:
    return {
        "daily": "Une fois par jour",
        "interval": "Tous les X jours",
    }.get(mode or "daily", "Une fois par jour")


def account_backup_schedule_status_label(status: str | None) -> str:
    return {
        "started": "Lance",
        "completed": "Termine",
        "failed": "Erreur",
        "skipped": "Ignore",
        "waiting": "En attente",
    }.get(status or "waiting", status or "En attente")


def account_backup_schedule_status_badge_class(status: str | None) -> str:
    return {
        "started": "text-bg-primary",
        "completed": "text-bg-success",
        "failed": "text-bg-danger",
        "skipped": "text-bg-secondary",
        "waiting": "text-bg-secondary",
    }.get(status or "waiting", "text-bg-secondary")


def load_account_backup_schedule_settings() -> dict[str, Any]:
    conn = get_db_connection()
    row = row_to_dict(conn.execute("SELECT * FROM account_backup_schedule_settings WHERE id = 1").fetchone())
    if not row:
        conn.execute("INSERT OR IGNORE INTO account_backup_schedule_settings (id) VALUES (1)")
        conn.commit()
        row = row_to_dict(conn.execute("SELECT * FROM account_backup_schedule_settings WHERE id = 1").fetchone())
    conn.close()
    settings = row or {}
    settings["mode"] = settings.get("mode") if settings.get("mode") in {"daily", "interval"} else "daily"
    settings["interval_days"] = max(1, int(settings.get("interval_days") or 1))
    settings["daily_time"] = normalize_backup_daily_time(settings.get("daily_time"))
    return settings


def account_backup_latest_run_for_profile(conn: sqlite3.Connection, profile_id: int) -> dict[str, Any] | None:
    return row_to_dict(
        conn.execute(
            """
            SELECT *
            FROM account_backup_runs
            WHERE profile_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (profile_id,),
        ).fetchone()
    )


def account_backup_profile_has_active_run(profile_id: int) -> bool:
    with account_backup_lock:
        active_ids = sorted(account_backup_active_runs)

    conn = get_db_connection()
    if active_ids:
        placeholders = ",".join("?" for _ in active_ids)
        active_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM account_backup_runs WHERE id IN ({placeholders}) AND profile_id = ?",
                tuple(active_ids) + (profile_id,),
            ).fetchone()[0]
            or 0
        )
        if active_count:
            conn.close()
            return True

    pending_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM account_backup_runs
            WHERE profile_id = ?
              AND status IN ('pending', 'running')
            """,
            (profile_id,),
        ).fetchone()[0]
        or 0
    )
    conn.close()
    return pending_count > 0


def account_backup_schedule_due_info(
    settings: dict[str, Any],
    profile_row: dict[str, Any],
    now: datetime | None = None,
    ignore_daily_time: bool = False,
) -> dict[str, Any]:
    current = now or datetime.now().astimezone()
    mode = settings.get("mode") if settings.get("mode") in {"daily", "interval"} else "daily"
    last_run_at = parse_local_datetime(profile_row.get("latest_created_at")) or parse_local_datetime(profile_row.get("last_run_at"))

    if mode == "interval":
        interval_days = max(1, int(settings.get("interval_days") or 1))
        next_run = last_run_at + timedelta(days=interval_days) if last_run_at else current
        due = current >= next_run
        return {
            "due": due,
            "already_done": bool(last_run_at and not due),
            "next_run_at": current.isoformat(timespec="seconds") if due else next_run.isoformat(timespec="seconds"),
            "reason": "Intervalle atteint." if due else f"Backup deja fait dans les {interval_days} jour(s).",
        }

    daily_time = normalize_backup_daily_time(settings.get("daily_time"))
    hour, minute = [int(part) for part in daily_time.split(":", 1)]
    today_slot = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    already_done_today = bool(last_run_at and last_run_at.date() == current.date())
    if already_done_today:
        next_run = today_slot + timedelta(days=1)
        return {
            "due": False,
            "already_done": True,
            "next_run_at": next_run.isoformat(timespec="seconds"),
            "reason": "Backup deja fait aujourd'hui.",
        }
    if not ignore_daily_time and current < today_slot:
        return {
            "due": False,
            "already_done": False,
            "next_run_at": today_slot.isoformat(timespec="seconds"),
            "reason": f"En attente de {daily_time}.",
        }
    return {
        "due": True,
        "already_done": False,
        "next_run_at": current.isoformat(timespec="seconds"),
        "reason": "Backup quotidien manquant.",
    }


def list_account_backup_schedule_profiles(settings: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    schedule_settings = settings or load_account_backup_schedule_settings()
    now = datetime.now().astimezone()
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT
                p.id AS profile_id,
                p.name,
                p.user_id,
                p.username,
                p.discriminator,
                p.avatar_url,
                COALESCE(sp.enabled, 0) AS schedule_enabled,
                sp.last_run_id,
                sp.last_run_at,
                sp.next_run_at,
                sp.last_status,
                sp.last_error,
                lr.id AS latest_run_id,
                lr.status AS latest_status,
                lr.source AS latest_source,
                lr.friend_count AS latest_friend_count,
                lr.guild_count AS latest_guild_count,
                lr.invite_count AS latest_invite_count,
                lr.created_at AS latest_created_at,
                lr.completed_at AS latest_completed_at,
                lr.error AS latest_error
            FROM profiles p
            LEFT JOIN account_backup_schedule_profiles sp ON sp.profile_id = p.id
            LEFT JOIN account_backup_runs lr ON lr.id = (
                SELECT r.id
                FROM account_backup_runs r
                WHERE r.profile_id = p.id
                ORDER BY r.created_at DESC
                LIMIT 1
            )
            ORDER BY p.name COLLATE NOCASE
            """
        ).fetchall()
    )
    conn.close()

    for row in rows:
        due_info = account_backup_schedule_due_info(schedule_settings, row, now)
        row["due"] = due_info["due"]
        row["already_done"] = due_info["already_done"]
        row["computed_next_run_at"] = due_info["next_run_at"]
        row["schedule_reason"] = due_info["reason"]
        row["active_backup"] = account_backup_profile_has_active_run(int(row["profile_id"]))
    return rows


def record_account_backup_schedule_event(
    conn: sqlite3.Connection,
    profile_id: int | None,
    run_id: str | None,
    status: str,
    message: str,
) -> None:
    conn.execute(
        """
        INSERT INTO account_backup_schedule_events (profile_id, run_id, status, message)
        VALUES (?, ?, ?, ?)
        """,
        (profile_id, run_id, status, message[:1000]),
    )


def list_account_backup_schedule_events(limit: int = 20) -> list[dict[str, Any]]:
    conn = get_db_connection()
    rows = rows_to_dicts(
        conn.execute(
            """
            SELECT e.*, p.name AS profile_name, p.username AS profile_username
            FROM account_backup_schedule_events e
            LEFT JOIN profiles p ON p.id = e.profile_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    )
    conn.close()
    return rows


def account_backup_status_label(status: str | None) -> str:
    return {
        "pending": "En attente",
        "running": "Backup en cours",
        "completed": "Termine",
        "failed": "Erreur",
    }.get(status or "pending", status or "En attente")


def account_backup_status_badge_class(status: str | None) -> str:
    return {
        "pending": "text-bg-secondary",
        "running": "text-bg-primary",
        "completed": "text-bg-success",
        "failed": "text-bg-danger",
    }.get(status or "pending", "text-bg-secondary")


def invite_status_label(status: str | None) -> str:
    return {
        "created": "Cree",
        "existing": "Existant",
        "existing_limited": "Existant (limite)",
        "invite_limit": "Limite atteinte",
        "not_requested": "Non demande",
        "no_permission": "Permission absente",
        "no_channel": "Aucun salon",
        "failed": "Erreur",
    }.get(status or "not_requested", status or "Non demande")


def invite_status_badge_class(status: str | None) -> str:
    return {
        "created": "text-bg-success",
        "existing": "text-bg-success",
        "existing_limited": "text-bg-warning",
        "invite_limit": "text-bg-warning",
        "not_requested": "text-bg-secondary",
        "no_permission": "text-bg-warning",
        "no_channel": "text-bg-warning",
        "failed": "text-bg-danger",
    }.get(status or "not_requested", "text-bg-secondary")


def invite_payload_from_existing(invite: dict[str, Any], status: str, error: str | None = None) -> dict[str, Any]:
    code = invite.get("code")
    channel = invite.get("channel") or {}
    return {
        "status": status,
        "code": code,
        "url": f"https://discord.gg/{code}" if code else None,
        "max_age": invite.get("max_age"),
        "channel_id": channel.get("id"),
        "channel_name": channel.get("name"),
        "error": error,
        "raw": invite,
    }


def best_existing_invite(invites: list[dict[str, Any]], permanent_only: bool = False) -> dict[str, Any] | None:
    usable = [invite for invite in invites if invite.get("code")]
    if permanent_only:
        usable = [invite for invite in usable if invite.get("max_age") == 0]
    if not usable:
        return None
    return sorted(
        usable,
        key=lambda invite: (
            0 if invite.get("max_age") == 0 else 1,
            0 if invite.get("max_uses") in (0, None) else 1,
            str(invite.get("code") or ""),
        ),
    )[0]


def backup_invite_payload(token: str, guild: dict[str, Any], channels: list[dict[str, Any]]) -> dict[str, Any]:
    if not guild_can_create_invite(guild):
        return {
            "status": "no_permission",
            "error": "Permission creer une invitation absente sur ce serveur.",
        }
    if not channels:
        return {
            "status": "no_channel",
            "error": "Aucun salon texte disponible pour creer une invitation.",
        }

    existing_invites, existing_error = get_guild_invites(token, str(guild["id"]))
    permanent_invite = best_existing_invite(existing_invites, permanent_only=True)
    if permanent_invite:
        return invite_payload_from_existing(permanent_invite, "existing")

    last_error = None
    last_error_code = None
    for channel in channels:
        invite, error, error_code = create_channel_invite(token, str(channel["id"]), max_age=0)
        if invite:
            code = invite.get("code")
            return {
                "status": "created",
                "code": code,
                "url": f"https://discord.gg/{code}" if code else None,
                "max_age": invite.get("max_age"),
                "channel_id": channel["id"],
                "channel_name": channel["name"],
                "raw": invite,
            }
        last_error = error
        last_error_code = error_code
        if error_code == 30016:
            reusable_invite = best_existing_invite(existing_invites)
            if reusable_invite:
                return invite_payload_from_existing(
                    reusable_invite,
                    "existing_limited",
                    "Maximum d'invitations atteint; lien existant reutilise.",
                )
            return {
                "status": "invite_limit",
                "error": invite_limit_error_message(existing_error),
            }

    if last_error_code == 50013:
        return {
            "status": "no_permission",
            "error": "Permissions manquantes pour creer une invitation dans les salons testes.",
        }

    return {
        "status": "failed",
        "error": last_error or "Impossible de creer une invitation sur les salons testes.",
    }


def run_account_backup_task(run_id: str) -> None:
    with account_backup_lock:
        if run_id in account_backup_active_runs:
            return
        account_backup_active_runs.add(run_id)

    try:
        conn = get_db_connection()
        run = row_to_dict(conn.execute("SELECT * FROM account_backup_runs WHERE id = ?", (run_id,)).fetchone())
        if not run:
            conn.close()
            return
        profile = row_to_dict(conn.execute("SELECT * FROM profiles WHERE id = ?", (run["profile_id"],)).fetchone())
        conn.close()
        if not profile:
            update_account_backup_run_fields(
                run_id,
                status="failed",
                current_phase="Erreur",
                current_detail="Profil introuvable.",
                error="Profil introuvable.",
                completed_at=datetime.now().isoformat(timespec="seconds"),
            )
            return

        create_invites = bool(run.get("create_invites"))
        update_account_backup_run_fields(
            run_id,
            status="running",
            current_phase="Amis",
            current_detail="Lecture de la liste d'amis.",
        )
        relationships, relationship_error = get_user_relationships(profile["token"])
        friends = [
            relation
            for relation in relationships
            if str(relation.get("relationship_type")) == "1"
        ]

        conn = get_db_connection()
        conn.execute("DELETE FROM account_backup_friends WHERE run_id = ?", (run_id,))
        for friend in friends:
            conn.execute(
                """
                INSERT OR REPLACE INTO account_backup_friends (
                    run_id, user_id, username, global_name, discriminator, display_name,
                    avatar_url, relationship_type, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    friend["user_id"],
                    friend.get("username"),
                    friend.get("global_name"),
                    friend.get("discriminator"),
                    friend.get("display_name"),
                    friend.get("avatar_url"),
                    friend.get("relationship_type"),
                    json.dumps(friend.get("raw") or {}, ensure_ascii=False),
                ),
            )
        conn.commit()
        conn.close()

        update_account_backup_run_fields(
            run_id,
            friend_count=len(friends),
            current_phase="Serveurs",
            current_detail="Lecture des serveurs du compte.",
        )
        guilds = get_user_guilds(profile["token"], profile.get("user_id"))

        conn = get_db_connection()
        conn.execute("DELETE FROM account_backup_guilds WHERE run_id = ?", (run_id,))
        upsert_guilds(conn, guilds)
        conn.commit()
        conn.close()

        invite_count = 0
        failed_invite_count = 0
        for index, guild in enumerate(guilds, 1):
            guild_id = str(guild.get("id") or "")
            guild_name = guild.get("name") or guild_id
            update_account_backup_run_fields(
                run_id,
                guild_count=len(guilds),
                processed_guilds=index - 1,
                invite_count=invite_count,
                failed_invite_count=failed_invite_count,
                current_phase="Invitations" if create_invites else "Serveurs",
                current_detail=f"{index}/{len(guilds)} - {guild_name}",
            )

            channels = get_guild_channels(profile["token"], guild_id)
            invite_payload = {"status": "not_requested"}
            if create_invites:
                invite_payload = backup_invite_payload(profile["token"], guild, channels)
                if invite_payload.get("url"):
                    invite_count += 1
                elif invite_payload.get("status") in {"failed", "no_permission", "no_channel", "invite_limit"}:
                    failed_invite_count += 1

            channel_id = invite_payload.get("channel_id")
            channel_name = invite_payload.get("channel_name")
            if not channel_id and channels:
                channel_id = channels[0].get("id")
                channel_name = channels[0].get("name")

            conn = get_db_connection()
            upsert_channels(conn, guild_id, channels)
            conn.execute(
                """
                INSERT OR REPLACE INTO account_backup_guilds (
                    run_id, guild_id, guild_name, icon_url, owner, permissions,
                    channel_id, channel_name, invite_code, invite_url, invite_max_age,
                    invite_status, invite_error, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    guild_id,
                    guild_name,
                    guild.get("icon_url"),
                    1 if guild.get("owner") else 0,
                    str(guild.get("permissions") or ""),
                    channel_id,
                    channel_name,
                    invite_payload.get("code"),
                    invite_payload.get("url"),
                    invite_payload.get("max_age"),
                    invite_payload.get("status"),
                    invite_payload.get("error"),
                    json.dumps({"guild": guild, "invite": invite_payload.get("raw")}, ensure_ascii=False),
                ),
            )
            conn.commit()
            conn.close()

        error_detail = relationship_error if relationship_error else None
        update_account_backup_run_fields(
            run_id,
            status="completed",
            friend_count=len(friends),
            guild_count=len(guilds),
            processed_guilds=len(guilds),
            invite_count=invite_count,
            failed_invite_count=failed_invite_count,
            current_phase="Termine",
            current_detail=f"{len(friends)} ami(s), {len(guilds)} serveur(s), {invite_count} invitation(s).",
            error=error_detail,
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    except Exception as error:
        update_account_backup_run_fields(
            run_id,
            status="failed",
            current_phase="Erreur",
            current_detail=str(error)[:1000],
            error=str(error)[:1000],
            completed_at=datetime.now().isoformat(timespec="seconds"),
        )
    finally:
        update_account_backup_schedule_profile_from_run(run_id)
        with account_backup_lock:
            account_backup_active_runs.discard(run_id)


def account_backup_payload(run_id: str) -> dict[str, Any] | None:
    run = load_account_backup_run(run_id)
    if not run:
        return None
    return {
        "run": run,
        "friends": list_account_backup_friends(run_id),
        "guilds": list_account_backup_guilds(run_id),
    }


def account_backup_generated_at() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def account_backup_invite_advice(status: str | None, error: str | None = None) -> str | None:
    return {
        "created": "Lien cree pendant ce backup.",
        "existing": "Lien existant reutilise.",
        "existing_limited": "Limite d'invitations atteinte; un lien existant visible a ete reutilise.",
        "invite_limit": "Limite d'invitations atteinte. Supprime un ancien lien ou utilise un compte qui peut gerer les invitations.",
        "not_requested": "Creation d'invitations desactivee pour ce backup.",
        "no_permission": "Compte sans permission suffisante pour creer une invitation sur ce serveur.",
        "no_channel": "Aucun salon texte lisible pour creer une invitation.",
        "failed": error or "Creation d'invitation impossible.",
    }.get(status or "not_requested")


def account_backup_friend_export(friend: dict[str, Any]) -> dict[str, Any]:
    relationship_type = friend.get("relationship_type")
    return {
        "display_name": friend.get("display_name") or friend.get("username") or friend.get("user_id"),
        "username": friend.get("username"),
        "global_name": friend.get("global_name"),
        "user_id": friend.get("user_id"),
        "avatar_url": friend.get("avatar_url"),
        "relationship": {
            "type": relationship_type,
            "label": relationship_type_label(relationship_type),
        },
    }


def account_backup_guild_export(guild: dict[str, Any]) -> dict[str, Any]:
    invite_status = guild.get("invite_status") or "not_requested"
    invite_max_age = guild.get("invite_max_age")
    try:
        invite_max_age = int(invite_max_age) if invite_max_age is not None else None
    except (TypeError, ValueError):
        invite_max_age = None

    return {
        "name": guild.get("guild_name") or guild.get("guild_id"),
        "id": guild.get("guild_id"),
        "icon_url": guild.get("icon_url"),
        "owner": bool(guild.get("owner")),
        "permissions": guild.get("permissions"),
        "selected_channel": {
            "id": guild.get("channel_id"),
            "name": guild.get("channel_name"),
        },
        "invite": {
            "url": guild.get("invite_url"),
            "code": guild.get("invite_code"),
            "status": invite_status,
            "status_label": invite_status_label(invite_status),
            "max_age_seconds": invite_max_age,
            "never_expires": bool(guild.get("invite_url") and invite_max_age == 0),
            "error": guild.get("invite_error"),
            "advice": account_backup_invite_advice(invite_status, guild.get("invite_error")),
        },
    }


def account_backup_invite_status_counts(guilds: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for guild in guilds:
        status = str(guild.get("invite_status") or "not_requested")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def account_backup_export_payload(payload: dict[str, Any]) -> dict[str, Any]:
    run = payload["run"]
    friends = payload["friends"]
    guilds = payload["guilds"]
    friendly_friends = [account_backup_friend_export(friend) for friend in friends]
    friendly_servers = [account_backup_guild_export(guild) for guild in guilds]
    invite_links = [
        {
            "server_name": server["name"],
            "server_id": server["id"],
            "channel_name": server["selected_channel"]["name"],
            "url": server["invite"]["url"],
            "status": server["invite"]["status_label"],
            "never_expires": server["invite"]["never_expires"],
        }
        for server in friendly_servers
        if server["invite"]["url"]
    ]

    return {
        "schema": "account_backup.v2",
        "generated_at": account_backup_generated_at(),
        "summary": {
            "backup_id": run.get("id"),
            "status": run.get("status"),
            "status_label": account_backup_status_label(run.get("status")),
            "source": run.get("source") or "manual",
            "created_at": run.get("created_at"),
            "completed_at": run.get("completed_at"),
            "create_invites_requested": bool(run.get("create_invites")),
            "current_phase": run.get("current_phase"),
            "current_detail": run.get("current_detail"),
            "error": run.get("error"),
            "counts": {
                "friends": len(friendly_friends),
                "servers": len(friendly_servers),
                "invite_links_available": len(invite_links),
                "invite_failures": int(run.get("failed_invite_count") or 0),
                "invite_statuses": account_backup_invite_status_counts(guilds),
            },
        },
        "account": {
            "profile_id": run.get("profile_id"),
            "profile_name": run.get("profile_name"),
            "username": run.get("profile_username"),
            "user_id": run.get("profile_user_id"),
            "avatar_url": run.get("profile_avatar_url"),
        },
        "friends": friendly_friends,
        "servers": friendly_servers,
        "invite_links": invite_links,
        "notes": [
            "friends contient uniquement les relations de type ami sauvegardees par Discord.",
            "servers contient tous les serveurs visibles par le compte au moment du backup.",
            "technical.legacy_payload garde les donnees brutes pour debug ou import futur.",
        ],
        "technical": {
            "legacy_payload": payload,
        },
    }


def csv_bytes(rows: list[dict[str, Any]], headers: list[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key) for key in headers})
    return buffer.getvalue().encode("utf-8")


def build_account_backup_zip(payload: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    friendly_payload = account_backup_export_payload(payload)
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "backup.json",
            json.dumps(friendly_payload, ensure_ascii=False, indent=2),
        )
        archive.writestr(
            "technical/raw_backup.json",
            json.dumps(payload, ensure_ascii=False, indent=2),
        )
        archive.writestr(
            "friends.csv",
            csv_bytes(
                payload["friends"],
                ["user_id", "display_name", "username", "global_name", "discriminator", "relationship_type"],
            ),
        )
        archive.writestr(
            "guilds.csv",
            csv_bytes(
                payload["guilds"],
                ["guild_id", "guild_name", "owner", "channel_id", "channel_name", "invite_url", "invite_status", "invite_error"],
            ),
        )
    buffer.seek(0)
    return buffer.getvalue()


def sqlite_count(conn: sqlite3.Connection, table_name: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)


def export_directory_summary() -> dict[str, Any]:
    total_files = 0
    total_bytes = 0
    latest_file = None
    latest_mtime = 0.0

    if not os.path.isdir(EXPORT_DIR):
        return {
            "files": 0,
            "bytes": 0,
            "latest_file": None,
            "latest_mtime": None,
        }

    for root, _, files in os.walk(EXPORT_DIR):
        for filename in files:
            path = os.path.join(root, filename)
            try:
                stat = os.stat(path)
            except OSError:
                continue
            total_files += 1
            total_bytes += stat.st_size
            if stat.st_mtime > latest_mtime:
                latest_mtime = stat.st_mtime
                latest_file = os.path.relpath(path, EXPORT_DIR)

    return {
        "files": total_files,
        "bytes": total_bytes,
        "latest_file": latest_file,
        "latest_mtime": latest_mtime or None,
    }


def clear_export_directory() -> tuple[int, int]:
    root_dir = os.path.abspath(EXPORT_DIR)
    if not os.path.isdir(root_dir):
        return (0, 0)

    deleted_files = 0
    deleted_bytes = 0
    for root, dirs, files in os.walk(root_dir, topdown=False):
        for filename in files:
            path = os.path.abspath(os.path.join(root, filename))
            if not path.startswith(root_dir + os.sep):
                continue
            try:
                deleted_bytes += os.path.getsize(path)
                os.remove(path)
                deleted_files += 1
            except OSError:
                continue
        for dirname in dirs:
            path = os.path.abspath(os.path.join(root, dirname))
            if not path.startswith(root_dir + os.sep):
                continue
            try:
                os.rmdir(path)
            except OSError:
                continue
    return (deleted_files, deleted_bytes)


def data_management_snapshot() -> dict[str, Any]:
    conn = get_db_connection()
    stats = {
        "profiles": sqlite_count(conn, "profiles"),
        "guilds": sqlite_count(conn, "guilds"),
        "channels": sqlite_count(conn, "channels"),
        "messages": sqlite_count(conn, "messages"),
        "direct_messages": sqlite_count(conn, "direct_messages"),
        "deletion_tasks": sqlite_count(conn, "deletion_tasks"),
        "mutual_runs": sqlite_count(conn, "mutual_guild_runs"),
        "mutual_results": sqlite_count(conn, "mutual_guild_results"),
        "scraper_runs": sqlite_count(conn, "message_scraper_runs"),
        "scraper_results": sqlite_count(conn, "message_scraper_results"),
        "account_backup_runs": sqlite_count(conn, "account_backup_runs"),
        "account_backup_friends": sqlite_count(conn, "account_backup_friends"),
        "account_backup_guilds": sqlite_count(conn, "account_backup_guilds"),
        "account_backup_schedule_events": sqlite_count(conn, "account_backup_schedule_events"),
        "selfbot_tool_runs": sqlite_count(conn, "selfbot_tool_runs"),
        "selfbot_spy_rules": sqlite_count(conn, "selfbot_spy_rules"),
        "selfbot_spy_events": sqlite_count(conn, "selfbot_spy_events"),
        "selfbot_antigroup_settings": sqlite_count(conn, "selfbot_antigroup_settings"),
        "server_backups": sqlite_count(conn, "server_backups"),
        "autobumper_events": sqlite_count(conn, "autobumper_events"),
        "scraper_payload_bytes": int(
            conn.execute(
                """
                SELECT COALESCE(SUM(
                    LENGTH(COALESCE(content, '')) +
                    LENGTH(COALESCE(raw_json, ''))
                ), 0)
                FROM message_scraper_results
                """
            ).fetchone()[0]
            or 0
        ),
    }
    scraper_runs = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, COUNT(m.id) AS stored_messages
            FROM message_scraper_runs r
            LEFT JOIN message_scraper_results m ON m.run_id = r.id
            GROUP BY r.id
            ORDER BY r.created_at DESC
            LIMIT 20
            """
        ).fetchall()
    )
    mutual_runs = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, COUNT(m.id) AS stored_guilds
            FROM mutual_guild_runs r
            LEFT JOIN mutual_guild_results m ON m.run_id = r.id
            GROUP BY r.id
            ORDER BY r.created_at DESC
            LIMIT 20
            """
        ).fetchall()
    )
    deletion_runs = rows_to_dicts(
        conn.execute(
            """
            SELECT d.*, p.name AS profile_name, g.name AS guild_name
            FROM deletion_tasks d
            LEFT JOIN profiles p ON p.id = d.profile_id
            LEFT JOIN guilds g ON g.id = d.guild_id
            ORDER BY d.created_at DESC
            LIMIT 12
            """
        ).fetchall()
    )
    selfbot_runs = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username
            FROM selfbot_tool_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            ORDER BY r.created_at DESC
            LIMIT 20
            """
        ).fetchall()
    )
    server_backup_rows = list_server_backups()
    conn.close()

    reconciled_scraper_runs = []
    for run in scraper_runs:
        reconciled = reconcile_message_scraper_run(run) or run
        if "stored_messages" not in reconciled:
            reconciled["stored_messages"] = run.get("stored_messages") or 0
        reconciled_scraper_runs.append(reconciled)

    return {
        "stats": stats,
        "exports": export_directory_summary(),
        "scraper_runs": reconciled_scraper_runs,
        "mutual_runs": mutual_runs,
        "deletion_runs": deletion_runs,
        "selfbot_runs": selfbot_runs,
        "server_backups": server_backup_rows,
    }


def tool_worker_active(kind: str, run_id: str | None = None) -> bool:
    if kind == "scraper":
        with message_scraper_lock:
            return bool(message_scraper_active_runs if run_id is None else run_id in message_scraper_active_runs)
    if kind == "mutual":
        with mutual_guild_lock:
            return bool(mutual_guild_active_runs if run_id is None else run_id in mutual_guild_active_runs)
    if kind == "account_backup":
        with account_backup_lock:
            return bool(account_backup_active_runs if run_id is None else run_id in account_backup_active_runs)
    if kind == "selfbot_tool":
        with selfbot_tool_lock:
            return bool(selfbot_tool_active_runs if run_id is None else run_id in selfbot_tool_active_runs)
    if kind == "deletion":
        with task_lock:
            if run_id is not None:
                task = deletion_tasks.get(run_id)
                return bool(task and task.get("status") in {"pending", "running"})
            return any(task.get("status") in {"pending", "running"} for task in deletion_tasks.values())
    return False


@app.route("/data-management", methods=["GET", "POST"])
def data_management():
    if request.method == "POST":
        action = request.form.get("action") or ""
        conn = get_db_connection()
        try:
            if action == "delete_scraper_run":
                run_id = (request.form.get("run_id") or "").strip()
                if tool_worker_active("scraper", run_id):
                    flash("Ce scrape est encore actif, suppression locale bloquee.", "error")
                else:
                    deleted = conn.execute("DELETE FROM message_scraper_results WHERE run_id = ?", (run_id,)).rowcount
                    conn.execute("DELETE FROM message_scraper_runs WHERE id = ?", (run_id,))
                    conn.commit()
                    flash(f"Run Message Scrapper supprime localement ({deleted} message(s)).", "success")
            elif action == "delete_mutual_run":
                run_id = (request.form.get("run_id") or "").strip()
                if tool_worker_active("mutual", run_id):
                    flash("Ce scan Mutual Guild est encore actif, suppression locale bloquee.", "error")
                else:
                    conn.execute(
                        """
                        DELETE FROM message_scraper_results
                        WHERE run_id IN (
                            SELECT id FROM message_scraper_runs WHERE mutual_run_id = ?
                        )
                        """,
                        (run_id,),
                    )
                    conn.execute("DELETE FROM message_scraper_runs WHERE mutual_run_id = ?", (run_id,))
                    deleted = conn.execute("DELETE FROM mutual_guild_results WHERE run_id = ?", (run_id,)).rowcount
                    conn.execute("DELETE FROM mutual_guild_runs WHERE id = ?", (run_id,))
                    conn.commit()
                    flash(f"Scan Mutual Guild supprime localement ({deleted} serveur(s)).", "success")
            elif action == "purge_scraper":
                if tool_worker_active("scraper"):
                    flash("Un scrape est actif, purge Message Scrapper bloquee.", "error")
                else:
                    deleted = conn.execute("DELETE FROM message_scraper_results").rowcount
                    conn.execute("DELETE FROM message_scraper_runs")
                    conn.commit()
                    flash(f"Donnees Message Scrapper purgees ({deleted} message(s)).", "success")
            elif action == "purge_mutual":
                if tool_worker_active("mutual") or tool_worker_active("scraper"):
                    flash("Un outil d'investigation est actif, purge Mutual Guild bloquee.", "error")
                else:
                    conn.execute("DELETE FROM message_scraper_results")
                    conn.execute("DELETE FROM message_scraper_runs")
                    deleted = conn.execute("DELETE FROM mutual_guild_results").rowcount
                    conn.execute("DELETE FROM mutual_guild_runs")
                    conn.commit()
                    flash(f"Donnees Mutual Guild purgees ({deleted} serveur(s)); les scrapes lies ont aussi ete retires.", "success")
            elif action == "purge_cache":
                if tool_worker_active("scraper") or tool_worker_active("mutual"):
                    flash("Un outil lit encore le cache Discord, purge bloquee.", "error")
                else:
                    conn.execute("DELETE FROM messages")
                    conn.execute("DELETE FROM direct_messages")
                    conn.execute("DELETE FROM channels")
                    conn.execute("DELETE FROM guilds")
                    conn.commit()
                    flash("Cache local serveurs/salons/DM purge.", "success")
            elif action == "purge_deletion_logs":
                if tool_worker_active("deletion"):
                    flash("Une suppression/export est active, purge des taches bloquee.", "error")
                else:
                    deleted = conn.execute("DELETE FROM deletion_tasks").rowcount
                    conn.commit()
                    flash(f"Historique suppression/export purge ({deleted} tache(s)).", "success")
            elif action == "purge_account_backup":
                if tool_worker_active("account_backup"):
                    flash("Un backup compte est actif, purge bloquee.", "error")
                else:
                    deleted_friends = conn.execute("DELETE FROM account_backup_friends").rowcount
                    deleted_guilds = conn.execute("DELETE FROM account_backup_guilds").rowcount
                    deleted_runs = conn.execute("DELETE FROM account_backup_runs").rowcount
                    conn.execute(
                        """
                        UPDATE account_backup_schedule_profiles
                        SET last_run_id = NULL,
                            last_run_at = NULL,
                            next_run_at = NULL,
                            last_status = 'waiting',
                            last_error = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        """
                    )
                    conn.commit()
                    flash(
                        f"Backups comptes purges ({deleted_runs} run(s), {deleted_friends} ami(s), {deleted_guilds} serveur(s)).",
                        "success",
                    )
            elif action == "delete_selfbot_run":
                run_id = (request.form.get("run_id") or "").strip()
                if tool_worker_active("selfbot_tool", run_id):
                    flash("Ce run Selfbot suite est encore actif, suppression locale bloquee.", "error")
                else:
                    deleted = conn.execute("DELETE FROM selfbot_tool_runs WHERE id = ?", (run_id,)).rowcount
                    conn.commit()
                    flash(f"Run Selfbot suite supprime ({deleted}).", "success")
            elif action == "purge_selfbot":
                if tool_worker_active("selfbot_tool"):
                    flash("Un run Selfbot suite est actif, purge bloquee.", "error")
                else:
                    deleted_events = conn.execute("DELETE FROM selfbot_spy_events").rowcount
                    deleted_rules = conn.execute("DELETE FROM selfbot_spy_rules").rowcount
                    deleted_runs = conn.execute("DELETE FROM selfbot_tool_runs").rowcount
                    conn.execute("DELETE FROM selfbot_antigroup_settings")
                    conn.commit()
                    flash(
                        f"Donnees Selfbot suite purgees ({deleted_runs} run(s), {deleted_rules} regle(s), {deleted_events} evenement(s)).",
                        "success",
                    )
            elif action == "purge_server_backups":
                if tool_worker_active("selfbot_tool"):
                    flash("Un run Selfbot suite est actif, purge backups serveur bloquee.", "error")
                else:
                    deleted = conn.execute("DELETE FROM server_backups").rowcount
                    conn.commit()
                    flash(f"Backups serveur purges ({deleted}).", "success")
            elif action == "purge_service_logs":
                deleted_bump = conn.execute("DELETE FROM autobumper_events").rowcount
                deleted_backup_schedule = conn.execute("DELETE FROM account_backup_schedule_events").rowcount
                conn.commit()
                flash(f"Logs services purges ({deleted_bump + deleted_backup_schedule} evenement(s)).", "success")
            elif action == "purge_exports":
                files, size = clear_export_directory()
                flash(f"Exports locaux supprimes ({files} fichier(s), {format_bytes(size)}).", "success")
            else:
                flash("Action data inconnue.", "error")
        except sqlite3.Error as error:
            conn.rollback()
            flash(f"Erreur base locale: {error}", "error")
        finally:
            conn.close()
        return redirect(url_for("data_management"))

    return render_template("data_management.html", snapshot=data_management_snapshot())


@app.route("/tools/selfbot-suite", methods=["GET", "POST"])
def selfbot_suite():
    profiles = load_profiles(include_tokens=False)
    profile_ids = {int(profile["id"]) for profile in profiles}
    valid_tabs = {"account", "spy", "messages", "backups", "runs"}

    def selected_profile_id(field_name: str = "profile_id") -> int | None:
        profile_id = parse_int(request.form.get(field_name), default=0, minimum=0, maximum=999999999)
        return profile_id if profile_id in profile_ids else None

    def suite_redirect(tab: str = "account", **values: Any):
        return redirect(url_for("selfbot_suite", tab=tab, **values))

    if request.method == "POST":
        action = request.form.get("action") or ""

        if action == "hypesquad":
            profile_id = selected_profile_id()
            house = (request.form.get("house") or "").strip()
            profile = load_profile_token(profile_id)
            if not profile:
                flash("Selectionne un profil valide pour HypeSquad.", "error")
                return suite_redirect("account")
            ok, message = change_hypesquad_house(profile["token"], house)
            flash(message, "success" if ok else "error")
            return suite_redirect("account")

        if action == "save_antigroup":
            selected_ids = {
                parse_int(raw_id, default=0, minimum=0, maximum=999999999)
                for raw_id in request.form.getlist("antigroup_profile_ids")
            }
            selected_ids = {profile_id for profile_id in selected_ids if profile_id in profile_ids}
            conn = get_db_connection()
            conn.execute("UPDATE selfbot_antigroup_settings SET enabled = 0, updated_at = CURRENT_TIMESTAMP")
            for profile_id in selected_ids:
                conn.execute(
                    """
                    INSERT INTO selfbot_antigroup_settings (profile_id, enabled, updated_at)
                    VALUES (?, 1, CURRENT_TIMESTAMP)
                    ON CONFLICT(profile_id) DO UPDATE SET
                        enabled = 1,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (profile_id,),
                )
            conn.commit()
            conn.close()
            flash(f"Antigroup configure pour {len(selected_ids)} compte(s).", "success")
            return suite_redirect("account")

        if action == "scan_antigroup_now":
            threading.Thread(target=process_selfbot_antigroup_once, daemon=True, name="selfbot-antigroup-manual").start()
            flash("Scan antigroup lance en arriere-plan.", "success")
            return suite_redirect("account")

        if action == "add_spy_rule":
            profile_id = selected_profile_id()
            profile = load_profile_token(profile_id)
            guild_id = (request.form.get("guild_id") or "").strip()
            channel_id = (request.form.get("channel_id") or "").strip()
            notify_channel_id = (request.form.get("notify_channel_id") or "").strip()
            keywords = parse_spy_keywords(request.form.get("keywords"))

            if not profile:
                flash("Selectionne un profil valide pour spy.", "error")
                return suite_redirect("spy")
            if not re.fullmatch(r"\d{15,25}", guild_id):
                flash("ID serveur invalide pour spy.", "error")
                return suite_redirect("spy")
            if channel_id and not re.fullmatch(r"\d{15,25}", channel_id):
                flash("ID salon invalide pour spy.", "error")
                return suite_redirect("spy")
            if notify_channel_id and not re.fullmatch(r"\d{15,25}", notify_channel_id):
                flash("ID salon notification invalide pour spy.", "error")
                return suite_redirect("spy")
            if not keywords:
                flash("Ajoute au moins un mot-cle pour spy.", "error")
                return suite_redirect("spy")

            guilds = get_user_guilds(profile["token"], profile.get("user_id"))
            guild_name = next((guild.get("name") for guild in guilds if str(guild.get("id")) == guild_id), guild_id)
            channel_name = None
            if channel_id:
                channel_detail = get_channel_detail(profile["token"], channel_id)
                channel_name = (channel_detail or {}).get("name") or channel_id

            conn = get_db_connection()
            conn.execute(
                """
                INSERT INTO selfbot_spy_rules (
                    profile_id, guild_id, guild_name, channel_id, channel_name,
                    keywords, notify_channel_id, enabled
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    profile_id,
                    guild_id,
                    guild_name,
                    channel_id or None,
                    channel_name,
                    json.dumps(keywords, ensure_ascii=False),
                    notify_channel_id or None,
                    1 if request.form.get("enabled", "on") == "on" else 0,
                ),
            )
            conn.commit()
            conn.close()
            flash("Regle spy ajoutee.", "success")
            return suite_redirect("spy")

        if action in {"toggle_spy_rule", "delete_spy_rule"}:
            rule_id = parse_int(request.form.get("rule_id"), default=0, minimum=0, maximum=999999999)
            conn = get_db_connection()
            if action == "toggle_spy_rule":
                conn.execute(
                    """
                    UPDATE selfbot_spy_rules
                    SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (rule_id,),
                )
                flash("Regle spy mise a jour.", "success")
            else:
                conn.execute("DELETE FROM selfbot_spy_events WHERE rule_id = ?", (rule_id,))
                conn.execute("DELETE FROM selfbot_spy_rules WHERE id = ?", (rule_id,))
                flash("Regle spy supprimee.", "success")
            conn.commit()
            conn.close()
            return suite_redirect("spy")

        if action == "scan_spy_now":
            threading.Thread(target=process_selfbot_spy_once, daemon=True, name="selfbot-spy-manual").start()
            flash("Scan spy lance en arriere-plan.", "success")
            return suite_redirect("spy")

        if action == "dmall":
            profile_id = selected_profile_id()
            message = (request.form.get("message") or "").strip()
            include_friends = request.form.get("include_friends") == "on"
            include_dms = request.form.get("include_dms") == "on"
            delay = parse_float(request.form.get("delay"), default=1.0, minimum=0.0, maximum=120.0)
            if not load_profile_token(profile_id):
                flash("Selectionne un profil valide pour DM all.", "error")
                return suite_redirect("messages")
            if not message:
                flash("Message DM all vide.", "error")
                return suite_redirect("messages")
            if not include_friends and not include_dms:
                flash("Selectionne au moins amis ou conversations DM.", "error")
                return suite_redirect("messages")
            run_id = create_selfbot_tool_run(
                profile_id,
                "dmall",
                {"message_preview": truncate_log_content(message, 500), "include_friends": include_friends, "include_dms": include_dms},
            )
            start_selfbot_tool_thread(run_id, run_dmall_task, message, include_friends, include_dms, delay)
            flash("DM all lance en arriere-plan.", "success")
            return suite_redirect("runs", run_id=run_id)

        if action == "delallfriends":
            profile_id = selected_profile_id()
            delay = parse_float(request.form.get("delay"), default=0.8, minimum=0.0, maximum=120.0)
            if not load_profile_token(profile_id):
                flash("Selectionne un profil valide pour supprimer les amis.", "error")
                return suite_redirect("messages")
            run_id = create_selfbot_tool_run(profile_id, "delallfriends")
            start_selfbot_tool_thread(run_id, run_delallfriends_task, delay)
            flash("Suppression des amis lancee en arriere-plan.", "success")
            return suite_redirect("runs", run_id=run_id)

        if action == "bcreate":
            profile_id = selected_profile_id()
            guild_id = (request.form.get("guild_id") or "").strip()
            backup_name = (request.form.get("backup_name") or "").strip()
            if not load_profile_token(profile_id):
                flash("Selectionne un profil valide pour bcreate.", "error")
                return suite_redirect("backups")
            if not re.fullmatch(r"\d{15,25}", guild_id):
                flash("ID serveur invalide pour bcreate.", "error")
                return suite_redirect("backups")
            run_id = create_selfbot_tool_run(profile_id, "bcreate", {"guild_id": guild_id, "backup_name": backup_name})
            start_selfbot_tool_thread(run_id, run_bcreate_task, guild_id, backup_name or None)
            flash("Backup serveur lance en arriere-plan.", "success")
            return suite_redirect("runs", run_id=run_id)

        if action == "bload":
            profile_id = selected_profile_id()
            backup_id = (request.form.get("backup_id") or "").strip()
            target_guild_id = (request.form.get("target_guild_id") or "").strip()
            clear_channels = request.form.get("clear_channels") == "on"
            restore_guild_name = request.form.get("restore_guild_name") == "on"
            if not load_profile_token(profile_id):
                flash("Selectionne un profil valide pour bload.", "error")
                return suite_redirect("backups")
            if not load_server_backup(backup_id):
                flash("Backup serveur introuvable.", "error")
                return suite_redirect("backups")
            if not re.fullmatch(r"\d{15,25}", target_guild_id):
                flash("ID serveur cible invalide pour bload.", "error")
                return suite_redirect("backups")
            run_id = create_selfbot_tool_run(
                profile_id,
                "bload",
                {"backup_id": backup_id, "target_guild_id": target_guild_id, "clear_channels": clear_channels},
            )
            start_selfbot_tool_thread(run_id, run_bload_task, backup_id, target_guild_id, clear_channels, restore_guild_name)
            flash("Deploy backup lance en arriere-plan.", "success")
            return suite_redirect("runs", run_id=run_id)

        if action == "delete_server_backup":
            backup_id = (request.form.get("backup_id") or "").strip()
            conn = get_db_connection()
            deleted = conn.execute("DELETE FROM server_backups WHERE id = ?", (backup_id,)).rowcount
            conn.commit()
            conn.close()
            flash("Backup serveur supprime." if deleted else "Backup serveur introuvable.", "success" if deleted else "error")
            return suite_redirect("backups")

        flash("Action selfbot inconnue.", "error")
        return suite_redirect("account")

    selected_run = load_selfbot_tool_run((request.args.get("run_id") or "").strip())
    if selected_run:
        selected_run["result"] = selfbot_tool_result_payload(selected_run)
        selected_run["result_pretty"] = json.dumps(selected_run["result"], ensure_ascii=False, indent=2)
    active_selfbot_tab = (request.args.get("tab") or ("runs" if selected_run else "account")).strip()
    if active_selfbot_tab not in valid_tabs:
        active_selfbot_tab = "runs" if selected_run else "account"

    return render_template(
        "selfbot_suite.html",
        profiles=profiles,
        spy_rules=list_selfbot_spy_rules(),
        spy_events=list_selfbot_spy_events(),
        antigroup_settings=list_selfbot_antigroup_settings(),
        runs=list_selfbot_tool_runs(),
        selected_run=selected_run,
        active_selfbot_tab=active_selfbot_tab,
        server_backups=list_server_backups(),
        worker_enabled=SELFBOT_TOOLS_WORKER_ENABLED,
        spy_interval=int(SELFBOT_TOOLS_SPY_INTERVAL),
        antigroup_interval=int(SELFBOT_TOOLS_ANTIGROUP_INTERVAL),
        spy_lookback=SELFBOT_SPY_MESSAGE_LOOKBACK,
        spy_max_channels=SELFBOT_SPY_MAX_CHANNELS,
    )


@app.route("/service_logs")
def service_logs():
    conn = get_db_connection()
    autobump_events = rows_to_dicts(
        conn.execute(
            """
            SELECT e.*, t.guild_name, t.channel_name, p.name AS profile_name, p.username AS profile_username
            FROM autobumper_events e
            LEFT JOIN autobumper_targets t ON t.id = e.target_id
            LEFT JOIN profiles p ON p.id = e.profile_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT 60
            """
        ).fetchall()
    )
    autobump_targets = rows_to_dicts(
        conn.execute(
            """
            SELECT t.*, p.name AS profile_name, p.username AS profile_username
            FROM autobumper_targets t
            LEFT JOIN profiles p ON p.id = t.profile_id
            ORDER BY t.enabled DESC, COALESCE(CAST(t.next_bump_at AS REAL), 9999999999), t.updated_at DESC
            LIMIT 40
            """
        ).fetchall()
    )
    deletion_rows = conn.execute(
        """
        SELECT d.*, p.name AS profile_name, g.name AS guild_name
        FROM deletion_tasks d
        LEFT JOIN profiles p ON p.id = d.profile_id
        LEFT JOIN guilds g ON g.id = d.guild_id
        ORDER BY d.created_at DESC
        LIMIT 30
        """
    ).fetchall()
    scraper_runs = rows_to_dicts(
        conn.execute(
            """
            SELECT *
            FROM message_scraper_runs
            ORDER BY created_at DESC
            LIMIT 30
            """
        ).fetchall()
    )
    backup_schedule_events = rows_to_dicts(
        conn.execute(
            """
            SELECT e.*, p.name AS profile_name, p.username AS profile_username
            FROM account_backup_schedule_events e
            LEFT JOIN profiles p ON p.id = e.profile_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT 60
            """
        ).fetchall()
    )
    selfbot_runs = rows_to_dicts(
        conn.execute(
            """
            SELECT r.*, p.name AS profile_name, p.username AS profile_username
            FROM selfbot_tool_runs r
            LEFT JOIN profiles p ON p.id = r.profile_id
            ORDER BY r.created_at DESC
            LIMIT 60
            """
        ).fetchall()
    )
    selfbot_spy_events = rows_to_dicts(
        conn.execute(
            """
            SELECT e.*, r.guild_name, r.channel_name, p.name AS profile_name, p.username AS profile_username
            FROM selfbot_spy_events e
            LEFT JOIN selfbot_spy_rules r ON r.id = e.rule_id
            LEFT JOIN profiles p ON p.id = e.profile_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT 60
            """
        ).fetchall()
    )
    conn.close()

    deletion_logs = []
    for row in deletion_rows:
        task = task_from_row(row)
        if not task:
            continue
        deletion_logs.append(
            {
                "id": row["id"],
                "profile_name": row["profile_name"],
                "guild_name": row["guild_name"],
                "status": task["status"],
                "total_messages": task["total_messages"],
                "deleted_messages": task["deleted_messages"],
                "failed_messages": task["failed_messages"],
                "skipped_messages": task["skipped_messages"],
                "export_status": task["export_status"],
                "created_at": row["created_at"],
                "end_time": task["end_time"],
                "error": task["error"],
            }
        )

    unified_logs: list[dict[str, Any]] = []

    def log_timestamp(value: str | None) -> float:
        if not value:
            return 0
        try:
            raw_value = str(value)
            if raw_value.endswith("Z"):
                return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).timestamp()
            if re.fullmatch(r".*[+-]\d{2}:\d{2}$", raw_value):
                return datetime.fromisoformat(raw_value).timestamp()
            return datetime.fromisoformat(raw_value).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            return 0

    for event in autobump_events:
        unified_logs.append(
            {
                "created_at": event.get("created_at"),
                "sort_ts": log_timestamp(event.get("created_at")),
                "service": "auto-bump",
                "status": autobumper_status_label(event.get("status")),
                "badge_class": autobumper_status_badge_class(event.get("status"), True),
                "context": event.get("guild_name") or event.get("guild_id") or "Serveur inconnu",
                "profile": event.get("profile_name") or event.get("profile_username") or event.get("profile_id") or "",
                "target": event.get("channel_name") or event.get("channel_id") or "",
                "message": event.get("message") or "",
                "link_url": None,
            }
        )

    for task in deletion_logs:
        unified_logs.append(
            {
                "created_at": task.get("created_at"),
                "sort_ts": log_timestamp(task.get("created_at")),
                "service": "suppression/export",
                "status": status_label(task["status"]),
                "badge_class": status_badge_class(task["status"]),
                "context": task.get("guild_name") or "Messages prives",
                "profile": task.get("profile_name") or "",
                "target": task["id"],
                "message": (
                    f"{task['deleted_messages']}/{task['total_messages']} supprime(s), "
                    f"{task['failed_messages']} echec(s), export {export_status_label(task['export_status'])}."
                ),
                "link_url": url_for("deletion_status", task_id=task["id"]),
            }
        )

    for run in scraper_runs:
        unified_logs.append(
            {
                "created_at": run.get("created_at"),
                "sort_ts": log_timestamp(run.get("created_at")),
                "service": "message-scrapper",
                "status": message_scraper_status_label(run.get("status")),
                "badge_class": message_scraper_status_badge_class(run.get("status")),
                "context": run.get("target_user_id") or "Cible inconnue",
                "profile": "",
                "target": run.get("id"),
                "message": (
                    f"{run.get('total_messages') or 0} message(s), "
                    f"{run.get('completed_guilds') or 0}/{run.get('total_guilds') or 0} serveur(s)."
                ),
                "link_url": url_for(
                    "message_scraper_tool",
                    mutual_run_id=run.get("mutual_run_id") or "",
                    run_id=run.get("id"),
                ),
            }
        )

    for event in backup_schedule_events:
        unified_logs.append(
            {
                "created_at": event.get("created_at"),
                "sort_ts": log_timestamp(event.get("created_at")),
                "service": "backup-compte",
                "status": account_backup_schedule_status_label(event.get("status")),
                "badge_class": account_backup_schedule_status_badge_class(event.get("status")),
                "context": event.get("run_id") or "Routine",
                "profile": event.get("profile_name") or event.get("profile_username") or event.get("profile_id") or "",
                "target": "",
                "message": event.get("message") or "",
                "link_url": url_for("account_backup_settings"),
            }
        )

    for run in selfbot_runs:
        unified_logs.append(
            {
                "created_at": run.get("created_at"),
                "sort_ts": log_timestamp(run.get("created_at")),
                "service": f"selfbot-{run.get('tool') or 'tool'}",
                "status": selfbot_tool_status_label(run.get("status")),
                "badge_class": selfbot_tool_status_badge_class(run.get("status")),
                "context": run.get("id") or "",
                "profile": run.get("profile_name") or run.get("profile_username") or run.get("profile_id") or "",
                "target": selfbot_tool_label(run.get("tool")),
                "message": (
                    f"{run.get('success_count') or 0} succes, {run.get('fail_count') or 0} echec(s), "
                    f"{run.get('current_detail') or ''}"
                ),
                "link_url": url_for("selfbot_suite", tab="runs", run_id=run.get("id") or ""),
            }
        )

    for event in selfbot_spy_events:
        unified_logs.append(
            {
                "created_at": event.get("created_at"),
                "sort_ts": log_timestamp(event.get("created_at")),
                "service": "selfbot-spy",
                "status": "Match",
                "badge_class": "text-bg-primary",
                "context": event.get("guild_name") or event.get("guild_id") or "",
                "profile": event.get("profile_name") or event.get("profile_username") or event.get("profile_id") or "",
                "target": event.get("channel_name") or event.get("channel_id") or "",
                "message": f"{event.get('keyword') or ''} · {event.get('author_name') or event.get('author_id') or ''} · {event.get('content') or ''}",
                "link_url": event.get("jump_url"),
            }
        )

    unified_logs.sort(key=lambda item: (item["sort_ts"], item["service"], item["context"]))

    return render_template(
        "service_logs.html",
        autobump_events=autobump_events,
        autobump_targets=[normalize_autobumper_target(target) for target in autobump_targets],
        deletion_logs=deletion_logs,
        scraper_runs=scraper_runs,
        backup_schedule_events=backup_schedule_events,
        selfbot_runs=selfbot_runs,
        selfbot_spy_events=selfbot_spy_events,
        unified_logs=unified_logs,
    )


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
        conn.execute("DELETE FROM autobumper_events WHERE profile_id = ?", (profile_id,))
        conn.execute("DELETE FROM autobumper_targets WHERE profile_id = ?", (profile_id,))
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


@app.route("/autobumper/<int:profile_id>", methods=["GET", "POST"])
def autobumper(profile_id: int):
    profile = load_profile(profile_id)
    if not profile:
        flash("Profil non trouve.", "error")
        return redirect(url_for("index"))

    guilds = get_user_guilds(profile["token"], profile.get("user_id"))
    conn = get_db_connection()
    upsert_guilds(conn, guilds)
    conn.commit()
    conn.close()

    guilds_by_id = {guild["id"]: guild for guild in guilds}

    if request.method == "POST":
        guild_id = (request.form.get("guild_id") or "").strip()
        channel_id = (request.form.get("channel_id") or "").strip()
        custom_message = (request.form.get("custom_message") or "").strip()
        delay_seconds = 0
        enabled = request.form.get("enabled") == "on"

        guild = guilds_by_id.get(guild_id)
        if not guild:
            flash("Serveur introuvable pour ce profil.", "error")
            return redirect(url_for("autobumper", profile_id=profile_id))

        channels_list = get_guild_channels(profile["token"], guild_id)
        conn = get_db_connection()
        upsert_channels(conn, guild_id, channels_list)
        conn.commit()
        conn.close()

        channel = next((item for item in channels_list if item["id"] == channel_id), None)
        if not channel:
            flash("Salon texte introuvable pour ce serveur.", "error")
            return redirect(url_for("autobumper", profile_id=profile_id, guild_id=guild_id))

        save_autobumper_target(profile_id, guild, channel, custom_message, enabled, delay_seconds)
        flash("Configuration Auto Bump enregistree.", "success")
        return redirect(url_for("autobumper", profile_id=profile_id, guild_id=guild_id))

    selected_guild_id = (request.args.get("guild_id") or "").strip()
    if selected_guild_id not in guilds_by_id:
        selected_guild_id = ""

    selected_guild = guilds_by_id.get(selected_guild_id)
    channels_list: list[dict[str, Any]] = []
    if selected_guild:
        channels_list = get_guild_channels(profile["token"], selected_guild_id)
        conn = get_db_connection()
        upsert_channels(conn, selected_guild_id, channels_list)
        conn.commit()
        conn.close()

    targets = list_autobumper_targets(profile_id)
    selected_target = next((target for target in targets if target["guild_id"] == selected_guild_id), None)

    return render_template(
        "autobumper.html",
        profile=profile,
        guilds=guilds,
        selected_guild=selected_guild,
        selected_guild_id=selected_guild_id,
        channels=channels_list,
        targets=targets,
        selected_target=selected_target,
        events=list_autobumper_events(profile_id),
        stats=autobumper_stats(profile_id),
        bump_interval_hours=AUTOBUMPER_BUMP_INTERVAL / 3600,
        check_interval_seconds=int(AUTOBUMPER_CHECK_INTERVAL),
        max_retries=AUTOBUMPER_MAX_RETRIES,
    )


@app.route("/autobumper/<int:profile_id>/targets/<int:target_id>/toggle", methods=["POST"])
def toggle_autobumper_target(profile_id: int, target_id: int):
    target = load_autobumper_target(profile_id, target_id)
    if not target:
        flash("Configuration Auto Bump introuvable.", "error")
        return redirect(url_for("autobumper", profile_id=profile_id))

    enabled = not target["enabled"]
    next_bump_at = (
        next_autobumper_schedule_at(target)
        if enabled
        else target.get("next_bump_at") or current_timestamp()
    )
    conn = get_db_connection()
    conn.execute(
        """
        UPDATE autobumper_targets
        SET enabled = ?, next_bump_at = ?, last_status = ?, last_error = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND profile_id = ?
        """,
        (1 if enabled else 0, next_bump_at, "queued" if enabled else "paused", target_id, profile_id),
    )
    conn.commit()
    conn.close()

    flash("Auto Bump active." if enabled else "Auto Bump mis en pause.", "success")
    return redirect(url_for("autobumper", profile_id=profile_id, guild_id=target["guild_id"]))


@app.route("/autobumper/<int:profile_id>/targets/<int:target_id>/delay", methods=["POST"])
def update_autobumper_target_delay(profile_id: int, target_id: int):
    target = load_autobumper_target(profile_id, target_id)
    if not target:
        flash("Configuration Auto Bump introuvable.", "error")
        return redirect(url_for("autobumper", profile_id=profile_id))

    next_bump_at = next_autobumper_schedule_at(target) if target["enabled"] else target.get("next_bump_at")

    conn = get_db_connection()
    conn.execute(
        """
        UPDATE autobumper_targets
        SET delay_seconds = ?, next_bump_at = ?, last_status = ?, last_error = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND profile_id = ?
        """,
        (
            0,
            next_bump_at,
            "queued" if target["enabled"] else "paused",
            target_id,
            profile_id,
        ),
    )
    record_autobumper_event(
        conn,
        target,
        "queued" if target["enabled"] else "paused",
        "Delai Auto Bump desactive.",
    )
    conn.commit()
    conn.close()

    flash("Delai Auto Bump desactive.", "success")
    return redirect(url_for("autobumper", profile_id=profile_id, guild_id=target["guild_id"]))


@app.route("/autobumper/<int:profile_id>/targets/<int:target_id>/force", methods=["POST"])
def force_autobumper_target(profile_id: int, target_id: int):
    target = load_autobumper_target(profile_id, target_id)
    if not target:
        flash("Configuration Auto Bump introuvable.", "error")
        return redirect(url_for("autobumper", profile_id=profile_id))

    now = current_timestamp()
    last_bump_at = float(target["last_bump_at"] or 0)
    next_allowed_at = last_bump_at + AUTOBUMPER_BUMP_INTERVAL if last_bump_at else now
    if last_bump_at and last_bump_at + AUTOBUMPER_BUMP_INTERVAL > now:
        conn = get_db_connection()
        conn.execute(
            """
            UPDATE autobumper_targets
            SET enabled = 1, next_bump_at = ?, last_status = 'queued', updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND profile_id = ?
            """,
            (next_allowed_at, target_id, profile_id),
        )
        conn.commit()
        conn.close()
        flash("Bump recent: le prochain passage est conserve pour respecter l'intervalle minimum.", "warning")
        return redirect(url_for("autobumper", profile_id=profile_id, guild_id=target["guild_id"]))

    conn = get_db_connection()
    conn.execute(
        """
        UPDATE autobumper_targets
        SET enabled = 1, next_bump_at = ?, last_status = 'queued', updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND profile_id = ?
        """,
        (now, target_id, profile_id),
    )
    conn.commit()
    conn.close()

    thread = threading.Thread(target=run_autobumper_target, args=(target_id, True), daemon=True)
    thread.start()
    flash("Bump lance en arriere-plan.", "success")
    return redirect(url_for("autobumper", profile_id=profile_id, guild_id=target["guild_id"]))


@app.route("/autobumper/<int:profile_id>/targets/<int:target_id>/sync", methods=["POST"])
def sync_autobumper_target(profile_id: int, target_id: int):
    target = load_autobumper_target(profile_id, target_id)
    if not target:
        flash("Configuration Auto Bump introuvable.", "error")
        return redirect(url_for("autobumper", profile_id=profile_id))

    conn = get_db_connection()
    conn.execute(
        """
        UPDATE autobumper_targets
        SET enabled = 1, last_status = 'queued', last_error = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = ? AND profile_id = ?
        """,
        (target_id, profile_id),
    )
    conn.commit()
    conn.close()

    thread = threading.Thread(target=run_autobumper_target, args=(target_id, True), daemon=True)
    thread.start()
    flash("Synchronisation lancee: l'app va lire la reponse Disboard et ajuster le prochain bump.", "success")
    return redirect(url_for("autobumper", profile_id=profile_id, guild_id=target["guild_id"]))


@app.route("/autobumper/<int:profile_id>/targets/<int:target_id>/delete", methods=["POST"])
def delete_autobumper_target(profile_id: int, target_id: int):
    target = load_autobumper_target(profile_id, target_id)
    if not target:
        flash("Configuration Auto Bump introuvable.", "error")
        return redirect(url_for("autobumper", profile_id=profile_id))

    conn = get_db_connection()
    conn.execute("DELETE FROM autobumper_events WHERE target_id = ? AND profile_id = ?", (target_id, profile_id))
    conn.execute("DELETE FROM autobumper_targets WHERE id = ? AND profile_id = ?", (target_id, profile_id))
    conn.commit()
    conn.close()

    flash("Configuration Auto Bump supprimee.", "success")
    return redirect(url_for("autobumper", profile_id=profile_id))


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
start_autobumper_worker()
start_account_backup_scheduler_worker()
start_selfbot_tools_worker()


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1") != "0"
    app.run(debug=debug, host=host, port=port)
