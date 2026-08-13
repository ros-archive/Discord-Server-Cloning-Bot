"""Persistent storage for per-channel webhooks and source->dest message id mappings.

SQLite is used synchronously but every public method offloads the actual
query to a thread via asyncio.to_thread, so the event loop never blocks on
disk I/O.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import NamedTuple, Optional

DB_PATH = Path(__file__).parent / "relay.db"


class WebhookRecord(NamedTuple):
    webhook_id: int
    webhook_token: str


class MessageMapping(NamedTuple):
    dest_message_id: int
    dest_channel_id: int
    webhook_id: int
    webhook_token: str


class FailedMessage(NamedTuple):
    source_message_id: int
    attempts: int


class Database:
    def __init__(self, path: Path = DB_PATH) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS webhooks (
                dest_channel_id INTEGER PRIMARY KEY,
                webhook_id INTEGER NOT NULL,
                webhook_token TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_map (
                source_message_id INTEGER PRIMARY KEY,
                source_channel_id INTEGER NOT NULL,
                dest_message_id INTEGER NOT NULL,
                dest_channel_id INTEGER NOT NULL,
                webhook_id INTEGER NOT NULL,
                webhook_token TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS emoji_map (
                source_emoji_id INTEGER PRIMARY KEY,
                dest_emoji_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backfill_cursor (
                source_channel_id INTEGER PRIMARY KEY,
                last_message_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS failed_messages (
                source_message_id INTEGER PRIMARY KEY,
                source_channel_id INTEGER NOT NULL,
                dest_channel_id INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1,
                last_error TEXT
            );
            """
        )
        self._conn.commit()
        self._seed_cursor_from_message_map()

    def _seed_cursor_from_message_map(self) -> None:
        """One-time (but idempotent) backfill of backfill_cursor from
        whatever's already in message_map, so upgrading an existing
        relay.db doesn't force a full history rescan on the next
        !backfill -- the cursor starts at the highest source message id
        already relayed per channel and only ever moves forward from
        there.

        Safe to re-run: MAX() means running this again after new messages
        have advanced a channel's cursor further via set_cursor never
        moves it backward.
        """
        self._conn.execute(
            """
            INSERT INTO backfill_cursor (source_channel_id, last_message_id)
            SELECT source_channel_id, MAX(source_message_id)
            FROM message_map
            GROUP BY source_channel_id
            ON CONFLICT(source_channel_id) DO UPDATE SET
                last_message_id = MAX(last_message_id, excluded.last_message_id)
            """
        )
        self._conn.commit()

    # --- webhooks ---------------------------------------------------

    async def get_webhook(self, dest_channel_id: int) -> Optional[WebhookRecord]:
        return await asyncio.to_thread(self._get_webhook, dest_channel_id)

    def _get_webhook(self, dest_channel_id: int) -> Optional[WebhookRecord]:
        row = self._conn.execute(
            "SELECT webhook_id, webhook_token FROM webhooks WHERE dest_channel_id = ?",
            (dest_channel_id,),
        ).fetchone()
        return WebhookRecord(*row) if row else None

    async def save_webhook(self, dest_channel_id: int, webhook_id: int, webhook_token: str) -> None:
        await asyncio.to_thread(self._save_webhook, dest_channel_id, webhook_id, webhook_token)

    def _save_webhook(self, dest_channel_id: int, webhook_id: int, webhook_token: str) -> None:
        self._conn.execute(
            """
            INSERT INTO webhooks (dest_channel_id, webhook_id, webhook_token)
            VALUES (?, ?, ?)
            ON CONFLICT(dest_channel_id) DO UPDATE SET
                webhook_id = excluded.webhook_id,
                webhook_token = excluded.webhook_token
            """,
            (dest_channel_id, webhook_id, webhook_token),
        )
        self._conn.commit()

    # --- message map --------------------------------------------------

    async def get_mapping(self, source_message_id: int) -> Optional[MessageMapping]:
        return await asyncio.to_thread(self._get_mapping, source_message_id)

    def _get_mapping(self, source_message_id: int) -> Optional[MessageMapping]:
        row = self._conn.execute(
            "SELECT dest_message_id, dest_channel_id, webhook_id, webhook_token "
            "FROM message_map WHERE source_message_id = ?",
            (source_message_id,),
        ).fetchone()
        return MessageMapping(*row) if row else None

    async def save_mapping(
        self,
        source_message_id: int,
        source_channel_id: int,
        dest_message_id: int,
        dest_channel_id: int,
        webhook_id: int,
        webhook_token: str,
    ) -> None:
        await asyncio.to_thread(
            self._save_mapping,
            source_message_id,
            source_channel_id,
            dest_message_id,
            dest_channel_id,
            webhook_id,
            webhook_token,
        )

    def _save_mapping(
        self,
        source_message_id: int,
        source_channel_id: int,
        dest_message_id: int,
        dest_channel_id: int,
        webhook_id: int,
        webhook_token: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO message_map
                (source_message_id, source_channel_id, dest_message_id,
                 dest_channel_id, webhook_id, webhook_token)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_message_id) DO UPDATE SET
                dest_message_id = excluded.dest_message_id,
                dest_channel_id = excluded.dest_channel_id,
                webhook_id = excluded.webhook_id,
                webhook_token = excluded.webhook_token
            """,
            (
                source_message_id,
                source_channel_id,
                dest_message_id,
                dest_channel_id,
                webhook_id,
                webhook_token,
            ),
        )
        self._conn.commit()

    async def already_relayed(self, source_message_id: int) -> bool:
        return (await self.get_mapping(source_message_id)) is not None

    # --- emoji map ---------------------------------------------------

    async def get_emoji_mapping(self, source_emoji_id: int) -> Optional[int]:
        return await asyncio.to_thread(self._get_emoji_mapping, source_emoji_id)

    def _get_emoji_mapping(self, source_emoji_id: int) -> Optional[int]:
        row = self._conn.execute(
            "SELECT dest_emoji_id FROM emoji_map WHERE source_emoji_id = ?",
            (source_emoji_id,),
        ).fetchone()
        return row[0] if row else None

    async def save_emoji_mapping(self, source_emoji_id: int, dest_emoji_id: int) -> None:
        await asyncio.to_thread(self._save_emoji_mapping, source_emoji_id, dest_emoji_id)

    def _save_emoji_mapping(self, source_emoji_id: int, dest_emoji_id: int) -> None:
        self._conn.execute(
            """
            INSERT INTO emoji_map (source_emoji_id, dest_emoji_id)
            VALUES (?, ?)
            ON CONFLICT(source_emoji_id) DO UPDATE SET
                dest_emoji_id = excluded.dest_emoji_id
            """,
            (source_emoji_id, dest_emoji_id),
        )
        self._conn.commit()

    # --- backfill cursor -----------------------------------------------
    #
    # Tracks the highest source message id !backfill has already walked
    # per channel, so a re-run can ask Discord for only what's new
    # (history(after=cursor)) instead of re-fetching and re-checking the
    # channel's entire history every time.

    async def get_cursor(self, source_channel_id: int) -> Optional[int]:
        return await asyncio.to_thread(self._get_cursor, source_channel_id)

    def _get_cursor(self, source_channel_id: int) -> Optional[int]:
        row = self._conn.execute(
            "SELECT last_message_id FROM backfill_cursor WHERE source_channel_id = ?",
            (source_channel_id,),
        ).fetchone()
        return row[0] if row else None

    async def set_cursor(self, source_channel_id: int, last_message_id: int) -> None:
        await asyncio.to_thread(self._set_cursor, source_channel_id, last_message_id)

    def _set_cursor(self, source_channel_id: int, last_message_id: int) -> None:
        # MAX(...) guards against ever moving the cursor backward, e.g. if
        # calls race or a message id is processed out of order.
        self._conn.execute(
            """
            INSERT INTO backfill_cursor (source_channel_id, last_message_id)
            VALUES (?, ?)
            ON CONFLICT(source_channel_id) DO UPDATE SET
                last_message_id = MAX(last_message_id, excluded.last_message_id)
            """,
            (source_channel_id, last_message_id),
        )
        self._conn.commit()

    async def reset_cursor(self, source_channel_id: int) -> None:
        """Used by `!backfill full` to force a complete history rescan."""
        await asyncio.to_thread(self._reset_cursor, source_channel_id)

    def _reset_cursor(self, source_channel_id: int) -> None:
        self._conn.execute(
            "DELETE FROM backfill_cursor WHERE source_channel_id = ?", (source_channel_id,)
        )
        self._conn.commit()

    # --- failed message retry queue -------------------------------------
    #
    # Only ever holds *retryable* failures (rate limits, Discord-side
    # errors) -- permanent failures (bad payload, too large, etc.) are
    # never inserted, so this table can't accumulate messages that will
    # fail identically forever.

    async def get_failed_messages(self, source_channel_id: int) -> list[FailedMessage]:
        return await asyncio.to_thread(self._get_failed_messages, source_channel_id)

    def _get_failed_messages(self, source_channel_id: int) -> list[FailedMessage]:
        rows = self._conn.execute(
            "SELECT source_message_id, attempts FROM failed_messages WHERE source_channel_id = ?",
            (source_channel_id,),
        ).fetchall()
        return [FailedMessage(*row) for row in rows]

    async def record_failure(
        self, source_message_id: int, source_channel_id: int, dest_channel_id: int, error: str
    ) -> None:
        await asyncio.to_thread(
            self._record_failure, source_message_id, source_channel_id, dest_channel_id, error
        )

    def _record_failure(
        self, source_message_id: int, source_channel_id: int, dest_channel_id: int, error: str
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO failed_messages (source_message_id, source_channel_id, dest_channel_id, attempts, last_error)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(source_message_id) DO UPDATE SET
                attempts = attempts + 1,
                last_error = excluded.last_error
            """,
            (source_message_id, source_channel_id, dest_channel_id, error),
        )
        self._conn.commit()

    async def clear_failure(self, source_message_id: int) -> None:
        await asyncio.to_thread(self._clear_failure, source_message_id)

    def _clear_failure(self, source_message_id: int) -> None:
        self._conn.execute("DELETE FROM failed_messages WHERE source_message_id = ?", (source_message_id,))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
