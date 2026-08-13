"""History backfill: copies existing messages into matched channels.

Resumable: each channel's progress is checkpointed in relay.db
(backfill_cursor), so a re-run only asks Discord for messages after the
last one it already walked instead of re-fetching and re-checking the
channel's entire history from scratch every time. Retryable failures (rate
limits, Discord-side errors) are tracked separately (failed_messages) and
retried by fetching that specific message id, without needing a rescan.
Permanent failures (e.g. an empty/unsupported payload Discord will always
reject) are never queued for retry in the first place -- see
relay.RelayOutcome.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import discord

from db import Database
from relay import RelayOutcome, relay_new_message

logger = logging.getLogger("disclone")

# Small pause between sends so we don't hammer the per-webhook rate limit
# during a large backfill. discord.py handles 429s automatically; this just
# keeps us comfortably under them.
# Capped at 19 messages per 30 seconds (30 / 19 s between sends).
BACKFILL_DELAY_SECONDS = 30 / 19

# Give up on a retryable failure after this many attempts across backfill
# runs, rather than queuing it forever.
MAX_RETRY_ATTEMPTS = 3


async def _retry_failed(
    bot: discord.Client,
    db: Database,
    source_channel: discord.abc.Messageable,
    dest_channel: discord.TextChannel,
) -> tuple[int, int]:
    """Re-attempt this channel's queued retryable failures by fetching each
    message directly (no history walk needed). Returns (retried_ok, gave_up)."""
    retried_ok = 0
    gave_up = 0
    for failed in await db.get_failed_messages(source_channel.id):
        try:
            message = await source_channel.fetch_message(failed.source_message_id)
        except discord.NotFound:
            # Source message itself is gone now; nothing left to retry.
            await db.clear_failure(failed.source_message_id)
            continue
        except discord.HTTPException as exc:
            logger.warning("Could not re-fetch message %s for retry: %s", failed.source_message_id, exc)
            continue

        outcome = await relay_new_message(bot, db, message, dest_channel)
        await asyncio.sleep(BACKFILL_DELAY_SECONDS)

        if outcome in (RelayOutcome.SENT, RelayOutcome.SKIPPED):
            await db.clear_failure(failed.source_message_id)
            if outcome is RelayOutcome.SENT:
                retried_ok += 1
        elif outcome is RelayOutcome.FAILED_PERMANENT:
            await db.clear_failure(failed.source_message_id)
            gave_up += 1
        else:  # still FAILED_RETRYABLE
            if failed.attempts + 1 >= MAX_RETRY_ATTEMPTS:
                logger.warning(
                    "Giving up on message %s after %d attempts.", failed.source_message_id, failed.attempts + 1
                )
                await db.clear_failure(failed.source_message_id)
                gave_up += 1
            else:
                await db.record_failure(failed.source_message_id, source_channel.id, dest_channel.id, "retry failed again")

    return retried_ok, gave_up


async def run_backfill(
    bot: discord.Client,
    db: Database,
    channel_map: dict[int, discord.TextChannel],
    per_channel_limit: Optional[int] = None,
    full: bool = False,
) -> str:
    """Walk each matched channel and relay anything not yet relayed.

    By default this resumes from each channel's saved cursor, so a re-run
    only looks at messages posted since the last backfill. Pass full=True
    (the `!backfill full` command) to ignore the cursor and rescan a
    channel's entire history -- useful if you suspect gaps, e.g. messages
    that predate this resuming behavior being added, or a manually edited
    relay.db.

    per_channel_limit only applies when a channel has no cursor yet (or
    full=True): it caps how many of the most recent messages to copy on
    that initial pass. It's ignored on incremental (cursor-resumed) runs,
    since "most recent N" wouldn't have a stable meaning applied to just
    the messages posted since last time.
    """
    total_sent = 0
    total_skipped = 0
    total_failed = 0
    total_retried_ok = 0
    total_gave_up = 0

    for source_channel_id, dest_channel in channel_map.items():
        source_channel = bot.get_channel(source_channel_id)
        if source_channel is None:
            continue

        if full:
            # The full history walk below re-attempts every message anyway
            # (already_relayed only skips confirmed successes), so running
            # the retry queue first would double-attempt anything in it --
            # once here, once again in the walk -- burning through its
            # retry budget twice as fast.
            await db.reset_cursor(source_channel_id)
        else:
            retried_ok, gave_up = await _retry_failed(bot, db, source_channel, dest_channel)
            total_retried_ok += retried_ok
            total_gave_up += gave_up
        cursor = await db.get_cursor(source_channel_id)

        after = discord.Object(id=cursor) if cursor is not None else None
        limit = per_channel_limit if cursor is None else None
        if cursor is not None and per_channel_limit is not None:
            logger.info(
                "#%s is resuming from a saved position; ignoring the message limit for this run.",
                source_channel.name,
            )

        logger.info(
            "Backfilling #%s -> #%s%s", source_channel.name, dest_channel.name,
            " (resuming)" if cursor is not None else "",
        )
        try:
            async for message in source_channel.history(after=after, limit=limit, oldest_first=True):
                if await db.already_relayed(message.id):
                    total_skipped += 1
                else:
                    outcome = await relay_new_message(bot, db, message, dest_channel)
                    if outcome is RelayOutcome.SENT:
                        total_sent += 1
                        await asyncio.sleep(BACKFILL_DELAY_SECONDS)
                    elif outcome is RelayOutcome.SKIPPED:
                        total_skipped += 1
                    elif outcome is RelayOutcome.FAILED_RETRYABLE:
                        total_failed += 1
                        await db.record_failure(
                            message.id, source_channel_id, dest_channel.id, "send failed (retryable)"
                        )
                        await asyncio.sleep(BACKFILL_DELAY_SECONDS)
                    else:  # FAILED_PERMANENT
                        total_failed += 1
                        await asyncio.sleep(BACKFILL_DELAY_SECONDS)

                # Advance the checkpoint regardless of outcome: a retryable
                # failure is tracked (and retried) via failed_messages, not
                # by re-walking history from here again next time.
                await db.set_cursor(source_channel_id, message.id)
        except discord.Forbidden:
            logger.warning("Missing permission to read history in #%s; skipping.", source_channel.name)

    summary = f"{total_sent} sent, {total_skipped} skipped, {total_failed} failed"
    if total_retried_ok or total_gave_up:
        summary += f" ({total_retried_ok} previously-failed message(s) recovered, {total_gave_up} gave up)"
    return summary + "."
