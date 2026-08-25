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

import aiohttp
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

# Connection drops worth automatically resuming from instead of leaving the
# operator to notice and re-run !backfill -- most commonly the machine
# running the bot going to sleep mid-backfill and waking back up to a
# stale/timed-out connection, or a transient network blip. discord.py
# already retries rate limits, connection resets, and 5xx responses
# internally (see HTTPClient.request); what reaches us here is the stuff it
# gives up on: outright timeouts (TimeoutError is an OSError subclass),
# aiohttp-level connection errors, gateway drops, and a 5xx that's still
# happening after discord.py's own retries -- relay._classify_http_failure
# treats status >= 500 as retryable for the same reason.
TRANSIENT_EXCEPTIONS = (
    TimeoutError,
    aiohttp.ClientError,
    discord.ConnectionClosed,
    discord.GatewayNotFound,
    discord.DiscordServerError,
    OSError,
)

# Capped exponential backoff between reconnect attempts for a channel.
TRANSIENT_RETRY_BASE_SECONDS = 5
TRANSIENT_RETRY_MAX_SECONDS = 120

# Give up on a channel (not the whole backfill run) after this many
# *consecutive* transient failures -- progress is checkpointed after every
# message either way, so a later !backfill still resumes right where this
# left off.
MAX_CONSECUTIVE_TRANSIENT_RETRIES = 8


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

    Also resumes automatically *within* a single run: a timeout or dropped
    connection (e.g. the machine sleeping mid-backfill) is caught and
    retried with backoff from the last checkpoint, up to
    MAX_CONSECUTIVE_TRANSIENT_RETRIES per channel, instead of aborting the
    whole command.
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
            # retry budget twice as fast. Reset once up front only -- not on
            # every reconnect below, or a retry after partial progress would
            # wipe out the cursor it just made.
            await db.reset_cursor(source_channel_id)

        logger.info("Backfilling #%s -> #%s", source_channel.name, dest_channel.name)

        # Captured once, before any reconnect: whether this is a fresh
        # (uncursored) backfill of this channel decides whether
        # per_channel_limit applies at all. remaining_limit is then carried
        # across reconnects and counted down as messages are consumed, so a
        # mid-run timeout can't turn `!backfill 200` into an unbounded walk
        # just because the cursor is no longer None on re-entry.
        had_cursor_at_start = (await db.get_cursor(source_channel_id)) is not None
        remaining_limit = per_channel_limit if not had_cursor_at_start else None
        if had_cursor_at_start:
            if per_channel_limit is not None:
                logger.info(
                    "#%s is resuming from a saved position; ignoring the message limit for this run.",
                    source_channel.name,
                )
            else:
                logger.info("#%s is resuming from a saved position.", source_channel.name)

        consecutive_failures = 0
        # Drain the failed-message queue at most once per !backfill run, not
        # once per reconnect -- otherwise a message stuck at
        # FAILED_RETRYABLE gets record_failure()'d again on every
        # reconnect, burning through MAX_RETRY_ATTEMPTS within a single run
        # instead of across separate runs as intended. Only flips to True
        # once _retry_failed actually returns, so a reconnect that
        # interrupts the drain itself still finishes it (already-cleared
        # entries are gone from the DB, so re-running is safe).
        retry_queue_drained = full  # full=True skips the queue entirely
        while True:
            try:
                if not retry_queue_drained:
                    retried_ok, gave_up = await _retry_failed(bot, db, source_channel, dest_channel)
                    total_retried_ok += retried_ok
                    total_gave_up += gave_up
                    retry_queue_drained = True

                cursor = await db.get_cursor(source_channel_id)
                after = discord.Object(id=cursor) if cursor is not None else None

                if remaining_limit is not None and remaining_limit <= 0:
                    break  # limit already satisfied before an earlier reconnect

                async for message in source_channel.history(
                    after=after, limit=remaining_limit, oldest_first=True
                ):
                    consecutive_failures = 0  # made progress -- reset the retry budget
                    if remaining_limit is not None:
                        remaining_limit -= 1
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
                break  # finished this channel cleanly
            except discord.Forbidden:
                logger.warning("Missing permission to read history in #%s; skipping.", source_channel.name)
                break
            except TRANSIENT_EXCEPTIONS as exc:
                consecutive_failures += 1
                if consecutive_failures > MAX_CONSECUTIVE_TRANSIENT_RETRIES:
                    logger.error(
                        "Giving up on #%s after %d connection interruptions in a row; progress is "
                        "checkpointed, so re-running !backfill later will resume from here.",
                        source_channel.name, consecutive_failures - 1,
                    )
                    break
                delay = min(
                    TRANSIENT_RETRY_BASE_SECONDS * 2 ** (consecutive_failures - 1),
                    TRANSIENT_RETRY_MAX_SECONDS,
                )
                logger.warning(
                    "Connection interrupted while backfilling #%s (%s); auto-resuming from the last "
                    "checkpoint in %ds (attempt %d/%d).",
                    source_channel.name, exc, delay, consecutive_failures, MAX_CONSECUTIVE_TRANSIENT_RETRIES,
                )
                await asyncio.sleep(delay)
                # Loop back around: cursor/failed_messages state was
                # checkpointed as we went, so this just re-enters right
                # after the last message (or retry) that made it through.

    summary = f"{total_sent} sent, {total_skipped} skipped, {total_failed} failed"
    if total_retried_ok or total_gave_up:
        summary += f" ({total_retried_ok} previously-failed message(s) recovered, {total_gave_up} gave up)"
    return summary + "."
