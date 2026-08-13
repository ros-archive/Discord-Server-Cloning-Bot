"""Core relay logic: channel matching, webhook management, and message forwarding."""

from __future__ import annotations

import asyncio
import io
import logging
import re
from collections import defaultdict
from enum import Enum
from typing import Optional

import discord
from discord.utils import MISSING

from db import Database

logger = logging.getLogger("disclone")

# Conservative default; Discord's actual per-guild upload cap varies by boost
# tier (8-500MB). Anything over this is skipped and linked instead of
# re-uploaded, since re-uploads that exceed the destination guild's real cap
# would just fail anyway.
MAX_UPLOAD_BYTES = 25 * 1024 * 1024

# Small pause between emoji creates -- guild emoji create is rate limited
# more tightly than most endpoints, and cloning a large emoji set at once
# will otherwise spend most of its time waiting on 429s anyway.
EMOJI_CLONE_DELAY_SECONDS = 1.0

RELAYABLE_TYPES = {discord.MessageType.default, discord.MessageType.reply}


class RelayOutcome(Enum):
    """Result of attempting to relay one message, so callers (backfill in
    particular) can tell "nothing to do here, never will be" apart from
    "this is worth retrying" instead of treating every non-send as the same
    kind of failure."""

    SENT = "sent"
    SKIPPED = "skipped"  # not relayable, from a webhook, our own, or genuinely empty payload
    FAILED_RETRYABLE = "failed_retryable"  # rate limited / Discord-side error; try again later
    FAILED_PERMANENT = "failed_permanent"  # Discord will reject this payload every time


def _classify_http_failure(exc: discord.HTTPException) -> RelayOutcome:
    """429s and 5xx are worth retrying; every other 4xx (bad payload, too
    large, etc.) will fail identically forever, so don't keep retrying it."""
    if exc.status == 429 or exc.status >= 500:
        return RelayOutcome.FAILED_RETRYABLE
    return RelayOutcome.FAILED_PERMANENT


# --------------------------------------------------------------------
# Channel matching
# --------------------------------------------------------------------


def build_channel_map(
    source_guild: discord.Guild, dest_guild: discord.Guild
) -> dict[int, discord.TextChannel]:
    """Map source channel id -> destination TextChannel, matched by name.

    Prefers matching (category name, channel name) pairs so that e.g.
    "general" under "Announcements" doesn't collide with "general" under
    "Chat". Falls back to a bare name match. Logs a warning (and picks the
    first candidate) if a source channel name is still ambiguous.
    """
    by_key: dict[tuple[Optional[str], str], list[discord.TextChannel]] = defaultdict(list)
    by_name: dict[str, list[discord.TextChannel]] = defaultdict(list)

    for ch in dest_guild.text_channels:
        cat_name = ch.category.name if ch.category else None
        by_key[(cat_name, ch.name)].append(ch)
        by_name[ch.name].append(ch)

    mapping: dict[int, discord.TextChannel] = {}
    for ch in source_guild.text_channels:
        cat_name = ch.category.name if ch.category else None
        candidates = by_key.get((cat_name, ch.name)) or by_name.get(ch.name) or []
        if not candidates:
            logger.info("No destination channel matches #%s (category %s); skipping.", ch.name, cat_name)
            continue
        if len(candidates) > 1:
            logger.warning(
                "Multiple destination channels named '%s' match source #%s; using %s (id=%s).",
                ch.name, ch.name, candidates[0].name, candidates[0].id,
            )
        mapping[ch.id] = candidates[0]
    return mapping


# --------------------------------------------------------------------
# Webhook management
# --------------------------------------------------------------------


async def get_or_create_webhook(
    db: Database, channel: discord.TextChannel, bot: discord.Client
) -> discord.Webhook:
    record = await db.get_webhook(channel.id)
    if record is not None:
        return discord.Webhook.partial(record.webhook_id, record.webhook_token, client=bot)

    try:
        existing = await channel.webhooks()
    except discord.Forbidden as exc:
        raise RuntimeError(f"Missing 'Manage Webhooks' permission in #{channel.name}.") from exc

    for wh in existing:
        if wh.user and wh.user.id == bot.user.id and wh.token:
            await db.save_webhook(channel.id, wh.id, wh.token)
            return wh

    if len(existing) >= 15:
        raise RuntimeError(
            f"#{channel.name} already has the maximum of 15 webhooks; delete an unused one and retry."
        )

    new_wh = await channel.create_webhook(name="disclone-relay")
    await db.save_webhook(channel.id, new_wh.id, new_wh.token)
    return new_wh


async def recreate_webhook(
    db: Database, channel: discord.TextChannel, bot: discord.Client
) -> discord.Webhook:
    """Force-create a fresh webhook, e.g. after the cached one 404s."""
    new_wh = await channel.create_webhook(name="disclone-relay")
    await db.save_webhook(channel.id, new_wh.id, new_wh.token)
    return new_wh


# --------------------------------------------------------------------
# Message forwarding
# --------------------------------------------------------------------


def _sanitize_username(name: str) -> str:
    name = (name or "").strip() or "Unknown User"
    # Discord rejects webhook usernames containing "discord" or "clyde".
    name = re.sub(r"discord", "disc0rd", name, flags=re.IGNORECASE)
    name = re.sub(r"clyde", "clyd3", name, flags=re.IGNORECASE)
    return name[:80]


async def _build_files_and_fallback(message: discord.Message) -> tuple[list[discord.File], list[str]]:
    """Download attachments as re-uploadable files; return (files, fallback_links).

    Attachments too large to re-upload are reported back as plain links
    (note: Discord CDN attachment URLs are signed and expire after a
    period, so these fallback links will eventually rot).
    """
    files: list[discord.File] = []
    fallback_links: list[str] = []
    for att in message.attachments:
        if att.size > MAX_UPLOAD_BYTES:
            fallback_links.append(att.url)
            continue
        try:
            data = await att.read()
        except discord.HTTPException:
            fallback_links.append(att.url)
            continue
        files.append(discord.File(io.BytesIO(data), filename=att.filename))
    return files, fallback_links


def _rich_embeds(embeds: list[discord.Embed]) -> list[discord.Embed]:
    """Only forward author-authored rich embeds; auto-generated link/image/
    video previews either get rejected by webhook.send or get regenerated
    from the message content, producing duplicates."""
    return [e for e in embeds if e.type == "rich"][:10]


async def relay_new_message(
    bot: discord.Client, db: Database, message: discord.Message, dest_channel: discord.TextChannel
) -> RelayOutcome:
    if message.type not in RELAYABLE_TYPES:
        return RelayOutcome.SKIPPED
    if message.webhook_id is not None:
        return RelayOutcome.SKIPPED  # don't re-relay messages that already came from a webhook
    if message.author.id == bot.user.id:
        return RelayOutcome.SKIPPED

    content = (message.content or "")[:2000]
    embeds = _rich_embeds(message.embeds)
    files, fallback_links = await _build_files_and_fallback(message)
    if fallback_links:
        note = "\n".join(f"[attachment too large, original link]({url})" for url in fallback_links)
        content = f"{content}\n{note}" if content else note
        content = content[:2000]

    # Historical messages picked up by !backfill may already have reactions;
    # fold those into the initial send too, not just later edits.
    content = _content_with_reactions(content, message.reactions)

    if not content and not embeds and not files:
        # Nothing left to send -- e.g. sticker-only messages, or messages
        # whose only embeds were auto-generated previews we deliberately
        # drop. Discord's API rejects a fully empty webhook payload
        # (400/50006), so catching it here avoids sending a request that's
        # guaranteed to fail, and (more importantly) avoids marking it as a
        # retryable failure that backfill would otherwise redo forever.
        logger.info("Skipping message %s: no content/embeds/files to relay.", message.id)
        return RelayOutcome.SKIPPED

    username = _sanitize_username(message.author.display_name)
    avatar_url = message.author.display_avatar.url

    webhook = await get_or_create_webhook(db, dest_channel, bot)

    try:
        sent = await webhook.send(
            content=content or None,
            username=username,
            avatar_url=avatar_url,
            embeds=embeds,
            files=files,
            allowed_mentions=discord.AllowedMentions.none(),
            wait=True,
        )
    except discord.NotFound:
        logger.warning("Webhook for #%s was gone; recreating and retrying once.", dest_channel.name)
        webhook = await recreate_webhook(db, dest_channel, bot)
        try:
            sent = await webhook.send(
                content=content or None,
                username=username,
                avatar_url=avatar_url,
                embeds=embeds,
                files=files,
                allowed_mentions=discord.AllowedMentions.none(),
                wait=True,
            )
        except discord.HTTPException as exc:
            logger.error("Failed to relay message %s after webhook recreate: %s", message.id, exc)
            return _classify_http_failure(exc)
    except discord.HTTPException as exc:
        logger.error("Failed to relay message %s: %s", message.id, exc)
        return _classify_http_failure(exc)

    await db.save_mapping(
        source_message_id=message.id,
        source_channel_id=message.channel.id,
        dest_message_id=sent.id,
        dest_channel_id=dest_channel.id,
        webhook_id=webhook.id,
        webhook_token=webhook.token,
    )
    return RelayOutcome.SENT


async def relay_edit_from_payload(
    bot: discord.Client, db: Database, payload: discord.RawMessageUpdateEvent
) -> None:
    """Handle discord.py's raw edit event (fires even for uncached / pre-restart
    messages), since that's the population the persistent message_map serves."""
    mapping = await db.get_mapping(payload.message_id)
    if mapping is None:
        return  # never relayed (e.g. predates the bot, or wasn't in a mapped channel)

    data = payload.data
    content = data["content"][:2000] if "content" in data else MISSING
    if "embeds" in data:
        embeds = [discord.Embed.from_dict(e) for e in data["embeds"] if e.get("type") == "rich"][:10]
    else:
        embeds = MISSING

    if content is MISSING and embeds is MISSING:
        return  # edit didn't touch content or embeds (e.g. pin/flags change)

    webhook = discord.Webhook.partial(mapping.webhook_id, mapping.webhook_token, client=bot)
    try:
        await webhook.edit_message(
            mapping.dest_message_id,
            content=content,
            embeds=embeds,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.NotFound:
        logger.warning(
            "Could not edit relayed message %s (webhook or message no longer exists).",
            mapping.dest_message_id,
        )
    except discord.HTTPException as exc:
        logger.error("Failed to edit relayed message %s: %s", mapping.dest_message_id, exc)


# --------------------------------------------------------------------
# Reaction forwarding
# --------------------------------------------------------------------
#
# Webhooks can't react to messages themselves, and per-user reaction state
# isn't cheap to mirror faithfully anyway, so instead of reacting we append
# a summary line -- e.g. "\U0001F44D 3  \U0001F602 1" -- to the relayed
# message's own content and re-edit it via the webhook whenever reactions
# change. The summary is always rebuilt from a fresh fetch of the source
# message, so it can't drift out of sync with edits to the base content.

REACTION_SUMMARY_SEPARATOR = "\n\n"


def _format_reaction_summary(reactions: list[discord.Reaction]) -> str:
    if not reactions:
        return ""
    return "  ".join(f"{reaction.emoji} {reaction.count}" for reaction in reactions)


def _content_with_reactions(base_content: str, reactions: list[discord.Reaction]) -> str:
    summary = _format_reaction_summary(reactions)
    parts = [p for p in (base_content, summary) if p]
    return REACTION_SUMMARY_SEPARATOR.join(parts)[:2000]


async def _sync_reaction_summary(bot: discord.Client, db: Database, channel_id: int, message_id: int) -> None:
    mapping = await db.get_mapping(message_id)
    if mapping is None:
        return

    source_channel = bot.get_channel(channel_id)
    if source_channel is None:
        try:
            source_channel = await bot.fetch_channel(channel_id)
        except discord.HTTPException:
            return

    try:
        source_message = await source_channel.fetch_message(message_id)
    except discord.NotFound:
        return  # source message itself was deleted; nothing to resync
    except discord.HTTPException as exc:
        logger.warning("Failed to fetch source message %s for reaction sync: %s", message_id, exc)
        return

    new_content = _content_with_reactions(source_message.content or "", source_message.reactions)

    webhook = discord.Webhook.partial(mapping.webhook_id, mapping.webhook_token, client=bot)
    try:
        await webhook.edit_message(
            mapping.dest_message_id,
            content=new_content or None,
            allowed_mentions=discord.AllowedMentions.none(),
        )
    except discord.NotFound:
        logger.warning(
            "Could not update reaction summary on relayed message %s (webhook or message gone).",
            mapping.dest_message_id,
        )
    except discord.HTTPException as exc:
        logger.error("Failed to update reaction summary on relayed message %s: %s", mapping.dest_message_id, exc)


async def relay_reaction_add(bot: discord.Client, db: Database, payload: discord.RawReactionActionEvent) -> None:
    if payload.user_id == bot.user.id:
        return
    await _sync_reaction_summary(bot, db, payload.channel_id, payload.message_id)


async def relay_reaction_remove(bot: discord.Client, db: Database, payload: discord.RawReactionActionEvent) -> None:
    await _sync_reaction_summary(bot, db, payload.channel_id, payload.message_id)


async def relay_reaction_clear(bot: discord.Client, db: Database, payload: discord.RawReactionClearEvent) -> None:
    """All reactions of every emoji were cleared from the source message."""
    await _sync_reaction_summary(bot, db, payload.channel_id, payload.message_id)


async def relay_reaction_clear_emoji(
    bot: discord.Client, db: Database, payload: discord.RawReactionClearEmojiEvent
) -> None:
    """One specific emoji's reactions were cleared from the source message."""
    await _sync_reaction_summary(bot, db, payload.channel_id, payload.message_id)


# --------------------------------------------------------------------
# Emoji cloning
# --------------------------------------------------------------------
#
# Copies custom emojis from the source guild into the destination guild so
# destination members can actually use them (react, type them), not just
# see them rendered in relayed message content. Deletes aren't synced here
# either, consistent with the rest of the bot: removing the source emoji
# leaves the destination clone in place.


async def clone_emoji(
    db: Database, dest_guild: discord.Guild, source_emoji: discord.Emoji
) -> Optional[discord.Emoji]:
    """Clone a single emoji into dest_guild, or return the existing clone if
    one's already mapped and still present. Returns None on skip/failure."""
    existing_id = await db.get_emoji_mapping(source_emoji.id)
    if existing_id is not None:
        existing = dest_guild.get_emoji(existing_id)
        if existing is not None:
            return existing
        # Mapped clone no longer exists in the destination guild (deleted
        # there); fall through and re-create it.

    if discord.utils.get(dest_guild.emojis, name=source_emoji.name):
        logger.info(
            "Destination guild already has an emoji named '%s'; skipping clone of %s.",
            source_emoji.name, source_emoji.id,
        )
        return None

    try:
        image_bytes = await source_emoji.read()
    except discord.HTTPException as exc:
        logger.warning("Failed to download emoji '%s' for cloning: %s", source_emoji.name, exc)
        return None

    try:
        new_emoji = await dest_guild.create_custom_emoji(
            name=source_emoji.name,
            image=image_bytes,
            reason=f"Cloned from source server by disclone (source emoji id {source_emoji.id})",
        )
    except discord.HTTPException as exc:
        # Most commonly: emoji slots (static or animated) are full.
        logger.warning("Failed to create emoji '%s' in destination guild: %s", source_emoji.name, exc)
        return None

    await db.save_emoji_mapping(source_emoji.id, new_emoji.id)
    logger.info("Cloned emoji '%s' (%s -> %s).", source_emoji.name, source_emoji.id, new_emoji.id)
    return new_emoji


async def sync_emojis(db: Database, source_guild: discord.Guild, dest_guild: discord.Guild) -> str:
    """Clone every source emoji not already present in the destination guild."""
    cloned = 0
    skipped = 0
    failed = 0
    for emoji in source_guild.emojis:
        existing_id = await db.get_emoji_mapping(emoji.id)
        if existing_id is not None and dest_guild.get_emoji(existing_id) is not None:
            skipped += 1
            continue
        result = await clone_emoji(db, dest_guild, emoji)
        if result is not None:
            cloned += 1
        else:
            failed += 1
        await asyncio.sleep(EMOJI_CLONE_DELAY_SECONDS)
    return f"{cloned} emoji(s) cloned, {skipped} already present, {failed} skipped/failed."
