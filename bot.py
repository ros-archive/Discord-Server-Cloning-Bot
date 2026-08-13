"""Entry point: connects to Discord, wires up event handlers, and exposes the
owner-only !backfill command.

Run with:  python bot.py
"""

from __future__ import annotations

import os

import certifi

# Must run before aiohttp/discord are imported: they build and cache a
# default SSLContext the first time they're imported, so setting this any
# later has no effect. python.org's macOS installers in particular ship a
# Python that doesn't use the system trust store, which otherwise fails
# every HTTPS request with CERTIFICATE_VERIFY_FAILED until this is set (or
# "Install Certificates.command" is run once for that Python installation).
os.environ.setdefault("SSL_CERT_FILE", certifi.where())

import asyncio  # noqa: E402
import logging  # noqa: E402
from typing import Optional  # noqa: E402

import discord  # noqa: E402
from discord.ext import commands  # noqa: E402

from backfill import run_backfill  # noqa: E402
from config import load_config  # noqa: E402
from db import Database  # noqa: E402
from relay import (  # noqa: E402
    EMOJI_CLONE_DELAY_SECONDS,
    build_channel_map,
    clone_emoji,
    relay_edit_from_payload,
    relay_new_message,
    relay_reaction_add,
    relay_reaction_clear,
    relay_reaction_clear_emoji,
    relay_reaction_remove,
    sync_emojis,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("disclone")

cfg = load_config()

intents = discord.Intents.default()
intents.message_content = True  # privileged; must also be enabled in the dev portal
intents.guilds = True
intents.reactions = True  # non-privileged, but set explicitly for clarity
intents.emojis_and_stickers = True  # non-privileged; needed for on_guild_emojis_update

bot = commands.Bot(command_prefix=cfg.command_prefix, intents=intents)
db = Database()

# source_channel_id -> destination TextChannel, rebuilt on_ready and via !resync
channel_map: dict[int, discord.TextChannel] = {}


def _rebuild_channel_map() -> None:
    global channel_map
    source_guild = bot.get_guild(cfg.source_guild_id)
    dest_guild = bot.get_guild(cfg.dest_guild_id)
    if source_guild is None or dest_guild is None:
        logger.error(
            "Bot is not a member of the configured source and/or destination guild "
            "(source=%s, dest=%s). Invite it to both and restart.",
            cfg.source_guild_id, cfg.dest_guild_id,
        )
        channel_map = {}
        return
    channel_map = build_channel_map(source_guild, dest_guild)
    logger.info(
        "Mapped %d/%d source channels to destination channels.",
        len(channel_map), len(source_guild.text_channels),
    )


@bot.check
async def _commands_only_in_dest_guild(ctx: commands.Context) -> bool:
    # Owner commands (!backfill, !resync, !clone-emojis, !help) only work
    # when invoked from the destination server, so the source server's
    # members never see or trigger them.
    return ctx.guild is not None and ctx.guild.id == cfg.dest_guild_id


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, (commands.CommandNotFound, commands.CheckFailure)):
        return  # e.g. a command typed in the source server -- ignore silently
    logger.error("Command error in #%s: %s", getattr(ctx.channel, "name", ctx.channel), error)


@bot.event
async def on_ready() -> None:
    _rebuild_channel_map()
    logger.info(
        "Live message relay is %s.",
        "ENABLED" if cfg.live_relay_enabled else "DISABLED (set ENABLE_LIVE_RELAY=true in .env to turn on; "
        "until then only !backfill sends messages)",
    )
    logger.info("Logged in as %s (id=%s). Ready.", bot.user, bot.user.id)


@bot.event
async def on_message(message: discord.Message) -> None:
    await bot.process_commands(message)

    if not cfg.live_relay_enabled:
        return
    if message.guild is None or message.guild.id != cfg.source_guild_id:
        return
    dest_channel = channel_map.get(message.channel.id)
    if dest_channel is None:
        return
    await relay_new_message(bot, db, message, dest_channel)


@bot.event
async def on_raw_message_edit(payload: discord.RawMessageUpdateEvent) -> None:
    # Use the raw event (not on_message_edit) so edits to messages sent
    # before this process started, or evicted from the message cache, still
    # sync -- that's the whole reason the mapping is persisted in SQLite.
    if payload.guild_id != cfg.source_guild_id:
        return
    await relay_edit_from_payload(bot, db, payload)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    if payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_add(bot, db, payload)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent) -> None:
    if payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_remove(bot, db, payload)


@bot.event
async def on_raw_reaction_clear(payload: discord.RawReactionClearEvent) -> None:
    if payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_clear(bot, db, payload)


@bot.event
async def on_raw_reaction_clear_emoji(payload: discord.RawReactionClearEmojiEvent) -> None:
    if payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_clear_emoji(bot, db, payload)


@bot.event
async def on_guild_emojis_update(
    guild: discord.Guild, before: list[discord.Emoji], after: list[discord.Emoji]
) -> None:
    # Live-clone any emoji newly added to the source guild. Removals aren't
    # mirrored -- consistent with the rest of the bot not syncing deletes.
    if guild.id != cfg.source_guild_id:
        return
    dest_guild = bot.get_guild(cfg.dest_guild_id)
    if dest_guild is None:
        return
    before_ids = {e.id for e in before}
    new_emojis = [e for e in after if e.id not in before_ids]
    for emoji in new_emojis:
        await clone_emoji(db, dest_guild, emoji)
        await asyncio.sleep(EMOJI_CLONE_DELAY_SECONDS)


@bot.command(name="resync")
@commands.is_owner()
async def resync_cmd(ctx: commands.Context) -> None:
    """Owner-only: rebuild the source->destination channel name map (e.g. after adding channels)."""
    _rebuild_channel_map()
    await ctx.reply(f"Rebuilt channel map: {len(channel_map)} channel(s) matched.")


@bot.command(name="backfill")
@commands.is_owner()
async def backfill_cmd(ctx: commands.Context, arg: Optional[str] = None) -> None:
    """Owner-only: copy existing message history into matching destination channels.

    Usage:
      !backfill          resume from each channel's last backfill (or copy
                          full history the first time)
      !backfill 200       on a channel's first backfill, cap it to the 200
                          most recent messages (ignored on a resumed run)
      !backfill full       ignore saved progress and rescan every channel's
                            full history from the start

    Safe to re-run either way -- already-relayed messages (tracked in
    relay.db) are skipped, and normal runs resume instead of rescanning.
    """
    if not channel_map:
        await ctx.reply("Channel map is empty -- is the bot in both guilds, and do any channel names match?")
        return

    full = False
    limit: Optional[int] = None
    if arg is not None:
        if arg.strip().lower() in ("full", "reset"):
            full = True
        else:
            try:
                limit = int(arg)
            except ValueError:
                await ctx.reply(f"Usage: `{cfg.command_prefix}backfill [limit|full]`")
                return

    await ctx.reply(
        f"Starting {'full ' if full else ''}backfill across {len(channel_map)} channel(s)... this may take a while."
    )
    summary = await run_backfill(bot, db, channel_map, per_channel_limit=limit, full=full)
    await ctx.reply(f"Backfill complete: {summary}")


@bot.command(name="clone-emojis")
@commands.is_owner()
async def clone_emojis_cmd(ctx: commands.Context) -> None:
    """Owner-only: clone every custom emoji from the source guild into the destination guild.

    Safe to re-run -- emojis already cloned (tracked in relay.db) are skipped.
    New emojis added to the source guild afterwards are cloned automatically.
    """
    source_guild = bot.get_guild(cfg.source_guild_id)
    dest_guild = bot.get_guild(cfg.dest_guild_id)
    if source_guild is None or dest_guild is None:
        await ctx.reply("Bot isn't a member of both configured guilds.")
        return
    await ctx.reply(f"Cloning up to {len(source_guild.emojis)} emoji(s) into {dest_guild.name}...")
    summary = await sync_emojis(db, source_guild, dest_guild)
    await ctx.reply(f"Emoji clone complete: {summary}")


def main() -> None:
    try:
        bot.run(cfg.token)
    finally:
        db.close()


if __name__ == "__main__":
    main()
