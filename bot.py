"""Entry point: connects to Discord, wires up event handlers, and exposes
owner-only history and live-sync commands.

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

# `!livesync` can enable live relaying without changing the on-disk config.
# Keeping this separate from the frozen Config makes that choice explicitly
# session-scoped: restart the bot to return to ENABLE_LIVE_RELAY's value.
live_relay_enabled = cfg.live_relay_enabled
livesync_task: Optional[asyncio.Task] = None

# Persisted in relay.db while a livesync is active, so a restart/crash resumes
# it automatically. Value = id of the channel to report progress in. Cleared
# only by !stoplivesync / !stopsync.
LIVESYNC_SETTING = "livesync_channel_id"


def _livesync_running() -> bool:
    return livesync_task is not None and not livesync_task.done()


async def _notify(channel_id: int, text: str) -> None:
    channel = bot.get_channel(channel_id)
    if channel is not None:
        try:
            await channel.send(text)
        except discord.HTTPException as exc:
            logger.warning("Could not post livesync status: %s", exc)


async def _livesync_job(channel_id: int, resumed: bool) -> None:
    summary = await run_backfill(bot, db, channel_map)
    await _notify(
        channel_id,
        f"Live sync {'resumed: ' if resumed else 'ready: '}backfill complete: {summary} "
        "New source messages will continue syncing until `!stoplivesync`.",
    )


async def _start_livesync(channel_id: int, resumed: bool) -> None:
    global live_relay_enabled, livesync_task
    live_relay_enabled = True
    await db.set_setting(LIVESYNC_SETTING, str(channel_id))
    livesync_task = asyncio.create_task(_livesync_job(channel_id, resumed))


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
    # Owner commands (!backfill, !livesync, !resync, !clone-emojis, !help) only work
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
        "ENABLED" if live_relay_enabled else "DISABLED (set ENABLE_LIVE_RELAY=true in .env to turn on; "
        "until then only !backfill sends messages)",
    )
    # on_ready also fires after gateway reconnects; only resume if not already running.
    saved = await db.get_setting(LIVESYNC_SETTING)
    if saved is not None and not _livesync_running() and channel_map:
        channel_id = int(saved)
        logger.info("Livesync was active when the bot last stopped; resuming.")
        await _notify(channel_id, "Bot restarted; resuming live sync...")
        await _start_livesync(channel_id, resumed=True)
    logger.info("Logged in as %s (id=%s). Ready.", bot.user, bot.user.id)


@bot.event
async def on_message(message: discord.Message) -> None:
    await bot.process_commands(message)

    if not live_relay_enabled:
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
    if not live_relay_enabled or payload.guild_id != cfg.source_guild_id:
        return
    await relay_edit_from_payload(bot, db, payload)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent) -> None:
    if not live_relay_enabled or payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_add(bot, db, payload)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent) -> None:
    if not live_relay_enabled or payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_remove(bot, db, payload)


@bot.event
async def on_raw_reaction_clear(payload: discord.RawReactionClearEvent) -> None:
    if not live_relay_enabled or payload.guild_id != cfg.source_guild_id:
        return
    await relay_reaction_clear(bot, db, payload)


@bot.event
async def on_raw_reaction_clear_emoji(payload: discord.RawReactionClearEmojiEvent) -> None:
    if not live_relay_enabled or payload.guild_id != cfg.source_guild_id:
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


@bot.command(name="livesync")
@commands.is_owner()
async def livesync_cmd(ctx: commands.Context) -> None:
    """Owner-only: backfill history, then keep relaying new source activity.

    Live relaying is enabled before the backfill starts so messages sent while
    history is being copied cannot slip through the gap. Duplicate relays are
    avoided by the persistent source-message map. Survives bot restarts
    (resumes automatically) until stopped with !stoplivesync / !stopsync.
    """
    if _livesync_running():
        await ctx.reply("Live sync is already running.")
        return
    if not channel_map:
        await ctx.reply("Channel map is empty -- is the bot in both guilds, and do any channel names match?")
        return

    await ctx.reply(
        f"Live relay enabled. Starting backfill across {len(channel_map)} channel(s); "
        "new source messages will sync while it runs. Use `!stoplivesync` to stop."
    )
    await _start_livesync(ctx.channel.id, resumed=False)


@bot.command(name="stoplivesync", aliases=["stopsync"])
@commands.is_owner()
async def stoplivesync_cmd(ctx: commands.Context) -> None:
    """Owner-only: stop live sync (cancels a running backfill and disables live relaying)."""
    global live_relay_enabled, livesync_task

    was_active = (await db.get_setting(LIVESYNC_SETTING)) is not None or _livesync_running()
    await db.delete_setting(LIVESYNC_SETTING)
    task, livesync_task = livesync_task, None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    live_relay_enabled = cfg.live_relay_enabled

    if not was_active:
        await ctx.reply("No live sync is running.")
    else:
        await ctx.reply("Live sync stopped. It won't resume on restart; run `!livesync` to start again.")


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
