"""Loads and validates configuration from environment variables / .env."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _require_int(name: str) -> int:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Env var {name} must be an integer, got: {value!r}") from exc


def _parse_bool(value: str | None, *, default: bool) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Config:
    token: str
    source_guild_id: int
    dest_guild_id: int
    command_prefix: str
    live_relay_enabled: bool


def load_config() -> Config:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("Missing required env var: DISCORD_TOKEN")

    return Config(
        token=token,
        source_guild_id=_require_int("SOURCE_GUILD_ID"),
        dest_guild_id=_require_int("DEST_GUILD_ID"),
        command_prefix=os.getenv("COMMAND_PREFIX", "!"),
        # Off by default: with this unset, the bot only ever sends messages
        # when explicitly told to via !backfill (new messages, edits, and
        # reactions on the source server are otherwise ignored). Set to
        # true/1/yes in .env to also relay new messages live.
        live_relay_enabled=_parse_bool(os.getenv("ENABLE_LIVE_RELAY"), default=False),
    )
