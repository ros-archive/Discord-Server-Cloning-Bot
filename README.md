# disclone

A Discord bot that mirrors messages from one server (the *source*) into
another (the *destination*), matching channels by name. By default it only
sends anything when you explicitly run `!backfill` to copy existing
history; live relaying of new messages is an opt-in setting. Relayed
messages are posted through a per-channel webhook so they show the original
author's name and avatar.

## How it works

- One bot account, invited to **both** the source and destination servers.
- On startup it matches source text channels to destination text channels
  with the same name (preferring same-category matches when names collide).
- New messages in a matched source channel are re-posted in the matching
  destination channel via a webhook (created once per destination channel
  and reused) -- but **only if `ENABLE_LIVE_RELAY=true` is set in `.env`**.
  Left unset (the default), the bot sends nothing until you run
  `!backfill`; it still watches and matches channels, it just doesn't post.
- Edits to relayed messages are kept in sync, including edits to messages
  sent before the bot was last restarted or backfilled. (Only applies to
  messages that were actually relayed, whether live or via backfill.)
- Reactions are mirrored as text: whenever reactions on a source message
  change, the bot re-edits the relayed message (via the webhook) appending
  a summary line like `👍 3  😂 1` below the original content. It's rebuilt
  from a fresh read of the source message each time, so it can't drift out
  of sync and always shows exact counts.
- `!backfill` walks each matched channel (oldest first) and relays anything
  not already relayed, so it's safe to re-run. Each channel's progress is
  checkpointed, so a re-run resumes from where the last one left off
  instead of rescanning the channel's entire history -- see
  [Backfilling existing history](#5-backfill-existing-history).
- Custom emojis are cloned from the source server into the destination
  server, so destination members can actually use them (react, type them),
  not just see them rendered in relayed content. `!clone-emojis` backfills
  everything currently in the source server; any emoji added afterwards is
  cloned automatically.

## Limitations

- **Text channels only** in this version — no threads, forums, or voice
  channel text chat.
- **Deletes are not synced** — deleting the original message does not
  delete the mirrored copy.
- **Reactions show as a text line, not real reaction buttons** on the
  mirrored message — nobody can click them to react in the destination
  server; it's a readout of the source server's reaction counts.
- **Stickers and reply-context** are not reproduced (Discord webhooks can't
  attach a "replying to" reference or stickers).
- Auto-generated embeds (link previews, image/video unfurls) are not
  forwarded — only explicit "rich" embeds are, to avoid duplicate previews.
- Attachments over ~25MB are not re-uploaded; a link to the original is
  posted instead, and that link **will eventually expire** (Discord CDN
  URLs are signed and time-limited).
- Cross-server @mentions in relayed content won't resolve to real pings in
  the destination server.
- **Cloned emojis aren't kept deleted in sync either** — removing an emoji
  from the source server leaves its destination clone in place. Renaming a
  source emoji doesn't rename its clone (the two are matched by ID, not
  name, once cloned).
- Emoji cloning stops once the destination server's static/animated emoji
  slots are full (capped by its boost level); `!clone-emojis` reports how
  many failed rather than erroring out.
- Owner commands (`!backfill`, `!livesync`, `!resync`, `!clone-emojis`,
  `!help`) only
  work when typed in the **destination** server — they're silently ignored
  if typed in the source server, so source-server members never see them.

## Setup

### 1. Create the bot application

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications) → **New Application**.
2. **Bot** tab → note the token (you'll put it in `.env`) → keep **Public Bot** off if you don't want strangers adding it.
3. Under **Privileged Gateway Intents**, enable **Message Content Intent**.

### 2. Invite it to both servers

Generate an invite URL (**OAuth2 → URL Generator**), scope `bot`, with these
permissions:

- View Channels
- Read Message History
- Send Messages
- Manage Webhooks
- Embed Links
- Attach Files
- Manage Expressions (needed for `!clone-emojis` to create emojis in the
  destination server)

Use that URL twice — once authorizing it into the source server, once into
the destination server. You need "Manage Server" (or an admin's help) on
both.

### 3. Configure

```bash
cp .env.example .env
```

Fill in `DISCORD_TOKEN`, `SOURCE_GUILD_ID`, and `DEST_GUILD_ID` (right-click
a server icon with Developer Mode on → Copy Server ID). Leave
`ENABLE_LIVE_RELAY` unset/`false` unless you want new messages relayed
automatically as they're posted, in addition to `!backfill`.

### 4. Install and run

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python bot.py
```

> **macOS + python.org Python note:** `bot.py` sets `SSL_CERT_FILE` to
> `certifi`'s CA bundle before importing `discord`/`aiohttp`, so it works
> out of the box even though python.org's macOS installers ship a Python
> that doesn't use the system trust store. If you ever see
> `CERTIFICATE_VERIFY_FAILED` anyway (e.g. running some other script with
> this same Python that doesn't do that), run
> `/Applications/Python\ 3.x/Install\ Certificates.command` once.

On connect, the log prints how many channels matched by name. Channels
that don't exist (by name) in the destination server are skipped and
logged — create them there and run `!resync` (as the bot owner, in any
channel it can see) to pick them up without restarting.

### 5. Backfill existing history

Commands only work when typed **in the destination server** (they're
ignored in the source server). As the bot owner, in a destination-server
channel the bot can see:

```
!backfill          # resume from each channel's last backfill (full history the first time)
!backfill 200       # on a channel's first backfill, copy only the 200 most recent messages
!backfill full       # ignore saved progress and rescan every channel's full history
!livesync            # backfill, then relay new messages for this bot session
```

Safe to re-run — already-relayed messages (tracked in `relay.db`) are
skipped. `!backfill` is the only history-only option; use
`ENABLE_LIVE_RELAY=true` or `!livesync` when new messages should continue
relaying too.

`!livesync` turns on live message, edit, and reaction relaying for the
current bot process, then runs the normal resumable backfill. Live relaying
starts immediately (before the history walk) so messages posted during a
long backfill are not missed. It remains on until the bot restarts; after a
restart, `ENABLE_LIVE_RELAY` again controls whether it starts enabled.

Re-running `!backfill` doesn't rescan history from the start: each
channel's progress (the last source message id it walked) is checkpointed
in `relay.db`, so a normal re-run only asks Discord for messages posted
since the previous backfill. `limit` only applies on a channel's first
backfill (no checkpoint yet) or under `full`, since "N most recent"
doesn't have a stable meaning applied to just the messages since last
time.

Messages that fail to send because of a rate limit or a Discord-side error
are queued and retried automatically on the next `!backfill` (up to 3
attempts, by re-fetching that specific message rather than rescanning).
Messages Discord will reject no matter how many times you send them
(e.g. sticker-only messages, which resolve to an empty payload) are
recognized as permanent and skipped once rather than retried forever.

**Upgrading an existing `relay.db`:** the first time you run this version,
each channel's saved progress is seeded from the highest message id
already in `relay.db`, on the assumption that everything up to that point
was relayed by a straight oldest-first `!backfill` walk. If that
`relay.db` was ever populated with `ENABLE_LIVE_RELAY=true` (which relays
new messages out of order relative to a backfill walk) or from a run that
stopped partway through, that assumption can be wrong for a channel and
older un-relayed messages could be silently skipped from then on. Run
`!backfill full` once after upgrading to rescan everything and be sure;
after that, normal `!backfill` runs resume safely on their own.

## Files

| File | Purpose |
|---|---|
| `bot.py` | Entry point, event handlers, commands |
| `config.py` | `.env` loading/validation |
| `relay.py` | Channel matching, webhook management, message forwarding |
| `db.py` | SQLite-backed webhook cache + source→dest message id map |
| `backfill.py` | History backfill logic |
| `relay.db` | Created on first run; safe to delete to reset all mappings (will recreate webhooks and, on next `!backfill`, re-send everything). Also stores per-channel backfill progress and the queue of failed messages awaiting retry. |
