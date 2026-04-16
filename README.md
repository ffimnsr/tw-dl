# tw-dl (Telegram Web Downloader)

`tw-dl` is a Rust CLI for downloading Telegram media through MTProto.

It signs in as your normal Telegram account and can download media from chats, channels, and groups your account can already access. It does not bypass Telegram permissions or restrictions.

## What It Does

- Downloads media from Telegram message links
- Supports public links like `https://t.me/channel/123`
- Supports private/supergroup links like `https://t.me/c/1234567890/123`
- Supports direct targeting with `--peer` and `--msg`
- Supports batch downloads from a text file with `--file`
- Supports resumable partial downloads with `--resume`
- Supports collision policies with `--skip-existing`, `--overwrite`, and `--resume`
- Supports retry/backoff for transient network failures
- Supports configurable request, per-item, and batch timeouts
- Supports adaptive pacing after flood-wait/rate-limit responses
- Supports concurrent batch jobs with `--jobs`
- Supports checkpoint manifests for large batch runs
- Supports `--keep-partial` to preserve `.part` files after failed or timed-out downloads
- Supports cached peer/message metadata lookups for faster repeated batch downloads
- Supports optional parallel chunk downloads for large files with `--parallel-chunks`
- Shows transfer throughput, retry count, and backoff state in progress output
- Supports stale session-lock expiry and manual recovery with `--force-unlock`
- Supports metadata inspection without downloading via `inspect`
- Supports diagnostics via `doctor`
- Stores your login session locally so you only need to sign in once
- Loads Telegram API credentials from `.env` automatically

## Quick Start

```bash
git clone https://github.com/ffimnsr/tw-dl
cd tw-dl
cargo build --release

cp .env.example .env
# edit .env and fill in your Telegram API credentials

./target/release/tw-dl login
./target/release/tw-dl download https://t.me/channelname/123
```

## Requirements

- A Telegram account
- Telegram API credentials from <https://my.telegram.org/apps>
- Rust 1.70+ if building from source

## Installation

Build a release binary:

```bash
cargo build --release
```

The compiled binary will be available at:

```bash
./target/release/tw-dl
```

Optional global install:

```bash
cargo install --path .
```

## Configuration

### 1. Get Telegram API Credentials

Go to <https://my.telegram.org/apps> and create an application. Telegram will give you:

- `api_id`
- `api_hash`

Treat these like secrets.

### 2. Create a `.env` File

`tw-dl` loads `.env` automatically with `dotenvy`.

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
```

You can start from the included example:

```bash
cp .env.example .env
```

Shell environment variables also work and take precedence over `.env` values if both are set.

Linux/macOS:

```bash
export TELEGRAM_API_ID=12345678
export TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
```

Windows PowerShell:

```powershell
$env:TELEGRAM_API_ID="12345678"
$env:TELEGRAM_API_HASH="abcdef1234567890abcdef1234567890"
```

## First Login

Authenticate once and `tw-dl` will save your session locally.

```bash
tw-dl login
```

You will be prompted for:

1. Your phone number in international format
2. The login code sent by Telegram
3. Your 2FA password, if enabled

Example:

```bash
$ tw-dl login
Phone number (international format, e.g. +15550001234): +15550001234
Enter the code you received: 12345
Signed in as Alice (id: 123456789)
Session saved to '/Users/alice/.config/tw-dl/session'.
```

## Command Overview

Top-level help:

```text
Usage: tw-dl [OPTIONS] <COMMAND>
```

Commands:

- `init` - create `~/.config/tw-dl/config.toml` interactively
- `login` - authenticate and save a session
- `logout` - remove the saved session
- `whoami` - print the authenticated user as JSON
- `list-chats` - list accessible chats as JSON
- `resolve` - resolve a link, username, or numeric id as JSON
- `download` - download media from one or more Telegram messages
- `inspect` - inspect a message and print metadata as JSON
- `doctor` - validate config, session, and authorization state

Global options:

- `--session-path <FILE>`: use a custom session file
- `--yes`: assume yes for confirmation prompts
- `--force-unlock`: remove an existing session lock before opening the session
- `--stale-lock-age-secs <SECS>`: treat session locks older than this as stale
- `-h, --help`: show help
- `-V, --version`: show version

## Commands

### `init`

Create `~/.config/tw-dl/config.toml` interactively.

```bash
tw-dl init
```

Options:

- `--force`: overwrite an existing config file
- `--yes`: skip overwrite confirmation when `--force` is used

Examples:

```bash
tw-dl init
tw-dl init --force
```

### `login`

Authenticate interactively and save your session.

```bash
tw-dl login
```

Options:

- `--session-path <FILE>`: path to the session file
- `--yes`: skip session removal confirmation

Examples:

```bash
tw-dl login
tw-dl --session-path ~/.config/tw-dl/work.session login
```

### `logout`

Remove the saved Telegram session file.

```bash
tw-dl logout
```

Options:

- `--session-path <FILE>`: path to the session file

### `whoami`

Print the currently authenticated user as JSON.

```bash
tw-dl whoami
```

Options:

- `--session-path <FILE>`: path to the session file

Example output:

```json
{
  "first_name": "Alice",
  "id": 123456789,
  "last_name": null,
  "phone": "+15550001234",
  "username": "alice"
}
```

### `download`

Download media from a Telegram message.

```bash
tw-dl download [OPTIONS] [LINK]
```

Arguments:

- `[LINK]`: Telegram message link such as `https://t.me/...` or `https://t.me/c/...`

Options:

- `--peer <USERNAME_OR_ID>`: peer username or numeric channel ID
- `--msg <ID>`: message ID used together with `--peer`
- `-f, --file <FILE>`: file containing one Telegram link per line for batch download
- `-o, --out <DIR>`: output directory
- `--skip-existing`: skip downloads if the final output file already exists
- `--overwrite`: overwrite existing output files or partial files
- `--resume`: resume from an existing `.part` file when possible
- `--retries <N>`: retry transient failures up to `N` times
- `--retry-delay-ms <MS>`: initial retry delay in milliseconds
- `--max-retry-delay-ms <MS>`: maximum retry delay in milliseconds
- `--jobs <N>`: number of concurrent jobs in batch mode
- `--checkpoint <FILE>`: JSONL checkpoint manifest for batch mode
- `--dry-run`: preview what would be downloaded without writing files
- `--retry-from <FILE>`: replay only failed links from a previous manifest/checkpoint
- `--parallel-chunks <N>`: number of parallel chunk workers for large-file downloads
- `--keep-partial`: keep `.part` files after download errors or timeouts
- `--request-timeout-ms <MS>`: timeout for individual network requests
- `--item-timeout-ms <MS>`: timeout for a single download item
- `--batch-timeout-ms <MS>`: timeout for the entire batch run
- `--session-path <FILE>`: path to the session file
- `--yes`: reserved for future confirmation flows and non-interactive usage

Notes:

- `--file` conflicts with `LINK`, `--peer`, and `--msg`
- `--retry-from` conflicts with `LINK`, `--peer`, `--msg`, and `--file`
- default output directory is `./downloads`
- `--skip-existing`, `--overwrite`, and `--resume` are mutually exclusive
- `--checkpoint` is valid with `--file` and `--retry-from`
- `--parallel-chunks` is most useful for large fresh downloads; resumed partial files continue with the sequential resume path
- `--keep-partial` is useful with `--item-timeout-ms` or aggressive retry settings when you want to inspect or resume partial results later
- bare numeric `--peer` values are accepted but less safe than full Telegram links or `@username` targets

### `list-chats`

List accessible chats, groups, channels, and their latest message summary as JSON.

```bash
tw-dl list-chats
```

### `resolve`

Resolve a Telegram message link, username, or numeric chat id and print what `tw-dl` can infer about it.

```bash
tw-dl resolve https://t.me/mychannel/42
tw-dl resolve mychannel
tw-dl resolve @mychannel
tw-dl resolve 1234567890
```

### `inspect`

Inspect a Telegram message and print metadata as JSON without downloading media.

```bash
tw-dl inspect [OPTIONS] [LINK]
```

Arguments:

- `[LINK]`: Telegram message link such as `https://t.me/...` or `https://t.me/c/...`

Options:

- `--peer <USERNAME_OR_ID>`: peer username or numeric channel ID
- `--msg <ID>`: message ID used together with `--peer`
- `--session-path <FILE>`: path to the session file

### `doctor`

Validate local configuration, session state, and whether the saved session is authorized.

```bash
tw-dl doctor
```

Options:

- `--session-path <FILE>`: path to the session file

## Download Examples

Download from a public channel:

```bash
tw-dl download https://t.me/mychannel/42
```

Download from a private channel or supergroup:

```bash
tw-dl download https://t.me/c/1234567890/10
```

Download using a username and message ID:

```bash
tw-dl download --peer mychannel --msg 42
```

Download using a numeric channel ID:

```bash
tw-dl download --peer 1234567890 --msg 42
```

Use a profile-like named session for work vs personal accounts:

```bash
tw-dl --session-path ~/.config/tw-dl/work.session login
tw-dl --session-path ~/.config/tw-dl/work.session download https://t.me/mychannel/42
```

Download to a specific directory:

```bash
tw-dl download https://t.me/mychannel/42 --out ./media
```

Batch download from a file:

```bash
tw-dl download --file links.txt --out ./media
```

Batch download with retries, concurrency, and a checkpoint manifest:

```bash
tw-dl download \
  --file links.txt \
  --out ./media \
  --jobs 4 \
  --parallel-chunks 4 \
  --resume \
  --retries 5 \
  --retry-delay-ms 1000 \
  --max-retry-delay-ms 30000 \
  --checkpoint ./links.checkpoint.jsonl
```

Batch download with explicit timeouts and preserved partials:

```bash
tw-dl download \
  --file links.txt \
  --jobs 4 \
  --resume \
  --keep-partial \
  --request-timeout-ms 15000 \
  --item-timeout-ms 300000 \
  --batch-timeout-ms 7200000
```

Skip existing completed files:

```bash
tw-dl download --file links.txt --skip-existing
```

Overwrite existing outputs:

```bash
tw-dl download https://t.me/mychannel/42 --overwrite
```

Resume an interrupted download:

```bash
tw-dl download https://t.me/mychannel/42 --resume
```

Preview what would happen without downloading:

```bash
tw-dl download https://t.me/mychannel/42 --dry-run
tw-dl download --file links.txt --dry-run --jobs 4
```

Replay only failed links from a previous manifest:

```bash
tw-dl download --retry-from ./links.checkpoint.jsonl --resume --jobs 4 --parallel-chunks 4
```

Use a dedicated checkpoint file for a private-link batch:

```bash
tw-dl download \
  --file private-links.txt \
  --checkpoint ./private-links.checkpoint.jsonl \
  --resume \
  --retries 5
```

Use parallel chunk downloading for large files:

```bash
tw-dl download https://t.me/mychannel/42 --parallel-chunks 4
```

Example `links.txt`:

```text
# one Telegram link per line
https://t.me/channelname/101
https://t.me/channelname/102
https://t.me/c/1234567890/15
```

Use a custom session:

```bash
tw-dl --session-path ~/.config/tw-dl/work.session download https://t.me/channel/123
```

Recover from a stale session lock:

```bash
tw-dl --force-unlock whoami
tw-dl --stale-lock-age-secs 300 doctor
```

Inspect a message without downloading:

```bash
tw-dl inspect https://t.me/mychannel/42
tw-dl inspect --peer mychannel --msg 42
```

Run diagnostics:

```bash
tw-dl doctor
```

Skip confirmation prompts in automation:

```bash
tw-dl --yes init --force
tw-dl --yes logout
```

List accessible chats:

```bash
tw-dl list-chats
```

Resolve a link or peer input:

```bash
tw-dl resolve https://t.me/mychannel/42
tw-dl resolve @mychannel
tw-dl resolve 1234567890
```

## Supported Media

`tw-dl` downloads media Telegram exposes as downloadable documents or photos, including common:

- videos
- audio files
- documents
- archives
- photos

If a message has no downloadable media, the command will fail with a clear error.

## Progress and Output

During downloads, progress is shown on stderr.

The progress display includes:

- transfer throughput
- ETA for sized downloads
- total retry count
- current backoff delay when a retry is pending
- whether parallel chunk mode is enabled

Successful downloads print JSON to stdout, for example:

```json
{
  "message_id": 42,
  "mime_type": "video/mp4",
  "peer_id": -1001234567890,
  "file": "downloads/video.mp4",
  "filename": "video.mp4",
  "resumed": false,
  "size": 104857600,
  "status": "downloaded"
}
```

This makes the tool easy to use both interactively and from scripts.

Batch runs also print one JSON object per processed item to stdout and append progress records to the checkpoint JSONL file when `--file` is used.
Dry runs also print JSON, but with `"status": "planned"` and a collision preview instead of downloading files.

For scripting, `tw-dl` now returns different non-zero exit codes for common failure classes:

- `1`: general runtime failure
- `2`: usage/input error
- `3`: auth/session failure
- `4`: access/permission failure
- `5`: file collision or local output-state failure

`inspect` prints message metadata as JSON, for example:

```json
{
  "date": "2026-04-16T08:54:31+00:00",
  "has_media": true,
  "id": 42,
  "media": {
    "filename": "video.mp4",
    "id": 1234567890123456789,
    "mime_type": "video/mp4",
    "size": 104857600,
    "type": "document"
  },
  "peer": {
    "id": -1001234567890,
    "name": "My Channel",
    "type": "channel",
    "username": "mychannel",
    "usernames": [
      "mychannel"
    ]
  },
  "peer_id": -1001234567890,
  "sender": null,
  "text": "Episode 42"
}
```

## Session Storage

Session files are stored as SQLite databases.

Default session locations:

- Linux/macOS: `~/.config/tw-dl/session`
- Windows: `%APPDATA%\tw-dl\session`

Security notes:

- session files authenticate your Telegram account
- do not commit them to version control
- do not share them
- restrict file permissions where possible

To sign out, delete the session file and log in again later if needed.
You can also run:

```bash
tw-dl logout
```

## Finding Telegram Message Links

For public channels and groups:

1. Open the message in Telegram
2. Open the message menu
3. Copy the message link

Example format:

```text
https://t.me/channelname/123
```

For private channels or supergroups, the format usually looks like:

```text
https://t.me/c/1234567890/123
```

## Troubleshooting

### `TELEGRAM_API_ID environment variable is not set`

Create a `.env` file or export the variable in your shell.

### `Not logged in. Run tw-dl login first.`

Run:

```bash
tw-dl login
```

### `Username '...' not found`

- check the username
- make sure the chat exists
- make sure your account can access it
- use a numeric channel ID if the chat is private

### `Could not find a chat with id=... in your dialogs`

Your account likely has not joined that chat, or the numeric ID is wrong.
Prefer a full Telegram message link or `@username` when possible, because bare numeric peer ids are easier to misuse.

### `This message does not contain downloadable media`

The target message exists, but it does not have downloadable photo/document media attached.

### Access denied / not a member of this channel

If the error mentions a private channel, admin requirement, invite request, or access denial, the account you logged in with cannot currently access that chat or message. Join the chat first or switch to an account that already has access.

### `Temporary download file '...' already exists`

The previous download was interrupted. Re-run with `--resume` to continue from the `.part` file, or `--overwrite` to discard it and restart.

### Batch download stopped partway through

If you used `--file`, re-run the same command with the same checkpoint file. Completed items recorded as `"downloaded"` in the checkpoint manifest will be skipped automatically.

### Session lock exists

If `doctor` reports that the session lock file exists, make sure another `tw-dl` process is not still running. If it is stale, remove the `.lock` file next to the session file.

## Roadmap

Planned feature work is tracked in [ISSUES.md](ISSUES.md).

## License

MIT. See [LICENSE](LICENSE).
