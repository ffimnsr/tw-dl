# tw-dl

A Rust CLI for downloading Telegram media (videos, documents, audio, photos) via MTProto.

`tw-dl` authenticates as a regular Telegram user and downloads media from messages in channels or groups that your account is a member of and can legitimately view. It does **not** bypass any Telegram restrictions.

## Requirements

- A Telegram API application: create one at <https://my.telegram.org/apps> to get your `api_id` and `api_hash`.
- Rust toolchain (1.70+) for building from source.

## Installation

```bash
git clone https://github.com/ffimnsr/tw-dl
cd tw-dl
cargo build --release
# Binary is at target/release/tw-dl
```

## Configuration

Set the following environment variables before using `tw-dl`:

| Variable           | Description                                  |
|--------------------|----------------------------------------------|
| `TELEGRAM_API_ID`  | Your Telegram API ID (numeric)               |
| `TELEGRAM_API_HASH`| Your Telegram API hash (hex string)          |

```bash
export TELEGRAM_API_ID=12345
export TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
```

## Commands

### Login

Authenticate with your Telegram account. The session is persisted to disk (SQLite) so you only need to do this once.

```bash
tw-dl login
# Enter your phone number and the code sent to your Telegram app.
# Two-factor authentication (2FA) is also supported.
```

A custom session path can be specified:

```bash
tw-dl --session-path /path/to/my.session login
```

### Who am I?

Print the currently authenticated user's info as JSON:

```bash
tw-dl whoami
```

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

### Download media

Download media from a Telegram message link:

```bash
# Public channel/group message
tw-dl download https://t.me/mychannel/42

# Private/supergroup message (you must be a member)
tw-dl download https://t.me/c/1234567890/10

# Using explicit peer + message id
tw-dl download --peer mychannel --msg 42

# Specify output directory (default: ./downloads)
tw-dl download https://t.me/mychannel/42 --out ./videos
```

On success, a JSON object is printed to stdout:
```json
{
  "file": "downloads/video.mp4",
  "filename": "video.mp4",
  "mime_type": "video/mp4",
  "size": 104857600
}
```

A progress bar is shown on stderr while downloading.

## Session storage

Sessions are stored as SQLite databases. The default location is:

- Linux/macOS: `~/.config/tw-dl/session`
- Windows: `%APPDATA%\tw-dl\session`

Override with `--session-path <FILE>`.

## License

MIT – see [LICENSE](LICENSE).
