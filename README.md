# tw-dl

`tw-dl` is a Rust CLI for downloading Telegram media through MTProto.

It signs in as your normal Telegram account and can download media from chats, channels, and groups your account can already access. It does not bypass Telegram permissions or restrictions.

## What It Does

- Downloads media from Telegram message links
- Supports public links like `https://t.me/channel/123`
- Supports private/supergroup links like `https://t.me/c/1234567890/123`
- Supports direct targeting with `--peer` and `--msg`
- Supports batch downloads from a text file with `--file`
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

- `login` - authenticate and save a session
- `whoami` - print the authenticated user as JSON
- `download` - download media from one or more Telegram messages

Global options:

- `--session-path <FILE>`: use a custom session file
- `-h, --help`: show help
- `-V, --version`: show version

## Commands

### `login`

Authenticate interactively and save your session.

```bash
tw-dl login
```

Options:

- `--session-path <FILE>`: path to the session file

Examples:

```bash
tw-dl login
tw-dl --session-path ~/.config/tw-dl/work.session login
```

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
- `--session-path <FILE>`: path to the session file

Notes:

- `--file` conflicts with `LINK`, `--peer`, and `--msg`
- default output directory is `./downloads`

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

Download to a specific directory:

```bash
tw-dl download https://t.me/mychannel/42 --out ./media
```

Batch download from a file:

```bash
tw-dl download --file links.txt --out ./media
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

Successful downloads print JSON to stdout, for example:

```json
{
  "file": "downloads/video.mp4",
  "filename": "video.mp4",
  "mime_type": "video/mp4",
  "size": 104857600
}
```

This makes the tool easy to use both interactively and from scripts.

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

### `This message does not contain downloadable media`

The target message exists, but it does not have downloadable photo/document media attached.

## License

MIT. See [LICENSE](LICENSE).
