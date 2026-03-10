# tw-dl

A Rust CLI for downloading Telegram media (videos, documents, audio, photos) via MTProto.

`tw-dl` authenticates as a regular Telegram user and downloads media from messages in channels or groups that your account is a member of and can legitimately view. It does **not** bypass any Telegram restrictions.

## Quick Start

```bash
# 1. Get API credentials from https://my.telegram.org/apps
# 2. Clone and build
git clone https://github.com/ffimnsr/tw-dl
cd tw-dl
cargo build --release

# 3. Create a .env file
cat > .env <<'EOF'
TELEGRAM_API_ID=your_api_id
TELEGRAM_API_HASH=your_api_hash
EOF

# 4. Login once
./target/release/tw-dl login

# 5. Download media
./target/release/tw-dl download https://t.me/channelname/123
```

## Requirements

- **Telegram account**: You need an active Telegram account (phone number)
- **Telegram API credentials**: `api_id` and `api_hash` from <https://my.telegram.org/apps>
- **Rust toolchain** (1.70+) for building from source

## Getting Started

### Step 1: Obtain Telegram API Credentials

1. Go to <https://my.telegram.org> and log in with your Telegram account
2. Click on **"API development tools"**
3. Fill out the form to create a new application:
   - **App title**: (e.g., "tw-dl")
   - **Short name**: (e.g., "twdl")
   - **Platform**: Choose any (e.g., "Desktop")
   - **Description**: (optional)
4. Click **"Create application"**
5. You'll receive your **api_id** (numeric) and **api_hash** (32-character hex string)
6. **Keep these credentials secure** – treat them like passwords

### Step 2: Installation

```bash
git clone https://github.com/ffimnsr/tw-dl
cd tw-dl
cargo build --release
# Binary is at target/release/tw-dl
```

Optionally, install to your system:
```bash
cargo install --path .
# Now 'tw-dl' is available globally
```

### Step 3: Configuration

Create a `.env` file in the project directory with the credentials from Step 1:

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
```

`tw-dl` loads `.env` automatically at startup using `dotenvy`.

Shell environment variables still work and override values from `.env` when both are present.

**Alternative shell setup (Linux/macOS):**
```bash
export TELEGRAM_API_ID=12345678
export TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
```

**Alternative shell setup (Windows PowerShell):**
```powershell
$env:TELEGRAM_API_ID="12345678"
$env:TELEGRAM_API_HASH="abcdef1234567890abcdef1234567890"
```

## Usage

### Step 4: Login to Telegram

Authenticate with your Telegram account. This only needs to be done **once** – the session is saved to disk.

```bash
tw-dl login
```

**What happens during login:**
1. You'll be prompted to enter your **phone number** (in international format, e.g., `+15550001234`)
2. Telegram will send a **verification code** to your Telegram app (or SMS)
3. Enter the code when prompted
4. If you have **two-factor authentication (2FA)** enabled, you'll be asked for your password
5. Once authenticated, the session is saved to `~/.config/tw-dl/session` (Linux/macOS) or `%APPDATA%\tw-dl\session` (Windows)

**Example:**
```bash
$ tw-dl login
Phone number (international format, e.g. +15550001234): +15550001234
Enter the code you received: 12345
Signed in as Alice (id: 123456789)
Session saved to '/Users/alice/.config/tw-dl/session'.
```

**With 2FA:**
```bash
$ tw-dl login
Phone number (international format, e.g. +15550001234): +15550001234
Enter the code you received: 12345
Two-factor authentication password (hint: your pet's name): ********
Signed in successfully with 2FA.
Session saved to '/Users/alice/.config/tw-dl/session'.
```

**Custom session path:**
```bash
tw-dl --session-path /path/to/my.session login
```
```

### Verify Your Login

Print the currently authenticated user's info as JSON:

```bash
tw-dl whoami
```

**Example output:**
```json
{
  "first_name": "Alice",
  "id": 123456789,
  "last_name": null,
  "phone": "+15550001234",
  "username": "alice"
}
```

### Step 5: Download Videos and Media

Download media (videos, documents, photos, audio) from Telegram messages in channels or groups that you have access to.

#### Basic Usage

**Download from a public channel:**
```bash
tw-dl download https://t.me/mychannel/42
```

**Download from a private channel/supergroup:**
```bash
# You must be a member of the channel
tw-dl download https://t.me/c/1234567890/10
```

**Download using username and message ID:**
```bash
tw-dl download --peer mychannel --msg 42
# Or with channel ID
tw-dl download --peer 1234567890 --msg 42
```

**Specify output directory:**
```bash
tw-dl download https://t.me/mychannel/42 --out ./my-videos
# Default output directory is ./downloads
```

#### Finding Telegram Message Links

**For public channels/groups:**
1. Open the channel in Telegram
2. Click on a message with media
3. Click the three dots (⋮) → "Copy Message Link"
4. The link looks like: `https://t.me/channelname/123`

**For private channels/supergroups:**
1. The link looks like: `https://t.me/c/1234567890/123`
2. Right-click on a message → "Copy Message Link"

#### Download Output

On success, `tw-dl` prints a JSON object with file details:
```json
{
  "file": "downloads/video.mp4",
  "filename": "video.mp4",
  "mime_type": "video/mp4",
  "size": 104857600
}
```

A **progress bar** is shown on stderr while downloading:
```
Downloading: video.mp4
████████████████████████████████░░░░░░░░ 75% (78 MB / 104 MB)
```

#### Supported Media Types

- **Videos** (MP4, MKV, AVI, etc.)
- **Documents** (PDF, ZIP, etc.)
- **Photos** (JPEG, PNG, etc.)
- **Audio** (MP3, FLAC, etc.)

#### Important Notes

⚠️ **Limitations:**
- You can only download media from channels/groups you are a member of
- `tw-dl` respects all Telegram restrictions and permissions
- Private media requires proper access rights
- Large files may take time depending on your connection speed

#### Troubleshooting

**"Not logged in" error:**
```bash
tw-dl login
```

**"Username not found" error:**
- Verify the channel username is correct
- Ensure you're a member of the channel/group
- For private channels, use the numeric channel ID instead

**"Failed to resolve username" error:**
- The channel may not exist or is private
- Use the full message link instead

## Advanced Usage

### Multiple Accounts

You can manage multiple Telegram accounts using different session files:

```bash
# Login with first account
tw-dl --session-path ~/.config/tw-dl/work.session login

# Login with second account
tw-dl --session-path ~/.config/tw-dl/personal.session login

# Download using specific account
tw-dl --session-path ~/.config/tw-dl/work.session download https://t.me/channel/123
```

### Batch Downloads

Download multiple videos by running the command multiple times:

```bash
tw-dl download https://t.me/channel/1 --out ./videos
tw-dl download https://t.me/channel/2 --out ./videos
tw-dl download https://t.me/channel/3 --out ./videos
```

Or use a shell script:
```bash
#!/bin/bash
for msg_id in {1..10}; do
  tw-dl download --peer mychannel --msg $msg_id --out ./videos
done
```

## Session Storage

Sessions are stored as **SQLite databases** and contain your authentication data. They persist across runs, so you only need to login once.

**Default locations:**
- **Linux/macOS**: `~/.config/tw-dl/session`
- **Windows**: `%APPDATA%\tw-dl\session`

**Custom session path:**
```bash
tw-dl --session-path /path/to/custom.session login
tw-dl --session-path /path/to/custom.session download https://t.me/channel/123
```

**Session security:**
- Session files grant access to your Telegram account
- Keep them secure (chmod 600 on Unix systems)
- Don't share or commit them to version control
- To logout, simply delete the session file

## License

MIT – see [LICENSE](LICENSE).
