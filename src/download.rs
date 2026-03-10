use anyhow::{bail, Context, Result};
use grammers_client::media::{Document, Downloadable, Media};
use grammers_client::message::Message;
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::AsyncBufReadExt;

use crate::link::{parse_link, ParsedLink};

/// Options for the `download` command.
pub struct DownloadArgs {
    pub link: Option<String>,
    pub peer: Option<String>,
    pub msg_id: Option<i32>,
    pub out_dir: PathBuf,
    /// Path to a file containing one Telegram link per line.
    pub file_list: Option<PathBuf>,
}

/// Entry-point for the `download` subcommand.
pub async fn cmd_download(
    api_id: i32,
    session_path: PathBuf,
    args: DownloadArgs,
) -> Result<()> {
    let client = crate::auth::build_client(api_id, &session_path).await?;

    if !client.is_authorized().await? {
        bail!("Not logged in. Run `tw-dl login` first.");
    }

    if let Some(list_path) = &args.file_list {
        // Batch mode: read links from file, one per line
        let file = fs::File::open(list_path)
            .await
            .with_context(|| format!("Cannot open file list '{}'" , list_path.display()))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut index: usize = 0;
        while let Some(line) = lines.next_line().await? {
            let link = line.trim().to_string();
            // Skip blank lines and comments
            if link.is_empty() || link.starts_with('#') {
                continue;
            }
            index += 1;
            eprintln!("[{}] {}", index, link);
            let single = DownloadArgs {
                link: Some(link.clone()),
                peer: None,
                msg_id: None,
                out_dir: args.out_dir.clone(),
                file_list: None,
            };
            match download_one(&client, &single).await {
                Ok(result) => println!("{}", serde_json::to_string_pretty(&result)?),
                Err(e) => eprintln!("[{}] ERROR {}: {:#}", index, link, e),
            }
        }
    } else {
        // Single download
        let result = download_one(&client, &args).await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
    }

    Ok(())
}

/// Download a single link/peer+msg defined in `args`.
async fn download_one(client: &Client, args: &DownloadArgs) -> Result<serde_json::Value> {
    let (peer_spec, msg_id) = resolve_peer_msg(args)?;
    let message = fetch_message(client, &peer_spec, msg_id).await?;
    download_media(client, &message, &args.out_dir).await
}

// ── peer / message resolution ─────────────────────────────────────────────────

/// A specification of how to find the peer for a message.
enum PeerSpec {
    Username(String),
    ChannelId(i64),
}

fn resolve_peer_msg(args: &DownloadArgs) -> Result<(PeerSpec, i32)> {
    // Explicit --peer / --msg flags take priority
    if let (Some(peer), Some(msg)) = (&args.peer, args.msg_id) {
        let spec = if let Ok(id) = peer.parse::<i64>() {
            PeerSpec::ChannelId(id)
        } else {
            PeerSpec::Username(peer.trim_start_matches('@').to_string())
        };
        return Ok((spec, msg));
    }

    // Otherwise parse the positional link
    let link_str = args
        .link
        .as_deref()
        .context("Provide a link or --peer / --msg")?;

    match parse_link(link_str)? {
        ParsedLink::Username { username, msg_id } => Ok((PeerSpec::Username(username), msg_id)),
        ParsedLink::Channel { channel_id, msg_id } => {
            Ok((PeerSpec::ChannelId(channel_id), msg_id))
        }
    }
}

/// Resolve `PeerSpec` to a `PeerRef` and fetch the specified message.
async fn fetch_message(client: &Client, spec: &PeerSpec, msg_id: i32) -> Result<Message> {
    let peer_ref: PeerRef = match spec {
        PeerSpec::Username(username) => {
            let peer = client
                .resolve_username(username)
                .await
                .with_context(|| format!("Failed to resolve username '{}'", username))?
                .with_context(|| format!("Username '{}' not found", username))?;
            peer.to_ref()
                .await
                .with_context(|| format!("Could not obtain a usable reference for '@{}'", username))?
        }

        PeerSpec::ChannelId(channel_id) => {
            find_peer_ref_by_id(client, *channel_id).await?
        }
    };

    let mut messages = client
        .get_messages_by_id(peer_ref, &[msg_id])
        .await
        .with_context(|| format!("Failed to fetch message id={}", msg_id))?;

    let message = messages
        .pop()
        .flatten()
        .with_context(|| format!("Message id={} not found in chat", msg_id))?;

    Ok(message)
}

/// Iterate dialogs to find a channel/supergroup/group by its bare numeric id
/// and return a usable `PeerRef`.
async fn find_peer_ref_by_id(client: &Client, target_id: i64) -> Result<PeerRef> {
    let mut dialogs = client.iter_dialogs();
    while let Some(dialog) = dialogs.next().await? {
        let peer = dialog.peer();
        // bare_id() gives back the "raw" Telegram API channel/chat/user id.
        // For channels it matches the number in t.me/c/<channel_id> links.
        let matches = match peer {
            Peer::Channel(ch) => ch.id().bare_id() == target_id,
            Peer::Group(g) => g.id().bare_id() == target_id,
            Peer::User(u) => u.id().bare_id() == target_id,
        };
        if matches {
            return Ok(dialog.peer_ref());
        }
    }
    bail!(
        "Could not find a chat with id={} in your dialogs. \
         Make sure your account has joined that channel/group.",
        target_id
    );
}

// ── media download ────────────────────────────────────────────────────────────

/// Download the media attached to `message` into `out_dir`.
///
/// Returns a JSON value with metadata (file path, size, mime type, etc.).
async fn download_media(
    client: &Client,
    message: &Message,
    out_dir: &Path,
) -> Result<serde_json::Value> {
    let media = message
        .media()
        .context("This message does not contain downloadable media")?;

    fs::create_dir_all(out_dir)
        .await
        .with_context(|| format!("Failed to create output directory '{}'", out_dir.display()))?;

    match media {
        Media::Document(ref doc) => {
            let filename = choose_filename_document(doc, message);
            let out_path = out_dir.join(&filename);
            let total = doc.size().unwrap_or(0) as u64;
            let mime = doc
                .mime_type()
                .unwrap_or("application/octet-stream")
                .to_string();

            stream_download(client, doc, &out_path, total).await?;

            let size = fs::metadata(&out_path).await?.len();
            Ok(json!({
                "file": out_path.display().to_string(),
                "filename": filename,
                "size": size,
                "mime_type": mime,
            }))
        }

        Media::Photo(ref photo) => {
            let filename = format!(
                "photo_{}_{}.jpg",
                message.peer_id().bot_api_dialog_id(),
                message.id()
            );
            let out_path = out_dir.join(&filename);

            stream_download(client, photo, &out_path, 0).await?;

            let size = fs::metadata(&out_path).await?.len();
            Ok(json!({
                "file": out_path.display().to_string(),
                "filename": filename,
                "size": size,
                "mime_type": "image/jpeg",
            }))
        }

        other => {
            bail!(
                "Unsupported media type: {:?}. Only documents and photos are supported.",
                other
            );
        }
    }
}

/// Choose a filename for a document message.
fn choose_filename_document(doc: &Document, message: &Message) -> String {
    if let Some(name) = doc.name() {
        if !name.is_empty() {
            return sanitize_filename(name);
        }
    }

    // Derive from MIME type
    let ext = mime_to_ext(doc.mime_type().unwrap_or(""));
    format!(
        "file_{}_{}{}",
        message.peer_id().bot_api_dialog_id(),
        message.id(),
        ext
    )
}

/// Stream-download `downloadable` to `out_path`, showing a live progress bar.
async fn stream_download<D: Downloadable>(
    client: &Client,
    downloadable: &D,
    out_path: &Path,
    total_bytes: u64,
) -> Result<()> {
    let pb = if total_bytes > 0 {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] \
                 {bytes}/{total_bytes} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bytes} downloaded",
            )
            .unwrap(),
        );
        pb
    };

    let mut file = fs::File::create(out_path)
        .await
        .with_context(|| format!("Failed to create file '{}'", out_path.display()))?;

    let mut download = client.iter_download(downloadable);
    while let Some(chunk) = download
        .next()
        .await
        .map_err(|e| anyhow::anyhow!("Download error: {}", e))?
    {
        file.write_all(&chunk)
            .await
            .with_context(|| format!("Failed to write to '{}'", out_path.display()))?;
        pb.inc(chunk.len() as u64);
    }

    file.flush()
        .await
        .with_context(|| format!("Failed to flush '{}'", out_path.display()))?;

    pb.finish_with_message(format!("Saved to '{}'", out_path.display()));

    Ok(())
}

// ── utilities ─────────────────────────────────────────────────────────────────

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c => c,
        })
        .collect()
}

fn mime_to_ext(mime: &str) -> &str {
    match mime {
        "video/mp4" => ".mp4",
        "video/x-matroska" => ".mkv",
        "video/webm" => ".webm",
        "video/quicktime" => ".mov",
        "audio/mpeg" => ".mp3",
        "audio/ogg" => ".ogg",
        "audio/opus" => ".opus",
        "audio/aac" => ".aac",
        "audio/flac" => ".flac",
        "image/jpeg" => ".jpg",
        "image/png" => ".png",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "application/pdf" => ".pdf",
        "application/zip" => ".zip",
        "application/x-tar" => ".tar",
        "application/gzip" => ".gz",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("foo/bar.mp4"), "foo_bar.mp4");
        assert_eq!(sanitize_filename("normal.mkv"), "normal.mkv");
        assert_eq!(sanitize_filename("a:b*c?d"), "a_b_c_d");
    }

    #[test]
    fn test_mime_to_ext() {
        assert_eq!(mime_to_ext("video/mp4"), ".mp4");
        assert_eq!(mime_to_ext("audio/mpeg"), ".mp3");
        assert_eq!(mime_to_ext("unknown/type"), "");
    }
}

