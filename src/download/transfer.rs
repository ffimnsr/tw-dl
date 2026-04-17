//! Media download I/O: streaming, parallel chunks, collision handling, sidecars,
//! file naming and output-path resolution.

use anyhow::{bail, Context, Result};
use grammers_client::media::{Document, Downloadable, Media};
use grammers_client::message::Message;
use grammers_client::Client;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::task::JoinSet;

use super::client::{run_request, ResilientClient};
use super::describe::{canonical_source_link, describe_message, describe_peer};
use super::resolver::{
    fetch_messages_with_retry, warn_about_ambiguous_selector, MessageSelectorArgs,
};
use super::retry::{prepare_retry, retryable_delay};
use super::types::{
    CaptionSidecarFormat, CollisionPolicy, MediaVariant, OutputLayout, RetryConfig, TimeoutConfig,
};

const DOWNLOAD_CHUNK_SIZE: i32 = 128 * 1024;
const MAX_PARALLEL_CHUNK_SIZE: i32 = 512 * 1024;
const PARALLEL_DOWNLOAD_THRESHOLD: u64 = 10 * 1024 * 1024;

// ── request struct ─────────────────────────────────────────────────────────────

pub(crate) struct SingleDownloadRequest {
    pub(crate) selector: MessageSelectorArgs,
    pub(crate) out_dir: PathBuf,
    pub(crate) collision: CollisionPolicy,
    pub(crate) retry: RetryConfig,
    pub(crate) dry_run: bool,
    pub(crate) media_variant: MediaVariant,
    pub(crate) name_template: Option<String>,
    pub(crate) output_layout: OutputLayout,
    pub(crate) metadata_sidecar: bool,
    pub(crate) caption_sidecar: Option<CaptionSidecarFormat>,
    pub(crate) hash: bool,
    pub(crate) redownload_on_mismatch: bool,
    pub(crate) print_path_only: bool,
    pub(crate) parallel_chunks: usize,
    pub(crate) keep_partial: bool,
    pub(crate) timeouts: TimeoutConfig,
}

// ── media types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct ResolvedDownloadTarget {
    pub(crate) out_path: PathBuf,
    pub(crate) filename: String,
    pub(crate) media_type: String,
    pub(crate) mime_type: String,
    pub(crate) expected_size: u64,
    pub(crate) canonical_source_link: Option<String>,
    pub(crate) grouped_id: Option<i64>,
    pub(crate) caption: String,
    pub(crate) metadata: Value,
}

#[derive(Debug, Clone)]
pub(crate) enum DownloadSource {
    Document(Document),
    Photo(grammers_client::media::Photo),
}

impl DownloadSource {
    pub(crate) fn expected_size(&self) -> u64 {
        match self {
            Self::Document(doc) => doc.size().unwrap_or(0) as u64,
            Self::Photo(photo) => photo.size().unwrap_or(0) as u64,
        }
    }

    pub(crate) fn mime_type(&self) -> String {
        match self {
            Self::Document(doc) => doc
                .mime_type()
                .unwrap_or("application/octet-stream")
                .to_string(),
            Self::Photo(_) => "image/jpeg".to_string(),
        }
    }

    pub(crate) fn media_type(&self) -> &'static str {
        match self {
            Self::Document(_) => "document",
            Self::Photo(_) => "photo",
        }
    }
}

pub(crate) enum DownloadOutcome {
    Downloaded { resumed: bool },
    SkippedExisting,
}

pub(crate) enum PreparedDownload {
    Ready {
        file: fs::File,
        initial_bytes: u64,
        resumed: bool,
    },
    SkippedExisting,
}

struct StreamDownloadConfig {
    total_bytes: u64,
    collision: CollisionPolicy,
    retry: RetryConfig,
    parallel_chunks: usize,
    keep_partial: bool,
    request_timeout: Option<Duration>,
}

struct ParallelDownloadContext<'a, D> {
    client: Client,
    downloadable: D,
    file: fs::File,
    out_path: &'a Path,
    temp_path: &'a Path,
    total_bytes: u64,
    retry: RetryConfig,
    parallel_chunks: usize,
    keep_partial: bool,
    request_timeout: Option<Duration>,
    pb: ProgressBar,
    shutdown: &'a AtomicBool,
}

struct ParallelChunkResult {
    offset: u64,
    bytes: Vec<u8>,
    retry_count: u32,
    last_backoff: Option<Duration>,
}

// ── download entry point ───────────────────────────────────────────────────────

pub(crate) async fn download_one(
    client: &ResilientClient,
    request: &SingleDownloadRequest,
    shutdown: &AtomicBool,
    caches: &super::client::DownloadCaches,
) -> Result<Value> {
    let operation = async {
        warn_about_ambiguous_selector(&request.selector);
        let messages = fetch_messages_with_retry(
            client,
            &request.selector,
            request.retry,
            request.timeouts,
            caches,
        )
        .await?;
        if request.dry_run {
            plan_download(&messages, request).await
        } else {
            download_media(client, &messages, request, shutdown).await
        }
    };

    if let Some(timeout) = request.timeouts.item_timeout {
        tokio::time::timeout(timeout, operation)
            .await
            .context("Per-item timeout exceeded")?
    } else {
        operation.await
    }
}

pub(crate) async fn download_media(
    client: &ResilientClient,
    messages: &[Message],
    request: &SingleDownloadRequest,
    shutdown: &AtomicBool,
) -> Result<Value> {
    let mut items = Vec::new();
    for message in messages {
        let Some((target, source)) = resolve_download_target(request, message).await? else {
            continue;
        };
        let outcome = match &source {
            DownloadSource::Document(doc) => {
                stream_download_verified(client, doc, &target, request, shutdown).await?
            }
            DownloadSource::Photo(photo) => {
                stream_download_verified(client, photo, &target, request, shutdown).await?
            }
        };
        let mut item = download_result_value(
            message,
            &target.out_path,
            target.filename.clone(),
            &target.mime_type,
            target.expected_size,
            outcome,
        );
        augment_item_result(&mut item, &target, message, request).await?;
        items.push(item);
    }

    summarize_download_items(items, request)
}

pub(crate) async fn plan_download(
    messages: &[Message],
    request: &SingleDownloadRequest,
) -> Result<Value> {
    let mut items = Vec::new();
    for message in messages {
        let Some((target, _source)) = resolve_download_target(request, message).await? else {
            continue;
        };
        let collision_result =
            inspect_output_collision(&target.out_path, request.collision).await?;
        items.push(json!({
            "status": "planned",
            "message_id": message.id(),
            "peer_id": message.peer_id().bot_api_dialog_id(),
            "canonical_source_link": target.canonical_source_link,
            "file": target.out_path.display().to_string(),
            "filename": target.filename,
            "mime_type": target.mime_type,
            "size": target.expected_size,
            "media_type": target.media_type,
            "grouped_id": target.grouped_id,
            "collision_policy": collision_policy_name(request.collision),
            "collision_result": collision_result,
            "dry_run": true,
        }));
    }

    summarize_download_items(items, request)
}

// ── download result helpers ────────────────────────────────────────────────────

pub(crate) fn download_result_value(
    message: &Message,
    out_path: &Path,
    filename: String,
    mime_type: &str,
    expected_size: u64,
    outcome: DownloadOutcome,
) -> Value {
    let actual_size = std::fs::metadata(out_path)
        .map(|meta| meta.len())
        .unwrap_or(expected_size);
    let status = match outcome {
        DownloadOutcome::Downloaded { .. } => "downloaded",
        DownloadOutcome::SkippedExisting => "skipped",
    };
    let resumed = matches!(outcome, DownloadOutcome::Downloaded { resumed: true });

    json!({
        "status": status,
        "file": out_path.display().to_string(),
        "filename": filename,
        "size": actual_size,
        "mime_type": mime_type,
        "message_id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "canonical_source_link": canonical_source_link(message),
        "resumed": resumed,
    })
}

async fn augment_item_result(
    item: &mut Value,
    target: &ResolvedDownloadTarget,
    message: &Message,
    request: &SingleDownloadRequest,
) -> Result<()> {
    if let Some(object) = item.as_object_mut() {
        object.insert(
            "media_type".to_string(),
            Value::String(target.media_type.clone()),
        );
        object.insert(
            "grouped_id".to_string(),
            target.grouped_id.map(Value::from).unwrap_or(Value::Null),
        );
    }

    if request.hash && item.get("status").and_then(Value::as_str) == Some("downloaded") {
        let sha256 = sha256_file(&target.out_path)?;
        item.as_object_mut()
            .context("download result must be an object")?
            .insert("sha256".to_string(), Value::String(sha256));
    }

    if request.metadata_sidecar {
        let metadata_path = write_metadata_sidecar(target, message, item).await?;
        item.as_object_mut()
            .context("download result must be an object")?
            .insert(
                "metadata_sidecar".to_string(),
                Value::String(metadata_path.display().to_string()),
            );
    }

    if let Some(format) = request.caption_sidecar {
        if !target.caption.is_empty() {
            let caption_path = write_caption_sidecar(target, format).await?;
            item.as_object_mut()
                .context("download result must be an object")?
                .insert(
                    "caption_sidecar".to_string(),
                    Value::String(caption_path.display().to_string()),
                );
        }
    }
    Ok(())
}

pub(crate) fn summarize_download_items(
    items: Vec<Value>,
    request: &SingleDownloadRequest,
) -> Result<Value> {
    if items.is_empty() {
        bail!("No downloadable media matched the selected media variant");
    }

    let first = items[0].clone();
    let all_statuses: Vec<&str> = items
        .iter()
        .filter_map(|item| item.get("status").and_then(Value::as_str))
        .collect();
    let status = if all_statuses.iter().all(|s| *s == "planned") {
        "planned"
    } else if all_statuses.iter().all(|s| *s == "skipped") {
        "skipped"
    } else {
        "downloaded"
    };
    let files: Vec<Value> = items
        .iter()
        .filter_map(|item| item.get("file").cloned())
        .collect();

    let mut summary = json!({
        "status": status,
        "item_count": items.len(),
        "files": files,
        "items": items,
        "print_path_only": request.print_path_only,
    });

    if let Some(object) = summary.as_object_mut() {
        for key in [
            "file",
            "filename",
            "mime_type",
            "message_id",
            "peer_id",
            "canonical_source_link",
            "grouped_id",
        ] {
            if let Some(value) = first.get(key).cloned() {
                object.insert(key.to_string(), value);
            }
        }
    }

    Ok(summary)
}

// ── sidecar helpers ────────────────────────────────────────────────────────────

async fn write_metadata_sidecar(
    target: &ResolvedDownloadTarget,
    message: &Message,
    item: &Value,
) -> Result<PathBuf> {
    let path = sidecar_path(&target.out_path, "json");
    let metadata = json!({
        "message_id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "grouped_id": message.grouped_id(),
        "chat": message.peer().map(describe_peer),
        "sender": message.sender().map(describe_peer),
        "date": message.date().to_rfc3339(),
        "caption": message.text(),
        "mime_type": target.mime_type,
        "media_type": target.media_type,
        "source_link": target.canonical_source_link,
        "file": target.out_path.display().to_string(),
        "filename": target.filename,
        "result": item,
        "message": target.metadata.clone(),
    });
    fs::write(&path, serde_json::to_vec_pretty(&metadata)?)
        .await
        .with_context(|| format!("Failed to write metadata sidecar '{}'", path.display()))?;
    Ok(path)
}

async fn write_caption_sidecar(
    target: &ResolvedDownloadTarget,
    format: CaptionSidecarFormat,
) -> Result<PathBuf> {
    let (path, bytes) = match format {
        CaptionSidecarFormat::Txt => (
            sidecar_path(&target.out_path, "txt"),
            target.caption.as_bytes().to_vec(),
        ),
        CaptionSidecarFormat::Json => (
            sidecar_path(&target.out_path, "caption.json"),
            serde_json::to_vec_pretty(&json!({
                "caption": target.caption,
                "file": target.out_path.display().to_string(),
                "source_link": target.canonical_source_link,
            }))?,
        ),
    };
    fs::write(&path, bytes)
        .await
        .with_context(|| format!("Failed to write caption sidecar '{}'", path.display()))?;
    Ok(path)
}

fn sidecar_path(out_path: &Path, suffix: &str) -> PathBuf {
    let file_name = out_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    out_path.with_file_name(format!("{}.{}", file_name, suffix))
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("Failed to read '{}' for hashing", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

// ── verified download orchestration ───────────────────────────────────────────

async fn stream_download_verified<D: Downloadable + Clone + Send + Sync + 'static>(
    client: &ResilientClient,
    downloadable: &D,
    target: &ResolvedDownloadTarget,
    request: &SingleDownloadRequest,
    shutdown: &AtomicBool,
) -> Result<DownloadOutcome> {
    let mut redownloaded = false;
    loop {
        let outcome = stream_download(
            client,
            downloadable,
            &target.out_path,
            StreamDownloadConfig {
                total_bytes: target.expected_size,
                collision: if redownloaded {
                    CollisionPolicy::Overwrite
                } else {
                    request.collision
                },
                retry: request.retry,
                parallel_chunks: request.parallel_chunks,
                keep_partial: request.keep_partial,
                request_timeout: request.timeouts.request_timeout,
            },
            shutdown,
        )
        .await?;
        match verify_size_match(&target.out_path, target.expected_size) {
            Ok(()) => return Ok(outcome),
            Err(_error) if request.redownload_on_mismatch && !redownloaded => {
                redownloaded = true;
                if fs::try_exists(&target.out_path).await.unwrap_or(false) {
                    let _ = fs::remove_file(&target.out_path).await;
                }
                let partial = partial_download_path(&target.out_path);
                if fs::try_exists(&partial).await.unwrap_or(false) {
                    let _ = fs::remove_file(&partial).await;
                }
                crate::output::stderrln(format!(
                    "Size mismatch for '{}', redownloading from scratch.",
                    target.out_path.display()
                ));
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}

fn verify_size_match(out_path: &Path, expected_size: u64) -> Result<()> {
    if expected_size == 0 {
        return Ok(());
    }

    let actual_size = std::fs::metadata(out_path)
        .with_context(|| format!("Failed to inspect '{}'", out_path.display()))?
        .len();
    if actual_size != expected_size {
        bail!(
            "Downloaded file '{}' has size {} but Telegram reported {}",
            out_path.display(),
            actual_size,
            expected_size
        );
    }

    Ok(())
}

// ── stream download ────────────────────────────────────────────────────────────

async fn stream_download<D: Downloadable + Clone + Send + Sync + 'static>(
    client: &ResilientClient,
    downloadable: &D,
    out_path: &Path,
    config: StreamDownloadConfig,
    shutdown: &AtomicBool,
) -> Result<DownloadOutcome> {
    let temp_path = partial_download_path(out_path);

    let prepared = prepare_output_file(out_path, &temp_path, config.collision).await?;
    let (file, initial_bytes, resumed) = match prepared {
        PreparedDownload::Ready {
            file,
            initial_bytes,
            resumed,
        } => (file, initial_bytes, resumed),
        PreparedDownload::SkippedExisting => return Ok(DownloadOutcome::SkippedExisting),
    };
    if resumed && initial_bytes > config.total_bytes && config.total_bytes > 0 {
        bail!(
            "Partial file '{}' is larger than the expected download size.",
            temp_path.display()
        );
    }

    let pb = make_progress_bar(config.total_bytes, initial_bytes, config.parallel_chunks);
    update_progress_status(&pb, 0, None, config.parallel_chunks);

    if config.parallel_chunks > 1
        && config.total_bytes >= PARALLEL_DOWNLOAD_THRESHOLD
        && initial_bytes == 0
        && downloadable.to_raw_input_location().is_some()
    {
        let parallel_client = client.client().await;
        return parallel_stream_download(ParallelDownloadContext {
            client: parallel_client,
            downloadable: downloadable.clone(),
            file,
            out_path,
            temp_path: &temp_path,
            total_bytes: config.total_bytes,
            retry: config.retry,
            parallel_chunks: config.parallel_chunks,
            keep_partial: config.keep_partial,
            request_timeout: config.request_timeout,
            pb,
            shutdown,
        })
        .await;
    }

    let mut written_bytes = initial_bytes;
    let mut attempt = 0u32;
    let mut total_retry_count = 0u32;
    let mut writer = BufWriter::with_capacity(MAX_PARALLEL_CHUNK_SIZE as usize, file);
    loop {
        if shutdown.load(Ordering::SeqCst) {
            pb.abandon_with_message(format!(
                "Interrupted; kept partial download '{}'",
                temp_path.display()
            ));
            writer.flush().await.ok();
            bail!("Interrupted by Ctrl+C");
        }

        let current_client = client.client().await;
        let mut download = current_client
            .iter_download(downloadable)
            .chunk_size(DOWNLOAD_CHUNK_SIZE);
        if written_bytes > 0 {
            if written_bytes % (DOWNLOAD_CHUNK_SIZE as u64) != 0 {
                pb.abandon_with_message(format!(
                    "Partial download '{}' is misaligned and cannot be resumed safely.",
                    temp_path.display()
                ));
                bail!(
                    "Partial download '{}' is misaligned and cannot be resumed safely.",
                    temp_path.display()
                );
            }
            download = download.skip_chunks((written_bytes / DOWNLOAD_CHUNK_SIZE as u64) as i32);
        }

        loop {
            if shutdown.load(Ordering::SeqCst) {
                pb.abandon_with_message(format!(
                    "Interrupted; kept partial download '{}'",
                    temp_path.display()
                ));
                writer.flush().await.ok();
                bail!("Interrupted by Ctrl+C");
            }

            client.wait_for_pacing().await;
            match run_request(config.request_timeout, "Chunk download timed out", async {
                download
                    .next()
                    .await
                    .map_err(|error| anyhow::anyhow!("Download error: {}", error))
            })
            .await?
            {
                Ok(Some(chunk)) => {
                    writer
                        .write_all(&chunk)
                        .await
                        .with_context(|| format!("Failed to write to '{}'", temp_path.display()))?;
                    written_bytes += chunk.len() as u64;
                    pb.inc(chunk.len() as u64);
                    attempt = 0;
                    update_progress_status(&pb, total_retry_count, None, config.parallel_chunks);
                }
                Ok(None) => {
                    writer
                        .flush()
                        .await
                        .with_context(|| format!("Failed to flush '{}'", temp_path.display()))?;
                    let mut file = writer.into_inner();
                    file.flush()
                        .await
                        .with_context(|| format!("Failed to flush '{}'", temp_path.display()))?;
                    drop(file);
                    fs::rename(&temp_path, out_path).await.with_context(|| {
                        format!(
                            "Failed to move completed download from '{}' to '{}'",
                            temp_path.display(),
                            out_path.display()
                        )
                    })?;
                    pb.finish_with_message(format!("Saved to '{}'", out_path.display()));
                    return Ok(DownloadOutcome::Downloaded { resumed });
                }
                Err(error) => {
                    attempt += 1;
                    total_retry_count += 1;
                    let error = anyhow::anyhow!("Download error: {}", error);
                    if let Some(delay) = retryable_delay(&error, attempt, config.retry) {
                        writer.flush().await.ok();
                        prepare_retry(client, &error, delay).await?;
                        update_progress_status(
                            &pb,
                            total_retry_count,
                            Some(delay),
                            config.parallel_chunks,
                        );
                        if crate::output::progress_enabled() {
                            pb.println(format!(
                                "Transient download error for '{}', retrying in {} ms (attempt {}/{}).",
                                out_path.display(),
                                delay.as_millis(),
                                attempt,
                                config.retry.retries
                            ));
                        }
                        break;
                    }

                    finalize_partial_failure(&pb, &temp_path, config.keep_partial).await;
                    writer.flush().await.ok();
                    return Err(error);
                }
            }
        }
    }
}

// ── parallel download ──────────────────────────────────────────────────────────

async fn parallel_stream_download<D: Downloadable + Clone + Send + Sync + 'static>(
    ctx: ParallelDownloadContext<'_, D>,
) -> Result<DownloadOutcome> {
    let ParallelDownloadContext {
        client,
        downloadable,
        mut file,
        out_path,
        temp_path,
        total_bytes,
        retry,
        parallel_chunks,
        keep_partial,
        request_timeout,
        pb,
        shutdown,
    } = ctx;
    file.set_len(total_bytes)
        .await
        .with_context(|| format!("Failed to preallocate '{}'", temp_path.display()))?;
    file.seek(SeekFrom::Start(0))
        .await
        .with_context(|| format!("Failed to seek '{}'", temp_path.display()))?;

    let chunk_size = MAX_PARALLEL_CHUNK_SIZE as u64;
    let total_parts = total_bytes.div_ceil(chunk_size);
    let mut join_set = JoinSet::new();
    let mut next_part = 0u64;
    let mut completed_chunks: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut next_write_offset = 0u64;
    let mut total_retry_count = 0u32;

    while next_write_offset < total_bytes {
        while join_set.len() < parallel_chunks && next_part < total_parts {
            let offset = next_part * chunk_size;
            let task_client = client.clone();
            let task_downloadable = downloadable.clone();
            let task_retry = retry;
            let chunk_index = next_part as i32;
            join_set.spawn(async move {
                fetch_parallel_chunk(
                    task_client,
                    task_downloadable,
                    offset,
                    chunk_index,
                    task_retry,
                    request_timeout,
                )
                .await
            });
            next_part += 1;
        }

        if shutdown.load(Ordering::SeqCst) {
            pb.abandon_with_message(format!(
                "Interrupted; kept partial download '{}'",
                temp_path.display()
            ));
            file.flush().await.ok();
            bail!("Interrupted by Ctrl+C");
        }

        let Some(result) = join_set.join_next().await else {
            break;
        };
        let fetched = match result {
            Ok(Ok(fetched)) => fetched,
            Ok(Err(error)) => {
                finalize_partial_failure(&pb, temp_path, keep_partial).await;
                file.flush().await.ok();
                return Err(error);
            }
            Err(error) => {
                finalize_partial_failure(&pb, temp_path, keep_partial).await;
                file.flush().await.ok();
                return Err(error.into());
            }
        };
        total_retry_count += fetched.retry_count;
        update_progress_status(
            &pb,
            total_retry_count,
            fetched.last_backoff,
            parallel_chunks,
        );
        completed_chunks.insert(fetched.offset, fetched.bytes);

        while let Some(bytes) = completed_chunks.remove(&next_write_offset) {
            if file.stream_position().await? != next_write_offset {
                file.seek(SeekFrom::Start(next_write_offset)).await?;
            }
            file.write_all(&bytes)
                .await
                .with_context(|| format!("Failed to write to '{}'", temp_path.display()))?;
            next_write_offset += bytes.len() as u64;
            pb.inc(bytes.len() as u64);
        }
    }

    file.flush()
        .await
        .with_context(|| format!("Failed to flush '{}'", temp_path.display()))?;
    fs::rename(temp_path, out_path).await.with_context(|| {
        format!(
            "Failed to move completed download from '{}' to '{}'",
            temp_path.display(),
            out_path.display()
        )
    })?;
    pb.finish_with_message(format!("Saved to '{}'", out_path.display()));
    Ok(DownloadOutcome::Downloaded { resumed: false })
}

async fn fetch_parallel_chunk<D: Downloadable + Clone + Send + Sync + 'static>(
    client: Client,
    downloadable: D,
    offset: u64,
    chunk_index: i32,
    retry: RetryConfig,
    request_timeout: Option<Duration>,
) -> Result<ParallelChunkResult> {
    let mut attempt = 0u32;
    let mut retry_count = 0u32;
    let mut last_backoff = None;

    loop {
        let mut download = client
            .iter_download(&downloadable)
            .chunk_size(MAX_PARALLEL_CHUNK_SIZE)
            .skip_chunks(chunk_index);
        match run_request(request_timeout, "Parallel chunk request timed out", async {
            download
                .next()
                .await
                .map_err(|error| anyhow::anyhow!("Download error: {}", error))
        })
        .await
        {
            Ok(Ok(Some(bytes))) => {
                return Ok(ParallelChunkResult {
                    offset,
                    bytes,
                    retry_count,
                    last_backoff,
                });
            }
            Ok(Ok(None)) => {
                return Ok(ParallelChunkResult {
                    offset,
                    bytes: Vec::new(),
                    retry_count,
                    last_backoff,
                });
            }
            Ok(Err(anyhow_error)) | Err(anyhow_error) => {
                attempt += 1;
                retry_count += 1;
                if let Some(delay) = retryable_delay(&anyhow_error, attempt, retry) {
                    last_backoff = Some(delay);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(anyhow_error);
            }
        }
    }
}

pub(crate) async fn prepare_output_file(
    out_path: &Path,
    temp_path: &Path,
    collision: CollisionPolicy,
) -> Result<PreparedDownload> {
    let out_exists = fs::try_exists(out_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", out_path.display()))?;
    let temp_exists = fs::try_exists(temp_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", temp_path.display()))?;

    if out_exists {
        match collision {
            CollisionPolicy::Error => {
                bail!(
                    "Refusing to overwrite existing file '{}'. Use --skip-existing, --overwrite, or --resume.",
                    out_path.display()
                );
            }
            CollisionPolicy::SkipExisting | CollisionPolicy::Resume => {
                return Ok(PreparedDownload::SkippedExisting);
            }
            CollisionPolicy::Overwrite => {
                fs::remove_file(out_path)
                    .await
                    .with_context(|| format!("Failed to remove '{}'", out_path.display()))?;
            }
            CollisionPolicy::SuffixExisting => {}
        }
    }

    if temp_exists {
        match collision {
            CollisionPolicy::Error => {
                bail!(
                    "Temporary download file '{}' already exists. Use --resume or --overwrite.",
                    temp_path.display()
                );
            }
            CollisionPolicy::SkipExisting => {
                return Ok(PreparedDownload::SkippedExisting);
            }
            CollisionPolicy::Overwrite => {
                fs::remove_file(temp_path)
                    .await
                    .with_context(|| format!("Failed to remove '{}'", temp_path.display()))?;
            }
            CollisionPolicy::Resume => {
                let meta = fs::metadata(temp_path)
                    .await
                    .with_context(|| format!("Failed to inspect '{}'", temp_path.display()))?;
                let file = OpenOptions::new()
                    .append(true)
                    .open(temp_path)
                    .await
                    .with_context(|| format!("Failed to reopen '{}'", temp_path.display()))?;
                return Ok(PreparedDownload::Ready {
                    file,
                    initial_bytes: meta.len(),
                    resumed: true,
                });
            }
            CollisionPolicy::SuffixExisting => {}
        }
    }

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)
        .await
        .with_context(|| format!("Failed to create file '{}'", temp_path.display()))?;

    Ok(PreparedDownload::Ready {
        file,
        initial_bytes: 0,
        resumed: false,
    })
}

// ── progress bar ───────────────────────────────────────────────────────────────

fn make_progress_bar(total_bytes: u64, initial_bytes: u64, parallel_chunks: usize) -> ProgressBar {
    if !crate::output::progress_enabled() {
        return ProgressBar::hidden();
    }

    if total_bytes > 0 {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} @ {bytes_per_sec} ({eta}) {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        pb.set_position(initial_bytes);
        update_progress_status(&pb, 0, None, parallel_chunks);
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] {bytes} downloaded @ {bytes_per_sec} {msg}",
            )
            .unwrap(),
        );
        pb.inc(initial_bytes);
        update_progress_status(&pb, 0, None, parallel_chunks);
        pb
    }
}

fn update_progress_status(
    pb: &ProgressBar,
    total_retries: u32,
    backoff: Option<Duration>,
    parallel_chunks: usize,
) {
    let mode = if parallel_chunks > 1 {
        format!("parallel={}", parallel_chunks)
    } else {
        "parallel=off".to_string()
    };
    let backoff_text = backoff
        .map(|delay| format!(" backoff={}ms", delay.as_millis()))
        .unwrap_or_default();
    pb.set_message(format!(
        "retries={}{} {}",
        total_retries, backoff_text, mode
    ));
}

async fn finalize_partial_failure(pb: &ProgressBar, temp_path: &Path, keep_partial: bool) {
    if keep_partial {
        pb.abandon_with_message(format!(
            "Failed; kept partial download '{}'",
            temp_path.display()
        ));
        return;
    }

    match fs::remove_file(temp_path).await {
        Ok(()) => pb.abandon_with_message(format!(
            "Failed; removed partial download '{}'",
            temp_path.display()
        )),
        Err(_) => pb.abandon_with_message(format!(
            "Failed; partial download may remain at '{}'",
            temp_path.display()
        )),
    }
}

// ── output-path resolution ─────────────────────────────────────────────────────

pub(crate) async fn resolve_download_target(
    request: &SingleDownloadRequest,
    message: &Message,
) -> Result<Option<(ResolvedDownloadTarget, DownloadSource)>> {
    let Some(source) = resolve_download_source(message, request.media_variant) else {
        return Ok(None);
    };
    let (out_path, filename) = resolve_target_path(request, message, &source).await?;
    let mime_type = source.mime_type();
    let media_type = source.media_type().to_string();
    let expected_size = source.expected_size();
    let canonical_link = canonical_source_link(message);

    Ok(Some((
        ResolvedDownloadTarget {
            out_path,
            filename,
            media_type,
            mime_type,
            expected_size,
            canonical_source_link: canonical_link,
            grouped_id: message.grouped_id(),
            caption: message.text().to_string(),
            metadata: describe_message(message),
        },
        source,
    )))
}

fn resolve_download_source(message: &Message, media_variant: MediaVariant) -> Option<DownloadSource> {
    match message.media()? {
        Media::Document(doc) => {
            if matches!(media_variant, MediaVariant::LargestPhoto) {
                None
            } else {
                Some(DownloadSource::Document(doc.clone()))
            }
        }
        Media::Photo(photo) => {
            if matches!(media_variant, MediaVariant::OriginalDocument) {
                None
            } else {
                Some(DownloadSource::Photo(photo.clone()))
            }
        }
        _ => None,
    }
}

async fn resolve_target_path(
    request: &SingleDownloadRequest,
    message: &Message,
    source: &DownloadSource,
) -> Result<(PathBuf, String)> {
    let media_type = source.media_type();
    let base_dir =
        output_dir_for_layout(&request.out_dir, request.output_layout, message, media_type);
    fs::create_dir_all(&base_dir)
        .await
        .with_context(|| format!("Failed to create output directory '{}'", base_dir.display()))?;

    let base_filename = choose_filename_for_source(source, message);
    let rendered = render_name_template(
        request.name_template.as_deref(),
        &base_filename,
        message,
        media_type,
    );
    let filename = sanitize_filename(&rendered);
    let out_path = safe_output_path(&base_dir, &filename)?;
    if matches!(request.collision, CollisionPolicy::SuffixExisting) {
        let suffixed = choose_suffix_output_path(&out_path).await?;
        return Ok((
            suffixed.clone(),
            suffixed
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(&filename)
                .to_string(),
        ));
    }

    Ok((out_path, filename))
}

pub(crate) async fn choose_suffix_output_path(base_path: &Path) -> Result<PathBuf> {
    if !fs::try_exists(base_path).await?
        && !fs::try_exists(&partial_download_path(base_path)).await?
    {
        return Ok(base_path.to_path_buf());
    }

    let stem = base_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("file");
    let ext = base_path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{}", ext))
        .unwrap_or_default();

    for index in 1..10_000usize {
        let candidate = base_path.with_file_name(format!("{} ({}){}", stem, index, ext));
        if !fs::try_exists(&candidate).await?
            && !fs::try_exists(&partial_download_path(&candidate)).await?
        {
            return Ok(candidate);
        }
    }

    bail!(
        "Could not find a free suffixed filename for '{}'",
        base_path.display()
    )
}

fn output_dir_for_layout(
    base: &Path,
    layout: OutputLayout,
    message: &Message,
    media_type: &str,
) -> PathBuf {
    match layout {
        OutputLayout::Flat => base.to_path_buf(),
        OutputLayout::Chat => base.join(sanitize_filename(&peer_label(message))),
        OutputLayout::Date => base.join(message_date_slug(message)),
        OutputLayout::MediaType => base.join(media_type),
    }
}

fn render_name_template(
    template: Option<&str>,
    base_filename: &str,
    message: &Message,
    media_type: &str,
) -> String {
    let Some(template) = template else {
        return base_filename.to_string();
    };
    let rendered = template
        .replace("{chat}", &peer_label(message))
        .replace(
            "{chat_id}",
            &message.peer_id().bot_api_dialog_id().to_string(),
        )
        .replace("{msg_id}", &message.id().to_string())
        .replace("{filename}", base_filename)
        .replace("{media_type}", media_type)
        .replace("{date}", &message_date_slug(message));

    if Path::new(&rendered).extension().is_none() {
        if let Some(ext) = Path::new(base_filename)
            .extension()
            .and_then(|ext| ext.to_str())
        {
            return format!("{}.{}", rendered, ext);
        }
    }

    rendered
}

fn choose_filename_for_source(source: &DownloadSource, message: &Message) -> String {
    match source {
        DownloadSource::Document(doc) => choose_filename_document(doc, message),
        DownloadSource::Photo(_) => format!(
            "photo_{}_{}.jpg",
            message.peer_id().bot_api_dialog_id(),
            message.id()
        ),
    }
}

fn choose_filename_document(doc: &Document, message: &Message) -> String {
    if let Some(name) = doc.name() {
        if !name.is_empty() {
            return sanitize_filename(name);
        }
    }

    let ext = mime_to_ext(doc.mime_type().unwrap_or(""));
    format!(
        "file_{}_{}{}",
        message.peer_id().bot_api_dialog_id(),
        message.id(),
        ext
    )
}

fn peer_label(message: &Message) -> String {
    message
        .peer()
        .and_then(|peer| peer.username().map(ToOwned::to_owned))
        .or_else(|| {
            message
                .peer()
                .and_then(|peer| peer.name().map(sanitize_filename))
        })
        .unwrap_or_else(|| message.peer_id().bot_api_dialog_id().to_string())
}

fn message_date_slug(message: &Message) -> String {
    message.date().format("%Y-%m-%d").to_string()
}

// ── collision detection ────────────────────────────────────────────────────────

pub(crate) async fn inspect_output_collision(
    out_path: &Path,
    collision: CollisionPolicy,
) -> Result<&'static str> {
    let temp_path = partial_download_path(out_path);
    let out_exists = fs::try_exists(out_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", out_path.display()))?;
    let temp_exists = fs::try_exists(&temp_path)
        .await
        .with_context(|| format!("Failed to inspect '{}'", temp_path.display()))?;

    let result = match (out_exists, temp_exists, collision) {
        (true, _, CollisionPolicy::SkipExisting) => "skip-existing-file",
        (true, _, CollisionPolicy::Resume) => "skip-complete-file",
        (true, _, CollisionPolicy::Overwrite) => "overwrite-file",
        (true, _, CollisionPolicy::SuffixExisting) => "suffix-existing-file",
        (true, _, CollisionPolicy::Error) => "error-existing-file",
        (false, true, CollisionPolicy::SkipExisting) => "skip-partial-file",
        (false, true, CollisionPolicy::Resume) => "resume-partial-file",
        (false, true, CollisionPolicy::Overwrite) => "overwrite-partial-file",
        (false, true, CollisionPolicy::SuffixExisting) => "suffix-partial-file",
        (false, true, CollisionPolicy::Error) => "error-partial-file",
        (false, false, _) => "write-new-file",
    };

    Ok(result)
}

pub(crate) fn collision_policy_name(policy: CollisionPolicy) -> &'static str {
    match policy {
        CollisionPolicy::Error => "error",
        CollisionPolicy::SkipExisting => "skip-existing",
        CollisionPolicy::Overwrite => "overwrite",
        CollisionPolicy::Resume => "resume",
        CollisionPolicy::SuffixExisting => "suffix-existing",
    }
}

// ── filename utilities ─────────────────────────────────────────────────────────

pub(crate) fn sanitize_filename(name: &str) -> String {
    let normalized_input = name.replace('\\', "/");
    let basename = std::path::Path::new(&normalized_input)
        .file_name()
        .and_then(|part| part.to_str())
        .unwrap_or(name);
    let mut sanitized: String = basename
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();

    sanitized = sanitized.trim_matches(['.', ' ']).to_string();
    sanitized = sanitized.split_whitespace().collect::<Vec<_>>().join(" ");

    if sanitized.is_empty() {
        sanitized = "download".to_string();
    }

    let (stem, ext) = split_extension(&sanitized);
    let stem = stem.trim_end_matches(['.', ' ']);
    let ext = ext.trim_matches(['.', ' ']);
    let normalized_stem = if is_windows_reserved_name(stem) {
        format!("_{}", stem)
    } else {
        stem.to_string()
    };
    sanitized = if ext.is_empty() {
        normalized_stem
    } else {
        format!("{}.{}", normalized_stem, ext)
    };

    sanitized
}

pub(crate) fn safe_output_path(out_dir: &Path, filename: &str) -> Result<PathBuf> {
    let sanitized = sanitize_filename(filename);
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        bail!("Resolved filename was empty after sanitization");
    }

    let path = out_dir.join(&sanitized);
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!("Refusing to write outside of the output directory");
    }

    Ok(path)
}

pub(crate) fn partial_download_path(out_path: &Path) -> PathBuf {
    let mut filename = out_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    filename.push(".part");
    out_path.with_file_name(filename)
}

fn split_extension(name: &str) -> (&str, &str) {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && !ext.is_empty() => (stem, ext),
        _ => (name, ""),
    }
}

fn is_windows_reserved_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
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

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename() {
        assert_eq!(sanitize_filename("foo/bar.mp4"), "bar.mp4");
        assert_eq!(sanitize_filename("normal.mkv"), "normal.mkv");
        assert_eq!(sanitize_filename("a:b*c?d"), "a_b_c_d");
        assert_eq!(sanitize_filename("..\\..\\CON .txt"), "_CON.txt");
        assert_eq!(sanitize_filename(" trailing . "), "trailing");
    }

    #[test]
    fn test_mime_to_ext() {
        assert_eq!(mime_to_ext("video/mp4"), ".mp4");
        assert_eq!(mime_to_ext("audio/mpeg"), ".mp3");
        assert_eq!(mime_to_ext("unknown/type"), "");
    }

    #[test]
    fn test_partial_download_path() {
        let path = Path::new("/tmp/video.mp4");
        assert_eq!(
            partial_download_path(path),
            PathBuf::from("/tmp/video.mp4.part")
        );
    }

    #[test]
    fn test_sidecar_path_appends_suffix_to_filename() {
        let path = Path::new("/tmp/video.mp4");
        assert_eq!(
            sidecar_path(path, "json"),
            PathBuf::from("/tmp/video.mp4.json")
        );
        assert_eq!(
            sidecar_path(path, "caption.json"),
            PathBuf::from("/tmp/video.mp4.caption.json")
        );
    }

    #[tokio::test]
    async fn test_choose_suffix_output_path_picks_next_available_name() {
        let base = std::env::temp_dir().join(format!(
            "tw-dl-suffix-{}-{}",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        std::fs::create_dir_all(&base).unwrap();

        let path = base.join("video.mp4");
        let path_1 = base.join("video (1).mp4");
        let path_2_part = partial_download_path(&base.join("video (2).mp4"));
        std::fs::write(&path, b"done").unwrap();
        std::fs::write(&path_1, b"done").unwrap();
        std::fs::write(&path_2_part, b"partial").unwrap();

        let candidate = choose_suffix_output_path(&path).await.unwrap();
        assert_eq!(candidate, base.join("video (3).mp4"));

        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path_1);
        let _ = std::fs::remove_file(path_2_part);
        let _ = std::fs::remove_dir(base);
    }

    #[test]
    fn test_safe_output_path_stays_in_output_dir() {
        let out_dir = Path::new("/tmp/downloads");
        let path = safe_output_path(out_dir, "../../secret.txt").unwrap();
        assert_eq!(path, PathBuf::from("/tmp/downloads/secret.txt"));
    }

    #[test]
    fn test_windows_reserved_name_detection() {
        assert!(is_windows_reserved_name("CON"));
        assert!(is_windows_reserved_name("lpt1"));
        assert!(!is_windows_reserved_name("video"));
    }

    #[tokio::test]
    async fn test_inspect_output_collision() {
        let base = std::env::temp_dir().join(format!(
            "tw-dl-collision-{}-{}",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        std::fs::create_dir_all(&base).unwrap();
        let out_path = base.join("video.mp4");
        let part_path = partial_download_path(&out_path);

        std::fs::write(&part_path, b"partial").unwrap();
        assert_eq!(
            inspect_output_collision(&out_path, CollisionPolicy::Resume)
                .await
                .unwrap(),
            "resume-partial-file"
        );

        std::fs::remove_file(&part_path).unwrap();
        std::fs::write(&out_path, b"done").unwrap();
        assert_eq!(
            inspect_output_collision(&out_path, CollisionPolicy::SkipExisting)
                .await
                .unwrap(),
            "skip-existing-file"
        );
        assert_eq!(
            inspect_output_collision(&out_path, CollisionPolicy::SuffixExisting)
                .await
                .unwrap(),
            "suffix-existing-file"
        );

        let _ = std::fs::remove_file(out_path);
        let _ = std::fs::remove_dir(base);
    }
}
