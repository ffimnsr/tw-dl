//! Batch download orchestration: input parsing (text/CSV/JSONL), streaming
//! input, job scheduling, checkpointing, and manifest replay.
//!
//! Batch input is now streamed rather than buffered entirely in memory: a
//! background task reads and parses the source file or stdin line-by-line and
//! sends parsed [`BatchEntry`] items through a bounded channel.  Downloads
//! begin as soon as the first entries arrive, so large stdin/file jobs no
//! longer stall waiting for the full input to be read.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{self, AsyncBufReadExt, AsyncRead, BufReader};
use tokio::task::JoinSet;

use crate::link::parse_link;
use crate::manifest::{
    append_manifest_record, open_manifest_writer, read_manifest_records, ManifestRecord,
};
use super::client::{DownloadCaches, ResilientClient};
use super::transfer::{download_one, SingleDownloadRequest};
use super::resolver::MessageSelectorArgs;
use super::types::{
    BatchFailureMode, BatchInputFormat, CaptionSidecarFormat, CollisionPolicy, MediaVariant,
    OutputLayout, RetryConfig, TimeoutConfig,
};

// ── batch source types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) enum BatchSource {
    File(PathBuf),
    Stdin,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchLineRange {
    from_line: Option<usize>,
    to_line: Option<usize>,
}

impl BatchLineRange {
    pub(crate) fn new(from_line: Option<usize>, to_line: Option<usize>) -> Result<Self> {
        if matches!(from_line, Some(0)) {
            bail!("--from-line must be at least 1");
        }
        if matches!(to_line, Some(0)) {
            bail!("--to-line must be at least 1");
        }

        Ok(Self { from_line, to_line })
    }

    pub(crate) fn contains(self, line_number: usize) -> bool {
        if self.from_line.is_some_and(|from| line_number < from) {
            return false;
        }
        if self.to_line.is_some_and(|to| line_number > to) {
            return false;
        }
        true
    }

    fn past_end(self, line_number: usize) -> bool {
        self.to_line.is_some_and(|to| line_number > to)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BatchEntry {
    pub(crate) line_number: usize,
    pub(crate) link: String,
}

pub(crate) struct BatchContext {
    pub(crate) source: BatchSource,
    pub(crate) out_dir: PathBuf,
    pub(crate) collision: CollisionPolicy,
    pub(crate) retry: RetryConfig,
    pub(crate) jobs: usize,
    pub(crate) input_format: BatchInputFormat,
    pub(crate) failure_mode: BatchFailureMode,
    pub(crate) max_failures: Option<usize>,
    pub(crate) line_range: BatchLineRange,
    pub(crate) checkpoint: PathBuf,
    pub(crate) dry_run: bool,
    pub(crate) success_hook: Option<String>,
    pub(crate) failure_hook: Option<String>,
    pub(crate) archive_path: Option<PathBuf>,
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

pub(crate) struct ManifestReplayContext {
    pub(crate) manifest_path: PathBuf,
    pub(crate) out_dir: PathBuf,
    pub(crate) collision: CollisionPolicy,
    pub(crate) retry: RetryConfig,
    pub(crate) jobs: usize,
    pub(crate) failure_mode: BatchFailureMode,
    pub(crate) max_failures: Option<usize>,
    pub(crate) checkpoint: PathBuf,
    pub(crate) dry_run: bool,
    pub(crate) success_hook: Option<String>,
    pub(crate) failure_hook: Option<String>,
    pub(crate) archive_path: Option<PathBuf>,
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

struct LinkJobContext<'a> {
    client: &'a Arc<ResilientClient>,
    entries: tokio::sync::mpsc::Receiver<Result<BatchEntry>>,
    out_dir: &'a Path,
    collision: CollisionPolicy,
    retry: RetryConfig,
    jobs: usize,
    failure_mode: BatchFailureMode,
    max_failures: Option<usize>,
    checkpoint_path: &'a Path,
    dry_run: bool,
    success_hook: Option<&'a str>,
    failure_hook: Option<&'a str>,
    archive_path: Option<&'a Path>,
    media_variant: MediaVariant,
    name_template: Option<&'a str>,
    output_layout: OutputLayout,
    metadata_sidecar: bool,
    caption_sidecar: Option<CaptionSidecarFormat>,
    hash: bool,
    redownload_on_mismatch: bool,
    print_path_only: bool,
    parallel_chunks: usize,
    keep_partial: bool,
    timeouts: TimeoutConfig,
    caches: Arc<DownloadCaches>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug)]
struct BatchTaskResult {
    line_number: usize,
    link: String,
    result: Result<Value>,
}

struct BatchResultContext<'a> {
    checkpoint_path: &'a Path,
    writer: &'a mut fs::File,
    completed_links: &'a mut HashSet<String>,
    success_count: &'a mut usize,
    failure_count: &'a mut usize,
    skipped_count: &'a mut usize,
    success_hook: Option<&'a str>,
    failure_hook: Option<&'a str>,
    archive_path: Option<&'a Path>,
}

// ── batch source helpers ───────────────────────────────────────────────────────

pub(crate) fn resolve_batch_source(args: &super::types::DownloadArgs) -> Result<Option<BatchSource>> {
    if let Some(path) = &args.file_list {
        return Ok(Some(if path == Path::new("-") {
            BatchSource::Stdin
        } else {
            BatchSource::File(path.clone())
        }));
    }

    if args.link.is_none()
        && args.peer.is_none()
        && args.msg_id.is_none()
        && args.retry_from.is_none()
        && !std::io::stdin().is_terminal()
    {
        return Ok(Some(BatchSource::Stdin));
    }

    Ok(None)
}

pub(crate) fn checkpoint_path_for_batch_source(
    source: &BatchSource,
    override_path: Option<PathBuf>,
) -> PathBuf {
    if let Some(path) = override_path {
        return path;
    }

    match source {
        BatchSource::File(path) => checkpoint_path_for(path, None),
        BatchSource::Stdin => PathBuf::from("stdin.checkpoint.jsonl"),
    }
}

pub(crate) fn normalize_link_key(link: &str) -> String {
    use crate::link::ParsedLink;
    match parse_link(link.trim()) {
        Ok(ParsedLink::Username { username, msg_id }) => {
            format!("username:{}:{}", username.to_ascii_lowercase(), msg_id)
        }
        Ok(ParsedLink::Channel { channel_id, msg_id }) => {
            format!("channel:{}:{}", channel_id, msg_id)
        }
        Err(_) => link.trim().to_string(),
    }
}

pub(crate) fn checkpoint_path_for(list_path: &Path, override_path: Option<PathBuf>) -> PathBuf {
    if let Some(path) = override_path {
        return path;
    }

    let base_name = list_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "batch".to_string());
    list_path.with_file_name(format!("{}.checkpoint.jsonl", base_name))
}

// ── streaming batch input ──────────────────────────────────────────────────────

/// Spawn a background task that reads the batch source and sends parsed
/// [`BatchEntry`] items through a bounded channel.  The channel buffer limits
/// how much input is held in memory at once; the producer blocks when the
/// consumer (job scheduler) is busy.
pub(crate) fn spawn_batch_entry_stream(
    source: BatchSource,
    input_format: BatchInputFormat,
    line_range: BatchLineRange,
) -> tokio::sync::mpsc::Receiver<Result<BatchEntry>> {
    // 512-entry buffer: large enough to keep the job queue fed without
    // buffering the entire input for a large file or stdin stream.
    let (tx, rx) = tokio::sync::mpsc::channel(512);
    tokio::spawn(async move {
        if let Err(e) = stream_entries_task(source, input_format, line_range, &tx).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    rx
}

/// Convert a `Vec<BatchEntry>` into a channel receiver so that both the
/// streaming (live source) and the in-memory (manifest replay) paths share a
/// single [`run_link_jobs`] implementation.
fn vec_to_entry_receiver(
    entries: Vec<BatchEntry>,
) -> tokio::sync::mpsc::Receiver<Result<BatchEntry>> {
    let (tx, rx) = tokio::sync::mpsc::channel(entries.len().max(1));
    for entry in entries {
        // Channel capacity equals entry count, so this never blocks.
        let _ = tx.try_send(Ok(entry));
    }
    rx
}

async fn stream_entries_task(
    source: BatchSource,
    input_format: BatchInputFormat,
    line_range: BatchLineRange,
    tx: &tokio::sync::mpsc::Sender<Result<BatchEntry>>,
) -> Result<()> {
    let reader: Box<dyn AsyncRead + Unpin + Send> = match &source {
        BatchSource::File(path) => Box::new(
            fs::File::open(path)
                .await
                .with_context(|| format!("Cannot open file list '{}'", path.display()))?,
        ),
        BatchSource::Stdin => Box::new(io::stdin()),
    };

    let mut lines = BufReader::new(reader).lines();
    let mut line_number = 0usize;

    // Resolve the format: extension or content-sniff of the first non-empty line.
    let mut format = input_format;
    if matches!(format, BatchInputFormat::Auto) {
        if let BatchSource::File(path) = &source {
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                format = match ext.to_ascii_lowercase().as_str() {
                    "csv" => BatchInputFormat::Csv,
                    "jsonl" | "ndjson" => BatchInputFormat::Jsonl,
                    _ => format,
                };
            }
        }
    }

    // If still Auto, sniff from the first non-empty line.
    let mut peeked: Option<(usize, String)> = None;
    if matches!(format, BatchInputFormat::Auto) {
        while let Some(line) = lines.next_line().await? {
            line_number += 1;
            let trimmed = line.trim().to_string();
            if trimmed.is_empty() {
                continue;
            }
            format = if trimmed.starts_with('{') || trimmed.starts_with('"') {
                BatchInputFormat::Jsonl
            } else if trimmed.contains(',') {
                BatchInputFormat::Csv
            } else {
                BatchInputFormat::Text
            };
            peeked = Some((line_number, line));
            break;
        }
        if matches!(format, BatchInputFormat::Auto) {
            // Source was entirely empty.
            return Ok(());
        }
    }

    // State for CSV header detection.
    let mut csv_header_processed = false;
    let mut csv_selected_column: Option<usize> = None;

    // Process any peeked line first, then the rest of the stream.
    let mut process = |ln: usize, text: &str| -> Option<Result<BatchEntry>> {
        if !line_range.contains(ln) {
            return None;
        }
        parse_line(
            ln,
            text,
            format,
            &mut csv_header_processed,
            &mut csv_selected_column,
            &line_range,
        )
    };

    if let Some((ln, text)) = peeked {
        if let Some(result) = process(ln, &text) {
            if tx.send(result).await.is_err() {
                return Ok(());
            }
        }
    }

    while let Some(line) = lines.next_line().await? {
        line_number += 1;
        if line_range.past_end(line_number) {
            break;
        }
        if let Some(result) = process(line_number, &line) {
            if tx.send(result).await.is_err() {
                break;
            }
        }
    }

    Ok(())
}

/// Parse a single line according to the resolved format.
/// Returns `None` for lines that should be silently skipped (empty, comments,
/// out-of-range).  Returns `Some(Ok(_))` for valid entries and `Some(Err(_))`
/// for parse failures.
fn parse_line(
    line_number: usize,
    text: &str,
    format: BatchInputFormat,
    csv_header_processed: &mut bool,
    csv_selected_column: &mut Option<usize>,
    _line_range: &BatchLineRange,
) -> Option<Result<BatchEntry>> {
    match format {
        BatchInputFormat::Text | BatchInputFormat::Auto => {
            let link = text.trim();
            if link.is_empty() || link.starts_with('#') {
                return None;
            }
            Some(Ok(BatchEntry {
                line_number,
                link: link.to_string(),
            }))
        }
        BatchInputFormat::Csv => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return None;
            }
            let row = match parse_csv_record(trimmed) {
                Ok(row) => row,
                Err(e) => {
                    return Some(Err(e.context(format!("Invalid CSV row at line {}", line_number))))
                }
            };
            if !*csv_header_processed {
                *csv_header_processed = true;
                if let Some((index, _)) = row.iter().enumerate().find(|(_, cell)| {
                    matches!(
                        cell.trim().to_ascii_lowercase().as_str(),
                        "link" | "url" | "message_link" | "message_url"
                    )
                }) {
                    *csv_selected_column = Some(index);
                    return None; // header row — skip
                }
            }
            extract_csv_link(&row, *csv_selected_column).map(|link| Ok(BatchEntry { line_number, link }))
        }
        BatchInputFormat::Jsonl => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return None;
            }
            let value: Value = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    return Some(Err(anyhow::anyhow!(
                        "Invalid JSONL entry at line {}: {}",
                        line_number,
                        e
                    )))
                }
            };
            let link = match value {
                Value::String(s) => s,
                Value::Object(map) => {
                    match ["link", "url", "message_link", "message_url"]
                        .into_iter()
                        .find_map(|key| map.get(key).and_then(Value::as_str))
                        .map(ToOwned::to_owned)
                    {
                        Some(s) => s,
                        None => {
                            return Some(Err(anyhow::anyhow!(
                                "JSONL entry at line {} must be a string or object \
                                 containing link/url/message_link/message_url",
                                line_number
                            )))
                        }
                    }
                }
                _ => {
                    return Some(Err(anyhow::anyhow!(
                        "JSONL entry at line {} must be a string or object",
                        line_number
                    )))
                }
            };
            Some(Ok(BatchEntry {
                line_number,
                link: link.trim().to_string(),
            }))
        }
    }
}

// ── CSV helpers (kept for direct use in tests) ─────────────────────────────────

pub(crate) fn parse_csv_record(line: &str) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(field.trim().to_string());
                field.clear();
            }
            _ => field.push(ch),
        }
    }

    if in_quotes {
        bail!("Unterminated quoted CSV field");
    }

    fields.push(field.trim().to_string());
    Ok(fields)
}

fn is_probably_link(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with("https://t.me/")
        || trimmed.starts_with("http://t.me/")
        || parse_link(trimmed).is_ok()
}

fn extract_csv_link(row: &[String], selected_column: Option<usize>) -> Option<String> {
    if let Some(index) = selected_column {
        return row
            .get(index)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    }

    row.iter()
        .find(|value| is_probably_link(value))
        .or_else(|| row.iter().find(|value| !value.trim().is_empty()))
        .map(|value| value.trim().to_string())
}

// ── batch entry loading (only used in tests) ──────────────────────────────────

#[cfg(test)]
/// A source line — used in unit tests that exercise the parsers directly.
#[derive(Debug, Clone)]
struct SourceLine {
    line_number: usize,
    text: String,
}

#[cfg(test)]
fn parse_text_batch_entries(
    lines: &[SourceLine],
    line_range: BatchLineRange,
) -> Vec<BatchEntry> {
    lines
        .iter()
        .filter(|line| line_range.contains(line.line_number))
        .filter_map(|line| {
            let link = line.text.trim();
            if link.is_empty() || link.starts_with('#') {
                return None;
            }
            Some(BatchEntry {
                line_number: line.line_number,
                link: link.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
fn parse_csv_batch_entries(
    lines: &[SourceLine],
    line_range: BatchLineRange,
) -> Result<Vec<BatchEntry>> {
    let mut entries = Vec::new();
    let mut selected_column = None;
    let mut header_processed = false;

    for line in lines
        .iter()
        .filter(|line| line_range.contains(line.line_number))
    {
        if line.text.trim().is_empty() {
            continue;
        }

        let row = parse_csv_record(&line.text)
            .with_context(|| format!("Invalid CSV row at line {}", line.line_number))?;
        if !header_processed {
            header_processed = true;
            if let Some((index, _)) = row.iter().enumerate().find(|(_, cell)| {
                matches!(
                    cell.trim().to_ascii_lowercase().as_str(),
                    "link" | "url" | "message_link" | "message_url"
                )
            }) {
                selected_column = Some(index);
                continue;
            }
        }

        if let Some(link) = extract_csv_link(&row, selected_column) {
            entries.push(BatchEntry {
                line_number: line.line_number,
                link,
            });
        }
    }

    Ok(entries)
}

#[cfg(test)]
fn parse_jsonl_batch_entries(
    lines: &[SourceLine],
    line_range: BatchLineRange,
) -> Result<Vec<BatchEntry>> {
    let mut entries = Vec::new();

    for line in lines
        .iter()
        .filter(|line| line_range.contains(line.line_number))
    {
        let trimmed = line.text.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("Invalid JSONL entry at line {}", line.line_number))?;
        let link = match value {
            Value::String(link) => link,
            Value::Object(map) => ["link", "url", "message_link", "message_url"]
                .into_iter()
                .find_map(|key| map.get(key).and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!(
                        "JSONL entry at line {} must be a string or object containing link/url/message_link/message_url",
                        line.line_number
                    )
                })?,
            _ => {
                bail!(
                    "JSONL entry at line {} must be a string or object",
                    line.line_number
                )
            }
        };

        entries.push(BatchEntry {
            line_number: line.line_number,
            link: link.trim().to_string(),
        });
    }

    Ok(entries)
}

// ── batch run-level entry points ───────────────────────────────────────────────

/// Run batch downloads, streaming source input rather than loading it all into
/// memory.  No upfront prefetch is performed; individual downloads fetch and
/// cache their own messages, so large batches start immediately.
pub(crate) async fn run_batch_downloads(
    client: &Arc<ResilientClient>,
    ctx: BatchContext,
    shutdown: Arc<AtomicBool>,
    caches: Arc<DownloadCaches>,
) -> Result<()> {
    let entries_rx =
        spawn_batch_entry_stream(ctx.source, ctx.input_format, ctx.line_range);

    run_link_jobs(LinkJobContext {
        client,
        entries: entries_rx,
        out_dir: &ctx.out_dir,
        collision: ctx.collision,
        retry: ctx.retry,
        jobs: ctx.jobs,
        failure_mode: ctx.failure_mode,
        max_failures: ctx.max_failures,
        checkpoint_path: &ctx.checkpoint,
        dry_run: ctx.dry_run,
        success_hook: ctx.success_hook.as_deref(),
        failure_hook: ctx.failure_hook.as_deref(),
        archive_path: ctx.archive_path.as_deref(),
        media_variant: ctx.media_variant,
        name_template: ctx.name_template.as_deref(),
        output_layout: ctx.output_layout,
        metadata_sidecar: ctx.metadata_sidecar,
        caption_sidecar: ctx.caption_sidecar,
        hash: ctx.hash,
        redownload_on_mismatch: ctx.redownload_on_mismatch,
        print_path_only: ctx.print_path_only,
        parallel_chunks: ctx.parallel_chunks,
        keep_partial: ctx.keep_partial,
        timeouts: ctx.timeouts,
        caches,
        shutdown,
    })
    .await
}

pub(crate) async fn run_manifest_replay(
    client: &Arc<ResilientClient>,
    ctx: ManifestReplayContext,
    shutdown: Arc<AtomicBool>,
    caches: Arc<DownloadCaches>,
) -> Result<()> {
    let entries = load_failed_links(&ctx.manifest_path).await?;
    if entries.is_empty() {
        crate::output::write_command_output(
            "download",
            json!({
                "status": "completed",
                "mode": "retry-from",
                "succeeded": 0,
                "skipped": 0,
                "checkpoint_skipped": 0,
                "failed": 0,
                "checkpoint": ctx.checkpoint.display().to_string(),
                "source_manifest": ctx.manifest_path.display().to_string(),
                "message": "No failed links found in manifest",
            }),
        )?;
        return Ok(());
    }

    let entries_rx = vec_to_entry_receiver(entries);

    run_link_jobs(LinkJobContext {
        client,
        entries: entries_rx,
        out_dir: &ctx.out_dir,
        collision: ctx.collision,
        retry: ctx.retry,
        jobs: ctx.jobs,
        failure_mode: ctx.failure_mode,
        max_failures: ctx.max_failures,
        checkpoint_path: &ctx.checkpoint,
        dry_run: ctx.dry_run,
        success_hook: ctx.success_hook.as_deref(),
        failure_hook: ctx.failure_hook.as_deref(),
        archive_path: ctx.archive_path.as_deref(),
        media_variant: ctx.media_variant,
        name_template: ctx.name_template.as_deref(),
        output_layout: ctx.output_layout,
        metadata_sidecar: ctx.metadata_sidecar,
        caption_sidecar: ctx.caption_sidecar,
        hash: ctx.hash,
        redownload_on_mismatch: ctx.redownload_on_mismatch,
        print_path_only: ctx.print_path_only,
        parallel_chunks: ctx.parallel_chunks,
        keep_partial: ctx.keep_partial,
        timeouts: ctx.timeouts,
        caches,
        shutdown,
    })
    .await
}

// ── job loop ───────────────────────────────────────────────────────────────────

async fn run_link_jobs(mut ctx: LinkJobContext<'_>) -> Result<()> {
    let LinkJobContext {
        client,
        ref mut entries,
        out_dir,
        collision,
        retry,
        jobs,
        failure_mode,
        max_failures,
        checkpoint_path,
        dry_run,
        success_hook,
        failure_hook,
        archive_path,
        media_variant,
        name_template,
        output_layout,
        metadata_sidecar,
        caption_sidecar,
        hash,
        redownload_on_mismatch,
        print_path_only,
        parallel_chunks,
        keep_partial,
        timeouts,
        ref caches,
        ref shutdown,
    } = ctx;

    let mut completed_links = load_completed_links(checkpoint_path).await?;
    let mut writer = open_manifest_writer(checkpoint_path).await?;
    let mut join_set: JoinSet<BatchTaskResult> = JoinSet::new();
    let mut success_count = 0usize;
    let mut failure_count = 0usize;
    let mut skipped_count = 0usize;
    let mut checkpoint_skip_count = 0usize;
    let mut duplicate_skip_count = 0usize;
    let mut seen_links = HashSet::new();
    let mut stop_scheduling = false;
    let client_handle = client.clone();

    loop {
        // Drain completed tasks whenever the pool is full or no more entries
        // are being added to it.
        while join_set.len() >= jobs {
            let finished = join_set
                .join_next()
                .await
                .expect("join set length checked")?;
            handle_batch_result(
                finished,
                BatchResultContext {
                    checkpoint_path,
                    writer: &mut writer,
                    completed_links: &mut completed_links,
                    success_count: &mut success_count,
                    failure_count: &mut failure_count,
                    skipped_count: &mut skipped_count,
                    success_hook,
                    failure_hook,
                    archive_path,
                },
            )
            .await?;
            if should_stop_batch(failure_mode, max_failures, failure_count) {
                stop_scheduling = true;
                break;
            }
        }

        if shutdown.load(Ordering::SeqCst) || stop_scheduling {
            break;
        }

        // Pull the next entry from the stream.
        let entry = match entries.recv().await {
            Some(Ok(entry)) => entry,
            Some(Err(e)) => return Err(e),
            None => break, // stream exhausted
        };

        let line_number = entry.line_number;
        let link = entry.link;
        let link_key = normalize_link_key(&link);

        if completed_links.contains(&link_key) {
            checkpoint_skip_count += 1;
            crate::output::stderrln(format!(
                "[{}] Skipping completed checkpoint entry {}",
                line_number, link
            ));
            continue;
        }

        if !seen_links.insert(link_key.clone()) {
            duplicate_skip_count += 1;
            let value = json!({
                "status": "skipped",
                "reason": "duplicate-input",
                "input_line": line_number,
                "link": link,
            });
            append_manifest_record(
                &mut writer,
                &ManifestRecord {
                    index: line_number,
                    link,
                    status: "skipped".to_string(),
                    file: None,
                    error: Some("duplicate input".to_string()),
                    timestamp_unix: crate::output::unix_timestamp(),
                    canonical_source_link: None,
                },
            )
            .await?;
            skipped_count += 1;
            crate::output::log_event("download", "item_skipped", &value)?;
            emit_download_result(&value)?;
            continue;
        }

        let request = SingleDownloadRequest {
            selector: MessageSelectorArgs {
                link: Some(link.clone()),
                peer: None,
                msg_id: None,
            },
            out_dir: out_dir.to_path_buf(),
            collision,
            retry,
            dry_run,
            media_variant,
            name_template: name_template.map(ToOwned::to_owned),
            output_layout,
            metadata_sidecar,
            caption_sidecar,
            hash,
            redownload_on_mismatch,
            print_path_only,
            parallel_chunks,
            keep_partial,
            timeouts,
        };
        let task_client = client_handle.clone();
        let task_shutdown = shutdown.clone();
        let task_caches = Arc::clone(caches);
        join_set.spawn(async move {
            BatchTaskResult {
                line_number,
                link,
                result: download_one(&task_client, &request, &task_shutdown, &task_caches).await,
            }
        });
    }

    // Drain remaining in-flight tasks.
    while let Some(task_result) = join_set.join_next().await {
        let finished = task_result?;
        handle_batch_result(
            finished,
            BatchResultContext {
                checkpoint_path,
                writer: &mut writer,
                completed_links: &mut completed_links,
                success_count: &mut success_count,
                failure_count: &mut failure_count,
                skipped_count: &mut skipped_count,
                success_hook,
                failure_hook,
                archive_path,
            },
        )
        .await?;
    }

    if shutdown.load(Ordering::SeqCst) {
        bail!(
            "Batch interrupted with {} succeeded, {} skipped, {} failed, {} restored from checkpoint.",
            success_count,
            skipped_count,
            failure_count,
            checkpoint_skip_count + duplicate_skip_count
        );
    }

    if failure_count > 0 {
        bail!(
            "Batch download completed with {} succeeded, {} skipped, {} restored from checkpoint or dedupe, and {} failed.",
            success_count,
            skipped_count,
            checkpoint_skip_count + duplicate_skip_count,
            failure_count
        );
    }

    let summary = json!({
        "status": "completed",
        "succeeded": success_count,
        "skipped": skipped_count,
        "checkpoint_skipped": checkpoint_skip_count,
        "dedupe_skipped": duplicate_skip_count,
        "failed": failure_count,
        "checkpoint": checkpoint_path.display().to_string(),
        "dry_run": dry_run,
    });
    crate::output::log_event("download", "batch_completed", &summary)?;
    emit_download_result(&summary)?;
    Ok(())
}

async fn handle_batch_result(result: BatchTaskResult, ctx: BatchResultContext<'_>) -> Result<()> {
    let BatchResultContext {
        checkpoint_path,
        writer,
        completed_links,
        success_count,
        failure_count,
        skipped_count,
        success_hook,
        failure_hook,
        archive_path,
    } = ctx;

    match result.result {
        Ok(value) => {
            let status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("downloaded");
            if status == "skipped" {
                *skipped_count += 1;
            } else {
                *success_count += 1;
                completed_links.insert(normalize_link_key(&result.link));
            }

            append_manifest_record(
                writer,
                &ManifestRecord {
                    index: result.line_number,
                    link: result.link,
                    status: status.to_string(),
                    file: value
                        .get("file")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    error: None,
                    timestamp_unix: crate::output::unix_timestamp(),
                    canonical_source_link: value
                        .get("canonical_source_link")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                },
            )
            .await?;

            maybe_archive_result(archive_path, &value)?;
            crate::output::log_event("download", "item_completed", &value)?;
            crate::output::run_hook(success_hook, "download", "item_completed", &value).await?;
            emit_download_result(&value)?;
        }
        Err(error) => {
            *failure_count += 1;
            let rendered = format!("{:#}", error);
            let failure_value = json!({
                "status": "failed",
                "input_line": result.line_number,
                "link": result.link.clone(),
                "error": rendered,
            });
            append_manifest_record(
                writer,
                &ManifestRecord {
                    index: result.line_number,
                    link: result.link.clone(),
                    status: "failed".to_string(),
                    file: None,
                    error: Some(
                        failure_value
                            .get("error")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    ),
                    timestamp_unix: crate::output::unix_timestamp(),
                    canonical_source_link: None,
                },
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to update checkpoint '{}'",
                    checkpoint_path.display()
                )
            })?;

            crate::output::log_event("download", "item_failed", &failure_value)?;
            crate::output::run_hook(failure_hook, "download", "item_failed", &failure_value)
                .await?;
            crate::output::stderrln(format!(
                "[{}] ERROR {}: {}",
                result.line_number,
                result.link,
                failure_value["error"].as_str().unwrap_or_default()
            ));
        }
    }

    Ok(())
}

pub(crate) fn should_stop_batch(
    failure_mode: BatchFailureMode,
    max_failures: Option<usize>,
    failure_count: usize,
) -> bool {
    if matches!(failure_mode, BatchFailureMode::FailFast) && failure_count > 0 {
        return true;
    }

    max_failures.is_some_and(|limit| failure_count >= limit)
}

pub(crate) fn emit_download_result(value: &Value) -> Result<()> {
    if value
        .get("print_path_only")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(items) = value.get("items").and_then(Value::as_array) {
            for item in items {
                if let Some(path) = item.get("file").and_then(Value::as_str) {
                    println!("{}", path);
                }
            }
            return Ok(());
        }
        if let Some(path) = value.get("file").and_then(Value::as_str) {
            println!("{}", path);
            return Ok(());
        }
    }

    crate::output::write_command_output("download", value.clone())
}

pub(crate) fn maybe_archive_result(path: Option<&Path>, value: &Value) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    crate::output::archive_append(
        path,
        &crate::output::event_output("download", "archive_item", value.clone()),
    )
}

pub(crate) async fn load_completed_links(path: &Path) -> Result<HashSet<String>> {
    let mut completed = HashSet::new();
    let records = match read_manifest_records(path).await {
        Ok(records) => records,
        Err(error) if format!("{:#}", error).contains("does not exist") => return Ok(completed),
        Err(error) => return Err(error),
    };
    for entry in records {
        if entry.status == "downloaded" {
            completed.insert(normalize_link_key(&entry.link));
        }
    }

    Ok(completed)
}

pub(crate) async fn load_failed_links(path: &Path) -> Result<Vec<BatchEntry>> {
    let records = read_manifest_records(path).await?;
    let mut failed = Vec::new();
    let mut index = 0usize;

    for entry in records {
        if entry.status == "failed" {
            index += 1;
            failed.push(BatchEntry {
                line_number: index,
                link: entry.link,
            });
        }
    }

    Ok(failed)
}

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv_batch_entries_with_header() {
        let entries = parse_csv_batch_entries(
            &[
                SourceLine {
                    line_number: 1,
                    text: "link,label".to_string(),
                },
                SourceLine {
                    line_number: 2,
                    text: "https://t.me/example/1,first".to_string(),
                },
                SourceLine {
                    line_number: 3,
                    text: "https://t.me/example/2,second".to_string(),
                },
            ],
            BatchLineRange::new(None, None).unwrap(),
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].line_number, 2);
        assert_eq!(entries[0].link, "https://t.me/example/1");
        assert_eq!(entries[1].line_number, 3);
        assert_eq!(entries[1].link, "https://t.me/example/2");
    }

    #[test]
    fn test_parse_jsonl_batch_entries() {
        let entries = parse_jsonl_batch_entries(
            &[
                SourceLine {
                    line_number: 1,
                    text: "\"https://t.me/example/1\"".to_string(),
                },
                SourceLine {
                    line_number: 2,
                    text: "{\"link\":\"https://t.me/example/2\"}".to_string(),
                },
            ],
            BatchLineRange::new(None, None).unwrap(),
        )
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].link, "https://t.me/example/1");
        assert_eq!(entries[1].link, "https://t.me/example/2");
    }

    #[test]
    fn test_parse_text_batch_entries_respects_line_range() {
        let entries = parse_text_batch_entries(
            &[
                SourceLine {
                    line_number: 1,
                    text: "# comment".to_string(),
                },
                SourceLine {
                    line_number: 2,
                    text: "https://t.me/example/1".to_string(),
                },
                SourceLine {
                    line_number: 3,
                    text: "https://t.me/example/2".to_string(),
                },
            ],
            BatchLineRange::new(Some(3), Some(3)).unwrap(),
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line_number, 3);
        assert_eq!(entries[0].link, "https://t.me/example/2");
    }

    #[test]
    fn test_should_stop_batch() {
        assert!(should_stop_batch(BatchFailureMode::FailFast, None, 1));
        assert!(should_stop_batch(
            BatchFailureMode::ContinueOnError,
            Some(2),
            2
        ));
        assert!(!should_stop_batch(
            BatchFailureMode::ContinueOnError,
            Some(2),
            1
        ));
    }

    #[test]
    fn test_checkpoint_path_default() {
        let list = Path::new("/tmp/links.txt");
        assert_eq!(
            checkpoint_path_for(list, None),
            PathBuf::from("/tmp/links.txt.checkpoint.jsonl")
        );
    }

    #[tokio::test]
    async fn test_load_completed_links() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-checkpoint-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let mut file = open_manifest_writer(&path).await.unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 1,
                link: "https://t.me/example/1".to_string(),
                status: "downloaded".to_string(),
                file: Some("/tmp/out.mp4".to_string()),
                error: None,
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 2,
                link: "https://t.me/example/2".to_string(),
                status: "failed".to_string(),
                file: None,
                error: Some("boom".to_string()),
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();

        let links = load_completed_links(&path).await.unwrap();
        assert!(links.contains(&normalize_link_key("https://t.me/example/1")));
        assert!(!links.contains(&normalize_link_key("https://t.me/example/2")));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn test_load_failed_links() {
        let path = std::env::temp_dir().join(format!(
            "tw-dl-failed-links-{}-{}.jsonl",
            std::process::id(),
            crate::output::unix_timestamp()
        ));
        let mut file = open_manifest_writer(&path).await.unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 1,
                link: "https://t.me/example/1".to_string(),
                status: "downloaded".to_string(),
                file: Some("/tmp/out.mp4".to_string()),
                error: None,
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();
        append_manifest_record(
            &mut file,
            &ManifestRecord {
                index: 2,
                link: "https://t.me/example/2".to_string(),
                status: "failed".to_string(),
                file: None,
                error: Some("boom".to_string()),
                timestamp_unix: crate::output::unix_timestamp(),
                canonical_source_link: None,
            },
        )
        .await
        .unwrap();

        let links = load_failed_links(&path).await.unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].line_number, 1);
        assert_eq!(links[0].link, "https://t.me/example/2");

        let _ = std::fs::remove_file(path);
    }
}
