# ISSUES

This file tracks feature ideas and follow-up work for `tw-dl`.

The list is intentionally descriptive so it can serve as both a roadmap and an issue drafting source later. Items are grouped by theme, not by implementation order.

## Resiliency

- [x] Add retry with exponential backoff and jitter for username resolution, message fetches, and media chunk downloads.
- [x] Distinguish retryable errors from permanent ones more precisely. Network disconnects, flood waits, timeouts, and transient RPC failures should retry; auth and permission errors should fail immediately.
- [x] Add configurable timeouts for network operations, per-item work, and full batch runs.
- [x] Support resumable downloads from existing `.part` files instead of forcing manual cleanup.
- [x] Add `--keep-partial` so interrupted downloads can be preserved even when the user is not using resume mode.
- [x] Add checkpointing for batch mode so large `--file` runs can continue from previously completed items.
- [x] Handle graceful shutdown on `Ctrl+C` so the CLI stops new work, flushes state, and leaves downloads resumable.
- [x] Detect stale session locks more robustly and add either automatic expiry or a `--force-unlock` recovery flow.
- [x] Reconnect automatically if the MTProto connection drops mid-run in ways not already covered by retry logic.
- [x] Add adaptive pacing to slow down after rate-limit signals instead of continuing at a fixed retry schedule.

## Batch Downloading

- [x] Add `--jobs <N>` for concurrent batch downloads.
- [ ] Add explicit `--continue-on-error` and `--fail-fast` modes so batch behavior is intentional instead of implicit.
- [ ] Add `--max-failures <N>` to stop a batch after too many failed items.
- [ ] Add `--from-line` and `--to-line` to process only part of a large input file.
- [ ] Accept CSV or JSONL input in addition to plain text links.
- [ ] Support stdin input so users can pipe links directly into the downloader.
- [x] Write a JSONL-style checkpoint/result manifest for every batch item.
- [ ] Add input deduplication so the same message is not downloaded twice in one run.
- [x] Add explicit file collision policies with `--skip-existing`, `--overwrite`, and `--resume`.
- [x] Add a way to rerun only failures from a previous checkpoint or manifest file.

## Download Behavior

- [ ] Support downloading multiple media items from albums or grouped messages.
- [ ] Support choosing media variants, such as largest photo only or original document only.
- [ ] Add file naming templates like `--name-template "{chat}_{msg_id}_{filename}"`.
- [ ] Support output subdirectories by chat, date, or media type.
- [ ] Add filename collision suffixing as an alternative to skip, overwrite, or resume.
- [ ] Preserve Telegram message metadata in a sidecar file: message id, chat, sender, date, caption, mime type, and source link.
- [ ] Optionally write captions to `.txt` or `.json` sidecars.
- [ ] Add content hashing during or after download to support integrity checks and dedupe workflows.
- [ ] Add size verification or redownload-on-mismatch when a transfer looks truncated.
- [ ] Add `--print-path-only` for scripting use cases.
- [ ] Add `--no-progress` and `--quiet` modes for CI and automation.
- [ ] Add consistent `--json` and human-readable output modes across commands.

## Discovery and Inspection

- [x] Add `list-chats` to enumerate chats, channels, and groups the current account can access.
- [x] Add `resolve <link|peer>` to show how the CLI interprets a link, username, or numeric id.
- [x] Add `inspect <link|--peer --msg>` to print message metadata without downloading.
- [ ] Add `show-message` or `fetch-caption` to display message text, caption, and media summary directly.
- [ ] Add `history --peer <chat> --limit <N>` to browse recent messages.
- [ ] Add `search --peer <chat> --query <text>` for basic message lookup.
- [ ] Add `list-media --peer <chat> --from <id> --to <id>` to find downloadable messages before downloading them.
- [ ] Add `verify-session` to test whether a saved session is still authorized without doing a full download.
- [ ] Add `config show` to print effective config values and where they were loaded from.
- [x] Add `doctor` to validate config, session path, credentials, and authorization state.
- [ ] Add `version --json` for machine-readable diagnostics.

## Auth and Session

- [ ] Add non-interactive login inputs for automation where Telegram allows them safely.
- [ ] Add account switching with named profiles such as `--profile work` and `--profile personal`.
- [ ] Store multiple sessions under profile names instead of a single default session path.
- [ ] Add `session info` to show session path, last-used time, and authorization status.
- [ ] Add `logout --all-profiles` or `session clear --profile <name>`.
- [ ] Add `whoami --full` to expose more account fields when requested.
- [ ] Warn before destructive logout when multiple profile/session layouts exist.
- [ ] Add optional secure permission checks for session and config files on Unix-like systems.

## Performance

- [x] Cache peer resolution so repeated downloads from the same username do not resolve every time.
- [x] Cache numeric chat-id lookups instead of scanning dialogs repeatedly for the same batch.
- [x] Improve transfer and file-write pipelining for large downloads.
- [x] Add progress metrics like throughput, ETA stability, retry count, and backoff state.
- [x] Add optional parallel chunk downloading if the Telegram client layer supports it safely.
- [x] Avoid repeated metadata fetches when multiple batch items target the same peer.

## Filtering and Selection

- [ ] Add `download-range --peer <chat> --from <id> --to <id>` to fetch multiple messages at once.
- [ ] Add filters by media type such as photo, video, audio, document, voice, and sticker.
- [ ] Add filters by date range.
- [ ] Add filters by filename extension or MIME type.
- [ ] Add `--min-size` and `--max-size`.
- [ ] Add `--caption-contains` and `--sender`.
- [ ] Add `--only-albums` and `--exclude-albums`.

## Output and Integration

- [ ] Add shell completion generation for Bash, Zsh, Fish, and PowerShell.
- [ ] Stabilize and document the JSON output schema for scripting and integrations.
- [ ] Add `--log-file` and structured logs for long-running jobs.
- [ ] Add webhook support or success/failure hooks for automation.
- [ ] Add archive mode that stores downloaded item metadata in SQLite or JSON.
- [ ] Add export/import support for manifests and checkpoints.
- [ ] Emit canonical source links in manifests and output where possible.
- [ ] Split reusable logic into a library crate so other Rust tools can integrate with `tw-dl`.

## Safety and UX

- [x] Sanitize filenames more thoroughly for Windows reserved names and trailing dots/spaces.
- [x] Add stronger path traversal hardening for user-supplied filenames.
- [x] Add clearer error classification and exit codes for scripting.
- [x] Add confirmation or warning for ambiguous peer IDs.
- [x] Surface access and permission failures more explicitly, such as “not a member of this channel”.
- [x] Add `--dry-run` to resolve and inspect targets without downloading.
- [x] Add `--yes` for future non-interactive flows that may otherwise prompt.
- [x] Expand help text and examples for private links, batch checkpoints, retries, and profile-based usage.

## Testing and Quality

- [ ] Add integration tests around CLI flows such as inspect, doctor, and batch checkpoint recovery.
- [ ] Add retry/backoff tests with injected transient failures.
- [ ] Add tests for interrupted download recovery and resume behavior across multiple retries.
- [ ] Add golden tests for JSON output stability.
- [ ] Add CI checks for `cargo clippy` and cross-platform filename behavior.
