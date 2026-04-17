use anyhow::{bail, Result};
use std::path::PathBuf;
use std::time::Duration;

/// Options for the `download` command.
pub struct DownloadArgs {
    pub link: Option<String>,
    pub peer: Option<String>,
    pub msg_id: Option<i32>,
    pub out_dir: PathBuf,
    pub file_list: Option<PathBuf>,
    pub collision: CollisionPolicy,
    pub retry: RetryConfig,
    pub jobs: usize,
    pub input_format: BatchInputFormat,
    pub failure_mode: BatchFailureMode,
    pub max_failures: Option<usize>,
    pub from_line: Option<usize>,
    pub to_line: Option<usize>,
    pub checkpoint: Option<PathBuf>,
    pub dry_run: bool,
    pub retry_from: Option<PathBuf>,
    pub log_file: Option<PathBuf>,
    pub success_hook: Option<String>,
    pub failure_hook: Option<String>,
    pub archive_path: Option<PathBuf>,
    pub media_variant: MediaVariant,
    pub name_template: Option<String>,
    pub output_layout: OutputLayout,
    pub metadata_sidecar: bool,
    pub caption_sidecar: Option<CaptionSidecarFormat>,
    pub hash: bool,
    pub redownload_on_mismatch: bool,
    pub print_path_only: bool,
    pub parallel_chunks: usize,
    pub keep_partial: bool,
    pub request_timeout: Option<Duration>,
    pub item_timeout: Option<Duration>,
    pub batch_timeout: Option<Duration>,
}

/// Options for the `inspect` command.
pub struct InspectArgs {
    pub link: Option<String>,
    pub peer: Option<String>,
    pub msg_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchInputFormat {
    Auto,
    Text,
    Csv,
    Jsonl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFailureMode {
    ContinueOnError,
    FailFast,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollisionPolicy {
    Error,
    SkipExisting,
    Overwrite,
    Resume,
    SuffixExisting,
}

impl CollisionPolicy {
    pub fn from_flags(
        skip_existing: bool,
        overwrite: bool,
        resume: bool,
        suffix_existing: bool,
    ) -> Self {
        if skip_existing {
            Self::SkipExisting
        } else if overwrite {
            Self::Overwrite
        } else if resume {
            Self::Resume
        } else if suffix_existing {
            Self::SuffixExisting
        } else {
            Self::Error
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaVariant {
    Auto,
    LargestPhoto,
    OriginalDocument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputLayout {
    Flat,
    Chat,
    Date,
    MediaType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptionSidecarFormat {
    Txt,
    Json,
}

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub(crate) retries: u32,
    pub(crate) initial_delay: Duration,
    pub(crate) max_delay: Duration,
}

impl RetryConfig {
    pub fn new(retries: u32, initial_delay: Duration, max_delay: Duration) -> Result<Self> {
        if initial_delay.is_zero() {
            bail!("retry_delay_ms must be greater than zero");
        }
        if max_delay < initial_delay {
            bail!("max_retry_delay_ms must be greater than or equal to retry_delay_ms");
        }

        Ok(Self {
            retries,
            initial_delay,
            max_delay,
        })
    }

    pub(crate) fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        let base = self.initial_delay.saturating_mul(factor);
        let capped = base.min(self.max_delay);
        capped.saturating_add(Duration::from_millis(((attempt as u64) * 137) % 251))
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            retries: 3,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct TimeoutConfig {
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) item_timeout: Option<Duration>,
    pub(crate) batch_timeout: Option<Duration>,
}

impl TimeoutConfig {
    pub(crate) fn from_args(args: &DownloadArgs) -> Self {
        Self {
            request_timeout: args.request_timeout,
            item_timeout: args.item_timeout,
            batch_timeout: args.batch_timeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collision_policy_from_flags() {
        assert_eq!(
            CollisionPolicy::from_flags(true, false, false, false),
            CollisionPolicy::SkipExisting
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, true, false, false),
            CollisionPolicy::Overwrite
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, true, false),
            CollisionPolicy::Resume
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, false, true),
            CollisionPolicy::SuffixExisting
        );
        assert_eq!(
            CollisionPolicy::from_flags(false, false, false, false),
            CollisionPolicy::Error
        );
    }

    #[test]
    fn test_retry_delay_validation() {
        assert!(RetryConfig::new(3, Duration::ZERO, Duration::from_secs(1)).is_err());
        assert!(RetryConfig::new(3, Duration::from_secs(2), Duration::from_secs(1)).is_err());
    }
}
