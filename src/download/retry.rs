use std::sync::OnceLock;
use std::time::Duration;

use regex::Regex;

use super::client::ResilientClient;
use super::types::RetryConfig;

/// Return the delay to use before the next retry, or `None` if the error is
/// non-retryable or the retry budget is exhausted.
pub(crate) fn retryable_delay(
    error: &anyhow::Error,
    attempt: u32,
    retry: RetryConfig,
) -> Option<Duration> {
    if attempt > retry.retries {
        return None;
    }

    let message = error.to_string();
    if let Some(wait_seconds) = flood_wait_seconds(&message) {
        return Some(Duration::from_secs(wait_seconds.max(1)));
    }

    if is_retryable_error(&message) {
        return Some(retry.delay_for_attempt(attempt));
    }

    None
}

pub(crate) async fn prepare_retry(
    client: &ResilientClient,
    error: &anyhow::Error,
    delay: Duration,
) -> anyhow::Result<()> {
    let message = error.to_string();
    if flood_wait_seconds(&message).is_some() {
        client.apply_pacing(delay).await;
    }
    if should_reconnect(&message) {
        client.reconnect().await?;
    }
    tokio::time::sleep(delay).await;
    client.clear_pacing_if_elapsed().await;
    Ok(())
}

pub(crate) fn flood_wait_seconds(message: &str) -> Option<u64> {
    static FLOOD_WAIT_RE: OnceLock<Regex> = OnceLock::new();
    let regex = FLOOD_WAIT_RE
        .get_or_init(|| Regex::new(r"(?i)(?:FLOOD_WAIT_|wait of )(\d+)(?: seconds?)?").unwrap());
    regex
        .captures(message)
        .and_then(|captures| captures.get(1))
        .and_then(|matched| matched.as_str().parse::<u64>().ok())
}

pub(crate) fn is_retryable_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    if is_permanent_error(&normalized) {
        return false;
    }

    [
        "flood wait",
        "rpc call fail",
        "timeout",
        "timed out",
        "connection reset",
        "connection aborted",
        "connection refused",
        "network",
        "temporarily unavailable",
        "transport",
        "broken pipe",
        "unexpected eof",
        "download error",
        "disconnected",
        "interrupted by peer",
    ]
    .iter()
    .any(|pattern| normalized.contains(pattern))
}

/// Actionable hint attached to session-level failures.
pub(crate) const SESSION_REPAIR_HINT: &str = "Telegram revoked this session's media authorization; run `tw-dl session repair` to refresh its cached datacenter keys";

/// Whether the error means the Telegram session itself is no longer usable
/// (revoked/expired authorization, or an unregistered auth key).
pub(crate) fn is_invalid_session_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    [
        "session_revoked",
        "session_expired",
        "auth_key_unregistered",
    ]
    .iter()
    .any(|pattern| normalized.contains(pattern))
}

/// Whether the error means the media's `file_reference` is no longer valid.
///
/// Telegram file references expire over time, so long downloads can fail partway
/// through. The fix is to re-fetch the message and resume with the new reference,
/// not to retry the same location.
pub(crate) fn is_file_reference_error(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    ["file_reference_expired", "file_reference_invalid"]
        .iter()
        .any(|pattern| normalized.contains(pattern))
}

/// Attach the repair hint to session-level failures, without duplicating it when
/// the error already carries it.
pub(crate) fn hint_invalid_session(error: anyhow::Error) -> anyhow::Error {
    if error.to_string().contains(SESSION_REPAIR_HINT) {
        return error;
    }

    if is_invalid_session_error(&error.to_string()) {
        error.context(SESSION_REPAIR_HINT)
    } else {
        error
    }
}

pub(crate) fn should_reconnect(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    [
        "connection reset",
        "connection aborted",
        "connection refused",
        "broken pipe",
        "transport",
        "unexpected eof",
        "disconnected",
        "interrupted by peer",
        "network",
    ]
    .iter()
    .any(|pattern| normalized.contains(pattern))
}

pub(crate) fn is_permanent_error(normalized_message: &str) -> bool {
    if is_invalid_session_error(normalized_message) || is_file_reference_error(normalized_message) {
        return true;
    }

    [
        "not logged in",
        "username '",
        "username not found",
        "message id=",
        "this message does not contain downloadable media",
        "unsupported media type",
        "could not find a chat with id=",
        "could not parse link",
        "phone code invalid",
        "phone code expired",
        "password hash invalid",
        "session password needed",
        "unauthorized",
        "forbidden",
        "access denied",
        "invalid peer",
        "peer id invalid",
        "chat admin required",
        "channel private",
        "invite request sent",
        "auth key",
    ]
    .iter()
    .any(|pattern| normalized_message.contains(pattern))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_error_detection() {
        assert!(is_retryable_error("network timeout while downloading"));
        assert!(!is_retryable_error("username not found"));
        assert!(!is_retryable_error("CHANNEL_PRIVATE"));
    }

    #[test]
    fn test_revoked_session_is_permanent() {
        let error = anyhow::anyhow!(
            "Download error: request error: rpc error 401: SESSION_REVOKED caused by upload.getFile"
        );
        assert!(!is_retryable_error(&error.to_string()));
        assert!(retryable_delay(&error, 1, RetryConfig::default()).is_none());
    }

    #[test]
    fn test_invalid_session_error_detection() {
        assert!(is_invalid_session_error("rpc error 401: SESSION_REVOKED"));
        assert!(is_invalid_session_error("AUTH_KEY_UNREGISTERED"));
        assert!(!is_invalid_session_error("FLOOD_WAIT_12"));
    }

    #[test]
    fn test_file_reference_error_is_permanent() {
        let error = anyhow::anyhow!(
            "Download error: request error: rpc error 400: FILE_REFERENCE_EXPIRED caused by upload.getFile"
        );
        assert!(is_file_reference_error(&error.to_string()));
        assert!(!is_retryable_error(&error.to_string()));
        assert!(retryable_delay(&error, 1, RetryConfig::default()).is_none());
    }

    #[test]
    fn test_hint_invalid_session_is_idempotent() {
        let error = anyhow::anyhow!(
            "Download error: request error: rpc error 401: SESSION_REVOKED caused by upload.getFile"
        );
        let hinted = hint_invalid_session(error);
        assert!(hinted.to_string().contains(SESSION_REPAIR_HINT));

        let twice = hint_invalid_session(hinted);
        assert_eq!(
            format!("{:#}", twice).matches(SESSION_REPAIR_HINT).count(),
            1
        );
    }

    #[test]
    fn test_hint_invalid_session_skips_unrelated_errors() {
        let error = anyhow::anyhow!("connection reset by peer");
        assert!(!hint_invalid_session(error)
            .to_string()
            .contains(SESSION_REPAIR_HINT));
    }

    #[test]
    fn test_flood_wait_parsing() {
        assert_eq!(flood_wait_seconds("FLOOD_WAIT_12"), Some(12));
        assert_eq!(
            flood_wait_seconds("A wait of 9 seconds is required"),
            Some(9)
        );
        assert_eq!(flood_wait_seconds("username not found"), None);
    }
}
