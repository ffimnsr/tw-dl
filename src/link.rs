use anyhow::{bail, Result};
use regex::Regex;
use std::sync::OnceLock;

fn private_link_regex() -> &'static Regex {
    static PRIVATE_RE: OnceLock<Regex> = OnceLock::new();
    PRIVATE_RE.get_or_init(|| Regex::new(r"^t\.me/c/(\d+)/(\d+)$").unwrap())
}

fn public_link_regex() -> &'static Regex {
    static PUBLIC_RE: OnceLock<Regex> = OnceLock::new();
    PUBLIC_RE.get_or_init(|| Regex::new(r"^t\.me/([\w\d_]+)/(\d+)$").unwrap())
}

/// A parsed Telegram message link.
#[derive(Debug, Clone)]
pub enum ParsedLink {
    /// Public channel/group link: t.me/<username>/<msg_id>
    Username { username: String, msg_id: i32 },
    /// Private/supergroup link: t.me/c/<channel_id>/<msg_id>
    Channel { channel_id: i64, msg_id: i32 },
}

/// Parse a Telegram message URL or peer+msg_id specification.
///
/// Accepted formats:
/// - `https://t.me/<username>/<msg_id>`
/// - `https://t.me/c/<channel_id>/<msg_id>`
/// - `t.me/<username>/<msg_id>`
/// - `t.me/c/<channel_id>/<msg_id>`
pub fn parse_link(input: &str) -> Result<ParsedLink> {
    // Normalise by stripping leading https?:// and trailing slash
    let input = input.trim().trim_end_matches('/');
    let input = input
        .strip_prefix("https://")
        .or_else(|| input.strip_prefix("http://"))
        .unwrap_or(input);

    // t.me/c/<channel_id>/<msg_id>  (private/supergroup)
    if let Some(caps) = private_link_regex().captures(input) {
        let channel_id: i64 = caps[1].parse()?;
        let msg_id: i32 = caps[2].parse()?;
        return Ok(ParsedLink::Channel { channel_id, msg_id });
    }

    // t.me/<username>/<msg_id>  (public)
    if let Some(caps) = public_link_regex().captures(input) {
        let username = caps[1].to_string();
        let msg_id: i32 = caps[2].parse()?;
        return Ok(ParsedLink::Username { username, msg_id });
    }

    bail!(
        "Could not parse link {:?}. Expected formats:\n  \
         https://t.me/<username>/<msg_id>\n  \
         https://t.me/c/<channel_id>/<msg_id>",
        input
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_link_https() {
        let parsed = parse_link("https://t.me/mychannel/42").unwrap();
        match parsed {
            ParsedLink::Username { username, msg_id } => {
                assert_eq!(username, "mychannel");
                assert_eq!(msg_id, 42);
            }
            _ => panic!("expected Username variant"),
        }
    }

    #[test]
    fn test_private_link_https() {
        let parsed = parse_link("https://t.me/c/1234567890/99").unwrap();
        match parsed {
            ParsedLink::Channel { channel_id, msg_id } => {
                assert_eq!(channel_id, 1234567890);
                assert_eq!(msg_id, 99);
            }
            _ => panic!("expected Channel variant"),
        }
    }

    #[test]
    fn test_link_no_scheme() {
        let parsed = parse_link("t.me/some_channel/7").unwrap();
        match parsed {
            ParsedLink::Username { username, msg_id } => {
                assert_eq!(username, "some_channel");
                assert_eq!(msg_id, 7);
            }
            _ => panic!("expected Username variant"),
        }
    }

    #[test]
    fn test_invalid_link() {
        assert!(parse_link("https://example.com/foo").is_err());
    }
}
