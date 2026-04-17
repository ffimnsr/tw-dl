use anyhow::{bail, Context, Result};
use grammers_client::message::Message;
use grammers_client::peer::Peer;
use grammers_session::types::PeerRef;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::link::{parse_link, ParsedLink};
use super::client::{DownloadCaches, MessageCacheKey, ResilientClient, run_request};
use super::retry::{prepare_retry, retryable_delay};
use super::types::{RetryConfig, TimeoutConfig};

const GROUP_FETCH_WINDOW: i32 = 64;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum PeerSpec {
    Username(String),
    ChannelId(i64),
}

#[derive(Clone)]
pub(crate) struct MessageSelectorArgs {
    pub(crate) link: Option<String>,
    pub(crate) peer: Option<String>,
    pub(crate) msg_id: Option<i32>,
}

pub(crate) fn warn_about_ambiguous_selector(selector: &MessageSelectorArgs) {
    if let Some(peer) = selector.peer.as_deref() {
        warn_about_numeric_peer_input(peer);
    }
}

pub(crate) fn warn_about_numeric_peer_input(input: &str) {
    if input.parse::<i64>().is_ok() {
        crate::output::stderrln(
            "Warning: numeric peer ids are Telegram bare ids and can be ambiguous. \
Prefer a full Telegram message link or @username when possible.",
        );
    }
}

pub(crate) fn resolve_peer_msg(args: &MessageSelectorArgs) -> Result<(PeerSpec, i32)> {
    if let (Some(peer), Some(msg)) = (&args.peer, args.msg_id) {
        let spec = if let Ok(id) = peer.parse::<i64>() {
            PeerSpec::ChannelId(id)
        } else {
            PeerSpec::Username(peer.trim_start_matches('@').to_string())
        };
        return Ok((spec, msg));
    }

    let link_str = args
        .link
        .as_deref()
        .context("Provide a link or --peer / --msg")?;

    match parse_link(link_str)? {
        ParsedLink::Username { username, msg_id } => Ok((PeerSpec::Username(username), msg_id)),
        ParsedLink::Channel { channel_id, msg_id } => Ok((PeerSpec::ChannelId(channel_id), msg_id)),
    }
}

pub(crate) async fn fetch_message_with_retry(
    client: &ResilientClient,
    selector: &MessageSelectorArgs,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Message> {
    let (peer_spec, msg_id) = resolve_peer_msg(selector)?;
    retry_fetch_message(client, &peer_spec, msg_id, retry, timeouts, caches).await
}

pub(crate) async fn fetch_messages_with_retry(
    client: &ResilientClient,
    selector: &MessageSelectorArgs,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Vec<Message>> {
    let (peer_spec, msg_id) = resolve_peer_msg(selector)?;
    let message = retry_fetch_message(client, &peer_spec, msg_id, retry, timeouts, caches).await?;
    if let Some(grouped_id) = message.grouped_id() {
        return fetch_grouped_messages(client, &peer_spec, msg_id, grouped_id, timeouts, caches)
            .await;
    }

    Ok(vec![message])
}

async fn fetch_grouped_messages(
    client: &ResilientClient,
    peer_spec: &PeerSpec,
    anchor_msg_id: i32,
    grouped_id: i64,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Vec<Message>> {
    let peer_ref = resolve_peer_ref(client, peer_spec, timeouts, caches).await?;
    let current_client = client.client().await;
    let upper_bound = anchor_msg_id.saturating_add(GROUP_FETCH_WINDOW);
    let lower_bound = anchor_msg_id.saturating_sub(GROUP_FETCH_WINDOW);
    let mut iter = current_client
        .iter_messages(peer_ref)
        .offset_id(upper_bound);
    let mut messages = Vec::new();

    while let Some(message) = run_request(
        timeouts.request_timeout,
        "Grouped message request timed out",
        iter.next(),
    )
    .await??
    {
        if message.id() < lower_bound {
            break;
        }
        if message.grouped_id() == Some(grouped_id) {
            caches.messages.write().await.insert(
                MessageCacheKey {
                    peer_spec: peer_spec.clone(),
                    msg_id: message.id(),
                },
                message.clone(),
            );
            messages.push(message);
        }
    }

    messages.sort_by_key(Message::id);
    messages.dedup_by_key(|message| message.id());

    if messages.is_empty() {
        bail!(
            "Grouped media {} was not found around message {}",
            grouped_id,
            anchor_msg_id
        );
    }

    Ok(messages)
}

pub(crate) async fn retry_fetch_message(
    client: &ResilientClient,
    spec: &PeerSpec,
    msg_id: i32,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Message> {
    let cache_key = MessageCacheKey {
        peer_spec: spec.clone(),
        msg_id,
    };
    if let Some(message) = caches.messages.read().await.get(&cache_key).cloned() {
        return Ok(message);
    }

    let mut attempt = 0u32;

    loop {
        match fetch_message_once(client, spec, msg_id, timeouts, caches).await {
            Ok(message) => {
                caches
                    .messages
                    .write()
                    .await
                    .insert(cache_key.clone(), message.clone());
                return Ok(message);
            }
            Err(error) => {
                attempt += 1;
                if let Some(delay) = retryable_delay(&error, attempt, retry) {
                    prepare_retry(client, &error, delay).await?;
                    crate::output::stderrln(format!(
                        "Retrying message fetch after error (attempt {}/{}): {}",
                        attempt, retry.retries, error
                    ));
                    continue;
                }
                return Err(error);
            }
        }
    }
}

async fn fetch_message_once(
    client: &ResilientClient,
    spec: &PeerSpec,
    msg_id: i32,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Message> {
    let peer_ref = resolve_peer_ref(client, spec, timeouts, caches).await?;
    client.wait_for_pacing().await;
    let current_client = client.client().await;
    let mut messages = run_request(
        timeouts.request_timeout,
        "Message request timed out",
        current_client.get_messages_by_id(peer_ref, &[msg_id]),
    )
    .await
    .with_context(|| format!("Failed to fetch message id={}", msg_id))??;

    messages
        .pop()
        .flatten()
        .with_context(|| format!("Message id={} not found in chat", msg_id))
}

pub(crate) async fn find_peer_by_id(
    client: &ResilientClient,
    target_id: i64,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Peer> {
    if let Some(peer) = caches.id_peers.read().await.get(&target_id).cloned() {
        return Ok(peer);
    }

    populate_dialog_caches(client, timeouts, caches).await?;
    if let Some(peer) = caches.id_peers.read().await.get(&target_id).cloned() {
        return Ok(peer);
    }

    bail!(
        "Could not find a chat with id={} in your dialogs. Make sure your account has joined that channel/group.",
        target_id
    );
}

pub(crate) async fn find_peer_ref_by_id(
    client: &ResilientClient,
    target_id: i64,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<PeerRef> {
    if let Some(peer_ref) = caches.id_peer_refs.read().await.get(&target_id).copied() {
        return Ok(peer_ref);
    }

    populate_dialog_caches(client, timeouts, caches).await?;
    if let Some(peer_ref) = caches.id_peer_refs.read().await.get(&target_id).copied() {
        return Ok(peer_ref);
    }

    bail!(
        "Could not find a chat with id={} in your dialogs. Make sure your account has joined that channel/group.",
        target_id
    );
}

pub(crate) async fn resolve_peer_ref(
    client: &ResilientClient,
    spec: &PeerSpec,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<PeerRef> {
    match spec {
        PeerSpec::Username(username) => {
            if let Some(peer_ref) = caches
                .username_peer_refs
                .read()
                .await
                .get(username)
                .copied()
            {
                return Ok(peer_ref);
            }

            client.wait_for_pacing().await;
            let current_client = client.client().await;
            let peer = run_request(
                timeouts.request_timeout,
                "Username resolution timed out",
                current_client.resolve_username(username),
            )
            .await
            .with_context(|| format!("Failed to resolve username '{}'", username))??
            .with_context(|| format!("Username '{}' not found", username))?;
            let peer_ref = peer.to_ref().await.with_context(|| {
                format!("Could not obtain a usable reference for '@{}'", username)
            })?;
            caches
                .username_peer_refs
                .write()
                .await
                .insert(username.clone(), peer_ref);
            caches
                .id_peer_refs
                .write()
                .await
                .insert(peer.id().bare_id(), peer_ref);
            caches
                .id_peers
                .write()
                .await
                .insert(peer.id().bare_id(), peer);
            Ok(peer_ref)
        }
        PeerSpec::ChannelId(channel_id) => {
            find_peer_ref_by_id(client, *channel_id, timeouts, caches).await
        }
    }
}

pub(crate) async fn resolve_peer_from_ref(
    client: &ResilientClient,
    peer_ref: PeerRef,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Peer> {
    if let Some(peer) = caches
        .id_peers
        .read()
        .await
        .get(&peer_ref.id.bare_id())
        .cloned()
    {
        return Ok(peer);
    }

    populate_dialog_caches(client, timeouts, caches).await?;
    if let Some(peer) = caches
        .id_peers
        .read()
        .await
        .get(&peer_ref.id.bare_id())
        .cloned()
    {
        return Ok(peer);
    }

    find_peer_by_id(client, peer_ref.id.bare_id(), timeouts, caches).await
}

pub(crate) async fn populate_dialog_caches(
    client: &ResilientClient,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<()> {
    if caches.dialog_cache_loaded.load(Ordering::SeqCst) {
        return Ok(());
    }

    let current_client = client.client().await;
    let mut dialogs = current_client.iter_dialogs();
    loop {
        client.wait_for_pacing().await;
        let next_dialog = run_request(
            timeouts.request_timeout,
            "Dialog iteration timed out",
            dialogs.next(),
        )
        .await?;
        let Some(dialog) = next_dialog? else {
            break;
        };
        let bare_id = dialog.peer_id().bare_id();
        let peer_ref = dialog.peer_ref();
        let peer = dialog.peer().clone();
        caches.id_peer_refs.write().await.insert(bare_id, peer_ref);
        caches.id_peers.write().await.insert(bare_id, peer.clone());
        if let Some(username) = peer.username() {
            caches
                .username_peer_refs
                .write()
                .await
                .insert(username.to_string(), peer_ref);
        }
    }
    caches.dialog_cache_loaded.store(true, Ordering::SeqCst);
    Ok(())
}

/// Prefetch messages for a batch of entries into the cache so that subsequent
/// per-entry fetches are served from memory. Errors are silently swallowed so
/// that a failed prefetch never prevents the actual download from running.
#[allow(dead_code)]
pub(crate) async fn prefetch_messages_for_entries(
    client: &Arc<ResilientClient>,
    entries: &[super::batch::BatchEntry],
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<()> {
    let mut grouped: HashMap<PeerSpec, Vec<i32>> = HashMap::new();
    for entry in entries {
        let selector = MessageSelectorArgs {
            link: Some(entry.link.clone()),
            peer: None,
            msg_id: None,
        };
        if let Ok((peer_spec, msg_id)) = resolve_peer_msg(&selector) {
            grouped.entry(peer_spec).or_default().push(msg_id);
        }
    }

    for (peer_spec, msg_ids) in grouped {
        let peer_ref = match resolve_peer_ref(client, &peer_spec, timeouts, caches).await {
            Ok(peer_ref) => peer_ref,
            Err(_) => continue,
        };
        let unique_ids: HashSet<i32> = msg_ids.into_iter().collect();
        let ids: Vec<i32> = unique_ids.into_iter().collect();
        for chunk in ids.chunks(100) {
            client.wait_for_pacing().await;
            let current_client = client.client().await;
            let messages = match run_request(
                timeouts.request_timeout,
                "Prefetch message request timed out",
                current_client.get_messages_by_id(peer_ref, chunk),
            )
            .await
            {
                Ok(Ok(messages)) => messages,
                Err(_) => continue,
                Ok(Err(_)) => continue,
            };
            for message in messages.into_iter().flatten() {
                caches.messages.write().await.insert(
                    MessageCacheKey {
                        peer_spec: peer_spec.clone(),
                        msg_id: message.id(),
                    },
                    message,
                );
            }
        }
    }

    Ok(())
}

pub(crate) async fn resolve_target(
    client: &ResilientClient,
    target: &str,
    retry: RetryConfig,
    timeouts: TimeoutConfig,
    caches: &DownloadCaches,
) -> Result<Value> {
    let trimmed = target.trim();

    if parse_link(trimmed).is_ok() {
        let selector = MessageSelectorArgs {
            link: Some(trimmed.to_string()),
            peer: None,
            msg_id: None,
        };
        let (peer_spec, msg_id) = resolve_peer_msg(&selector)?;
        let message =
            retry_fetch_message(client, &peer_spec, msg_id, retry, timeouts, caches).await?;
        return Ok(json!({
            "input": trimmed,
            "input_type": "message_link",
            "peer_spec": describe_peer_spec(&peer_spec),
            "message_id": msg_id,
            "message": super::describe::describe_message(&message),
        }));
    }

    if let Ok(channel_id) = trimmed.parse::<i64>() {
        warn_about_numeric_peer_input(trimmed);
        let peer = find_peer_by_id(client, channel_id, timeouts, caches).await?;
        return Ok(json!({
            "input": trimmed,
            "input_type": "numeric_id",
            "peer": super::describe::describe_peer(&peer),
        }));
    }

    let username = trimmed.trim_start_matches('@');
    let peer_ref = resolve_peer_ref(
        client,
        &PeerSpec::Username(username.to_string()),
        timeouts,
        caches,
    )
    .await?;
    let peer = resolve_peer_from_ref(client, peer_ref, timeouts, caches).await?;

    Ok(json!({
        "input": trimmed,
        "input_type": "username",
        "peer": super::describe::describe_peer(&peer),
    }))
}

pub(crate) fn describe_peer_spec(spec: &PeerSpec) -> Value {
    match spec {
        PeerSpec::Username(username) => json!({
            "type": "username",
            "value": username,
        }),
        PeerSpec::ChannelId(channel_id) => json!({
            "type": "channel_id",
            "value": channel_id,
        }),
    }
}
