use anyhow::{Context, Result};
use grammers_client::message::Message;
use grammers_client::peer::Peer;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::RwLock;

use super::resolver::PeerSpec;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct MessageCacheKey {
    pub(crate) peer_spec: PeerSpec,
    pub(crate) msg_id: i32,
}

#[derive(Default)]
pub(crate) struct DownloadCaches {
    pub(crate) username_peer_refs: RwLock<HashMap<String, PeerRef>>,
    pub(crate) id_peer_refs: RwLock<HashMap<i64, PeerRef>>,
    pub(crate) id_peers: RwLock<HashMap<i64, Peer>>,
    pub(crate) messages: RwLock<HashMap<MessageCacheKey, Message>>,
    pub(crate) dialog_cache_loaded: AtomicBool,
}

pub(crate) struct ResilientClient {
    pub(crate) api_id: i32,
    pub(crate) session_path: PathBuf,
    pub(crate) session_options: crate::auth::SessionOptions,
    inner: tokio::sync::Mutex<crate::auth::AppClient>,
    pacing_until: tokio::sync::Mutex<Option<tokio::time::Instant>>,
}

impl ResilientClient {
    pub(crate) async fn new(
        api_id: i32,
        session_path: PathBuf,
        session_options: crate::auth::SessionOptions,
    ) -> Result<Self> {
        let inner = crate::auth::build_client(api_id, &session_path, session_options).await?;
        Ok(Self {
            api_id,
            session_path,
            session_options,
            inner: tokio::sync::Mutex::new(inner),
            pacing_until: tokio::sync::Mutex::new(None),
        })
    }

    pub(crate) async fn client(&self) -> Client {
        self.inner.lock().await.client()
    }

    pub(crate) async fn reconnect(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        *guard = crate::auth::build_client(self.api_id, &self.session_path, self.session_options)
            .await?;
        Ok(())
    }

    pub(crate) async fn wait_for_pacing(&self) {
        let until = *self.pacing_until.lock().await;
        if let Some(until) = until {
            let now = tokio::time::Instant::now();
            if until > now {
                tokio::time::sleep_until(until).await;
            }
        }
    }

    pub(crate) async fn apply_pacing(&self, delay: Duration) {
        let until = tokio::time::Instant::now() + delay;
        let mut guard = self.pacing_until.lock().await;
        match *guard {
            Some(existing) if existing > until => {}
            _ => *guard = Some(until),
        }
    }

    pub(crate) async fn clear_pacing_if_elapsed(&self) {
        let now = tokio::time::Instant::now();
        let mut guard = self.pacing_until.lock().await;
        if guard.is_some_and(|until| until <= now) {
            *guard = None;
        }
    }

    /// Check whether the current session is authorized with Telegram.
    #[allow(dead_code)]
    pub(crate) async fn is_authorized(&self) -> Result<bool> {
        let current_client = self.inner.lock().await.client();
        current_client.is_authorized().await.map_err(Into::into)
    }
}

pub(crate) async fn run_request<T, F>(
    timeout: Option<Duration>,
    timeout_message: &str,
    future: F,
) -> Result<T>
where
    F: Future<Output = T>,
{
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, future)
            .await
            .with_context(|| timeout_message.to_string())
    } else {
        Ok(future.await)
    }
}
