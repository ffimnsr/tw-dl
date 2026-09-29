use grammers_client::media::Media;
use grammers_client::message::Message;
use grammers_client::peer::Peer;
use serde_json::{json, Value};

pub(crate) fn describe_message(message: &Message) -> Value {
    let peer = message.peer().map(describe_peer);
    let sender = message.sender().map(describe_peer);
    json!({
        "id": message.id(),
        "peer_id": message.peer_id().bot_api_dialog_id(),
        "canonical_source_link": canonical_source_link(message),
        "date": message.date().to_rfc3339(),
        "text": message.text(),
        "has_media": message.media().is_some(),
        "grouped_id": message.grouped_id(),
        "peer": peer,
        "sender": sender,
        "media": message.media().map(|media| describe_media(&media)),
    })
}

pub(crate) fn describe_messages(messages: &[Message], anchor_msg_id: Option<i32>) -> Value {
    if messages.is_empty() {
        return json!({});
    }

    let items: Vec<Value> = messages.iter().map(describe_message).collect();

    let anchor_index = anchor_msg_id
        .and_then(|id| messages.iter().position(|m| m.id() == id))
        .unwrap_or(0);

    let mut base = items[anchor_index].clone();
    if let Some(map) = base.as_object_mut() {
        map.insert("item_count".to_string(), json!(messages.len()));
        if let Some(gid) = messages[anchor_index]
            .grouped_id()
            .or_else(|| messages.iter().find_map(|m| m.grouped_id()))
        {
            map.insert("grouped_id".to_string(), json!(gid));
        }

        let text_empty = map
            .get("text")
            .and_then(Value::as_str)
            .map_or(true, str::is_empty);
        if text_empty {
            if let Some(other_text) = messages.iter().find_map(|m| {
                let t = m.text();
                if !t.is_empty() {
                    Some(t)
                } else {
                    None
                }
            }) {
                map.insert("text".to_string(), json!(other_text));
            }
        }

        map.insert("items".to_string(), json!(items.clone()));
        map.insert("messages".to_string(), json!(items));
    }
    base
}

pub(crate) fn describe_peer(peer: &Peer) -> Value {
    json!({
        "bare_id": peer.id().bare_id(),
        "id": peer.id().bot_api_dialog_id(),
        "type": peer_kind_name(peer),
        "name": peer.name(),
        "username": peer.username(),
        "usernames": peer.usernames(),
    })
}

pub(crate) fn peer_kind_name(peer: &Peer) -> &'static str {
    match peer {
        Peer::User(_) => "user",
        Peer::Group(_) => "group",
        Peer::Channel(_) => "channel",
    }
}

pub(crate) fn describe_media(media: &Media) -> Value {
    match media {
        Media::Document(doc) => json!({
            "type": "document",
            "id": doc.id(),
            "filename": doc.name(),
            "mime_type": doc.mime_type(),
            "size": doc.size(),
            "creation_date": doc.creation_date().map(|date| date.to_rfc3339()),
        }),
        Media::Photo(photo) => json!({
            "type": "photo",
            "id": photo.id(),
            "size": photo.size(),
            "ttl_seconds": photo.ttl_seconds(),
            "thumb_count": photo.thumbs().len(),
        }),
        other => json!({
            "type": "unsupported",
            "debug": format!("{:?}", other),
        }),
    }
}

pub(crate) fn canonical_source_link(message: &Message) -> Option<String> {
    if let Some(peer) = message.peer() {
        if let Some(username) = peer.username() {
            return Some(format!("https://t.me/{}/{}", username, message.id()));
        }
    }

    let dialog_id = message.peer_id().bot_api_dialog_id().to_string();
    dialog_id
        .strip_prefix("-100")
        .map(|channel_id| format!("https://t.me/c/{}/{}", channel_id, message.id()))
}
