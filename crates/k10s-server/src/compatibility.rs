use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};

use axum::extract::ws::Message;
use k10s_protocol::{PROTOCOL_MINOR, ProtocolVersion, ServerFrame};

const TYPED_DETAIL_PROJECTIONS_MINOR: u16 = 3;

/// Per-session protocol policy applied at the outbound serialization boundary.
#[derive(Debug, Clone)]
pub(crate) struct SessionProtocol {
    minor: Arc<AtomicU16>,
}

impl SessionProtocol {
    pub(crate) fn current() -> Self {
        Self {
            minor: Arc::new(AtomicU16::new(PROTOCOL_MINOR)),
        }
    }

    pub(crate) fn set_negotiated(&self, protocol: ProtocolVersion) {
        self.minor.store(protocol.minor, Ordering::Release);
    }

    pub(crate) fn compatible_value<T: serde::Serialize>(&self, value: T) -> serde_json::Value {
        let mut value = serde_json::to_value(value).expect("server payload serializes");
        self.prepare_value(&mut value);
        value
    }

    pub(crate) fn prepare_message(&self, message: Message) -> Message {
        if self.minor.load(Ordering::Acquire) >= TYPED_DETAIL_PROJECTIONS_MINOR {
            return message;
        }
        let Message::Text(text) = message else {
            return message;
        };
        let Ok(mut frame) = serde_json::from_str::<ServerFrame>(&text) else {
            return Message::Text(text);
        };
        self.prepare_value(&mut frame.payload);
        Message::Text(
            serde_json::to_string(&frame)
                .expect("compatible server frame serializes")
                .into(),
        )
    }

    fn prepare_value(&self, value: &mut serde_json::Value) {
        if self.minor.load(Ordering::Acquire) >= TYPED_DETAIL_PROJECTIONS_MINOR {
            return;
        }
        strip_v13_projections(value);
    }
}

fn strip_v13_projections(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            let incompatible = object
                .get("projection")
                .and_then(serde_json::Value::as_object)
                .and_then(|projection| projection.get("kind"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(|kind| matches!(kind, "pod" | "deployment" | "replicaSet"));
            if incompatible {
                object.remove("projection");
            }
            for child in object.values_mut() {
                strip_v13_projections(child);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                strip_v13_projections(child);
            }
        }
        _ => {}
    }
}
