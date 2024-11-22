use crate::glue::{analyze_glue_message, decode_glue_message, GlueSchemaRegistryFacade};
use crate::highlight::{format_avro_value, format_json_value, format_null, Highlighting};

pub enum Payload {
    Null,
    String(String),
    Json(serde_json::Value),
    Avro(avro_rs::types::Value),
    Unknown(String),
}

pub async fn parse_payload(payload: Option<&[u8]>, glue_schema_registry_facade: &GlueSchemaRegistryFacade) -> Payload {
    match payload {
        None => Payload::Null,
        Some(payload) => {
            if payload.len() == 0 {
                Payload::String("".to_string())
            } else {
                parse_payload_bytes(payload, glue_schema_registry_facade).await
            }
        }
    }
}

async fn parse_payload_bytes(payload: &[u8], glue_schema_registry_facade: &GlueSchemaRegistryFacade) -> Payload {
    if let Ok(string) = std::str::from_utf8(payload) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(string) {
            Payload::Json(value)
        } else {
            Payload::String(string.to_string())
        }
    } else {
        if let Ok(message) = analyze_glue_message(payload) {
            let schema_id = message.schema_id.clone();
            match decode_glue_message(message, glue_schema_registry_facade).await {
                Ok(value) =>
                    Payload::Avro(value),
                Err(error) =>
                    panic!("Could not decode AVRO encoded message with AWS Glue Schema Registry schema id {schema_id}: {error}."),
            }
        } else {
            Payload::Unknown(String::from_utf8_lossy(payload).to_string())
        }
    }
}

pub fn render_payload(payload: &Payload, highlighting: &Highlighting) -> String {
    match payload {
        Payload::Null => format_null(highlighting),
        Payload::String(string) => string.clone(),
        Payload::Json(value) => format_json_value(value, highlighting),
        Payload::Avro(value) => format_avro_value(value, highlighting),
        Payload::Unknown(string) => format!("{style}{string}{style:#}", style = highlighting.dimmed),
    }
}
