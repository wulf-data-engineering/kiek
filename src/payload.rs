use crate::glue::{analyze_glue_message, decode_glue_message, GlueSchemaRegistryFacade};
use crate::highlight::{write_avro_value, write_json_value, write_null, Highlighting};
use std::fmt::Display;

#[derive(PartialEq, Debug)]
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
            if payload.is_empty() {
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
    } else if let Ok(message) = analyze_glue_message(payload) {
        let schema_id = message.schema_id;
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

pub fn format_payload<'a, 'b>(payload: &'a Payload, highlighting: &'b Highlighting) -> PayloadFormatting<'a, 'b> {
    PayloadFormatting { payload, highlighting }
}

pub(crate) struct PayloadFormatting<'a, 'b> {
    payload: &'a Payload,
    highlighting: &'b Highlighting,
}

impl<'a, 'b> Display for PayloadFormatting<'a, 'b> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.payload {
            Payload::Null => write_null(f, self.highlighting),
            Payload::String(string) => write!(f, "{string}"),
            Payload::Json(value) => write_json_value(f, value, self.highlighting),
            Payload::Avro(value) => write_avro_value(f, value, self.highlighting),
            Payload::Unknown(string) => write!(f, "{style}{string}{style:#}", style = self.highlighting.dimmed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feedback::Feedback;
    use aws_credential_types::provider::SharedCredentialsProvider;
    use aws_credential_types::Credentials;
    use aws_types::region::Region;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_parsing() {
        let h = Highlighting::plain();
        let f = Feedback::prepare(&h, true);
        let credentials = Credentials::for_tests();
        let credentials_provider = SharedCredentialsProvider::new(credentials);
        let facade = GlueSchemaRegistryFacade::new(credentials_provider, Region::from_static("eu-central-1"), &f);

        assert_eq!(parse_payload(None, &facade).await, Payload::Null);
        assert_eq!(parse_payload(Some(&[]), &facade).await, Payload::String("".to_string()));
        assert_eq!(parse_payload(Some("string".as_bytes()), &facade).await, Payload::String("string".to_string()));
        assert_eq!(parse_payload(Some("\"string\"".as_bytes()), &facade).await, Payload::Json(serde_json::Value::String("string".into())));

        let some_bytes = Uuid::new_v4().as_bytes().to_vec();
        assert_eq!(parse_payload(Some(&some_bytes), &facade).await, Payload::Unknown(String::from_utf8_lossy(&some_bytes).to_string()));
    }

    #[test]
    fn test_formatting() {
        let h = Highlighting::plain();

        assert_eq!(format!("{}", format_payload(&Payload::Null, &h)), "null");
        assert_eq!(format!("{}", format_payload(&Payload::String("string".into()), &h)), "string");
        assert_eq!(format!("{}", format_payload(&Payload::Json(serde_json::Value::String("string".into())), &h)), "\"string\"");
        assert_eq!(format!("{}", format_payload(&Payload::Avro(avro_rs::types::Value::String("string".into())), &h)), "\"string\"");
        assert_eq!(format!("{}", format_payload(&Payload::Unknown("string".into()), &h)), "string");
    }
}

