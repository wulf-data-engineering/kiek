use crate::error::KiekError;
use crate::glue::{analyze_glue_message, decode_glue_message, GlueSchemaRegistryFacade};
use crate::highlight::{write_avro_value, write_json_value, write_null, Highlighting};
use crate::schema_registry::{
    analyze_schema_registry_message, decode_schema_registry_message, SchemaRegistryFacade,
};
use crate::Result;
use std::fmt::Display;

#[derive(PartialEq, Debug)]
pub enum Payload {
    Null,
    String(String),
    Json(serde_json::Value),
    Avro(apache_avro::types::Value),
    Unknown(String),
}

pub async fn parse_payload(
    payload: Option<&[u8]>,
    glue_schema_registry_facade: &GlueSchemaRegistryFacade,
    schema_registry_facade: Option<&SchemaRegistryFacade>,
    highlighting: &Highlighting,
) -> Result<Payload> {
    match payload {
        None => Ok(Payload::Null),
        Some(payload) => {
            if payload.is_empty() {
                Ok(Payload::String("".to_string()))
            } else {
                parse_payload_bytes(
                    payload,
                    glue_schema_registry_facade,
                    schema_registry_facade,
                    highlighting,
                )
                .await
            }
        }
    }
}

async fn parse_payload_bytes(
    payload: &[u8],
    glue_schema_registry_facade: &GlueSchemaRegistryFacade,
    schema_registry_facade: Option<&SchemaRegistryFacade>,
    highlighting: &Highlighting,
) -> Result<Payload> {
    if let Ok(message) = analyze_schema_registry_message(payload) {
        let schema_registry_facade = schema_registry_facade.ok_or(
            KiekError::new(format!("Received an AVRO encoded message without Schema Registry. Use {bold}--schema-registry-url{bold:#} to configure.", bold = highlighting.bold)))?;
        let value = decode_schema_registry_message(message, schema_registry_facade).await?;
        Ok(Payload::Avro(value))
    } else if let Ok(message) = analyze_glue_message(payload) {
        let value = decode_glue_message(message, glue_schema_registry_facade).await?;
        Ok(Payload::Avro(value))
    } else if let Ok(string) = std::str::from_utf8(payload) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(string) {
            Ok(Payload::Json(value))
        } else {
            Ok(Payload::String(string.to_string()))
        }
    } else {
        Ok(Payload::Unknown(
            String::from_utf8_lossy(payload).to_string(),
        ))
    }
}

pub fn format_payload<'a>(
    payload: &'a Payload,
    indent: bool,
    highlighting: &'static Highlighting,
) -> PayloadFormatting<'a> {
    PayloadFormatting {
        payload,
        indent,
        highlighting,
    }
}

pub(crate) struct PayloadFormatting<'a> {
    payload: &'a Payload,
    indent: bool,
    highlighting: &'static Highlighting,
}

impl Display for PayloadFormatting<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.payload {
            Payload::Null => write_null(f, self.highlighting),
            Payload::String(string) => write!(f, "{string}"),
            Payload::Json(value) => write_json_value(f, value, self.indent, self.highlighting),
            Payload::Avro(value) => write_avro_value(f, value, self.indent, self.highlighting),
            Payload::Unknown(string) => write!(
                f,
                "{style}{string}{style:#}",
                style = self.highlighting.dimmed
            ),
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
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_parsing() {
        let h = Highlighting::plain();
        let f = Feedback::prepare(h, true);
        let credentials = Credentials::for_tests();
        let credentials_provider = SharedCredentialsProvider::new(credentials);
        let glue_facade = GlueSchemaRegistryFacade::new(
            credentials_provider,
            Region::from_static("eu-central-1"),
            &f,
        );

        assert_eq!(
            parse_payload(None, &glue_facade, None, h).await.unwrap(),
            Payload::Null
        );
        assert_eq!(
            parse_payload(Some(&[]), &glue_facade, None, h)
                .await
                .unwrap(),
            Payload::String("".to_string())
        );
        assert_eq!(
            parse_payload(Some("string".as_bytes()), &glue_facade, None, h)
                .await
                .unwrap(),
            Payload::String("string".to_string())
        );
        assert_eq!(
            parse_payload(Some("\"string\"".as_bytes()), &glue_facade, None, h)
                .await
                .unwrap(),
            Payload::Json(serde_json::Value::String("string".into()))
        );

        let some_bytes = Uuid::new_v4().as_bytes().to_vec();
        assert_eq!(
            parse_payload(Some(&some_bytes), &glue_facade, None, h)
                .await
                .unwrap(),
            Payload::Unknown(String::from_utf8_lossy(&some_bytes).to_string())
        );
    }

    #[tokio::test]
    async fn test_glue_decoding() {
        let h = Highlighting::plain();
        let f = Feedback::prepare(h, true);
        let credentials = Credentials::for_tests();
        let credentials_provider = SharedCredentialsProvider::new(credentials);
        let glue_facade = GlueSchemaRegistryFacade::new(
            credentials_provider,
            Region::from_static("eu-central-1"),
            &f,
        );

        let schema = apache_avro::Schema::parse_str(
            r#"
            {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "field1", "type": "string"},
                    {"name": "field2", "type": "int"}
                ]
            }
            "#,
        )
        .unwrap();

        let uuid = Uuid::new_v4();
        glue_facade.register_schema(&uuid, &schema).await;

        let value: apache_avro::types::Value = apache_avro::types::Value::Record(vec![
            (
                "field1".to_string(),
                apache_avro::types::Value::String("field1".to_string()),
            ),
            ("field2".to_string(), apache_avro::types::Value::Int(42)),
        ]);

        let encoded = apache_avro::to_avro_datum(&schema, value.clone()).unwrap();

        let buffer = &mut Vec::new();
        let mut encoder = ZlibEncoder::new(buffer, flate2::Compression::default());
        encoder.write_all(encoded.as_slice()).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut payload: Vec<u8> = Vec::new();
        payload.push(3); // magic byte
        payload.push(5); // zlib compression
        payload.extend_from_slice(uuid.as_bytes().as_slice());
        payload.append(compressed);

        let parsed_payload = parse_payload(Some(payload.as_slice()), &glue_facade, None, h)
            .await
            .unwrap();

        assert_eq!(parsed_payload, Payload::Avro(value));
    }

    #[test]
    fn test_formatting() {
        let h = Highlighting::plain();

        assert_eq!(
            format!("{}", format_payload(&Payload::Null, false, h)),
            "null"
        );
        assert_eq!(
            format!(
                "{}",
                format_payload(&Payload::String("string".into()), false, h)
            ),
            "string"
        );
        assert_eq!(
            format!(
                "{}",
                format_payload(
                    &Payload::Json(serde_json::Value::String("string".into())),
                    false,
                    h
                )
            ),
            "\"string\""
        );
        assert_eq!(
            format!(
                "{}",
                format_payload(
                    &Payload::Avro(apache_avro::types::Value::String("string".into())),
                    false,
                    h
                )
            ),
            "\"string\""
        );
        assert_eq!(
            format!(
                "{}",
                format_payload(&Payload::Unknown("string".into()), false, h)
            ),
            "string"
        );
    }
}
