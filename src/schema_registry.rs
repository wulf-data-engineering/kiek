use crate::args::Password;
use crate::feedback::Feedback;
use crate::Result;
use apache_avro::Schema;
use log::{debug, info, trace};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;

///
/// Try to analyze an AVRO encoded message from a Confluent-compatible Schema Registry.
/// If this is successful, the message is most likely a Schema Registry message that can be decoded
/// with the corresponding schema.
///
/// An AVRO encoded message for Confluent's Schema Registry has a header of 5 bytes:
/// * 1 byte: "Magic Byte" (0)
/// * 4 bytes: schema id (integer)
///
pub fn analyze_schema_registry_message(data: &[u8]) -> Result<SchemaRegistryMessage> {
    let magic_byte = data[0];

    if magic_byte != 0 {
        return Err("Invalid magic byte".into());
    }

    if data.len() < 5 {
        return Err(
            "Message too short: expected header of 5 bytes: magic byte and schema id".into(),
        );
    }

    let schema_id_bytes = &data[1..5];
    let schema_id = i32::from_be_bytes(schema_id_bytes.try_into().unwrap());

    if schema_id <= 0 {
        return Err(format!("Invalid schema id {schema_id}").into());
    }

    let payload = &data[5..];

    Ok(SchemaRegistryMessage { schema_id, payload })
}

#[derive(Debug)]
pub struct SchemaRegistryMessage<'a> {
    pub schema_id: i32,
    pub payload: &'a [u8],
}

///
/// Tries to read an already analyzed AVRO encoded message from Confluent's Schema Registry.
///
pub async fn decode_schema_registry_message<'a>(
    message: SchemaRegistryMessage<'a>,
    schema_registry_facade: &SchemaRegistryFacade,
) -> Result<apache_avro::types::Value> {
    let schema = schema_registry_facade.get_schema(message.schema_id).await?;
    let value =
        apache_avro::from_avro_datum(&schema, &mut std::io::Cursor::new(message.payload), None)?;
    Ok(value)
}

///
/// A facade for the Confluent Schema Registry that caches schema definitions locally
///
pub struct SchemaRegistryFacade {
    url: String,
    credentials: Option<(String, Password)>,
    client: Client,
    cache: Mutex<std::collections::HashMap<i32, Schema>>,
    feedback: Feedback,
}

#[derive(Deserialize)]
struct SchemaRegistryResponse {
    schema: String,
}

impl SchemaRegistryFacade {
    pub fn new(url: String, credentials: Option<(String, Password)>, feedback: &Feedback) -> Self {
        let feedback = feedback.clone();

        Self {
            url,
            credentials,
            client: Client::builder().build().unwrap(),
            cache: Mutex::new(std::collections::HashMap::new()),
            feedback,
        }
    }

    pub async fn get_schema(&self, schema_id: i32) -> Result<Schema> {
        let mut cache = self.cache.lock().await;

        if let Some(schema) = cache.get(&schema_id) {
            trace!("Schema cache hit for version {}", schema_id);
            return Ok(schema.clone());
        }

        self.feedback
            .info("Loading", format!("schema version {}", schema_id));

        info!("Loading schema version {}.", schema_id);

        let mut request = self
            .client
            .get(format!("{url}/schemas/ids/{schema_id}", url = self.url));

        if let Some((username, password)) = &self.credentials {
            debug!(
                "Using basic auth as {username}:{password} for {}.",
                self.url
            );
            request = request.basic_auth(username, Some(password.plain()));
        }

        let response = request.send().await?.error_for_status()?;
        let response = response.text().await?;
        let response = serde_json::from_str::<SchemaRegistryResponse>(&response)?;

        info!("Parsing schema version {schema_id}:\n{}", response.schema);

        let schema = Schema::parse_str(&response.schema)?;

        cache.insert(schema_id, schema.clone());

        info!("Cached schema version {}.", schema_id);

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analysis() {
        assert!(analyze_schema_registry_message("plain text".as_bytes()).is_err());
        assert!(analyze_schema_registry_message("{\"json\":42}".as_bytes()).is_err());
        assert!(analyze_schema_registry_message(&[42, 0, 0, 0])
            .err()
            .unwrap()
            .to_string()
            .contains("Invalid magic byte"));

        assert!(analyze_schema_registry_message(&[0, 0, 2, 4])
            .err()
            .unwrap()
            .to_string()
            .contains("Message too short"));

        let mut data = vec![0, 0, 0, 1, 42];

        let message = analyze_schema_registry_message(data.as_slice()).unwrap();
        assert_eq!(message.schema_id, 256 + 42);
        assert!(message.payload.is_empty());

        let some_payload: [u8; 4] = [1, 2, 3, 4];

        data.extend(some_payload);

        let message = analyze_schema_registry_message(data.as_slice()).unwrap();
        assert_eq!(message.schema_id, 256 + 42);
        assert_eq!(message.payload, some_payload);
    }

    #[test]
    fn test_decode() {
        let schema =
            r#"{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}"#;
        let schema = Schema::parse_str(schema).unwrap();
        let fixture = "AAAAAAEOdmFsdWU0Mg==";
        let data = openssl::base64::decode_block(fixture).unwrap();

        let message = analyze_schema_registry_message(&data).unwrap();

        let value =
            apache_avro::from_avro_datum(&schema, &mut std::io::Cursor::new(message.payload), None)
                .unwrap();

        match value {
            apache_avro::types::Value::Record(record) => {
                let (f1, value42) = record.first().unwrap();
                assert_eq!(f1, "f1");
                assert_eq!(
                    value42,
                    &apache_avro::types::Value::String("value42".to_string())
                );
            }
            _ => panic!("Unexpected value: {:?}", value),
        }
    }
}
