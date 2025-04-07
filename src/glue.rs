use crate::feedback::Feedback;
use crate::Result;
use apache_avro::{AvroResult, Schema};
use aws_sdk_glue::config::BehaviorVersion;
use aws_sdk_glue::Client;
use aws_sdk_sts::config::SharedCredentialsProvider;
use aws_types::region::Region;
use flate2::read::ZlibDecoder;
use log::{debug, info};
use std::io::Read;
use tokio::sync::Mutex;
use uuid::Uuid;

///
/// Try to analyze an AVRO encoded message from the Glue Schema Registry.
/// If this is successful, the message is most likely a Glue Schema Registry message that can be
/// decoded with the corresponding schema.
///
/// An AVRO encoded message for the Glue Schema Registry has a header of 18 bytes:
/// * 1 byte: Header version (3)
/// * 1 byte: Compression type (0 = None, 5 = Zlib)
/// * 16 bytes: schema id (UUID)
///
pub fn analyze_glue_message(data: &[u8]) -> Result<GlueMessage> {
    let header_version_byte = data[0];

    if header_version_byte != 3 {
        return Err("Invalid header version".into());
    }

    let compression_byte = data[1];

    if compression_byte != 0 && compression_byte != 5 {
        return Err("Invalid compression type".into());
    }

    let zlib_compressed = compression_byte == 5;

    if data.len() < 18 {
        return Err(
            "Message too short: expected header of 18 bytes: version, compression, UUID".into(),
        );
    }

    let schema_id = Uuid::from_slice(&data[2..18])?;

    let payload = &data[18..];

    Ok(GlueMessage {
        schema_id,
        zlib_compressed,
        payload,
    })
}

pub struct GlueMessage<'a> {
    pub schema_id: Uuid,
    pub zlib_compressed: bool,
    pub payload: &'a [u8],
}

///
/// Tries to read an already analyzed AVRO encoded message from the Glue Schema Registry.
///
pub async fn decode_glue_message<'a>(
    message: GlueMessage<'a>,
    glue_schema_registry_facade: &GlueSchemaRegistryFacade,
) -> Result<apache_avro::types::Value> {
    let schema = glue_schema_registry_facade
        .get_schema(&message.schema_id)
        .await?;

    let mut cursor = std::io::Cursor::new(message.payload);

    fn decode_value<R: Read>(
        schema: &Schema,
        read: &mut R,
    ) -> AvroResult<apache_avro::types::Value> {
        apache_avro::from_avro_datum(schema, read, None)
    }

    let value = if message.zlib_compressed {
        decode_value(&schema, &mut ZlibDecoder::new(cursor))?
    } else {
        decode_value(&schema, &mut cursor)?
    };

    Ok(value)
}

///
/// A facade for the Glue Schema Registry that caches schema definitions locally
///
pub struct GlueSchemaRegistryFacade {
    client: Client,
    cache: Mutex<std::collections::HashMap<Uuid, Schema>>,
    feedback: Feedback,
}

impl GlueSchemaRegistryFacade {
    pub fn new(
        credentials_provider: SharedCredentialsProvider,
        region: Region,
        feedback: &Feedback,
    ) -> Self {
        let feedback = feedback.clone();
        let client = Client::from_conf(
            aws_sdk_glue::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(region)
                .credentials_provider(credentials_provider)
                .build(),
        );

        Self {
            client,
            cache: Mutex::new(std::collections::HashMap::new()),
            feedback,
        }
    }

    pub async fn get_schema(&self, schema_id: &Uuid) -> Result<Schema> {
        let mut cache = self.cache.lock().await;

        if let Some(schema) = cache.get(schema_id) {
            debug!("Schema cache hit for version {}", schema_id);
            return Ok(schema.clone());
        }

        self.feedback
            .info("Loading", format!("schema version {}", schema_id));

        info!("Loading schema version {}.", schema_id);

        let schema_version_output = self
            .client
            .get_schema_version()
            .schema_version_id(schema_id.to_string())
            .send()
            .await?;

        let schema_definition = schema_version_output
            .schema_definition
            .ok_or("Schema definition not found")?;

        info!("Parsing schema version {schema_id}:\n{schema_definition}");

        let schema = Schema::parse_str(&schema_definition)?;

        cache.insert(*schema_id, schema.clone());

        info!("Cached schema version {}.", schema_id);

        Ok(schema)
    }

    #[cfg(test)]
    pub(crate) async fn register_schema(&self, schema_id: &Uuid, schema: &Schema) {
        let mut cache = self.cache.lock().await;
        cache.insert(*schema_id, schema.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::base64;

    #[test]
    fn test_analysis() {
        assert!(analyze_glue_message("plain text".as_bytes()).is_err());
        assert!(analyze_glue_message("{\"json\":42}".as_bytes()).is_err());
        assert!(analyze_glue_message(&[42, 0])
            .err()
            .unwrap()
            .to_string()
            .contains("Invalid header version"));
        assert!(analyze_glue_message(&[3, 1])
            .err()
            .unwrap()
            .to_string()
            .contains("Invalid compression type"));
        assert!(analyze_glue_message(&[3, 0, 2])
            .err()
            .unwrap()
            .to_string()
            .contains("Message too short"));

        let uuid = Uuid::new_v4();
        let mut data = vec![3, 0];
        data.extend(uuid.into_bytes());

        let glue_message = analyze_glue_message(data.as_slice()).unwrap();
        assert_eq!(glue_message.schema_id, uuid);
        assert!(glue_message.payload.is_empty());

        let some_payload: [u8; 4] = [1, 2, 3, 4];

        data.extend(some_payload);

        let glue_message = analyze_glue_message(data.as_slice()).unwrap();
        assert_eq!(glue_message.schema_id, uuid);
        assert_eq!(glue_message.payload, some_payload);

        let zlib_encoded = base64::decode_block("AwVbUsB4hvBEKbu/zLjzsIGneJytk0Fv2jAUx0PUE0IIVahC1Q6utAPSGuI4ISFohzaDDUQHFSAm9WaIS6IYm8XO6LbrPsOu+xJTjzus36OfZQ7rWKUh7dKT/f/5+b3/80teIoiaBkQGcqew1W7abeg3HOR5sGUh9+qwCR3Xhr7rln7v6v3htDueXI7GUzAbTd71h4Puhb6X1gZ3tyxeijXF8tOBaxVtx7K80gALQahe6hG1Mr2i3Y7uvfsbrXzy4/33z8eaXoNN1zIR9HzkWI9Vy9aPoN+2oYFgG0LtaZSm6bNk68lIONkZPouZJKlY81QaH7jYxCwhtBGS2mGAw5U65KwaYJEQOceUFgN+Q1ixg1MpKq+zu285rPQwC/PNsz6jMSPGJMEyZsvqmFMqFlEWkfI4Y0yh4/yIzDlOQ6XKU5wtIsKqMxVIPuYplMufhUjKtWib5mazafy1p0yZESYspArk98wtXhJVApNrSYxo+9Tm9v2Nh173tWfqXx5q/FshNIXkKRH/z1LQ6pOcgNkfAt6s5j19L33+CqeUG5NFtIpD41y1Sw4su+g4yLUrHRW/yli4+1ieIgcUJEmJxGmM5dnjyergheMDZFvA94HvAgj1r4W3OF1kAgxJRk/BGNNrcEGiVZJFSqrpXGO25CDgLIxPQR7LwSVXI5BS6YCnsQBXmHGaA71+vpJiSdJ4EUmw81XpjQNL/XFIP+p0bde1POT51i+62h9Z").unwrap();
        let glue_message = analyze_glue_message(zlib_encoded.as_slice()).unwrap();
        assert!(glue_message.zlib_compressed);
    }
}
