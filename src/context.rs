use crate::highlight::Highlighting;
use crate::Result;
use log::{debug, error, info, warn};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use std::sync::{Arc, Mutex};

pub(crate) trait KiekContext: ConsumerContext {
    ///
    /// Performs context specific verification that should be checked before starting the consumer.
    ///
    async fn verify(&self, highlighting: &Highlighting) -> Result<()>;

    ///
    /// Points to the last captured failure message, if any.
    ///
    fn last_fail(&self) -> Arc<Mutex<Option<String>>>;

    ///
    /// Passes a Kafka log message to the logger, and captures the last failure message.
    ///
    /// In most occasions the Kafka library exposes just a broker transport failure without the
    /// details but is more verbose in the log. The last FAIL log message is captured for more
    /// user-friendly feedback.
    ///
    fn capturing_log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if fac == "FAIL" {
            match self.last_fail().lock() {
                Ok(mut last_fail) => {
                    *last_fail = Some(log_message.to_string());
                }
                Err(_) => {
                    // ignore, things went south anyway
                }
            }
        }

        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!("{} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                warn!("{} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice
            | RDKafkaLogLevel::Info => {
                info!("{} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                debug!("{} {}", fac, log_message)
            }
        }
    }
}
