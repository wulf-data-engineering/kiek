use crate::highlight::Highlighting;
use crate::Result;
use log::{debug, error, info, warn};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
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
                error!("{fac} {log_message}")
            }
            RDKafkaLogLevel::Warning => {
                warn!("{fac} {log_message}")
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                info!("{fac} {log_message}")
            }
            RDKafkaLogLevel::Debug => {
                debug!("{fac} {log_message}")
            }
        }
    }
}

///
/// Simple context to capture the last failure message from the Kafka consumer's log
///
#[derive(Clone)]
pub(crate) struct DefaultKiekContext {
    last_fail: Arc<Mutex<Option<String>>>,
}

impl DefaultKiekContext {
    pub(crate) fn new() -> Self {
        Self {
            last_fail: Arc::new(Mutex::new(None)),
        }
    }
}

impl KiekContext for DefaultKiekContext {
    async fn verify(&self, _: &Highlighting) -> Result<()> {
        Ok(())
    }

    fn last_fail(&self) -> Arc<Mutex<Option<String>>> {
        self.last_fail.clone()
    }
}

impl ConsumerContext for DefaultKiekContext {}

impl ClientContext for DefaultKiekContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.capturing_log(level, fac, log_message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capturing_log() {
        let ctx = DefaultKiekContext::new();

        assert_eq!(ctx.last_fail().lock().unwrap().clone(), None);

        ctx.capturing_log(RDKafkaLogLevel::Info, "TEST", "This is a test message");
        ctx.capturing_log(
            RDKafkaLogLevel::Warning,
            "TEST",
            "This is a warning message",
        );
        ctx.capturing_log(RDKafkaLogLevel::Error, "TEST", "This is an error message");
        ctx.capturing_log(RDKafkaLogLevel::Debug, "TEST", "This is a debug message");
        ctx.capturing_log(RDKafkaLogLevel::Notice, "TEST", "This is a notice message");

        assert_eq!(ctx.last_fail().lock().unwrap().clone(), None);

        ctx.capturing_log(RDKafkaLogLevel::Error, "FAIL", "broken");

        assert_eq!(
            ctx.last_fail().lock().unwrap().clone(),
            Some("broken".to_string())
        );

        ctx.capturing_log(RDKafkaLogLevel::Notice, "FAIL", "still broken");

        assert_eq!(
            ctx.last_fail().lock().unwrap().clone(),
            Some("still broken".to_string())
        );

        ctx.capturing_log(RDKafkaLogLevel::Info, "TEST", "This is a test message");
        ctx.capturing_log(
            RDKafkaLogLevel::Warning,
            "TEST",
            "This is a warning message",
        );
        ctx.capturing_log(RDKafkaLogLevel::Error, "TEST", "This is an error message");
        ctx.capturing_log(RDKafkaLogLevel::Debug, "TEST", "This is a debug message");
        ctx.capturing_log(RDKafkaLogLevel::Notice, "TEST", "This is a notice message");

        assert_eq!(
            ctx.last_fail().lock().unwrap().clone(),
            Some("still broken".to_string())
        );
    }
}
