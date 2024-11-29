use lazy_static::lazy_static;
use regex::Regex;
use std::error::Error;
use std::sync::{Arc, Mutex};

///
/// `KiekException` is a custom error type that can be used to wrap any error message
///
/// In some occasions, the Kafka library exposes just a broker transport failure without the
/// details but logs them after raising an error.
/// Then the exception is constructed with a pointer to the last failure message of the Kafka
/// consumer. When the exception is printed, the last failure message is analyzed.
///
#[derive(Clone, Debug)]
pub(crate) struct KiekException {
    message: String,
    delayed: Arc<Mutex<Option<String>>>,
}

lazy_static! {
    static ref KAFKA_ERROR_REGEX: Regex =
        Regex::new(r"^\[[^\]]+\]: [^ ]+: (.+) \(after .+\)$").unwrap();
}

impl KiekException {
    pub fn new<S: Into<String>>(message: S) -> Box<Self> {
        Box::new(Self {
            message: message.into(),
            delayed: Arc::new(Mutex::new(None)),
        })
    }

    pub fn delayed<S: Into<String>>(message: S, delayed: &Arc<Mutex<Option<String>>>) -> Box<Self> {
        Box::new(Self {
            message: message.into(),
            delayed: delayed.clone(),
        })
    }

    pub fn from<E: Error>(e: E) -> Box<Self> {
        Self::new(e.to_string())
    }

    fn user_friendly(fail: &str) -> String {
        // Try to decompose the error message to the actual message
        let fail = KAFKA_ERROR_REGEX
            .captures(fail)
            .map_or_else(|| fail.to_string(), |c| c[1].to_string());
        if fail.contains("SASL authentication error") && fail.contains("Access denied") {
            "SASL authentication error: Access denied.".to_string()
        } else if fail.contains("Disconnected while requesting ApiVersion") {
            "Disconnected while requesting API version: Most likely the authentication mechanism is wrong and SSL is expected. Verify your -a, --authentication configuration.".to_string()
        } else if fail.contains(
            "Unsupported SASL mechanism: broker's supported mechanisms: OAUTHBEARER,AWS_MSK_IAM",
        ) {
            "The broker requires MSK IAM based authentication. Specify using --authentication=msk-iam.".to_string()
        } else {
            fail
        }
    }
}

impl std::fmt::Display for KiekException {
    ///
    /// If the last failure message is set, it is printed instead of the original message.
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.delayed.lock() {
            Ok(fail) => {
                if let Some(fail) = fail.as_ref() {
                    write!(f, "{}", Self::user_friendly(fail))
                } else {
                    write!(f, "{}", self.message)
                }
            }
            Err(_) => write!(f, "{}", self.message),
        }
    }
}

impl Error for KiekException {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_friendly_error_messages() {
        assert_eq!(
            KiekException::user_friendly("FAIL [thrd:sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-ce]: sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: SASL authentication error: [4a380ffc-1778-418a-9bb5-264f49c29bcb]: Access denied (after 395ms in state AUTH_REQ)://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: SASL PLAIN mechanism handshake failed: Broker: Unsupported SASL mechanism: broker's supported mechanisms: OAUTHBEARER,AWS_MSK_IAM (after 379ms in state AUTH_HANDSHAKE)"),
            "SASL authentication error: Access denied.");

        assert_eq!(
            KiekException::user_friendly("FAIL [thrd:sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-ce]: sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: SASL PLAIN mechanism handshake failed: Broker: Unsupported SASL mechanism: broker's supported mechanisms: OAUTHBEARER,AWS_MSK_IAM (after 379ms in state AUTH_HANDSHAKE)"),
            "The broker requires MSK IAM based authentication. Specify using --authentication=msk-iam.");

        assert_eq!(
            KiekException::user_friendly("FAIL [thrd:b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.ama]: b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: Disconnected while requesting ApiVersion: might be caused by incorrect security.protocol configuration (connecting to a SSL listener?) or broker version is < 0.10 (see api.version.request) (after 18ms in state APIVERSION_QUERY, 1 identical error(s) suppressed)"),
            "Disconnected while requesting API version: Most likely the authentication mechanism is wrong and SSL is expected. Verify your -a, --authentication configuration.");

        assert_eq!(
            KiekException::user_friendly("[thrd:sasl_ssl://127.0.0.1:9092/bootstrap]: sasl_ssl://127.0.0.1:9092/bootstrap: SSL handshake failed: Disconnected: connecting to a PLAINTEXT broker listener? (after 2ms in state SSL_HANDSHAKE)"),
            "SSL handshake failed: Disconnected: connecting to a PLAINTEXT broker listener?");

        assert_eq!(
            KiekException::user_friendly("[thrd:sasl_ssl://127.0.0.1:9092/bootstrap]: sasl_ssl://127.0.0.1:9092/bootstrap: kapott (after 2ms in state SSL_HANDSHAKE)"),
            "kapott");
    }

    #[test]
    fn test_write() {
        assert_eq!(format!("{}", KiekException::new("foo")), "foo");
        assert_eq!(
            format!(
                "{}",
                KiekException::delayed("foo", &Arc::new(Mutex::new(None)))
            ),
            "foo"
        );
        assert_eq!(
            format!(
                "{}",
                KiekException::delayed("foo", &Arc::new(Mutex::new(Some("bar".to_string()))))
            ),
            "bar"
        );
        assert_eq!(format!("{}", KiekException::delayed("foo", &Arc::new(Mutex::new(Some("[thrd:sasl_ssl://127.0.0.1:9092/bootstrap]: sasl_ssl://127.0.0.1:9092/bootstrap: baz (after 2ms in state SSL_HANDSHAKE)".to_string()))))), "baz");
    }
}
