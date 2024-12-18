use lazy_static::lazy_static;
use regex::Regex;
use std::error::Error;

///
/// `KiekError` is a custom error type that can be used to wrap any error message.
///
/// It turns Kafka error messages into user-friendly messages.
///
#[derive(Clone, Debug)]
pub(crate) struct KiekError {
    message: String,
}

lazy_static! {
    static ref KAFKA_ERROR_REGEX: Regex =
        Regex::new(r"^\[[^\]]+\]: [^ ]+: (.+) \(after .+\)$").unwrap();
}

impl KiekError {
    pub const BROKER_TRANSPORT_FAILURE: &'static str = "Connection to broker is interrupted";

    pub fn new<S: Into<String>>(message: S) -> Box<Self> {
        Box::new(Self {
            message: Self::user_friendly(message.into()),
        })
    }

    pub fn from<E: Error>(e: E) -> Box<Self> {
        Self::new(e.to_string())
    }

    fn user_friendly(fail: String) -> String {
        // Try to decompose the error message to the actual message
        let fail = KAFKA_ERROR_REGEX
            .captures(&fail)
            .map_or_else(|| fail.clone(), |c| c[1].to_string());
        if fail.contains("SASL authentication error") && fail.contains("Access denied") {
            "SASL authentication error: Access denied.".to_string()
        } else if fail.contains("Disconnected while requesting ApiVersion") {
            "Disconnected while requesting API version: Most likely the authentication mechanism is wrong and SSL is expected. Verify your -a, --authentication configuration.".to_string()
        } else if fail.contains(
            "SSL handshake failed: Disconnected: connecting to a PLAINTEXT broker listener?",
        ) {
            "SSL handshake failed: If you connect to a PLAINTEXT broker listener use --no-ssl."
                .to_string()
        } else if fail.contains(
            "Unsupported SASL mechanism: broker's supported mechanisms: OAUTHBEARER,AWS_MSK_IAM",
        ) {
            "The broker requires MSK IAM based authentication. Specify using --authentication=msk-iam.".to_string()
        } else {
            fail
        }
    }
}

impl std::fmt::Display for KiekError {
    ///
    /// If the last failure message is set, it is printed instead of the original message.
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for KiekError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_friendly_error_messages() {
        assert_eq!(
            KiekError::new("Some error").message,
            "Some error");

        assert_eq!(
            KiekError::new("FAIL [thrd:sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-ce]: sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: SASL authentication error: [4a380ffc-1778-418a-9bb5-264f49c29bcb]: Access denied (after 395ms in state AUTH_REQ)://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: SASL PLAIN mechanism handshake failed: Broker: Unsupported SASL mechanism: broker's supported mechanisms: OAUTHBEARER,AWS_MSK_IAM (after 379ms in state AUTH_HANDSHAKE)").message,
            "SASL authentication error: Access denied.");

        assert_eq!(
            KiekError::new("FAIL [thrd:sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-ce]: sasl_ssl://b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: SASL PLAIN mechanism handshake failed: Broker: Unsupported SASL mechanism: broker's supported mechanisms: OAUTHBEARER,AWS_MSK_IAM (after 379ms in state AUTH_HANDSHAKE)").message,
            "The broker requires MSK IAM based authentication. Specify using --authentication=msk-iam.");

        assert_eq!(
            KiekError::new("FAIL [thrd:b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.ama]: b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198/bootstrap: Disconnected while requesting ApiVersion: might be caused by incorrect security.protocol configuration (connecting to a SSL listener?) or broker version is < 0.10 (see api.version.request) (after 18ms in state APIVERSION_QUERY, 1 identical error(s) suppressed)").message,
            "Disconnected while requesting API version: Most likely the authentication mechanism is wrong and SSL is expected. Verify your -a, --authentication configuration.");

        assert_eq!(
            KiekError::new("[thrd:sasl_ssl://127.0.0.1:9092/bootstrap]: sasl_ssl://127.0.0.1:9092/bootstrap: SSL handshake failed: Disconnected: connecting to a PLAINTEXT broker listener? (after 2ms in state SSL_HANDSHAKE)").message,
            "SSL handshake failed: If you connect to a PLAINTEXT broker listener use --no-ssl.");

        assert_eq!(
            KiekError::new("[thrd:sasl_ssl://127.0.0.1:9092/bootstrap]: sasl_ssl://127.0.0.1:9092/bootstrap: kapott (after 2ms in state SSL_HANDSHAKE)").message,
            "kapott");
    }

    #[test]
    fn test_write() {
        assert_eq!(format!("{}", KiekError::new("foo")), "foo");
    }
}
