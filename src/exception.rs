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

impl KiekException {
    pub fn new<S: Into<String>>(message: S) -> Box<Self> {
        Box::new(Self { message: message.into(), delayed: Arc::new(Mutex::new(None)) })
    }

    pub fn delayed<S: Into<String>>(message: S, delayed: &Arc<Mutex<Option<String>>>) -> Box<Self> {
        Box::new(Self { message: message.into(), delayed: delayed.clone() })
    }

    pub fn from<E: Error>(e: E) -> Box<Self> {
        Self::new(e.to_string())
    }

    fn user_friendly(fail: &str) -> String {
        if fail.contains("SASL authentication error") && fail.contains("Access denied") {
            "SASL authentication error: Access denied.".to_string()
        } else {
            fail.to_string()
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
            Err(_) => write!(f, "{}", self.message)
        }
    }
}

impl Error for KiekException {}