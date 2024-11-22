use std::io::Write;
use std::sync::{Arc, Mutex};
use log::warn;
use crate::highlight::Highlighting;

///
/// Simple feedback mechanism to provide information and warnings to the user unless silent mode is enabled
///
impl Feedback {

    pub(crate) fn prepare(highlighting: &Highlighting, silent: bool) -> Feedback {
        Feedback { highlighting: Arc::new(highlighting.clone()), silent, last: Arc::new(Mutex::new(0)) }
    }

    ///
    /// Print an info message with an emphasized header.
    /// Will be overwritten by the next output.
    ///
    pub(crate) fn info<S: Into<String>>(&self, header: &str, message: S) {
        if !self.silent {
            self.clear();
            for _ in 0..MIN_INFO_HEADER_LENGTH.saturating_sub(header.len()) {
                print!(" ");
            }
            let message = format!("{style}{header}{style:#} {}", message.into(), style = self.highlighting.success);
            print!("{message}\r");
            if std::io::stdout().flush().is_err() {
                warn!("Could not flush stdout.");
            }
            *self.last.lock().unwrap() = message.len();
        }
    }

    ///
    /// Print a warning message that is not overwritten by the next output.
    ///
    pub(crate) fn warning<S: Into<String>>(&self, message: S) {
        if !self.silent {
            self.clear();
            println!("{style}warning{style:#}: {}", message.into(), style = self.highlighting.warning);
        }
    }

    ///
    /// Clear the last output
    ///
    pub(crate) fn clear(&self) {
        let last = *self.last.lock().unwrap();
        if !self.silent && last > 0 {
            print!("\r");
            for _ in 0..last {
                print!(" ");
            }
            print!("\r");
            *self.last.lock().unwrap() = 0;
        }
    }
}

#[derive(Clone)]
pub(crate) struct Feedback {
    highlighting: Arc<Highlighting>,
    silent: bool,
    last: Arc<Mutex<usize>>,
}

const MIN_INFO_HEADER_LENGTH: usize = 12;