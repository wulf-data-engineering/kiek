use std::io::{IsTerminal, Write};
use std::sync::{Arc, Mutex};
use log::{info, warn};
use crate::highlight::Highlighting;

///
/// Simple feedback mechanism to provide information and warnings to the user in interactive mode
/// (running not silent in a terminal).
///
impl Feedback {
    pub(crate) fn prepare(highlighting: &Highlighting, silent: bool) -> Feedback {
        let interactive = !silent && std::io::stdout().is_terminal();
        Feedback { highlighting: Arc::new(highlighting.clone()), interactive, last: Arc::new(Mutex::new(0)) }
    }

    ///
    /// Print an info message with an emphasized header.
    /// Will be overwritten by the next output in interactive mode.
    ///
    pub(crate) fn info<S: Into<String>>(&self, header: &str, message: S) {
        let message = message.into();
        info!("{header}: {message}");
        if self.interactive {
            self.clear();
            for _ in 0..MIN_INFO_HEADER_LENGTH.saturating_sub(header.len()) {
                print!(" ");
            }
            let message = format!("{style}{header}{style:#} {}", message, style = self.highlighting.success);
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
        if self.interactive {
            self.clear();
            println!("{style}warning{style:#}: {}", message.into(), style = self.highlighting.warning);
        }
    }

    ///
    /// Clear the last output if interactive mode is enabled.
    ///
    pub(crate) fn clear(&self) {
        if self.interactive {
            let last = *self.last.lock().unwrap();
            if last > 0 {
                print!("\r");
                for _ in 0..last {
                    print!(" ");
                }
                print!("\r");
                if std::io::stdout().flush().is_err() {
                    warn!("Could not flush stdout.");
                }
                *self.last.lock().unwrap() = 0;
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct Feedback {
    pub(crate) highlighting: Arc<Highlighting>,
    pub(crate) interactive: bool,
    last: Arc<Mutex<usize>>,
}

const MIN_INFO_HEADER_LENGTH: usize = 12;