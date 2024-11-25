use std::cmp::max;
use std::io::{IsTerminal, Write};
use std::sync::Arc;
use log::{info, warn};
use termion::clear;
use crate::highlight::Highlighting;

///
/// Simple feedback mechanism to provide information and warnings to the user in interactive mode
/// (running not silent in a terminal).
///
impl Feedback {
    pub(crate) fn prepare(highlighting: &Highlighting, silent: bool) -> Feedback {
        let interactive = !silent && std::io::stdout().is_terminal();
        Feedback { highlighting: Arc::new(highlighting.clone()), interactive }
    }

    ///
    /// Print an info message with an emphasized header.
    /// Will be overwritten by the next output in interactive mode.
    ///
    pub(crate) fn info<S: Into<String>>(&self, header: &str, message: S) {
        let message = message.into();
        info!("{header}: {message}");
        if self.interactive {
            let header_length = max(MIN_INFO_HEADER_LENGTH, header.len()) + 1;
            let text_length = header_length + message.len();

            // truncate message if it is too long for the terminal
            // otherwise the overwriting of the next output will not work correctly
            let message =
                match termion::terminal_size() {
                    Ok((width, _)) if text_length > width as usize => {
                        let mut message = message;
                        message.truncate(width as usize - header_length - 3);
                        format!("{}...", message)
                    }
                    _ => message,
                };

            print!("{}{style}{header: >MIN_INFO_HEADER_LENGTH$}{style:#} {message}\r", clear::CurrentLine, style = self.highlighting.success);
            if std::io::stdout().flush().is_err() {
                warn!("Could not flush stdout.");
            }
        }
    }

    ///
    /// Print a warning message that is not overwritten by the next output.
    ///
    pub(crate) fn warning<S: Into<String>>(&self, message: S) {
        if self.interactive {
            println!("{}{style}warning{style:#}: {}", clear::CurrentLine, message.into(), style = self.highlighting.warning);
        }
    }

    ///
    /// Clear the last output if interactive mode is enabled.
    ///
    pub(crate) fn clear(&self) {
        if self.interactive {
            print!("{}", clear::CurrentLine);
            std::io::stdout().flush().unwrap();
        }
    }
}

#[derive(Clone)]
pub(crate) struct Feedback {
    pub(crate) highlighting: Arc<Highlighting>,
    pub(crate) interactive: bool,
}

const MIN_INFO_HEADER_LENGTH: usize = 12;