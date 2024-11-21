mod app;
mod aws;
mod kafka;
mod payload;
mod highlight;
mod glue;

use std::error::Error;

pub(crate) type Result<T> = core::result::Result<T, Box<dyn Error + Send + Sync>>;
pub(crate) type CoreResult<T> = core::result::Result<T, Box<dyn Error>>;

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct KiekException {
    pub message: String,
}

impl KiekException {
    pub fn boxed(message: String) -> Box<dyn Error + Send + Sync> {
        Box::new(Self { message })
    }
}

impl std::fmt::Display for KiekException {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for KiekException {}

const NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    match app::run().await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    Ok(())
}
