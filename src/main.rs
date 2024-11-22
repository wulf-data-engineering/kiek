mod app;
mod aws;
mod kafka;
mod payload;
mod highlight;
mod glue;
mod feedback;

use std::error::Error;

pub(crate) type Result<T> = core::result::Result<T, Box<dyn Error + Send + Sync>>;
pub(crate) type CoreResult<T> = core::result::Result<T, Box<dyn Error>>;

const NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    app::run().await?;
    Ok(())
}