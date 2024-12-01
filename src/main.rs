mod app;
mod args;
mod aws;
mod context;
mod exception;
mod feedback;
mod glue;
mod highlight;
mod kafka;
mod msk_iam_context;
mod payload;

use std::error::Error;

pub(crate) type Result<T> = core::result::Result<T, Box<dyn Error + Send + Sync>>;
pub(crate) type CoreResult<T> = core::result::Result<T, Box<dyn Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    app::start().await?;
    Ok(())
}
