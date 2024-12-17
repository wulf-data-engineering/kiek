use std::error::Error;
use std::io;
use std::io::Write;
#[tokio::main]
async fn main() -> kiek::Result<()> {
    kiek::start().await?;
    Ok(())
}
