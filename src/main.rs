#[tokio::main]
async fn main() -> kiek::Result<()> {
    kiek::start().await?;
    Ok(())
}
