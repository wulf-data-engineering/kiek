#[tokio::main]
async fn main() -> kiek::Result<()> {
    kiek::check_completions();
    kiek::start().await?;
    Ok(())
}
