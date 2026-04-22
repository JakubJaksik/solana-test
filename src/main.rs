use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("tx-cutoff v{}", env!("CARGO_PKG_VERSION"));
    Ok(())
}
