use anyhow::{Error, Result};
use proxy::start_listener;

#[tokio::main]
async fn main() -> Result<(), Error> {
    start_listener().await
}
