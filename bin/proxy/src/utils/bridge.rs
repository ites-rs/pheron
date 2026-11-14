use anyhow::{Context, Result};
use tokio::io::copy_bidirectional;
use tokio::net::TcpStream;
use tracing::info;

pub async fn bridge_streams(
    mut incoming_stream: TcpStream,
    mut outgoing_stream: TcpStream,
) -> Result<()> {
    let (to_outgoing, to_incoming) = copy_bidirectional(&mut incoming_stream, &mut outgoing_stream)
        .await
        .context("failed to bridge streams")?;

    info!("bridge closed: {to_outgoing} bytes sent, {to_incoming} bytes received");
    Ok(())
}
