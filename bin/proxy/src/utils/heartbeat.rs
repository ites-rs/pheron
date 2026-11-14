use crate::models::SessionManager;
use tokio::time::Duration;

/// checks every 30s,
/// evicts idle implants after 2m silence
pub fn start_heartbeat(sessions: SessionManager, is_last_proxy: bool) {
    let hb = sessions.clone();
    tokio::spawn(async move {
        hb.run_heartbeat(
            Duration::from_secs(10),
            Duration::from_secs(120),
            is_last_proxy,
        )
        .await;
    });
}