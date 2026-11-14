use tokio::net::TcpStream;

const SECRET_URI: &str = "/cdn/updates";
const C2_HEADER: &str = "X-C2-Register: true";
const PEEK_SIZE: usize = 512;

#[derive(Debug)]
pub enum ConnectionKind {
    C2,
    Implant,
    Unknown,
}

pub async fn peek(stream: &TcpStream, is_last_proxy: bool) -> ConnectionKind {
    let mut buf = [0u8; PEEK_SIZE];

    let n = match stream.peek(&mut buf).await {
        Ok(n) => n,
        Err(_) => return ConnectionKind::Unknown,
    };

    let preview = String::from_utf8_lossy(&buf[..n]);

    if preview.contains(C2_HEADER) && is_last_proxy {
        ConnectionKind::C2
    } else if preview.contains("Upgrade: websocket") && preview.contains(SECRET_URI) {
        ConnectionKind::Implant
    } else {
        ConnectionKind::Unknown
    }
}
