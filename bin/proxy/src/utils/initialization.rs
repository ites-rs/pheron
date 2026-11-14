use crate::configs::init_tracing;
use crate::models::ServerConfig;
use std::env;
use tokio::time::Duration;

pub fn server_initialization() -> ServerConfig {
    init_tracing();
    get_cli_args()
}

fn get_cli_args() -> ServerConfig {
    let args: Vec<String> = env::args().collect();
    let addr = &args[1];
    let next_hop = args[2].clone();
    let is_last = next_hop == "last";

    ServerConfig::new(addr, Duration::from_secs(10), &next_hop, is_last)
}
