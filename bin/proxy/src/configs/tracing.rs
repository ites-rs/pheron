use tracing_subscriber::{EnvFilter, fmt, prelude::*};

pub fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,proxy=info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false).without_time().compact())
        .init();
}
