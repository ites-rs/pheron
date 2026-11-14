mod c2;
mod implant;
mod kind;

pub use c2::manage_c2_conn;
pub use implant::manage_implant_conn;
pub use kind::{ConnectionKind, peek};
