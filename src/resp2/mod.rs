/// Decoding functions for BytesMut and slices.
pub mod decode;
/// Encoding functions for BytesMut and slices.
pub mod encode;
/// RESP2 frame types.
pub mod types;

mod _decode;

pub(crate) mod utils;

/// Shorthand for `use`'ing `types`, `encode`, `decode`, etc.
pub mod prelude {
  pub use super::{decode::*, encode::*, types::*};

  pub use crate::utils::redis_keyslot;
}
