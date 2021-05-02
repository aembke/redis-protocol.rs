/// Decoding functions for BytesMut and slices.
pub mod decode;
/// Encoding functions for BytesMut and slices.
pub mod encode;
/// RESP3 frame types.
pub mod types;

pub(crate) mod utils;

/// Shorthand for `use`'ing `types`, `encode`, `decode`, etc.
pub mod prelude {
  pub use super::decode::*;
  pub use super::encode::*;
  pub use super::types::*;

  pub use crate::utils::redis_keyslot;
}
