/// Decoding functions for `BytesMut` and slices.
pub mod decode;
/// Encoding functions for `BytesMut` and slices.
pub mod encode;
/// RESP2 frame types.
pub mod types;

pub(crate) mod utils;

#[cfg(test)]
mod _test {
  #[test]
  #[cfg(feature = "decode-logs")]
  fn _enable_logging() {
    let _ = pretty_env_logger::try_init();
  }
}
