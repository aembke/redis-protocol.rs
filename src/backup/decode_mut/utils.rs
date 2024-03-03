use crate::types::{RedisProtocolError, RedisProtocolErrorKind};
use bytes::Bytes;
use core::hash::{Hash, Hasher};

pub fn hash_tuple<H: Hasher>(state: &mut H, range: &(usize, usize)) {
  range.0.hash(state);
  range.1.hash(state);
}

pub fn range_to_bytes(buf: &Bytes, start: usize, end: usize) -> Result<Bytes, RedisProtocolError> {
  let len = buf.len();
  if start > len || end > len {
    return Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid frame byte offset.",
    ));
  }

  Ok(buf.slice(start..end))
}
