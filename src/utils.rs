use crate::error::{RedisParseError, RedisProtocolError, RedisProtocolErrorKind};
use alloc::vec::Vec;
use cookie_factory::GenError;
use core::{iter::repeat, str};

#[cfg(feature = "bytes")]
use bytes::BytesMut;

#[cfg(feature = "routing")]
use crc16::{State, XMODEM};

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

/// Returns the number of bytes necessary to encode a string representation of `d`.
#[cfg(feature = "std")]
pub fn digits_in_number(d: usize) -> usize {
  if d == 0 {
    return 1;
  }

  ((d as f64).log10()).floor() as usize + 1
}

#[cfg(feature = "libm")]
pub fn digits_in_number(d: usize) -> usize {
  if d == 0 {
    return 1;
  }

  libm::floor(libm::log10(d as f64)) as usize + 1
}

pub fn check_offset(x: &(&mut [u8], usize)) -> Result<(), GenError> {
  if x.1 > x.0.len() {
    Err(GenError::InvalidOffset)
  } else {
    Ok(())
  }
}

pub fn isize_to_usize<'a, T>(val: isize) -> Result<usize, RedisParseError<T>> {
  if val >= 0 {
    Ok(val as usize)
  } else {
    Err(RedisParseError::new_custom("isize_to_usize", "Invalid length."))
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn zero_extend(buf: &mut BytesMut, amt: usize) {
  buf.reserve(amt);
  buf.extend(repeat(0).take(amt));
}

#[cfg(feature = "bytes")]
fn zero_extend_1(buf: &mut BytesMut, amt: usize) {
  buf.reserve(amt);
  buf.extend(repeat(0).take(amt));
}

#[cfg(feature = "bytes")]
fn zero_extend_2(buf: &mut BytesMut, amt: usize) {
  use bytes::BufMut;

  buf.reserve(amt);
  let offset = buf.len();
  for _ in 0 .. amt {
    buf.put_u8(0);
  }
  unsafe {
    buf.set_len(offset);
  }
}

#[cfg(feature = "bytes")]
fn zero_extend_3(buf: &mut BytesMut, amt: usize) {
  buf.reserve(amt);
  buf.extend(vec![0; amt])
}

/// Whether an error payload is a `MOVED` or `ASK` redirection.
pub(crate) fn is_redirection(payload: &str) -> bool {
  if payload.starts_with("MOVED") || payload.starts_with("ASK") {
    payload.split(" ").count() == 3
  } else {
    false
  }
}

/// Perform a crc16 XMODEM operation against a string slice.
#[cfg(feature = "routing")]
fn crc16_xmodem(key: &[u8]) -> u16 {
  State::<XMODEM>::calculate(key) % REDIS_CLUSTER_SLOTS
}

/// Map a Redis key to its cluster key slot.
///
/// ```ignore
/// $ redis-cli -p 30001 cluster keyslot "8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ"
/// (integer) 5458
/// ```
///
/// ```no_run
/// # use redis_protocol::redis_keyslot;
/// assert_eq!(redis_keyslot(b"8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ"), 5458);
/// ```
#[cfg(feature = "routing")]
#[cfg_attr(docsrs, doc(cfg(feature = "routing")))]
pub fn redis_keyslot(key: &[u8]) -> u16 {
  let (mut i, mut j): (Option<usize>, Option<usize>) = (None, None);

  for (idx, c) in key.iter().enumerate() {
    if *c == b'{' {
      i = Some(idx);
      break;
    }
  }

  if i.is_none() || (i.is_some() && i.unwrap() == key.len() - 1) {
    return crc16_xmodem(key);
  }

  let i = i.unwrap();
  for (idx, c) in key[i + 1 ..].iter().enumerate() {
    if *c == b'}' {
      j = Some(idx);
      break;
    }
  }

  if j.is_none() {
    return crc16_xmodem(key);
  }

  let j = j.unwrap();
  let out = if i + j == key.len() || j == 0 {
    crc16_xmodem(key)
  } else {
    crc16_xmodem(&key[i + 1 .. i + j + 1])
  };

  out
}

/// Convert a string to a double, supporting "+inf" and "-inf".
pub fn str_to_f64(s: &str) -> Result<f64, RedisProtocolError> {
  // this is changing in newer versions of redis to lose the "+" prefix
  if s == "+inf" || s == "inf" {
    Ok(f64::INFINITY)
  } else if s == "-inf" {
    Ok(f64::NEG_INFINITY)
  } else {
    s.parse::<f64>().map_err(|_| {
      RedisProtocolError::new(
        RedisProtocolErrorKind::Unknown,
        "Could not convert to floating point value.",
      )
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[cfg(feature = "routing")]
  fn read_kitten_file() -> Vec<u8> {
    include_bytes!("../tests/kitten.jpeg").to_vec()
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_crc16_123456789() {
    let key = "123456789";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_crc16_with_brackets() {
    let key = "foo{123456789}bar";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_crc16_with_brackets_no_padding() {
    let key = "{123456789}";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_crc16_with_invalid_brackets_lhs() {
    let key = "foo{123456789";
    // 288A
    let expected: u16 = 10378;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_crc16_with_invalid_brackets_rhs() {
    let key = "foo}123456789";
    // 5B35 = 23349, 23349 % 16384 = 6965
    let expected: u16 = 6965;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_crc16_with_random_string() {
    let key = "8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ";
    // 127.0.0.1:30001> cluster keyslot 8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ
    // (integer) 5458
    let expected: u16 = 5458;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_hash_non_ascii_string_bytes() {
    let key = "💩 👻 💀 ☠️ 👽 👾";
    // 127.0.0.1:30001> cluster keyslot "💩 👻 💀 ☠️ 👽 👾"
    // (integer) 13954
    let expected: u16 = 13954;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_hash_non_ascii_string_bytes_with_tag() {
    let key = "💩 👻 💀{123456789}☠️ 👽 👾";
    // 127.0.0.1:30001> cluster keyslot "💩 👻 💀{123456789}☠️ 👽 👾"
    // (integer) 12739
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_hash_non_utf8_string_bytes() {
    let key = read_kitten_file();
    let expected: u16 = 1589;
    let actual = redis_keyslot(&key);

    assert_eq!(actual, expected)
  }

  #[test]
  #[cfg(feature = "routing")]
  fn should_hash_non_utf8_string_bytes_with_tag() {
    let mut key = read_kitten_file();
    for (idx, c) in "{123456789}".as_bytes().iter().enumerate() {
      key[242 + idx] = *c;
    }

    let expected: u16 = 12739;
    let actual = redis_keyslot(&key);
    assert_eq!(actual, expected)
  }
}
