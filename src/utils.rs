use crate::error::{RedisParseError, RedisProtocolError, RedisProtocolErrorKind};
use core::str;
use crc16::{State, XMODEM};

use crate::types::REDIS_CLUSTER_SLOTS;
#[cfg(feature = "bytes")]
use bytes::BytesMut;

/// Returns the number of bytes necessary to encode a string representation of `d`.
pub fn digits_in_usize(d: usize) -> usize {
  d.checked_ilog10().map_or(1, |n| n as usize + 1)
}

/// Returns the number of bytes necessary to encode a string representation of `d`.
pub fn digits_in_i64(d: i64) -> usize {
  let prefix = if d.is_negative() { 1 } else { 0 };
  prefix + d.unsigned_abs().checked_ilog10().map_or(1, |n| n as usize + 1)
}

/// Split `buf` around the first CRLF, returning the bytes before the CRLF and the bytes after it.
///
/// Returns `Incomplete` if the buffer does not contain a CRLF.
#[cfg(any(feature = "resp2", feature = "resp3"))]
#[inline]
pub(crate) fn find_crlf(buf: &[u8]) -> Result<(&[u8], &[u8]), nom::Err<RedisParseError<&[u8]>>> {
  let mut offset = 0;
  while let Some(idx) = memchr::memchr(b'\r', &buf[offset ..]) {
    let pos = offset + idx;
    match buf.get(pos + 1) {
      Some(b'\n') => return Ok((&buf[.. pos], &buf[pos + 2 ..])),
      Some(_) => offset = pos + 1,
      None => break,
    }
  }

  Err(nom::Err::Incomplete(nom::Needed::Unknown))
}

/// Whether `buf[start..]` contains a CRLF, resuming the search from `*searched` (the buffer length at the end of the
/// last unsuccessful search) so that a line arriving in many small reads is only searched once.
#[cfg(any(feature = "resp2", feature = "resp3"))]
pub(crate) fn line_is_buffered(buf: &[u8], start: usize, searched: &mut usize) -> bool {
  // the last search may have stopped between the `\r` and the `\n`
  let from = start.max(searched.saturating_sub(1));
  if buf.get(from ..).is_some_and(|rest| find_crlf(rest).is_ok()) {
    true
  } else {
    *searched = buf.len();
    false
  }
}

pub fn isize_to_usize<T>(val: isize) -> Result<usize, RedisParseError<T>> {
  if val >= 0 {
    Ok(val as usize)
  } else {
    Err(RedisParseError::new_custom("isize_to_usize", "Invalid length."))
  }
}

/// Extend the buffer by `amt`.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn zero_extend(buf: &mut BytesMut, amt: usize) {
  buf.resize(buf.len() + amt, 0);
}

/// Whether an error payload is a `MOVED` or `ASK` redirection.
pub(crate) fn is_redirection(payload: &str) -> bool {
  if payload.starts_with("MOVED") || payload.starts_with("ASK") {
    payload.split(' ').count() == 3
  } else {
    false
  }
}

/// Perform a crc16 XMODEM operation against a string slice.
fn crc16_xmodem(key: &[u8]) -> u16 {
  State::<XMODEM>::calculate(key) % REDIS_CLUSTER_SLOTS
}

/// Map a key to the corresponding cluster key slot.
///
/// ```ignore
/// $ redis-cli cluster keyslot "8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ"
/// (integer) 5458
/// ```
///
/// ```
/// # use redis_protocol::redis_keyslot;
/// assert_eq!(redis_keyslot(b"8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ"), 5458);
/// ```
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
  if i + j == key.len() || j == 0 {
    crc16_xmodem(key)
  } else {
    crc16_xmodem(&key[i + 1 .. i + j + 1])
  }
}

/// Convert a string to a double, supporting "nan", "+inf" and "-inf".
pub fn str_to_f64(s: &str) -> Result<f64, RedisProtocolError> {
  // this is changing in newer versions of redis to lose the "+" prefix
  match s {
    "+inf" | "inf" => Ok(f64::INFINITY),
    "-inf" => Ok(f64::NEG_INFINITY),
    "nan" => Ok(f64::NAN),
    _ => s.parse::<f64>().map_err(|_| {
      RedisProtocolError::new(
        RedisProtocolErrorKind::Unknown,
        "Could not convert to floating point value.",
      )
    }),
  }
}

/// Convert bytes to a boolean.
pub(crate) fn bytes_to_bool(b: &[u8]) -> Option<bool> {
  match b {
    b"true" | b"TRUE" | b"t" | b"T" | b"1" => Some(true),
    b"false" | b"FALSE" | b"f" | b"F" | b"0" => Some(false),
    _ => None,
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::vec::Vec;

  fn read_kitten_file() -> Vec<u8> {
    include_bytes!("../tests/kitten.jpeg").to_vec()
  }

  #[test]
  fn should_crc16_123456789() {
    let key = "123456789";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_brackets() {
    let key = "foo{123456789}bar";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_brackets_no_padding() {
    let key = "{123456789}";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_invalid_brackets_lhs() {
    let key = "foo{123456789";
    // 288A
    let expected: u16 = 10378;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_invalid_brackets_rhs() {
    let key = "foo}123456789";
    // 5B35 = 23349, 23349 % 16384 = 6965
    let expected: u16 = 6965;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_random_string() {
    let key = "8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ";
    // 127.0.0.1:30001> cluster keyslot 8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ
    // (integer) 5458
    let expected: u16 = 5458;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_hash_non_ascii_string_bytes() {
    let key = "💩 👻 💀 ☠️ 👽 👾";
    // 127.0.0.1:30001> cluster keyslot "💩 👻 💀 ☠️ 👽 👾"
    // (integer) 13954
    let expected: u16 = 13954;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_hash_non_ascii_string_bytes_with_tag() {
    let key = "💩 👻 💀{123456789}☠️ 👽 👾";
    // 127.0.0.1:30001> cluster keyslot "💩 👻 💀{123456789}☠️ 👽 👾"
    // (integer) 12739
    let expected: u16 = 12739;
    let actual = redis_keyslot(key.as_bytes());

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_hash_non_utf8_string_bytes() {
    let key = read_kitten_file();
    let expected: u16 = 1589;
    let actual = redis_keyslot(&key);

    assert_eq!(actual, expected)
  }

  #[test]
  fn should_hash_non_utf8_string_bytes_with_tag() {
    let mut key = read_kitten_file();
    for (idx, c) in "{123456789}".as_bytes().iter().enumerate() {
      key[242 + idx] = *c;
    }

    let expected: u16 = 12739;
    let actual = redis_keyslot(&key);
    assert_eq!(actual, expected)
  }

  #[test]
  fn should_count_digits_in_usize() {
    for d in [0, 1, 9, 10, 99, 100, 12345, 999_999_999_999_999_999, usize::MAX] {
      assert_eq!(digits_in_usize(d), d.to_string().len(), "{d}");
    }
  }

  #[test]
  fn should_count_digits_in_i64() {
    for d in [0, 1, -1, 9, -9, 10, -10, 999_999_999_999_999_999, i64::MAX, i64::MIN] {
      assert_eq!(digits_in_i64(d), d.to_string().len(), "{d}");
    }
  }

  #[test]
  #[cfg(any(feature = "resp2", feature = "resp3"))]
  fn should_find_crlf() {
    assert_eq!(find_crlf(b"foo\r\nbar").unwrap(), (&b"foo"[..], &b"bar"[..]));
    assert_eq!(find_crlf(b"\r\n").unwrap(), (&b""[..], &b""[..]));
    assert_eq!(find_crlf(b"a\rb\r\n").unwrap(), (&b"a\rb"[..], &b""[..]));
    assert_eq!(find_crlf(b"a\r\r\nb").unwrap(), (&b"a\r"[..], &b"b"[..]));
  }

  #[test]
  #[cfg(any(feature = "resp2", feature = "resp3"))]
  fn should_return_incomplete_without_crlf() {
    for buf in [&b""[..], b"foo", b"foo\r", b"foo\nbar", b"\r"] {
      assert!(matches!(find_crlf(buf), Err(nom::Err::Incomplete(_))), "{buf:?}");
    }
  }
}
