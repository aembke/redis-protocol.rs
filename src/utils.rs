use bytes::BytesMut;
use cookie_factory::GenError;
use crc16::{State, XMODEM};
use types::*;

pub const KB: usize = 1024;
/// A pre-defined zeroed out KB of data, used to speed up extending buffers while encoding.
pub const ZEROED_KB: &'static [u8; 1024] = &[0; 1024];

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

/// Prefix on normal pubsub messages.
pub const PUBSUB_PREFIX: &'static str = "message";
/// Prefix on pubsub messages from a pattern matching subscription.
pub const PATTERN_PUBSUB_PREFIX: &'static str = "pmessage";

macro_rules! unwrap_return(
  ($expr:expr) => {
    match $expr {
      Some(val) => val,
      None => return None,
    }
  };
);

macro_rules! encode_checks(
  ($x:ident, $required:expr) => {
    let _ = crate::utils::check_offset(&$x)?;
    let required = $required;
    let remaining = $x.0.len() - $x.1;

    if remaining < required {
      return Err(cookie_factory::GenError::BufferTooSmall(required - remaining));
    }
  }
);

pub fn check_offset(x: &(&mut [u8], usize)) -> Result<(), GenError> {
  if x.1 > x.0.len() {
    Err(GenError::InvalidOffset)
  } else {
    Ok(())
  }
}

/// Returns the number of bytes necessary to encode a string representation of `d`.
pub fn digits_in_number(d: usize) -> usize {
  if d == 0 {
    return 1;
  }

  ((d as f64).log10()).floor() as usize + 1
}

// this is faster than repeat(0).take(amt) at the cost of some memory
pub fn zero_extend(buf: &mut BytesMut, mut amt: usize) {
  trace!("allocating more, len: {}, amt: {}", buf.len(), amt);

  buf.reserve(amt);
  while amt >= KB {
    buf.extend_from_slice(ZEROED_KB);
    amt -= KB;
  }
  if amt > 0 {
    buf.extend_from_slice(&ZEROED_KB[0..amt]);
  }
}

pub fn redirection_to_frame(prefix: &'static str, slot: u16, server: &str) -> String {
  format!("{} {} {}", prefix, slot, server)
}

pub fn is_cluster_error(payload: &str) -> bool {
  if payload.starts_with("MOVED") || payload.starts_with("ASK") {
    payload.split(" ").fold(0, |c, _| c + 1) == 3
  } else {
    false
  }
}

pub fn read_cluster_error(payload: &str) -> Option<Redirection> {
  if payload.starts_with("MOVED") {
    let parts: Vec<&str> = payload.split(" ").collect();
    if parts.len() == 3 {
      let slot = unwrap_return!(parts[1].parse::<u16>().ok());
      let server = parts[2].to_owned();

      Some(Redirection::Moved { slot, server })
    } else {
      None
    }
  } else if payload.starts_with("ASK") {
    let parts: Vec<&str> = payload.split(" ").collect();
    if parts.len() == 3 {
      let slot = unwrap_return!(parts[1].parse::<u16>().ok());
      let server = parts[2].to_owned();

      Some(Redirection::Ask { slot, server })
    } else {
      None
    }
  } else {
    None
  }
}

/// Perform a crc16 XMODEM operation against a string slice.
fn crc16_xmodem(key: &str) -> u16 {
  State::<XMODEM>::calculate(key.as_bytes()) % REDIS_CLUSTER_SLOTS
}

/// Map a Redis key to its cluster key slot.
pub fn redis_keyslot(key: &str) -> u16 {
  let (mut i, mut j): (Option<usize>, Option<usize>) = (None, None);

  for (idx, c) in key.chars().enumerate() {
    if c == '{' {
      i = Some(idx);
      break;
    }
  }

  if i.is_none() || (i.is_some() && i.unwrap() == key.len() - 1) {
    return crc16_xmodem(key);
  }

  let i = i.unwrap();
  for (idx, c) in key[i + 1..].chars().enumerate() {
    if c == '}' {
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
    crc16_xmodem(&key[i + 1..i + j + 1])
  };

  trace!("mapped {} to redis slot {}", key, out);
  out
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_crc16_123456789() {
    let key = "123456789";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_brackets() {
    let key = "foo{123456789}bar";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_brackets_no_padding() {
    let key = "{123456789}";
    // 31C3
    let expected: u16 = 12739;
    let actual = redis_keyslot(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_invalid_brackets_lhs() {
    let key = "foo{123456789";
    // 288A
    let expected: u16 = 10378;
    let actual = redis_keyslot(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_invalid_brackets_rhs() {
    let key = "foo}123456789";
    // 5B35 = 23349, 23349 % 16384 = 6965
    let expected: u16 = 6965;
    let actual = redis_keyslot(key);

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_crc16_with_random_string() {
    let key = "8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ";
    // 127.0.0.1:30001> cluster keyslot 8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ
    // (integer) 5458
    let expected: u16 = 5458;
    let actual = redis_keyslot(key);

    assert_eq!(actual, expected);
  }
}
