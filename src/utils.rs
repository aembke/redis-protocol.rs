
use ::types::*;

use crc16::{
  State,
  XMODEM
};

use bytes::BytesMut;

use cookie_factory::GenError;

/// Terminating bytes between frames.
pub const CRLF: &'static str = "\r\n";
/// Byte representation of a `null` value.
pub const NULL: &'static str = "$-1\r\n";

pub const KB: usize = 1024;

/// A pre-defined zeroed out KB of data, used to speed up extending buffers while encoding.
pub const ZEROED_KB: &'static [u8; 1024] = &[0; 1024];

const REDIS_CLUSTER_SLOTS: u16 = 16384;

const PUBSUB_PREFIX: &'static str = "message";
const PATTERN_PUBSUB_PREFIX: &'static str = "pmessage";

#[inline]
pub fn check_offset(x: &(&mut [u8], usize)) -> Result<(), GenError> {
  if x.1 > x.0.len() {
    Err(GenError::InvalidOffset)
  }else{
    Ok(())
  }
}

/// Returns the number of bytes necessary to encode a string representation of `d`.
#[inline]
pub fn digits_in_number(d: usize) -> usize {
  if d == 0 {
    return 1;
  }

  ((d as f64).log10()).floor() as usize + 1
}

#[inline]
pub fn bulkstring_encode_len(b: &[u8]) -> usize {
  1 + digits_in_number(b.len()) + 2 + b.len() + 2
}

#[inline]
pub fn array_encode_len(frames: &Vec<Frame>) -> Result<usize, GenError> {
  let padding = 1 + digits_in_number(frames.len()) + 2;

  frames.iter().fold(Ok(padding), |m, f| {
    m.and_then(|s| encode_len(f).map(|l| s + l))
  })
}

#[inline]
pub fn simplestring_encode_len(s: &str) -> usize {
  1 + s.len() + 2
}

#[inline]
pub fn error_encode_len(s: &str) -> usize {
  1 + s.len() + 2
}

#[inline]
pub fn integer_encode_len(i: &i64) -> usize {
  let prefix = if *i < 0 {
    1
  }else{
    0
  };
  let as_usize = if *i < 0 {
    (*i * -1) as usize
  }else{
    *i as usize
  };

  1 + digits_in_number(as_usize) + 2 + prefix
}

/// Returns the number of bytes necessary to represent the frame.
pub fn encode_len(data: &Frame) -> Result<usize, GenError> {
  match *data {
    Frame::BulkString(ref b)   => Ok(bulkstring_encode_len(&b)),
    Frame::Array(ref frames)   => array_encode_len(frames),
    Frame::Null                => Ok(NULL.as_bytes().len()),
    Frame::SimpleString(ref s) => Ok(simplestring_encode_len(s)),
    Frame::Error(ref s)        => Ok(error_encode_len(s)),
    Frame::Integer(ref i)      => Ok(integer_encode_len(i)),
    Frame::Moved(ref s)        => Ok(error_encode_len(s)),
    Frame::Ask(ref s)          => Ok(error_encode_len(s))
  }
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

#[inline]
pub fn redirection_to_frame(prefix: &'static str, slot: u16, host: &str, port: u16) -> String {
  format!("{} {} {}:{}", prefix, slot, host, port)
}

pub fn string_to_redirection(s: &str) -> Result<Redirection, RedisProtocolError> {
  let parts: Vec<&str> = s.split(" ").collect();

  if parts.len() != 3 {
    return Err(RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid redirection."));
  }

  let is_moved = match parts[0].as_ref() {
    "MOVED" => true,
    "ASK"   => false,
    _ => return Err(RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid redirection kind."))
  };

  let slot = match parts[1].parse::<u16>() {
    Ok(s) => s,
    Err(_) => return Err(RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid hash slot redirection."))
  };

  let address_parts: Vec<&str> = parts[2].split(":").collect();
  if address_parts.len() != 2 {
    return Err(RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid redirection address."));
  }

  let host = address_parts[0].to_owned();
  let port = match address_parts[1].parse::<u16>() {
    Ok(p) => p,
    Err(_) => return Err(RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid redirection address port."))
  };

  if is_moved {
    Ok(Redirection::Moved {slot, host, port})
  }else{
    Ok(Redirection::Ask {slot, host, port})
  }
}

/// Perform a crc16 XMODEM operation against a string slice.
#[inline]
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
  for (idx, c) in key[i+1..].chars().enumerate() {
    if c == '}' {
      j = Some(idx);
      break;
    }
  }

  if j.is_none() {
    return crc16_xmodem(key);
  }

  let j = j.unwrap();
  let out = if i+j == key.len() || j == 0 {
    crc16_xmodem(key)
  }else{
    crc16_xmodem(&key[i+1..i+j+1])
  };

  trace!("mapped {} to redis slot {}", key, out);
  out
}

pub fn read_cluster_error(payload: &str) -> Option<Frame> {
  if payload.starts_with("MOVED") {
    let parts: Vec<&str> = payload.split(" ").collect();
    Some(Frame::Moved(parts[1..].join(" ").to_owned()))
  }else if payload.starts_with("ASK") {
    let parts: Vec<&str> = payload.split(" ").collect();
    Some(Frame::Ask(parts[1..].join(" ").to_owned()))
  }else{
    None
  }
}

pub fn opt_frame_to_string_panic(f: Option<Frame>, msg: &str) -> String {
  f.expect(msg).to_string().expect(msg)
}

pub fn is_normal_pubsub(frames: &Vec<Frame>) -> bool {
  frames.len() == 3
    && frames[0].kind() == FrameKind::BulkString
    && frames[0].as_str().map(|s| s == PUBSUB_PREFIX).unwrap_or(false)
}

pub fn is_pattern_pubsub(frames: &Vec<Frame>) -> bool {
  frames.len() == 4
    && frames[0].kind() == FrameKind::BulkString
    && frames[0].as_str().map(|s| s == PATTERN_PUBSUB_PREFIX).unwrap_or(false)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_get_encode_len_simplestring() {
    let ss1 = "Ok";
    let ss2 = "FooBarBaz";
    let ss3 = "-&#$@9232";

    assert_eq!(simplestring_encode_len(ss1), 5);
    assert_eq!(simplestring_encode_len(ss2), 12);
    assert_eq!(simplestring_encode_len(ss3), 12);
  }

  #[test]
  fn should_get_encode_len_error() {
    let e1 = "MOVED 3999 127.0.0.1:6381";
    let e2 = "ERR unknown command 'foobar'";
    let e3 = "WRONGTYPE Operation against a key holding the wrong kind of value";

    assert_eq!(error_encode_len(e1), 28);
    assert_eq!(error_encode_len(e2), 31);
    assert_eq!(error_encode_len(e3), 68);
  }

  #[test]
  fn should_get_encode_len_integer() {
    let i1: i64 = 38473;
    let i2: i64 = -74834;

    assert_eq!(integer_encode_len(&i1), 8);
    assert_eq!(integer_encode_len(&i2), 9);
  }

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
