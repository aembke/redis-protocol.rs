use crate::nom_bytes::NomBytes;
use crate::resp2::types::Frame as Resp2Frame;
use crate::resp3::types::Frame as Resp3Frame;
use crate::types::*;
use bytes::BytesMut;
use bytes_utils::Str;
use cookie_factory::GenError;
use crc16::{State, XMODEM};
use nom::error::ErrorKind as NomErrorKind;
use alloc::borrow::ToOwned;
use alloc::vec::Vec;
use core::str;

pub const KB: usize = 1024;
/// A pre-defined zeroed out KB of data, used to speed up extending buffers while encoding.
pub const ZEROED_KB: &'static [u8; 1024] = &[0; 1024];

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

/// Prefix on normal pubsub messages.
///
/// In RESP2 this may be the first inner frame, in RESP3 it may be the second inner frame.
pub const PUBSUB_PREFIX: &'static str = "message";
/// Prefix on pubsub messages from a pattern matching subscription.
///
/// In RESP2 this may be the first inner frame, in RESP3 it may be the second inner frame.
pub const PATTERN_PUBSUB_PREFIX: &'static str = "pmessage";
/// Prefix on RESP3 push pubsub messages.
///
/// In RESP3 this is the first inner frame in a push pubsub message.
pub const PUBSUB_PUSH_PREFIX: &'static str = "pubsub";

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

macro_rules! e (
  ($err:expr) => {
    return Err(RedisParseError::from($err).into_nom_error())
  }
);

macro_rules! etry (
  ($expr:expr) => {
    match $expr {
      Ok(result) => result,
      Err(e) => return Err(RedisParseError::from(e).into_nom_error())
    }
  }
);

#[cfg(feature = "decode-logs")]
macro_rules! decode_log(
  ($buf:ident, $($arg:tt)*) => (
    if log_enabled!(log::Level::Trace) {
      if let Some(s) = std::str::from_utf8(&$buf).ok() {
        let $buf = s;
        trace!($($arg)*)
      }else{
        trace!($($arg)*)
      }
    }
  );
  ($buf:expr, $name:ident, $($arg:tt)*) => (
    if log_enabled!(log::Level::Trace) {
      if let Some(s) = std::str::from_utf8(&$buf).ok() {
        let $name = s;
        trace!($($arg)*)
      }else{
        trace!($($arg)*)
      }
    }
  );
  ($($arg:tt)*) => (
    if log_enabled!(log::Level::Trace) {
      trace!($($arg)*)
    }
  );
);

#[cfg(not(feature = "decode-logs"))]
macro_rules! decode_log(
  ($buf:ident, $($arg:tt)*) => ();
  ($($arg:tt)*) => ();
);

/// Utility function to translate RESP2 frames to RESP3 frames.
///
/// RESP2 frames and RESP3 frames are quite different, but RESP3 is largely a superset of RESP2 so this function will never return an error.
///
/// Redis handles the protocol choice based on the response to the `HELLO` command, so developers of higher level clients can be faced with a decision on which `Frame`
/// struct to expose to callers.
///
/// This function can allow callers to take a dependency on the RESP3 interface by lazily translating RESP2 frames from the server to RESP3 frames.
pub fn resp2_frame_to_resp3(frame: Resp2Frame) -> Resp3Frame {
  if frame.is_normal_pubsub() {
    let mut out = Vec::with_capacity(4);
    out.push(Resp3Frame::SimpleString {
      data: PUBSUB_PUSH_PREFIX.into(),
      attributes: None,
    });
    out.push(Resp3Frame::SimpleString {
      data: PUBSUB_PREFIX.into(),
      attributes: None,
    });
    if let Resp2Frame::Array(mut inner) = frame {
      // unwrap checked in is_normal_pubsub
      let message = inner.pop().unwrap();
      let channel = inner.pop().unwrap();
      out.push(resp2_frame_to_resp3(channel));
      out.push(resp2_frame_to_resp3(message));
    } else {
      panic!("Invalid pubsub frame conversion to resp3. This is a bug.");
    }

    return Resp3Frame::Push {
      data: out,
      attributes: None,
    };
  }
  if frame.is_pattern_pubsub_message() {
    let mut out = Vec::with_capacity(4);
    out.push(Resp3Frame::SimpleString {
      data: PUBSUB_PUSH_PREFIX.into(),
      attributes: None,
    });
    out.push(Resp3Frame::SimpleString {
      data: PATTERN_PUBSUB_PREFIX.into(),
      attributes: None,
    });
    if let Resp2Frame::Array(mut inner) = frame {
      // unwrap checked in is_normal_pubsub
      let message = inner.pop().unwrap();
      let channel = inner.pop().unwrap();
      out.push(resp2_frame_to_resp3(channel));
      out.push(resp2_frame_to_resp3(message));
    } else {
      panic!("Invalid pattern pubsub frame conversion to resp3. This is a bug.");
    }

    return Resp3Frame::Push {
      data: out,
      attributes: None,
    };
  }

  match frame {
    Resp2Frame::Integer(i) => Resp3Frame::Number {
      data: i,
      attributes: None,
    },
    Resp2Frame::Error(s) => Resp3Frame::SimpleError {
      data: s,
      attributes: None,
    },
    Resp2Frame::BulkString(d) => {
      if d.len() < 6 {
        match str::from_utf8(&d).ok() {
          Some(s) => match s.as_ref() {
            "true" => Resp3Frame::Boolean {
              data: true,
              attributes: None,
            },
            "false" => Resp3Frame::Boolean {
              data: false,
              attributes: None,
            },
            _ => Resp3Frame::BlobString {
              data: d,
              attributes: None,
            },
          },
          None => Resp3Frame::BlobString {
            data: d,
            attributes: None,
          },
        }
      } else {
        Resp3Frame::BlobString {
          data: d,
          attributes: None,
        }
      }
    },
    Resp2Frame::SimpleString(s) => Resp3Frame::SimpleString {
      data: s,
      attributes: None,
    },
    Resp2Frame::Null => Resp3Frame::Null,
    Resp2Frame::Array(data) => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(resp2_frame_to_resp3(frame));
      }
      Resp3Frame::Array {
        data: out,
        attributes: None,
      }
    },
  }
}

pub fn check_offset(x: &(&mut [u8], usize)) -> Result<(), GenError> {
  if x.1 > x.0.len() {
    error!("Invalid offset of {} with buf len {}", x.1, x.0.len());
    Err(GenError::InvalidOffset)
  } else {
    Ok(())
  }
}

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
/// assert_eq!(redis_keyslot("8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ".as_bytes()), 5458);
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
  for (idx, c) in key[i + 1..].iter().enumerate() {
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
    crc16_xmodem(&key[i + 1..i + j + 1])
  };

  out
}

pub(crate) fn to_byte_str<T>(data: T) -> Result<Str, RedisParseError<NomBytes>>
where
  T: AsRef<NomBytes>,
{
  let data_ref = data.as_ref();
  Str::from_inner(data_ref.clone().into_bytes()).map_err(|_| RedisParseError::Nom(data_ref.clone(), NomErrorKind::Char))
}

#[cfg(test)]
mod tests {
  use super::*;

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
}
