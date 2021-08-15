use crate::resp2::types::Frame as Resp2Frame;
use crate::resp3::types::Frame as Resp3Frame;
use crate::types::*;
use bytes::BytesMut;
use cookie_factory::GenError;
use crc16::{State, XMODEM};
use std::str;

pub const KB: usize = 1024;
/// A pre-defined zeroed out KB of data, used to speed up extending buffers while encoding.
pub const ZEROED_KB: &'static [u8; 1024] = &[0; 1024];

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

/// Prefix on normal pubsub messages.
pub const PUBSUB_PREFIX: &'static str = "message";
/// Prefix on pubsub messages from a pattern matching subscription.
pub const PATTERN_PUBSUB_PREFIX: &'static str = "pmessage";
/// Prefix on push pubsub messages.
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

/// Utility function to translate RESP2 frames to RESP3 frames.
///
/// RESP2 frames and RESP3 frames are quite different, but RESP3 is largely a superset of RESP2. Redis handles the protocol choice based on
/// the response to the HELLO command so developers of higher level clients are faced with a decision on how to handle the situation where
/// a higher level library may want to use RESP3 since the plan is to upgrade Redis in the future, but the server only supports RESP2 today.
/// This function can allow callers to take a dependency on the RESP3 interface by lazily translating RESP2 frames from the server to RESP3
/// frames while exposing the RESP3 interface up the stack.
pub fn resp2_frame_to_resp3(frame: Resp2Frame) -> Resp3Frame {
  if frame.is_normal_pubsub() {
    let mut out = Vec::with_capacity(4);
    out.push(Resp3Frame::SimpleString {
      data: PUBSUB_PUSH_PREFIX.to_owned(),
      attributes: None,
    });
    out.push(Resp3Frame::SimpleString {
      data: PUBSUB_PREFIX.to_owned(),
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
      data: PUBSUB_PUSH_PREFIX.to_owned(),
      attributes: None,
    });
    out.push(Resp3Frame::SimpleString {
      data: PATTERN_PUBSUB_PREFIX.to_owned(),
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
    }
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
    }
  }
}

/// Utility function for converting RESP3 frames back to RESP2 frames.
///
/// RESP2 has no concept of attributes, maps, blob errors, or certain other frames. The following policy is used for translating new RESP3 frames back to RESP2:
///
/// * Push - If the Push frame corresponds to a pubsub message then it's converted to an array, otherwise an error is returned.
/// * BlobError - An error is returned since the inner bytes might not be a UTF8 string, or they might be too large for a RESP2 SimpleError.
/// * BigNumber - This is converted to a RESP2 BulkString
/// * Boolean - This is converted to a BulkString with values of `true` or `false`. The associated [resp2_frame_to_resp3] function will convert back to Boolean from these values.
/// * Double - The inner floating point value is converted to a `String` and sent as a BulkString.
/// * VerbatimString - The inner data is sent as a BulkString. The format is discarded.
/// * Set - The inner data is sent as an Array.
/// * Map - An error is returned. RESP2 has no concept of a map.
/// * Hello - An error is returned. If the caller wants to send Hello it needs to be encoded manually.
/// * ChunkedString - An error is returned. RESP2 has no concept of streaming strings.
///
/// **Calling this with any RESP3 frame with attributes will result in an error.**
pub fn resp3_frame_to_resp2(frame: Resp3Frame) -> Result<Resp2Frame, RedisProtocolError> {
  if frame.attributes().is_some() {
    return Err(RedisProtocolError::new(
      RedisProtocolErrorKind::Unknown,
      "Cannot convert RESP3 frame with attributes to RESP2.",
    ));
  }

  if frame.is_pubsub_message() {
    let mut out = Vec::with_capacity(3);
    out.push(Resp2Frame::SimpleString(PUBSUB_PREFIX.to_owned()));

    if let Resp3Frame::Push { mut data, .. } = frame {
      // unwrap checked in is_normal_pubsub
      let message = data.pop().unwrap();
      let channel = data.pop().unwrap();

      out.push(resp3_frame_to_resp2(channel)?);
      out.push(resp3_frame_to_resp2(message)?);
    } else {
      panic!("Invalid pubsub frame converting to resp2 frame.");
    }

    return Ok(Resp2Frame::Array(out));
  }

  match frame {
    Resp3Frame::Array { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(resp3_frame_to_resp2(frame)?);
      }
      Ok(Resp2Frame::Array(out))
    }
    Resp3Frame::Push { data: _, .. } => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::Unknown,
      "Cannot convert non-pubsub PUSH frame to RESP2 frame.",
    )),
    Resp3Frame::BlobString { data, .. } => Ok(Resp2Frame::BulkString(data)),
    Resp3Frame::BlobError { data: _, .. } => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::Unknown,
      "Cannot convert BlobError to RESP2 frame.",
    )),
    Resp3Frame::BigNumber { data, .. } => Ok(Resp2Frame::BulkString(data)),
    Resp3Frame::Boolean { data, .. } => {
      if data {
        Ok(Resp2Frame::BulkString("true".into()))
      } else {
        Ok(Resp2Frame::BulkString("false".into()))
      }
    }
    Resp3Frame::Number { data, .. } => Ok(Resp2Frame::Integer(data)),
    Resp3Frame::Double { data, .. } => Ok(Resp2Frame::BulkString(data.to_string().into_bytes())),
    Resp3Frame::VerbatimString { data, .. } => Ok(Resp2Frame::BulkString(data.into_bytes())),
    Resp3Frame::SimpleError { data, .. } => Ok(Resp2Frame::Error(data)),
    Resp3Frame::SimpleString { data, .. } => Ok(Resp2Frame::SimpleString(data)),
    Resp3Frame::Set { data, .. } => {
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(resp3_frame_to_resp2(frame)?);
      }
      Ok(Resp2Frame::Array(out))
    }
    Resp3Frame::Map { data: _, .. } => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::Unknown,
      "Cannot convert Map to RESP2 frame.",
    )),
    Resp3Frame::Null => Ok(Resp2Frame::Null),
    Resp3Frame::ChunkedString(_) => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::Unknown,
      "Cannot convert ChunkedString to RESP2 frame.",
    )),
    Resp3Frame::Hello { .. } => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::Unknown,
      "Cannot convert HELLO to RESP2 frame.",
    )),
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
///
/// ```ignore
/// $ redis-cli -p 30001 cluster keyslot "8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ"
/// (integer) 5458
/// ```
///
/// ```no_run
/// # use redis_protocol::redis_keyslot;
/// assert_eq!(redis_keyslot("8xjx7vWrfPq54mKfFD3Y1CcjjofpnAcQ"), 5458);
/// ```
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
