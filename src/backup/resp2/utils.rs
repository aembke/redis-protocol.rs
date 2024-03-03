use crate::resp2::types::{Frame, FrameKind, NULL};
use crate::utils::{digits_in_number, PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX};
use alloc::string::String;
use alloc::vec::Vec;
use cookie_factory::GenError;

pub fn bulkstring_encode_len(b: &[u8]) -> usize {
  1 + digits_in_number(b.len()) + 2 + b.len() + 2
}

pub fn array_encode_len(frames: &Vec<Frame>) -> Result<usize, GenError> {
  let padding = 1 + digits_in_number(frames.len()) + 2;

  frames
    .iter()
    .fold(Ok(padding), |m, f| m.and_then(|s| encode_len(f).map(|l| s + l)))
}

pub fn simplestring_encode_len(s: &[u8]) -> usize {
  1 + s.len() + 2
}

pub fn error_encode_len(s: &str) -> usize {
  1 + s.as_bytes().len() + 2
}

pub fn integer_encode_len(i: &i64) -> usize {
  let prefix = if *i < 0 { 1 } else { 0 };
  let as_usize = if *i < 0 { (*i * -1) as usize } else { *i as usize };

  1 + digits_in_number(as_usize) + 2 + prefix
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

/// Returns the number of bytes necessary to represent the frame.
pub fn encode_len(data: &Frame) -> Result<usize, GenError> {
  match *data {
    Frame::BulkString(ref b) => Ok(bulkstring_encode_len(&b)),
    Frame::Array(ref frames) => array_encode_len(frames),
    Frame::Null => Ok(NULL.as_bytes().len()),
    Frame::SimpleString(ref s) => Ok(simplestring_encode_len(s)),
    Frame::Error(ref s) => Ok(error_encode_len(s)),
    Frame::Integer(ref i) => Ok(integer_encode_len(i)),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_get_encode_len_simplestring() {
    let ss1 = "Ok";
    let ss2 = "FooBarBaz";
    let ss3 = "-&#$@9232";

    assert_eq!(simplestring_encode_len(ss1.as_bytes()), 5);
    assert_eq!(simplestring_encode_len(ss2.as_bytes()), 12);
    assert_eq!(simplestring_encode_len(ss3.as_bytes()), 12);
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
}
