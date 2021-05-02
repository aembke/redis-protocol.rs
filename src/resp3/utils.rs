use crate::resp3::types::{Frame, FrameKind, HELLO, NULL};
use crate::types::{Redirection, CRLF};
use crate::utils::{digits_in_number, PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX};
use cookie_factory::GenError;
use resp3::types::{Auth, RespVersion, VerbatimStringFormat, INFINITY, NEG_INFINITY};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

pub const BOOLEAN_ENCODE_LEN: usize = 4;

pub fn blobstring_encode_len(b: &[u8]) -> usize {
  1 + digits_in_number(b.len()) + 2 + b.len() + 2
}

pub fn array_or_push_encode_len(frames: &Vec<Frame>) -> Result<usize, GenError> {
  let mut total_len = 1 + digits_in_number(frames.len()) + 2;

  for frame in frames.iter() {
    total_len += encode_len(frame)?;
  }
  Ok(total_len)
}

pub fn bignumber_encode_len(b: &[u8]) -> usize {
  1 + b.len() + 2
}

pub fn simplestring_encode_len(s: &str) -> usize {
  1 + s.as_bytes().len() + 2
}

pub fn verbatimstring_encode_len(data: &str) -> usize {
  // prefix, data len + format len, crlf, format, colon, data, crlf
  1 + digits_in_number(data.len() + 4) + 2 + 3 + 1 + data.len() + 2
}

pub fn number_encode_len(i: &i64) -> usize {
  let prefix = if *i < 0 { 1 } else { 0 };
  let as_usize = if *i < 0 { (*i * -1) as usize } else { *i as usize };

  1 + digits_in_number(as_usize) + 2 + prefix
}

pub fn double_encode_len(f: &f64) -> Result<usize, GenError> {
  if f.is_nan() {
    Err(GenError::CustomError(2))
  } else if f.is_infinite() {
    let inf_len = if f.is_sign_negative() {
      NEG_INFINITY.as_bytes().len()
    } else {
      INFINITY.as_bytes().len()
    };

    // comma, inf|-inf, CRLF
    Ok(1 + inf_len + 2)
  } else {
    // TODO there's probably a more clever way to do this
    Ok(1 + format!("{}", f).as_bytes().len() + 2)
  }
}

pub fn map_encode_len(map: &HashMap<Frame, Frame>) -> Result<usize, GenError> {
  let mut total_len = 1 + digits_in_number(map.len()) + 2;

  for (key, value) in map.iter() {
    total_len += encode_len(key)? + encode_len(value)?;
  }
  Ok(total_len)
}

pub fn set_encode_len(set: &HashSet<Frame>) -> Result<usize, GenError> {
  let mut total_len = 1 + digits_in_number(set.len()) + 2;

  for frame in set.iter() {
    total_len += encode_len(frame)?;
  }
  Ok(total_len)
}

pub fn hello_encode_len(version: &RespVersion, auth: &Option<Auth>) -> usize {
  let mut total_len = HELLO.as_bytes().len() + 2;

  if let Some(ref auth) = *auth {
    total_len += 1 + auth.username.as_bytes().len() + 1 + auth.password.as_bytes().len();
  }
  total_len
}

pub fn opt_frame_to_string_panic(f: Option<Frame>, msg: &str) -> String {
  f.expect(msg).to_string().expect(msg)
}

pub fn is_normal_pubsub(frames: &Vec<Frame>) -> bool {
  unimplemented!()
}

pub fn is_pattern_pubsub(frames: &Vec<Frame>) -> bool {
  unimplemented!()
}

/// Returns the number of bytes necessary to represent the frame.
pub fn encode_len(data: &Frame) -> Result<usize, GenError> {
  use crate::resp3::types::Frame::*;

  match *data {
    Array(ref a) | Push(ref a) => array_or_push_encode_len(a),
    BlobString(ref b) | BlobError(ref b) => Ok(blobstring_encode_len(b)),
    SimpleString(ref s) | SimpleError(ref s) => Ok(simplestring_encode_len(s)),
    Number(ref i) => Ok(number_encode_len(i)),
    Null => Ok(NULL.as_bytes().len()),
    Double(ref f) => double_encode_len(f),
    Boolean(_) => Ok(BOOLEAN_ENCODE_LEN),
    VerbatimString { ref data, .. } => Ok(verbatimstring_encode_len(data)),
    Map(ref m) | Attribute(ref m) => map_encode_len(m),
    Set(ref s) => set_encode_len(s),
    Hello { ref version, ref auth } => Ok(hello_encode_len(version, auth)),
    BigNumber(ref b) => Ok(bignumber_encode_len(b)),
  }
}

/// Return the string representation of a double, accounting for `inf` and `-inf`.
///
/// NaN is not checked here.
pub fn f64_to_redis_string(data: &f64) -> Cow<'static, str> {
  if data.is_infinite() {
    if data.is_sign_negative() {
      Cow::Borrowed(NEG_INFINITY)
    } else {
      Cow::Borrowed(INFINITY)
    }
  } else {
    Cow::Owned(data.to_string())
  }
}
