use crate::{
  digits_in_number,
  resp2::utils::{bulkstring_encode_len, error_encode_len, integer_encode_len, simplestring_encode_len},
  resp3::types::OwnedFrame as Resp3OwnedFrame,
  types::{_Range, PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX, PUBSUB_PUSH_PREFIX, SHARD_PUBSUB_PREFIX},
  utils,
};
use alloc::{
  string::{String, ToString},
  vec::Vec,
};
use core::{
  fmt::Debug,
  hash::{Hash, Hasher},
  mem,
  str,
};

#[cfg(feature = "convert")]
use crate::convert::FromResp2;
#[cfg(any(feature = "bytes", feature = "convert"))]
use crate::error::RedisProtocolError;
#[cfg(feature = "bytes")]
use crate::error::RedisProtocolErrorKind;
#[cfg(feature = "bytes")]
use crate::resp3::types::BytesFrame as Resp3BytesFrame;
#[cfg(feature = "bytes")]
use bytes::Bytes;
#[cfg(feature = "bytes")]
use bytes_utils::Str;

/// Byte prefix before a simple string type.
pub const SIMPLESTRING_BYTE: u8 = b'+';
/// Byte prefix before an error type.
pub const ERROR_BYTE: u8 = b'-';
/// Byte prefix before an integer type.
pub const INTEGER_BYTE: u8 = b':';
/// Byte prefix before a bulk string type.
pub const BULKSTRING_BYTE: u8 = b'$';
/// Byte prefix before an array type.
pub const ARRAY_BYTE: u8 = b'*';

/// The binary representation of NULL in RESP2.
pub const NULL: &str = "$-1\r\n";

/// An enum representing the type of RESP frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameKind {
  SimpleString,
  Error,
  Integer,
  BulkString,
  Array,
  Null,
}

impl FrameKind {
  /// Whether the frame is an error.
  pub fn is_error(&self) -> bool {
    matches!(self, FrameKind::Error)
  }

  /// Read the frame type from the RESP2 byte prefix.
  pub fn from_byte(d: u8) -> Option<FrameKind> {
    use self::FrameKind::*;

    match d {
      SIMPLESTRING_BYTE => Some(SimpleString),
      ERROR_BYTE => Some(Error),
      INTEGER_BYTE => Some(Integer),
      BULKSTRING_BYTE => Some(BulkString),
      ARRAY_BYTE => Some(Array),
      _ => None,
    }
  }

  /// Convert to the associated RESP2 byte prefix.
  pub fn to_byte(&self) -> u8 {
    use self::FrameKind::*;

    match *self {
      SimpleString => SIMPLESTRING_BYTE,
      Error => ERROR_BYTE,
      Integer => INTEGER_BYTE,
      BulkString | Null => BULKSTRING_BYTE,
      Array => ARRAY_BYTE,
    }
  }

  /// A function used to differentiate data types that may have the same inner binary representation when hashing.
  pub fn hash_prefix(&self) -> &'static str {
    use self::FrameKind::*;

    match *self {
      Array => "a",
      BulkString => "b",
      SimpleString => "s",
      Error => "e",
      Integer => "i",
      Null => "N",
    }
  }
}

/// A reference-free frame type representing ranges into an associated buffer, typically used to implement zero-copy
/// parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RangeFrame {
  /// A RESP2 simple string.
  SimpleString(_Range),
  /// A short string representing an error.
  Error(_Range),
  /// A signed 64-bit integer.
  Integer(i64),
  /// A byte array.
  BulkString(_Range),
  /// An array of frames.
  Array(Vec<RangeFrame>),
  /// A null value.
  Null,
}

/// Generic operations on a RESP2 frame.
pub trait Resp2Frame: Debug + Hash + Eq {
  /// Replace `self` with Null, returning the original value.
  fn take(&mut self) -> Self;

  /// Read the `FrameKind` value for this frame.
  fn kind(&self) -> FrameKind;

  /// Attempt to read the frame value as a string slice.
  fn as_str(&self) -> Option<&str>;

  /// Convert the value to a boolean.
  fn as_bool(&self) -> Option<bool>;

  /// Attempt to read the frame value as a byte slice.
  fn as_bytes(&self) -> Option<&[u8]>;

  /// Copy and read the inner value as a string, if possible.
  fn to_string(&self) -> Option<String>;

  /// Read the number of bytes needed to encode this frame.
  fn encode_len(&self) -> usize;

  /// Whether the frame is a message from a `subscribe` call.
  fn is_normal_pubsub_message(&self) -> bool;

  /// Whether the frame is a message from a `psubscribe` call.
  fn is_pattern_pubsub_message(&self) -> bool;

  /// Whether the frame is a message from a `ssubscribe` call.
  fn is_shard_pubsub_message(&self) -> bool;

  /// Whether the frame is a `MOVED` or `ASK` redirection.
  fn is_redirection(&self) -> bool {
    self.as_str().map(utils::is_redirection).unwrap_or(false)
  }

  /// Convert the frame to a double.
  fn as_f64(&self) -> Option<f64> {
    self.as_str().and_then(|s| s.parse::<f64>().ok())
  }

  /// Convert the frame to another type.
  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn convert<T>(self) -> Result<T, RedisProtocolError>
  where
    Self: Sized,
    T: FromResp2<Self>,
  {
    T::from_frame(self)
  }

  /// Whether frame is a bulk array with a single element.
  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn is_single_element_vec(&self) -> bool;

  /// Pop an element from the inner array or return the original frame.
  ///
  /// This function is intended to be used with [Self::is_single_element_vec] and may panic.
  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn pop_or_take(self) -> Self;
}

/// A RESP2 frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnedFrame {
  /// A RESP2 simple string.
  SimpleString(Vec<u8>),
  /// A short string representing an error.
  Error(String),
  /// A signed 64-bit integer.
  Integer(i64),
  /// A byte array.
  BulkString(Vec<u8>),
  /// An array of frames,
  Array(Vec<OwnedFrame>),
  /// A null value.
  Null,
}

impl OwnedFrame {
  /// Move the frame contents into a new [BytesFrame].
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn into_bytes_frame(self) -> BytesFrame {
    match self {
      OwnedFrame::SimpleString(s) => BytesFrame::SimpleString(s.into()),
      OwnedFrame::Error(e) => BytesFrame::Error(e.into()),
      OwnedFrame::Integer(i) => BytesFrame::Integer(i),
      OwnedFrame::BulkString(b) => BytesFrame::BulkString(b.into()),
      OwnedFrame::Array(frames) => {
        BytesFrame::Array(frames.into_iter().map(|frame| frame.into_bytes_frame()).collect())
      },
      OwnedFrame::Null => BytesFrame::Null,
    }
  }

  /// Convert the frame to RESP3.
  ///
  /// For the most part RESP3 is a superset of RESP2, but there is one notable difference with publish-subscribe
  /// messages - in RESP3 pubsub arrays have an additional prefix frame with the SimpleString value "pubsub". This
  /// function will add this frame.
  pub fn into_resp3(self) -> Resp3OwnedFrame {
    let is_pubsub =
      self.is_normal_pubsub_message() || self.is_pattern_pubsub_message() || self.is_shard_pubsub_message();
    if is_pubsub {
      let prefix = Resp3OwnedFrame::SimpleString {
        data:       PUBSUB_PUSH_PREFIX.as_bytes().to_vec(),
        attributes: None,
      };
      let frames = match self {
        OwnedFrame::Array(inner) => {
          let mut buf = Vec::with_capacity(inner.len() + 1);
          buf.push(prefix);
          buf.extend(inner.into_iter().map(|f| f.into_resp3()));
          buf
        },
        _ => unreachable!(),
      };

      Resp3OwnedFrame::Push {
        data:       frames,
        attributes: None,
      }
    } else {
      match self {
        OwnedFrame::SimpleString(data) => Resp3OwnedFrame::SimpleString { data, attributes: None },
        OwnedFrame::Error(data) => Resp3OwnedFrame::SimpleError { data, attributes: None },
        OwnedFrame::Integer(data) => Resp3OwnedFrame::Number { data, attributes: None },
        OwnedFrame::BulkString(data) => Resp3OwnedFrame::BlobString { data, attributes: None },
        OwnedFrame::Array(frames) => Resp3OwnedFrame::Array {
          data:       frames.into_iter().map(|f| f.into_resp3()).collect(),
          attributes: None,
        },
        OwnedFrame::Null => Resp3OwnedFrame::Null,
      }
    }
  }
}

impl Hash for OwnedFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.kind().hash_prefix().hash(state);

    match self {
      OwnedFrame::SimpleString(b) | OwnedFrame::BulkString(b) => b.hash(state),
      OwnedFrame::Error(s) => s.hash(state),
      OwnedFrame::Integer(i) => i.hash(state),
      OwnedFrame::Array(f) => f.iter().for_each(|f| f.hash(state)),
      OwnedFrame::Null => NULL.hash(state),
    }
  }
}

impl Resp2Frame for OwnedFrame {
  fn encode_len(&self) -> usize {
    match self {
      OwnedFrame::BulkString(b) => bulkstring_encode_len(b),
      OwnedFrame::Array(frames) => frames
        .iter()
        .fold(1 + digits_in_number(frames.len()) + 2, |m, f| m + f.encode_len()),
      OwnedFrame::Null => NULL.as_bytes().len(),
      OwnedFrame::SimpleString(s) => simplestring_encode_len(s),
      OwnedFrame::Error(s) => error_encode_len(s),
      OwnedFrame::Integer(i) => integer_encode_len(*i),
    }
  }

  fn take(&mut self) -> OwnedFrame {
    mem::replace(self, OwnedFrame::Null)
  }

  fn kind(&self) -> FrameKind {
    match self {
      OwnedFrame::SimpleString(_) => FrameKind::SimpleString,
      OwnedFrame::Error(_) => FrameKind::Error,
      OwnedFrame::Integer(_) => FrameKind::Integer,
      OwnedFrame::BulkString(_) => FrameKind::BulkString,
      OwnedFrame::Array(_) => FrameKind::Array,
      OwnedFrame::Null => FrameKind::Null,
    }
  }

  fn as_str(&self) -> Option<&str> {
    match self {
      OwnedFrame::BulkString(b) => str::from_utf8(b).ok(),
      OwnedFrame::SimpleString(s) => str::from_utf8(s).ok(),
      OwnedFrame::Error(s) => Some(s),
      _ => None,
    }
  }

  fn as_bool(&self) -> Option<bool> {
    match self {
      OwnedFrame::BulkString(b) | OwnedFrame::SimpleString(b) => utils::bytes_to_bool(b),
      OwnedFrame::Integer(0) => Some(false),
      OwnedFrame::Integer(1) => Some(true),
      OwnedFrame::Null => Some(false),
      _ => None,
    }
  }

  fn as_bytes(&self) -> Option<&[u8]> {
    Some(match self {
      OwnedFrame::BulkString(b) => b,
      OwnedFrame::SimpleString(s) => s,
      OwnedFrame::Error(s) => s.as_bytes(),
      _ => return None,
    })
  }

  fn to_string(&self) -> Option<String> {
    match self {
      OwnedFrame::BulkString(b) | OwnedFrame::SimpleString(b) => String::from_utf8(b.to_vec()).ok(),
      OwnedFrame::Error(b) => Some(b.clone()),
      OwnedFrame::Integer(i) => Some(i.to_string()),
      _ => None,
    }
  }

  fn is_normal_pubsub_message(&self) -> bool {
    // format is ["message", <channel>, <message>]
    match self {
      OwnedFrame::Array(data) => {
        data.len() == 3
          && data[0].kind() == FrameKind::BulkString
          && data[0].as_str().map(|s| s == PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_pattern_pubsub_message(&self) -> bool {
    // format is ["pmessage", <pattern>, <channel>, <message>]
    match self {
      OwnedFrame::Array(data) => {
        data.len() == 4
          && data[0].kind() == FrameKind::BulkString
          && data[0].as_str().map(|s| s == PATTERN_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_shard_pubsub_message(&self) -> bool {
    // format is ["smessage", <channel>, <message>]
    match self {
      OwnedFrame::Array(data) => {
        data.len() == 3
          && data[0].kind() == FrameKind::BulkString
          && data[0].as_str().map(|s| s == SHARD_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn is_single_element_vec(&self) -> bool {
    if let OwnedFrame::Array(values) = self {
      values.len() == 1
    } else {
      false
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn pop_or_take(self) -> Self {
    if let OwnedFrame::Array(mut values) = self {
      values.pop().unwrap().pop_or_take()
    } else {
      self
    }
  }
}

/// A RESP2 frame that uses [Bytes](bytes::Bytes) and [Str](bytes_utils::Str) as the underlying buffer type.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BytesFrame {
  /// A RESP2 simple string.
  SimpleString(Bytes),
  /// A short string representing an error.
  Error(Str),
  /// A signed 64-bit integer.
  Integer(i64),
  /// A byte array.
  BulkString(Bytes),
  /// An array of frames.
  Array(Vec<BytesFrame>),
  /// A null value.
  Null,
}

#[cfg(feature = "bytes")]
impl Hash for BytesFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.kind().hash_prefix().hash(state);

    match self {
      BytesFrame::SimpleString(b) | BytesFrame::BulkString(b) => b.hash(state),
      BytesFrame::Error(s) => s.hash(state),
      BytesFrame::Integer(i) => i.hash(state),
      BytesFrame::Array(f) => f.iter().for_each(|f| f.hash(state)),
      BytesFrame::Null => NULL.hash(state),
    }
  }
}

#[cfg(feature = "bytes")]
impl Resp2Frame for BytesFrame {
  fn encode_len(&self) -> usize {
    match self {
      BytesFrame::BulkString(b) => bulkstring_encode_len(b),
      BytesFrame::Array(frames) => frames
        .iter()
        .fold(1 + digits_in_number(frames.len()) + 2, |m, f| m + f.encode_len()),
      BytesFrame::Null => NULL.as_bytes().len(),
      BytesFrame::SimpleString(s) => simplestring_encode_len(s),
      BytesFrame::Error(s) => error_encode_len(s),
      BytesFrame::Integer(i) => integer_encode_len(*i),
    }
  }

  fn take(&mut self) -> BytesFrame {
    mem::replace(self, BytesFrame::Null)
  }

  fn kind(&self) -> FrameKind {
    match self {
      BytesFrame::SimpleString(_) => FrameKind::SimpleString,
      BytesFrame::Error(_) => FrameKind::Error,
      BytesFrame::Integer(_) => FrameKind::Integer,
      BytesFrame::BulkString(_) => FrameKind::BulkString,
      BytesFrame::Array(_) => FrameKind::Array,
      BytesFrame::Null => FrameKind::Null,
    }
  }

  fn as_str(&self) -> Option<&str> {
    match self {
      BytesFrame::BulkString(b) => str::from_utf8(b).ok(),
      BytesFrame::SimpleString(s) => str::from_utf8(s).ok(),
      BytesFrame::Error(s) => Some(s),
      _ => None,
    }
  }

  fn as_bool(&self) -> Option<bool> {
    match self {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => utils::bytes_to_bool(b),
      BytesFrame::Integer(0) => Some(false),
      BytesFrame::Integer(1) => Some(true),
      BytesFrame::Null => Some(false),
      _ => None,
    }
  }

  fn as_bytes(&self) -> Option<&[u8]> {
    Some(match self {
      BytesFrame::BulkString(b) => b,
      BytesFrame::SimpleString(s) => s,
      BytesFrame::Error(s) => s.as_bytes(),
      _ => return None,
    })
  }

  fn to_string(&self) -> Option<String> {
    match self {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => String::from_utf8(b.to_vec()).ok(),
      BytesFrame::Error(b) => Some(b.to_string()),
      BytesFrame::Integer(i) => Some(i.to_string()),
      _ => None,
    }
  }

  fn is_normal_pubsub_message(&self) -> bool {
    // format is ["message", <channel>, <message>]
    match self {
      BytesFrame::Array(data) => {
        data.len() == 3
          && data[0].kind() == FrameKind::BulkString
          && data[0].as_str().map(|s| s == PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_pattern_pubsub_message(&self) -> bool {
    // format is ["pmessage", <pattern>, <channel>, <message>]
    match self {
      BytesFrame::Array(data) => {
        data.len() == 4
          && data[0].kind() == FrameKind::BulkString
          && data[0].as_str().map(|s| s == PATTERN_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_shard_pubsub_message(&self) -> bool {
    // format is ["smessage", <channel>, <message>]
    match self {
      BytesFrame::Array(data) => {
        data.len() == 3
          && data[0].kind() == FrameKind::BulkString
          && data[0].as_str().map(|s| s == SHARD_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn is_single_element_vec(&self) -> bool {
    if let BytesFrame::Array(values) = self {
      values.len() == 1
    } else {
      false
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn pop_or_take(self) -> Self {
    if let BytesFrame::Array(mut values) = self {
      values.pop().unwrap().pop_or_take()
    } else {
      self
    }
  }
}

#[cfg(feature = "bytes")]
impl BytesFrame {
  /// Copy the contents into a new [OwnedFrame].
  pub fn to_owned_frame(&self) -> OwnedFrame {
    match self {
      BytesFrame::SimpleString(s) => OwnedFrame::SimpleString(s.to_vec()),
      BytesFrame::Error(e) => OwnedFrame::Error(e.to_string()),
      BytesFrame::Integer(i) => OwnedFrame::Integer(*i),
      BytesFrame::BulkString(b) => OwnedFrame::BulkString(b.to_vec()),
      BytesFrame::Array(frames) => OwnedFrame::Array(frames.iter().map(|f| f.to_owned_frame()).collect()),
      BytesFrame::Null => OwnedFrame::Null,
    }
  }

  /// Convert the frame to RESP3.
  ///
  /// For the most part RESP3 is a superset of RESP2, but there is one notable difference with publish-subscribe
  /// messages - in RESP3 pubsub arrays have an additional prefix frame with the SimpleString value "pubsub". This
  /// function will add this frame.
  pub fn into_resp3(self) -> Resp3BytesFrame {
    let is_pubsub =
      self.is_normal_pubsub_message() || self.is_pattern_pubsub_message() || self.is_shard_pubsub_message();
    if is_pubsub {
      let prefix = Resp3BytesFrame::SimpleString {
        data:       Bytes::from_static(PUBSUB_PUSH_PREFIX.as_bytes()),
        attributes: None,
      };
      let frames = match self {
        BytesFrame::Array(inner) => {
          let mut buf = Vec::with_capacity(inner.len() + 1);
          buf.push(prefix);
          buf.extend(inner.into_iter().map(|f| f.into_resp3()));
          buf
        },
        _ => unreachable!(),
      };

      Resp3BytesFrame::Push {
        data:       frames,
        attributes: None,
      }
    } else {
      match self {
        BytesFrame::SimpleString(data) => Resp3BytesFrame::SimpleString { data, attributes: None },
        BytesFrame::Error(data) => Resp3BytesFrame::SimpleError { data, attributes: None },
        BytesFrame::Integer(data) => Resp3BytesFrame::Number { data, attributes: None },
        BytesFrame::BulkString(data) => Resp3BytesFrame::BlobString { data, attributes: None },
        BytesFrame::Array(frames) => Resp3BytesFrame::Array {
          data:       frames.into_iter().map(|f| f.into_resp3()).collect(),
          attributes: None,
        },
        BytesFrame::Null => Resp3BytesFrame::Null,
      }
    }
  }
}

#[cfg(feature = "bytes")]
impl<B: Into<Bytes>> TryFrom<(FrameKind, B)> for BytesFrame {
  type Error = RedisProtocolError;

  fn try_from((kind, buf): (FrameKind, B)) -> Result<Self, RedisProtocolError> {
    Ok(match kind {
      FrameKind::SimpleString => BytesFrame::SimpleString(buf.into()),
      FrameKind::Error => BytesFrame::Error(Str::from_inner(buf.into())?),
      FrameKind::BulkString => BytesFrame::BulkString(buf.into()),
      FrameKind::Null => BytesFrame::Null,
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Cannot convert to frame.",
        ))
      },
    })
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
mod tests {
  use super::*;

  #[test]
  fn should_parse_pattern_pubsub_message() {
    let frame = BytesFrame::Array(vec![
      BytesFrame::BulkString("pmessage".into()),
      BytesFrame::BulkString("fo*".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::BulkString("bar".into()),
    ]);
    assert!(frame.is_pattern_pubsub_message());
  }

  #[test]
  fn should_parse_normal_pubsub_message() {
    let frame = BytesFrame::Array(vec![
      BytesFrame::BulkString("message".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::BulkString("bar".into()),
    ]);
    assert!(frame.is_normal_pubsub_message());
  }

  #[test]
  fn should_parse_shard_pubsub_message() {
    let frame = BytesFrame::Array(vec![
      BytesFrame::BulkString("smessage".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::BulkString("bar".into()),
    ]);
    assert!(frame.is_shard_pubsub_message());
  }

  #[test]
  fn should_decode_frame_kind_byte() {
    assert_eq!(FrameKind::from_byte(SIMPLESTRING_BYTE), Some(FrameKind::SimpleString));
    assert_eq!(FrameKind::from_byte(ERROR_BYTE), Some(FrameKind::Error));
    assert_eq!(FrameKind::from_byte(BULKSTRING_BYTE), Some(FrameKind::BulkString));
    assert_eq!(FrameKind::from_byte(INTEGER_BYTE), Some(FrameKind::Integer));
    assert_eq!(FrameKind::from_byte(ARRAY_BYTE), Some(FrameKind::Array));
  }

  #[test]
  fn should_encode_frame_kind_byte() {
    assert_eq!(FrameKind::SimpleString.to_byte(), SIMPLESTRING_BYTE);
    assert_eq!(FrameKind::Error.to_byte(), ERROR_BYTE);
    assert_eq!(FrameKind::BulkString.to_byte(), BULKSTRING_BYTE);
    assert_eq!(FrameKind::Integer.to_byte(), INTEGER_BYTE);
    assert_eq!(FrameKind::Array.to_byte(), ARRAY_BYTE);
  }
}
