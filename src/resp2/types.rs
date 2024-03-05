use crate::{
  digits_in_number,
  error::{RedisProtocolError, RedisProtocolErrorKind},
  resp2::{
    utils as resp2_utils,
    utils::{bulkstring_encode_len, error_encode_len, integer_encode_len, simplestring_encode_len},
  },
  types::_Range,
  utils,
};
use alloc::{string::String, vec::Vec};
use bytes::{Bytes, BytesMut};
use bytes_utils::Str;
use core::{mem, str};
use nom::AsBytes;

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
pub const NULL: &'static str = "$-1\r\n";

pub use crate::utils::{PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX};

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
  /// Move the frame contents into a new [BytesFrame](crate::resp2::types::BytesFrame).
  pub fn into_bytes_frame(self) -> BytesFrame {
    match self {
      OwnedFrame::SimpleString(s) => BytesFrame::SimpleString(s.into()),
      OwnedFrame::Error(e) => BytesFrame::Error(e.into()),
      OwnedFrame::Integer(i) => BytesFrame::Integer(i),
      OwnedFrame::BulkString(b) => BytesFrame::BulkString(b.into()),
      OwnedFrame::Array(frames) => {
        BytesFrame::Array(frames.into_iter().map(|frame| frame.to_bytes_frame()).collect())
      },
      OwnedFrame::Null => BytesFrame::Null,
    }
  }

  /// Read the number of bytes needed to encode this frame.
  pub fn encode_len(&self) -> usize {
    match self {
      OwnedFrame::BulkString(b) => bulkstring_encode_len(&b),
      OwnedFrame::Array(frames) => frames
        .iter()
        .fold(1 + digits_in_number(frames.len()) + 2, |m, f| m + f.encode_len()),
      OwnedFrame::Null => NULL.as_bytes().len(),
      OwnedFrame::SimpleString(s) => simplestring_encode_len(s),
      OwnedFrame::Error(s) => error_encode_len(s),
      OwnedFrame::Integer(i) => integer_encode_len(*i),
    }
  }

  /// Replace `self` with Null, returning the original value.
  pub fn take(&mut self) -> OwnedFrame {
    mem::replace(self, OwnedFrame::Null)
  }

  /// Whether the frame is an error.
  pub fn is_error(&self) -> bool {
    match self.kind() {
      FrameKind::Error => true,
      _ => false,
    }
  }

  /// Read the `FrameKind` value for this frame.
  pub fn kind(&self) -> FrameKind {
    match self {
      OwnedFrame::SimpleString(_) => FrameKind::SimpleString,
      OwnedFrame::Error(_) => FrameKind::Error,
      OwnedFrame::Integer(_) => FrameKind::Integer,
      OwnedFrame::BulkString(_) => FrameKind::BulkString,
      OwnedFrame::Array(_) => FrameKind::Array,
      OwnedFrame::Null => FrameKind::Null,
    }
  }

  /// Attempt to read the frame value as a string slice.
  pub fn as_str(&self) -> Option<&str> {
    match self {
      OwnedFrame::BulkString(b) => str::from_utf8(b).ok(),
      OwnedFrame::SimpleString(s) => str::from_utf8(s).ok(),
      OwnedFrame::Error(s) => Some(s),
      _ => None,
    }
  }

  /// Attempt to read the frame value as a byte slice.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    Some(match self {
      OwnedFrame::BulkString(b) => b,
      OwnedFrame::SimpleString(s) => s,
      OwnedFrame::Error(s) => s.as_bytes(),
      _ => return None,
    })
  }

  /// Whether the framed is a Moved or Ask error.
  pub fn is_moved_or_ask_error(&self) -> bool {
    match self {
      OwnedFrame::Error(s) => utils::is_cluster_error(s),
      _ => false,
    }
  }

  /// Copy and read the inner value as a string, if possible.
  pub fn to_string(&self) -> Option<String> {
    match self {
      OwnedFrame::BulkString(b) | OwnedFrame::SimpleString(b) => String::from_utf8(b.to_vec()).ok(),
      OwnedFrame::Error(b) => Some(b.clone()),
      _ => None,
    }
  }
}

/// A RESP2 frame that uses [Bytes](bytes::Bytes) and [Str](bytes_utils::Str) as the backing types for dynamically
/// sized frame types.
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

impl BytesFrame {
  /// Copy the contents into a new [OwnedFrame](crate::resp2::types::OwnedFrame).
  pub fn to_owned_frame(&self) -> OwnedFrame {
    match self {
      BytesFrame::SimpleString(s) => OwnedFrame::SimpleString(s.to_vec()),
      BytesFrame::Error(e) => OwnedFrame::Error((&*e).to_string()),
      BytesFrame::Integer(i) => OwnedFrame::Integer(*i),
      BytesFrame::BulkString(b) => OwnedFrame::BulkString(b.to_vec()),
      BytesFrame::Array(frames) => {
        OwnedFrame::Array(frames.into_iter().map(|frame| frame.to_owned_frame()).collect())
      },
      BytesFrame::Null => OwnedFrame::Null,
    }
  }

  /// Replace `self` with Null, returning the original value.
  pub fn take(&mut self) -> BytesFrame {
    mem::replace(self, BytesFrame::Null)
  }

  /// Whether the frame is an error.
  pub fn is_error(&self) -> bool {
    match self.kind() {
      FrameKind::Error => true,
      _ => false,
    }
  }

  /// Read the `FrameKind` value for this frame.
  pub fn kind(&self) -> FrameKind {
    match self {
      BytesFrame::SimpleString(_) => FrameKind::SimpleString,
      BytesFrame::Error(_) => FrameKind::Error,
      BytesFrame::Integer(_) => FrameKind::Integer,
      BytesFrame::BulkString(_) => FrameKind::BulkString,
      BytesFrame::Array(_) => FrameKind::Array,
      BytesFrame::Null => FrameKind::Null,
    }
  }

  /// Attempt to read the frame value as a string slice.
  pub fn as_str(&self) -> Option<&str> {
    match self {
      BytesFrame::BulkString(b) => str::from_utf8(b).ok(),
      BytesFrame::SimpleString(s) => str::from_utf8(s).ok(),
      BytesFrame::Error(s) => Some(s),
      _ => None,
    }
  }

  /// Attempt to read the frame value as a byte slice.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    Some(match self {
      BytesFrame::BulkString(b) => b,
      BytesFrame::SimpleString(s) => s,
      BytesFrame::Error(s) => s.as_bytes(),
      _ => return None,
    })
  }

  /// Whether the framed is a Moved or Ask error.
  pub fn is_moved_or_ask_error(&self) -> bool {
    match self {
      BytesFrame::Error(s) => utils::is_cluster_error(s),
      _ => false,
    }
  }

  /// Copy and read the inner value as a string, if possible.
  pub fn to_string(&self) -> Option<String> {
    match self {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => String::from_utf8(b.to_vec()).ok(),
      BytesFrame::Error(s) => Some(s.to_string()),
      _ => None,
    }
  }

  /// Read the number of bytes needed to encode this frame.
  pub fn encode_len(&self) -> usize {
    match self {
      BytesFrame::BulkString(b) => bulkstring_encode_len(&b),
      BytesFrame::Array(frames) => frames
        .iter()
        .fold(1 + digits_in_number(frames.len()) + 2, |m, f| m + f.encode_len()),
      BytesFrame::Null => NULL.as_bytes().len(),
      BytesFrame::SimpleString(s) => simplestring_encode_len(s),
      BytesFrame::Error(s) => error_encode_len(&*s),
      BytesFrame::Integer(i) => integer_encode_len(*i),
    }
  }
}

/// Convenience wrapper type for the various frame representations.
pub enum BorrowedFrame<'a> {
  Owned(&'a OwnedFrame),
  Bytes(&'a BytesFrame),
}

impl<'a> From<&'a OwnedFrame> for BorrowedFrame<'a> {
  fn from(value: &'a OwnedFrame) -> Self {
    BorrowedFrame::Owned(value)
  }
}

impl<'a> From<&'a BytesFrame> for BorrowedFrame<'a> {
  fn from(value: &'a BytesFrame) -> Self {
    BorrowedFrame::Bytes(value)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error_invalid_1() {
    let f1 = BytesFrame::Error("abc def".into());
    let _ = f1.to_redirection().unwrap();
  }

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error_invalid_2() {
    let f2 = BytesFrame::Error("abc def ghi".into());
    let _ = f2.to_redirection().unwrap();
  }

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error_invalid_3() {
    let f3 = BytesFrame::Error("MOVED abc def".into());
    let _ = f3.to_redirection().unwrap();
  }

  #[test]
  fn should_parse_pattern_pubsub_message() {
    let frames = vec![
      BytesFrame::BulkString("pmessage".into()),
      BytesFrame::BulkString("fo*".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::BulkString("bar".into()),
    ];
    assert!(resp2_utils::is_pattern_pubsub(&frames));
    let frame = BytesFrame::Array(frames);

    let (channel, message) = frame.parse_as_pubsub().expect("Expected pubsub frames");

    assert_eq!(channel, "foo");
    assert_eq!(message, "bar");
  }

  #[test]
  fn should_parse_pubsub_message() {
    let frames = vec![
      BytesFrame::BulkString("message".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::BulkString("bar".into()),
    ];
    assert!(!resp2_utils::is_pattern_pubsub(&frames));
    let frame = BytesFrame::Array(frames);

    let (channel, message) = frame.parse_as_pubsub().expect("Expected pubsub frames");

    assert_eq!(channel, "foo");
    assert_eq!(message, "bar");
  }

  #[test]
  #[should_panic]
  fn should_fail_parsing_non_pubsub_message() {
    let frame = BytesFrame::Array(vec![
      BytesFrame::BulkString("baz".into()),
      BytesFrame::BulkString("foo".into()),
    ]);

    frame.parse_as_pubsub().expect("Expected non pubsub frames");
  }

  #[test]
  fn should_check_frame_types() {
    let f = BytesFrame::Null;
    assert!(f.is_null());
    assert!(!f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = BytesFrame::BulkString("foo".as_bytes().into());
    assert!(!f.is_null());
    assert!(f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = BytesFrame::SimpleString("foo".into());
    assert!(!f.is_null());
    assert!(f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = BytesFrame::Error("foo".into());
    assert!(!f.is_null());
    assert!(!f.is_string());
    assert!(f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = BytesFrame::Array(vec![BytesFrame::SimpleString("foo".into())]);
    assert!(!f.is_null());
    assert!(!f.is_string());
    assert!(!f.is_error());
    assert!(f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = BytesFrame::Integer(10);
    assert!(!f.is_null());
    assert!(!f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(f.is_integer());
    assert!(!f.is_moved_or_ask_error());
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
