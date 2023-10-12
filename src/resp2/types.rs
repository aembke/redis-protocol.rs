use crate::resp2::utils as resp2_utils;
use crate::types::{Redirection, RedisProtocolError};
use crate::utils;
use alloc::string::String;
use alloc::vec::Vec;
use bytes::Bytes;
use bytes_utils::Str;
use core::mem;
use core::str;

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

/// An enum representing the kind of a Frame without references to any inner data.
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

/// An enum representing a Frame of data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
  /// A short non binary-safe string.
  SimpleString(Bytes),
  /// A short non binary-safe string representing an error.
  Error(Str),
  /// A signed 64 bit integer.
  Integer(i64),
  /// A binary-safe string.
  BulkString(Bytes),
  /// An array of frames, arbitrarily nested.
  Array(Vec<Frame>),
  /// A null value.
  Null,
}

impl Frame {
  /// Replace `self` with Null, returning the original value.
  pub fn take(&mut self) -> Frame {
    mem::replace(self, Frame::Null)
  }

  /// Whether or not the frame is an error.
  pub fn is_error(&self) -> bool {
    match self.kind() {
      FrameKind::Error => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents a publish-subscribe message, but not a pattern publish-subscribe message.
  pub fn is_normal_pubsub(&self) -> bool {
    if let Frame::Array(ref frames) = *self {
      resp2_utils::is_normal_pubsub(frames)
    } else {
      false
    }
  }

  /// Whether or not the frame represents a message on a publish-subscribe channel.
  pub fn is_pubsub_message(&self) -> bool {
    if let Frame::Array(ref frames) = *self {
      resp2_utils::is_normal_pubsub(frames) || resp2_utils::is_pattern_pubsub(frames)
    } else {
      false
    }
  }

  /// Whether or not the frame represents a message on a publish-subscribe channel matched against a pattern subscription.
  pub fn is_pattern_pubsub_message(&self) -> bool {
    if let Frame::Array(ref frames) = *self {
      resp2_utils::is_pattern_pubsub(frames)
    } else {
      false
    }
  }

  /// Read the `FrameKind` value for this frame.
  pub fn kind(&self) -> FrameKind {
    match *self {
      Frame::SimpleString(_) => FrameKind::SimpleString,
      Frame::Error(_) => FrameKind::Error,
      Frame::Integer(_) => FrameKind::Integer,
      Frame::BulkString(_) => FrameKind::BulkString,
      Frame::Array(_) => FrameKind::Array,
      Frame::Null => FrameKind::Null,
    }
  }

  /// Attempt to read the frame value as a string slice without allocating.
  pub fn as_str(&self) -> Option<&str> {
    match *self {
      Frame::BulkString(ref b) => str::from_utf8(b).ok(),
      Frame::SimpleString(ref s) => str::from_utf8(s).ok(),
      Frame::Error(ref s) => Some(s),
      _ => None,
    }
  }

  /// Whether or not the frame is a simple string or bulk string.
  pub fn is_string(&self) -> bool {
    match *self {
      Frame::SimpleString(_) | Frame::BulkString(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame is Null.
  pub fn is_null(&self) -> bool {
    match *self {
      Frame::Null => true,
      _ => false,
    }
  }

  /// Whether or not the frame is an array of frames.
  pub fn is_array(&self) -> bool {
    match *self {
      Frame::Array(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame is an integer.
  pub fn is_integer(&self) -> bool {
    match *self {
      Frame::Integer(_) => true,
      _ => false,
    }
  }

  /// Whether or not the framed is a a Moved or Ask error.
  pub fn is_moved_or_ask_error(&self) -> bool {
    match *self {
      Frame::Error(ref s) => utils::is_cluster_error(s),
      _ => false,
    }
  }

  // Copy and read the inner value as a string, if possible.
  pub fn to_string(&self) -> Option<String> {
    match *self {
      Frame::BulkString(ref b) | Frame::SimpleString(ref b) => String::from_utf8(b.to_vec()).ok(),
      _ => None,
    }
  }

  /// Attempt to parse the frame as a publish-subscribe message, returning the `(channel, message)` tuple
  /// if successful, or the original frame if the inner data is not a publish-subscribe message.
  pub fn parse_as_pubsub(self) -> Result<(String, String), Self> {
    if self.is_pubsub_message() {
      // if `is_pubsub_message` returns true but this panics then there's a bug in `is_pubsub_message`, so this fails loudly
      let (message, channel, _) = match self {
        Frame::Array(mut frames) => (
          resp2_utils::opt_frame_to_string_panic(frames.pop(), "Expected pubsub payload. This is a bug."),
          resp2_utils::opt_frame_to_string_panic(frames.pop(), "Expected pubsub channel. This is a bug."),
          resp2_utils::opt_frame_to_string_panic(frames.pop(), "Expected pubsub message kind. This is a bug."),
        ),
        _ => panic!("Unreachable 1. This is a bug."),
      };

      Ok((channel, message))
    } else {
      Err(self)
    }
  }

  /// Attempt to parse the frame as a cluster redirection.
  pub fn to_redirection(&self) -> Option<Redirection> {
    match *self {
      Frame::Error(ref s) => utils::read_cluster_error(s),
      _ => None,
    }
  }

  /// Attempt to read the number of bytes needed to encode this frame.
  pub fn encode_len(&self) -> Result<usize, RedisProtocolError> {
    resp2_utils::encode_len(self).map_err(|e| e.into())
  }
}

impl From<Redirection> for Frame {
  fn from(redirection: Redirection) -> Self {
    redirection.to_resp2_frame()
  }
}

impl<'a> From<&'a Redirection> for Frame {
  fn from(redirection: &'a Redirection) -> Self {
    redirection.to_resp2_frame()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_convert_ask_redirection_to_frame() {
    let redirection = Redirection::Ask {
      slot: 3999,
      server: "127.0.0.1:6381".into(),
    };
    let frame = Frame::Error("ASK 3999 127.0.0.1:6381".into());

    assert_eq!(Frame::from(redirection), frame);
  }

  #[test]
  fn should_convert_moved_redirection_to_frame() {
    let redirection = Redirection::Moved {
      slot: 3999,
      server: "127.0.0.1:6381".into(),
    };
    let frame = Frame::Error("MOVED 3999 127.0.0.1:6381".into());

    assert_eq!(Frame::from(redirection), frame);
  }

  #[test]
  fn should_convert_frame_to_redirection_moved() {
    let redirection = Redirection::Moved {
      slot: 3999,
      server: "127.0.0.1:6381".into(),
    };
    let frame = Frame::Error("MOVED 3999 127.0.0.1:6381".into());

    assert_eq!(frame.to_redirection().unwrap(), redirection);
  }

  #[test]
  fn should_convert_frame_to_redirection_ask() {
    let redirection = Redirection::Ask {
      slot: 3999,
      server: "127.0.0.1:6381".into(),
    };
    let frame = Frame::Error("ASK 3999 127.0.0.1:6381".into());

    assert_eq!(frame.to_redirection().unwrap(), redirection);
  }

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error() {
    let redirection = Redirection::Ask {
      slot: 3999,
      server: "127.0.0.1:6381".into(),
    };
    let frame = Frame::BulkString("ASK 3999 127.0.0.1:6381".into());

    assert_eq!(frame.to_redirection().unwrap(), redirection);
  }

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error_invalid_1() {
    let f1 = Frame::Error("abc def".into());
    let _ = f1.to_redirection().unwrap();
  }

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error_invalid_2() {
    let f2 = Frame::Error("abc def ghi".into());
    let _ = f2.to_redirection().unwrap();
  }

  #[test]
  #[should_panic]
  fn should_convert_frame_to_redirection_error_invalid_3() {
    let f3 = Frame::Error("MOVED abc def".into());
    let _ = f3.to_redirection().unwrap();
  }

  #[test]
  fn should_parse_pattern_pubsub_message() {
    let frames = vec![
      Frame::BulkString("pmessage".into()),
      Frame::BulkString("fo*".into()),
      Frame::BulkString("foo".into()),
      Frame::BulkString("bar".into()),
    ];
    assert!(resp2_utils::is_pattern_pubsub(&frames));
    let frame = Frame::Array(frames);

    let (channel, message) = frame.parse_as_pubsub().expect("Expected pubsub frames");

    assert_eq!(channel, "foo");
    assert_eq!(message, "bar");
  }

  #[test]
  fn should_parse_pubsub_message() {
    let frames = vec![
      Frame::BulkString("message".into()),
      Frame::BulkString("foo".into()),
      Frame::BulkString("bar".into()),
    ];
    assert!(!resp2_utils::is_pattern_pubsub(&frames));
    let frame = Frame::Array(frames);

    let (channel, message) = frame.parse_as_pubsub().expect("Expected pubsub frames");

    assert_eq!(channel, "foo");
    assert_eq!(message, "bar");
  }

  #[test]
  #[should_panic]
  fn should_fail_parsing_non_pubsub_message() {
    let frame = Frame::Array(vec![Frame::BulkString("baz".into()), Frame::BulkString("foo".into())]);

    frame.parse_as_pubsub().expect("Expected non pubsub frames");
  }

  #[test]
  fn should_check_frame_types() {
    let f = Frame::Null;
    assert!(f.is_null());
    assert!(!f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = Frame::BulkString("foo".as_bytes().into());
    assert!(!f.is_null());
    assert!(f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = Frame::SimpleString("foo".into());
    assert!(!f.is_null());
    assert!(f.is_string());
    assert!(!f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = Frame::Error("foo".into());
    assert!(!f.is_null());
    assert!(!f.is_string());
    assert!(f.is_error());
    assert!(!f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = Frame::Array(vec![Frame::SimpleString("foo".into())]);
    assert!(!f.is_null());
    assert!(!f.is_string());
    assert!(!f.is_error());
    assert!(f.is_array());
    assert!(!f.is_integer());
    assert!(!f.is_moved_or_ask_error());

    let f = Frame::Integer(10);
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
