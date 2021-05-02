use crate::resp3::utils as resp3_utils;
use crate::utils;
use bytes::buf::Chain;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use float_cmp::approx_eq;
use resp3::utils::reconstruct_map;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::mem;
use std::str;
use types::{Redirection, RedisProtocolError, RedisProtocolErrorKind, CRLF};

/// Byte prefix before a simple string type.
pub const SIMPLE_STRING_BYTE: u8 = b'+';
/// Byte prefix before a simple error type.
pub const SIMPLE_ERROR_BYTE: u8 = b'-';
/// Byte prefix before a Number type.
pub const NUMBER_BYTE: u8 = b':';
/// Byte prefix before a Double type.
pub const DOUBLE_BYTE: u8 = b',';
/// Byte prefix before a blob string type.
pub const BLOB_STRING_BYTE: u8 = b'$';
/// Byte prefix before a blob error type.
pub const BLOB_ERROR_BYTE: u8 = b'!';
/// Byte prefix before a boolean type.
pub const BOOLEAN_BYTE: u8 = b'#';
/// Byte prefix before a verbatim string type.
pub const VERBATIM_STRING_BYTE: u8 = b'=';
/// Byte prefix before a big number type.
pub const BIG_NUMBER_BYTE: u8 = b'(';
/// Byte prefix before an array type.
pub const ARRAY_BYTE: u8 = b'*';
/// Byte prefix before a map type.
pub const MAP_BYTE: u8 = b'%';
/// Byte prefix before a set type.
pub const SET_BYTE: u8 = b'~';
/// Byte prefix before an attribute type.
pub const ATTRIBUTE_BYTE: u8 = b'|';
/// Byte prefix before a push type.
pub const PUSH_BYTE: u8 = b'>';
/// Byte prefix before a NULL value.
pub const NULL_BYTE: u8 = b'_';
/// Byte used to separate the verbatim string from the format prefix.
pub const VERBATIM_FORMAT_BYTE: u8 = b':';
/// Byte representation of a chunked string.
pub const CHUNKED_STRING_BYTE: u8 = b';';
/// Byte used to signify the end of a stream.
pub const END_STREAM_BYTE: u8 = b'.';

/// Byte prefix on a streamed type, following the byte that identifies the type.
pub const STREAMED_LENGTH_BYTE: u8 = b'?';
/// The terminating bytes when a streaming operation is done from a blob string.
pub const END_STREAM_STRING_BYTES: &'static str = ";0\r\n";
/// The terminating bytes when a streaming operation is done from an aggregate type.
pub const END_STREAM_AGGREGATE_BYTES: &'static str = ".\r\n";

/// The binary representation of NULL in RESP3.
pub const NULL: &'static str = "_\r\n";

/// Byte representation of positive infinity.
pub const INFINITY: &'static str = "inf";
/// Byte representation of negative infinity.
pub const NEG_INFINITY: &'static str = "-inf";

/// Byte representation of HELLO.
pub const HELLO: &'static str = "HELLO";
/// Byte representation of `true`.
pub const BOOL_TRUE_BYTES: &'static str = "#t\r\n";
/// Byte representation of `false`.
pub const BOOL_FALSE_BYTES: &'static str = "#f\r\n";
/// Byte representation of an empty space.
pub const EMPTY_SPACE: &'static str = " ";
/// Byte representation of `AUTH`.
pub const AUTH: &'static str = "AUTH";

/// Enum describing the byte ordering for numbers and doubles when cast to byte slices.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ByteOrder {
  BigEndian,
  LittleEndian,
}

/// The RESP version used in the `HELLO` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RespVersion {
  RESP2,
  RESP3,
}

impl RespVersion {
  pub fn to_byte(&self) -> u8 {
    match *self {
      RespVersion::RESP2 => b'2',
      RespVersion::RESP3 => b'3',
    }
  }
}

/// Authentication information used in the `HELLO` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Auth {
  pub username: Cow<'static, str>,
  pub password: Cow<'static, str>,
}

impl Auth {
  /// Create an [Auth] struct using the "default" user with the provided password.
  pub fn from_password<S: Into<String>>(password: S) -> Auth {
    Auth {
      username: Cow::Borrowed("default"),
      password: Cow::Owned(password.into()),
    }
  }
}

/// The format of a verbatim string frame.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum VerbatimStringFormat {
  Text,
  Markdown,
}

impl VerbatimStringFormat {
  pub fn to_str(&self) -> &'static str {
    match *self {
      VerbatimStringFormat::Text => "txt",
      VerbatimStringFormat::Markdown => "mkd",
    }
  }
}

/// The type of frame without any associated data.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Copy)]
pub enum FrameKind {
  Array,
  BlobString,
  SimpleString,
  SimpleError,
  Number,
  Null,
  Double,
  Boolean,
  BlobError,
  VerbatimString,
  Map,
  Set,
  Attribute,
  Push,
  Hello,
  BigNumber,
  ChunkedString,
  EndStream,
}

impl FrameKind {
  /// Whether or not the frame is an aggregate type (array, set, map).
  pub fn is_aggregate_type(&self) -> bool {
    match *self {
      FrameKind::Array | FrameKind::Set | FrameKind::Map => true,
      _ => false,
    }
  }

  /// A function used to differentiate data types that may have the same inner binary representation when hashing a `Frame`.
  pub fn hash_prefix(&self) -> &'static str {
    use self::FrameKind::*;

    match *self {
      Array => "a",
      BlobString => "b",
      SimpleString => "s",
      SimpleError => "e",
      Number => "n",
      Null => "N",
      Double => "d",
      Boolean => "B",
      BlobError => "E",
      VerbatimString => "v",
      Map => "m",
      Set => "S",
      Attribute => "A",
      Push => "p",
      Hello => "h",
      BigNumber => "bn",
      ChunkedString => "cs",
      EndStream => "es",
    }
  }

  /// Attempt to detect the type of the frame from the first byte.
  pub fn from_byte(d: u8) -> Option<FrameKind> {
    use self::FrameKind::*;

    match d {
      SIMPLE_STRING_BYTE => Some(SimpleString),
      SIMPLE_ERROR_BYTE => Some(SimpleError),
      NUMBER_BYTE => Some(Number),
      DOUBLE_BYTE => Some(Double),
      BLOB_STRING_BYTE => Some(BlobString),
      BLOB_ERROR_BYTE => Some(BlobError),
      BOOLEAN_BYTE => Some(Boolean),
      VERBATIM_STRING_BYTE => Some(VerbatimString),
      BIG_NUMBER_BYTE => Some(BigNumber),
      ARRAY_BYTE => Some(Array),
      MAP_BYTE => Some(Map),
      SET_BYTE => Some(Set),
      ATTRIBUTE_BYTE => Some(Attribute),
      PUSH_BYTE => Some(Push),
      NULL_BYTE => Some(Null),
      CHUNKED_STRING_BYTE => Some(ChunkedString),
      END_STREAM_BYTE => Some(EndStream),
      _ => None,
    }
  }

  /// Read the byte prefix for the associated frame type.
  pub fn to_byte(&self) -> u8 {
    use self::FrameKind::*;

    match *self {
      SimpleString => SIMPLE_STRING_BYTE,
      SimpleError => SIMPLE_ERROR_BYTE,
      Number => NUMBER_BYTE,
      Double => DOUBLE_BYTE,
      BlobString => BLOB_STRING_BYTE,
      BlobError => BLOB_ERROR_BYTE,
      Boolean => BOOLEAN_BYTE,
      VerbatimString => VERBATIM_STRING_BYTE,
      BigNumber => BIG_NUMBER_BYTE,
      Array => ARRAY_BYTE,
      Map => MAP_BYTE,
      Set => SET_BYTE,
      Attribute => ATTRIBUTE_BYTE,
      Push => PUSH_BYTE,
      Null => NULL_BYTE,
      ChunkedString => CHUNKED_STRING_BYTE,
      EndStream => END_STREAM_BYTE,
      Hello => panic!("HELLO does not have a byte prefix."),
    }
  }

  /// Whether or not the frame type is for a `HELLO` frame.
  ///
  /// `HELLO` is encoded differently than other frames so this is used to prevent panics in [to_byte](Self::to_byte).
  pub fn is_hello(&self) -> bool {
    match *self {
      FrameKind::Hello => true,
      _ => false,
    }
  }
}

/// An enum describing the possible data types in RESP3 along with the corresponding Rust data type to represent the payload.
///
/// <https://github.com/antirez/RESP3/blob/master/spec.md>
#[derive(Clone, Debug)]
pub enum Frame {
  /// An array of frames, arbitrarily nested.
  Array(Vec<Frame>),
  /// A binary-safe blob.
  BlobString(Vec<u8>),
  /// A small non binary-safe string.
  SimpleString(String),
  /// A small non binary-safe string representing an error.
  SimpleError(String),
  /// A signed 64 bit integer.
  Number(i64),
  /// A null type.
  Null,
  /// A boolean type.
  Boolean(bool),
  /// A signed 64 bit floating point number.
  Double(f64),
  /// A binary-safe blob representing an error.
  BlobError(Vec<u8>),
  /// A binary-safe string to be displayed without any escaping or filtering.
  VerbatimString { data: String, format: VerbatimStringFormat },
  /// An unordered map of key-value pairs.
  ///
  /// According to the spec keys can be any other RESP3 data type. However, it doesn't make sense to implement `Hash` for certain Rust data types like
  /// `HashMap`, `Vec`, `HashSet`, etc, so this library limits the possible data types for keys to only those that can be hashed in a semi-sane way.
  ///
  /// For example, attempting to create a `Frame::Map<HashMap<Frame::Set<HashSet<Frame>>, Frame::Foo>>` from bytes will panic.
  Map(HashMap<Frame, Frame>),
  /// An unordered collection of other frames with a uniqueness constraint.
  Set(HashSet<Frame>),
  /// Additional information to be returned when parsing other data types.
  Attribute(HashMap<Frame, Frame>),
  /// Out-of-band data to be returned to the caller if necessary.
  Push(Vec<Frame>),
  /// A special frame type used when first connecting to the server to describe the protocol version and optional credentials.
  Hello { version: RespVersion, auth: Option<Auth> },
  /// A large number not representable as a `Number` or `Double`.
  ///
  /// This library does not attempt to parse this, nor does it offer any utilities to do so.
  BigNumber(Vec<u8>),
  /// One chunk of a streaming string.
  ChunkedString(Vec<u8>),
}

impl Hash for Frame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    use self::Frame::*;
    self.kind().hash_prefix().hash(state);

    match *self {
      BlobString(ref b) => b.hash(state),
      SimpleString(ref s) => s.hash(state),
      SimpleError(ref s) => s.hash(state),
      Number(ref i) => i.hash(state),
      Null => NULL.hash(state),
      Double(ref f) => f.to_bits().hash(state),
      Boolean(ref b) => b.hash(state),
      BlobError(ref b) => b.hash(state),
      VerbatimString { ref data, ref format } => {
        format.hash(state);
        data.hash(state);
      }
      ChunkedString(ref b) => b.hash(state),
      BigNumber(ref b) => b.hash(state),
      _ => panic!("Invalid RESP3 data type to use as hash key."),
    };
  }
}

impl PartialEq for Frame {
  fn eq(&self, other: &Self) -> bool {
    use self::Frame::*;

    match *self {
      ChunkedString(ref b) => match *other {
        ChunkedString(ref _b) => b == _b,
        _ => false,
      },
      Array(ref b) => match *other {
        Array(ref _b) => b == _b,
        _ => false,
      },
      BlobString(ref b) => match *other {
        BlobString(ref _b) => b == _b,
        _ => false,
      },
      SimpleString(ref s) => match *other {
        SimpleString(ref _s) => s == _s,
        _ => false,
      },
      SimpleError(ref s) => match *other {
        SimpleError(ref _s) => s == _s,
        _ => false,
      },
      Number(ref i) => match *other {
        Number(ref _i) => i == _i,
        _ => false,
      },
      Null => match *other {
        Null => true,
        _ => false,
      },
      Boolean(ref b) => match *other {
        Boolean(ref _b) => b == _b,
        _ => false,
      },
      Double(ref f) => match *other {
        Double(ref _f) => approx_eq!(f64, *f, *_f),
        _ => false,
      },
      BlobError(ref b) => match *other {
        BlobError(ref _b) => b == _b,
        _ => false,
      },
      VerbatimString { ref data, ref format } => {
        let (_data, _format) = (data, format);
        match *other {
          VerbatimString { ref data, ref format } => _data == data && _format == format,
          _ => false,
        }
      }
      Map(ref m) => match *other {
        Map(ref _m) => m == _m,
        _ => false,
      },
      Set(ref s) => match *other {
        Set(ref _s) => s == _s,
        _ => false,
      },
      Attribute(ref m) => match *other {
        Attribute(ref _m) => m == _m,
        _ => false,
      },
      Push(ref a) => match *other {
        Push(ref _a) => a == _a,
        _ => false,
      },
      Hello { ref version, ref auth } => {
        let (_version, _auth) = (version, auth);
        match *other {
          Hello { ref version, ref auth } => _version == version && _auth == auth,
          _ => false,
        }
      }
      BigNumber(ref b) => match *other {
        BigNumber(ref _b) => b == _b,
        _ => false,
      },
    }
  }
}

impl Eq for Frame {}

impl Frame {
  /// Create a new `Frame` that terminates a stream.
  pub fn new_end_stream() -> Self {
    Frame::ChunkedString(vec![])
  }

  /// A context-aware length function that returns the length of the inner frame.
  pub fn len(&self) -> usize {
    use self::Frame::*;

    match *self {
      Array(ref a) | Push(ref a) => a.len(),
      BlobString(ref b) | BlobError(ref b) | BigNumber(ref b) | ChunkedString(ref b) => b.len(),
      SimpleString(ref s) | SimpleError(ref s) => s.len(),
      Number(_) | Double(_) | Boolean(_) => 1,
      Null => 0,
      VerbatimString { ref data, .. } => data.as_bytes().len(),
      Map(ref m) | Attribute(ref m) => m.len(),
      Set(ref s) => s.len(),
      Hello { .. } => 1,
    }
  }

  /// Replace `self` with Null, returning the original value.
  pub fn take(&mut self) -> Frame {
    mem::replace(self, Frame::Null)
  }

  /// Read the associated `FrameKind`.
  pub fn kind(&self) -> FrameKind {
    use self::Frame::*;

    match *self {
      Array(_) => FrameKind::Array,
      BlobString(_) => FrameKind::BlobString,
      SimpleString(_) => FrameKind::SimpleString,
      SimpleError(_) => FrameKind::SimpleError,
      Number(_) => FrameKind::Number,
      Null => FrameKind::Null,
      Double(_) => FrameKind::Double,
      BlobError(_) => FrameKind::BlobError,
      VerbatimString { .. } => FrameKind::VerbatimString,
      Boolean(_) => FrameKind::Boolean,
      Map(_) => FrameKind::Map,
      Set(_) => FrameKind::Set,
      Attribute(_) => FrameKind::Attribute,
      Push(_) => FrameKind::Push,
      Hello { .. } => FrameKind::Hello,
      BigNumber(_) => FrameKind::BigNumber,
      ChunkedString(_) => FrameKind::ChunkedString,
    }
  }

  /// Whether or not the frame is a boolean value.
  pub fn is_boolean(&self) -> bool {
    match *self {
      Frame::Boolean(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents an error.
  pub fn is_error(&self) -> bool {
    match *self {
      Frame::BlobError(_) | Frame::SimpleError(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame is an array, map, or set.
  pub fn is_aggregate_type(&self) -> bool {
    match *self {
      Frame::Map(_) | Frame::Set(_) | Frame::Array(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents a `BlobString` or `BlobError`.
  pub fn is_blob(&self) -> bool {
    match *self {
      Frame::BlobString(_) | Frame::BlobError(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents a chunked string.
  pub fn is_chunked_string(&self) -> bool {
    match *self {
      Frame::ChunkedString(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame is an empty chunked string, signifying the end of a chunked string stream.
  pub fn is_end_stream_frame(&self) -> bool {
    match *self {
      Frame::ChunkedString(ref s) => s.is_empty(),
      _ => false,
    }
  }

  /// Whether or not the frame is a verbatim string.
  pub fn is_verbatim_string(&self) -> bool {
    match *self {
      Frame::VerbatimString { .. } => true,
      _ => false,
    }
  }

  /// If the frame is a verbatim string then read the associated format.
  pub fn verbatim_string_format(&self) -> Option<&VerbatimStringFormat> {
    match *self {
      Frame::VerbatimString { ref format, .. } => Some(format),
      _ => None,
    }
  }

  /// Read the frame as a string slice if it can be parsed as a UTF-8 string without allocating.
  ///
  /// Numbers and Doubles will not be cast to a string since that would require allocating.
  pub fn as_str(&self) -> Option<&str> {
    match *self {
      Frame::SimpleError(ref s) | Frame::SimpleString(ref s) => Some(s),
      Frame::BlobError(ref b) | Frame::BlobString(ref b) | Frame::BigNumber(ref b) => str::from_utf8(b).ok(),
      Frame::VerbatimString { ref data, .. } => Some(data),
      Frame::ChunkedString(ref b) => str::from_utf8(b).ok(),
      _ => None,
    }
  }

  /// Read the frame as a `String` if it can be parsed as a UTF-8 string.
  pub fn to_string(&self) -> Option<String> {
    match *self {
      Frame::SimpleError(ref s) | Frame::SimpleString(ref s) => Some(s.to_owned()),
      Frame::BlobError(ref b) | Frame::BlobString(ref b) | Frame::BigNumber(ref b) => {
        String::from_utf8(b.to_vec()).ok()
      }
      Frame::VerbatimString { ref data, .. } => Some(data.to_owned()),
      Frame::ChunkedString(ref b) => String::from_utf8(b.to_vec()).ok(),
      Frame::Double(ref f) => Some(f.to_string()),
      Frame::Number(ref i) => Some(i.to_string()),
      _ => None,
    }
  }

  /// Attempt to read the frame as a byte slice.
  ///
  /// Number and Double will not be returned as a byte slice. Use [number_or_double_as_bytes](Self::number_or_double_as_bytes) instead.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match *self {
      Frame::SimpleError(ref s) | Frame::SimpleString(ref s) => Some(s.as_bytes()),
      Frame::BlobError(ref b) | Frame::BlobString(ref b) | Frame::BigNumber(ref b) => Some(b),
      Frame::VerbatimString { ref data, .. } => Some(data.as_bytes()),
      Frame::ChunkedString(ref b) => Some(b),
      _ => None,
    }
  }

  /// Attempt the read the frame as bytes if the inner type is an `i64` or `f64`.
  pub fn number_or_double_as_bytes(&self, ordering: ByteOrder) -> Option<[u8; 8]> {
    match *self {
      Frame::Double(ref f) => Some(match ordering {
        ByteOrder::BigEndian => f.to_be_bytes(),
        ByteOrder::LittleEndian => f.to_le_bytes(),
      }),
      Frame::Number(ref i) => Some(match ordering {
        ByteOrder::BigEndian => i.to_be_bytes(),
        ByteOrder::LittleEndian => i.to_le_bytes(),
      }),
      _ => None,
    }
  }

  /// Attempt to read the frame as an `i64` without casting.
  pub fn as_i64(&self) -> Option<i64> {
    match *self {
      Frame::Number(ref i) => Some(*i),
      _ => None,
    }
  }

  /// Attempt to read the frame as an `f64` without casting.
  pub fn as_f64(&self) -> Option<f64> {
    match *self {
      Frame::Double(ref f) => Some(*f),
      _ => None,
    }
  }

  /// Whether or not the frame represents a MOVED or ASK error.
  pub fn is_moved_or_ask_error(&self) -> bool {
    match *self {
      Frame::SimpleError(ref s) => utils::is_cluster_error(s),
      _ => false,
    }
  }

  /// Attempt to parse the frame as a cluster redirection error.
  pub fn to_redirection(&self) -> Option<Redirection> {
    match *self {
      Frame::SimpleError(ref s) => utils::read_cluster_error(s),
      _ => None,
    }
  }

  /// Whether or not the frame is a Push frame.
  pub fn is_push(&self) -> bool {
    match *self {
      Frame::Push(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame is an attribute map frame.
  pub fn is_attribute(&self) -> bool {
    match *self {
      Frame::Attribute(_) => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents a publish-subscribe message, but not a pattern publish-subscribe message.
  pub fn is_normal_pubsub(&self) -> bool {
    if let Frame::Push(ref frames) = *self {
      unimplemented!()
    } else {
      false
    }
  }

  /// Whether or not the frame represents a message on a publish-subscribe channel.
  pub fn is_pubsub_message(&self) -> bool {
    if let Frame::Push(ref frames) = *self {
      unimplemented!()
    } else {
      false
    }
  }

  /// Whether or not the frame represents a message on a publish-subscribe channel matched against a pattern subscription.
  pub fn is_pattern_pubsub_message(&self) -> bool {
    if let Frame::Push(ref frames) = *self {
      unimplemented!()
    } else {
      false
    }
  }

  /// Attempt to parse the frame as a publish-subscribe message, returning the `(channel, message)` tuple
  /// if successful, or the original frame if the inner data is not a publish-subscribe message.
  pub fn parse_as_pubsub(self) -> Result<(String, String), Self> {
    if self.is_pubsub_message() {
      if let Frame::Push(ref frames) = self {
        unimplemented!()
      } else {
        warn!("Invalid pubsub frame. Expected a Push frame.");
        Err(self)
      }
    } else {
      Err(self)
    }
  }
}

/// A helper struct for reading in streams of bytes such that the underlying bytes don't need to be in one continuous, growing block of memory.
#[derive(Debug)]
pub struct StreamedFrame {
  /// The internal buffer of frames.
  buffer: VecDeque<Frame>,
  /// The type of frame to which this will eventually be cast.
  pub kind: FrameKind,
}

impl StreamedFrame {
  /// Create a new `StreamedFrame` from the first section of data in a streaming response.
  pub fn new(kind: FrameKind) -> Self {
    let buffer = VecDeque::new();
    StreamedFrame { buffer, kind }
  }

  /// Convert the internal buffer into one frame matching `self.kind`.
  pub fn into_frame(mut self) -> Result<Frame, RedisProtocolError> {
    if self.is_finished() {
      // the last frame is an empty chunked string when the stream is finished
      let _ = self.buffer.pop_back();
    }

    match self.kind {
      FrameKind::ChunkedString => resp3_utils::reconstruct_blobstring(self.buffer),
      FrameKind::Map => resp3_utils::reconstruct_map(self.buffer),
      FrameKind::Set => resp3_utils::reconstruct_set(self.buffer),
      FrameKind::Array => resp3_utils::reconstruct_array(self.buffer),
      _ => Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Streaming frames only supported for chunked strings, maps, sets, and arrays.",
      )),
    }
  }

  /// Add a frame to the internal buffer.
  pub fn add_frame(&mut self, data: Frame) {
    self.buffer.push_back(data);
  }

  /// Whether or not the last frame represents the terminating sequence at the end of a frame stream.
  pub fn is_finished(&self) -> bool {
    self.buffer.back().map(|f| f.is_end_stream_frame()).unwrap_or(false)
  }
}

/// Wrapper enum around a decoded frame that supports streaming frames.
#[derive(Debug)]
pub enum DecodedFrame {
  Streaming(StreamedFrame),
  Complete(Frame),
}

impl DecodedFrame {
  /// Convert the decoded frame to a complete frame, returning an error if a streaming variant is found.
  pub fn into_complete_frame(self) -> Result<Frame, RedisProtocolError> {
    match self {
      DecodedFrame::Complete(frame) => Ok(frame),
      DecodedFrame::Streaming(_) => Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected complete frame.",
      )),
    }
  }

  /// Whether or not the frame is a complete frame representing a map of attributes.
  pub fn is_attribute(&self) -> bool {
    match *self {
      DecodedFrame::Complete(ref frame) => frame.is_attribute(),
      _ => false,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::str;

  /*
  #[test]
  fn should_convert_basic_streaming_buffer_to_frame() {
    let mut streaming_buf = StreamedFrame::new(FrameKind::BlobString, "foo".as_bytes(), 4);
    streaming_buf.add_bytes("bar".as_bytes());
    streaming_buf.add_bytes("baz".as_bytes());
    streaming_buf.add_crlf();
    let final_buffer = streaming_buf.into_bytes();

    assert_eq!(final_buffer.len(), 12);
    assert_eq!(final_buffer.capacity(), 14);
    assert_eq!(
      str::from_utf8(&final_buffer).unwrap(),
      &format!("{}{}", BLOB_STRING_BYTE as char, "foobarbaz\r\n")
    );
  }
   */
}
