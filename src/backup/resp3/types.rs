use crate::resp3::utils as resp3_utils;
use crate::types::{Redirection, RedisProtocolError, RedisProtocolErrorKind};
use crate::utils;
use alloc::collections::VecDeque;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use bytes::Bytes;
use bytes_utils::Str;
use core::convert::TryFrom;
use core::hash::{Hash, Hasher};
use core::mem;
use core::str;

#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};

#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};

#[cfg(test)]
use std::convert::TryInto;

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

pub use crate::utils::{PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX, PUBSUB_PUSH_PREFIX};

/// A map struct for frames.
#[cfg(not(feature = "index-map"))]
pub type FrameMap = HashMap<Frame, Frame>;
/// A set struct for frames.
#[cfg(not(feature = "index-map"))]
pub type FrameSet = HashSet<Frame>;
/// A map struct for frames.
#[cfg(feature = "index-map")]
pub type FrameMap = IndexMap<Frame, Frame>;
/// A set struct for frames.
#[cfg(feature = "index-map")]
pub type FrameSet = IndexSet<Frame>;

/// Additional information returned alongside a frame.
pub type Attributes = FrameMap;

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
  pub username: Str,
  pub password: Str,
}

impl Auth {
  /// Create an [Auth] struct using the "default" user with the provided password.
  pub fn from_password<S: Into<Str>>(password: S) -> Auth {
    Auth {
      username: Str::from("default"),
      password: password.into(),
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
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      VerbatimStringFormat::Text => "txt",
      VerbatimStringFormat::Markdown => "mkd",
    }
  }

  pub(crate) fn encode_len(&self) -> usize {
    match *self {
      // the trailing colon is included here
      VerbatimStringFormat::Text => 4,
      VerbatimStringFormat::Markdown => 4,
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

  /// Whether or not the frame is an aggregate type or blob string.
  pub fn is_streaming_type(&self) -> bool {
    match *self {
      FrameKind::Array | FrameKind::Set | FrameKind::Map | FrameKind::BlobString => true,
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
  /// A binary-safe blob.
  BlobString {
    data: Bytes,
    attributes: Option<Attributes>,
  },
  /// A binary-safe blob representing an error.
  BlobError {
    data: Bytes,
    attributes: Option<Attributes>,
  },
  /// A small non binary-safe string.
  ///
  /// The internal data type is `Bytes` in order to support callers that use this interface to parse a `MONITOR` stream.
  SimpleString {
    data: Bytes,
    attributes: Option<Attributes>,
  },
  /// A small non binary-safe string representing an error.
  SimpleError { data: Str, attributes: Option<Attributes> },
  /// A boolean type.
  Boolean { data: bool, attributes: Option<Attributes> },
  /// A null type.
  Null,
  /// A signed 64 bit integer.
  Number { data: i64, attributes: Option<Attributes> },
  /// A signed 64 bit floating point number.
  Double { data: f64, attributes: Option<Attributes> },
  /// A large number not representable as a `Number` or `Double`.
  ///
  /// This library does not attempt to parse this, nor does it offer any utilities to do so.
  BigNumber {
    data: Bytes,
    attributes: Option<Attributes>,
  },
  /// A binary-safe string to be displayed without any escaping or filtering.
  VerbatimString {
    data: Bytes,
    format: VerbatimStringFormat,
    attributes: Option<Attributes>,
  },
  /// An array of frames, arbitrarily nested.
  Array {
    data: Vec<Frame>,
    attributes: Option<Attributes>,
  },
  /// An unordered map of key-value pairs.
  ///
  /// According to the spec keys can be any other RESP3 data type. However, it doesn't make sense to implement `Hash` for certain Rust data types like
  /// `HashMap`, `Vec`, `HashSet`, etc, so this library limits the possible data types for keys to only those that can be hashed in a semi-sane way.
  ///
  /// For example, attempting to create a `Frame::Map<HashMap<Frame::Set<HashSet<Frame>>, Frame::Foo>>` from bytes will panic.
  Map {
    data: FrameMap,
    attributes: Option<Attributes>,
  },
  /// An unordered collection of other frames with a uniqueness constraint.
  Set {
    data: FrameSet,
    attributes: Option<Attributes>,
  },
  /// Out-of-band data to be returned to the caller if necessary.
  Push {
    data: Vec<Frame>,
    attributes: Option<Attributes>,
  },
  /// A special frame type used when first connecting to the server to describe the protocol version and optional credentials.
  Hello { version: RespVersion, auth: Option<Auth> },
  /// One chunk of a streaming string.
  ChunkedString(Bytes),
}

impl Hash for Frame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    use self::Frame::*;
    self.kind().hash_prefix().hash(state);

    match *self {
      BlobString { ref data, .. } => data.hash(state),
      SimpleString { ref data, .. } => data.hash(state),
      SimpleError { ref data, .. } => data.hash(state),
      Number { ref data, .. } => data.hash(state),
      Null => NULL.hash(state),
      Double { ref data, .. } => data.to_string().hash(state),
      Boolean { ref data, .. } => data.hash(state),
      BlobError { ref data, .. } => data.hash(state),
      VerbatimString {
        ref data, ref format, ..
      } => {
        format.hash(state);
        data.hash(state);
      }
      ChunkedString(ref data) => data.hash(state),
      BigNumber { ref data, .. } => data.hash(state),
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
      Array {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Array {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      BlobString {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          BlobString {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      SimpleString {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          SimpleString {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      SimpleError {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          SimpleError {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Number {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Number {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Null => match *other {
        Null => true,
        _ => false,
      },
      Boolean {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Boolean {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Double {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Double {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      BlobError {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          BlobError {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      VerbatimString {
        ref data,
        ref format,
        ref attributes,
      } => {
        let (_data, _format, _attributes) = (data, format, attributes);
        match *other {
          VerbatimString {
            ref data,
            ref format,
            ref attributes,
          } => _data == data && _format == format && attributes == _attributes,
          _ => false,
        }
      }
      Map {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Map {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Set {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Set {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Push {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Push {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Hello { ref version, ref auth } => {
        let (_version, _auth) = (version, auth);
        match *other {
          Hello { ref version, ref auth } => _version == version && _auth == auth,
          _ => false,
        }
      }
      BigNumber {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          BigNumber {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
    }
  }
}

impl Eq for Frame {}

#[cfg(test)]
impl TryFrom<(FrameKind, Vec<u8>)> for Frame {
  type Error = RedisProtocolError;

  fn try_from((kind, value): (FrameKind, Vec<u8>)) -> Result<Self, Self::Error> {
    let frame = match kind {
      FrameKind::BlobString => Frame::BlobString {
        data: value.into(),
        attributes: None,
      },
      FrameKind::BlobError => Frame::BlobError {
        data: value.into(),
        attributes: None,
      },
      FrameKind::BigNumber => Frame::BigNumber {
        data: value.into(),
        attributes: None,
      },
      FrameKind::ChunkedString => Frame::ChunkedString(value.into()),
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Cannot convert to Frame.",
        ))
      }
    };

    Ok(frame)
  }
}

#[cfg(test)]
impl TryFrom<(FrameKind, Vec<Frame>)> for Frame {
  type Error = RedisProtocolError;

  fn try_from((kind, data): (FrameKind, Vec<Frame>)) -> Result<Self, Self::Error> {
    let frame = match kind {
      FrameKind::Array => Frame::Array { data, attributes: None },
      FrameKind::Push => Frame::Push { data, attributes: None },
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Cannot convert to Frame.",
        ))
      }
    };

    Ok(frame)
  }
}

#[cfg(test)]
impl TryFrom<(FrameKind, VecDeque<Frame>)> for Frame {
  type Error = RedisProtocolError;

  fn try_from((kind, value): (FrameKind, VecDeque<Frame>)) -> Result<Self, Self::Error> {
    let data: Vec<Frame> = value.into_iter().collect();

    let frame = match kind {
      FrameKind::Array => Frame::Array { data, attributes: None },
      FrameKind::Push => Frame::Push { data, attributes: None },
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Cannot convert to Frame.",
        ))
      }
    };

    Ok(frame)
  }
}

impl TryFrom<HashMap<Frame, Frame>> for Frame {
  type Error = RedisProtocolError;

  fn try_from(value: HashMap<Frame, Frame>) -> Result<Self, Self::Error> {
    Ok(Frame::Map {
      data: resp3_utils::hashmap_to_frame_map(value),
      attributes: None,
    })
  }
}

impl TryFrom<HashSet<Frame>> for Frame {
  type Error = RedisProtocolError;

  fn try_from(value: HashSet<Frame>) -> Result<Self, Self::Error> {
    Ok(Frame::Set {
      data: resp3_utils::hashset_to_frame_set(value),
      attributes: None,
    })
  }
}

#[cfg(feature = "index-map")]
impl TryFrom<IndexMap<Frame, Frame>> for Frame {
  type Error = RedisProtocolError;

  fn try_from(value: IndexMap<Frame, Frame>) -> Result<Self, Self::Error> {
    Ok(Frame::Map {
      data: value,
      attributes: None,
    })
  }
}

#[cfg(feature = "index-map")]
impl TryFrom<IndexSet<Frame>> for Frame {
  type Error = RedisProtocolError;

  fn try_from(value: IndexSet<Frame>) -> Result<Self, Self::Error> {
    Ok(Frame::Set {
      data: value,
      attributes: None,
    })
  }
}

impl From<i64> for Frame {
  fn from(value: i64) -> Self {
    Frame::Number {
      data: value,
      attributes: None,
    }
  }
}

impl From<bool> for Frame {
  fn from(value: bool) -> Self {
    Frame::Boolean {
      data: value,
      attributes: None,
    }
  }
}

impl TryFrom<f64> for Frame {
  type Error = RedisProtocolError;

  fn try_from(value: f64) -> Result<Self, Self::Error> {
    if value.is_nan() {
      Err(RedisProtocolError::new(
        RedisProtocolErrorKind::Unknown,
        "Cannot cast NaN to double.",
      ))
    } else {
      Ok(Frame::Double {
        data: value,
        attributes: None,
      })
    }
  }
}

#[cfg(test)]
impl TryFrom<(FrameKind, String)> for Frame {
  type Error = RedisProtocolError;

  fn try_from((kind, value): (FrameKind, String)) -> Result<Self, Self::Error> {
    let frame = match kind {
      FrameKind::SimpleError => Frame::SimpleError {
        data: value.into(),
        attributes: None,
      },
      FrameKind::SimpleString => Frame::SimpleString {
        data: value.into(),
        attributes: None,
      },
      FrameKind::BlobError => Frame::BlobError {
        data: value.into(),
        attributes: None,
      },
      FrameKind::BlobString => Frame::BlobString {
        data: value.into(),
        attributes: None,
      },
      FrameKind::BigNumber => Frame::BigNumber {
        data: value.into(),
        attributes: None,
      },
      FrameKind::ChunkedString => Frame::ChunkedString(value.into()),
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Cannot convert to Frame.",
        ))
      }
    };

    Ok(frame)
  }
}

#[cfg(test)]
impl<'a> TryFrom<(FrameKind, &'a str)> for Frame {
  type Error = RedisProtocolError;

  fn try_from((kind, value): (FrameKind, &'a str)) -> Result<Self, Self::Error> {
    (kind, value.to_owned()).try_into()
  }
}

impl Frame {
  /// Whether or not the frame can be used as a key in a `HashMap` or `HashSet`.
  ///
  /// Not all frame types can be hashed, and trying to do so can panic. This function can be used to handle this gracefully.
  pub fn can_hash(&self) -> bool {
    match self.kind() {
      FrameKind::BlobString
      | FrameKind::BlobError
      | FrameKind::SimpleString
      | FrameKind::SimpleError
      | FrameKind::Number
      | FrameKind::Double
      | FrameKind::Boolean
      | FrameKind::Null
      | FrameKind::ChunkedString
      | FrameKind::EndStream
      | FrameKind::VerbatimString
      | FrameKind::BigNumber => true,
      _ => false,
    }
  }

  /// Read the attributes attached to the frame.
  pub fn attributes(&self) -> Option<&Attributes> {
    let attributes = match *self {
      Frame::Array { ref attributes, .. } => attributes,
      Frame::Push { ref attributes, .. } => attributes,
      Frame::BlobString { ref attributes, .. } => attributes,
      Frame::BlobError { ref attributes, .. } => attributes,
      Frame::BigNumber { ref attributes, .. } => attributes,
      Frame::Boolean { ref attributes, .. } => attributes,
      Frame::Number { ref attributes, .. } => attributes,
      Frame::Double { ref attributes, .. } => attributes,
      Frame::VerbatimString { ref attributes, .. } => attributes,
      Frame::SimpleError { ref attributes, .. } => attributes,
      Frame::SimpleString { ref attributes, .. } => attributes,
      Frame::Set { ref attributes, .. } => attributes,
      Frame::Map { ref attributes, .. } => attributes,
      Frame::Null | Frame::ChunkedString(_) | Frame::Hello { .. } => return None,
    };

    attributes.as_ref()
  }

  /// Take the attributes off this frame.
  pub fn take_attributes(&mut self) -> Option<Attributes> {
    let attributes = match *self {
      Frame::Array { ref mut attributes, .. } => attributes,
      Frame::Push { ref mut attributes, .. } => attributes,
      Frame::BlobString { ref mut attributes, .. } => attributes,
      Frame::BlobError { ref mut attributes, .. } => attributes,
      Frame::BigNumber { ref mut attributes, .. } => attributes,
      Frame::Boolean { ref mut attributes, .. } => attributes,
      Frame::Number { ref mut attributes, .. } => attributes,
      Frame::Double { ref mut attributes, .. } => attributes,
      Frame::VerbatimString { ref mut attributes, .. } => attributes,
      Frame::SimpleError { ref mut attributes, .. } => attributes,
      Frame::SimpleString { ref mut attributes, .. } => attributes,
      Frame::Set { ref mut attributes, .. } => attributes,
      Frame::Map { ref mut attributes, .. } => attributes,
      Frame::Null | Frame::ChunkedString(_) | Frame::Hello { .. } => return None,
    };

    attributes.take()
  }

  /// Read a mutable reference to any attributes attached to the frame.
  pub fn attributes_mut(&mut self) -> Option<&mut Attributes> {
    let attributes = match *self {
      Frame::Array { ref mut attributes, .. } => attributes,
      Frame::Push { ref mut attributes, .. } => attributes,
      Frame::BlobString { ref mut attributes, .. } => attributes,
      Frame::BlobError { ref mut attributes, .. } => attributes,
      Frame::BigNumber { ref mut attributes, .. } => attributes,
      Frame::Boolean { ref mut attributes, .. } => attributes,
      Frame::Number { ref mut attributes, .. } => attributes,
      Frame::Double { ref mut attributes, .. } => attributes,
      Frame::VerbatimString { ref mut attributes, .. } => attributes,
      Frame::SimpleError { ref mut attributes, .. } => attributes,
      Frame::SimpleString { ref mut attributes, .. } => attributes,
      Frame::Set { ref mut attributes, .. } => attributes,
      Frame::Map { ref mut attributes, .. } => attributes,
      Frame::Null | Frame::ChunkedString(_) | Frame::Hello { .. } => return None,
    };

    attributes.as_mut()
  }

  /// Attempt to add attributes to the frame, extending the existing attributes if needed.
  pub fn add_attributes(&mut self, attributes: Attributes) -> Result<(), RedisProtocolError> {
    let _attributes = match *self {
      Frame::Array { ref mut attributes, .. } => attributes,
      Frame::Push { ref mut attributes, .. } => attributes,
      Frame::BlobString { ref mut attributes, .. } => attributes,
      Frame::BlobError { ref mut attributes, .. } => attributes,
      Frame::BigNumber { ref mut attributes, .. } => attributes,
      Frame::Boolean { ref mut attributes, .. } => attributes,
      Frame::Number { ref mut attributes, .. } => attributes,
      Frame::Double { ref mut attributes, .. } => attributes,
      Frame::VerbatimString { ref mut attributes, .. } => attributes,
      Frame::SimpleError { ref mut attributes, .. } => attributes,
      Frame::SimpleString { ref mut attributes, .. } => attributes,
      Frame::Set { ref mut attributes, .. } => attributes,
      Frame::Map { ref mut attributes, .. } => attributes,
      Frame::Null | Frame::ChunkedString(_) | Frame::Hello { .. } => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          format!("{:?} cannot have attributes.", self.kind()),
        ))
      }
    };

    if let Some(_attributes) = _attributes.as_mut() {
      _attributes.extend(attributes.into_iter());
    } else {
      *_attributes = Some(attributes);
    }

    Ok(())
  }

  /// Create a new `Frame` that terminates a stream.
  pub fn new_end_stream() -> Self {
    Frame::ChunkedString(Bytes::new())
  }

  /// A context-aware length function that returns the length of the inner frame contents.
  ///
  /// This does not return the encoded length, but rather the length of the contents of the frame such as the number of elements in an array, the size of any inner buffers, etc.
  ///
  /// Note: `Null` has a length of 0 and `Hello`, `Number`, `Double`, and `Boolean` have a length of 1.
  ///
  /// See [encode_len](Self::encode_len) to read the number of bytes necessary to encode the frame.
  pub fn len(&self) -> usize {
    use self::Frame::*;

    match *self {
      Array { ref data, .. } | Push { ref data, .. } => data.len(),
      BlobString { ref data, .. }
      | BlobError { ref data, .. }
      | BigNumber { ref data, .. }
      | ChunkedString(ref data) => data.len(),
      SimpleString { ref data, .. } => data.len(),
      SimpleError { ref data, .. } => data.len(),
      Number { .. } | Double { .. } | Boolean { .. } => 1,
      Null => 0,
      VerbatimString { ref data, .. } => data.len(),
      Map { ref data, .. } => data.len(),
      Set { ref data, .. } => data.len(),
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
      Array { .. } => FrameKind::Array,
      BlobString { .. } => FrameKind::BlobString,
      SimpleString { .. } => FrameKind::SimpleString,
      SimpleError { .. } => FrameKind::SimpleError,
      Number { .. } => FrameKind::Number,
      Null => FrameKind::Null,
      Double { .. } => FrameKind::Double,
      BlobError { .. } => FrameKind::BlobError,
      VerbatimString { .. } => FrameKind::VerbatimString,
      Boolean { .. } => FrameKind::Boolean,
      Map { .. } => FrameKind::Map,
      Set { .. } => FrameKind::Set,
      Push { .. } => FrameKind::Push,
      Hello { .. } => FrameKind::Hello,
      BigNumber { .. } => FrameKind::BigNumber,
      ChunkedString(ref inner) => {
        if inner.is_empty() {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      }
    }
  }

  /// Whether or not the frame is a `Null` variant.
  pub fn is_null(&self) -> bool {
    match *self {
      Frame::Null => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents an array of frames.
  pub fn is_array(&self) -> bool {
    match *self {
      Frame::Array { .. } => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents data pushed to the client from the server.
  pub fn is_push(&self) -> bool {
    match *self {
      Frame::Push { .. } => true,
      _ => false,
    }
  }

  /// Whether or not the frame is a boolean value.
  pub fn is_boolean(&self) -> bool {
    match *self {
      Frame::Boolean { .. } => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents an error.
  pub fn is_error(&self) -> bool {
    match *self {
      Frame::BlobError { .. } | Frame::SimpleError { .. } => true,
      _ => false,
    }
  }

  /// Whether or not the frame is an array, map, or set.
  pub fn is_aggregate_type(&self) -> bool {
    match *self {
      Frame::Map { .. } | Frame::Set { .. } | Frame::Array { .. } => true,
      _ => false,
    }
  }

  /// Whether or not the frame represents a `BlobString` or `BlobError`.
  pub fn is_blob(&self) -> bool {
    match *self {
      Frame::BlobString { .. } | Frame::BlobError { .. } => true,
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
      Frame::SimpleError { ref data, .. } => Some(data),
      Frame::SimpleString { ref data, .. } => str::from_utf8(data).ok(),
      Frame::BlobError { ref data, .. } | Frame::BlobString { ref data, .. } | Frame::BigNumber { ref data, .. } => {
        str::from_utf8(data).ok()
      }
      Frame::VerbatimString { ref data, .. } => str::from_utf8(data).ok(),
      Frame::ChunkedString(ref data) => str::from_utf8(data).ok(),
      _ => None,
    }
  }

  /// Read the frame as a `String` if it can be parsed as a UTF-8 string.
  pub fn to_string(&self) -> Option<String> {
    match *self {
      Frame::SimpleError { ref data, .. } => Some(data.to_string()),
      Frame::SimpleString { ref data, .. } => String::from_utf8(data.to_vec()).ok(),
      Frame::BlobError { ref data, .. } | Frame::BlobString { ref data, .. } | Frame::BigNumber { ref data, .. } => {
        String::from_utf8(data.to_vec()).ok()
      }
      Frame::VerbatimString { ref data, .. } => String::from_utf8(data.to_vec()).ok(),
      Frame::ChunkedString(ref b) => String::from_utf8(b.to_vec()).ok(),
      Frame::Double { ref data, .. } => Some(data.to_string()),
      Frame::Number { ref data, .. } => Some(data.to_string()),
      _ => None,
    }
  }

  /// Attempt to read the frame as a byte slice.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match *self {
      Frame::SimpleError { ref data, .. } => Some(data.as_bytes()),
      Frame::SimpleString { ref data, .. } => Some(&data),
      Frame::BlobError { ref data, .. } | Frame::BlobString { ref data, .. } | Frame::BigNumber { ref data, .. } => {
        Some(data)
      }
      Frame::VerbatimString { ref data, .. } => Some(data),
      Frame::ChunkedString(ref b) => Some(b),
      _ => None,
    }
  }

  /// Attempt to read the frame as an `i64`.
  pub fn as_i64(&self) -> Option<i64> {
    match *self {
      Frame::Number { ref data, .. } => Some(*data),
      Frame::Double { ref data, .. } => Some(*data as i64),
      Frame::BlobString { ref data, .. } => str::from_utf8(data).ok().and_then(|s| s.parse::<i64>().ok()),
      Frame::SimpleString { ref data, .. } => str::from_utf8(data).ok().and_then(|s| s.parse::<i64>().ok()),
      _ => None,
    }
  }

  /// Attempt to read the frame as an `f64`.
  pub fn as_f64(&self) -> Option<f64> {
    match *self {
      Frame::Double { ref data, .. } => Some(*data),
      Frame::Number { ref data, .. } => Some(*data as f64),
      Frame::BlobString { ref data, .. } => str::from_utf8(data).ok().and_then(|s| s.parse::<f64>().ok()),
      Frame::SimpleString { ref data, .. } => str::from_utf8(data).ok().and_then(|s| s.parse::<f64>().ok()),
      _ => None,
    }
  }

  /// Whether or not the frame represents a MOVED or ASK error.
  pub fn is_moved_or_ask_error(&self) -> bool {
    match *self {
      Frame::SimpleError { ref data, .. } => utils::is_cluster_error(data),
      _ => false,
    }
  }

  /// Attempt to parse the frame as a cluster redirection error.
  pub fn to_redirection(&self) -> Option<Redirection> {
    match *self {
      Frame::SimpleError { ref data, .. } => utils::read_cluster_error(data),
      _ => None,
    }
  }

  /// Whether or not the frame represents a publish-subscribe message, but not a pattern publish-subscribe message.
  pub fn is_normal_pubsub(&self) -> bool {
    if let Frame::Push { ref data, .. } = *self {
      resp3_utils::is_normal_pubsub(data)
    } else {
      false
    }
  }

  /// Whether or not the frame represents a message on a publish-subscribe channel.
  pub fn is_pubsub_message(&self) -> bool {
    if let Frame::Push { ref data, .. } = *self {
      resp3_utils::is_normal_pubsub(data) || resp3_utils::is_pattern_pubsub(data)
    } else {
      false
    }
  }

  /// Whether or not the frame represents a message on a publish-subscribe channel matched against a pattern subscription.
  pub fn is_pattern_pubsub_message(&self) -> bool {
    if let Frame::Push { ref data, .. } = *self {
      resp3_utils::is_pattern_pubsub(data)
    } else {
      false
    }
  }

  /// Attempt to parse the frame as a publish-subscribe message, returning the `(channel, message)` tuple
  /// if successful, or the original frame if the inner data is not a publish-subscribe message.
  pub fn parse_as_pubsub(self) -> Result<(Frame, Frame), Self> {
    if self.is_pubsub_message() {
      if let Frame::Push { mut data, .. } = self {
        // array len checked in `is_pubsub_message`
        let message = data.pop().unwrap();
        let channel = data.pop().unwrap();

        Ok((channel, message))
      } else {
        warn!("Invalid pubsub frame. Expected a Push frame.");
        Err(self)
      }
    } else {
      Err(self)
    }
  }

  /// Attempt to read the number of bytes needed to encode the frame.
  pub fn encode_len(&self) -> Result<usize, RedisProtocolError> {
    resp3_utils::encode_len(self).map_err(|e| e.into())
  }
}

/// A helper struct for reading and managing streaming data types.
///
/// ```rust edition2018
/// # use bytes::Bytes;
/// use redis_protocol::resp3::decode::streaming::decode;
///
/// fn main() {
///   let parts: Vec<Bytes> = vec!["*?\r\n".into(), ":1\r\n".into(), ":2\r\n".into(), ".\r\n".into()];
///
///   let (frame, _) = decode(&parts[0]).unwrap().unwrap();
///   assert!(frame.is_streaming());
///   let mut streaming = frame.into_streaming_frame().unwrap();
///   println!("Reading streaming {:?}", streaming.kind);
///
///   let (frame, _) = decode(&parts[1]).unwrap().unwrap();
///   assert!(frame.is_complete());
///   // add frames to the buffer until we reach the terminating byte sequence
///   streaming.add_frame(frame.into_complete_frame().unwrap());
///
///   let (frame, _) = decode(&parts[2]).unwrap().unwrap();
///   assert!(frame.is_complete());
///   streaming.add_frame(frame.into_complete_frame().unwrap());
///
///   let (frame, _) = decode(&parts[3]).unwrap().unwrap();
///   assert!(frame.is_complete());
///   streaming.add_frame(frame.into_complete_frame().unwrap());
///
///   assert!(streaming.is_finished());
///   // convert the buffer into one frame
///   let result = streaming.into_frame().unwrap();
///
///   // Frame::Array { data: [1, 2], attributes: None }
///   println!("{:?}", result);
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct StreamedFrame {
  /// The internal buffer of frames and attributes.
  buffer: VecDeque<Frame>,
  /// Any leading attributes before the stream starts.
  pub attributes: Option<Attributes>,
  /// The data type being streamed.  
  pub kind: FrameKind,
}

impl StreamedFrame {
  /// Create a new `StreamedFrame` from the first section of data in a streaming response.
  pub fn new(kind: FrameKind) -> Self {
    let buffer = VecDeque::new();
    StreamedFrame {
      buffer,
      kind,
      attributes: None,
    }
  }

  /// Convert the internal buffer into one frame matching `self.kind`, clearing the internal buffer.
  pub fn into_frame(&mut self) -> Result<Frame, RedisProtocolError> {
    if !self.kind.is_streaming_type() {
      // try to catch invalid type errors early so the caller can modify the frame before we clear the buffer
      return Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Only blob strings, sets, maps, and arrays can be streamed.",
      ));
    }

    if self.is_finished() {
      // the last frame is an empty chunked string when the stream is finished
      let _ = self.buffer.pop_back();
    }
    let buffer = mem::replace(&mut self.buffer, VecDeque::new());
    let attributes = self.attributes.take();

    let frame = match self.kind {
      FrameKind::BlobString => resp3_utils::reconstruct_blobstring(buffer, attributes)?,
      FrameKind::Map => resp3_utils::reconstruct_map(buffer, attributes)?,
      FrameKind::Set => resp3_utils::reconstruct_set(buffer, attributes)?,
      FrameKind::Array => resp3_utils::reconstruct_array(buffer, attributes)?,
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Streaming frames only supported for blob strings, maps, sets, and arrays.",
        ))
      }
    };

    Ok(frame)
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
#[derive(Debug, Eq, PartialEq)]
pub enum DecodedFrame {
  Streaming(StreamedFrame),
  Complete(Frame),
}

impl DecodedFrame {
  /// Add attributes to the decoded frame, if possible.
  pub fn add_attributes(&mut self, attributes: Attributes) -> Result<(), RedisProtocolError> {
    let _ = match *self {
      DecodedFrame::Streaming(ref mut inner) => inner.attributes = Some(attributes),
      DecodedFrame::Complete(ref mut inner) => inner.add_attributes(attributes)?,
    };

    Ok(())
  }

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

  /// Convert the decoded frame into a streaming frame, returning an error if a complete variant is found.
  pub fn into_streaming_frame(self) -> Result<StreamedFrame, RedisProtocolError> {
    match self {
      DecodedFrame::Streaming(frame) => Ok(frame),
      DecodedFrame::Complete(_) => Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected streamed frame.",
      )),
    }
  }

  /// Whether or not the decoded frame starts a stream.
  pub fn is_streaming(&self) -> bool {
    match *self {
      DecodedFrame::Streaming(_) => true,
      _ => false,
    }
  }

  /// Whether or not the decoded frame is a complete frame.
  pub fn is_complete(&self) -> bool {
    !self.is_streaming()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::resp3::utils::new_map;

  #[test]
  fn should_convert_basic_streaming_buffer_to_frame() {
    let mut streaming_buf = StreamedFrame::new(FrameKind::BlobString);
    streaming_buf.add_frame((FrameKind::ChunkedString, "foo").try_into().unwrap());
    streaming_buf.add_frame((FrameKind::ChunkedString, "bar").try_into().unwrap());
    streaming_buf.add_frame((FrameKind::ChunkedString, "baz").try_into().unwrap());
    streaming_buf.add_frame(Frame::new_end_stream());
    let frame = streaming_buf
      .into_frame()
      .expect("Failed to build frame from chunked stream");

    assert_eq!(frame.as_str(), Some("foobarbaz"));
  }

  #[test]
  fn should_convert_basic_streaming_buffer_to_frame_with_attributes() {
    let mut attributes = new_map(None);
    attributes.insert((FrameKind::SimpleString, "a").try_into().unwrap(), 1.into());
    attributes.insert((FrameKind::SimpleString, "b").try_into().unwrap(), 2.into());
    attributes.insert((FrameKind::SimpleString, "c").try_into().unwrap(), 3.into());

    let mut streaming_buf = StreamedFrame::new(FrameKind::BlobString);
    streaming_buf.attributes = Some(attributes.clone());

    streaming_buf.add_frame((FrameKind::ChunkedString, "foo").try_into().unwrap());
    streaming_buf.add_frame((FrameKind::ChunkedString, "bar").try_into().unwrap());
    streaming_buf.add_frame((FrameKind::ChunkedString, "baz").try_into().unwrap());
    streaming_buf.add_frame(Frame::new_end_stream());

    let frame = streaming_buf
      .into_frame()
      .expect("Failed to build frame from chunked stream");

    assert_eq!(frame.as_str(), Some("foobarbaz"));
    assert_eq!(frame.attributes(), Some(&attributes));
  }
}
