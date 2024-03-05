use crate::{
  error::{RedisProtocolError, RedisProtocolErrorKind},
  resp3::utils as resp3_utils,
  types::{PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX, PUBSUB_PUSH_PREFIX, SHARD_PUBSUB_PREFIX},
  utils,
};
use alloc::{
  collections::VecDeque,
  format,
  string::{String, ToString},
  vec::Vec,
};
use bytes::Bytes;
use bytes_utils::Str;
use core::{
  convert::TryFrom,
  hash::{Hash, Hasher},
  mem,
  str,
};

#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};
#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};
#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

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
pub const END_STREAM_STRING_BYTES: &str = ";0\r\n";
/// The terminating bytes when a streaming operation is done from an aggregate type.
pub const END_STREAM_AGGREGATE_BYTES: &str = ".\r\n";

/// The binary representation of NULL in RESP3.
pub const NULL: &str = "_\r\n";

/// Byte representation of positive infinity.
pub const INFINITY: &str = "inf";
/// Byte representation of negative infinity.
pub const NEG_INFINITY: &str = "-inf";

/// Byte representation of HELLO.
pub const HELLO: &str = "HELLO";
/// Byte representation of `true`.
pub const BOOL_TRUE_BYTES: &str = "#t\r\n";
/// Byte representation of `false`.
pub const BOOL_FALSE_BYTES: &str = "#f\r\n";
/// Byte representation of an empty space.
pub const EMPTY_SPACE: &str = " ";
/// Byte representation of `AUTH`.
pub const AUTH: &str = "AUTH";

/// A map struct for frames that uses either [indexmap::IndexMap], [hashbrown::HashMap], or
/// [std::collections::HashMap] depending on the enabled feature flags.
#[cfg(not(feature = "index-map"))]
pub type FrameMap<T> = HashMap<T, T>;
/// A set struct for frames that uses either [indexmap::IndexSet], [hashbrown::HashSet], or
/// [std::collections::HashSet] depending on the enabled feature flags.
#[cfg(not(feature = "index-map"))]
pub type FrameSet<T> = HashSet<T>;
/// A map struct for frames that uses either [indexmap::IndexMap], [hashbrown::HashMap], or
/// [std::collections::HashMap] depending on the enabled feature flags.
#[cfg(feature = "index-map")]
pub type FrameMap<T> = IndexMap<T, T>;
/// A set struct for frames that uses either [indexmap::IndexSet], [hashbrown::HashSet], or
/// [std::collections::HashSet] depending on the enabled feature flags.
#[cfg(feature = "index-map")]
pub type FrameSet<T> = IndexSet<T>;

/// Additional information returned alongside a [BytesFrame].
pub type BytesAttributes = FrameMap<BytesFrame>;
/// Additional information returned alongside an [OwnedFrame].
pub type OwnedAttributes = FrameMap<OwnedFrame>;

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
  /// Whether the frame is an aggregate type (array, set, map).
  pub fn is_aggregate_type(&self) -> bool {
    matches!(self, FrameKind::Array | FrameKind::Set | FrameKind::Map)
  }

  /// Whether the frame is an aggregate type or blob string.
  pub fn is_streaming_type(&self) -> bool {
    matches!(
      self,
      FrameKind::Array | FrameKind::Set | FrameKind::Map | FrameKind::BlobString
    )
  }

  /// Whether the frame can be used as a key in a `HashMap` or `HashSet`.
  ///
  /// Not all frame types can be hashed, and trying to do so can panic. This function can be used to handle this
  /// gracefully.
  pub fn can_hash(&self) -> bool {
    matches!(
      self,
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
        | FrameKind::BigNumber
    )
  }

  /// A function used to differentiate data types that may have the same inner binary representation when hashing a
  /// `BytesFrame`.
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

  /// Whether the frame type is a `HELLO` frame.
  ///
  /// `HELLO` is encoded differently than other frames so this is used to prevent panics in [to_byte](Self::to_byte).
  pub(crate) fn is_hello(&self) -> bool {
    matches!(self, FrameKind::Hello)
  }
}

///
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnedFrame {
  /// A blob of bytes.
  BlobString {
    data:       Vec<u8>,
    attributes: Option<OwnedAttributes>,
  },
  /// A blob representing an error.
  BlobError {
    data:       Vec<u8>,
    attributes: Option<OwnedAttributes>,
  },
  /// A small string.
  ///
  /// The internal data type is `Vec<u8>` in order to support callers that use this interface to parse a `MONITOR`
  /// stream.
  SimpleString {
    data:       Vec<u8>,
    attributes: Option<OwnedAttributes>,
  },
  /// A small string representing an error.
  SimpleError {
    data:       String,
    attributes: Option<OwnedAttributes>,
  },
  /// A boolean type.
  Boolean {
    data:       bool,
    attributes: Option<OwnedAttributes>,
  },
  /// A null type.
  Null,
  /// A signed 64-bit integer.
  Number {
    data:       i64,
    attributes: Option<OwnedAttributes>,
  },
  /// A signed 64-bit floating point number.
  Double {
    data:       f64,
    attributes: Option<OwnedAttributes>,
  },
  /// A large number not representable as a `Number` or `Double`.
  ///
  /// This library does not attempt to parse this.
  BigNumber {
    data:       Vec<u8>,
    attributes: Option<OwnedAttributes>,
  },
  /// A string to be displayed without any escaping or filtering.
  VerbatimString {
    data:       Vec<u8>,
    format:     VerbatimStringFormat,
    attributes: Option<OwnedAttributes>,
  },
  /// An array of frames.
  Array {
    data:       Vec<OwnedFrame>,
    attributes: Option<OwnedAttributes>,
  },
  /// An unordered map of key-value pairs.
  ///
  /// According to the spec keys can be any other RESP3 data type. However, it doesn't make sense to implement `Hash`
  /// for certain aggregate types. The [can_hash](crate::resp3::types::FrameKind::can_hash) function can be used to
  /// detect this.
  Map {
    data:       FrameMap<OwnedFrame>,
    attributes: Option<OwnedAttributes>,
  },
  /// An unordered collection of other frames with a uniqueness constraint.
  Set {
    data:       FrameSet<OwnedFrame>,
    attributes: Option<OwnedAttributes>,
  },
  /// Out-of-band data.
  Push {
    data:       Vec<OwnedFrame>,
    attributes: Option<OwnedAttributes>,
  },
  /// A special frame type used when first connecting to the server to describe the protocol version and optional
  /// credentials.
  Hello {
    version:  RespVersion,
    username: Option<String>,
    password: Option<String>,
  },
  /// One chunk of a streaming blob.
  ChunkedString(Vec<u8>),
}

impl Hash for OwnedFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    use self::OwnedFrame::*;
    self.kind().hash_prefix().hash(state);

    match self {
      BlobString { data, .. } => data.hash(state),
      SimpleString { data, .. } => data.hash(state),
      SimpleError { data, .. } => data.hash(state),
      Number { data, .. } => data.hash(state),
      Null => NULL.hash(state),
      Double { data, .. } => data.to_string().hash(state),
      Boolean { data, .. } => data.hash(state),
      BlobError { data, .. } => data.hash(state),
      VerbatimString { data, format, .. } => {
        format.hash(state);
        data.hash(state);
      },
      ChunkedString(data) => data.hash(state),
      BigNumber { data, .. } => data.hash(state),
      _ => panic!("Invalid RESP3 data type to use as hash key."),
    };
  }
}

impl OwnedFrame {
  /// Read the attributes attached to the frame.
  pub fn attributes(&self) -> Option<&OwnedAttributes> {
    let attributes = match self {
      OwnedFrame::Array { attributes, .. } => attributes,
      OwnedFrame::Push { attributes, .. } => attributes,
      OwnedFrame::BlobString { attributes, .. } => attributes,
      OwnedFrame::BlobError { attributes, .. } => attributes,
      OwnedFrame::BigNumber { attributes, .. } => attributes,
      OwnedFrame::Boolean { attributes, .. } => attributes,
      OwnedFrame::Number { attributes, .. } => attributes,
      OwnedFrame::Double { attributes, .. } => attributes,
      OwnedFrame::VerbatimString { attributes, .. } => attributes,
      OwnedFrame::SimpleError { attributes, .. } => attributes,
      OwnedFrame::SimpleString { attributes, .. } => attributes,
      OwnedFrame::Set { attributes, .. } => attributes,
      OwnedFrame::Map { attributes, .. } => attributes,
      OwnedFrame::Null | OwnedFrame::ChunkedString(_) | OwnedFrame::Hello { .. } => return None,
    };

    attributes.as_ref()
  }

  /// Take the attributes off this frame.
  pub fn take_attributes(&mut self) -> Option<OwnedAttributes> {
    let attributes = match self {
      OwnedFrame::Array { attributes, .. } => attributes,
      OwnedFrame::Push { attributes, .. } => attributes,
      OwnedFrame::BlobString { attributes, .. } => attributes,
      OwnedFrame::BlobError { attributes, .. } => attributes,
      OwnedFrame::BigNumber { attributes, .. } => attributes,
      OwnedFrame::Boolean { attributes, .. } => attributes,
      OwnedFrame::Number { attributes, .. } => attributes,
      OwnedFrame::Double { attributes, .. } => attributes,
      OwnedFrame::VerbatimString { attributes, .. } => attributes,
      OwnedFrame::SimpleError { attributes, .. } => attributes,
      OwnedFrame::SimpleString { attributes, .. } => attributes,
      OwnedFrame::Set { attributes, .. } => attributes,
      OwnedFrame::Map { attributes, .. } => attributes,
      OwnedFrame::Null | OwnedFrame::ChunkedString(_) | OwnedFrame::Hello { .. } => return None,
    };

    attributes.take()
  }

  /// Read a mutable reference to any attributes attached to the frame.
  pub fn attributes_mut(&mut self) -> Option<&mut OwnedAttributes> {
    let attributes = match self {
      OwnedFrame::Array { attributes, .. } => attributes,
      OwnedFrame::Push { attributes, .. } => attributes,
      OwnedFrame::BlobString { attributes, .. } => attributes,
      OwnedFrame::BlobError { attributes, .. } => attributes,
      OwnedFrame::BigNumber { attributes, .. } => attributes,
      OwnedFrame::Boolean { attributes, .. } => attributes,
      OwnedFrame::Number { attributes, .. } => attributes,
      OwnedFrame::Double { attributes, .. } => attributes,
      OwnedFrame::VerbatimString { attributes, .. } => attributes,
      OwnedFrame::SimpleError { attributes, .. } => attributes,
      OwnedFrame::SimpleString { attributes, .. } => attributes,
      OwnedFrame::Set { attributes, .. } => attributes,
      OwnedFrame::Map { attributes, .. } => attributes,
      OwnedFrame::Null | OwnedFrame::ChunkedString(_) | OwnedFrame::Hello { .. } => return None,
    };

    attributes.as_mut()
  }

  /// Attempt to add attributes to the frame, extending the existing attributes if needed.
  pub fn add_attributes(&mut self, attributes: OwnedAttributes) -> Result<(), RedisProtocolError> {
    let _attributes = match self {
      OwnedFrame::Array { attributes, .. } => attributes,
      OwnedFrame::Push { attributes, .. } => attributes,
      OwnedFrame::BlobString { attributes, .. } => attributes,
      OwnedFrame::BlobError { attributes, .. } => attributes,
      OwnedFrame::BigNumber { attributes, .. } => attributes,
      OwnedFrame::Boolean { attributes, .. } => attributes,
      OwnedFrame::Number { attributes, .. } => attributes,
      OwnedFrame::Double { attributes, .. } => attributes,
      OwnedFrame::VerbatimString { attributes, .. } => attributes,
      OwnedFrame::SimpleError { attributes, .. } => attributes,
      OwnedFrame::SimpleString { attributes, .. } => attributes,
      OwnedFrame::Set { attributes, .. } => attributes,
      OwnedFrame::Map { attributes, .. } => attributes,
      OwnedFrame::Null | OwnedFrame::ChunkedString(_) | OwnedFrame::Hello { .. } => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          format!("{:?} cannot have attributes.", self.kind()),
        ))
      },
    };

    if let Some(_attributes) = _attributes.as_mut() {
      _attributes.extend(attributes.into_iter());
    } else {
      *_attributes = Some(attributes);
    }

    Ok(())
  }

  /// Create a new `OwnedFrame` that terminates a stream.
  pub fn new_end_stream() -> Self {
    OwnedFrame::ChunkedString(Vec::new())
  }

  /// A context-aware length function that returns the length of the inner frame contents.
  ///
  /// This does not return the encoded length, but rather the length of the contents of the frame such as the number
  /// of elements in an array, the size of any inner buffers, etc.
  ///
  /// Note: `Null` has a length of 0 and `Hello`, `Number`, `Double`, and `Boolean` have a length of 1.
  ///
  /// See [encode_len](Self::encode_len) to read the number of bytes necessary to encode the frame.
  pub fn len(&self) -> usize {
    use self::OwnedFrame::*;

    match self {
      Array { data, .. } | Push { data, .. } => data.len(),
      BlobString { data, .. } | BlobError { data, .. } | BigNumber { data, .. } | ChunkedString(data) => data.len(),
      SimpleString { data, .. } => data.len(),
      SimpleError { data, .. } => data.len(),
      Number { .. } | Double { .. } | Boolean { .. } => 1,
      Null => 0,
      VerbatimString { data, .. } => data.len(),
      Map { data, .. } => data.len(),
      Set { data, .. } => data.len(),
      Hello { .. } => 1,
    }
  }

  /// Replace `self` with Null, returning the original value.
  pub fn take(&mut self) -> OwnedFrame {
    mem::replace(self, OwnedFrame::Null)
  }

  /// Read the associated `FrameKind`.
  pub fn kind(&self) -> FrameKind {
    use self::OwnedFrame::*;

    match self {
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
      ChunkedString(inner) => {
        if inner.is_empty() {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      },
    }
  }

  /// Whether the frame is an empty chunked string, signifying the end of a chunked string stream.
  pub fn is_end_stream_frame(&self) -> bool {
    match self {
      BytesFrame::ChunkedString(s) => s.is_empty(),
      _ => false,
    }
  }

  /// If the frame is a verbatim string then read the associated format.
  pub fn verbatim_string_format(&self) -> Option<&VerbatimStringFormat> {
    match self {
      BytesFrame::VerbatimString { format, .. } => Some(format),
      _ => None,
    }
  }

  /// Read the frame as a string slice if it can be parsed as a UTF-8 string without allocating.
  pub fn as_str(&self) -> Option<&str> {
    match self {
      OwnedFrame::SimpleError { data, .. } => Some(data),
      OwnedFrame::SimpleString { data, .. } => str::from_utf8(data).ok(),
      OwnedFrame::BlobError { data, .. }
      | OwnedFrame::BlobString { data, .. }
      | OwnedFrame::BigNumber { data, .. } => str::from_utf8(data).ok(),
      OwnedFrame::VerbatimString { data, .. } => str::from_utf8(data).ok(),
      OwnedFrame::ChunkedString(data) => str::from_utf8(data).ok(),
      _ => None,
    }
  }

  /// Read the frame as a `String` if it can be parsed as a UTF-8 string.
  pub fn to_string(&self) -> Option<String> {
    match self {
      OwnedFrame::SimpleError { data, .. } => Some(data.to_string()),
      OwnedFrame::SimpleString { data, .. } => String::from_utf8(data.to_vec()).ok(),
      OwnedFrame::BlobError { data, .. }
      | OwnedFrame::BlobString { data, .. }
      | OwnedFrame::BigNumber { data, .. } => String::from_utf8(data.to_vec()).ok(),
      OwnedFrame::VerbatimString { data, .. } => String::from_utf8(data.to_vec()).ok(),
      OwnedFrame::ChunkedString(b) => String::from_utf8(b.to_vec()).ok(),
      OwnedFrame::Double { data, .. } => Some(data.to_string()),
      OwnedFrame::Number { data, .. } => Some(data.to_string()),
      _ => None,
    }
  }

  /// Attempt to read the frame as a byte slice.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      OwnedFrame::SimpleError { data, .. } => Some(data.as_bytes()),
      OwnedFrame::SimpleString { data, .. } => Some(&data),
      OwnedFrame::BlobError { data, .. }
      | OwnedFrame::BlobString { data, .. }
      | OwnedFrame::BigNumber { data, .. } => Some(data),
      OwnedFrame::VerbatimString { data, .. } => Some(data),
      OwnedFrame::ChunkedString(b) => Some(b),
      _ => None,
    }
  }

  /// Move the frame contents into a new [BytesFrame].
  pub fn into_bytes_frame(self) -> BytesFrame {
    unimplemented!()
  }

  /// Read the number of bytes necessary to represent the frame and any associated attributes.
  pub fn encode_len(&self) -> usize {
    resp3_utils::owned_encode_len(self)
  }
}

/// An enum describing the possible data types in RESP3 along with the corresponding Rust data type to represent the
/// payload.
///
/// <https://github.com/antirez/RESP3/blob/master/spec.md>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BytesFrame {
  /// A blob of bytes.
  BlobString {
    data:       Bytes,
    attributes: Option<BytesAttributes>,
  },
  /// A blob representing an error.
  BlobError {
    data:       Bytes,
    attributes: Option<BytesAttributes>,
  },
  /// A small string.
  ///
  /// The internal data type is `Bytes` in order to support callers that use this interface to parse a `MONITOR`
  /// stream.
  SimpleString {
    data:       Bytes,
    attributes: Option<BytesAttributes>,
  },
  /// A small string representing an error.
  SimpleError {
    data:       Str,
    attributes: Option<BytesAttributes>,
  },
  /// A boolean type.
  Boolean {
    data:       bool,
    attributes: Option<BytesAttributes>,
  },
  /// A null type.
  Null,
  /// A signed 64-bit integer.
  Number {
    data:       i64,
    attributes: Option<BytesAttributes>,
  },
  /// A signed 64-bit floating point number.
  Double {
    data:       f64,
    attributes: Option<BytesAttributes>,
  },
  /// A large number not representable as a `Number` or `Double`.
  ///
  /// This library does not attempt to parse this.
  BigNumber {
    data:       Bytes,
    attributes: Option<BytesAttributes>,
  },
  /// A string to be displayed without any escaping or filtering.
  VerbatimString {
    data:       Bytes,
    format:     VerbatimStringFormat,
    attributes: Option<BytesAttributes>,
  },
  /// An array of frames.
  Array {
    data:       Vec<BytesFrame>,
    attributes: Option<BytesAttributes>,
  },
  /// An unordered map of key-value pairs.
  ///
  /// According to the spec keys can be any other RESP3 data type. However, it doesn't make sense to implement `Hash`
  /// for certain aggregate types. The [can_hash](crate::resp3::types::FrameKind::can_hash) function can be used to
  /// detect this.
  Map {
    data:       FrameMap<BytesFrame>,
    attributes: Option<BytesAttributes>,
  },
  /// An unordered collection of other frames with a uniqueness constraint.
  Set {
    data:       FrameSet<BytesFrame>,
    attributes: Option<BytesAttributes>,
  },
  /// Out-of-band data.
  Push {
    data:       Vec<BytesFrame>,
    attributes: Option<BytesAttributes>,
  },
  /// A special frame type used when first connecting to the server to describe the protocol version and optional
  /// credentials.
  Hello {
    version:  RespVersion,
    username: Option<Str>,
    password: Option<Str>,
  },
  /// One chunk of a streaming blob.
  ChunkedString(Bytes),
}

impl Hash for BytesFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    use self::BytesFrame::*;
    self.kind().hash_prefix().hash(state);

    match self {
      BlobString { data, .. } => data.hash(state),
      SimpleString { data, .. } => data.hash(state),
      SimpleError { data, .. } => data.hash(state),
      Number { data, .. } => data.hash(state),
      Null => NULL.hash(state),
      Double { data, .. } => data.to_string().hash(state),
      Boolean { data, .. } => data.hash(state),
      BlobError { data, .. } => data.hash(state),
      VerbatimString { data, format, .. } => {
        format.hash(state);
        data.hash(state);
      },
      ChunkedString(data) => data.hash(state),
      BigNumber { data, .. } => data.hash(state),
      _ => panic!("Invalid RESP3 data type to use as hash key."),
    };
  }
}

impl BytesFrame {
  /// Read the attributes attached to the frame.
  pub fn attributes(&self) -> Option<&BytesAttributes> {
    let attributes = match self {
      BytesFrame::Array { attributes, .. } => attributes,
      BytesFrame::Push { attributes, .. } => attributes,
      BytesFrame::BlobString { attributes, .. } => attributes,
      BytesFrame::BlobError { attributes, .. } => attributes,
      BytesFrame::BigNumber { attributes, .. } => attributes,
      BytesFrame::Boolean { attributes, .. } => attributes,
      BytesFrame::Number { attributes, .. } => attributes,
      BytesFrame::Double { attributes, .. } => attributes,
      BytesFrame::VerbatimString { attributes, .. } => attributes,
      BytesFrame::SimpleError { attributes, .. } => attributes,
      BytesFrame::SimpleString { attributes, .. } => attributes,
      BytesFrame::Set { attributes, .. } => attributes,
      BytesFrame::Map { attributes, .. } => attributes,
      BytesFrame::Null | BytesFrame::ChunkedString(_) | BytesFrame::Hello { .. } => return None,
    };

    attributes.as_ref()
  }

  /// Take the attributes off this frame.
  pub fn take_attributes(&mut self) -> Option<BytesAttributes> {
    let attributes = match self {
      BytesFrame::Array { attributes, .. } => attributes,
      BytesFrame::Push { attributes, .. } => attributes,
      BytesFrame::BlobString { attributes, .. } => attributes,
      BytesFrame::BlobError { attributes, .. } => attributes,
      BytesFrame::BigNumber { attributes, .. } => attributes,
      BytesFrame::Boolean { attributes, .. } => attributes,
      BytesFrame::Number { attributes, .. } => attributes,
      BytesFrame::Double { attributes, .. } => attributes,
      BytesFrame::VerbatimString { attributes, .. } => attributes,
      BytesFrame::SimpleError { attributes, .. } => attributes,
      BytesFrame::SimpleString { attributes, .. } => attributes,
      BytesFrame::Set { attributes, .. } => attributes,
      BytesFrame::Map { attributes, .. } => attributes,
      BytesFrame::Null | BytesFrame::ChunkedString(_) | BytesFrame::Hello { .. } => return None,
    };

    attributes.take()
  }

  /// Read a mutable reference to any attributes attached to the frame.
  pub fn attributes_mut(&mut self) -> Option<&mut BytesAttributes> {
    let attributes = match self {
      BytesFrame::Array { attributes, .. } => attributes,
      BytesFrame::Push { attributes, .. } => attributes,
      BytesFrame::BlobString { attributes, .. } => attributes,
      BytesFrame::BlobError { attributes, .. } => attributes,
      BytesFrame::BigNumber { attributes, .. } => attributes,
      BytesFrame::Boolean { attributes, .. } => attributes,
      BytesFrame::Number { attributes, .. } => attributes,
      BytesFrame::Double { attributes, .. } => attributes,
      BytesFrame::VerbatimString { attributes, .. } => attributes,
      BytesFrame::SimpleError { attributes, .. } => attributes,
      BytesFrame::SimpleString { attributes, .. } => attributes,
      BytesFrame::Set { attributes, .. } => attributes,
      BytesFrame::Map { attributes, .. } => attributes,
      BytesFrame::Null | BytesFrame::ChunkedString(_) | BytesFrame::Hello { .. } => return None,
    };

    attributes.as_mut()
  }

  /// Attempt to add attributes to the frame, extending the existing attributes if needed.
  pub fn add_attributes(&mut self, attributes: BytesAttributes) -> Result<(), RedisProtocolError> {
    let _attributes = match self {
      BytesFrame::Array { attributes, .. } => attributes,
      BytesFrame::Push { attributes, .. } => attributes,
      BytesFrame::BlobString { attributes, .. } => attributes,
      BytesFrame::BlobError { attributes, .. } => attributes,
      BytesFrame::BigNumber { attributes, .. } => attributes,
      BytesFrame::Boolean { attributes, .. } => attributes,
      BytesFrame::Number { attributes, .. } => attributes,
      BytesFrame::Double { attributes, .. } => attributes,
      BytesFrame::VerbatimString { attributes, .. } => attributes,
      BytesFrame::SimpleError { attributes, .. } => attributes,
      BytesFrame::SimpleString { attributes, .. } => attributes,
      BytesFrame::Set { attributes, .. } => attributes,
      BytesFrame::Map { attributes, .. } => attributes,
      BytesFrame::Null | BytesFrame::ChunkedString(_) | BytesFrame::Hello { .. } => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          format!("{:?} cannot have attributes.", self.kind()),
        ))
      },
    };

    if let Some(_attributes) = _attributes.as_mut() {
      _attributes.extend(attributes.into_iter());
    } else {
      *_attributes = Some(attributes);
    }

    Ok(())
  }

  /// Create a new `BytesFrame` that terminates a stream.
  pub fn new_end_stream() -> Self {
    BytesFrame::ChunkedString(Bytes::new())
  }

  /// A context-aware length function that returns the length of the inner frame contents.
  ///
  /// This does not return the encoded length, but rather the length of the contents of the frame such as the number
  /// of elements in an array, the size of any inner buffers, etc.
  ///
  /// Note: `Null` has a length of 0 and `Hello`, `Number`, `Double`, and `Boolean` have a length of 1.
  ///
  /// See [encode_len](Self::encode_len) to read the number of bytes necessary to encode the frame.
  pub fn len(&self) -> usize {
    use self::BytesFrame::*;

    match self {
      Array { data, .. } | Push { data, .. } => data.len(),
      BlobString { data, .. } | BlobError { data, .. } | BigNumber { data, .. } | ChunkedString(data) => data.len(),
      SimpleString { data, .. } => data.len(),
      SimpleError { data, .. } => data.len(),
      Number { .. } | Double { .. } | Boolean { .. } => 1,
      Null => 0,
      VerbatimString { data, .. } => data.len(),
      Map { data, .. } => data.len(),
      Set { data, .. } => data.len(),
      Hello { .. } => 1,
    }
  }

  /// Replace `self` with Null, returning the original value.
  pub fn take(&mut self) -> BytesFrame {
    mem::replace(self, BytesFrame::Null)
  }

  /// Read the associated `FrameKind`.
  pub fn kind(&self) -> FrameKind {
    use self::BytesFrame::*;

    match self {
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
      ChunkedString(inner) => {
        if inner.is_empty() {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      },
    }
  }

  /// Whether the frame is an array, map, or set.
  pub fn is_aggregate_type(&self) -> bool {
    matches!(
      self,
      BytesFrame::Map { .. } | BytesFrame::Set { .. } | BytesFrame::Array { .. }
    )
  }

  /// Whether the frame is an empty chunked string, signifying the end of a chunked string stream.
  pub fn is_end_stream_frame(&self) -> bool {
    match self {
      BytesFrame::ChunkedString(s) => s.is_empty(),
      _ => false,
    }
  }

  /// If the frame is a verbatim string then read the associated format.
  pub fn verbatim_string_format(&self) -> Option<&VerbatimStringFormat> {
    match self {
      BytesFrame::VerbatimString { format, .. } => Some(format),
      _ => None,
    }
  }

  /// Read the frame as a string slice if it can be parsed as a UTF-8 string without allocating.
  pub fn as_str(&self) -> Option<&str> {
    match self {
      BytesFrame::SimpleError { data, .. } => Some(data),
      BytesFrame::SimpleString { data, .. } => str::from_utf8(data).ok(),
      BytesFrame::BlobError { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BigNumber { data, .. } => str::from_utf8(data).ok(),
      BytesFrame::VerbatimString { data, .. } => str::from_utf8(data).ok(),
      BytesFrame::ChunkedString(data) => str::from_utf8(data).ok(),
      _ => None,
    }
  }

  /// Read the frame as a `String` if it can be parsed as a UTF-8 string.
  pub fn to_string(&self) -> Option<String> {
    match self {
      BytesFrame::SimpleError { data, .. } => Some(data.to_string()),
      BytesFrame::SimpleString { data, .. } => String::from_utf8(data.to_vec()).ok(),
      BytesFrame::BlobError { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BigNumber { data, .. } => String::from_utf8(data.to_vec()).ok(),
      BytesFrame::VerbatimString { data, .. } => String::from_utf8(data.to_vec()).ok(),
      BytesFrame::ChunkedString(b) => String::from_utf8(b.to_vec()).ok(),
      BytesFrame::Double { data, .. } => Some(data.to_string()),
      BytesFrame::Number { data, .. } => Some(data.to_string()),
      _ => None,
    }
  }

  /// Attempt to read the frame as a byte slice.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      BytesFrame::SimpleError { data, .. } => Some(data.as_bytes()),
      BytesFrame::SimpleString { data, .. } => Some(&data),
      BytesFrame::BlobError { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BigNumber { data, .. } => Some(data),
      BytesFrame::VerbatimString { data, .. } => Some(data),
      BytesFrame::ChunkedString(b) => Some(b),
      _ => None,
    }
  }

  /// Read the number of bytes necessary to represent the frame and any associated attributes.
  pub fn encode_len(&self) -> usize {
    resp3_utils::bytes_encode_len(self)
  }

  /// Copy the frame contents into a new [OwnedFrame].
  pub fn to_owned_frame(&self) -> OwnedFrame {
    unimplemented!()
  }
}

/// An owned internal enum for the various frame representations.
#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) enum Owned {
  Owned(OwnedFrame),
  Bytes(BytesFrame),
}
// impl from owned

impl Owned {
  /// TODO docs
  pub(crate) fn is_end_stream_frame(&self) -> bool {
    match self {
      Owned::Owned(f) => f.is_end_stream_frame(),
      Owned::Bytes(f) => f.is_end_stream_frame(),
    }
  }
}

/// A convenience struct for the various frame representations.
pub enum Borrowed<'a> {
  Owned(&'a OwnedFrame),
  Bytes(&'a BytesFrame),
}
// TODO impl From refs

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
///   // BytesFrame::Array { data: [1, 2], attributes: None }
///   println!("{:?}", result);
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct StreamedFrame {
  /// The internal buffer of frames and attributes.
  buffer:         VecDeque<Owned>,
  /// Any leading attributes before the stream starts.
  pub attributes: Option<FrameMap<Owned>>,
  /// The data type being streamed.
  pub kind:       FrameKind,
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
  pub fn into_bytes_frame(&mut self) -> Result<BytesFrame, RedisProtocolError> {
    if !self.kind.is_streaming_type() {
      // try to catch invalid type errors early so the caller can modify the frame before we clear the buffer
      return Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Only blob strings, sets, maps, and arrays can be streamed.",
      ));
    }

    if self.is_finished() {
      // the last frame is an empty chunked string when the stream is finished
      self.buffer.pop_back();
    }
    let buffer = mem::replace(&mut self.buffer, VecDeque::new());
    let attributes = self.attributes.take();

    let mut frame = match self.kind {
      FrameKind::BlobString => resp3_utils::reconstruct_blobstring(buffer)?,
      FrameKind::Map => resp3_utils::reconstruct_map(buffer)?,
      FrameKind::Set => resp3_utils::reconstruct_set(buffer)?,
      FrameKind::Array => resp3_utils::reconstruct_array(buffer)?,
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Streaming frames only supported for blob strings, maps, sets, and arrays.",
        ))
      },
    };
    frame.add_attributes(attributes);

    Ok(frame)
  }

  /// Convert the internal buffer into one frame matching `self.kind`, clearing the internal buffer.
  pub fn into_owned_frame(&mut self) -> Result<(), RedisProtocolError> {
    if !self.kind.is_streaming_type() {
      // try to catch invalid type errors early so the caller can modify the frame before we clear the buffer
      return Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Only blob strings, sets, maps, and arrays can be streamed.",
      ));
    }

    if self.is_finished() {
      // the last frame is an empty chunked string when the stream is finished
      self.buffer.pop_back();
    }
    unimplemented!()
  }

  /// Add a frame to the internal buffer.
  pub fn add_bytes_frame(&mut self, data: BytesFrame) {
    self.buffer.push_back(data.into());
  }

  /// Add a frame to the internal buffer.
  pub fn add_owned_frame(&mut self, data: OwnedFrame) {
    self.buffer.push_back(data.into());
  }

  /// Whether the last frame represents the terminating sequence at the end of a frame stream.
  pub fn is_finished(&self) -> bool {
    self.buffer.back().map(|f| f.is_end_stream_frame()).unwrap_or(false)
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
    streaming_buf.add_frame(BytesFrame::new_end_stream());
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
    streaming_buf.add_frame(BytesFrame::new_end_stream());

    let frame = streaming_buf
      .into_frame()
      .expect("Failed to build frame from chunked stream");

    assert_eq!(frame.as_str(), Some("foobarbaz"));
    assert_eq!(frame.attributes(), Some(&attributes));
  }
}
