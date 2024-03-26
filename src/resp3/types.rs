use crate::{
  error::{RedisProtocolError, RedisProtocolErrorKind},
  resp3::utils as resp3_utils,
  types::{_Range, PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX, PUBSUB_PUSH_PREFIX, SHARD_PUBSUB_PREFIX},
  utils,
};
use alloc::{
  collections::VecDeque,
  format,
  string::{String, ToString},
  vec::Vec,
};
use core::{
  convert::TryFrom,
  fmt::Debug,
  hash::{Hash, Hasher},
  mem,
  str,
};

#[cfg(feature = "convert")]
use crate::convert::FromResp3;
#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "bytes")]
use bytes_utils::Str;

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

/// A map struct for frames that uses either `indexmap::IndexMap`, `hashbrown::HashMap`, or
/// `std::collections::HashMap` depending on the enabled feature flags.
#[cfg(not(feature = "index-map"))]
pub type FrameMap<K, V> = HashMap<K, V>;
/// A map struct for frames that uses either `indexmap::IndexSet`, `hashbrown::HashSet`, or
/// `std::collections::HashSet` depending on the enabled feature flags.
#[cfg(not(feature = "index-map"))]
pub type FrameSet<T> = HashSet<T>;
/// A map struct for frames that uses either `indexmap::IndexMap`, `hashbrown::HashMap`, or
/// `std::collections::HashMap` depending on the enabled feature flags.
#[cfg(feature = "index-map")]
pub type FrameMap<K, V> = IndexMap<K, V>;
/// A map struct for frames that uses either `indexmap::IndexSet`, `hashbrown::HashSet`, or
/// `std::collections::HashSet` depending on the enabled feature flags.
#[cfg(feature = "index-map")]
pub type FrameSet<T> = IndexSet<T>;

/// Additional information returned alongside a [BytesFrame].
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub type BytesAttributes = FrameMap<BytesFrame, BytesFrame>;
/// Additional information returned alongside an [OwnedFrame].
pub type OwnedAttributes = FrameMap<OwnedFrame, OwnedFrame>;
/// Additional information returned alongside an [RangeFrame].
pub type RangeAttributes = FrameMap<RangeFrame, RangeFrame>;

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

  /// A function used to differentiate data types that may have the same inner binary representation when hashing.
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
}

/// A reference-free frame type representing ranges into an associated buffer, typically used to implement zero-copy
/// parsing.
#[derive(Clone, Debug, PartialEq)]
pub enum RangeFrame {
  /// A blob of bytes.
  BlobString {
    data:       _Range,
    attributes: Option<RangeAttributes>,
  },
  /// A blob representing an error.
  BlobError {
    data:       _Range,
    attributes: Option<RangeAttributes>,
  },
  /// A small string.
  SimpleString {
    data:       _Range,
    attributes: Option<RangeAttributes>,
  },
  /// A small string representing an error.
  SimpleError {
    data:       _Range,
    attributes: Option<RangeAttributes>,
  },
  /// A boolean type.
  Boolean {
    data:       bool,
    attributes: Option<RangeAttributes>,
  },
  /// A null type.
  Null,
  /// A signed 64-bit integer.
  Number {
    data:       i64,
    attributes: Option<RangeAttributes>,
  },
  /// A signed 64-bit floating point number.
  Double {
    data:       f64,
    attributes: Option<RangeAttributes>,
  },
  /// A large number not representable as a `Number` or `Double`.
  BigNumber {
    data:       _Range,
    attributes: Option<RangeAttributes>,
  },
  /// A string to be displayed without any escaping or filtering.
  VerbatimString {
    data:       _Range,
    format:     VerbatimStringFormat,
    attributes: Option<RangeAttributes>,
  },
  /// An array of frames.
  Array {
    data:       Vec<RangeFrame>,
    attributes: Option<RangeAttributes>,
  },
  /// An unordered map of key-value pairs.
  Map {
    data:       FrameMap<RangeFrame, RangeFrame>,
    attributes: Option<RangeAttributes>,
  },
  /// An unordered collection of other frames with a uniqueness constraint.
  Set {
    data:       FrameSet<RangeFrame>,
    attributes: Option<RangeAttributes>,
  },
  /// Out-of-band data.
  Push {
    data:       Vec<RangeFrame>,
    attributes: Option<RangeAttributes>,
  },
  /// A special frame type used when first connecting to the server to describe the protocol version and optional
  /// credentials.
  Hello {
    version:  RespVersion,
    username: Option<_Range>,
    password: Option<_Range>,
  },
  /// One chunk of a streaming blob.
  ChunkedString(_Range),
}

impl Eq for RangeFrame {}

impl RangeFrame {
  /// Read the associated `FrameKind`.
  pub fn kind(&self) -> FrameKind {
    match self {
      RangeFrame::Array { .. } => FrameKind::Array,
      RangeFrame::BlobString { .. } => FrameKind::BlobString,
      RangeFrame::SimpleString { .. } => FrameKind::SimpleString,
      RangeFrame::SimpleError { .. } => FrameKind::SimpleError,
      RangeFrame::Number { .. } => FrameKind::Number,
      RangeFrame::Null => FrameKind::Null,
      RangeFrame::Double { .. } => FrameKind::Double,
      RangeFrame::BlobError { .. } => FrameKind::BlobError,
      RangeFrame::VerbatimString { .. } => FrameKind::VerbatimString,
      RangeFrame::Boolean { .. } => FrameKind::Boolean,
      RangeFrame::Map { .. } => FrameKind::Map,
      RangeFrame::Set { .. } => FrameKind::Set,
      RangeFrame::Push { .. } => FrameKind::Push,
      RangeFrame::Hello { .. } => FrameKind::Hello,
      RangeFrame::BigNumber { .. } => FrameKind::BigNumber,
      RangeFrame::ChunkedString(inner) => {
        if inner.1 == 0 && inner.0 == 0 {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      },
    }
  }

  /// A context-aware length function that returns the length of the inner frame contents.
  ///
  /// This does not return the encoded length, but rather the length of the contents of the frame such as the number
  /// of elements in an array, the size of any inner buffers, etc.
  ///
  /// Note: `Null` has a length of 0 and `Hello`, `Number`, `Double`, and `Boolean` have a length of 1.
  pub fn len(&self) -> usize {
    match self {
      RangeFrame::Array { data, .. } | RangeFrame::Push { data, .. } => data.len(),
      RangeFrame::BlobString { data, .. }
      | RangeFrame::BlobError { data, .. }
      | RangeFrame::BigNumber { data, .. }
      | RangeFrame::ChunkedString(data) => data.1 - data.0,
      RangeFrame::SimpleString { data, .. } => data.1 - data.0,
      RangeFrame::SimpleError { data, .. } => data.1 - data.0,
      RangeFrame::Number { .. } | RangeFrame::Double { .. } | RangeFrame::Boolean { .. } => 1,
      RangeFrame::Null => 0,
      RangeFrame::VerbatimString { data, .. } => data.1 - data.0,
      RangeFrame::Map { data, .. } => data.len(),
      RangeFrame::Set { data, .. } => data.len(),
      RangeFrame::Hello { .. } => 1,
    }
  }

  /// Add attributes to the frame.
  pub fn add_attributes(&mut self, attributes: RangeAttributes) -> Result<(), RedisProtocolError> {
    let _attributes = match self {
      RangeFrame::Array { attributes, .. } => attributes,
      RangeFrame::Push { attributes, .. } => attributes,
      RangeFrame::BlobString { attributes, .. } => attributes,
      RangeFrame::BlobError { attributes, .. } => attributes,
      RangeFrame::BigNumber { attributes, .. } => attributes,
      RangeFrame::Boolean { attributes, .. } => attributes,
      RangeFrame::Number { attributes, .. } => attributes,
      RangeFrame::Double { attributes, .. } => attributes,
      RangeFrame::VerbatimString { attributes, .. } => attributes,
      RangeFrame::SimpleError { attributes, .. } => attributes,
      RangeFrame::SimpleString { attributes, .. } => attributes,
      RangeFrame::Set { attributes, .. } => attributes,
      RangeFrame::Map { attributes, .. } => attributes,
      RangeFrame::Null | RangeFrame::ChunkedString(_) | RangeFrame::Hello { .. } => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          format!("{:?} cannot have attributes.", self.kind()),
        ))
      },
    };

    if let Some(_attributes) = _attributes.as_mut() {
      _attributes.extend(attributes);
    } else {
      *_attributes = Some(attributes);
    }

    Ok(())
  }

  pub fn new_end_stream() -> Self {
    RangeFrame::ChunkedString((0, 0))
  }
}

impl Hash for RangeFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    use self::RangeFrame::*;
    self.kind().hash_prefix().hash(state);

    match self {
      BlobString { data, .. } => resp3_utils::hash_tuple(state, data),
      SimpleString { data, .. } => resp3_utils::hash_tuple(state, data),
      SimpleError { data, .. } => resp3_utils::hash_tuple(state, data),
      Number { data, .. } => data.hash(state),
      Null => NULL.hash(state),
      Double { data, .. } => data.to_string().hash(state),
      Boolean { data, .. } => data.hash(state),
      BlobError { data, .. } => resp3_utils::hash_tuple(state, data),
      VerbatimString { data, format, .. } => {
        format.hash(state);
        resp3_utils::hash_tuple(state, data);
      },
      ChunkedString(data) => resp3_utils::hash_tuple(state, data),
      BigNumber { data, .. } => resp3_utils::hash_tuple(state, data),
      _ => panic!("Invalid RESP3 data type to use as hash key."),
    };
  }
}

/// A frame type representing ranges into an associated buffer that contains the starting portion of a streaming
/// frame.
#[derive(Debug)]
pub struct StreamedRangeFrame {
  pub kind:       FrameKind,
  pub attributes: Option<RangeAttributes>,
}

impl StreamedRangeFrame {
  /// Create a new streamed frame with the provided frame type.
  pub fn new(kind: FrameKind) -> Self {
    StreamedRangeFrame { kind, attributes: None }
  }

  /// Add range attributes to the frame.
  pub fn add_attributes(&mut self, attributes: RangeAttributes) {
    if let Some(_attributes) = self.attributes.as_mut() {
      _attributes.extend(attributes);
    } else {
      self.attributes = Some(attributes);
    }
  }
}

/// A reference-free frame type representing ranges into an associated buffer, typically used to implement zero-copy
/// parsing.
#[derive(Debug)]
pub enum DecodedRangeFrame {
  Complete(RangeFrame),
  Streaming(StreamedRangeFrame),
}

impl DecodedRangeFrame {
  /// Add attributes to the decoded frame, if possible.
  pub fn add_attributes(&mut self, attributes: RangeAttributes) -> Result<(), RedisProtocolError> {
    match self {
      DecodedRangeFrame::Streaming(inner) => {
        inner.add_attributes(attributes);
        Ok(())
      },
      DecodedRangeFrame::Complete(inner) => inner.add_attributes(attributes),
    }
  }

  /// Convert the decoded frame to a complete frame, returning an error if a streaming variant is found.
  pub fn into_complete_frame(self) -> Result<RangeFrame, RedisProtocolError> {
    match self {
      DecodedRangeFrame::Complete(frame) => Ok(frame),
      DecodedRangeFrame::Streaming(_) => Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected complete frame.",
      )),
    }
  }
}

/// Generic operations on a RESP3 frame.
pub trait Resp3Frame: Debug + Hash + Eq + Sized {
  type Attributes;

  /// Create the target aggregate type based on a buffered set of chunked frames.
  fn from_buffer(
    target: FrameKind,
    buf: impl IntoIterator<Item = Self>,
    attributes: Option<Self::Attributes>,
  ) -> Result<Self, RedisProtocolError>;

  /// Read the attributes attached to the frame.
  fn attributes(&self) -> Option<&Self::Attributes>;

  /// Take the attributes off this frame.
  fn take_attributes(&mut self) -> Option<Self::Attributes>;

  /// Read a mutable reference to any attributes attached to the frame.
  fn attributes_mut(&mut self) -> Option<&mut Self::Attributes>;

  /// Attempt to add attributes to the frame, extending the existing attributes if needed.
  fn add_attributes(&mut self, attributes: Self::Attributes) -> Result<(), RedisProtocolError>;

  /// Create a new frame that terminates a stream.
  fn new_end_stream() -> Self;

  /// Create a new empty frame with attribute support.
  fn new_empty() -> Self;

  /// A context-aware length function that returns the length of the inner frame contents.
  ///
  /// This does not return the encoded length, but rather the length of the contents of the frame such as the number
  /// of elements in an array, the size of any inner buffers, etc.
  ///
  /// Note: `Null` has a length of 0 and `Hello`, `Number`, `Double`, and `Boolean` have a length of 1.
  ///
  /// See [encode_len](Self::encode_len) to read the number of bytes necessary to encode the frame.
  fn len(&self) -> usize;

  /// Replace `self` with Null, returning the original value.
  fn take(&mut self) -> Self;

  /// Read the associated `FrameKind`.
  fn kind(&self) -> FrameKind;

  /// Whether the frame is an empty chunked string, signifying the end of a chunked string stream.
  fn is_end_stream_frame(&self) -> bool;

  /// If the frame is a verbatim string then read the associated format.
  fn verbatim_string_format(&self) -> Option<&VerbatimStringFormat>;

  /// Read the frame as a string slice if it can be parsed as a UTF-8 string without allocating.
  fn as_str(&self) -> Option<&str>;

  /// Attempt to convert the frame to a bool.
  fn as_bool(&self) -> Option<bool>;

  /// Read the frame as a `String` if it can be parsed as a UTF-8 string.
  fn to_string(&self) -> Option<String>;

  /// Attempt to read the frame as a byte slice.
  fn as_bytes(&self) -> Option<&[u8]>;

  /// Read the number of bytes necessary to represent the frame and any associated attributes.
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
    T: FromResp3<Self>,
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

/// An enum describing a RESP3 frame that uses owned byte containers.
///
/// <https://github.com/antirez/RESP3/blob/master/spec.md>
#[derive(Clone, Debug, PartialEq)]
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
    data:       FrameMap<OwnedFrame, OwnedFrame>,
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

impl Eq for OwnedFrame {}

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

impl Resp3Frame for OwnedFrame {
  type Attributes = OwnedAttributes;

  fn from_buffer(
    target: FrameKind,
    buf: impl IntoIterator<Item = Self>,
    attributes: Option<Self::Attributes>,
  ) -> Result<Self, RedisProtocolError> {
    let mut data: Vec<_> = buf.into_iter().collect();

    Ok(match target {
      FrameKind::BlobString => {
        let total_len = data.iter().fold(0, |m, f| m + f.len());
        let mut buf = Vec::with_capacity(total_len);
        for frame in data.into_iter() {
          buf.extend(match frame {
            OwnedFrame::ChunkedString(chunk) => chunk,
            OwnedFrame::BlobString { data, .. } => data,
            _ => {
              return Err(RedisProtocolError::new(
                RedisProtocolErrorKind::DecodeError,
                "Expected chunked or blob string.",
              ));
            },
          });
        }

        OwnedFrame::BlobString { data: buf, attributes }
      },
      FrameKind::Map => OwnedFrame::Map {
        attributes,
        data: data
          .chunks_exact_mut(2)
          .map(|chunk| (chunk[0].take(), chunk[1].take()))
          .collect(),
      },
      FrameKind::Set => OwnedFrame::Set {
        attributes,
        data: data.into_iter().collect(),
      },
      FrameKind::Array => OwnedFrame::Array { attributes, data },
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Streaming frames only supported for blob strings, maps, sets, and arrays.",
        ))
      },
    })
  }

  fn add_attributes(&mut self, attributes: Self::Attributes) -> Result<(), RedisProtocolError> {
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
      _attributes.extend(attributes);
    } else {
      *_attributes = Some(attributes);
    }

    Ok(())
  }

  fn attributes(&self) -> Option<&Self::Attributes> {
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

  fn attributes_mut(&mut self) -> Option<&mut Self::Attributes> {
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

  fn take_attributes(&mut self) -> Option<Self::Attributes> {
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

  fn new_end_stream() -> Self {
    OwnedFrame::ChunkedString(Vec::new())
  }

  fn new_empty() -> Self {
    OwnedFrame::Number {
      data:       0,
      attributes: None,
    }
  }

  fn len(&self) -> usize {
    match self {
      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => data.len(),
      OwnedFrame::BlobString { data, .. }
      | OwnedFrame::BlobError { data, .. }
      | OwnedFrame::BigNumber { data, .. }
      | OwnedFrame::ChunkedString(data) => data.len(),
      OwnedFrame::SimpleString { data, .. } => data.len(),
      OwnedFrame::SimpleError { data, .. } => data.len(),
      OwnedFrame::Number { .. } | OwnedFrame::Double { .. } | OwnedFrame::Boolean { .. } => 1,
      OwnedFrame::Null => 0,
      OwnedFrame::VerbatimString { data, .. } => data.len(),
      OwnedFrame::Map { data, .. } => data.len(),
      OwnedFrame::Set { data, .. } => data.len(),
      OwnedFrame::Hello { .. } => 1,
    }
  }

  fn take(&mut self) -> Self {
    mem::replace(self, OwnedFrame::Null)
  }

  fn kind(&self) -> FrameKind {
    match self {
      OwnedFrame::Array { .. } => FrameKind::Array,
      OwnedFrame::BlobString { .. } => FrameKind::BlobString,
      OwnedFrame::SimpleString { .. } => FrameKind::SimpleString,
      OwnedFrame::SimpleError { .. } => FrameKind::SimpleError,
      OwnedFrame::Number { .. } => FrameKind::Number,
      OwnedFrame::Null => FrameKind::Null,
      OwnedFrame::Double { .. } => FrameKind::Double,
      OwnedFrame::BlobError { .. } => FrameKind::BlobError,
      OwnedFrame::VerbatimString { .. } => FrameKind::VerbatimString,
      OwnedFrame::Boolean { .. } => FrameKind::Boolean,
      OwnedFrame::Map { .. } => FrameKind::Map,
      OwnedFrame::Set { .. } => FrameKind::Set,
      OwnedFrame::Push { .. } => FrameKind::Push,
      OwnedFrame::Hello { .. } => FrameKind::Hello,
      OwnedFrame::BigNumber { .. } => FrameKind::BigNumber,
      OwnedFrame::ChunkedString(inner) => {
        if inner.is_empty() {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      },
    }
  }

  fn is_end_stream_frame(&self) -> bool {
    match self {
      OwnedFrame::ChunkedString(s) => s.is_empty(),
      _ => false,
    }
  }

  fn verbatim_string_format(&self) -> Option<&VerbatimStringFormat> {
    match self {
      OwnedFrame::VerbatimString { format, .. } => Some(format),
      _ => None,
    }
  }

  fn as_str(&self) -> Option<&str> {
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

  fn as_bool(&self) -> Option<bool> {
    match self {
      OwnedFrame::SimpleString { data, .. }
      | OwnedFrame::BlobString { data, .. }
      | OwnedFrame::VerbatimString { data, .. } => utils::bytes_to_bool(data),
      OwnedFrame::ChunkedString(data) => utils::bytes_to_bool(data),
      OwnedFrame::Boolean { data, .. } => Some(*data),
      OwnedFrame::Number { data, .. } => match data {
        0 => Some(false),
        1 => Some(true),
        _ => None,
      },
      _ => None,
    }
  }

  fn to_string(&self) -> Option<String> {
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

  fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      OwnedFrame::SimpleError { data, .. } => Some(data.as_bytes()),
      OwnedFrame::SimpleString { data, .. } => Some(data),
      OwnedFrame::BlobError { data, .. }
      | OwnedFrame::BlobString { data, .. }
      | OwnedFrame::BigNumber { data, .. } => Some(data),
      OwnedFrame::VerbatimString { data, .. } => Some(data),
      OwnedFrame::ChunkedString(b) => Some(b),
      _ => None,
    }
  }

  fn encode_len(&self) -> usize {
    resp3_utils::owned_encode_len(self)
  }

  fn is_normal_pubsub_message(&self) -> bool {
    // format is ["pubsub", "message", <channel>, <message>]
    match self {
      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => {
        data.len() == 4
          && data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_pattern_pubsub_message(&self) -> bool {
    // format is ["pubsub", "pmessage", <pattern>, <channel>, <message>]
    match self {
      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => {
        data.len() == 5
          && data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == PATTERN_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_shard_pubsub_message(&self) -> bool {
    // format is ["pubsub", "pmessage", <channel>, <message>]
    match self {
      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => {
        data.len() == 4
          && data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == SHARD_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn is_single_element_vec(&self) -> bool {
    match self {
      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => data.len() == 1,
      _ => false,
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn pop_or_take(self) -> Self {
    match self {
      OwnedFrame::Array { mut data, .. } | OwnedFrame::Push { mut data, .. } => data.pop().unwrap(),
      _ => self,
    }
  }
}

impl OwnedFrame {
  /// Move the frame contents into a new [BytesFrame].
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn into_bytes_frame(self) -> BytesFrame {
    resp3_utils::owned_to_bytes_frame(self)
  }
}

impl<B: Into<Vec<u8>>> TryFrom<(FrameKind, B)> for OwnedFrame {
  type Error = RedisProtocolError;

  fn try_from((kind, buf): (FrameKind, B)) -> Result<Self, Self::Error> {
    Ok(match kind {
      FrameKind::SimpleString => OwnedFrame::SimpleString {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::SimpleError => OwnedFrame::SimpleError {
        data:       String::from_utf8(buf.into())?,
        attributes: None,
      },
      FrameKind::BlobString => OwnedFrame::BlobString {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::BlobError => OwnedFrame::BlobError {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::BigNumber => OwnedFrame::BigNumber {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::ChunkedString => OwnedFrame::ChunkedString(buf.into()),
      FrameKind::Null => OwnedFrame::Null,
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Cannot convert to frame.",
        ))
      },
    })
  }
}

impl From<i64> for OwnedFrame {
  fn from(value: i64) -> Self {
    OwnedFrame::Number {
      data:       value,
      attributes: None,
    }
  }
}

impl From<bool> for OwnedFrame {
  fn from(value: bool) -> Self {
    OwnedFrame::Boolean {
      data:       value,
      attributes: None,
    }
  }
}

impl From<f64> for OwnedFrame {
  fn from(value: f64) -> Self {
    OwnedFrame::Double {
      data:       value,
      attributes: None,
    }
  }
}

/// A RESP3 frame that uses [Bytes] and [Str] as the underlying buffer type.
///
/// <https://github.com/antirez/RESP3/blob/master/spec.md>
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
#[derive(Clone, Debug, PartialEq)]
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
    data:       FrameMap<BytesFrame, BytesFrame>,
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

#[cfg(feature = "bytes")]
impl<B: Into<Bytes>> TryFrom<(FrameKind, B)> for BytesFrame {
  type Error = RedisProtocolError;

  fn try_from((kind, buf): (FrameKind, B)) -> Result<Self, Self::Error> {
    Ok(match kind {
      FrameKind::SimpleString => BytesFrame::SimpleString {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::SimpleError => BytesFrame::SimpleError {
        data:       Str::from_inner(buf.into())?,
        attributes: None,
      },
      FrameKind::BlobString => BytesFrame::BlobString {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::BlobError => BytesFrame::BlobError {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::BigNumber => BytesFrame::BigNumber {
        data:       buf.into(),
        attributes: None,
      },
      FrameKind::ChunkedString => BytesFrame::ChunkedString(buf.into()),
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

#[cfg(feature = "bytes")]
impl From<i64> for BytesFrame {
  fn from(value: i64) -> Self {
    BytesFrame::Number {
      data:       value,
      attributes: None,
    }
  }
}

#[cfg(feature = "bytes")]
impl From<bool> for BytesFrame {
  fn from(value: bool) -> Self {
    BytesFrame::Boolean {
      data:       value,
      attributes: None,
    }
  }
}

#[cfg(feature = "bytes")]
impl From<f64> for BytesFrame {
  fn from(value: f64) -> Self {
    BytesFrame::Double {
      data:       value,
      attributes: None,
    }
  }
}

#[cfg(feature = "bytes")]
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

#[cfg(feature = "bytes")]
impl Eq for BytesFrame {}

#[cfg(feature = "bytes")]
impl Resp3Frame for BytesFrame {
  type Attributes = BytesAttributes;

  fn from_buffer(
    target: FrameKind,
    buf: impl IntoIterator<Item = Self>,
    attributes: Option<Self::Attributes>,
  ) -> Result<Self, RedisProtocolError> {
    let mut data: Vec<_> = buf.into_iter().collect();

    Ok(match target {
      FrameKind::BlobString => {
        let total_len = data.iter().fold(0, |m, f| m + f.len());
        let mut buf = BytesMut::with_capacity(total_len);
        for frame in data.into_iter() {
          buf.extend(match frame {
            BytesFrame::ChunkedString(chunk) => chunk,
            BytesFrame::BlobString { data, .. } => data,
            _ => {
              return Err(RedisProtocolError::new(
                RedisProtocolErrorKind::DecodeError,
                "Expected chunked or blob string.",
              ));
            },
          });
        }

        BytesFrame::BlobString {
          data: buf.freeze(),
          attributes,
        }
      },
      FrameKind::Map => BytesFrame::Map {
        attributes,
        data: data
          .chunks_exact_mut(2)
          .map(|chunk| (chunk[0].take(), chunk[1].take()))
          .collect(),
      },
      FrameKind::Set => BytesFrame::Set {
        attributes,
        data: data.into_iter().collect(),
      },
      FrameKind::Array => BytesFrame::Array { attributes, data },
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Streaming frames only supported for blob strings, maps, sets, and arrays.",
        ))
      },
    })
  }

  fn attributes(&self) -> Option<&Self::Attributes> {
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

  fn take_attributes(&mut self) -> Option<Self::Attributes> {
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

  fn attributes_mut(&mut self) -> Option<&mut Self::Attributes> {
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
  fn add_attributes(&mut self, attributes: Self::Attributes) -> Result<(), RedisProtocolError> {
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
      _attributes.extend(attributes);
    } else {
      *_attributes = Some(attributes);
    }

    Ok(())
  }

  fn new_empty() -> Self {
    BytesFrame::Number {
      data:       0,
      attributes: None,
    }
  }

  fn new_end_stream() -> Self {
    BytesFrame::ChunkedString(Bytes::new())
  }

  fn len(&self) -> usize {
    match self {
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => data.len(),
      BytesFrame::BlobString { data, .. }
      | BytesFrame::BlobError { data, .. }
      | BytesFrame::BigNumber { data, .. }
      | BytesFrame::ChunkedString(data) => data.len(),
      BytesFrame::SimpleString { data, .. } => data.len(),
      BytesFrame::SimpleError { data, .. } => data.len(),
      BytesFrame::Number { .. } | BytesFrame::Double { .. } | BytesFrame::Boolean { .. } => 1,
      BytesFrame::Null => 0,
      BytesFrame::VerbatimString { data, .. } => data.len(),
      BytesFrame::Map { data, .. } => data.len(),
      BytesFrame::Set { data, .. } => data.len(),
      BytesFrame::Hello { .. } => 1,
    }
  }

  fn take(&mut self) -> BytesFrame {
    mem::replace(self, BytesFrame::Null)
  }

  fn kind(&self) -> FrameKind {
    match self {
      BytesFrame::Array { .. } => FrameKind::Array,
      BytesFrame::BlobString { .. } => FrameKind::BlobString,
      BytesFrame::SimpleString { .. } => FrameKind::SimpleString,
      BytesFrame::SimpleError { .. } => FrameKind::SimpleError,
      BytesFrame::Number { .. } => FrameKind::Number,
      BytesFrame::Null => FrameKind::Null,
      BytesFrame::Double { .. } => FrameKind::Double,
      BytesFrame::BlobError { .. } => FrameKind::BlobError,
      BytesFrame::VerbatimString { .. } => FrameKind::VerbatimString,
      BytesFrame::Boolean { .. } => FrameKind::Boolean,
      BytesFrame::Map { .. } => FrameKind::Map,
      BytesFrame::Set { .. } => FrameKind::Set,
      BytesFrame::Push { .. } => FrameKind::Push,
      BytesFrame::Hello { .. } => FrameKind::Hello,
      BytesFrame::BigNumber { .. } => FrameKind::BigNumber,
      BytesFrame::ChunkedString(inner) => {
        if inner.is_empty() {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      },
    }
  }

  fn is_end_stream_frame(&self) -> bool {
    match self {
      BytesFrame::ChunkedString(s) => s.is_empty(),
      _ => false,
    }
  }

  fn verbatim_string_format(&self) -> Option<&VerbatimStringFormat> {
    match self {
      BytesFrame::VerbatimString { format, .. } => Some(format),
      _ => None,
    }
  }

  fn as_str(&self) -> Option<&str> {
    match self {
      BytesFrame::SimpleError { data, .. } => Some(data),
      BytesFrame::SimpleString { data, .. }
      | BytesFrame::BlobError { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BigNumber { data, .. } => str::from_utf8(data).ok(),
      BytesFrame::VerbatimString { data, .. } => str::from_utf8(data).ok(),
      BytesFrame::ChunkedString(data) => str::from_utf8(data).ok(),
      _ => None,
    }
  }

  fn as_bool(&self) -> Option<bool> {
    match self {
      BytesFrame::SimpleString { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::VerbatimString { data, .. } => utils::bytes_to_bool(data),
      BytesFrame::ChunkedString(data) => utils::bytes_to_bool(data),
      BytesFrame::Boolean { data, .. } => Some(*data),
      BytesFrame::Number { data, .. } => match data {
        0 => Some(false),
        1 => Some(true),
        _ => None,
      },
      _ => None,
    }
  }

  fn to_string(&self) -> Option<String> {
    match self {
      BytesFrame::SimpleError { data, .. } => Some(data.to_string()),
      BytesFrame::SimpleString { data, .. }
      | BytesFrame::BlobError { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BigNumber { data, .. } => String::from_utf8(data.to_vec()).ok(),
      BytesFrame::VerbatimString { data, .. } => String::from_utf8(data.to_vec()).ok(),
      BytesFrame::ChunkedString(b) => String::from_utf8(b.to_vec()).ok(),
      BytesFrame::Double { data, .. } => Some(data.to_string()),
      BytesFrame::Number { data, .. } => Some(data.to_string()),
      _ => None,
    }
  }

  fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      BytesFrame::SimpleError { data, .. } => Some(data.as_bytes()),
      BytesFrame::SimpleString { data, .. } => Some(data),
      BytesFrame::BlobError { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BigNumber { data, .. } => Some(data),
      BytesFrame::VerbatimString { data, .. } => Some(data),
      BytesFrame::ChunkedString(b) => Some(b),
      _ => None,
    }
  }

  fn encode_len(&self) -> usize {
    resp3_utils::bytes_encode_len(self)
  }

  // TODO make sure this hasn't changed across redis versions
  fn is_normal_pubsub_message(&self) -> bool {
    // format is ["pubsub", "message", <channel>, <message>]
    match self {
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => {
        data.len() == 4
          && data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_pattern_pubsub_message(&self) -> bool {
    // format is ["pubsub", "pmessage", <pattern>, <channel>, <message>]
    match self {
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => {
        data.len() == 5
          && data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == PATTERN_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  fn is_shard_pubsub_message(&self) -> bool {
    // format is ["pubsub", "pmessage", <channel>, <message>]
    match self {
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => {
        data.len() == 4
          && data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == SHARD_PUBSUB_PREFIX).unwrap_or(false)
      },
      _ => false,
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn is_single_element_vec(&self) -> bool {
    match self {
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => data.len() == 1,
      _ => false,
    }
  }

  #[cfg(feature = "convert")]
  #[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
  fn pop_or_take(self) -> Self {
    match self {
      BytesFrame::Array { mut data, .. } | BytesFrame::Push { mut data, .. } => data.pop().unwrap(),
      _ => self,
    }
  }
}

#[cfg(feature = "bytes")]
impl BytesFrame {
  /// Copy the frame contents into a new [OwnedFrame].
  pub fn to_owned_frame(&self) -> OwnedFrame {
    resp3_utils::bytes_to_owned_frame(self)
  }
}

/// An enum describing the possible return types from the stream decoding interface.
#[derive(Debug, Eq, PartialEq)]
pub enum DecodedFrame<T: Resp3Frame> {
  Streaming(StreamedFrame<T>),
  Complete(T),
}

impl<T: Resp3Frame> DecodedFrame<T> {
  /// Add attributes to the decoded frame, if possible.
  pub fn add_attributes(&mut self, attributes: T::Attributes) -> Result<(), RedisProtocolError> {
    match self {
      DecodedFrame::Streaming(inner) => inner.add_attributes(attributes),
      DecodedFrame::Complete(inner) => inner.add_attributes(attributes),
    }
  }

  /// Convert the decoded frame to a complete frame, returning an error if a streaming variant is found.
  pub fn into_complete_frame(self) -> Result<T, RedisProtocolError> {
    match self {
      DecodedFrame::Complete(frame) => Ok(frame),
      DecodedFrame::Streaming(_) => Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected complete frame.",
      )),
    }
  }

  /// Convert the decoded frame into a streaming frame, returning an error if a complete variant is found.
  pub fn into_streaming_frame(self) -> Result<StreamedFrame<T>, RedisProtocolError> {
    match self {
      DecodedFrame::Streaming(frame) => Ok(frame),
      DecodedFrame::Complete(_) => Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected streamed frame.",
      )),
    }
  }

  /// Whether the decoded frame starts a stream.
  pub fn is_streaming(&self) -> bool {
    matches!(self, DecodedFrame::Streaming(_))
  }

  /// Whether the decoded frame is a complete frame.
  pub fn is_complete(&self) -> bool {
    matches!(self, DecodedFrame::Complete(_))
  }
}

/// A helper struct for reading and managing streaming data types.
///
/// ```rust
/// use redis_protocol::resp3::decode::streaming;
///
/// fn example() {
///   // decode the streamed array `[1,2]` one element at a time
///   let parts: Vec<Vec<u8>> = vec![
///     "*?\r\n".into(),
///     ":1\r\n".into(),
///     ":2\r\n".into(),
///     ".\r\n".into(),
///   ];
///
///   let (frame, _) = streaming::decode(&parts[0]).unwrap().unwrap();
///   assert!(frame.is_streaming());
///   let mut streaming = frame.into_streaming_frame().unwrap();
///   println!("Reading streaming {:?}", streaming.kind);
///
///   let (frame, _) = streaming::decode(&parts[1]).unwrap().unwrap();
///   assert!(frame.is_complete());
///   // add frames to the buffer until we reach the terminating byte sequence
///   streaming.add_frame(frame.into_complete_frame().unwrap());
///
///   let (frame, _) = streaming::decode(&parts[2]).unwrap().unwrap();
///   assert!(frame.is_complete());
///   streaming.add_frame(frame.into_complete_frame().unwrap());
///
///   let (frame, _) = streaming::decode(&parts[3]).unwrap().unwrap();
///   assert!(frame.is_complete());
///   streaming.add_frame(frame.into_complete_frame().unwrap());
///
///   assert!(streaming.is_finished());
///   // convert the buffer into one frame
///   let result = streaming.take().unwrap();
///
///   println!("{:?}", result); // OwnedFrame::Array { data: [1, 2], attributes: None }
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct StreamedFrame<T: Resp3Frame> {
  /// The internal buffer of frames and attributes.
  buffer:          VecDeque<T>,
  /// Any leading attributes before the stream starts.
  attribute_frame: T,
  /// The data type being streamed.
  pub kind:        FrameKind,
}

impl<T: Resp3Frame> StreamedFrame<T> {
  /// Create a new `StreamedFrame` from the first section of data in a streaming response.
  pub fn new(kind: FrameKind) -> Self {
    let buffer = VecDeque::new();
    StreamedFrame {
      buffer,
      kind,
      attribute_frame: T::new_empty(),
    }
  }

  /// Add the provided attributes to the frame buffer.
  pub fn add_attributes(&mut self, attributes: T::Attributes) -> Result<(), RedisProtocolError> {
    self.attribute_frame.add_attributes(attributes)
  }

  /// Convert the internal buffer into one frame matching `self.kind`, clearing the internal buffer.
  pub fn take(&mut self) -> Result<T, RedisProtocolError> {
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
    let buffer = mem::take(&mut self.buffer);
    let attributes = self.attribute_frame.take_attributes();
    T::from_buffer(self.kind, buffer, attributes)
  }

  /// Add a frame to the internal buffer.
  pub fn add_frame(&mut self, frame: T) {
    self.buffer.push_back(frame);
  }

  /// Whether the last frame represents the terminating sequence at the end of a frame stream.
  pub fn is_finished(&self) -> bool {
    self.buffer.back().map(|f| f.is_end_stream_frame()).unwrap_or(false)
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
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

    let frame = streaming_buf.take().expect("Failed to build frame from chunked stream");
    assert_eq!(frame.as_str(), Some("foobarbaz"));
  }

  #[test]
  fn should_convert_basic_streaming_buffer_to_frame_with_attributes() {
    let mut attributes = new_map(0);
    attributes.insert((FrameKind::SimpleString, "a").try_into().unwrap(), 1.into());
    attributes.insert((FrameKind::SimpleString, "b").try_into().unwrap(), 2.into());
    attributes.insert((FrameKind::SimpleString, "c").try_into().unwrap(), 3.into());

    let mut streaming_buf = StreamedFrame::new(FrameKind::BlobString);
    streaming_buf.add_attributes(attributes.clone()).unwrap();

    streaming_buf.add_frame((FrameKind::ChunkedString, "foo").try_into().unwrap());
    streaming_buf.add_frame((FrameKind::ChunkedString, "bar").try_into().unwrap());
    streaming_buf.add_frame((FrameKind::ChunkedString, "baz").try_into().unwrap());
    streaming_buf.add_frame(BytesFrame::new_end_stream());

    let frame = streaming_buf.take().expect("Failed to build frame from chunked stream");
    assert_eq!(frame.as_str(), Some("foobarbaz"));
    assert_eq!(frame.attributes(), Some(&attributes));
  }
}
