use crate::{
  error::{RedisProtocolError, RedisProtocolErrorKind},
  resp3::types::*,
  utils::digits_in_number,
};
use alloc::{borrow::Cow, format, string::ToString, vec::Vec};
use core::{
  hash::{Hash, Hasher},
  str,
};

#[cfg(feature = "zero-copy")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "zero-copy")]
use bytes_utils::Str;

#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};
#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};
use nom::Slice;
#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

pub const BOOLEAN_ENCODE_LEN: usize = 4;

pub fn hash_tuple<H: Hasher>(state: &mut H, range: &(usize, usize)) {
  range.0.hash(state);
  range.1.hash(state);
}

#[cfg(not(feature = "index-map"))]
pub fn new_set<K: Hash + Eq>(capacity: Option<usize>) -> HashSet<K> {
  if let Some(capacity) = capacity {
    HashSet::with_capacity(capacity)
  } else {
    HashSet::new()
  }
}

#[cfg(feature = "index-map")]
pub fn new_set<K: Hash + Eq>(capacity: Option<usize>) -> IndexSet<K> {
  if let Some(capacity) = capacity {
    IndexSet::with_capacity(capacity)
  } else {
    IndexSet::new()
  }
}

#[cfg(not(feature = "index-map"))]
pub fn new_map<K: Hash + Eq, V>(capacity: Option<usize>) -> HashMap<K, V> {
  if let Some(capacity) = capacity {
    HashMap::with_capacity(capacity)
  } else {
    HashMap::new()
  }
}

#[cfg(feature = "index-map")]
pub fn new_map<K: Hash + Eq, V>(capacity: Option<usize>) -> IndexMap<K, V> {
  if let Some(capacity) = capacity {
    IndexMap::with_capacity(capacity)
  } else {
    IndexMap::new()
  }
}

#[cfg(feature = "index-map")]
pub fn hashmap_to_frame_map<K: Hash + Eq, V>(data: HashMap<K, V>) -> FrameMap<K, V> {
  let mut out = IndexMap::with_capacity(data.len());
  for (key, value) in data.into_iter() {
    out.insert(key, value);
  }

  out
}

#[cfg(not(feature = "index-map"))]
pub fn hashmap_to_frame_map<K: Hash + Eq, V>(data: HashMap<K, V>) -> FrameMap<K, V> {
  data
}

#[cfg(feature = "index-map")]
pub fn hashset_to_frame_set<T: Hash + Eq>(data: HashSet<T>) -> FrameSet<T> {
  let mut out = IndexSet::with_capacity(data.len());
  for key in data.into_iter() {
    out.insert(key);
  }

  out
}

#[cfg(not(feature = "index-map"))]
pub fn hashset_to_frame_set<T: Hash + Eq>(data: HashSet<T>) -> FrameSet<T> {
  data
}

/// Read the number of bytes needed to encode the length prefix on any aggregate type frame.
pub fn length_prefix_encode_len(len: usize) -> usize {
  // prefix, length, CRLF
  1 + digits_in_number(len) + 2
}

pub fn blobstring_encode_len(b: &[u8]) -> usize {
  let len = b.len();
  length_prefix_encode_len(len) + len + 2
}

/// TODO docs
pub fn bignumber_encode_len(b: &[u8]) -> usize {
  1 + b.len() + 2
}

pub fn simplestring_encode_len(s: &[u8]) -> usize {
  1 + s.len() + 2
}

pub fn verbatimstring_encode_len(format: &VerbatimStringFormat, data: &[u8]) -> usize {
  // prefix, data len + format len, crlf, format, colon, data, crlf
  let payload_len = data.len() + format.encode_len();
  // the `format.encode_len()` call includes the trailing colon
  length_prefix_encode_len(payload_len) + payload_len + 2
}

pub fn number_encode_len(i: i64) -> usize {
  let prefix = if i < 0 { 1 } else { 0 };
  let as_usize = if i < 0 { (i * -1) as usize } else { i as usize };

  1 + digits_in_number(as_usize) + 2 + prefix
}

pub fn double_encode_len(f: f64) -> usize {
  if f.is_infinite() {
    let inf_len = if f.is_sign_negative() {
      NEG_INFINITY.as_bytes().len()
    } else {
      INFINITY.as_bytes().len()
    };

    // comma, inf|-inf, CRLF
    1 + inf_len + 2
  } else {
    // TODO there's gotta be a better way to do this
    1 + f.to_string().as_bytes().len() + 2
  }
}

pub fn auth_encode_len(username: Option<&str>, password: Option<&str>) -> usize {
  if username.is_none() && password.is_none() {
    return 0;
  }

  AUTH.as_bytes().len()
    + 1
    + username.map(|s| s.as_bytes().len() + 1).unwrap_or(0)
    + password.map(|s| s.as_bytes().len() + 1).unwrap_or(0)
}

// As mentioned elsewhere in the comments, some of the encoding and decoding logic differs in meaningful ways based on
// the byte array container type. These types often incur some overhead to convert between, and rather than make any
// of those trade-offs I decided to just copy a few private functions.

#[cfg(feature = "zero-copy")]
pub fn bytes_array_or_push_encode_len(frames: &[BytesFrame]) -> usize {
  frames
    .iter()
    .fold(length_prefix_encode_len(frames.len()), |m, f| m + bytes_encode_len(f))
}

pub fn owned_array_or_push_encode_len(frames: &[OwnedFrame]) -> usize {
  frames
    .iter()
    .fold(length_prefix_encode_len(frames.len()), |m, f| m + owned_encode_len(f))
}

pub fn owned_map_encode_len(map: &FrameMap<OwnedFrame, OwnedFrame>) -> usize {
  map.iter().fold(length_prefix_encode_len(map.len()), |m, (k, v)| {
    m + owned_encode_len(k) + owned_encode_len(v)
  })
}

#[cfg(feature = "zero-copy")]
pub fn bytes_map_encode_len(map: &FrameMap<BytesFrame, BytesFrame>) -> usize {
  map.iter().fold(length_prefix_encode_len(map.len()), |m, (k, v)| {
    m + bytes_encode_len(k) + bytes_encode_len(v)
  })
}

pub fn owned_set_encode_len(set: &FrameSet<OwnedFrame>) -> usize {
  set
    .iter()
    .fold(length_prefix_encode_len(set.len()), |m, f| m + owned_encode_len(f))
}

#[cfg(feature = "zero-copy")]
pub fn bytes_set_encode_len(set: &FrameSet<BytesFrame>) -> usize {
  set
    .iter()
    .fold(length_prefix_encode_len(set.len()), |m, f| m + bytes_encode_len(f))
}

#[cfg(feature = "zero-copy")]
pub fn bytes_hello_encode_len(username: &Option<Str>, password: &Option<Str>) -> usize {
  HELLO.as_bytes().len()
    + 3
    + auth_encode_len(
      username.as_ref().map(|s| s.as_ref()),
      password.as_ref().map(|s| s.as_ref()),
    )
}

pub fn owned_hello_encode_len(username: &Option<String>, password: &Option<String>) -> usize {
  HELLO.as_bytes().len()
    + 3
    + auth_encode_len(
      username.as_ref().map(|s| s.as_str()),
      password.as_ref().map(|s| s.as_str()),
    )
}

#[cfg(feature = "zero-copy")]
pub fn bytes_attribute_encode_len(attributes: &Option<BytesAttributes>) -> usize {
  attributes.as_ref().map(|a| bytes_map_encode_len(a)).unwrap_or(0)
}

pub fn owned_attribute_encode_len(attributes: &Option<OwnedAttributes>) -> usize {
  attributes.as_ref().map(|a| owned_map_encode_len(a)).unwrap_or(0)
}

/// Returns the number of bytes necessary to represent the frame and any associated attributes.
#[cfg(feature = "zero-copy")]
pub fn bytes_encode_len(data: &BytesFrame) -> usize {
  use BytesFrame::*;

  match data {
    Array { data, attributes } => bytes_array_or_push_encode_len(data) + bytes_attribute_encode_len(attributes),
    Push { data, attributes } => bytes_array_or_push_encode_len(data) + bytes_attribute_encode_len(attributes),
    BlobString { data, attributes } => blobstring_encode_len(data) + bytes_attribute_encode_len(attributes),
    BlobError { data, attributes } => blobstring_encode_len(data) + bytes_attribute_encode_len(attributes),
    SimpleString { data, attributes } => simplestring_encode_len(data) + bytes_attribute_encode_len(attributes),
    SimpleError { data, attributes } => {
      simplestring_encode_len(data.as_bytes()) + bytes_attribute_encode_len(attributes)
    },
    Number { data, attributes } => number_encode_len(*data) + bytes_attribute_encode_len(attributes),
    Double { data, attributes } => double_encode_len(*data) + bytes_attribute_encode_len(attributes),
    Boolean { attributes, .. } => BOOLEAN_ENCODE_LEN + bytes_attribute_encode_len(attributes),
    VerbatimString {
      data,
      attributes,
      format,
      ..
    } => verbatimstring_encode_len(format, data) + bytes_attribute_encode_len(attributes),
    Map { data, attributes } => bytes_map_encode_len(data) + bytes_attribute_encode_len(attributes),
    Set { data, attributes } => bytes_set_encode_len(data) + bytes_attribute_encode_len(attributes),
    BigNumber { data, attributes } => bignumber_encode_len(data) + bytes_attribute_encode_len(attributes),
    Hello { username, password, .. } => bytes_hello_encode_len(username, password),
    ChunkedString(data) => {
      if data.is_empty() {
        END_STREAM_STRING_BYTES.as_bytes().len()
      } else {
        blobstring_encode_len(data)
      }
    },
    Null => NULL.as_bytes().len(),
  }
}

/// Returns the number of bytes necessary to represent the frame and any associated attributes.
pub fn owned_encode_len(data: &OwnedFrame) -> usize {
  use OwnedFrame::*;

  match data {
    Array { data, attributes } => owned_array_or_push_encode_len(data) + owned_attribute_encode_len(attributes),
    Push { data, attributes } => owned_array_or_push_encode_len(data) + owned_attribute_encode_len(attributes),
    BlobString { data, attributes } => blobstring_encode_len(data) + owned_attribute_encode_len(attributes),
    BlobError { data, attributes } => blobstring_encode_len(data) + owned_attribute_encode_len(attributes),
    SimpleString { data, attributes } => simplestring_encode_len(data) + owned_attribute_encode_len(attributes),
    SimpleError { data, attributes } => {
      simplestring_encode_len(data.as_bytes()) + owned_attribute_encode_len(attributes)
    },
    Number { data, attributes } => number_encode_len(*data) + owned_attribute_encode_len(attributes),
    Double { data, attributes } => double_encode_len(*data) + owned_attribute_encode_len(attributes),
    Boolean { attributes, .. } => BOOLEAN_ENCODE_LEN + owned_attribute_encode_len(attributes),
    VerbatimString {
      data,
      attributes,
      format,
      ..
    } => verbatimstring_encode_len(format, data) + owned_attribute_encode_len(attributes),
    Map { data, attributes } => owned_map_encode_len(data) + owned_attribute_encode_len(attributes),
    Set { data, attributes } => owned_set_encode_len(data) + owned_attribute_encode_len(attributes),
    BigNumber { data, attributes } => bignumber_encode_len(data) + owned_attribute_encode_len(attributes),
    Hello { username, password, .. } => owned_hello_encode_len(username, password),
    ChunkedString(data) => {
      if data.is_empty() {
        END_STREAM_STRING_BYTES.as_bytes().len()
      } else {
        blobstring_encode_len(data)
      }
    },
    Null => NULL.as_bytes().len(),
  }
}

#[cfg(feature = "zero-copy")]
/// Convert a `BytesFrame` to an `OwnedFrame` by copying the frame contents.
pub fn bytes_to_owned_frame(frame: &BytesFrame) -> OwnedFrame {
  match frame {
    BytesFrame::Array { data, attributes } => OwnedFrame::Array {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.iter().map(bytes_to_owned_frame).collect(),
    },
    BytesFrame::Push { data, attributes } => OwnedFrame::Push {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.iter().map(bytes_to_owned_frame).collect(),
    },
    BytesFrame::BlobString { data, attributes } => OwnedFrame::BlobString {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.to_vec(),
    },
    BytesFrame::BlobError { data, attributes } => OwnedFrame::BlobError {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.to_vec(),
    },
    BytesFrame::BigNumber { data, attributes } => OwnedFrame::BigNumber {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.to_vec(),
    },
    BytesFrame::ChunkedString(data) => OwnedFrame::ChunkedString(data.to_vec()),
    BytesFrame::SimpleString { data, attributes } => OwnedFrame::SimpleString {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.to_vec(),
    },
    BytesFrame::SimpleError { data, attributes } => OwnedFrame::SimpleError {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.to_string(),
    },
    BytesFrame::Number { data, attributes } => OwnedFrame::Number {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       *data,
    },
    BytesFrame::Double { data, attributes } => OwnedFrame::Double {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       *data,
    },
    BytesFrame::Boolean { data, attributes } => OwnedFrame::Boolean {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       *data,
    },
    BytesFrame::Null => OwnedFrame::Null,
    BytesFrame::VerbatimString {
      data,
      attributes,
      format,
    } => OwnedFrame::VerbatimString {
      format:     format.clone(),
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.to_vec(),
    },
    BytesFrame::Map { data, attributes } => OwnedFrame::Map {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data
        .iter()
        .map(|(k, v)| (bytes_to_owned_frame(k), bytes_to_owned_frame(v)))
        .collect(),
    },
    BytesFrame::Set { data, attributes } => OwnedFrame::Set {
      attributes: attributes.as_ref().map(bytes_to_owned_attrs),
      data:       data.iter().map(bytes_to_owned_frame).collect(),
    },
    BytesFrame::Hello {
      version,
      username,
      password,
    } => OwnedFrame::Hello {
      version:  version.clone(),
      username: username.as_ref().map(|s| s.to_string()),
      password: password.as_ref().map(|s| s.to_string()),
    },
  }
}

#[cfg(feature = "zero-copy")]
/// Convert bytes attributes to owned attributes.
pub fn bytes_to_owned_attrs(attributes: &BytesAttributes) -> OwnedAttributes {
  attributes
    .iter()
    .map(|(k, v)| (bytes_to_owned_frame(k), bytes_to_owned_frame(v)))
    .collect()
}

#[cfg(feature = "zero-copy")]
/// Convert a `BytesFrame` to an `OwnedFrame` by moving the frame contents.
pub fn owned_to_bytes_frame(frame: OwnedFrame) -> BytesFrame {
  match frame {
    OwnedFrame::Array { data, attributes } => BytesFrame::Array {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into_iter().map(owned_to_bytes_frame).collect(),
    },
    OwnedFrame::Push { data, attributes } => BytesFrame::Push {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into_iter().map(owned_to_bytes_frame).collect(),
    },
    OwnedFrame::BlobString { data, attributes } => BytesFrame::BlobString {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into(),
    },
    OwnedFrame::BlobError { data, attributes } => BytesFrame::BlobError {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into(),
    },
    OwnedFrame::BigNumber { data, attributes } => BytesFrame::BigNumber {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into(),
    },
    OwnedFrame::ChunkedString(data) => BytesFrame::ChunkedString(data.into()),
    OwnedFrame::SimpleString { data, attributes } => BytesFrame::SimpleString {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into(),
    },
    OwnedFrame::SimpleError { data, attributes } => BytesFrame::SimpleError {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into(),
    },
    OwnedFrame::Number { data, attributes } => BytesFrame::Number {
      attributes: attributes.map(owned_to_bytes_attrs),
      data,
    },
    OwnedFrame::Double { data, attributes } => BytesFrame::Double {
      attributes: attributes.map(owned_to_bytes_attrs),
      data,
    },
    OwnedFrame::Boolean { data, attributes } => BytesFrame::Boolean {
      attributes: attributes.map(owned_to_bytes_attrs),
      data,
    },
    OwnedFrame::Null => BytesFrame::Null,
    OwnedFrame::VerbatimString {
      data,
      attributes,
      format,
    } => BytesFrame::VerbatimString {
      format,
      attributes: attributes.map(owned_to_bytes_attrs),
      data: data.into(),
    },
    OwnedFrame::Map { data, attributes } => BytesFrame::Map {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data
        .into_iter()
        .map(|(k, v)| (owned_to_bytes_frame(k), owned_to_bytes_frame(v)))
        .collect(),
    },
    OwnedFrame::Set { data, attributes } => BytesFrame::Set {
      attributes: attributes.map(owned_to_bytes_attrs),
      data:       data.into_iter().map(owned_to_bytes_frame).collect(),
    },
    OwnedFrame::Hello {
      version,
      username,
      password,
    } => BytesFrame::Hello {
      version,
      username: username.map(|s| s.into()),
      password: password.map(|s| s.into()),
    },
  }
}

/// Convert bytes attributes to owned attributes.
#[cfg(feature = "zero-copy")]
pub fn owned_to_bytes_attrs(attributes: OwnedAttributes) -> BytesAttributes {
  attributes
    .into_iter()
    .map(|(k, v)| (owned_to_bytes_frame(k), owned_to_bytes_frame(v)))
    .collect()
}

/// Return the string representation of a double, accounting for `inf` and `-inf`.
pub fn f64_to_redis_string(data: f64) -> Cow<'static, str> {
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

/// TODO docs
fn build_owned_attributes(
  buf: &[u8],
  attributes: Option<&RangeAttributes>,
) -> Result<Option<OwnedAttributes>, RedisProtocolError> {
  if let Some(attributes) = attributes {
    let mut out = new_map(Some(attributes.len()));
    for (key, value) in attributes.into_iter() {
      let key = build_owned_frame(buf, key)?;
      let value = build_owned_frame(buf, value)?;
      out.insert(key, value);
    }

    Ok(Some(out))
  } else {
    Ok(None)
  }
}

/// TODO docs
#[cfg(feature = "zero-copy")]
fn build_bytes_attributes(
  buf: &Bytes,
  attributes: Option<&RangeAttributes>,
) -> Result<Option<BytesAttributes>, RedisProtocolError> {
  if let Some(attributes) = attributes {
    let mut out = new_map(Some(attributes.len()));
    for (key, value) in attributes.into_iter() {
      let key = build_bytes_frame(buf, key)?;
      let value = build_bytes_frame(buf, value)?;
      out.insert(key, value);
    }

    Ok(Some(out))
  } else {
    Ok(None)
  }
}

/// Move or copy the contents of `buf` based on the ranges in the provided frame.
pub fn build_owned_frame(buf: &[u8], frame: &RangeFrame) -> Result<OwnedFrame, RedisProtocolError> {
  Ok(match frame {
    RangeFrame::SimpleString { data, attributes } => OwnedFrame::SimpleString {
      data:       buf[data.0 .. data.1].to_vec(),
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::SimpleError { data, attributes } => OwnedFrame::SimpleError {
      data:       str::from_utf8(&buf[data.0 .. data.1])?.to_string(),
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::BlobString { data, attributes } => OwnedFrame::BlobString {
      data:       buf[data.0 .. data.1].to_vec(),
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::BlobError { data, attributes } => OwnedFrame::BlobError {
      data:       buf[data.0 .. data.1].to_vec(),
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Number { data, attributes } => OwnedFrame::Number {
      data:       *data,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Double { data, attributes } => OwnedFrame::Double {
      data:       *data,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Boolean { data, attributes } => OwnedFrame::Boolean {
      data:       *data,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Null => OwnedFrame::Null,
    RangeFrame::VerbatimString {
      data,
      attributes,
      format,
    } => OwnedFrame::VerbatimString {
      data:       buf[data.0 .. data.1].to_vec(),
      format:     format.clone(),
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::BigNumber { data, attributes } => OwnedFrame::BigNumber {
      data:       buf[data.0 .. data.1].to_vec(),
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::ChunkedString(data) => OwnedFrame::ChunkedString(buf[data.0 .. data.1].to_vec()),
    RangeFrame::Hello {
      version,
      username,
      password,
    } => OwnedFrame::Hello {
      version:  version.clone(),
      username: if let Some(username) = username {
        Some(String::from_utf8(buf[username.0 .. username.1].to_vec())?)
      } else {
        None
      },
      password: if let Some(password) = password {
        Some(String::from_utf8(buf[password.0 .. password.1].to_vec())?)
      } else {
        None
      },
    },
    RangeFrame::Array { data, attributes } => OwnedFrame::Array {
      data:       data.iter().map(|f| build_owned_frame(buf, f)).collect()?,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Push { data, attributes } => OwnedFrame::Push {
      data:       data.iter().map(|f| build_owned_frame(buf, f)).collect()?,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Set { data, attributes } => OwnedFrame::Set {
      data:       data.iter().map(|f| build_owned_frame(buf, f)).collect()?,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Map { data, attributes } => OwnedFrame::Map {
      data:       data
        .iter()
        .map(|(k, v)| {
          let key = build_owned_frame(buf, k)?;
          let value = build_owned_frame(buf, v)?;
          Ok((key, value))
        })
        .collect()?,
      attributes: build_owned_attributes(buf, attributes.as_ref())?,
    },
  })
}

/// Use the `Bytes` interface to create owned views into the provided buffer for each of the provided range frames.
#[cfg(feature = "zero-copy")]
pub fn build_bytes_frame(buf: &Bytes, frame: &RangeFrame) -> Result<BytesFrame, RedisProtocolError> {
  Ok(match frame {
    RangeFrame::SimpleString { data, attributes } => BytesFrame::SimpleString {
      data:       buf.slice(data.0 .. data.1),
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::SimpleError { data, attributes } => BytesFrame::SimpleError {
      data:       Str::from_inner(buf.slice(data.0 .. data.1))?,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::BlobString { data, attributes } => BytesFrame::BlobString {
      data:       buf.slice(data.0 .. data.1),
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::BlobError { data, attributes } => BytesFrame::BlobError {
      data:       buf.slice(data.0 .. data.1),
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Number { data, attributes } => BytesFrame::Number {
      data:       *data,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Double { data, attributes } => BytesFrame::Double {
      data:       *data,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Boolean { data, attributes } => BytesFrame::Boolean {
      data:       *data,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Null => BytesFrame::Null,
    RangeFrame::VerbatimString {
      data,
      attributes,
      format,
    } => BytesFrame::VerbatimString {
      data:       buf.slice(data.0 .. data.1),
      format:     format.clone(),
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::BigNumber { data, attributes } => BytesFrame::BigNumber {
      data:       buf.slice(data.0 .. data.1),
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::ChunkedString(data) => BytesFrame::ChunkedString(buf.slice(data.0 .. data.1)),
    RangeFrame::Hello {
      version,
      username,
      password,
    } => BytesFrame::Hello {
      version:  version.clone(),
      username: if let Some(username) = username {
        Some(Str::from_inner(buf.slice(username.0 .. username.1))?)
      } else {
        None
      },
      password: if let Some(password) = password {
        Some(Str::from_inner(buf.slice(password.0 .. password.1))?)
      } else {
        None
      },
    },
    RangeFrame::Array { data, attributes } => BytesFrame::Array {
      data:       data.iter().map(|f| build_owned_frame(buf, f)).collect()?,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Push { data, attributes } => BytesFrame::Push {
      data:       data.iter().map(|f| build_owned_frame(buf, f)).collect()?,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Set { data, attributes } => BytesFrame::Set {
      data:       data.iter().map(|f| build_owned_frame(buf, f)).collect()?,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
    RangeFrame::Map { data, attributes } => BytesFrame::Map {
      data:       data
        .iter()
        .map(|(k, v)| {
          let key = build_owned_frame(buf, k)?;
          let value = build_owned_frame(buf, v)?;
          Ok((key, value))
        })
        .collect()?,
      attributes: build_bytes_attributes(buf, attributes.as_ref())?,
    },
  })
}

/// Split off and [freeze](bytes::BytesMut::freeze) `amt` bytes from `buf` and return owned views into the buffer
/// based on the provided range frame.
///
/// The returned `Bytes` represents the `Bytes` buffer that was sliced off `buf`. The returned frames hold
/// owned `Bytes` views to slices within this buffer.
#[cfg(feature = "zero-copy")]
pub fn freeze_parse(
  buf: &mut BytesMut,
  frame: &RangeFrame,
  amt: usize,
) -> Result<(BytesFrame, Bytes), RedisProtocolError> {
  let buffer = buf.split_to(amt).freeze();
  let frame = build_bytes_frame(&buffer, frame)?;
  Ok((frame, buffer))
}

/// Convert a streaming range frame into an owned streaming frame.
pub fn build_owned_streaming_frame(
  buf: &[u8],
  frame: &StreamedRangeFrame,
) -> Result<StreamedFrame<OwnedFrame>, RedisProtocolError> {
  let attributes = build_owned_attributes(buf, frame.attributes.as_ref())?;
  let mut streamed = StreamedFrame::new(frame.kind);

  if let Some(attributes) = attributes {
    streamed.add_attributes(attributes)?;
  }
  Ok(streamed)
}

/// Convert a streaming range frame into a streaming bytes frame.
#[cfg(feature = "zero-copy")]
pub fn build_bytes_streaming_frame(
  buf: &Bytes,
  frame: &StreamedRangeFrame,
) -> Result<StreamedFrame<BytesFrame>, RedisProtocolError> {
  let attributes = build_bytes_attributes(buf, frame.attributes.as_ref())?;
  let mut streamed = StreamedFrame::new(frame.kind);

  if let Some(attributes) = attributes {
    streamed.add_attributes(attributes)?;
  }
  Ok(streamed)
}

#[cfg(test)]
mod tests {
  use crate::resp3::{
    types::*,
    utils::{encode_len, new_map, new_set},
  };
  use alloc::vec;

  fn create_bytes_attributes() -> (FrameMap<BytesFrame>, usize) {
    let mut out = new_map(None);
    let key = BytesFrame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    };
    let value = BytesFrame::Number {
      data:       42,
      attributes: None,
    };
    out.insert(key, value);

    (out, 1 + 1 + 2 + 6 + 5)
  }

  #[test]
  fn should_reconstruct_blobstring() {
    let mut streamed_frame = StreamedFrame::new(FrameKind::BlobString);
    streamed_frame.add_bytes_frame(BytesFrame::ChunkedString("foo".as_bytes().into()));
    streamed_frame.add_bytes_frame(BytesFrame::ChunkedString("bar".as_bytes().into()));
    streamed_frame.add_bytes_frame(BytesFrame::ChunkedString("baz".as_bytes().into()));

    let expected = BytesFrame::BlobString {
      data:       "foobarbaz".as_bytes().into(),
      attributes: None,
    };
    assert_eq!(streamed_frame.take_bytes_frame().unwrap(), expected);

    let mut streamed_frame = StreamedFrame::new(FrameKind::BlobString);
    streamed_frame.add_bytes_frame(BytesFrame::ChunkedString("foo".as_bytes().into()));
    streamed_frame.add_bytes_frame(BytesFrame::ChunkedString("bar".as_bytes().into()));
    streamed_frame.add_bytes_frame(BytesFrame::ChunkedString("baz".as_bytes().into()));
    let (attributes, _) = create_bytes_attributes();
    streamed_frame.attributes = Some(attributes.clone());

    let expected = BytesFrame::BlobString {
      data:       "foobarbaz".as_bytes().into(),
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.take_bytes_frame().unwrap(), expected);
  }

  #[test]
  fn should_reconstruct_array() {
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Number {
      data:       42,
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Boolean {
      data:       true,
      attributes: None,
    });

    let expected = Frame::Array {
      data:       vec![
        Frame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        Frame::Number {
          data:       42,
          attributes: None,
        },
        Frame::Boolean {
          data:       true,
          attributes: None,
        },
      ],
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Number {
      data:       42,
      attributes: Some(attributes.clone()),
    });
    streamed_frame.add_frame(Frame::Boolean {
      data:       true,
      attributes: None,
    });
    streamed_frame.attributes = Some(attributes.clone());

    let expected = Frame::Array {
      data:       vec![
        Frame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        Frame::Number {
          data:       42,
          attributes: Some(attributes.clone()),
        },
        Frame::Boolean {
          data:       true,
          attributes: None,
        },
      ],
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_reconstruct_map() {
    let mut k1 = Frame::SimpleString {
      data:       "a".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data:       42,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data:       "b".as_bytes().into(),
      attributes: None,
    };
    let v2 = Frame::Boolean {
      data:       true,
      attributes: None,
    };

    let mut streamed_frame = StreamedFrame::new(FrameKind::Map);
    streamed_frame.add_frame(k1.clone());
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(k2.clone());
    streamed_frame.add_frame(v2.clone());

    let mut expected = new_map(None);
    expected.insert(k1.clone(), v1.clone());
    expected.insert(k2.clone(), v2.clone());
    let expected = Frame::Map {
      data:       expected,
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let _ = k1.add_attributes(attributes.clone()).unwrap();

    let mut streamed_frame = StreamedFrame::new(FrameKind::Map);
    streamed_frame.add_frame(k1.clone());
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(k2.clone());
    streamed_frame.add_frame(v2.clone());
    streamed_frame.attributes = Some(attributes.clone());

    let mut expected = new_map(None);
    expected.insert(k1.clone(), v1.clone());
    expected.insert(k2.clone(), v2.clone());
    let expected = Frame::Map {
      data:       expected,
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  #[should_panic]
  fn should_reconstruct_map_odd_elements() {
    let k1 = Frame::SimpleString {
      data:       "a".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data:       42,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data:       "b".as_bytes().into(),
      attributes: None,
    };

    let mut streamed_frame = StreamedFrame::new(FrameKind::Map);
    streamed_frame.add_frame(k1.clone());
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(k2.clone());

    let _ = streamed_frame.into_frame().unwrap();
  }

  #[test]
  fn should_reconstruct_set() {
    let mut v1 = Frame::SimpleString {
      data:       "a".into(),
      attributes: None,
    };
    let v2 = Frame::Number {
      data:       42,
      attributes: None,
    };
    let v3 = Frame::BlobString {
      data:       "b".as_bytes().into(),
      attributes: None,
    };
    let v4 = Frame::Boolean {
      data:       true,
      attributes: None,
    };

    let mut streamed_frame = StreamedFrame::new(FrameKind::Set);
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(v2.clone());
    streamed_frame.add_frame(v3.clone());
    streamed_frame.add_frame(v4.clone());

    let mut expected = new_set(None);
    expected.insert(v1.clone());
    expected.insert(v2.clone());
    expected.insert(v3.clone());
    expected.insert(v4.clone());
    let expected = Frame::Set {
      data:       expected,
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let _ = v1.add_attributes(attributes.clone()).unwrap();

    let mut streamed_frame = StreamedFrame::new(FrameKind::Set);
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(v2.clone());
    streamed_frame.add_frame(v3.clone());
    streamed_frame.add_frame(v4.clone());
    streamed_frame.attributes = Some(attributes.clone());

    let mut expected = new_set(None);
    expected.insert(v1);
    expected.insert(v2);
    expected.insert(v3);
    expected.insert(v4);
    let expected = Frame::Set {
      data:       expected,
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_reconstruct_nested_array() {
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Array {
      data:       vec![
        Frame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        Frame::Null,
        Frame::BigNumber {
          data:       "123456789".as_bytes().into(),
          attributes: None,
        },
      ],
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Boolean {
      data:       true,
      attributes: None,
    });

    let expected = Frame::Array {
      data:       vec![
        Frame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        Frame::Array {
          data:       vec![
            Frame::SimpleString {
              data:       "foo".into(),
              attributes: None,
            },
            Frame::Null,
            Frame::BigNumber {
              data:       "123456789".as_bytes().into(),
              attributes: None,
            },
          ],
          attributes: None,
        },
        Frame::Boolean {
          data:       true,
          attributes: None,
        },
      ],
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Array {
      data:       vec![
        Frame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        Frame::Null,
        Frame::BigNumber {
          data:       "123456789".as_bytes().into(),
          attributes: Some(attributes.clone()),
        },
      ],
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Boolean {
      data:       true,
      attributes: Some(attributes.clone()),
    });
    streamed_frame.attributes = Some(attributes.clone());

    let expected = Frame::Array {
      data:       vec![
        Frame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        Frame::Array {
          data:       vec![
            Frame::SimpleString {
              data:       "foo".into(),
              attributes: None,
            },
            Frame::Null,
            Frame::BigNumber {
              data:       "123456789".as_bytes().into(),
              attributes: Some(attributes.clone()),
            },
          ],
          attributes: None,
        },
        Frame::Boolean {
          data:       true,
          attributes: Some(attributes.clone()),
        },
      ],
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_reconstruct_nested_map() {
    let k1 = Frame::SimpleString {
      data:       "a".into(),
      attributes: None,
    };
    let mut v1 = Frame::Number {
      data:       42,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data:       "b".as_bytes().into(),
      attributes: None,
    };
    let inner_k1 = Frame::SimpleString {
      data:       "b".into(),
      attributes: None,
    };
    let mut inner_v1 = Frame::BlobString {
      data:       "foobarbaz".as_bytes().into(),
      attributes: None,
    };
    let mut inner_map = new_map(None);
    inner_map.insert(inner_k1.clone(), inner_v1.clone());
    let v2 = Frame::Map {
      data:       inner_map,
      attributes: None,
    };

    let mut streamed_frame = StreamedFrame::new(FrameKind::Map);
    streamed_frame.add_frame(k1.clone());
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(k2.clone());
    streamed_frame.add_frame(v2.clone());

    let mut expected = new_map(None);
    expected.insert(k1.clone(), v1.clone());
    expected.insert(k2.clone(), v2.clone());
    let expected = Frame::Map {
      data:       expected,
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let _ = v1.add_attributes(attributes.clone()).unwrap();
    let _ = inner_v1.add_attributes(attributes.clone()).unwrap();
    let mut inner_map = new_map(None);
    inner_map.insert(inner_k1, inner_v1);
    let v2 = Frame::Map {
      data:       inner_map,
      attributes: None,
    };

    let mut streamed_frame = StreamedFrame::new(FrameKind::Map);
    streamed_frame.add_frame(k1.clone());
    streamed_frame.add_frame(v1.clone());
    streamed_frame.add_frame(k2.clone());
    streamed_frame.add_frame(v2.clone());
    streamed_frame.attributes = Some(attributes.clone());

    let mut expected = new_map(None);
    expected.insert(k1.clone(), v1.clone());
    expected.insert(k2.clone(), v2.clone());
    let expected = Frame::Map {
      data:       expected,
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_get_encode_len_blobstring() {
    let mut frame = Frame::BlobString {
      data:       "foobarbaz".as_bytes().into(),
      attributes: None,
    };
    let expected_len = 1 + 1 + 2 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_bloberror() {
    let mut frame = Frame::BlobError {
      data:       "foobarbaz".as_bytes().into(),
      attributes: None,
    };
    let expected_len = 1 + 1 + 2 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_bignumber() {
    let mut frame = Frame::BigNumber {
      data:       "123456789".as_bytes().into(),
      attributes: None,
    };
    let expected_len = 1 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_bool() {
    let mut frame = Frame::Boolean {
      data:       true,
      attributes: None,
    };
    let expected_len = 1 + 1 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);

    let mut frame = Frame::Boolean {
      data:       false,
      attributes: None,
    };
    let expected_len = 1 + 1 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_number() {
    let mut frame = Frame::Number {
      data:       500,
      attributes: None,
    };
    let expected_len = 1 + 3 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_negative_number() {
    let mut frame = Frame::Number {
      data:       -500,
      attributes: None,
    };
    let expected_len = 1 + 4 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_double() {
    let mut frame = Frame::Double {
      data:       500.123,
      attributes: None,
    };
    let expected_len = 1 + 7 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_negative_double() {
    let mut frame = Frame::Double {
      data:       -500.123,
      attributes: None,
    };
    let expected_len = 1 + 8 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_inf_double() {
    let mut frame = Frame::Double {
      data:       f64::INFINITY,
      attributes: None,
    };
    let expected_len = 1 + 3 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);

    let mut frame = Frame::Double {
      data:       f64::NEG_INFINITY,
      attributes: None,
    };
    let expected_len = 1 + 4 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_null() {
    let frame = Frame::Null;
    let expected_len = 3;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);
  }

  #[test]
  fn should_get_encode_len_hello() {
    let frame = Frame::Hello {
      version: RespVersion::RESP3,
      auth:    None,
    };
    let expected_len = 5 + 1 + 1 + 1;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let frame = Frame::Hello {
      version: RespVersion::RESP2,
      auth:    None,
    };
    let expected_len = 5 + 1 + 1 + 1;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let frame = Frame::Hello {
      version: RespVersion::RESP3,
      auth:    Some(Auth {
        username: "foo".into(),
        password: "bar".into(),
      }),
    };
    let expected_len = 5 + 1 + 1 + 1 + 4 + 1 + 3 + 1 + 3 + 1;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);
  }

  #[test]
  fn should_get_encode_len_verbatimstring() {
    let mut frame = Frame::VerbatimString {
      format:     VerbatimStringFormat::Markdown,
      data:       "foobarbaz".into(),
      attributes: None,
    };
    let expected_len = 1 + 2 + 2 + 3 + 1 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);

    let mut frame = Frame::VerbatimString {
      format:     VerbatimStringFormat::Text,
      data:       "foobarbaz".into(),
      attributes: None,
    };
    let expected_len = 1 + 2 + 2 + 3 + 1 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);
  }

  #[test]
  fn should_get_encode_len_chunked_string() {
    let frame = Frame::ChunkedString("foobarbaz".as_bytes().into());
    let expected_len = 1 + 1 + 2 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);
  }

  #[test]
  fn should_get_encode_len_end_stream() {
    let end_frame = Frame::new_end_stream();
    let expected_len = 1 + 1 + 2;
    assert_eq!(encode_len(&end_frame).unwrap(), expected_len);
  }
}
