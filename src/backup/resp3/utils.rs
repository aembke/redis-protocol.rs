use crate::resp3::types::*;
use crate::types::{RedisProtocolError, RedisProtocolErrorKind};
use crate::utils::{digits_in_number, PATTERN_PUBSUB_PREFIX, PUBSUB_PREFIX, PUBSUB_PUSH_PREFIX};
use alloc::borrow::Cow;
use alloc::collections::VecDeque;
use alloc::format;
use alloc::string::ToString;
use alloc::vec::Vec;
use bytes::BytesMut;
use cookie_factory::GenError;

#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};

#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};

pub const BOOLEAN_ENCODE_LEN: usize = 4;

#[cfg(not(feature = "index-map"))]
pub fn new_set<K>(capacity: Option<usize>) -> HashSet<K> {
  if let Some(capacity) = capacity {
    HashSet::with_capacity(capacity)
  } else {
    HashSet::new()
  }
}

#[cfg(feature = "index-map")]
pub fn new_set<K>(capacity: Option<usize>) -> IndexSet<K> {
  if let Some(capacity) = capacity {
    IndexSet::with_capacity(capacity)
  } else {
    IndexSet::new()
  }
}

#[cfg(not(feature = "index-map"))]
pub fn new_map<K, V>(capacity: Option<usize>) -> HashMap<K, V> {
  if let Some(capacity) = capacity {
    HashMap::with_capacity(capacity)
  } else {
    HashMap::new()
  }
}

#[cfg(feature = "index-map")]
pub fn new_map<K, V>(capacity: Option<usize>) -> IndexMap<K, V> {
  if let Some(capacity) = capacity {
    IndexMap::with_capacity(capacity)
  } else {
    IndexMap::new()
  }
}

#[cfg(feature = "index-map")]
pub fn hashmap_to_frame_map(data: HashMap<Frame, Frame>) -> FrameMap {
  let mut out = IndexMap::with_capacity(data.len());
  for (key, value) in data.into_iter() {
    let _ = out.insert(key, value);
  }

  out
}

#[cfg(not(feature = "index-map"))]
pub fn hashmap_to_frame_map(data: HashMap<Frame, Frame>) -> FrameMap {
  data
}

#[cfg(feature = "index-map")]
pub fn hashset_to_frame_set(data: HashSet<Frame>) -> FrameSet {
  let mut out = IndexSet::with_capacity(data.len());
  for key in data.into_iter() {
    let _ = out.insert(key);
  }

  out
}

#[cfg(not(feature = "index-map"))]
pub fn hashset_to_frame_set(data: HashSet<Frame>) -> FrameSet {
  data
}

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

pub fn simplestring_encode_len(s: &[u8]) -> usize {
  1 + s.len() + 2
}

pub fn verbatimstring_encode_len(format: &VerbatimStringFormat, data: &[u8]) -> usize {
  // prefix, data len + format len, crlf, format, colon, data, crlf
  1 + digits_in_number(data.len() + format.encode_len()) + 2 + format.encode_len() + data.len() + 2
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
    Ok(1 + f.to_string().as_bytes().len() + 2)
  }
}

pub fn map_encode_len(map: &FrameMap) -> Result<usize, GenError> {
  let mut total_len = 1 + digits_in_number(map.len()) + 2;

  for (key, value) in map.iter() {
    total_len += encode_len(key)? + encode_len(value)?;
  }
  Ok(total_len)
}

pub fn set_encode_len(set: &FrameSet) -> Result<usize, GenError> {
  let mut total_len = 1 + digits_in_number(set.len()) + 2;

  for frame in set.iter() {
    total_len += encode_len(frame)?;
  }
  Ok(total_len)
}

pub fn hello_encode_len(_version: &RespVersion, auth: &Option<Auth>) -> usize {
  let mut total_len = HELLO.as_bytes().len() + 3;

  if let Some(ref auth) = *auth {
    total_len += AUTH.as_bytes().len() + 1 + auth.username.as_bytes().len() + 1 + auth.password.as_bytes().len() + 1;
  }
  total_len
}

pub fn attribute_encode_len(attributes: &Option<Attributes>) -> Result<usize, GenError> {
  let attribute_len = match attributes {
    Some(attributes) => map_encode_len(attributes)?,
    None => 0,
  };

  Ok(attribute_len)
}

pub fn is_normal_pubsub(frames: &Vec<Frame>) -> bool {
  (frames.len() == 4 || frames.len() == 5)
    && frames[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
    && frames[1].as_str().map(|s| s == PUBSUB_PREFIX).unwrap_or(false)
}

pub fn is_pattern_pubsub(frames: &Vec<Frame>) -> bool {
  (frames.len() == 4 || frames.len() == 5)
    && frames[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
    && frames[1].as_str().map(|s| s == PATTERN_PUBSUB_PREFIX).unwrap_or(false)
}

/// Returns the number of bytes necessary to represent the frame and any associated attributes.
pub fn encode_len(data: &Frame) -> Result<usize, GenError> {
  use crate::resp3::types::Frame::*;

  let total_len = match *data {
    Array {
      ref data,
      ref attributes,
    } => array_or_push_encode_len(data)? + attribute_encode_len(attributes)?,
    Push {
      ref data,
      ref attributes,
    } => array_or_push_encode_len(data)? + attribute_encode_len(attributes)?,
    BlobString {
      ref data,
      ref attributes,
    } => blobstring_encode_len(data) + attribute_encode_len(attributes)?,
    BlobError {
      ref data,
      ref attributes,
    } => blobstring_encode_len(data) + attribute_encode_len(attributes)?,
    SimpleString {
      ref data,
      ref attributes,
    } => simplestring_encode_len(data) + attribute_encode_len(attributes)?,
    SimpleError {
      ref data,
      ref attributes,
    } => simplestring_encode_len(data.as_bytes()) + attribute_encode_len(attributes)?,
    Number {
      ref data,
      ref attributes,
    } => number_encode_len(data) + attribute_encode_len(attributes)?,
    Double {
      ref data,
      ref attributes,
    } => double_encode_len(data)? + attribute_encode_len(attributes)?,
    Boolean {
      data: _,
      ref attributes,
    } => BOOLEAN_ENCODE_LEN + attribute_encode_len(attributes)?,
    VerbatimString {
      ref data,
      ref attributes,
      ref format,
      ..
    } => verbatimstring_encode_len(format, data) + attribute_encode_len(attributes)?,
    Map {
      ref data,
      ref attributes,
    } => map_encode_len(data)? + attribute_encode_len(attributes)?,
    Set {
      ref data,
      ref attributes,
    } => set_encode_len(data)? + attribute_encode_len(attributes)?,
    BigNumber {
      ref data,
      ref attributes,
    } => bignumber_encode_len(data) + attribute_encode_len(attributes)?,
    Hello { ref version, ref auth } => hello_encode_len(version, auth),
    ChunkedString(ref data) => {
      if data.is_empty() {
        END_STREAM_STRING_BYTES.as_bytes().len()
      } else {
        blobstring_encode_len(data)
      }
    }
    Null => NULL.as_bytes().len(),
  };

  Ok(total_len)
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

pub fn reconstruct_blobstring(
  frames: VecDeque<Frame>,
  attributes: Option<Attributes>,
) -> Result<Frame, RedisProtocolError> {
  let total_len = frames.iter().fold(0, |m, f| m + f.len());
  let mut data = BytesMut::with_capacity(total_len);

  for frame in frames.into_iter() {
    data.extend_from_slice(match frame {
      Frame::ChunkedString(ref inner) => inner,
      Frame::BlobString { ref data, .. } => &data,
      _ => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          format!("Cannot create blob string from {:?}", frame.kind()),
        ))
      }
    });
  }

  Ok(Frame::BlobString {
    data: data.freeze(),
    attributes,
  })
}

pub fn reconstruct_array(frames: VecDeque<Frame>, attributes: Option<Attributes>) -> Result<Frame, RedisProtocolError> {
  let mut data = Vec::with_capacity(frames.len());

  for frame in frames.into_iter() {
    data.push(frame);
  }
  Ok(Frame::Array { data, attributes })
}

pub fn reconstruct_map(
  mut frames: VecDeque<Frame>,
  attributes: Option<Attributes>,
) -> Result<Frame, RedisProtocolError> {
  if frames.is_empty() {
    return Ok(Frame::Map {
      data: new_map(None),
      attributes,
    });
  }

  if frames.len() % 2 != 0 {
    return Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Map must have an even number of inner frames.",
    ));
  }

  let mut data = new_map(Some(frames.len() / 2));
  while frames.len() > 0 {
    let value = frames.pop_back().unwrap();
    let key = match frames.pop_back() {
      Some(f) => f,
      None => {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Missing map key.",
        ))
      }
    };

    if !key.can_hash() {
      return Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        format!("{:?} cannot be used as hash key.", key.kind()),
      ));
    }

    data.insert(key, value);
  }

  Ok(Frame::Map { data, attributes })
}

pub fn reconstruct_set(frames: VecDeque<Frame>, attributes: Option<Attributes>) -> Result<Frame, RedisProtocolError> {
  let mut data = new_set(Some(frames.len()));

  for frame in frames.into_iter() {
    if !frame.can_hash() {
      return Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        format!("{:?} cannot be used as hash key.", frame.kind()),
      ));
    }

    data.insert(frame);
  }
  Ok(Frame::Set { data, attributes })
}

#[cfg(test)]
mod tests {
  use alloc::vec;
  use crate::resp3::types::*;
  use crate::resp3::utils::{encode_len, new_map, new_set};

  fn create_attributes() -> (FrameMap, usize) {
    let mut out = new_map(None);
    let key = Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    };
    let value = Frame::Number {
      data: 42,
      attributes: None,
    };
    out.insert(key, value);

    (out, 1 + 1 + 2 + 6 + 5)
  }

  #[test]
  fn should_reconstruct_blobstring() {
    let mut streamed_frame = StreamedFrame::new(FrameKind::BlobString);
    streamed_frame.add_frame(Frame::ChunkedString("foo".as_bytes().into()));
    streamed_frame.add_frame(Frame::ChunkedString("bar".as_bytes().into()));
    streamed_frame.add_frame(Frame::ChunkedString("baz".as_bytes().into()));

    let expected = Frame::BlobString {
      data: "foobarbaz".as_bytes().into(),
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let mut streamed_frame = StreamedFrame::new(FrameKind::BlobString);
    streamed_frame.add_frame(Frame::ChunkedString("foo".as_bytes().into()));
    streamed_frame.add_frame(Frame::ChunkedString("bar".as_bytes().into()));
    streamed_frame.add_frame(Frame::ChunkedString("baz".as_bytes().into()));
    let (attributes, _) = create_attributes();
    streamed_frame.attributes = Some(attributes.clone());

    let expected = Frame::BlobString {
      data: "foobarbaz".as_bytes().into(),
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_reconstruct_array() {
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Number {
      data: 42,
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Boolean {
      data: true,
      attributes: None,
    });

    let expected = Frame::Array {
      data: vec![
        Frame::SimpleString {
          data: "foo".into(),
          attributes: None,
        },
        Frame::Number {
          data: 42,
          attributes: None,
        },
        Frame::Boolean {
          data: true,
          attributes: None,
        },
      ],
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Number {
      data: 42,
      attributes: Some(attributes.clone()),
    });
    streamed_frame.add_frame(Frame::Boolean {
      data: true,
      attributes: None,
    });
    streamed_frame.attributes = Some(attributes.clone());

    let expected = Frame::Array {
      data: vec![
        Frame::SimpleString {
          data: "foo".into(),
          attributes: None,
        },
        Frame::Number {
          data: 42,
          attributes: Some(attributes.clone()),
        },
        Frame::Boolean {
          data: true,
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
      data: "a".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data: 42,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data: "b".as_bytes().into(),
      attributes: None,
    };
    let v2 = Frame::Boolean {
      data: true,
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
      data: expected,
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
      data: expected,
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  #[should_panic]
  fn should_reconstruct_map_odd_elements() {
    let k1 = Frame::SimpleString {
      data: "a".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data: 42,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data: "b".as_bytes().into(),
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
      data: "a".into(),
      attributes: None,
    };
    let v2 = Frame::Number {
      data: 42,
      attributes: None,
    };
    let v3 = Frame::BlobString {
      data: "b".as_bytes().into(),
      attributes: None,
    };
    let v4 = Frame::Boolean {
      data: true,
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
      data: expected,
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
      data: expected,
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_reconstruct_nested_array() {
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Array {
      data: vec![
        Frame::SimpleString {
          data: "foo".into(),
          attributes: None,
        },
        Frame::Null,
        Frame::BigNumber {
          data: "123456789".as_bytes().into(),
          attributes: None,
        },
      ],
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Boolean {
      data: true,
      attributes: None,
    });

    let expected = Frame::Array {
      data: vec![
        Frame::SimpleString {
          data: "foo".into(),
          attributes: None,
        },
        Frame::Array {
          data: vec![
            Frame::SimpleString {
              data: "foo".into(),
              attributes: None,
            },
            Frame::Null,
            Frame::BigNumber {
              data: "123456789".as_bytes().into(),
              attributes: None,
            },
          ],
          attributes: None,
        },
        Frame::Boolean {
          data: true,
          attributes: None,
        },
      ],
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let mut streamed_frame = StreamedFrame::new(FrameKind::Array);
    streamed_frame.add_frame(Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Array {
      data: vec![
        Frame::SimpleString {
          data: "foo".into(),
          attributes: None,
        },
        Frame::Null,
        Frame::BigNumber {
          data: "123456789".as_bytes().into(),
          attributes: Some(attributes.clone()),
        },
      ],
      attributes: None,
    });
    streamed_frame.add_frame(Frame::Boolean {
      data: true,
      attributes: Some(attributes.clone()),
    });
    streamed_frame.attributes = Some(attributes.clone());

    let expected = Frame::Array {
      data: vec![
        Frame::SimpleString {
          data: "foo".into(),
          attributes: None,
        },
        Frame::Array {
          data: vec![
            Frame::SimpleString {
              data: "foo".into(),
              attributes: None,
            },
            Frame::Null,
            Frame::BigNumber {
              data: "123456789".as_bytes().into(),
              attributes: Some(attributes.clone()),
            },
          ],
          attributes: None,
        },
        Frame::Boolean {
          data: true,
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
      data: "a".into(),
      attributes: None,
    };
    let mut v1 = Frame::Number {
      data: 42,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data: "b".as_bytes().into(),
      attributes: None,
    };
    let inner_k1 = Frame::SimpleString {
      data: "b".into(),
      attributes: None,
    };
    let mut inner_v1 = Frame::BlobString {
      data: "foobarbaz".as_bytes().into(),
      attributes: None,
    };
    let mut inner_map = new_map(None);
    inner_map.insert(inner_k1.clone(), inner_v1.clone());
    let v2 = Frame::Map {
      data: inner_map,
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
      data: expected,
      attributes: None,
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);

    let (attributes, _) = create_attributes();
    let _ = v1.add_attributes(attributes.clone()).unwrap();
    let _ = inner_v1.add_attributes(attributes.clone()).unwrap();
    let mut inner_map = new_map(None);
    inner_map.insert(inner_k1, inner_v1);
    let v2 = Frame::Map {
      data: inner_map,
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
      data: expected,
      attributes: Some(attributes),
    };
    assert_eq!(streamed_frame.into_frame().unwrap(), expected);
  }

  #[test]
  fn should_get_encode_len_blobstring() {
    let mut frame = Frame::BlobString {
      data: "foobarbaz".as_bytes().into(),
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
      data: "foobarbaz".as_bytes().into(),
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
      data: "123456789".as_bytes().into(),
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
      data: true,
      attributes: None,
    };
    let expected_len = 1 + 1 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);

    let mut frame = Frame::Boolean {
      data: false,
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
      data: 500,
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
      data: -500,
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
      data: 500.123,
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
      data: -500.123,
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
      data: f64::INFINITY,
      attributes: None,
    };
    let expected_len = 1 + 3 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);

    let mut frame = Frame::Double {
      data: f64::NEG_INFINITY,
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
      auth: None,
    };
    let expected_len = 5 + 1 + 1 + 1;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let frame = Frame::Hello {
      version: RespVersion::RESP2,
      auth: None,
    };
    let expected_len = 5 + 1 + 1 + 1;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let frame = Frame::Hello {
      version: RespVersion::RESP3,
      auth: Some(Auth {
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
      format: VerbatimStringFormat::Markdown,
      data: "foobarbaz".into(),
      attributes: None,
    };
    let expected_len = 1 + 2 + 2 + 3 + 1 + 9 + 2;
    assert_eq!(encode_len(&frame).unwrap(), expected_len);

    let (attributes, attributes_len) = create_attributes();
    let _ = frame.add_attributes(attributes).unwrap();
    assert_eq!(encode_len(&frame).unwrap(), expected_len + attributes_len);

    let mut frame = Frame::VerbatimString {
      format: VerbatimStringFormat::Text,
      data: "foobarbaz".into(),
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
