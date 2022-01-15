use crate::decode_mut::frame::{DecodedIndexFrame, IndexAttributes, IndexFrameMap, Resp3IndexFrame};
use crate::decode_mut::utils::range_to_bytes;
use crate::resp3::decode::*;
use crate::resp3::types::{Attributes, Frame as Resp3Frame};
use crate::resp3::utils as resp3_utils;
use crate::types::{RedisParseError, RedisProtocolError, RedisProtocolErrorKind};
use bytes::{Bytes, BytesMut};
use bytes_utils::Str;
use std::collections::{HashMap, HashSet};

fn to_map<'a>(mut data: Vec<Resp3IndexFrame>) -> Result<IndexFrameMap, RedisParseError<&'a [u8]>> {
  if data.len() % 2 != 0 {
    return Err(RedisParseError::new_custom("to_map", "Invalid hashmap frame length."));
  }

  let mut out = HashMap::with_capacity(data.len() / 2);
  while data.len() >= 2 {
    let value = data.pop().unwrap();
    let key = data.pop().unwrap();

    out.insert(key, value);
  }

  Ok(out)
}

fn to_set<'a>(data: Vec<Resp3IndexFrame>) -> Result<HashSet<Resp3IndexFrame>, RedisParseError<&'a [u8]>> {
  let mut out = HashSet::with_capacity(data.len());
  for frame in data.into_iter() {
    out.insert(frame);
  }
  Ok(out)
}

fn attach_attributes<'a>(
  attributes: IndexFrameMap,
  mut frame: DecodedIndexFrame,
) -> Result<DecodedIndexFrame, RedisParseError<&'a [u8]>> {
  if let Err(e) = frame.add_attributes(attributes) {
    Err(RedisParseError::new_custom("attach_attributes", format!("{:?}", e)))
  } else {
    Ok(frame)
  }
}

//
//
//
//
//
//
//

fn build_attributes(buf: &Bytes, attributes: IndexAttributes) -> Result<Option<Attributes>, RedisProtocolError> {
  if let Some(attributes) = attributes {
    let mut out = resp3_utils::new_map(Some(attributes.len()));
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

fn build_bytes_frame(buf: &Bytes, frame: Resp3IndexFrame) -> Result<Resp3Frame, RedisProtocolError> {
  let out = match frame {
    Resp3IndexFrame::SimpleString { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::SimpleString { data, attributes }
    }
    Resp3IndexFrame::SimpleError { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let data = Str::from_inner(data)?;
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::SimpleError { data, attributes }
    }
    Resp3IndexFrame::BlobString { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::BlobString { data, attributes }
    }
    Resp3IndexFrame::BlobError { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::BlobError { data, attributes }
    }
    Resp3IndexFrame::Number { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::Number { data, attributes }
    }
    Resp3IndexFrame::Double { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::Double { data, attributes }
    }
    Resp3IndexFrame::Boolean { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::Boolean { data, attributes }
    }
    Resp3IndexFrame::Null => Resp3Frame::Null,
    Resp3IndexFrame::BigNumber { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::BigNumber { data, attributes }
    }
    Resp3IndexFrame::VerbatimString {
      data,
      attributes,
      format,
    } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = build_attributes(buf, attributes)?;
      Resp3Frame::VerbatimString {
        data,
        attributes,
        format,
      }
    }
    Resp3IndexFrame::Hello { version, auth } => Resp3Frame::Hello { version, auth },
    Resp3IndexFrame::ChunkedString((start, end)) => {
      let data = range_to_bytes(buf, start, end)?;
      Resp3Frame::ChunkedString(data)
    }
    Resp3IndexFrame::Array { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(build_bytes_frame(buf, frame)?);
      }
      Resp3Frame::Array { data: out, attributes }
    }
    Resp3IndexFrame::Push { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(build_bytes_frame(buf, frame)?);
      }
      Resp3Frame::Push { data: out, attributes }
    }
    Resp3IndexFrame::Map { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      let mut out = resp3_utils::new_map(Some(data.len()));
      for (key, value) in data.into_iter() {
        let key = build_bytes_frame(buf, key)?;
        let value = build_bytes_frame(buf, value)?;
        out.insert(key, value);
      }
      Resp3Frame::Map { data: out, attributes }
    }
    Resp3IndexFrame::Set { data, attributes } => {
      let attributes = build_attributes(buf, attributes)?;
      let mut out = resp3_utils::new_set(Some(data.len()));
      for frame in data.into_iter() {
        out.insert(build_bytes_frame(buf, frame)?);
      }
      Resp3Frame::Set { data: out, attributes }
    }
  };

  Ok(out)
}

fn freeze_parse(
  buf: &mut BytesMut,
  frame: Resp3IndexFrame,
  amt: usize,
) -> Result<(Resp3Frame, usize, Bytes), RedisProtocolError> {
  if amt > buf.len() {
    return Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid parsed amount > buffer length.",
    ));
  }

  let buffer = buf.split_to(amt).freeze();
  let frame = build_bytes_frame(&buffer, frame)?;
  Ok((frame, amt, buffer))
}

pub mod complete {
  use super::*;

  /// Attempt to parse the contents of `buf`, returning the first valid frame, the number of bytes consumed, and the frozen consumed bytes.
  ///
  /// If the byte slice contains an incomplete frame then `None` is returned.
  ///
  /// Unlike `decode` this function works on a mutable `BytesMut` buffer in order to parse frames without copying the inner buffer contents
  /// and will automatically split off the consumed bytes before returning. If an error or `None` is returned the buffer will not be modified.
  pub fn decode_mut(buf: &mut BytesMut) -> Result<Option<(Resp3Frame, usize, Bytes)>, RedisProtocolError> {
    unimplemented!()
  }
}

pub mod streaming {
  use super::*;
  use crate::resp3::types::DecodedFrame;

  /// Attempt to parse the contents of `buf`, returning the first valid frame, the number of bytes consumed, and the frozen consumed bytes.
  ///
  /// If the byte slice contains an incomplete frame then `None` is returned.
  ///
  /// Unlike `decode` this function works on a mutable `BytesMut` buffer in order to parse frames without copying the inner buffer contents
  /// and will automatically split off the consumed bytes before returning. If an error or `None` is returned the buffer will not be modified.
  pub fn decode_mut(buf: &mut BytesMut) -> Result<Option<(DecodedFrame, usize, Bytes)>, RedisProtocolError> {
    unimplemented!()
  }
}
