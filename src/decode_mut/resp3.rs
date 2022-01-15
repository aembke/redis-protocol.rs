use crate::decode_mut::frame::Resp3IndexFrame;
use crate::decode_mut::utils::range_to_bytes;
use crate::resp3::types::{Attributes, Frame as Resp3Frame};
use crate::resp3::utils as resp3_utils;
use crate::types::{RedisProtocolError, RedisProtocolErrorKind};
use bytes::{Bytes, BytesMut};
use bytes_utils::Str;

fn parse_as_attributes(buf: &Bytes, range: Option<(usize, usize)>) -> Result<Option<Attributes>, RedisProtocolError> {
  unimplemented!()
}
// TODO reconsider storing attributes this way. they're already a map, so maybe make a FrameIndexMap and parse that...

fn make_bytes_frame(buf: &Bytes, frame: Resp3IndexFrame) -> Result<Resp3Frame, RedisProtocolError> {
  let out = match frame {
    Resp3IndexFrame::SimpleString { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::SimpleString { data, attributes }
    }
    Resp3IndexFrame::SimpleError { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let data = Str::from_inner(data)?;
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::SimpleError { data, attributes }
    }
    Resp3IndexFrame::BlobString { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::BlobString { data, attributes }
    }
    Resp3IndexFrame::BlobError { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::BlobError { data, attributes }
    }
    Resp3IndexFrame::Number { data, attributes } => {
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::Number { data, attributes }
    }
    Resp3IndexFrame::Double { data, attributes } => {
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::Double { data, attributes }
    }
    Resp3IndexFrame::Boolean { data, attributes } => {
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::Boolean { data, attributes }
    }
    Resp3IndexFrame::Null => Resp3Frame::Null,
    Resp3IndexFrame::BigNumber { data, attributes } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = parse_as_attributes(buf, attributes)?;
      Resp3Frame::BigNumber { data, attributes }
    }
    Resp3IndexFrame::VerbatimString {
      data,
      attributes,
      format,
    } => {
      let data = range_to_bytes(buf, data.0, data.1)?;
      let attributes = parse_as_attributes(buf, attributes)?;
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
      let attributes = parse_as_attributes(buf, attributes)?;
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(make_bytes_frame(buf, frame)?);
      }
      Resp3Frame::Array { data: out, attributes }
    }
    Resp3IndexFrame::Push { data, attributes } => {
      let attributes = parse_as_attributes(buf, attributes)?;
      let mut out = Vec::with_capacity(data.len());
      for frame in data.into_iter() {
        out.push(make_bytes_frame(buf, frame)?);
      }
      Resp3Frame::Push { data: out, attributes }
    }
    Resp3IndexFrame::Map { data, attributes } => {
      let attributes = parse_as_attributes(buf, attributes)?;
      let mut out = resp3_utils::new_map(Some(data.len()));
      for (key, value) in data.into_iter() {
        let key = make_bytes_frame(buf, key)?;
        let value = make_bytes_frame(buf, value)?;
        out.insert(key, value);
      }
      Resp3Frame::Map { data: out, attributes }
    }
    Resp3IndexFrame::Set { data, attributes } => {
      let attributes = parse_as_attributes(buf, attributes)?;
      let mut out = resp3_utils::new_set(Some(data.len()));
      for frame in data.into_iter() {
        out.insert(make_bytes_frame(buf, frame)?);
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
  let frame = make_bytes_frame(&buffer, frame)?;
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
