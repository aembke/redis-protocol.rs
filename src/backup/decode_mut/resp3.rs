use crate::decode_mut::frame::{
  DResult, DecodedIndexFrame, IndexAttributes, IndexFrameMap, Resp3IndexFrame, StreamedIndexFrame,
};
use crate::decode_mut::utils::range_to_bytes;
use crate::resp3::decode::*;
use crate::resp3::types::{
  Attributes, DecodedFrame, Frame as Resp3Frame, FrameKind, StreamedFrame, AUTH, EMPTY_SPACE, HELLO,
};
use crate::resp3::utils as resp3_utils;
use crate::types::{RedisParseError, RedisProtocolError, RedisProtocolErrorKind, CRLF};
use bytes::{Bytes, BytesMut};
use bytes_utils::Str;
use nom::bytes::streaming::{take as nom_take, take_until as nom_take_until};
use nom::combinator::{map as nom_map, map_res as nom_map_res, opt as nom_opt};
use nom::multi::count as nom_count;
use nom::number::streaming::be_u8;
use nom::sequence::terminated as nom_terminated;
use nom::{AsBytes, Err as NomErr};
use core::str;

#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};

fn map_hello<'a>(frame: Resp3Frame) -> Result<Resp3IndexFrame, RedisParseError<&'a [u8]>> {
  match frame {
    Resp3Frame::Hello { version, auth } => Ok(Resp3IndexFrame::Hello { version, auth }),
    _ => Err(RedisParseError::new_custom(
      "map_hello",
      "Invalid frame, expected HELLO.",
    )),
  }
}

fn map_complete_frame(frame: Resp3IndexFrame) -> DecodedIndexFrame {
  DecodedIndexFrame::Complete(frame)
}

fn unwrap_complete_index_frame<'a>(frame: DecodedIndexFrame) -> Result<Resp3IndexFrame, RedisParseError<&'a [u8]>> {
  frame
    .into_complete_frame()
    .map_err(|e| RedisParseError::new_custom("unwrap_complete_frame", format!("{:?}", e)))
}

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

fn d_read_to_crlf(input: (&[u8], usize)) -> DResult<usize> {
  decode_log!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data.len()))
}

fn d_read_to_crlf_take(input: (&[u8], usize)) -> DResult<&[u8]> {
  decode_log!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data))
}

fn d_read_prefix_len(input: (&[u8], usize)) -> DResult<usize> {
  let ((input, offset), data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok(((input, offset), etry!(to_usize(data))))
}

fn d_read_prefix_len_signed(input: (&[u8], usize)) -> DResult<isize> {
  let ((input, offset), data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok(((input, offset), etry!(to_isize(&data))))
}

fn d_frame_type(input: (&[u8], usize)) -> DResult<FrameKind> {
  let (input_bytes, byte) = be_u8(input.0)?;
  let kind = match FrameKind::from_byte(byte) {
    Some(k) => k,
    None => e!(RedisParseError::new_custom("frame_type", "Invalid frame type prefix.")),
  };
  decode_log!(
    input_bytes,
    "Parsed frame type {:?}, remaining: {:?}",
    kind,
    input_bytes
  );

  Ok(((input_bytes, input.1 + 1), kind))
}

fn d_parse_simplestring(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok((
    (input, next_offset),
    Resp3IndexFrame::SimpleString {
      data: (offset, offset + len),
      attributes: None,
    },
  ))
}

fn d_parse_simpleerror(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok((
    (input, next_offset),
    Resp3IndexFrame::SimpleError {
      data: (offset, offset + len),
      attributes: None,
    },
  ))
}

fn d_parse_number(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_i64(&data));
  Ok((
    (input, next_offset),
    Resp3IndexFrame::Number {
      data: parsed,
      attributes: None,
    },
  ))
}

fn d_parse_double(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_f64(&data));
  Ok((
    (input, next_offset),
    Resp3IndexFrame::Double {
      data: parsed,
      attributes: None,
    },
  ))
}

fn d_parse_boolean(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_bool(&data));
  Ok((
    (input, next_offset),
    Resp3IndexFrame::Boolean {
      data: parsed,
      attributes: None,
    },
  ))
}

fn d_parse_null(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let ((input, next_offset), _) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), Resp3IndexFrame::Null))
}

fn d_parse_blobstring(input: (&[u8], usize), len: usize) -> DResult<Resp3IndexFrame> {
  let offset = input.1;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input.0)?;

  Ok((
    (input, offset + len + 2),
    Resp3IndexFrame::BlobString {
      data: (offset, offset + data.len()),
      attributes: None,
    },
  ))
}

fn d_parse_bloberror(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input)?;

  Ok((
    (input, offset + len + 2),
    Resp3IndexFrame::BlobError {
      data: (offset, offset + data.len()),
      attributes: None,
    },
  ))
}

fn d_parse_verbatimstring(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let ((input, prefix_offset), len) = d_read_prefix_len(input)?;
  let (input, format_bytes) = nom_terminated(nom_take(3_usize), nom_take(1_usize))(input)?;
  let format = etry!(to_verbatimstring_format(&format_bytes));
  let (input, _) = nom_terminated(nom_take(len - 4), nom_take(2_usize))(input)?;

  Ok((
    (input, prefix_offset + len + 2),
    Resp3IndexFrame::VerbatimString {
      data: (prefix_offset + 4, prefix_offset + len),
      format,
      attributes: None,
    },
  ))
}

fn d_parse_bignumber(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;

  Ok((
    (input, next_offset),
    Resp3IndexFrame::BigNumber {
      data: (offset, offset + len),
      attributes: None,
    },
  ))
}

fn d_parse_array_frames(input: (&[u8], usize), len: usize) -> DResult<Vec<Resp3IndexFrame>> {
  nom_count(
    nom_map_res(d_parse_frame_or_attribute, unwrap_complete_index_frame),
    len,
  )(input)
}

fn d_parse_kv_pairs(input: (&[u8], usize), len: usize) -> DResult<IndexFrameMap> {
  nom_map_res(
    nom_count(
      nom_map_res(d_parse_frame_or_attribute, unwrap_complete_index_frame),
      len * 2,
    ),
    to_map,
  )(input)
}

fn d_parse_array(input: (&[u8], usize), len: usize) -> DResult<Resp3IndexFrame> {
  let (input, data) = d_parse_array_frames(input, len)?;
  Ok((input, Resp3IndexFrame::Array { data, attributes: None }))
}

fn d_parse_push(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, data) = d_parse_array_frames(input, len)?;
  Ok((input, Resp3IndexFrame::Push { data, attributes: None }))
}

fn d_parse_set(input: (&[u8], usize), len: usize) -> DResult<Resp3IndexFrame> {
  let (input, frames) = d_parse_array_frames(input, len)?;
  let set = etry!(to_set(frames));

  Ok((
    input,
    Resp3IndexFrame::Set {
      data: set,
      attributes: None,
    },
  ))
}

fn d_parse_map(input: (&[u8], usize), len: usize) -> DResult<Resp3IndexFrame> {
  let (input, frames) = d_parse_kv_pairs(input, len)?;

  Ok((
    input,
    Resp3IndexFrame::Map {
      data: frames,
      attributes: None,
    },
  ))
}

fn d_parse_attribute(input: (&[u8], usize)) -> DResult<IndexFrameMap> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, attributes) = d_parse_kv_pairs(input, len)?;
  Ok((input, attributes))
}

fn d_parse_hello(input: (&[u8], usize)) -> DResult<Resp3IndexFrame> {
  let mut offset = input.1;
  let (input, _) = nom_map_res(
    nom_terminated(nom_take_until(HELLO.as_bytes()), nom_take(1_usize)),
    str::from_utf8,
  )(input.0)?;
  offset += HELLO.as_bytes().len() + 1;

  let (input, version) = be_u8(input)?;
  offset += 1;

  let (input, auth) = nom_opt(nom_map_res(
    nom_terminated(nom_take_until(AUTH.as_bytes()), nom_take(1_usize)),
    str::from_utf8,
  ))(input)?;

  let (input, auth) = if auth.is_some() {
    offset += AUTH.as_bytes().len() + 1;

    let (input, username) = nom_map_res(
      nom_terminated(nom_take_until(EMPTY_SPACE.as_bytes()), nom_take(1_usize)),
      str::from_utf8,
    )(input)?;
    let (input, password) = nom_map_res(nom_take_until(CRLF.as_bytes()), str::from_utf8)(input)?;
    offset += username.as_bytes().len() + password.as_bytes().len() + 1 + 2;

    (input, Some((Str::from(username), Str::from(password))))
  } else {
    (input, None)
  };

  let hello = etry!(to_hello(version, auth));
  Ok(((input, offset), etry!(map_hello(hello))))
}

/// Check for a streaming variant of a frame, and if found then return the prefix bytes only, otherwise return the complete frame.
///
/// Only supported for arrays, sets, maps, and blob strings.
fn d_check_streaming(input: (&[u8], usize), kind: FrameKind) -> DResult<DecodedIndexFrame> {
  let (input, len) = d_read_prefix_len_signed(input)?;
  let (input, frame) = if len == -1 {
    (input, DecodedIndexFrame::Streaming(StreamedIndexFrame::new(kind)))
  } else {
    let len = etry!(isize_to_usize(len));
    let (input, frame) = match kind {
      FrameKind::Array => d_parse_array(input, len)?,
      FrameKind::Set => d_parse_set(input, len)?,
      FrameKind::Map => d_parse_map(input, len)?,
      FrameKind::BlobString => d_parse_blobstring(input, len)?,
      _ => e!(RedisParseError::new_custom(
        "check_streaming",
        format!("Invalid frame type: {:?}", kind)
      )),
    };

    (input, DecodedIndexFrame::Complete(frame))
  };

  Ok((input, frame))
}

fn d_parse_chunked_string(input: (&[u8], usize)) -> DResult<DecodedIndexFrame> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, frame) = if len == 0 {
    (input, Resp3IndexFrame::new_end_stream())
  } else {
    let offset = input.1;
    let (input, contents) = nom_terminated(nom_take(len), nom_take(2_usize))(input.0)?;

    (
      (input, offset + contents.len() + 2),
      Resp3IndexFrame::ChunkedString((offset, offset + len)),
    )
  };

  Ok((input, DecodedIndexFrame::Complete(frame)))
}

fn d_return_end_stream(input: (&[u8], usize)) -> DResult<DecodedIndexFrame> {
  let (input, _) = d_read_to_crlf(input)?;
  Ok((input, DecodedIndexFrame::Complete(Resp3IndexFrame::new_end_stream())))
}

fn d_parse_non_attribute_frame(input: (&[u8], usize), kind: FrameKind) -> DResult<DecodedIndexFrame> {
  let (input, frame) = match kind {
    FrameKind::Array => d_check_streaming(input, kind)?,
    FrameKind::BlobString => d_check_streaming(input, kind)?,
    FrameKind::Map => d_check_streaming(input, kind)?,
    FrameKind::Set => d_check_streaming(input, kind)?,
    FrameKind::SimpleString => nom_map(d_parse_simplestring, map_complete_frame)(input)?,
    FrameKind::SimpleError => nom_map(d_parse_simpleerror, map_complete_frame)(input)?,
    FrameKind::Number => nom_map(d_parse_number, map_complete_frame)(input)?,
    FrameKind::Null => nom_map(d_parse_null, map_complete_frame)(input)?,
    FrameKind::Double => nom_map(d_parse_double, map_complete_frame)(input)?,
    FrameKind::Boolean => nom_map(d_parse_boolean, map_complete_frame)(input)?,
    FrameKind::BlobError => nom_map(d_parse_bloberror, map_complete_frame)(input)?,
    FrameKind::VerbatimString => nom_map(d_parse_verbatimstring, map_complete_frame)(input)?,
    FrameKind::Push => nom_map(d_parse_push, map_complete_frame)(input)?,
    FrameKind::BigNumber => nom_map(d_parse_bignumber, map_complete_frame)(input)?,
    FrameKind::Hello => nom_map(d_parse_hello, map_complete_frame)(input)?,
    FrameKind::ChunkedString => d_parse_chunked_string(input)?,
    FrameKind::EndStream => d_return_end_stream(input)?,
    FrameKind::Attribute => {
      error!("Found unexpected attribute frame.");
      e!(RedisParseError::new_custom(
        "parse_non_attribute_frame",
        "Unexpected attribute frame.",
      ));
    }
  };

  Ok((input, frame))
}

fn d_parse_attribute_and_frame(input: (&[u8], usize)) -> DResult<DecodedIndexFrame> {
  let (input, attributes) = d_parse_attribute(input)?;
  let (input, kind) = d_frame_type(input)?;
  let (input, next_frame) = d_parse_non_attribute_frame(input, kind)?;
  let frame = etry!(attach_attributes(attributes, next_frame));

  Ok((input, frame))
}

fn d_parse_frame_or_attribute(input: (&[u8], usize)) -> DResult<DecodedIndexFrame> {
  let (input, kind) = d_frame_type(input)?;
  let (input, frame) = if let FrameKind::Attribute = kind {
    d_parse_attribute_and_frame(input)?
  } else {
    d_parse_non_attribute_frame(input, kind)?
  };

  Ok((input, frame))
}

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

fn build_bytes_decoded_frame(buf: &Bytes, frame: DecodedIndexFrame) -> Result<DecodedFrame, RedisProtocolError> {
  let out = match frame {
    DecodedIndexFrame::Streaming(streaming) => {
      let mut frame = StreamedFrame::new(streaming.kind);
      frame.attributes = build_attributes(buf, streaming.attributes)?;
      DecodedFrame::Streaming(frame)
    }
    DecodedIndexFrame::Complete(frame) => {
      let frame = build_bytes_frame(buf, frame)?;
      DecodedFrame::Complete(frame)
    }
  };

  Ok(out)
}

fn freeze_parse(
  buf: &mut BytesMut,
  frame: DecodedIndexFrame,
  amt: usize,
) -> Result<(DecodedFrame, usize, Bytes), RedisProtocolError> {
  if amt > buf.len() {
    return Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid parsed amount > buffer length.",
    ));
  }

  let buffer = buf.split_to(amt).freeze();
  let frame = build_bytes_decoded_frame(&buffer, frame)?;
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
  ///
  /// Implement a [codec](https://docs.rs/tokio-util/0.6.6/tokio_util/codec/index.html) that only supports complete frames...
  ///
  /// ```edition2018 no_run
  ///
  /// use redis_protocol::resp3::types::*;
  /// use redis_protocol::types::{RedisProtocolError, RedisProtocolErrorKind};
  /// use redis_protocol::resp3::decode::complete::decode_mut;
  /// use redis_protocol::resp3::encode::complete::encode_bytes;
  /// use bytes::BytesMut;
  /// use tokio_util::codec::{Decoder, Encoder};
  ///
  /// pub struct RedisCodec {}
  ///
  /// impl Encoder<Frame> for RedisCodec {
  ///   type Error = RedisProtocolError;
  ///
  ///   fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
  ///     // in this example we only show support for encoding complete frames
  ///     let _ = encode_bytes(dst, &item)?;
  ///     Ok(())
  ///   }
  /// }
  ///
  /// impl Decoder for RedisCodec {
  ///   type Item = Frame;
  ///   type Error = RedisProtocolError;
  ///
  ///   fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
  ///     if src.is_empty() {
  ///       return Ok(None);
  ///     }
  ///
  ///     if let Some((frame, amt, buf)) = decode_mut(src)? {
  ///       // the `src` buffer will have been modified already at this point, and the `buf` bytes will have been split off
  ///
  ///       Ok(Some(frame))
  ///     }else{
  ///       Ok(None)
  ///     }
  ///   }
  /// }
  /// ```
  #[cfg_attr(docsrs, doc(cfg(feature = "decode-mut")))]
  pub fn decode_mut(buf: &mut BytesMut) -> Result<Option<(Resp3Frame, usize, Bytes)>, RedisProtocolError> {
    let (offset, len) = (0, buf.len());

    let (frame, amt) = match d_parse_frame_or_attribute((buf.as_bytes(), offset)) {
      Ok(((remaining, offset), frame)) => {
        assert_eq!(offset, len - remaining.len(), "returned offset doesn't match");
        (frame, offset)
      }
      Err(NomErr::Incomplete(_)) => return Ok(None),
      Err(NomErr::Error(e)) => return Err(e.into()),
      Err(NomErr::Failure(e)) => return Err(e.into()),
    };
    decode_log!(buf, "Decoded frame with amt {}: {:?} from buffer {:?}", amt, frame, buf);

    freeze_parse(buf, frame, amt).and_then(|(frame, amt, buf)| {
      let frame = unwrap_complete_frame(frame)?;
      Ok(Some((frame, amt, buf)))
    })
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
  ///
  /// Implement a [codec](https://docs.rs/tokio-util/0.6.6/tokio_util/codec/index.html) that supports decoding streams...
  ///
  /// ```edition2018 no_run
  /// use redis_protocol::resp3::types::*;
  /// use redis_protocol::types::{RedisProtocolError, RedisProtocolErrorKind};
  /// use redis_protocol::resp3::decode::streaming::decode_mut;
  /// use redis_protocol::resp3::encode::complete::encode_bytes;
  /// use bytes::BytesMut;
  /// use tokio_util::codec::{Decoder, Encoder};
  ///
  /// pub struct RedisCodec {
  ///   decoder_stream: Option<StreamedFrame>
  /// }
  ///
  /// impl Encoder<Frame> for RedisCodec {
  ///   type Error = RedisProtocolError;
  ///
  ///   fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
  ///     // in this example we only show support for encoding complete frames. see the resp3 encoder
  ///     // documentation for examples showing how encode streaming frames
  ///     let _ = encode_bytes(dst, &item)?;
  ///     Ok(())
  ///   }
  /// }
  ///
  /// impl Decoder for RedisCodec {
  ///   type Item = Frame;
  ///   type Error = RedisProtocolError;
  ///
  ///   // Buffer the results of streamed frame before returning the complete frame to the caller.
  ///   // Callers that want to surface streaming frame chunks up the stack would simply return after calling `decode` here.
  ///   fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
  ///     if src.is_empty() {
  ///       return Ok(None);
  ///     }
  ///
  ///     if let Some((frame, amt, _)) = decode_mut(src)? {
  ///       if self.decoder_stream.is_some() && frame.is_streaming() {
  ///         // it doesn't make sense to start a stream while inside another stream
  ///         return Err(RedisProtocolError::new(
  ///           RedisProtocolErrorKind::DecodeError,
  ///           "Cannot start a stream while already inside a stream."
  ///         ));
  ///       }
  ///
  ///       let result = if let Some(ref mut streamed_frame) = self.decoder_stream {
  ///         // we started receiving streamed data earlier
  ///
  ///         // we already checked for streams within streams above
  ///         let frame = frame.into_complete_frame()?;
  ///         streamed_frame.add_frame(frame);
  ///
  ///         if streamed_frame.is_finished() {
  ///            // convert the inner stream buffer into the final output frame
  ///            Some(streamed_frame.into_frame()?)
  ///         }else{
  ///           // buffer the stream in memory until it completes
  ///           None
  ///         }
  ///       }else{
  ///         // we're processing a complete frame or starting a new streamed frame
  ///         if frame.is_streaming() {
  ///           // start a new stream, saving the internal buffer to the codec state
  ///           self.decoder_stream = Some(frame.into_streaming_frame()?);
  ///           // don't return anything to the caller until the stream finishes (shown above)
  ///           None
  ///         }else{
  ///           // we're not in the middle of a stream and we found a complete frame
  ///           Some(frame.into_complete_frame()?)
  ///         }
  ///       };
  ///
  ///       if result.is_some() {
  ///         // we're either done with the stream or we found a complete frame. either way clear the buffer.
  ///         let _ = self.decoder_stream.take();
  ///       }
  ///
  ///       Ok(result)
  ///     }else{
  ///       Ok(None)
  ///     }
  ///   }
  /// }
  /// ```
  #[cfg_attr(docsrs, doc(cfg(feature = "decode-mut")))]
  pub fn decode_mut(buf: &mut BytesMut) -> Result<Option<(DecodedFrame, usize, Bytes)>, RedisProtocolError> {
    let (offset, len) = (0, buf.len());

    let (frame, amt) = match d_parse_frame_or_attribute((buf.as_bytes(), offset)) {
      Ok(((remaining, offset), frame)) => {
        assert_eq!(offset, len - remaining.len(), "returned offset doesn't match");
        (frame, offset)
      }
      Err(NomErr::Incomplete(_)) => return Ok(None),
      Err(NomErr::Error(e)) => return Err(e.into()),
      Err(NomErr::Failure(e)) => return Err(e.into()),
    };
    decode_log!(buf, "Decoded frame with amt {}: {:?} from buffer {:?}", amt, frame, buf);

    freeze_parse(buf, frame, amt).map(|r| Some(r))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::decode_mut::resp3::complete::decode_mut;
  use crate::decode_mut::resp3::streaming::decode_mut as stream_decode_mut;
  use crate::resp3::decode::tests::*;
  use crate::resp3::types::{Frame, VerbatimStringFormat};
  use bytes::{Bytes, BytesMut};

  fn decode_and_verify_some(bytes: &Bytes, expected: &(Option<Frame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    let total_len = bytes.len();

    let (frame, len, buf) = match decode_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
    assert_eq!(buf.len(), expected.1, "output buffer len matched");
    assert_eq!(buf.len() + bytes.len(), total_len, "total len matched");
  }

  fn decode_and_verify_padded_some(bytes: &Bytes, expected: &(Option<Frame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    bytes.extend_from_slice(PADDING.as_bytes());
    let total_len = bytes.len();

    let (frame, len, buf) = match decode_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
    assert_eq!(buf.len(), expected.1, "output buffer len matched");
    assert_eq!(buf.len() + bytes.len(), total_len, "total len matched");
  }

  fn decode_and_verify_none(bytes: &Bytes) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    let (frame, len, buf) = match decode_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => (None, 0, Bytes::new()),
      Err(e) => return pretty_print_panic(e),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
    assert!(buf.is_empty());
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (
      Some(Frame::Number {
        data: 48293,
        attributes: None,
      }),
      8,
    );
    let bytes: Bytes = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (
      Some(Frame::SimpleString {
        data: "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (
      Some(Frame::SimpleString {
        data: "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_blob_string() {
    let expected = (
      Some(Frame::BlobString {
        data: "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_blob_string_incomplete() {
    let expected = (
      Some(Frame::BlobString {
        data: "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(Frame::Array {
        data: vec![
          Frame::SimpleString {
            data: "Foo".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "Bar".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      16,
    );
    let bytes: Bytes = "*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let bytes: Bytes = "*3\r\n$3\r\nFoo\r\n_\r\n$3\r\nBar\r\n".into();

    let expected = (
      Some(Frame::Array {
        data: vec![
          Frame::BlobString {
            data: "Foo".into(),
            attributes: None,
          },
          Frame::Null,
          Frame::BlobString {
            data: "Bar".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let bytes: Bytes = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(Frame::SimpleError {
        data: "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let bytes: Bytes = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (
      Some(Frame::SimpleError {
        data: "MOVED 3999 127.0.0.1:6381".into(),
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let bytes: Bytes = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (
      Some(Frame::SimpleError {
        data: "ASK 3999 127.0.0.1:6381".into(),
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_incomplete() {
    let bytes: Bytes = "*3\r\n$3\r\nFoo\r\n_\r\n$3\r\nBar".into();
    decode_and_verify_none(&bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    let mut bytes: BytesMut = "foobarbazwibblewobble".into();
    let _ = complete::decode_mut(&mut bytes).map_err(|e| pretty_print_panic(e));
  }

  // ----------------- end tests adapted from RESP2 ------------------------

  #[test]
  fn should_decode_blob_error() {
    let expected = (
      Some(Frame::BlobError {
        data: "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "!3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_blob_error_incomplete() {
    let expected = (
      Some(Frame::BlobError {
        data: "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "!3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_error() {
    let expected = (
      Some(Frame::SimpleError {
        data: "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "-string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_error_incomplete() {
    let expected = (
      Some(Frame::SimpleError {
        data: "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Bytes = "-strin".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_boolean_true() {
    let expected = (
      Some(Frame::Boolean {
        data: true,
        attributes: None,
      }),
      4,
    );
    let bytes: Bytes = "#t\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_boolean_false() {
    let expected = (
      Some(Frame::Boolean {
        data: false,
        attributes: None,
      }),
      4,
    );
    let bytes: Bytes = "#f\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_number() {
    let expected = (
      Some(Frame::Number {
        data: 42,
        attributes: None,
      }),
      5,
    );
    let bytes: Bytes = ":42\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_double_inf() {
    let expected = (
      Some(Frame::Double {
        data: f64::INFINITY,
        attributes: None,
      }),
      6,
    );
    let bytes: Bytes = ",inf\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_double_neg_inf() {
    let expected = (
      Some(Frame::Double {
        data: f64::NEG_INFINITY,
        attributes: None,
      }),
      7,
    );
    let bytes: Bytes = ",-inf\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_double_nan() {
    let expected = (
      Some(Frame::Double {
        data: f64::NAN,
        attributes: None,
      }),
      7,
    );
    let bytes: Bytes = ",foo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_double() {
    let expected = (
      Some(Frame::Double {
        data: 4.59193,
        attributes: None,
      }),
      10,
    );
    let bytes: Bytes = ",4.59193\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let expected = (
      Some(Frame::Double {
        data: 4_f64,
        attributes: None,
      }),
      4,
    );
    let bytes: Bytes = ",4\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bignumber() {
    let expected = (
      Some(Frame::BigNumber {
        data: "3492890328409238509324850943850943825024385".into(),
        attributes: None,
      }),
      46,
    );
    let bytes: Bytes = "(3492890328409238509324850943850943825024385\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_null() {
    let expected = (Some(Frame::Null), 3);
    let bytes: Bytes = "_\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_verbatim_string_mkd() {
    let expected = (
      Some(Frame::VerbatimString {
        data: "Some string".into(),
        format: VerbatimStringFormat::Markdown,
        attributes: None,
      }),
      22,
    );
    let bytes: Bytes = "=15\r\nmkd:Some string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_verbatim_string_txt() {
    let expected = (
      Some(Frame::VerbatimString {
        data: "Some string".into(),
        format: VerbatimStringFormat::Text,
        attributes: None,
      }),
      22,
    );
    let bytes: Bytes = "=15\r\ntxt:Some string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_map_no_nulls() {
    let k1 = Frame::SimpleString {
      data: "first".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data: 1,
      attributes: None,
    };
    let k2 = Frame::BlobString {
      data: "second".into(),
      attributes: None,
    };
    let v2 = Frame::Double {
      data: 4.2,
      attributes: None,
    };

    let mut expected_map = resp3_utils::new_map(None);
    expected_map.insert(k1, v1);
    expected_map.insert(k2, v2);
    let expected = (
      Some(Frame::Map {
        data: expected_map,
        attributes: None,
      }),
      34,
    );
    let bytes: Bytes = "%2\r\n+first\r\n:1\r\n$6\r\nsecond\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_map_with_nulls() {
    let k1 = Frame::SimpleString {
      data: "first".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data: 1,
      attributes: None,
    };
    let k2 = Frame::Number {
      data: 2,
      attributes: None,
    };
    let v2 = Frame::Null;
    let k3 = Frame::BlobString {
      data: "second".into(),
      attributes: None,
    };
    let v3 = Frame::Double {
      data: 4.2,
      attributes: None,
    };

    let mut expected_map = resp3_utils::new_map(None);
    expected_map.insert(k1, v1);
    expected_map.insert(k2, v2);
    expected_map.insert(k3, v3);
    let expected = (
      Some(Frame::Map {
        data: expected_map,
        attributes: None,
      }),
      41,
    );
    let bytes: Bytes = "%3\r\n+first\r\n:1\r\n:2\r\n_\r\n$6\r\nsecond\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_set_no_nulls() {
    let mut expected_set = resp3_utils::new_set(None);
    expected_set.insert(Frame::Number {
      data: 1,
      attributes: None,
    });
    expected_set.insert(Frame::SimpleString {
      data: "2".into(),
      attributes: None,
    });
    expected_set.insert(Frame::BlobString {
      data: "foobar".into(),
      attributes: None,
    });
    expected_set.insert(Frame::Double {
      data: 4.2,
      attributes: None,
    });
    let expected = (
      Some(Frame::Set {
        data: expected_set,
        attributes: None,
      }),
      30,
    );
    let bytes: Bytes = "~4\r\n:1\r\n+2\r\n$6\r\nfoobar\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_set_with_nulls() {
    let mut expected_set = resp3_utils::new_set(None);
    expected_set.insert(Frame::Number {
      data: 1,
      attributes: None,
    });
    expected_set.insert(Frame::SimpleString {
      data: "2".into(),
      attributes: None,
    });
    expected_set.insert(Frame::Null);
    expected_set.insert(Frame::Double {
      data: 4.2,
      attributes: None,
    });
    let expected = (
      Some(Frame::Set {
        data: expected_set,
        attributes: None,
      }),
      21,
    );
    let bytes: Bytes = "~4\r\n:1\r\n+2\r\n_\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_push_pubsub() {
    let expected = (
      Some(Frame::Push {
        data: vec![
          Frame::SimpleString {
            data: "pubsub".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "message".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "somechannel".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "this is the message".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      59,
    );
    let bytes: Bytes = ">4\r\n+pubsub\r\n+message\r\n+somechannel\r\n+this is the message\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let mut bytes = BytesMut::from(bytes.as_bytes());
    let (frame, _, _) = decode_mut(&mut bytes).unwrap().unwrap();
    assert!(frame.is_pubsub_message());
    assert!(frame.is_normal_pubsub());
  }

  #[test]
  fn should_decode_push_pattern_pubsub() {
    let expected = (
      Some(Frame::Push {
        data: vec![
          Frame::SimpleString {
            data: "pubsub".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "pmessage".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "somechannel".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "this is the message".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      60,
    );
    let bytes: Bytes = ">4\r\n+pubsub\r\n+pmessage\r\n+somechannel\r\n+this is the message\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let mut bytes = BytesMut::from(bytes.as_bytes());
    let (frame, _, _) = decode_mut(&mut bytes).unwrap().unwrap();
    assert!(frame.is_pattern_pubsub_message());
    assert!(frame.is_pubsub_message());
  }

  #[test]
  fn should_decode_keyevent_message() {
    let expected = (
      Some(Frame::Push {
        data: vec![
          Frame::SimpleString {
            data: "pubsub".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "pmessage".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "__key*".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "__keyevent@0__:set".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "foo".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      60,
    );
    let bytes: Bytes = ">5\r\n+pubsub\r\n+pmessage\r\n+__key*\r\n+__keyevent@0__:set\r\n+foo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let mut bytes = BytesMut::from(bytes.as_bytes());
    let (frame, _, _) = decode_mut(&mut bytes).unwrap().unwrap();
    assert!(frame.is_pattern_pubsub_message());
    assert!(frame.is_pubsub_message());
  }

  #[test]
  fn should_parse_outer_attributes() {
    let mut expected_inner_attrs = resp3_utils::new_map(None);
    expected_inner_attrs.insert(
      Frame::BlobString {
        data: "a".into(),
        attributes: None,
      },
      Frame::Double {
        data: 0.1923,
        attributes: None,
      },
    );
    expected_inner_attrs.insert(
      Frame::BlobString {
        data: "b".into(),
        attributes: None,
      },
      Frame::Double {
        data: 0.0012,
        attributes: None,
      },
    );
    let expected_inner_attrs = Frame::Map {
      data: expected_inner_attrs,
      attributes: None,
    };

    let mut expected_attrs = resp3_utils::new_map(None);
    expected_attrs.insert(
      Frame::SimpleString {
        data: "key-popularity".into(),
        attributes: None,
      },
      expected_inner_attrs,
    );

    let expected = (
      Some(Frame::Array {
        data: vec![
          Frame::Number {
            data: 2039123,
            attributes: None,
          },
          Frame::Number {
            data: 9543892,
            attributes: None,
          },
        ],
        attributes: Some(expected_attrs.into()),
      }),
      81,
    );

    let bytes: Bytes =
      "|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n"
        .into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_parse_inner_attributes() {
    let mut expected_attrs = resp3_utils::new_map(None);
    expected_attrs.insert(
      Frame::SimpleString {
        data: "ttl".into(),
        attributes: None,
      },
      Frame::Number {
        data: 3600,
        attributes: None,
      },
    );

    let expected = (
      Some(Frame::Array {
        data: vec![
          Frame::Number {
            data: 1,
            attributes: None,
          },
          Frame::Number {
            data: 2,
            attributes: None,
          },
          Frame::Number {
            data: 3,
            attributes: Some(expected_attrs),
          },
        ],
        attributes: None,
      }),
      33,
    );
    let bytes: Bytes = "*3\r\n:1\r\n:2\r\n|1\r\n+ttl\r\n:3600\r\n:3\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_end_stream() {
    let mut bytes: BytesMut = ";0\r\n".into();
    let (frame, _, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::new_end_stream()))
  }

  #[test]
  fn should_decode_streaming_string() {
    let mut bytes: BytesMut = "$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n".into();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Streaming(StreamedFrame::new(FrameKind::BlobString))
    );
    assert_eq!(amt, 4);

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::ChunkedString("Hell".into())));
    assert_eq!(amt, 10);

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::ChunkedString("o worl".into())));
    assert_eq!(amt, 12);

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::ChunkedString("d".into())));
    assert_eq!(amt, 7);

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::new_end_stream()));
    assert_eq!(amt, 4);
  }

  #[test]
  fn should_decode_streaming_array() {
    let mut bytes: BytesMut = "*?\r\n:1\r\n:2\r\n:3\r\n.\r\n".into();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Array)));
    assert_eq!(amt, 4);
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 1,
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 2,
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 3,
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.into_frame().unwrap();
    let expected = Frame::Array {
      data: vec![
        Frame::Number {
          data: 1,
          attributes: None,
        },
        Frame::Number {
          data: 2,
          attributes: None,
        },
        Frame::Number {
          data: 3,
          attributes: None,
        },
      ],
      attributes: None,
    };

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_streaming_set() {
    let mut bytes: BytesMut = "~?\r\n:1\r\n:2\r\n:3\r\n.\r\n".into();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Set)));
    assert_eq!(amt, 4);
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 1,
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 2,
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 3,
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.into_frame().unwrap();
    let mut expected_result = resp3_utils::new_set(None);
    expected_result.insert(Frame::Number {
      data: 1,
      attributes: None,
    });
    expected_result.insert(Frame::Number {
      data: 2,
      attributes: None,
    });
    expected_result.insert(Frame::Number {
      data: 3,
      attributes: None,
    });

    let expected = Frame::Set {
      data: expected_result,
      attributes: None,
    };

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_streaming_map() {
    let mut bytes: BytesMut = "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n".into();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Map)));
    assert_eq!(amt, 4);
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::SimpleString {
        data: "a".into(),
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 1.into(),
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::SimpleString {
        data: "b".into(),
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(Frame::Number {
        data: 2.into(),
        attributes: None
      })
    );
    assert_eq!(amt, 4);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt, _) = stream_decode_mut(&mut bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(Frame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.into_frame().unwrap();
    let mut expected_result = resp3_utils::new_map(None);
    expected_result.insert(
      Frame::SimpleString {
        data: "a".into(),
        attributes: None,
      },
      Frame::Number {
        data: 1,
        attributes: None,
      },
    );
    expected_result.insert(
      Frame::SimpleString {
        data: "b".into(),
        attributes: None,
      },
      Frame::Number {
        data: 2,
        attributes: None,
      },
    );
    let expected = Frame::Map {
      data: expected_result,
      attributes: None,
    };

    assert_eq!(actual, expected);
  }
}
