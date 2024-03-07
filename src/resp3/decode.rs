use crate::{
  error::*,
  resp3::{types::*, utils as resp3_utils},
  types::{DResult, CRLF},
  utils,
};
use core::{str, str::FromStr};
use nom::{
  bytes::streaming::{take as nom_take, take_until as nom_take_until},
  combinator::{map as nom_map, map_res as nom_map_res, opt as nom_opt},
  multi::count as nom_count,
  number::streaming::be_u8,
  sequence::terminated as nom_terminated,
  AsBytes,
  Err as NomErr,
};

#[cfg(feature = "zero-copy")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "zero-copy")]
use bytes_utils::Str;

use crate::types::_Range;
#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};
#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

fn map_complete_frame(frame: RangeFrame) -> DecodedRangeFrame {
  DecodedRangeFrame::Complete(frame)
}

fn expect_complete_index_frame(frame: DecodedRangeFrame) -> Result<RangeFrame, RedisParseError<&'_ [u8]>> {
  frame
    .into_complete_frame()
    .map_err(|e| RedisParseError::new_custom("expect_complete_frame", format!("{:?}", e)))
}

fn parse_as<V: FromStr, T>(s: &[u8]) -> Result<V, RedisParseError<T>> {
  str::from_utf8(s)?
    .parse::<V>()
    .map_err(|e| RedisParseError::new_custom("parse_as", format!("{:?}", e)))
}

fn to_bool<T>(s: &[u8]) -> Result<bool, RedisParseError<T>> {
  match str::from_utf8(s)? {
    "t" => Ok(true),
    "f" => Ok(false),
    _ => Err(RedisParseError::new_custom("to_bool", "Invalid boolean value.")),
  }
}

fn to_hello<T>(
  version: u8,
  username: Option<_Range>,
  password: Option<_Range>,
) -> Result<RangeFrame, RedisParseError<T>> {
  let version = match version {
    2 => RespVersion::RESP2,
    3 => RespVersion::RESP3,
    _ => {
      return Err(RedisParseError::new_custom("parse_hello", "Invalid RESP version."));
    },
  };

  Ok(RangeFrame::Hello {
    version,
    username,
    password,
  })
}

fn to_verbatimstring_format<T>(s: &[u8]) -> Result<VerbatimStringFormat, RedisParseError<T>> {
  match str::from_utf8(s)? {
    "txt" => Ok(VerbatimStringFormat::Text),
    "mkd" => Ok(VerbatimStringFormat::Markdown),
    _ => Err(RedisParseError::new_custom(
      "to_verbatimstring_format",
      "Invalid format.",
    )),
  }
}

fn to_map<'a>(mut data: Vec<RangeFrame>) -> Result<FrameMap<RangeFrame, RangeFrame>, RedisParseError<&'_ [u8]>> {
  if data.len() % 2 != 0 {
    return Err(RedisParseError::new_custom("to_map", "Invalid hashmap frame length."));
  }

  let mut out = resp3_utils::new_map(Some(data.len() / 2));
  while data.len() >= 2 {
    let value = data.pop().unwrap();
    let key = data.pop().unwrap();

    out.insert(key, value);
  }

  Ok(out)
}

fn to_set(data: Vec<RangeFrame>) -> Result<HashSet<RangeFrame>, RedisParseError<&'_ [u8]>> {
  let mut out = resp3_utils::new_set(Some(data.len()));
  for frame in data.into_iter() {
    out.insert(frame);
  }
  Ok(out)
}

fn attach_attributes(
  attributes: RangeAttributes,
  mut frame: DecodedRangeFrame,
) -> Result<DecodedRangeFrame, RedisParseError<&'_ [u8]>> {
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
  Ok(((input, offset), etry!(parse_as::<usize, _>(data))))
}

fn d_read_prefix_len_signed(input: (&[u8], usize)) -> DResult<isize> {
  let ((input, offset), data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok(((input, offset), etry!(parse_as::<isize, _>(&data))))
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

fn d_parse_simplestring(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::SimpleString {
    data:       (offset, offset + len),
    attributes: None,
  }))
}

fn d_parse_simpleerror(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::SimpleError {
    data:       (offset, offset + len),
    attributes: None,
  }))
}

fn d_parse_number(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(parse_as::<i64, _>(&data));
  Ok(((input, next_offset), RangeFrame::Number {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_double(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(parse_as::<f64, _>(&data));
  Ok(((input, next_offset), RangeFrame::Double {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_boolean(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_bool(&data));
  Ok(((input, next_offset), RangeFrame::Boolean {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_null(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), _) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::Null))
}

fn d_parse_blobstring(input: (&[u8], usize), len: usize) -> DResult<RangeFrame> {
  let offset = input.1;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input.0)?;

  Ok(((input, offset + len + 2), RangeFrame::BlobString {
    data:       (offset, offset + data.len()),
    attributes: None,
  }))
}

fn d_parse_bloberror(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input)?;

  Ok(((input, offset + len + 2), RangeFrame::BlobError {
    data:       (offset, offset + data.len()),
    attributes: None,
  }))
}

fn d_parse_verbatimstring(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, prefix_offset), len) = d_read_prefix_len(input)?;
  let (input, format_bytes) = nom_terminated(nom_take(3_usize), nom_take(1_usize))(input)?;
  let format = etry!(to_verbatimstring_format(&format_bytes));
  let (input, _) = nom_terminated(nom_take(len - 4), nom_take(2_usize))(input)?;

  Ok(((input, prefix_offset + len + 2), RangeFrame::VerbatimString {
    data: (prefix_offset + 4, prefix_offset + len),
    format,
    attributes: None,
  }))
}

fn d_parse_bignumber(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;

  Ok(((input, next_offset), RangeFrame::BigNumber {
    data:       (offset, offset + len),
    attributes: None,
  }))
}

fn d_parse_array_frames(input: (&[u8], usize), len: usize) -> DResult<Vec<RangeFrame>> {
  nom_count(
    nom_map_res(d_parse_frame_or_attribute, expect_complete_index_frame),
    len,
  )(input)
}

fn d_parse_kv_pairs(input: (&[u8], usize), len: usize) -> DResult<FrameMap<RangeFrame, RangeFrame>> {
  nom_map_res(
    nom_count(
      nom_map_res(d_parse_frame_or_attribute, expect_complete_index_frame),
      len * 2,
    ),
    to_map,
  )(input)
}

fn d_parse_array(input: (&[u8], usize), len: usize) -> DResult<RangeFrame> {
  let (input, data) = d_parse_array_frames(input, len)?;
  Ok((input, RangeFrame::Array { data, attributes: None }))
}

fn d_parse_push(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, data) = d_parse_array_frames(input, len)?;
  Ok((input, RangeFrame::Push { data, attributes: None }))
}

fn d_parse_set(input: (&[u8], usize), len: usize) -> DResult<RangeFrame> {
  let (input, frames) = d_parse_array_frames(input, len)?;

  Ok((input, RangeFrame::Set {
    data:       etry!(to_set(frames)),
    attributes: None,
  }))
}

fn d_parse_map(input: (&[u8], usize), len: usize) -> DResult<RangeFrame> {
  let (input, frames) = d_parse_kv_pairs(input, len)?;

  Ok((input, RangeFrame::Map {
    data:       frames,
    attributes: None,
  }))
}

fn d_parse_attribute(input: (&[u8], usize)) -> DResult<RangeAttributes> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, attributes) = d_parse_kv_pairs(input, len)?;
  Ok((input, attributes))
}

fn d_parse_hello(input: (&[u8], usize)) -> DResult<RangeFrame> {
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

  let (input, username, password) = if auth.is_some() {
    offset += AUTH.as_bytes().len() + 1;
    let username_offset = offset;

    let (input, username) = nom_terminated(nom_take_until(EMPTY_SPACE.as_bytes()), nom_take(1_usize))(input)?;
    let password_offset = username_offset + username.len() + 1;
    let (input, password) = nom_take_until(CRLF.as_bytes())(input)?;
    offset += username.as_bytes().len() + password.as_bytes().len() + 1 + 2;

    (
      input,
      Some((username_offset, username_offset + username.len())),
      Some((password_offset, password_offset + password.len())),
    )
  } else {
    (input, None, None)
  };

  Ok(((input, offset), etry!(to_hello(version, username, password))))
}

/// Check for a streaming variant of a frame, and if found then return the prefix bytes only, otherwise return the
/// complete frame.
///
/// Only supported for arrays, sets, maps, and blob strings.
fn d_check_streaming(input: (&[u8], usize), kind: FrameKind) -> DResult<DecodedRangeFrame> {
  let (input, len) = d_read_prefix_len_signed(input)?;
  let (input, frame) = if len == -1 {
    (input, DecodedRangeFrame::Streaming(StreamedRangeFrame::new(kind)))
  } else {
    let len = etry!(utils::isize_to_usize(len));
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

    (input, DecodedRangeFrame::Complete(frame))
  };

  Ok((input, frame))
}

fn d_parse_chunked_string(input: (&[u8], usize)) -> DResult<DecodedRangeFrame> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, frame) = if len == 0 {
    (input, RangeFrame::new_end_stream())
  } else {
    let offset = input.1;
    let (input, contents) = nom_terminated(nom_take(len), nom_take(2_usize))(input.0)?;

    (
      (input, offset + contents.len() + 2),
      RangeFrame::ChunkedString((offset, offset + len)),
    )
  };

  Ok((input, DecodedRangeFrame::Complete(frame)))
}

fn d_return_end_stream(input: (&[u8], usize)) -> DResult<DecodedRangeFrame> {
  let (input, _) = d_read_to_crlf(input)?;
  Ok((input, DecodedRangeFrame::Complete(RangeFrame::new_end_stream())))
}

fn d_parse_non_attribute_frame(input: (&[u8], usize), kind: FrameKind) -> DResult<DecodedRangeFrame> {
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
    },
  };

  Ok((input, frame))
}

fn d_parse_attribute_and_frame(input: (&[u8], usize)) -> DResult<DecodedRangeFrame> {
  let (input, attributes) = d_parse_attribute(input)?;
  let (input, kind) = d_frame_type(input)?;
  let (input, next_frame) = d_parse_non_attribute_frame(input, kind)?;
  let frame = etry!(attach_attributes(attributes, next_frame));

  Ok((input, frame))
}

fn d_parse_frame_or_attribute(input: (&[u8], usize)) -> DResult<DecodedRangeFrame> {
  let (input, kind) = d_frame_type(input)?;
  let (input, frame) = if kind == FrameKind::Attribute {
    d_parse_attribute_and_frame(input)?
  } else {
    d_parse_non_attribute_frame(input, kind)?
  };

  Ok((input, frame))
}

/// Parsing functions for complete RESP3 frames.
pub mod complete {
  use super::*;

  /// Attempt to the decode the contents of `buf`, returning frames that reference ranges into the provided buffer.
  ///
  /// This is the generic interface behind the zero-copy interface and can be used to implement zero-copy
  /// deserialization into other types.
  pub fn decode_range(buf: &[u8]) -> Result<Option<(RangeFrame, usize)>, RedisProtocolError> {
    let (offset, len) = (0, buf.len());

    let (frame, amt) = match d_parse_frame_or_attribute((&buf, offset)) {
      Ok(((_remaining, offset), frame)) => {
        #[cfg(feature = "std")]
        debug_assert_eq!(offset, len - _remaining.len(), "returned offset doesn't match");
        (frame, offset)
      },
      Err(NomErr::Incomplete(_)) => return Ok(None),
      Err(NomErr::Error(e)) => return Err(e.into()),
      Err(NomErr::Failure(e)) => return Err(e.into()),
    };

    Ok(Some((frame.into_complete_frame()?, amt)))
  }

  /// Attempt to decode the contents of `buf`, returning the first valid frame and the number of bytes consumed.
  ///
  /// If the buffer contains an incomplete frame then `None` is returned.
  pub fn decode(buf: &[u8]) -> Result<Option<(OwnedFrame, usize)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(&buf)? {
      Some(result) => result,
      None => return Ok(None),
    };

    resp3_utils::build_owned_frame(buf, &frame).map(|f| Some((f, amt)))
  }

  /// Attempt to decode the provided buffer without moving or copying the inner buffer contents.
  ///
  /// The returned frame(s) will hold owned views into the original buffer via [slice](bytes::Bytes::slice).
  ///
  /// Unlike [decode_bytes_mut](decode_bytes_mut), this function will not modify the input buffer.
  #[cfg(feature = "zero-copy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zero-copy")))]
  pub fn decode_bytes(buf: &Bytes) -> Result<Option<(BytesFrame, usize)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(&buf)? {
      Some(result) => result,
      None => return Ok(None),
    };

    resp3_utils::build_bytes_frame(buf, &frame).map(|f| Some((f, amt)))
  }

  /// Attempt to decode and [split](bytes::BytesMut::split_to) the provided buffer without moving or copying the inner
  /// buffer contents.
  ///
  /// The returned frame(s) will hold owned views into the original buffer.
  ///
  /// This function is designed to work best with a [codec](tokio_util::codec) interface.
  #[cfg(feature = "zero-copy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zero-copy")))]
  pub fn decode_bytes_mut(buf: &mut BytesMut) -> Result<Option<(BytesFrame, usize, Bytes)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(&buf)? {
      Some(result) => result,
      None => return Ok(None),
    };

    resp3_utils::freeze_parse(buf, &frame, amt).map(|(f, b)| Some((f, amt, b)))
  }
}

/// Decoding functions that support streaming and complete frames.
pub mod streaming {
  use super::*;

  /// Attempt to the decode the contents of `buf`, returning frames that reference ranges into the provided buffer.
  ///
  /// This is the generic interface behind the zero-copy interface and can be used to implement zero-copy
  /// deserialization into other types.
  pub fn decode_range(buf: &[u8]) -> Result<Option<(DecodedRangeFrame, usize)>, RedisProtocolError> {
    let (offset, len) = (0, buf.len());

    match d_parse_frame_or_attribute((&buf, offset)) {
      Ok(((_remaining, offset), frame)) => {
        #[cfg(feature = "std")]
        debug_assert_eq!(offset, len - _remaining.len(), "returned offset doesn't match");
        Ok(Some((frame, offset)))
      },
      Err(NomErr::Incomplete(_)) => Ok(None),
      Err(NomErr::Error(e)) => Err(e.into()),
      Err(NomErr::Failure(e)) => Err(e.into()),
    }
  }

  /// Attempt to decode the contents of `buf`, returning the first valid frame and the number of bytes consumed.
  ///
  /// If the buffer contains an incomplete frame then `None` is returned.
  pub fn decode(buf: &[u8]) -> Result<Option<(DecodedFrame<OwnedFrame>, usize)>, RedisProtocolError> {
    Ok(match decode_range(&buf)? {
      Some((DecodedRangeFrame::Complete(frame), amt)) => Some((
        DecodedFrame::Complete(resp3_utils::build_owned_frame(buf, &frame)?),
        amt,
      )),
      Some((DecodedRangeFrame::Streaming(frame), amt)) => Some((
        DecodedFrame::Streaming(resp3_utils::build_owned_streaming_frame(buf, &frame)?),
        amt,
      )),
      None => None,
    })
  }

  /// Attempt to decode the provided buffer without moving or copying the inner buffer contents.
  ///
  /// The returned frame(s) will hold owned views into the original buffer via [slice](bytes::Bytes::slice).
  ///
  /// Unlike [decode_bytes_mut](decode_bytes_mut), this function will not modify the input buffer.
  #[cfg(feature = "zero-copy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zero-copy")))]
  pub fn decode_bytes(buf: &Bytes) -> Result<Option<(DecodedFrame<BytesFrame>, usize)>, RedisProtocolError> {
    Ok(match decode_range(&buf)? {
      Some((DecodedRangeFrame::Complete(frame), amt)) => Some((
        DecodedFrame::Complete(resp3_utils::build_bytes_frame(buf, &frame)?),
        amt,
      )),
      Some((DecodedRangeFrame::Streaming(frame), amt)) => Some((
        DecodedFrame::Streaming(resp3_utils::build_bytes_streaming_frame(buf, &frame)?),
        amt,
      )),
      None => None,
    })
  }

  /// Attempt to decode and [split](bytes::BytesMut::split_to) the provided buffer without moving or copying the inner
  /// buffer contents.
  ///
  /// The returned frame(s) will hold owned views into the original buffer.
  ///
  /// This function is designed to work best with a [codec](tokio_util::codec) interface.
  #[cfg(feature = "zero-copy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zero-copy")))]
  pub fn decode_bytes_mut(
    buf: &mut BytesMut,
  ) -> Result<Option<(DecodedFrame<BytesFrame>, usize, Bytes)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(&buf)? {
      Some(result) => result,
      None => return Ok(None),
    };
    let buf = buf.split_to(amt).freeze();

    Ok(match frame {
      DecodedRangeFrame::Complete(frame) => Some((
        DecodedFrame::Complete(resp3_utils::build_bytes_frame(&buf, &frame)?),
        amt,
        buf,
      )),
      DecodedRangeFrame::Streaming(frame) => Some((
        DecodedFrame::Streaming(resp3_utils::build_bytes_streaming_frame(&buf, &frame)?),
        amt,
        buf,
      )),
    })
  }
}
