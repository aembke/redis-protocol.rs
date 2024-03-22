use crate::{
  error::*,
  resp3::{types::*, utils as resp3_utils},
  types::{DResult, _Range, CRLF},
  utils,
};
use core::{fmt::Debug, str, str::FromStr};
use nom::{
  bytes::streaming::{take as nom_take, take_until as nom_take_until},
  combinator::{map as nom_map, map_res as nom_map_res, opt as nom_opt},
  multi::count as nom_count,
  number::streaming::be_u8,
  sequence::terminated as nom_terminated,
  AsBytes,
  Err as NomErr,
};

#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};

fn map_complete_frame(frame: RangeFrame) -> DecodedRangeFrame {
  DecodedRangeFrame::Complete(frame)
}

fn expect_complete_index_frame<T>(frame: DecodedRangeFrame) -> Result<RangeFrame, RedisParseError<T>> {
  frame
    .into_complete_frame()
    .map_err(|e| RedisParseError::new_custom("expect_complete_frame", format!("{:?}", e)))
}

fn parse_as<V, T>(s: &[u8]) -> Result<V, RedisParseError<T>>
where
  V: FromStr,
  V::Err: Debug,
{
  str::from_utf8(s)?
    .parse::<V>()
    .map_err(|e| RedisParseError::new_custom("parse_as", format!("{:?}", e)))
}

fn to_isize<T>(s: &[u8]) -> Result<isize, RedisParseError<T>> {
  let s = str::from_utf8(s)?;

  if s == "?" {
    Ok(-1)
  } else {
    s.parse::<isize>()
      .map_err(|e| RedisParseError::new_custom("to_isize", format!("{:?}", e)))
  }
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

fn to_map<T>(mut data: Vec<RangeFrame>) -> Result<FrameMap<RangeFrame, RangeFrame>, RedisParseError<T>> {
  if data.len() % 2 != 0 {
    return Err(RedisParseError::new_custom("to_map", "Invalid hashmap frame length."));
  }

  let mut out = resp3_utils::new_map(data.len() / 2);
  while data.len() >= 2 {
    let value = data.pop().unwrap();
    let key = data.pop().unwrap();

    out.insert(key, value);
  }

  Ok(out)
}

fn to_set<T>(data: Vec<RangeFrame>) -> Result<FrameSet<RangeFrame>, RedisParseError<T>> {
  Ok(data.into_iter().collect())
}

fn attach_attributes<T>(
  attributes: RangeAttributes,
  mut frame: DecodedRangeFrame,
) -> Result<DecodedRangeFrame, RedisParseError<T>> {
  if let Err(e) = frame.add_attributes(attributes) {
    Err(RedisParseError::new_custom("attach_attributes", format!("{:?}", e)))
  } else {
    Ok(frame)
  }
}

fn d_read_to_crlf(input: (&[u8], usize)) -> DResult<usize> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", input);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data.len()))
}

fn d_read_to_crlf_take(input: (&[u8], usize)) -> DResult<&[u8]> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
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
  Ok(((input, offset), etry!(to_isize(data))))
}

fn d_frame_type(input: (&[u8], usize)) -> DResult<FrameKind> {
  let (input_bytes, byte) = be_u8(input.0)?;
  let kind = match FrameKind::from_byte(byte) {
    Some(k) => k,
    None => e!(RedisParseError::new_custom("frame_type", "Invalid frame type prefix.")),
  };
  decode_log_str!(
    input_bytes,
    _input,
    "Parsed frame type {:?}, remaining: {:?}",
    kind,
    _input
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
  let parsed = etry!(parse_as::<i64, _>(data));
  Ok(((input, next_offset), RangeFrame::Number {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_double(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(parse_as::<f64, _>(data));
  Ok(((input, next_offset), RangeFrame::Double {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_boolean(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_bool(data));
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
  let format = etry!(to_verbatimstring_format(format_bytes));
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
    nom_map_res(d_parse_frame_or_attribute, expect_complete_index_frame::<&[u8]>),
    len,
  )(input)
}

fn d_parse_kv_pairs(input: (&[u8], usize), len: usize) -> DResult<FrameMap<RangeFrame, RangeFrame>> {
  nom_map_res(
    nom_count(
      nom_map_res(d_parse_frame_or_attribute, expect_complete_index_frame::<&[u8]>),
      len * 2,
    ),
    to_map::<&[u8]>,
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

/// Decoding functions for complete RESP3 frames.
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
    let (frame, amt) = match decode_range(buf)? {
      Some(result) => result,
      None => return Ok(None),
    };

    resp3_utils::build_owned_frame(buf, &frame).map(|f| Some((f, amt)))
  }

  /// Attempt to decode the provided buffer without moving or copying the inner buffer contents.
  ///
  /// The returned frame(s) will hold owned views into the original buffer via [slice](bytes::Bytes::slice).
  ///
  /// Unlike [decode_bytes_mut], this function will not modify the input buffer.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn decode_bytes(buf: &Bytes) -> Result<Option<(BytesFrame, usize)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(buf)? {
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
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn decode_bytes_mut(buf: &mut BytesMut) -> Result<Option<(BytesFrame, usize, Bytes)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(buf)? {
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
    Ok(match decode_range(buf)? {
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
  /// Unlike [decode_bytes_mut], this function will not modify the input buffer.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn decode_bytes(buf: &Bytes) -> Result<Option<(DecodedFrame<BytesFrame>, usize)>, RedisProtocolError> {
    Ok(match decode_range(buf)? {
      Some((DecodedRangeFrame::Complete(frame), amt)) => Some((
        DecodedFrame::Complete(resp3_utils::build_bytes_frame(buf, &frame)?),
        amt,
      )),
      Some((DecodedRangeFrame::Streaming(frame), amt)) => Some((
        DecodedFrame::Streaming(resp3_utils::build_bytes_streaming_frame(buf, &frame)?),
        amt,
      )),
      _ => None,
    })
  }

  /// Attempt to decode and [split](bytes::BytesMut::split_to) the provided buffer without moving or copying the inner
  /// buffer contents.
  ///
  /// The returned frame(s) will hold owned views into the original buffer.
  ///
  /// This function is designed to work best with a [codec](tokio_util::codec) interface.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn decode_bytes_mut(
    buf: &mut BytesMut,
  ) -> Result<Option<(DecodedFrame<BytesFrame>, usize, Bytes)>, RedisProtocolError> {
    let (frame, amt) = match decode_range(buf)? {
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

#[cfg(test)]
#[cfg(feature = "bytes")]
pub mod tests {
  use super::*;
  use crate::resp3::decode::{complete::decode, streaming::decode_bytes as stream_decode};
  use bytes::{Bytes, BytesMut};
  use std::str;

  pub const PADDING: &'static str = "FOOBARBAZ";

  pub fn pretty_print_panic(e: RedisProtocolError) {
    panic!("{:?}", e);
  }

  pub fn panic_no_decode() {
    panic!("Failed to decode bytes. None returned.")
  }

  fn decode_and_verify_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let (frame, len) = match complete::decode_bytes(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    bytes.extend_from_slice(PADDING.as_bytes());
    let bytes = bytes.freeze();

    let (frame, len) = match complete::decode_bytes(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &Bytes) {
    let (frame, len) = match complete::decode_bytes(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => return pretty_print_panic(e),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  // ----------------------- tests adapted from RESP2 ------------------------

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (
      Some(BytesFrame::Number {
        data:       48293,
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
      Some(BytesFrame::SimpleString {
        data:       "string".into(),
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
      Some(BytesFrame::SimpleString {
        data:       "string".into(),
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
      Some(BytesFrame::BlobString {
        data:       "foo".into(),
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
      Some(BytesFrame::BlobString {
        data:       "foo".into(),
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
      Some(BytesFrame::Array {
        data:       vec![
          BytesFrame::SimpleString {
            data:       "Foo".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "Bar".into(),
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
      Some(BytesFrame::Array {
        data:       vec![
          BytesFrame::BlobString {
            data:       "Foo".into(),
            attributes: None,
          },
          BytesFrame::Null,
          BytesFrame::BlobString {
            data:       "Bar".into(),
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
      Some(BytesFrame::SimpleError {
        data:       "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
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
      Some(BytesFrame::SimpleError {
        data:       "MOVED 3999 127.0.0.1:6381".into(),
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
      Some(BytesFrame::SimpleError {
        data:       "ASK 3999 127.0.0.1:6381".into(),
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
    let bytes: Bytes = "foobarbazwibblewobble".into();
    let _ = complete::decode(&bytes).map_err(|e| pretty_print_panic(e));
  }

  // ----------------- end tests adapted from RESP2 ------------------------

  #[test]
  fn should_decode_blob_error() {
    let expected = (
      Some(BytesFrame::BlobError {
        data:       "foo".into(),
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
      Some(BytesFrame::BlobError {
        data:       "foo".into(),
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
      Some(BytesFrame::SimpleError {
        data:       "string".into(),
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
      Some(BytesFrame::SimpleError {
        data:       "string".into(),
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
      Some(BytesFrame::Boolean {
        data:       true,
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
      Some(BytesFrame::Boolean {
        data:       false,
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
      Some(BytesFrame::Number {
        data:       42,
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
      Some(BytesFrame::Double {
        data:       f64::INFINITY,
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
      Some(BytesFrame::Double {
        data:       f64::NEG_INFINITY,
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
      Some(BytesFrame::Double {
        data:       f64::NAN,
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
      Some(BytesFrame::Double {
        data:       4.59193,
        attributes: None,
      }),
      10,
    );
    let bytes: Bytes = ",4.59193\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let expected = (
      Some(BytesFrame::Double {
        data:       4_f64,
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
      Some(BytesFrame::BigNumber {
        data:       "3492890328409238509324850943850943825024385".into(),
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
    let expected = (Some(BytesFrame::Null), 3);
    let bytes: Bytes = "_\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_verbatim_string_mkd() {
    let expected = (
      Some(BytesFrame::VerbatimString {
        data:       "Some string".into(),
        format:     VerbatimStringFormat::Markdown,
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
      Some(BytesFrame::VerbatimString {
        data:       "Some string".into(),
        format:     VerbatimStringFormat::Text,
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
    let k1 = BytesFrame::SimpleString {
      data:       "first".into(),
      attributes: None,
    };
    let v1 = BytesFrame::Number {
      data:       1,
      attributes: None,
    };
    let k2 = BytesFrame::BlobString {
      data:       "second".into(),
      attributes: None,
    };
    let v2 = BytesFrame::Double {
      data:       4.2,
      attributes: None,
    };

    let mut expected_map = resp3_utils::new_map(0);
    expected_map.insert(k1, v1);
    expected_map.insert(k2, v2);
    let expected = (
      Some(BytesFrame::Map {
        data:       expected_map,
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
    let k1 = BytesFrame::SimpleString {
      data:       "first".into(),
      attributes: None,
    };
    let v1 = BytesFrame::Number {
      data:       1,
      attributes: None,
    };
    let k2 = BytesFrame::Number {
      data:       2,
      attributes: None,
    };
    let v2 = BytesFrame::Null;
    let k3 = BytesFrame::BlobString {
      data:       "second".into(),
      attributes: None,
    };
    let v3 = BytesFrame::Double {
      data:       4.2,
      attributes: None,
    };

    let mut expected_map = resp3_utils::new_map(0);
    expected_map.insert(k1, v1);
    expected_map.insert(k2, v2);
    expected_map.insert(k3, v3);
    let expected = (
      Some(BytesFrame::Map {
        data:       expected_map,
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
    let mut expected_set = resp3_utils::new_set(0);
    expected_set.insert(BytesFrame::Number {
      data:       1,
      attributes: None,
    });
    expected_set.insert(BytesFrame::SimpleString {
      data:       "2".into(),
      attributes: None,
    });
    expected_set.insert(BytesFrame::BlobString {
      data:       "foobar".into(),
      attributes: None,
    });
    expected_set.insert(BytesFrame::Double {
      data:       4.2,
      attributes: None,
    });
    let expected = (
      Some(BytesFrame::Set {
        data:       expected_set,
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
    let mut expected_set = resp3_utils::new_set(0);
    expected_set.insert(BytesFrame::Number {
      data:       1,
      attributes: None,
    });
    expected_set.insert(BytesFrame::SimpleString {
      data:       "2".into(),
      attributes: None,
    });
    expected_set.insert(BytesFrame::Null);
    expected_set.insert(BytesFrame::Double {
      data:       4.2,
      attributes: None,
    });
    let expected = (
      Some(BytesFrame::Set {
        data:       expected_set,
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
      Some(BytesFrame::Push {
        data:       vec![
          BytesFrame::SimpleString {
            data:       "pubsub".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "message".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "somechannel".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "this is the message".into(),
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

    let (frame, _) = decode(&bytes).unwrap().unwrap();
    assert!(frame.is_normal_pubsub_message());
  }

  #[test]
  fn should_decode_push_pattern_pubsub() {
    let expected = (
      Some(BytesFrame::Push {
        data:       vec![
          BytesFrame::SimpleString {
            data:       "pubsub".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "pmessage".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "pattern".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "somechannel".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "this is the message".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      70,
    );
    let bytes: Bytes = ">5\r\n+pubsub\r\n+pmessage\r\n+pattern\r\n+somechannel\r\n+this is the message\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let (frame, _) = decode(&bytes).unwrap().unwrap();
    assert!(frame.is_pattern_pubsub_message());
  }

  #[test]
  fn should_decode_keyevent_message() {
    let expected = (
      Some(BytesFrame::Push {
        data:       vec![
          BytesFrame::SimpleString {
            data:       "pubsub".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "pmessage".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "__key*".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "__keyevent@0__:set".into(),
            attributes: None,
          },
          BytesFrame::SimpleString {
            data:       "foo".into(),
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

    let (frame, _) = decode(&bytes).unwrap().unwrap();
    assert!(frame.is_pattern_pubsub_message());
  }

  #[test]
  fn should_parse_outer_attributes() {
    pretty_env_logger::init();

    let mut expected_inner_attrs = resp3_utils::new_map(0);
    expected_inner_attrs.insert(
      BytesFrame::BlobString {
        data:       "a".into(),
        attributes: None,
      },
      BytesFrame::Double {
        data:       0.1923,
        attributes: None,
      },
    );
    expected_inner_attrs.insert(
      BytesFrame::BlobString {
        data:       "b".into(),
        attributes: None,
      },
      BytesFrame::Double {
        data:       0.0012,
        attributes: None,
      },
    );
    let expected_inner_attrs = BytesFrame::Map {
      data:       expected_inner_attrs,
      attributes: None,
    };

    let mut expected_attrs = resp3_utils::new_map(0);
    expected_attrs.insert(
      BytesFrame::SimpleString {
        data:       "key-popularity".into(),
        attributes: None,
      },
      expected_inner_attrs,
    );

    let expected = (
      Some(BytesFrame::Array {
        data:       vec![
          BytesFrame::Number {
            data:       2039123,
            attributes: None,
          },
          BytesFrame::Number {
            data:       9543892,
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
    let mut expected_attrs = resp3_utils::new_map(0);
    expected_attrs.insert(
      BytesFrame::SimpleString {
        data:       "ttl".into(),
        attributes: None,
      },
      BytesFrame::Number {
        data:       3600,
        attributes: None,
      },
    );

    let expected = (
      Some(BytesFrame::Array {
        data:       vec![
          BytesFrame::Number {
            data:       1,
            attributes: None,
          },
          BytesFrame::Number {
            data:       2,
            attributes: None,
          },
          BytesFrame::Number {
            data:       3,
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
    let bytes: Bytes = ";0\r\n".into();
    let (frame, _) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::new_end_stream()))
  }

  #[test]
  fn should_decode_streaming_string() {
    let mut bytes: Bytes = "$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Streaming(StreamedFrame::new(FrameKind::BlobString))
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::ChunkedString("Hell".into())));
    assert_eq!(amt, 10);
    let _ = bytes.split_to(amt);

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::ChunkedString("o worl".into()))
    );
    assert_eq!(amt, 12);
    let _ = bytes.split_to(amt);

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::ChunkedString("d".into())));
    assert_eq!(amt, 7);
    let _ = bytes.split_to(amt);

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::new_end_stream()));
    assert_eq!(amt, 4);
  }

  #[test]
  fn should_decode_streaming_array() {
    let mut bytes: Bytes = "*?\r\n:1\r\n:2\r\n:3\r\n.\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Array)));
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       1,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       2,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       3,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.take().unwrap();
    let expected = BytesFrame::Array {
      data:       vec![
        BytesFrame::Number {
          data:       1,
          attributes: None,
        },
        BytesFrame::Number {
          data:       2,
          attributes: None,
        },
        BytesFrame::Number {
          data:       3,
          attributes: None,
        },
      ],
      attributes: None,
    };

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_streaming_set() {
    let mut bytes: Bytes = "~?\r\n:1\r\n:2\r\n:3\r\n.\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Set)));
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       1,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       2,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       3,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.take().unwrap();
    let mut expected_result = resp3_utils::new_set(0);
    expected_result.insert(BytesFrame::Number {
      data:       1,
      attributes: None,
    });
    expected_result.insert(BytesFrame::Number {
      data:       2,
      attributes: None,
    });
    expected_result.insert(BytesFrame::Number {
      data:       3,
      attributes: None,
    });

    let expected = BytesFrame::Set {
      data:       expected_result,
      attributes: None,
    };

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_streaming_map() {
    let mut bytes: Bytes = "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Map)));
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::SimpleString {
        data:       "a".into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       1.into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::SimpleString {
        data:       "b".into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(BytesFrame::Number {
        data:       2.into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    let _ = bytes.split_to(amt);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(BytesFrame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.take().unwrap();
    let mut expected_result = resp3_utils::new_map(0);
    expected_result.insert(
      BytesFrame::SimpleString {
        data:       "a".into(),
        attributes: None,
      },
      BytesFrame::Number {
        data:       1,
        attributes: None,
      },
    );
    expected_result.insert(
      BytesFrame::SimpleString {
        data:       "b".into(),
        attributes: None,
      },
      BytesFrame::Number {
        data:       2,
        attributes: None,
      },
    );
    let expected = BytesFrame::Map {
      data:       expected_result,
      attributes: None,
    };

    assert_eq!(actual, expected);
  }
}
