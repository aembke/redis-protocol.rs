use crate::{
  error::*,
  resp3::{types::*, utils as resp3_utils},
  types::{DResult, Scan, MAX_DECODE_DEPTH, MAX_INITIAL_CAPACITY_BYTES},
  utils,
};
use alloc::{boxed::Box, format, vec::Vec};
use core::{fmt::Debug, mem::size_of, str, str::FromStr};
use nom::{
  bytes::streaming::{tag as nom_tag, take as nom_take},
  combinator::map as nom_map,
  number::streaming::be_u8 as nom_be_u8,
  sequence::terminated as nom_terminated,
  Err as NomErr,
  Parser,
};

#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};

fn map_complete_frame(frame: RangeFrame) -> DecodedRangeFrame {
  DecodedRangeFrame::Complete(frame)
}

fn expect_complete_index_frame<T>(frame: DecodedRangeFrame) -> Result<RangeFrame, RedisParseError<T>> {
  frame
    .into_complete_frame()
    .map_err(|e| RedisParseError::new_custom("expect_complete_frame", format!("{e:?}")))
}

fn parse_as<V, T>(s: &[u8]) -> Result<V, RedisParseError<T>>
where
  V: FromStr,
  V::Err: Debug,
{
  str::from_utf8(s)?
    .parse::<V>()
    .map_err(|e| RedisParseError::new_custom("parse_as", format!("{e:?}")))
}

/// The length prefix of a blob string or aggregate frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrefixLen {
  /// `?`, the header of a streamed frame.
  Streamed,
  /// `-1`, which RESP2 uses for null bulk strings and arrays.
  Null,
  Len(usize),
}

fn to_prefix_len<T>(s: &[u8]) -> Result<PrefixLen, RedisParseError<T>> {
  if s == b"?" {
    return Ok(PrefixLen::Streamed);
  }
  match utils::parse_isize(s) {
    Some(-1) => Ok(PrefixLen::Null),
    Some(len) if len >= 0 => Ok(PrefixLen::Len(len as usize)),
    _ => Err(RedisParseError::new_custom("to_prefix_len", "Invalid length prefix.")),
  }
}

fn to_usize<T>(s: &[u8]) -> Result<usize, RedisParseError<T>> {
  utils::parse_usize(s).ok_or_else(|| RedisParseError::new_custom("to_usize", "Invalid length."))
}

fn to_i64<T>(s: &[u8]) -> Result<i64, RedisParseError<T>> {
  utils::parse_i64(s).ok_or_else(|| RedisParseError::new_custom("to_i64", "Invalid integer."))
}
fn to_bool<T>(s: &[u8]) -> Result<bool, RedisParseError<T>> {
  match str::from_utf8(s)? {
    "t" => Ok(true),
    "f" => Ok(false),
    _ => Err(RedisParseError::new_custom("to_bool", "Invalid boolean value.")),
  }
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

fn to_map<T>(data: Vec<RangeFrame>) -> Result<Vec<(RangeFrame, RangeFrame)>, RedisParseError<T>> {
  if data.len() % 2 != 0 {
    return Err(RedisParseError::new_custom("to_map", "Invalid hashmap frame length."));
  }

  // pairs stay in wire order. hashing the ranges wouldn't deduplicate anything, so that happens when building the
  // final frame.
  let mut out = Vec::with_capacity(data.len() / 2);
  let mut data = data.into_iter();
  while let (Some(key), Some(value)) = (data.next(), data.next()) {
    if key.kind().can_hash() {
      out.push((key, value));
    } else {
      return Err(RedisParseError::new_custom("to_map", "Invalid map key."));
    }
  }

  Ok(out)
}

fn to_set<T>(data: Vec<RangeFrame>) -> Result<Vec<RangeFrame>, RedisParseError<T>> {
  if data.iter().all(|value| value.kind().can_hash()) {
    Ok(data)
  } else {
    Err(RedisParseError::new_custom("to_set", "Invalid hash key."))
  }
}

fn attach_attributes<T>(
  attributes: RangeAttributes,
  mut frame: DecodedRangeFrame,
) -> Result<DecodedRangeFrame, RedisParseError<T>> {
  if let Err(e) = frame.add_attributes(attributes) {
    Err(RedisParseError::new_custom("attach_attributes", format!("{e:?}")))
  } else {
    Ok(frame)
  }
}

fn d_read_to_crlf(input: (&[u8], usize)) -> DResult<'_, usize> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", input);
  let (data, input_bytes) = utils::find_crlf(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data.len()))
}

fn d_read_to_crlf_take(input: (&[u8], usize)) -> DResult<'_, &[u8]> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (data, input_bytes) = utils::find_crlf(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data))
}

fn d_read_prefix_len(input: (&[u8], usize)) -> DResult<'_, usize> {
  let ((input, offset), data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok(((input, offset), etry!(to_usize(data))))
}

/// Read a length prefix that may also be `?` or `-1`.
fn d_read_prefix_len_or_marker(input: (&[u8], usize)) -> DResult<'_, PrefixLen> {
  let ((input, offset), data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok(((input, offset), etry!(to_prefix_len(data))))
}

fn d_frame_type(input: (&[u8], usize)) -> DResult<'_, FrameKind> {
  let (input_bytes, byte) = nom_be_u8(input.0)?;

  match FrameKind::from_byte(byte) {
    Some(kind) => {
      decode_log_str!(
        input_bytes,
        _input,
        "Parsed frame type {:?}, remaining: {:?}",
        kind,
        _input
      );

      Ok(((input_bytes, input.1 + 1), kind))
    },
    None => {
      if byte == b'H' {
        // return the original bytes w/o consuming anything
        Ok((input, FrameKind::Hello))
      } else {
        e!(RedisParseError::new_custom("frame_type", "Invalid frame type prefix."))
      }
    },
  }
}

fn d_parse_simplestring(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::SimpleString {
    data:       (offset, offset + len),
    attributes: None,
  }))
}

fn d_parse_simpleerror(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::SimpleError {
    data:       (offset, offset + len),
    attributes: None,
  }))
}

fn d_parse_number(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_i64(data));
  Ok(((input, next_offset), RangeFrame::Number {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_double(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(parse_as::<f64, _>(data));
  Ok(((input, next_offset), RangeFrame::Double {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_boolean(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_bool(data));
  Ok(((input, next_offset), RangeFrame::Boolean {
    data:       parsed,
    attributes: None,
  }))
}

fn d_parse_null(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, next_offset), _) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::Null))
}

fn d_parse_blobstring(input: (&[u8], usize), len: usize) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let (input, data) = utils::take_blob(input.0, len)?;

  Ok(((input, offset + len + 2), RangeFrame::BlobString {
    data:       (offset, offset + data.len()),
    attributes: None,
  }))
}

fn d_parse_bloberror(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  let (input, data) = utils::take_blob(input, len)?;

  Ok(((input, offset + len + 2), RangeFrame::BlobError {
    data:       (offset, offset + data.len()),
    attributes: None,
  }))
}

fn d_parse_verbatimstring(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, prefix_offset), len) = d_read_prefix_len(input)?;
  if len < 4 {
    // the contents start with the format and a colon, such as `txt:`
    e!(RedisParseError::new_custom(
      "parse_verbatimstring",
      "Invalid prefix length."
    ));
  }
  let (input, format_bytes) = nom_terminated(nom_take(3_usize), nom_tag(&b":"[..])).parse(input)?;
  let (input, _) = utils::take_blob(input, len - 4)?;

  Ok((
    (input, prefix_offset.saturating_add(len).saturating_add(2)),
    RangeFrame::VerbatimString {
      data:       (prefix_offset + 4, prefix_offset.saturating_add(len)),
      format:     etry!(to_verbatimstring_format(format_bytes)),
      attributes: None,
    },
  ))
}

fn d_parse_bignumber(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;

  Ok(((input, next_offset), RangeFrame::BigNumber {
    data:       (offset, offset + len),
    attributes: None,
  }))
}

/// Parse `len` complete frames.
///
/// This is a manual loop rather than `nom::multi::count` in order to keep the stack frames on the recursive parsing
/// path small.
fn d_parse_array_frames(input: (&[u8], usize), len: usize, depth: usize) -> DResult<'_, Vec<RangeFrame>> {
  // mirror nom's `count`, which caps the initial allocation to avoid trusting the length prefix
  let mut frames = Vec::with_capacity(len.min(MAX_INITIAL_CAPACITY_BYTES / size_of::<RangeFrame>()));
  let mut input = input;
  for _ in 0 .. len {
    let (next, frame) = d_parse_frame_or_attribute(input, depth + 1)?;
    let frame = etry!(expect_complete_index_frame::<&[u8]>(frame));
    if let RangeFrame::ChunkedString(_) = frame {
      // chunks and stream terminators only appear at the top level, between the frames of a streamed frame
      e!(RedisParseError::new_custom(
        "parse_array_frames",
        "Unexpected chunked string."
      ));
    }
    frames.push(frame);
    input = next;
  }

  Ok((input, frames))
}

fn d_parse_kv_pairs(input: (&[u8], usize), len: usize, depth: usize) -> DResult<'_, Vec<(RangeFrame, RangeFrame)>> {
  let (input, frames) = d_parse_array_frames(input, len.saturating_mul(2), depth)?;
  Ok((input, etry!(to_map::<&[u8]>(frames))))
}

fn d_parse_attribute(input: (&[u8], usize), depth: usize) -> DResult<'_, RangeAttributes> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, attributes) = d_parse_kv_pairs(input, len, depth)?;
  Ok((input, attributes))
}

fn d_parse_hello(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  // HELLO [protover [AUTH username password] [SETNAME clientname]]\r\n
  // The HELLO definition seems somewhat undefined, nor does it use length prefixes or CRLF delimiters, so most of the
  // above logic isn't useful here. this is hacky and should be improved
  let start_offset = input.1;
  let ((input, offset), data) = d_read_to_crlf_take(input)?;
  let parts: Vec<_> = data.split(|b| *b == b' ').collect();

  if parts.len() < 2 || parts[0] != HELLO.as_bytes() {
    e!(RedisParseError::new_custom("parse_hello", "Invalid HELLO."));
  }
  let version = match etry!(str::from_utf8(parts[1])) {
    "2" => RespVersion::RESP2,
    "3" => RespVersion::RESP3,
    _ => e!(RedisParseError::new_custom("parse_hello", "Invalid RESP version")),
  };

  let (username, password, setname) = if parts.len() == 5 {
    // auth, no setname
    if parts[2] == AUTH.as_bytes() {
      let username_start = start_offset + HELLO.as_bytes().len() + 3 + AUTH.as_bytes().len() + 1;
      let username_end = username_start + parts[3].len();
      let password_start = username_end + 1;
      let password_end = password_start + parts[4].len();

      (
        Some((username_start, username_end)),
        Some((password_start, password_end)),
        None,
      )
    } else {
      e!(RedisParseError::new_custom("parse_hello", "Invalid AUTH"))
    }
  } else if parts.len() == 7 {
    // auth and setname
    if parts[2] == AUTH.as_bytes() && parts[5] == SETNAME.as_bytes() {
      let username_start = start_offset + HELLO.as_bytes().len() + 3 + AUTH.as_bytes().len() + 1;
      let username_end = username_start + parts[3].len();
      let password_start = username_end + 1;
      let password_end = password_start + parts[4].len();
      let name_start = password_end + 1 + SETNAME.as_bytes().len() + 1;
      let name_end = name_start + parts[6].len();

      (
        Some((username_start, username_end)),
        Some((password_start, password_end)),
        Some((name_start, name_end)),
      )
    } else {
      e!(RedisParseError::new_custom("parse_hello", "Invalid AUTH or SETNAME"))
    }
  } else if parts.len() == 4 {
    // no auth, with setname
    if parts[2] == SETNAME.as_bytes() {
      let name_start = start_offset + HELLO.as_bytes().len() + 3 + SETNAME.as_bytes().len() + 1;
      let name_end = name_start + parts[3].len();

      (None, None, Some((name_start, name_end)))
    } else {
      e!(RedisParseError::new_custom("parse_hello", "Invalid SETNAME"))
    }
  } else if parts.len() == 2 {
    (None, None, None)
  } else {
    e!(RedisParseError::new_custom("parse_hello", "Invalid HELLO arguments"))
  };

  Ok(((input, offset), RangeFrame::Hello {
    version,
    auth: username.zip(password).map(Box::new),
    setname,
  }))
}

/// Parse a blob string, or the prefix bytes of a streamed blob string.
fn d_parse_blobstring_or_streaming(input: (&[u8], usize)) -> DResult<'_, DecodedRangeFrame> {
  match d_read_prefix_len_or_marker(input)? {
    (input, PrefixLen::Streamed) => Ok((
      input,
      DecodedRangeFrame::Streaming(StreamedRangeFrame::new(FrameKind::BlobString)),
    )),
    // RESP3 servers send `_` instead, but RESP2 servers and proxies may send `$-1`
    (input, PrefixLen::Null) => Ok((input, DecodedRangeFrame::Complete(RangeFrame::Null))),
    (input, PrefixLen::Len(len)) => {
      let (input, frame) = d_parse_blobstring(input, len)?;
      Ok((input, DecodedRangeFrame::Complete(frame)))
    },
  }
}

/// Parse an aggregate frame (array, set, map, or push), or the prefix bytes of a streamed aggregate frame.
///
/// This function is on the recursive parsing path, so it should use as little stack space as possible.
fn d_parse_aggregate(input: (&[u8], usize), kind: FrameKind, depth: usize) -> DResult<'_, DecodedRangeFrame> {
  let (input, len) = match d_read_prefix_len_or_marker(input)? {
    (input, PrefixLen::Streamed) if kind != FrameKind::Push => {
      return Ok((input, DecodedRangeFrame::Streaming(StreamedRangeFrame::new(kind))));
    },
    (_, PrefixLen::Streamed) => e!(RedisParseError::new_custom(
      "parse_aggregate",
      "Push frames cannot be streamed."
    )),
    // RESP3 servers send `_` instead, but RESP2 servers and proxies may send `*-1`
    (input, PrefixLen::Null) => return Ok((input, DecodedRangeFrame::Complete(RangeFrame::Null))),
    (input, PrefixLen::Len(len)) => (input, len),
  };
  let len = if kind == FrameKind::Map {
    len.saturating_mul(2)
  } else {
    len
  };

  let (input, data) = d_parse_array_frames(input, len, depth)?;
  let frame = match kind {
    FrameKind::Array => RangeFrame::Array { data, attributes: None },
    FrameKind::Push => RangeFrame::Push { data, attributes: None },
    FrameKind::Set => RangeFrame::Set {
      data:       etry!(to_set(data)),
      attributes: None,
    },
    FrameKind::Map => RangeFrame::Map {
      data:       etry!(to_map(data)),
      attributes: None,
    },
    _ => e!(RedisParseError::new_custom(
      "parse_aggregate",
      "Invalid aggregate frame type."
    )),
  };

  Ok((input, DecodedRangeFrame::Complete(frame)))
}

fn d_parse_chunked_string(input: (&[u8], usize)) -> DResult<'_, DecodedRangeFrame> {
  let (input, len) = d_read_prefix_len(input)?;
  let (input, frame) = if len == 0 {
    (input, RangeFrame::new_end_stream())
  } else {
    let offset = input.1;
    let (input, contents) = utils::take_blob(input.0, len)?;

    (
      (input, offset + contents.len() + 2),
      RangeFrame::ChunkedString((offset, offset + len)),
    )
  };

  Ok((input, DecodedRangeFrame::Complete(frame)))
}

fn d_return_end_stream(input: (&[u8], usize)) -> DResult<'_, DecodedRangeFrame> {
  let (input, _) = d_read_to_crlf(input)?;
  Ok((input, DecodedRangeFrame::Complete(RangeFrame::new_end_stream())))
}

fn d_parse_non_attribute_frame(
  input: (&[u8], usize),
  kind: FrameKind,
  depth: usize,
) -> DResult<'_, DecodedRangeFrame> {
  match kind {
    FrameKind::Array | FrameKind::Set | FrameKind::Map | FrameKind::Push => d_parse_aggregate(input, kind, depth),
    FrameKind::Attribute => e!(RedisParseError::new_custom(
      "parse_non_attribute_frame",
      "Unexpected attribute frame.",
    )),
    _ => d_parse_leaf_frame(input, kind),
  }
}

/// Parse a frame that cannot contain other frames.
///
/// This is kept out of line so that the stack frames on the recursive aggregate parsing path stay small.
#[inline(never)]
fn d_parse_leaf_frame(input: (&[u8], usize), kind: FrameKind) -> DResult<'_, DecodedRangeFrame> {
  let (input, frame) = match kind {
    FrameKind::BlobString => d_parse_blobstring_or_streaming(input)?,
    FrameKind::SimpleString => nom_map(d_parse_simplestring, map_complete_frame).parse(input)?,
    FrameKind::SimpleError => nom_map(d_parse_simpleerror, map_complete_frame).parse(input)?,
    FrameKind::Number => nom_map(d_parse_number, map_complete_frame).parse(input)?,
    FrameKind::Null => nom_map(d_parse_null, map_complete_frame).parse(input)?,
    FrameKind::Double => nom_map(d_parse_double, map_complete_frame).parse(input)?,
    FrameKind::Boolean => nom_map(d_parse_boolean, map_complete_frame).parse(input)?,
    FrameKind::BlobError => nom_map(d_parse_bloberror, map_complete_frame).parse(input)?,
    FrameKind::VerbatimString => nom_map(d_parse_verbatimstring, map_complete_frame).parse(input)?,
    FrameKind::BigNumber => nom_map(d_parse_bignumber, map_complete_frame).parse(input)?,
    FrameKind::Hello => nom_map(d_parse_hello, map_complete_frame).parse(input)?,
    FrameKind::ChunkedString => d_parse_chunked_string(input)?,
    FrameKind::EndStream => d_return_end_stream(input)?,
    _ => e!(RedisParseError::new_custom(
      "parse_leaf_frame",
      format!("Unexpected frame kind: {kind:?}")
    )),
  };

  Ok((input, frame))
}

#[inline(never)]
fn d_parse_attribute_and_frame(input: (&[u8], usize), depth: usize) -> DResult<'_, DecodedRangeFrame> {
  let (input, attributes) = d_parse_attribute(input, depth)?;
  let (input, kind) = d_frame_type(input)?;
  let (input, next_frame) = d_parse_non_attribute_frame(input, kind, depth)?;
  let frame = etry!(attach_attributes(attributes, next_frame));

  Ok((input, frame))
}

fn d_parse_frame_or_attribute(input: (&[u8], usize), depth: usize) -> DResult<'_, DecodedRangeFrame> {
  if depth > MAX_DECODE_DEPTH {
    e!(RedisParseError::new_custom(
      "parse_frame",
      "Max frame nesting depth exceeded."
    ));
  }

  let (input, kind) = d_frame_type(input)?;
  if kind == FrameKind::Attribute {
    d_parse_attribute_and_frame(input, depth)
  } else {
    d_parse_non_attribute_frame(input, kind, depth)
  }
}

/// Decoding functions for complete RESP3 frames.
pub mod complete {
  use super::*;

  /// Attempt to the decode the contents of `buf`, returning frames that reference ranges into the provided buffer.
  ///
  /// This is the generic interface behind the zero-copy interface and can be used to implement zero-copy
  /// deserialization into other types.
  pub fn decode_range(buf: &[u8]) -> Result<Option<(RangeFrame, usize)>, RedisProtocolError> {
    let (offset, _len) = (0, buf.len());

    let (frame, amt) = match d_parse_frame_or_attribute((buf, offset), 0) {
      Ok(((_remaining, offset), frame)) => {
        #[cfg(feature = "std")]
        debug_assert_eq!(offset, _len - _remaining.len(), "returned offset doesn't match");
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
    let (offset, _len) = (0, buf.len());

    match d_parse_frame_or_attribute((buf, offset), 0) {
      Ok(((_remaining, offset), frame)) => {
        #[cfg(feature = "std")]
        debug_assert_eq!(offset, _len - _remaining.len(), "returned offset doesn't match");
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

/// An aggregate frame that the scanner is inside of.
#[derive(Clone, Debug)]
struct OpenAggregate {
  kind:      FrameKind,
  /// The number of inner frames still expected. Maps and attributes count keys and values separately.
  remaining: usize,
  /// Whether a key that cannot be hashed was found, which the decoder rejects once the aggregate ends.
  bad_key:   bool,
}

/// An incremental scanner that finds the end of the next frame in a buffer that is filled over several reads.
///
/// The decoding functions in this module don't keep any state between calls. When the buffer holds only part of a
/// frame they return `Ok(None)`, and the next call parses the buffer again from the start. If a large frame arrives
/// over many small reads (TLS, for example, usually yields one record per read), the total decoding cost grows with
/// the square of the frame size.
///
/// A `FrameScanner` remembers how far it got. Each call to [scan](Self::scan) reads only the bytes added since the
/// previous call, and blob contents are skipped by their length prefix. Call a decoding function when the scanner
/// reports that the buffer contains a complete frame.
///
/// The scanner uses the same parsing logic as the [streaming] decoder, so the [Scan] value always matches what that
/// decoder would return for the same buffer. The [complete] decoder differs in one case: the scanner reports the
/// header of a streamed frame as a complete frame, which the complete decoder rejects. The scanner never copies or
/// stores the buffer contents.
///
/// ```rust
/// # use redis_protocol::{resp3::decode::{streaming::decode, FrameScanner}, types::Scan};
/// let mut scanner = FrameScanner::new();
/// let mut buf = b"%1\r\n+foo\r\n$3\r\nb".to_vec();
/// assert_eq!(scanner.scan(&buf), Scan::Incomplete);
///
/// // the next call only reads the new bytes
/// buf.extend_from_slice(b"ar\r\n");
/// assert_eq!(scanner.scan(&buf), Scan::Complete(buf.len()));
///
/// let (frame, amt) = decode(&buf).unwrap().unwrap();
/// assert_eq!(amt, 19);
/// ```
///
/// The scanner's offsets are relative to the start of the buffer, so the buffer must only grow between calls that
/// return [Scan::Incomplete]. The scanner resets itself when it returns [Scan::Complete] or [Scan::Invalid], at which
/// point the caller is expected to decode or discard the frame. Call [reset](Self::reset) if the buffer is cleared or
/// replaced in any other way.
#[derive(Clone, Debug, Default)]
pub struct FrameScanner {
  /// The offset of the next frame header to read.
  pos:             usize,
  /// The buffer length at the end of the last search for the CRLF that ends the next header.
  searched:        usize,
  /// Whether the next frame follows an attribute, which annotates it.
  after_attribute: bool,
  /// The aggregate frames that contain the next frame, innermost last.
  stack:           Vec<OpenAggregate>,
}

impl FrameScanner {
  /// Create a new scanner.
  pub fn new() -> Self {
    Self::default()
  }

  /// Reset the scanner so that the next call to [scan](Self::scan) starts from the beginning of the buffer.
  pub fn reset(&mut self) {
    self.pos = 0;
    self.searched = 0;
    self.after_attribute = false;
    self.stack.clear();
  }

  fn finish(&mut self, result: Scan) -> Scan {
    self.reset();
    result
  }

  /// Whether the scanner has not read any part of the next frame.
  fn is_idle(&self) -> bool {
    self.pos == 0 && self.searched == 0
  }

  /// Decode the next frame in `buf` with `decode`, using the scanner to avoid parsing a partial frame more than once.
  ///
  /// If no part of the next frame has been scanned yet, `decode` runs right away, since the whole frame is usually
  /// already buffered. If it returns `Ok(None)`, the scanner reads the partial frame, and later calls only run
  /// `decode` once the frame is complete. This costs one parse per frame when frames arrive whole, and keeps the
  /// cost linear in the frame size when they don't.
  ///
  /// ```rust
  /// # use redis_protocol::resp3::decode::{streaming::decode, FrameScanner};
  /// let mut scanner = FrameScanner::new();
  /// let mut buf = b"$5\r\nhel".to_vec();
  /// assert!(scanner
  ///   .decode_with(&mut buf, |buf| decode(buf))
  ///   .unwrap()
  ///   .is_none());
  ///
  /// buf.extend_from_slice(b"lo\r\n");
  /// let (frame, amt) = scanner
  ///   .decode_with(&mut buf, |buf| decode(buf))
  ///   .unwrap()
  ///   .unwrap();
  /// buf.drain(.. amt);
  /// ```
  pub fn decode_with<B, T, E, F>(&mut self, buf: &mut B, decode: F) -> Result<Option<T>, E>
  where
    B: AsRef<[u8]> + ?Sized,
    F: FnOnce(&mut B) -> Result<Option<T>, E>,
  {
    if self.is_idle() {
      let result = decode(buf);
      if let Ok(None) = result {
        let _ = self.scan(buf.as_ref());
      }
      result
    } else if self.scan(buf.as_ref()) == Scan::Incomplete {
      Ok(None)
    } else {
      decode(buf)
    }
  }

  /// Move past a frame header, or a whole frame.
  fn advance(&mut self, next: usize) {
    self.pos = next;
    self.searched = next;
    self.after_attribute = false;
  }

  /// Record the kind of the next inner frame in the innermost aggregate, which is a map or set key if the aggregate
  /// requires one.
  fn check_key(&mut self, kind: FrameKind) {
    if let Some(parent) = self.stack.last_mut() {
      let is_key = match parent.kind {
        FrameKind::Set => true,
        // the first of each pair. maps and attributes count down from an even number.
        FrameKind::Map | FrameKind::Attribute => parent.remaining % 2 == 0,
        _ => false,
      };

      if is_key && !kind.can_hash() {
        parent.bad_key = true;
      }
    }
  }

  /// Continue scanning `buf`, which must contain the same bytes as the previous call, plus any newly received bytes.
  pub fn scan(&mut self, buf: &[u8]) -> Scan {
    if buf.len() < self.pos.max(self.searched) {
      // the buffer was changed without calling `reset`. start over rather than read the wrong offsets.
      self.reset();
    }

    loop {
      let depth = self.stack.len();
      if depth > MAX_DECODE_DEPTH {
        return self.finish(Scan::Invalid);
      }
      // the decoder rejects an invalid type byte before reading the rest of the line
      let (input, kind) = match d_frame_type((&buf[self.pos ..], self.pos)) {
        Ok(result) => result,
        Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
        Err(_) => return self.finish(Scan::Invalid),
      };
      if kind == FrameKind::Attribute && self.after_attribute {
        // the decoder doesn't allow an attribute to annotate another attribute
        return self.finish(Scan::Invalid);
      }
      if !utils::line_is_buffered(buf, self.pos, &mut self.searched) {
        return Scan::Incomplete;
      }

      let after_attribute = self.after_attribute;
      if kind == FrameKind::Attribute {
        match d_read_prefix_len(input) {
          Ok(((_, next), len)) => {
            self.advance(next);
            if len > 0 {
              self.stack.push(OpenAggregate {
                kind:      FrameKind::Attribute,
                remaining: len.saturating_mul(2),
                bad_key:   false,
              });
            } else {
              // the annotated frame comes next. otherwise it follows the attribute's key/value pairs.
              self.after_attribute = true;
            }
            continue;
          },
          Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
          Err(_) => return self.finish(Scan::Invalid),
        }
      }

      if matches!(
        kind,
        FrameKind::Array | FrameKind::Set | FrameKind::Map | FrameKind::Push
      ) {
        let ((_, next), len) = match d_read_prefix_len_or_marker(input) {
          Ok(result) => result,
          Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
          Err(_) => return self.finish(Scan::Invalid),
        };

        let len = match len {
          PrefixLen::Streamed if kind == FrameKind::Push => return self.finish(Scan::Invalid),
          // the header of a streamed aggregate is decoded as a separate frame, which can't be inside another frame
          PrefixLen::Streamed if depth == 0 => return self.finish(Scan::Complete(next)),
          PrefixLen::Streamed => return self.finish(Scan::Invalid),
          // decoded as a null frame, which can't have attributes
          PrefixLen::Null if after_attribute => return self.finish(Scan::Invalid),
          PrefixLen::Null => {
            self.check_key(FrameKind::Null);
            0
          },
          PrefixLen::Len(len) => {
            self.check_key(kind);
            len
          },
        };
        self.advance(next);
        let len = if kind == FrameKind::Map {
          len.saturating_mul(2)
        } else {
          len
        };
        if len > 0 {
          self.stack.push(OpenAggregate {
            kind,
            remaining: len,
            bad_key: false,
          });
          continue;
        }
      } else {
        // leaf frames are parsed with the decoder's own logic, which only reads one header line plus any blob
        // contents
        let ((_, next), mut frame) = match d_parse_leaf_frame(input, kind) {
          Ok(result) => result,
          Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
          Err(_) => return self.finish(Scan::Invalid),
        };
        if after_attribute && frame.add_attributes(Vec::new()).is_err() {
          return self.finish(Scan::Invalid);
        }

        match frame {
          DecodedRangeFrame::Streaming(_) if depth == 0 => return self.finish(Scan::Complete(next)),
          DecodedRangeFrame::Streaming(_) => return self.finish(Scan::Invalid),
          DecodedRangeFrame::Complete(RangeFrame::ChunkedString(_)) if depth > 0 => {
            return self.finish(Scan::Invalid)
          },
          DecodedRangeFrame::Complete(frame) => self.check_key(frame.kind()),
        }
        self.advance(next);
      }

      // a frame ended, which may also end one or more of the aggregates that contain it
      loop {
        let parent = match self.stack.last_mut() {
          Some(parent) => parent,
          None => {
            let amt = self.pos;
            return self.finish(Scan::Complete(amt));
          },
        };

        parent.remaining -= 1;
        if parent.remaining > 0 {
          break;
        }
        let closed = self.stack.pop().unwrap();
        if closed.bad_key {
          return self.finish(Scan::Invalid);
        }
        if closed.kind == FrameKind::Attribute {
          // the frame that the attribute annotates comes next, in the same position
          self.after_attribute = true;
          break;
        }
      }
    }
  }
}

#[cfg(test)]
pub mod owned_tests {
  use super::*;
  use crate::resp3::decode::{complete::decode, streaming::decode as stream_decode};
  use std::str;

  pub const PADDING: &str = "FOOBARBAZ";

  #[test]
  #[cfg(feature = "index-map")]
  fn should_keep_map_and_attribute_order() {
    let keys = |map: &FrameMap<OwnedFrame, OwnedFrame>| -> Vec<String> {
      map.keys().map(|k| k.as_str().unwrap().to_owned()).collect()
    };

    let buf = b"|3\r\n+x\r\n:1\r\n+y\r\n:2\r\n+z\r\n:3\r\n%3\r\n+a\r\n:1\r\n+b\r\n:2\r\n+c\r\n:3\r\n";
    let (frame, _) = complete::decode(buf).unwrap().unwrap();
    match frame {
      OwnedFrame::Map {
        data,
        attributes: Some(attributes),
      } => {
        assert_eq!(keys(&data), ["a", "b", "c"]);
        assert_eq!(keys(&attributes), ["x", "y", "z"]);
      },
      _ => panic!("expected a map with attributes: {frame:?}"),
    }
  }

  #[test]
  fn should_resolve_duplicate_keys_when_building_frames() {
    let map = b"%3\r\n+a\r\n:1\r\n+b\r\n:2\r\n+a\r\n:3\r\n";
    let set = b"~3\r\n+x\r\n+y\r\n+x\r\n";

    // the range frames keep every pair and value as received
    let (range, _) = complete::decode_range(map).unwrap().unwrap();
    match range {
      RangeFrame::Map { ref data, .. } => assert_eq!(data.len(), 3),
      _ => panic!("expected a map: {range:?}"),
    }
    let (range, _) = complete::decode_range(set).unwrap().unwrap();
    match range {
      RangeFrame::Set { ref data, .. } => assert_eq!(data.len(), 3),
      _ => panic!("expected a set: {range:?}"),
    }

    // the last value wins for a duplicate map key, and duplicate set values are removed
    let (frame, _) = complete::decode(map).unwrap().unwrap();
    let data = match frame {
      OwnedFrame::Map { data, .. } => data,
      _ => panic!("expected a map: {frame:?}"),
    };
    assert_eq!(data.len(), 2);
    assert_eq!(
      data.get(&OwnedFrame::SimpleString {
        data:       b"a".to_vec(),
        attributes: None,
      }),
      Some(&OwnedFrame::Number {
        data:       3,
        attributes: None,
      })
    );
    let (frame, _) = complete::decode(set).unwrap().unwrap();
    assert_eq!(frame.len(), 2);
  }

  #[test]
  fn should_build_frames_from_range_frames() {
    let buf = b"|1\r\n+a\r\n:1\r\n*3\r\n$3\r\nfoo\r\n%1\r\n+k\r\n,1.5\r\n~1\r\n#t\r\n";
    let (range, amt) = complete::decode_range(buf).unwrap().unwrap();
    let (expected, expected_amt) = complete::decode(buf).unwrap().unwrap();
    assert_eq!(amt, expected_amt);
    assert_eq!(range.to_owned_frame(buf).unwrap(), expected);
    #[cfg(feature = "bytes")]
    assert_eq!(
      range.to_bytes_frame(&Bytes::from_static(buf)).unwrap().to_owned_frame(),
      expected
    );
  }

  #[test]
  fn should_decode_resp2_nulls_as_null() {
    for bytes in [&b"$-1\r\n"[..], b"*-1\r\n", b"%-1\r\n", b"~-1\r\n", b">-1\r\n"] {
      let expected = (OwnedFrame::Null, bytes.len());
      assert_eq!(complete::decode(bytes).unwrap(), Some(expected.clone()), "{bytes:?}");
      let (frame, amt) = stream_decode(bytes).unwrap().unwrap();
      assert_eq!((frame.into_complete_frame().unwrap(), amt), expected, "{bytes:?}");
    }

    // a null inside an aggregate, followed by another frame
    let bytes = b"*3\r\n$-1\r\n*-1\r\n+OK\r\n:1\r\n";
    let expected = OwnedFrame::Array {
      data:       vec![OwnedFrame::Null, OwnedFrame::Null, OwnedFrame::SimpleString {
        data:       b"OK".to_vec(),
        attributes: None,
      }],
      attributes: None,
    };
    assert_eq!(complete::decode(bytes).unwrap(), Some((expected, bytes.len() - 4)));

    // other negative lengths are still invalid, and nulls can't have attributes
    for bytes in [&b"$-2\r\n"[..], b"*-2\r\n", b"|1\r\n+a\r\n+b\r\n*-1\r\n"] {
      assert!(complete::decode(bytes).is_err(), "{bytes:?}");
    }
  }

  #[test]
  fn should_require_crlf_after_blob_contents() {
    for bytes in [&b"$3\r\nfooXY"[..], b"!3\r\nfooXY", b"=7\r\ntxt:fooXY", b";3\r\nfooXY"] {
      assert!(complete::decode(bytes).is_err(), "{bytes:?}");
      assert!(stream_decode(bytes).is_err(), "{bytes:?}");
      assert_eq!(FrameScanner::new().scan(bytes), Scan::Invalid, "{bytes:?}");
    }
    // a partial CRLF is incomplete
    assert_eq!(complete::decode(b"$3\r\nfoo\r").unwrap(), None);
  }

  #[test]
  fn should_validate_verbatim_string_prefix() {
    for bytes in [&b"=7\r\ntxtXfoo\r\n"[..], b"=0\r\n+OK\r\n", b"=3\r\ntxt\r\n"] {
      assert!(complete::decode(bytes).is_err(), "{bytes:?}");
      assert_eq!(FrameScanner::new().scan(bytes), Scan::Invalid, "{bytes:?}");
    }
    assert!(complete::decode(b"=4\r\ntxt:\r\n").unwrap().is_some());
  }

  #[test]
  fn should_reject_chunks_inside_aggregates() {
    for bytes in [
      &b"*2\r\n;3\r\nfoo\r\n.\r\n"[..],
      b"%1\r\n;3\r\nfoo\r\n:1\r\n",
      b"~1\r\n.\r\n",
      b">1\r\n;0\r\n",
    ] {
      assert!(complete::decode(bytes).is_err(), "{bytes:?}");
      assert!(stream_decode(bytes).is_err(), "{bytes:?}");
      assert_eq!(FrameScanner::new().scan(bytes), Scan::Invalid, "{bytes:?}");
    }
  }

  #[test]
  fn should_reject_invalid_streamed_aggregates() {
    let build = |kind: FrameKind, frames: Vec<OwnedFrame>| {
      let mut streamed = StreamedFrame::new(kind);
      for frame in frames {
        streamed.add_frame(frame);
      }
      streamed.add_frame(OwnedFrame::new_end_stream());
      streamed.take()
    };
    let number = |data| OwnedFrame::Number { data, attributes: None };
    let array = || OwnedFrame::Array {
      data:       vec![number(1)],
      attributes: None,
    };

    assert!(build(FrameKind::Map, vec![array(), number(1)]).is_err());
    assert!(build(FrameKind::Map, vec![number(1), number(2), number(3)]).is_err());
    assert!(build(FrameKind::Set, vec![number(1), array()]).is_err());
    assert!(build(FrameKind::Array, vec![OwnedFrame::ChunkedString(b"foo".to_vec())]).is_err());

    // values can be aggregates
    assert!(build(FrameKind::Map, vec![number(1), array()]).is_ok());
    assert!(build(FrameKind::Array, vec![array(), number(1)]).is_ok());
  }

  fn decode_and_verify_some(bytes: &[u8], expected: &(Option<OwnedFrame>, usize)) {
    let (frame, len) = match complete::decode(bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => panic!("Failed to decode bytes. None returned."),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &[u8], expected: &(Option<OwnedFrame>, usize)) {
    let mut bytes = bytes.to_vec();
    bytes.extend_from_slice(PADDING.as_bytes());

    let (frame, len) = match complete::decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => panic!("Failed to decode bytes. None returned."),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &[u8]) {
    let (frame, len) = match complete::decode(bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => panic!("{e:?}"),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  // ----------------------- tests adapted from RESP2 ------------------------

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (
      Some(OwnedFrame::Number {
        data:       48293,
        attributes: None,
      }),
      8,
    );
    let bytes: Vec<u8> = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (
      Some(OwnedFrame::SimpleString {
        data:       "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (
      Some(OwnedFrame::SimpleString {
        data:       "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_blob_string() {
    let expected = (
      Some(OwnedFrame::BlobString {
        data:       "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = b"$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_blob_string_incomplete() {
    let expected = (
      Some(OwnedFrame::BlobString {
        data:       "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = b"$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(OwnedFrame::Array {
        data:       vec![
          OwnedFrame::SimpleString {
            data:       "Foo".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "Bar".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      16,
    );
    let bytes: Vec<u8> = b"*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let bytes: Vec<u8> = b"*3\r\n$3\r\nFoo\r\n_\r\n$3\r\nBar\r\n".into();

    let expected = (
      Some(OwnedFrame::Array {
        data:       vec![
          OwnedFrame::BlobString {
            data:       "Foo".into(),
            attributes: None,
          },
          OwnedFrame::Null,
          OwnedFrame::BlobString {
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
    let bytes: Vec<u8> = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(OwnedFrame::SimpleError {
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
    let bytes: Vec<u8> = b"-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (
      Some(OwnedFrame::SimpleError {
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
    let bytes: Vec<u8> = b"-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (
      Some(OwnedFrame::SimpleError {
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
    let bytes: Vec<u8> = b"*3\r\n$3\r\nFoo\r\n_\r\n$3\r\nBar".into();
    decode_and_verify_none(&bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    complete::decode(b"foobarbazwibblewobble").unwrap();
  }

  // ----------------- end tests adapted from RESP2 ------------------------

  #[test]
  fn should_decode_blob_error() {
    let expected = (
      Some(OwnedFrame::BlobError {
        data:       "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = b"!3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_blob_error_incomplete() {
    let expected = (
      Some(OwnedFrame::BlobError {
        data:       "foo".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = b"!3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_error() {
    let expected = (
      Some(OwnedFrame::SimpleError {
        data:       "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = b"-string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_error_incomplete() {
    let expected = (
      Some(OwnedFrame::SimpleError {
        data:       "string".into(),
        attributes: None,
      }),
      9,
    );
    let bytes: Vec<u8> = b"-strin".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_boolean_true() {
    let expected = (
      Some(OwnedFrame::Boolean {
        data:       true,
        attributes: None,
      }),
      4,
    );
    let bytes: Vec<u8> = b"#t\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_boolean_false() {
    let expected = (
      Some(OwnedFrame::Boolean {
        data:       false,
        attributes: None,
      }),
      4,
    );
    let bytes: Vec<u8> = b"#f\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_number() {
    let expected = (
      Some(OwnedFrame::Number {
        data:       42,
        attributes: None,
      }),
      5,
    );
    let bytes: Vec<u8> = b":42\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_double_inf() {
    let expected = (
      Some(OwnedFrame::Double {
        data:       f64::INFINITY,
        attributes: None,
      }),
      6,
    );
    let bytes: Vec<u8> = b",inf\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_double_neg_inf() {
    let expected = (
      Some(OwnedFrame::Double {
        data:       f64::NEG_INFINITY,
        attributes: None,
      }),
      7,
    );
    let bytes: Vec<u8> = b",-inf\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_double_nan() {
    let expected = (
      Some(OwnedFrame::Double {
        data:       f64::NAN,
        attributes: None,
      }),
      7,
    );
    let bytes: Vec<u8> = b",foo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_double() {
    let expected = (
      Some(OwnedFrame::Double {
        data:       4.59193,
        attributes: None,
      }),
      10,
    );
    let bytes: Vec<u8> = b",4.59193\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let expected = (
      Some(OwnedFrame::Double {
        data:       4_f64,
        attributes: None,
      }),
      4,
    );
    let bytes: Vec<u8> = b",4\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bignumber() {
    let expected = (
      Some(OwnedFrame::BigNumber {
        data:       "3492890328409238509324850943850943825024385".into(),
        attributes: None,
      }),
      46,
    );
    let bytes: Vec<u8> = b"(3492890328409238509324850943850943825024385\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_null() {
    let expected = (Some(OwnedFrame::Null), 3);
    let bytes: Vec<u8> = b"_\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_verbatim_string_mkd() {
    let expected = (
      Some(OwnedFrame::VerbatimString {
        data:       "Some string".into(),
        format:     VerbatimStringFormat::Markdown,
        attributes: None,
      }),
      22,
    );
    let bytes: Vec<u8> = b"=15\r\nmkd:Some string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_verbatim_string_txt() {
    let expected = (
      Some(OwnedFrame::VerbatimString {
        data:       "Some string".into(),
        format:     VerbatimStringFormat::Text,
        attributes: None,
      }),
      22,
    );
    let bytes: Vec<u8> = b"=15\r\ntxt:Some string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_map_no_nulls() {
    let k1 = OwnedFrame::SimpleString {
      data:       "first".into(),
      attributes: None,
    };
    let v1 = OwnedFrame::Number {
      data:       1,
      attributes: None,
    };
    let k2 = OwnedFrame::BlobString {
      data:       "second".into(),
      attributes: None,
    };
    let v2 = OwnedFrame::Double {
      data:       4.2,
      attributes: None,
    };

    let mut expected_map = resp3_utils::new_map(0);
    expected_map.insert(k1, v1);
    expected_map.insert(k2, v2);
    let expected = (
      Some(OwnedFrame::Map {
        data:       expected_map,
        attributes: None,
      }),
      34,
    );
    let bytes: Vec<u8> = b"%2\r\n+first\r\n:1\r\n$6\r\nsecond\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_map_with_nulls() {
    let k1 = OwnedFrame::SimpleString {
      data:       "first".into(),
      attributes: None,
    };
    let v1 = OwnedFrame::Number {
      data:       1,
      attributes: None,
    };
    let k2 = OwnedFrame::Number {
      data:       2,
      attributes: None,
    };
    let v2 = OwnedFrame::Null;
    let k3 = OwnedFrame::BlobString {
      data:       "second".into(),
      attributes: None,
    };
    let v3 = OwnedFrame::Double {
      data:       4.2,
      attributes: None,
    };

    let mut expected_map = resp3_utils::new_map(0);
    expected_map.insert(k1, v1);
    expected_map.insert(k2, v2);
    expected_map.insert(k3, v3);
    let expected = (
      Some(OwnedFrame::Map {
        data:       expected_map,
        attributes: None,
      }),
      41,
    );
    let bytes: Vec<u8> = b"%3\r\n+first\r\n:1\r\n:2\r\n_\r\n$6\r\nsecond\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_set_no_nulls() {
    let mut expected_set = resp3_utils::new_set(0);
    expected_set.insert(OwnedFrame::Number {
      data:       1,
      attributes: None,
    });
    expected_set.insert(OwnedFrame::SimpleString {
      data:       "2".into(),
      attributes: None,
    });
    expected_set.insert(OwnedFrame::BlobString {
      data:       "foobar".into(),
      attributes: None,
    });
    expected_set.insert(OwnedFrame::Double {
      data:       4.2,
      attributes: None,
    });
    let expected = (
      Some(OwnedFrame::Set {
        data:       expected_set,
        attributes: None,
      }),
      30,
    );
    let bytes: Vec<u8> = b"~4\r\n:1\r\n+2\r\n$6\r\nfoobar\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_set_with_nulls() {
    let mut expected_set = resp3_utils::new_set(0);
    expected_set.insert(OwnedFrame::Number {
      data:       1,
      attributes: None,
    });
    expected_set.insert(OwnedFrame::SimpleString {
      data:       "2".into(),
      attributes: None,
    });
    expected_set.insert(OwnedFrame::Null);
    expected_set.insert(OwnedFrame::Double {
      data:       4.2,
      attributes: None,
    });
    let expected = (
      Some(OwnedFrame::Set {
        data:       expected_set,
        attributes: None,
      }),
      21,
    );
    let bytes: Vec<u8> = b"~4\r\n:1\r\n+2\r\n_\r\n,4.2\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_push_pubsub() {
    let expected = (
      Some(OwnedFrame::Push {
        data:       vec![
          OwnedFrame::SimpleString {
            data:       "pubsub".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "message".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "somechannel".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "this is the message".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      59,
    );
    let bytes: Vec<u8> = b">4\r\n+pubsub\r\n+message\r\n+somechannel\r\n+this is the message\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let (frame, _) = decode(&bytes).unwrap().unwrap();
    assert!(frame.is_normal_pubsub_message());
  }

  #[test]
  fn should_decode_push_pattern_pubsub() {
    let expected = (
      Some(OwnedFrame::Push {
        data:       vec![
          OwnedFrame::SimpleString {
            data:       "pubsub".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "pmessage".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "pattern".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "somechannel".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "this is the message".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      70,
    );
    let bytes: Vec<u8> = b">5\r\n+pubsub\r\n+pmessage\r\n+pattern\r\n+somechannel\r\n+this is the message\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let (frame, _) = decode(&bytes).unwrap().unwrap();
    assert!(frame.is_pattern_pubsub_message());
  }

  #[test]
  fn should_decode_keyevent_message() {
    let expected = (
      Some(OwnedFrame::Push {
        data:       vec![
          OwnedFrame::SimpleString {
            data:       "pubsub".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "pmessage".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "__key*".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "__keyevent@0__:set".into(),
            attributes: None,
          },
          OwnedFrame::SimpleString {
            data:       "foo".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      60,
    );
    let bytes: Vec<u8> = b">5\r\n+pubsub\r\n+pmessage\r\n+__key*\r\n+__keyevent@0__:set\r\n+foo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);

    let (frame, _) = decode(&bytes).unwrap().unwrap();
    assert!(frame.is_pattern_pubsub_message());
  }

  #[test]
  fn should_parse_outer_attributes() {
    let mut expected_inner_attrs = resp3_utils::new_map(0);
    expected_inner_attrs.insert(
      OwnedFrame::BlobString {
        data:       "a".into(),
        attributes: None,
      },
      OwnedFrame::Double {
        data:       0.1923,
        attributes: None,
      },
    );
    expected_inner_attrs.insert(
      OwnedFrame::BlobString {
        data:       "b".into(),
        attributes: None,
      },
      OwnedFrame::Double {
        data:       0.0012,
        attributes: None,
      },
    );
    let expected_inner_attrs = OwnedFrame::Map {
      data:       expected_inner_attrs,
      attributes: None,
    };

    let mut expected_attrs = resp3_utils::new_map(0);
    expected_attrs.insert(
      OwnedFrame::SimpleString {
        data:       "key-popularity".into(),
        attributes: None,
      },
      expected_inner_attrs,
    );

    let expected = (
      Some(OwnedFrame::Array {
        data:       vec![
          OwnedFrame::Number {
            data:       2039123,
            attributes: None,
          },
          OwnedFrame::Number {
            data:       9543892,
            attributes: None,
          },
        ],
        attributes: Some(Box::new(expected_attrs)),
      }),
      81,
    );

    let bytes: Vec<u8> =
      b"|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n"
        .into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_parse_inner_attributes() {
    let mut expected_attrs = resp3_utils::new_map(0);
    expected_attrs.insert(
      OwnedFrame::SimpleString {
        data:       "ttl".into(),
        attributes: None,
      },
      OwnedFrame::Number {
        data:       3600,
        attributes: None,
      },
    );

    let expected = (
      Some(OwnedFrame::Array {
        data:       vec![
          OwnedFrame::Number {
            data:       1,
            attributes: None,
          },
          OwnedFrame::Number {
            data:       2,
            attributes: None,
          },
          OwnedFrame::Number {
            data:       3,
            attributes: Some(Box::new(expected_attrs)),
          },
        ],
        attributes: None,
      }),
      33,
    );
    let bytes: Vec<u8> = b"*3\r\n:1\r\n:2\r\n|1\r\n+ttl\r\n:3600\r\n:3\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_end_stream() {
    let bytes: Vec<u8> = b";0\r\n".into();
    let (frame, _) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::new_end_stream()))
  }

  #[test]
  fn should_decode_streaming_string() {
    let mut bytes: Vec<u8> = "$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Streaming(StreamedFrame::new(FrameKind::BlobString))
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::ChunkedString("Hell".into())));
    assert_eq!(amt, 10);
    bytes = bytes[amt ..].to_vec();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::ChunkedString("o worl".into()))
    );
    assert_eq!(amt, 12);
    bytes = bytes[amt ..].to_vec();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::ChunkedString("d".into())));
    assert_eq!(amt, 7);
    bytes = bytes[amt ..].to_vec();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::new_end_stream()));
    assert_eq!(amt, 4);
  }

  #[test]
  fn should_decode_streaming_array() {
    let mut bytes: Vec<u8> = "*?\r\n:1\r\n:2\r\n:3\r\n.\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Array)));
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       1,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       2,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       3,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.take().unwrap();
    let expected = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::Number {
          data:       1,
          attributes: None,
        },
        OwnedFrame::Number {
          data:       2,
          attributes: None,
        },
        OwnedFrame::Number {
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
    let mut bytes: Vec<u8> = "~?\r\n:1\r\n:2\r\n:3\r\n.\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Set)));
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       1,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       2,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       3,
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.take().unwrap();
    let mut expected_result = resp3_utils::new_set(0);
    expected_result.insert(OwnedFrame::Number {
      data:       1,
      attributes: None,
    });
    expected_result.insert(OwnedFrame::Number {
      data:       2,
      attributes: None,
    });
    expected_result.insert(OwnedFrame::Number {
      data:       3,
      attributes: None,
    });

    let expected = OwnedFrame::Set {
      data:       expected_result,
      attributes: None,
    };

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_streaming_map() {
    let mut bytes: Vec<u8> = "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n".into();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Streaming(StreamedFrame::new(FrameKind::Map)));
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    let mut streamed = frame.into_streaming_frame().unwrap();

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::SimpleString {
        data:       "a".into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       1.into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::SimpleString {
        data:       "b".into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(
      frame,
      DecodedFrame::Complete(OwnedFrame::Number {
        data:       2.into(),
        attributes: None,
      })
    );
    assert_eq!(amt, 4);
    bytes = bytes[amt ..].to_vec();
    streamed.add_frame(frame.into_complete_frame().unwrap());

    let (frame, amt) = stream_decode(&bytes).unwrap().unwrap();
    assert_eq!(frame, DecodedFrame::Complete(OwnedFrame::new_end_stream()));
    assert_eq!(amt, 3);
    streamed.add_frame(frame.into_complete_frame().unwrap());

    assert!(streamed.is_finished());
    let actual = streamed.take().unwrap();
    let mut expected_result = resp3_utils::new_map(0);
    expected_result.insert(
      OwnedFrame::SimpleString {
        data:       "a".into(),
        attributes: None,
      },
      OwnedFrame::Number {
        data:       1,
        attributes: None,
      },
    );
    expected_result.insert(
      OwnedFrame::SimpleString {
        data:       "b".into(),
        attributes: None,
      },
      OwnedFrame::Number {
        data:       2,
        attributes: None,
      },
    );
    let expected = OwnedFrame::Map {
      data:       expected_result,
      attributes: None,
    };

    assert_eq!(actual, expected);
  }

  #[test]
  fn should_decode_hello_no_auth() {
    let expected = (
      Some(OwnedFrame::Hello {
        version: RespVersion::RESP3,
        auth:    None,
        setname: None,
      }),
      9,
    );
    let bytes = "HELLO 3\r\n";

    decode_and_verify_some(bytes.as_bytes(), &expected);
    decode_and_verify_padded_some(bytes.as_bytes(), &expected);
  }

  #[test]
  fn should_decode_hello_with_auth_no_setname() {
    let expected = (
      Some(OwnedFrame::Hello {
        version: RespVersion::RESP3,
        auth:    Some(Box::new(("foo".into(), "bar".into()))),
        setname: None,
      }),
      22,
    );
    let bytes = "HELLO 3 AUTH foo bar\r\n";

    decode_and_verify_some(bytes.as_bytes(), &expected);
    decode_and_verify_padded_some(bytes.as_bytes(), &expected);
  }

  #[test]
  fn should_decode_hello_with_setname() {
    let expected = (
      Some(OwnedFrame::Hello {
        version: RespVersion::RESP3,
        auth:    None,
        setname: Some("baz".into()),
      }),
      21,
    );
    let bytes = "HELLO 3 SETNAME baz\r\n";

    decode_and_verify_some(bytes.as_bytes(), &expected);
    decode_and_verify_padded_some(bytes.as_bytes(), &expected);
  }

  #[test]
  fn should_decode_hello_with_auth_and_setname() {
    let expected = (
      Some(OwnedFrame::Hello {
        version: RespVersion::RESP3,
        auth:    Some(Box::new(("foo".into(), "bar".into()))),
        setname: Some("baz".into()),
      }),
      34,
    );
    let bytes = "HELLO 3 AUTH foo bar SETNAME baz\r\n";

    decode_and_verify_some(bytes.as_bytes(), &expected);
    decode_and_verify_padded_some(bytes.as_bytes(), &expected);
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
pub mod bytes_tests {
  use super::*;
  use crate::resp3::decode::{complete::decode, streaming::decode_bytes as stream_decode};
  use bytes::{Bytes, BytesMut};
  use nom::AsBytes;
  use std::str;

  const PADDING: &str = "FOOBARBAZ";

  fn decode_and_verify_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let (frame, len) = match complete::decode_bytes(bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => panic!("Failed to decode bytes. None returned."),
      Err(e) => panic!("{e:?}"),
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
      Ok(None) => panic!("Failed to decode bytes. None returned."),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &Bytes) {
    let (frame, len) = match complete::decode_bytes(bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => panic!("{e:?}"),
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
    complete::decode("foobarbazwibblewobble".as_bytes()).unwrap();
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
        attributes: Some(Box::new(expected_attrs)),
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
            attributes: Some(Box::new(expected_attrs)),
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

  #[test]
  fn should_decode_hello_no_auth() {
    let expected = (
      Some(BytesFrame::Hello {
        version: RespVersion::RESP3,
        auth:    None,
        setname: None,
      }),
      9,
    );
    let bytes = "HELLO 3\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_hello_with_auth_no_setname() {
    let expected = (
      Some(BytesFrame::Hello {
        version: RespVersion::RESP3,
        auth:    Some(Box::new(("foo".into(), "bar".into()))),
        setname: None,
      }),
      22,
    );
    let bytes = "HELLO 3 AUTH foo bar\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_hello_with_setname() {
    let expected = (
      Some(BytesFrame::Hello {
        version: RespVersion::RESP3,
        auth:    None,
        setname: Some("baz".into()),
      }),
      21,
    );
    let bytes = "HELLO 3 SETNAME baz\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_hello_with_auth_and_setname() {
    let expected = (
      Some(BytesFrame::Hello {
        version: RespVersion::RESP3,
        auth:    Some(Box::new(("foo".into(), "bar".into()))),
        setname: Some("baz".into()),
      }),
      34,
    );
    let bytes = "HELLO 3 AUTH foo bar SETNAME baz\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }
}

#[cfg(test)]
mod depth_tests {
  use super::*;

  fn nested(prefix: &[u8], depth: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(depth * prefix.len() + 4);
    for _ in 0 .. depth {
      buf.extend_from_slice(prefix);
    }
    buf.extend_from_slice(b"+e\r\n");
    buf
  }

  #[test]
  fn should_decode_at_max_depth() {
    // sets cannot contain aggregate types, and attributes must precede a non-attribute frame
    for prefix in [&b"*1\r\n"[..], b">1\r\n", b"%1\r\n+k\r\n"] {
      let buf = nested(prefix, MAX_DECODE_DEPTH);
      let (_, amt) = complete::decode(&buf).unwrap().unwrap();
      assert_eq!(amt, buf.len());
    }
  }

  #[test]
  fn should_error_past_max_depth() {
    for prefix in [&b"*1\r\n"[..], b"~1\r\n", b">1\r\n", b"%1\r\n+k\r\n", b"|1\r\n+k\r\n"] {
      let buf = nested(prefix, MAX_DECODE_DEPTH + 1);
      assert!(complete::decode(&buf).is_err());
      assert!(streaming::decode(&buf).is_err());
    }
  }

  #[test]
  fn should_not_overflow_stack_with_deep_nesting() {
    // https://github.com/aembke/redis-protocol.rs/issues/45
    std::thread::Builder::new()
      .stack_size(1024 * 1024)
      .spawn(|| {
        for prefix in [&b"*1\r\n"[..], b"~1\r\n", b">1\r\n", b"%1\r\n+k\r\n", b"|1\r\n+k\r\n"] {
          let buf = nested(prefix, 100_000);
          assert!(complete::decode(&buf).is_err());
          assert!(streaming::decode(&buf).is_err());
        }
      })
      .unwrap()
      .join()
      .unwrap();
  }
}

#[cfg(test)]
mod scan_tests {
  use super::*;
  use alloc::{format, vec::Vec};

  struct Rng(u64);
  impl Rng {
    fn next(&mut self) -> u64 {
      self.0 ^= self.0 << 13;
      self.0 ^= self.0 >> 7;
      self.0 ^= self.0 << 17;
      self.0
    }

    fn below(&mut self, n: u64) -> u64 {
      self.next() % n
    }

    fn one_in(&mut self, n: u64) -> bool {
      self.below(n) == 0
    }
  }

  /// Bytes that must be skipped by length rather than parsed: CRLF and every type byte.
  const NOISE: &[u8] = b"ab\r\n*$:-+%|>~;._?H=!(,#01";

  fn noise(rng: &mut Rng, len: usize, out: &mut Vec<u8>) {
    for _ in 0 .. len {
      out.push(NOISE[rng.below(NOISE.len() as u64) as usize]);
    }
  }

  fn blob(rng: &mut Rng, kind: u8, out: &mut Vec<u8>) {
    let len = rng.below(200) as usize;
    out.push(kind);
    out.extend_from_slice(format!("{len}\r\n").as_bytes());
    noise(rng, len, out);
    out.extend_from_slice(b"\r\n");
  }

  /// A frame that can be a map key or set member.
  fn key(rng: &mut Rng, out: &mut Vec<u8>) {
    match rng.below(4) {
      0 => out.extend_from_slice(format!("+k{}\r\n", rng.below(1000)).as_bytes()),
      1 => out.extend_from_slice(format!(":{}\r\n", rng.below(1000)).as_bytes()),
      2 => out.extend_from_slice(b"_\r\n"),
      _ => blob(rng, b'$', out),
    }
  }

  /// A frame, which is occasionally invalid in a way that the decoder only detects after parsing other frames.
  fn frame(rng: &mut Rng, depth: u32, out: &mut Vec<u8>) {
    match rng.below(if depth == 0 { 13 } else { 20 }) {
      0 => out.extend_from_slice(b"+OK\r\n"),
      1 => out.extend_from_slice(b"-ERR wrong type\r\n"),
      2 => out.extend_from_slice(format!(":{}\r\n", rng.next() as i64).as_bytes()),
      // RESP2 nulls, which decode as null frames
      3 => out.extend_from_slice([&b"_\r\n"[..], b"$-1\r\n", b"*-1\r\n", b"%-1\r\n"][rng.below(4) as usize]),
      4 => out.extend_from_slice(if rng.one_in(2) { b"#t\r\n" } else { b"#f\r\n" }),
      5 => out.extend_from_slice(b",3.14159\r\n"),
      6 => out.extend_from_slice(b"(3492890328409238509324850943850943825024385\r\n"),
      7 => blob(rng, b'$', out),
      8 => blob(rng, b'!', out),
      9 => {
        let len = rng.below(100) as usize;
        let format = if rng.one_in(20) { "bad" } else { "txt" };
        out.extend_from_slice(format!("={}\r\n{format}:", len + 4).as_bytes());
        noise(rng, len, out);
        out.extend_from_slice(b"\r\n");
      },
      10 => out.extend_from_slice(b"HELLO 3 AUTH default password\r\n"),
      11 => {
        // a chunk of a streamed string, which the decoder rejects inside an aggregate
        let len = rng.below(50) as usize;
        out.extend_from_slice(format!(";{len}\r\n").as_bytes());
        if len > 0 {
          noise(rng, len, out);
          out.extend_from_slice(b"\r\n");
        }
      },
      12 => {
        // a streamed frame header, which is only valid at the top level
        out.extend_from_slice([&b"$?\r\n"[..], b"*?\r\n", b"%?\r\n", b"~?\r\n", b">?\r\n"][rng.below(5) as usize])
      },
      13 | 14 => {
        let len = rng.below(5);
        let kind = if rng.one_in(2) { '*' } else { '>' };
        out.extend_from_slice(format!("{kind}{len}\r\n").as_bytes());
        for _ in 0 .. len {
          frame(rng, depth - 1, out);
        }
      },
      15 => {
        let len = rng.below(4);
        out.extend_from_slice(format!("%{len}\r\n").as_bytes());
        for _ in 0 .. len {
          if rng.one_in(10) {
            // an aggregate key, which the decoder rejects once the map ends
            frame(rng, depth - 1, out);
          } else {
            key(rng, out);
          }
          frame(rng, depth - 1, out);
        }
      },
      16 => {
        let len = rng.below(4);
        out.extend_from_slice(format!("~{len}\r\n").as_bytes());
        for _ in 0 .. len {
          if rng.one_in(10) {
            frame(rng, depth - 1, out);
          } else {
            key(rng, out);
          }
        }
      },
      _ => {
        let len = rng.below(3);
        out.extend_from_slice(format!("|{len}\r\n").as_bytes());
        for _ in 0 .. len {
          if rng.one_in(10) {
            frame(rng, depth - 1, out);
          } else {
            key(rng, out);
          }
          frame(rng, depth - 1, out);
        }
        // the annotated frame, which may also be a null or another attribute, both of which the decoder rejects
        frame(rng, depth - 1, out);
      },
    }
  }

  fn decode_len(buf: &[u8]) -> Result<Option<usize>, RedisProtocolError> {
    streaming::decode_range(buf).map(|r| r.map(|(_, amt)| amt))
  }

  /// Feed `wire` to a scanner in random chunks, checking that each result matches the decoder's.
  fn check(rng: &mut Rng, wire: &[u8]) {
    let max_chunk = [1, 3, 16, 256, 4096][rng.below(5) as usize];
    let mut scanner = FrameScanner::new();
    let mut buf = Vec::new();
    let mut offset = 0;

    while offset < wire.len() {
      let end = (offset + 1 + rng.below(max_chunk) as usize).min(wire.len());
      buf.extend_from_slice(&wire[offset .. end]);
      offset = end;

      loop {
        match (scanner.scan(&buf), decode_len(&buf)) {
          (Scan::Incomplete, Ok(None)) => break,
          (Scan::Complete(amt), Ok(Some(expected))) if amt == expected => {
            buf.drain(.. amt);
          },
          (Scan::Invalid, Err(_)) => return,
          (scan, decoded) => panic!(
            "scan returned {:?}, decoder returned {:?}, buffer: {:?}",
            scan,
            decoded,
            String::from_utf8_lossy(&buf)
          ),
        }
      }
    }
  }

  #[test]
  fn should_match_decoder_on_chunked_input() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0 .. 10_000 {
      let mut wire = Vec::new();
      for _ in 0 .. 1 + rng.below(3) {
        frame(&mut rng, 3, &mut wire);
      }
      check(&mut rng, &wire);
    }
  }

  #[test]
  fn should_match_decoder_on_corrupted_input() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    for _ in 0 .. 10_000 {
      let mut wire = Vec::new();
      for _ in 0 .. 1 + rng.below(3) {
        frame(&mut rng, 3, &mut wire);
      }
      for _ in 0 .. 1 + rng.below(2) {
        let idx = rng.below(wire.len() as u64) as usize;
        wire[idx] = NOISE[rng.below(NOISE.len() as u64) as usize];
      }
      check(&mut rng, &wire);
    }
  }

  #[test]
  fn should_match_decoder_at_max_depth() {
    let mut rng = Rng(7);
    for depth in [
      MAX_DECODE_DEPTH - 1,
      MAX_DECODE_DEPTH,
      MAX_DECODE_DEPTH + 1,
      MAX_DECODE_DEPTH + 2,
    ] {
      for prefix in [
        &b"*1\r\n"[..],
        b"%1\r\n+k\r\n",
        b"|1\r\n+k\r\n",
        b"|0\r\n*1\r\n",
        b">1\r\n",
      ] {
        for tail in [
          &b":1\r\n"[..],
          b"*0\r\n",
          b"*?\r\n",
          b"*1\r\n",
          b"|1\r\n+a\r\n+b\r\n+c\r\n",
        ] {
          let mut wire = prefix.repeat(depth);
          wire.extend_from_slice(tail);
          if prefix.starts_with(b"|1") {
            // each attribute annotates the frame after the attributes
            wire.extend_from_slice(&b"+v\r\n".repeat(depth));
          }
          for _ in 0 .. 4 {
            check(&mut rng, &wire);
          }
        }
      }
    }
  }

  #[test]
  fn should_find_long_simple_strings_incrementally() {
    let mut wire = b"(".to_vec();
    wire.extend(core::iter::repeat_n(b'1', 10_000));
    wire.extend_from_slice(b"\r\n");
    let mut scanner = FrameScanner::new();
    for end in 1 .. wire.len() {
      assert_eq!(scanner.scan(&wire[.. end]), Scan::Incomplete);
      assert_eq!(scanner.searched, end);
    }
    assert_eq!(scanner.scan(&wire), Scan::Complete(wire.len()));
  }

  #[test]
  fn should_scan_streamed_frames_one_part_at_a_time() {
    let mut scanner = FrameScanner::new();
    let mut buf: &[u8] = b"*?\r\n:1\r\n.\r\n$?\r\n;3\r\nfoo\r\n;0\r\n";
    for expected in [4, 4, 3, 4, 9, 4] {
      assert_eq!(scanner.scan(buf), Scan::Complete(expected));
      buf = &buf[expected ..];
    }
    assert_eq!(scanner.scan(buf), Scan::Incomplete);
  }
}
