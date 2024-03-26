//! Functions for decoding the RESP2 protocol into frames.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::{
  error::{RedisParseError, RedisProtocolError},
  resp2::{types::*, utils::build_owned_frame},
  types::*,
  utils,
};
use alloc::vec::Vec;
use core::str;
use nom::{
  bytes::streaming::{take as nom_take, take_until as nom_take_until},
  multi::count as nom_count,
  number::streaming::be_u8,
  sequence::terminated as nom_terminated,
  Err as NomErr,
};

#[cfg(feature = "bytes")]
use crate::resp2::utils::{build_bytes_frame, freeze_parse};
#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};

pub(crate) const NULL_LEN: isize = -1;

fn to_isize(s: &[u8]) -> Result<isize, RedisParseError<&[u8]>> {
  str::from_utf8(s)?
    .parse::<isize>()
    .map_err(|_| RedisParseError::new_custom("to_isize", "Failed to parse as integer."))
}

fn to_i64(s: &[u8]) -> Result<i64, RedisParseError<&[u8]>> {
  str::from_utf8(s)?
    .parse::<i64>()
    .map_err(|_| RedisParseError::new_custom("to_i64", "Failed to parse as integer."))
}

fn d_read_to_crlf(input: (&[u8], usize)) -> DResult<usize> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data.len()))
}

fn d_read_to_crlf_take(input: (&[u8], usize)) -> DResult<&[u8]> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data))
}

fn d_read_prefix_len(input: (&[u8], usize)) -> DResult<isize> {
  let (input, data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok((input, etry!(to_isize(data))))
}

fn d_frame_type(input: (&[u8], usize)) -> DResult<FrameKind> {
  let (input_bytes, byte) = be_u8(input.0)?;
  decode_log_str!(
    input_bytes,
    _input,
    "Reading frame type. Kind byte: {:?}, remaining: {:?}",
    byte,
    _input
  );

  let kind = match byte {
    SIMPLESTRING_BYTE => FrameKind::SimpleString,
    ERROR_BYTE => FrameKind::Error,
    INTEGER_BYTE => FrameKind::Integer,
    BULKSTRING_BYTE => FrameKind::BulkString,
    ARRAY_BYTE => FrameKind::Array,
    _ => e!(RedisParseError::new_custom("frame_type", "Invalid frame type.")),
  };
  Ok(((input_bytes, input.1 + 1), kind))
}

fn d_parse_simplestring(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::SimpleString((offset, offset + len))))
}

fn d_parse_integer(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_i64(data));
  Ok(((input, next_offset), RangeFrame::Integer(parsed)))
}

// assumes the '$-1\r\n' has been consumed already, since nulls look like bulk strings until the length prefix is
// parsed, and parsing the length prefix consumes the trailing \r\n in the underlying `terminated!` call
fn d_parse_null(input: (&[u8], usize)) -> DResult<RangeFrame> {
  Ok((input, RangeFrame::Null))
}

fn d_parse_error(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::Error((offset, offset + len))))
}

fn d_parse_bulkstring(input: (&[u8], usize), len: usize) -> DResult<RangeFrame> {
  let offset = input.1;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input.0)?;
  Ok((
    (input, offset + len + 2),
    RangeFrame::BulkString((offset, offset + data.len())),
  ))
}

fn d_parse_bulkstring_or_null(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  decode_log_str!(
    input,
    _input,
    "Parsing bulkstring, Length: {:?}, remaining: {:?}",
    len,
    _input
  );

  if len == NULL_LEN {
    d_parse_null((input, offset))
  } else {
    d_parse_bulkstring((input, offset), etry!(utils::isize_to_usize(len)))
  }
}

fn d_parse_array_frames(input: (&[u8], usize), len: usize) -> DResult<Vec<RangeFrame>> {
  decode_log_str!(
    input.0,
    _input,
    "Parsing array frames. Length: {:?}, remaining: {:?}",
    len,
    _input
  );
  nom_count(d_parse_frame, len)(input)
}

fn d_parse_array(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  decode_log_str!(
    input,
    _input,
    "Parsing array. Length: {:?}, remaining: {:?}",
    len,
    _input
  );

  if len == NULL_LEN {
    d_parse_null((input, offset))
  } else {
    let len = etry!(utils::isize_to_usize(len));
    let ((input, offset), frames) = d_parse_array_frames((input, offset), len)?;
    Ok(((input, offset), RangeFrame::Array(frames)))
  }
}

fn d_parse_frame(input: (&[u8], usize)) -> DResult<RangeFrame> {
  let ((input, offset), kind) = d_frame_type(input)?;
  decode_log_str!(input, _input, "Parsed kind: {:?}, remaining: {:?}", kind, _input);

  match kind {
    FrameKind::SimpleString => d_parse_simplestring((input, offset)),
    FrameKind::Error => d_parse_error((input, offset)),
    FrameKind::Integer => d_parse_integer((input, offset)),
    FrameKind::BulkString => d_parse_bulkstring_or_null((input, offset)),
    FrameKind::Array => d_parse_array((input, offset)),
    _ => e!(RedisParseError::new_custom("parse_frame", "Invalid frame kind.")),
  }
}

/// Attempt to the decode the contents of `buf`, returning frames that reference ranges into the provided buffer.
///
/// This is the generic interface behind the zero-copy interface and can be used to implement zero-copy
/// deserialization into other types.
pub fn decode_range(buf: &[u8]) -> Result<Option<(RangeFrame, usize)>, RedisProtocolError> {
  let (offset, len) = (0, buf.len());

  match d_parse_frame((buf, offset)) {
    Ok(((_remaining, amt), frame)) => {
      #[cfg(feature = "std")]
      debug_assert_eq!(amt, len - _remaining.len(), "returned offset doesn't match");
      Ok(Some((frame, amt)))
    },
    Err(NomErr::Incomplete(_)) => Ok(None),
    Err(NomErr::Error(e)) => Err(e.into()),
    Err(NomErr::Failure(e)) => Err(e.into()),
  }
}

/// Attempt to decode the contents of `buf`, returning the first valid frame and the number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<Option<(OwnedFrame, usize)>, RedisProtocolError> {
  let (frame, amt) = match decode_range(buf)? {
    Some(result) => result,
    None => return Ok(None),
  };

  Ok(Some((build_owned_frame(buf, &frame)?, amt)))
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

  Ok(Some((build_bytes_frame(buf, &frame)?, amt)))
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
  let (frame, amt) = match decode_range(&*buf)? {
    Some(result) => result,
    None => return Ok(None),
  };
  let (frame, buf) = freeze_parse(buf, &frame, amt)?;

  Ok(Some((frame, amt, buf)))
}

// Regression tests duplicated for each frame type.

#[cfg(test)]
mod owned_tests {
  use super::*;

  pub const PADDING: &'static str = "FOOBARBAZ";

  pub fn pretty_print_panic(e: RedisProtocolError) {
    panic!("{:?}", e);
  }

  pub fn panic_no_decode() {
    panic!("Failed to decode bytes. None returned");
  }

  fn decode_and_verify_some(bytes: &[u8], expected: &(Option<OwnedFrame>, usize)) {
    let mut bytes = bytes.to_vec();

    let (frame, len) = match decode(&mut bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &[u8], expected: &(Option<OwnedFrame>, usize)) {
    let mut bytes = bytes.to_vec();
    bytes.extend_from_slice(PADDING.as_bytes());

    let (frame, len) = match decode(&mut bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &[u8]) {
    let mut bytes = bytes.to_vec();
    let (frame, len) = match decode(&mut bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => return pretty_print_panic(e),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (Some(OwnedFrame::Integer(48293)), 8);
    let bytes: Vec<u8> = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(OwnedFrame::SimpleString("string".into())), 9);
    let bytes: Vec<u8> = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (Some(OwnedFrame::SimpleString("string".into())), 9);
    let bytes: Vec<u8> = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(OwnedFrame::BulkString("foo".into())), 9);
    let bytes: Vec<u8> = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (Some(OwnedFrame::BulkString("foo".into())), 9);
    let bytes: Vec<u8> = "$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(OwnedFrame::Array(vec![
        OwnedFrame::SimpleString("Foo".into()),
        OwnedFrame::SimpleString("Bar".into()),
      ])),
      16,
    );
    let bytes: Vec<u8> = "*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let bytes: Vec<u8> = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();

    let expected = (
      Some(OwnedFrame::Array(vec![
        OwnedFrame::BulkString("Foo".into()),
        OwnedFrame::Null,
        OwnedFrame::BulkString("Bar".into()),
      ])),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let bytes: Vec<u8> = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(OwnedFrame::Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
      )),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let bytes: Vec<u8> = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(OwnedFrame::Error("MOVED 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let bytes: Vec<u8> = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(OwnedFrame::Error("ASK 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_incomplete() {
    let bytes: Vec<u8> = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar".into();
    decode_and_verify_none(&bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    let bytes: Vec<u8> = "foobarbazwibblewobble".into();
    decode(&bytes).map_err(|e| pretty_print_panic(e)).unwrap();
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
mod bytes_tests {
  use super::*;
  use nom::AsBytes;

  pub const PADDING: &'static str = "FOOBARBAZ";

  pub fn pretty_print_panic(e: RedisProtocolError) {
    panic!("{:?}", e);
  }

  pub fn panic_no_decode() {
    panic!("Failed to decode bytes. None returned");
  }

  fn decode_and_verify_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    let total_len = bytes.len();

    let (frame, len, buf) = match decode_bytes_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
    assert_eq!(buf.len(), expected.1, "output buffer len matched");
    assert_eq!(buf.len() + bytes.len(), total_len, "total len matched");
  }

  fn decode_and_verify_padded_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    bytes.extend_from_slice(PADDING.as_bytes());
    let total_len = bytes.len();

    let (frame, len, buf) = match decode_bytes_mut(&mut bytes) {
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
    let (frame, len, buf) = match decode_bytes_mut(&mut bytes) {
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
    let expected = (Some(BytesFrame::Integer(48293)), 8);
    let bytes: Bytes = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(BytesFrame::SimpleString("string".into())), 9);
    let bytes: Bytes = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (Some(BytesFrame::SimpleString("string".into())), 9);
    let bytes: Bytes = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(BytesFrame::BulkString("foo".into())), 9);
    let bytes: Bytes = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (Some(BytesFrame::BulkString("foo".into())), 9);
    let bytes: Bytes = "$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(BytesFrame::Array(vec![
        BytesFrame::SimpleString("Foo".into()),
        BytesFrame::SimpleString("Bar".into()),
      ])),
      16,
    );
    let bytes: Bytes = "*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let bytes: Bytes = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();

    let expected = (
      Some(BytesFrame::Array(vec![
        BytesFrame::BulkString("Foo".into()),
        BytesFrame::Null,
        BytesFrame::BulkString("Bar".into()),
      ])),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let bytes: Bytes = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(BytesFrame::Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
      )),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let bytes: Bytes = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(BytesFrame::Error("MOVED 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let bytes: Bytes = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(BytesFrame::Error("ASK 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_incomplete() {
    let bytes: Bytes = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar".into();
    decode_and_verify_none(&bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    let mut bytes: BytesMut = "foobarbazwibblewobble".into();
    decode_bytes_mut(&mut bytes).map_err(|e| pretty_print_panic(e)).unwrap();
  }
}
