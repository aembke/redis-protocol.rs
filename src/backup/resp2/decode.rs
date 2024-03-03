//! Functions for decoding the RESP2 protocol into frames.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::nom_bytes::NomBytes;
use crate::resp2::types::*;
use crate::types::*;
use crate::utils;
use alloc::vec::Vec;
use bytes::Bytes;
use nom::bytes::streaming::{take as nom_take, take_until as nom_take_until};
use nom::multi::count as nom_count;
use nom::number::streaming::be_u8;
use nom::sequence::terminated as nom_terminated;
use nom::{Err as NomErr, IResult};
use core::str;

pub(crate) const NULL_LEN: isize = -1;

fn to_isize(s: &NomBytes) -> Result<isize, RedisParseError<NomBytes>> {
  str::from_utf8(s)?
    .parse::<isize>()
    .map_err(|_| RedisParseError::new_custom("to_isize", "Failed to parse as integer."))
}

fn to_i64(s: &NomBytes) -> Result<i64, RedisParseError<NomBytes>> {
  str::from_utf8(s)?
    .parse::<i64>()
    .map_err(|_| RedisParseError::new_custom("to_i64", "Failed to parse as integer."))
}

pub fn isize_to_usize<'a, T>(s: isize) -> Result<usize, RedisParseError<T>> {
  if s >= 0 {
    Ok(s as usize)
  } else {
    Err(RedisParseError::new_custom("isize_to_usize", "Invalid length."))
  }
}

fn d_read_to_crlf(input: &NomBytes) -> IResult<NomBytes, NomBytes, RedisParseError<NomBytes>> {
  decode_log!(input, "Parsing to CRLF. Remaining: {:?}", input);
  nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.clone())
}

fn d_read_to_crlf_s(input: &NomBytes) -> IResult<NomBytes, NomBytes, RedisParseError<NomBytes>> {
  let (input, data) = d_read_to_crlf(input)?;
  decode_log!(data, "Parsing to StrMut. Data: {:?}", data);
  Ok((input, data))
}

fn d_read_prefix_len(input: &NomBytes) -> IResult<NomBytes, isize, RedisParseError<NomBytes>> {
  let (input, data) = d_read_to_crlf_s(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(&data));
  Ok((input, etry!(to_isize(&data))))
}

fn d_frame_type(input: &NomBytes) -> IResult<NomBytes, FrameKind, RedisParseError<NomBytes>> {
  let (input, byte) = be_u8(input.clone())?;
  decode_log!(
    input,
    "Reading frame type. Kind byte: {:?}, remaining: {:?}",
    byte,
    input
  );
  let kind = match byte {
    SIMPLESTRING_BYTE => FrameKind::SimpleString,
    ERROR_BYTE => FrameKind::Error,
    INTEGER_BYTE => FrameKind::Integer,
    BULKSTRING_BYTE => FrameKind::BulkString,
    ARRAY_BYTE => FrameKind::Array,
    _ => e!(RedisParseError::new_custom("frame_type", "Invalid frame type.")),
  };

  Ok((input, kind))
}

fn d_parse_simplestring(input: &NomBytes) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  let (input, data) = d_read_to_crlf_s(input)?;
  Ok((input, Frame::SimpleString(data.into_bytes())))
}

fn d_parse_integer(input: &NomBytes) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  let (input, data) = d_read_to_crlf_s(input)?;
  let parsed = etry!(to_i64(&data));
  Ok((input, Frame::Integer(parsed)))
}

// assumes the '$-1\r\n' has been consumed already, since nulls look like bulk strings until the length prefix is parsed,
// and parsing the length prefix consumes the trailing \r\n in the underlying `terminated!` call
fn d_parse_null(input: &NomBytes) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  Ok((input.clone(), Frame::Null))
}

fn d_parse_error(input: &NomBytes) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  let (input, data) = d_read_to_crlf_s(input)?;
  let data = etry!(utils::to_byte_str(data));
  Ok((input, Frame::Error(data)))
}

fn d_parse_bulkstring(input: &NomBytes, len: usize) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input.clone())?;
  Ok((input.clone(), Frame::BulkString(data.into_bytes())))
}

fn d_parse_bulkstring_or_null(input: &NomBytes) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  let (input, len) = d_read_prefix_len(input)?;
  decode_log!(input, "Parsing bulkstring, Length: {:?}, remaining: {:?}", len, input);

  if len == NULL_LEN {
    d_parse_null(&input)
  } else {
    d_parse_bulkstring(&input, etry!(isize_to_usize(len)))
  }
}

fn d_parse_array_frames<T>(input: T, len: usize) -> IResult<NomBytes, Vec<Frame>, RedisParseError<NomBytes>>
where
  T: AsRef<NomBytes>,
{
  let input_ref = input.as_ref();
  decode_log!(
    input_ref,
    "Parsing array frames. Length: {:?}, remaining: {:?}",
    len,
    input_ref
  );
  nom_count(d_parse_frame, len)(input_ref.clone())
}

fn d_parse_array(input: &NomBytes) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>> {
  let (input, len) = d_read_prefix_len(input)?;
  decode_log!(input, "Parsing array. Length: {:?}, remaining: {:?}", len, input);

  if len == NULL_LEN {
    d_parse_null(&input)
  } else {
    let len = etry!(isize_to_usize(len));
    let (input, frames) = d_parse_array_frames(&input, len)?;
    Ok((input, Frame::Array(frames)))
  }
}

fn d_parse_frame<T>(input: T) -> IResult<NomBytes, Frame, RedisParseError<NomBytes>>
where
  T: AsRef<NomBytes>,
{
  let (input, kind) = d_frame_type(input.as_ref())?;
  decode_log!(input, "Parsed kind: {:?}, remaining: {:?}", kind, input);

  match kind {
    FrameKind::SimpleString => d_parse_simplestring(&input),
    FrameKind::Error => d_parse_error(&input),
    FrameKind::Integer => d_parse_integer(&input),
    FrameKind::BulkString => d_parse_bulkstring_or_null(&input),
    FrameKind::Array => d_parse_array(&input),
    _ => e!(RedisParseError::new_custom("parse_frame", "Invalid frame kind.")),
  }
}

/// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
///
/// If the byte slice contains an incomplete frame then `None` is returned.
pub fn decode(buf: &Bytes) -> Result<Option<(Frame, usize)>, RedisProtocolError> {
  let len = buf.len();
  let buffer: NomBytes = buf.into();

  match d_parse_frame(buffer) {
    Ok((remaining, frame)) => Ok(Some((frame, len - remaining.len()))),
    Err(NomErr::Incomplete(_)) => Ok(None),
    Err(NomErr::Error(e)) => Err(e.into()),
    Err(NomErr::Failure(e)) => Err(e.into()),
  }
}

#[cfg(feature = "decode-mut")]
#[cfg_attr(docsrs, doc(cfg(feature = "decode-mut")))]
pub use crate::decode_mut::resp2::decode_mut;

#[cfg(test)]
pub mod tests {
  use super::*;
  use bytes::BytesMut;
  use nom::AsBytes;
  use std::str;

  pub const PADDING: &'static str = "FOOBARBAZ";

  pub fn pretty_print_panic(e: RedisProtocolError) {
    panic!("{:?}", e);
  }

  pub fn panic_no_decode() {
    panic!("Failed to decode bytes. None returned");
  }

  fn decode_and_verify_some(bytes: &Bytes, expected: &(Option<Frame>, usize)) {
    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &Bytes, expected: &(Option<Frame>, usize)) {
    let mut buf = BytesMut::from(bytes.as_bytes());
    buf.extend_from_slice(PADDING.as_bytes());
    let buf = buf.freeze();

    let (frame, len) = match decode(&buf) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &Bytes) {
    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => return pretty_print_panic(e),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (Some(Frame::Integer(48293)), 8);
    let bytes: Bytes = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(Frame::SimpleString("string".into())), 9);
    let bytes: Bytes = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (Some(Frame::SimpleString("string".into())), 9);
    let bytes: Bytes = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(Frame::BulkString("foo".into())), 9);
    let bytes: Bytes = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (Some(Frame::BulkString("foo".into())), 9);
    let bytes: Bytes = "$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(Frame::Array(vec![
        Frame::SimpleString("Foo".into()),
        Frame::SimpleString("Bar".into()),
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
      Some(Frame::Array(vec![
        Frame::BulkString("Foo".into()),
        Frame::Null,
        Frame::BulkString("Bar".into()),
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
      Some(Frame::Error(
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
    let expected = (Some(Frame::Error("MOVED 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let bytes: Bytes = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(Frame::Error("ASK 3999 127.0.0.1:6381".into())), bytes.len());

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
    let bytes: Bytes = "foobarbazwibblewobble".into();
    let _ = decode(&bytes).map_err(|e| pretty_print_panic(e));
  }
}
