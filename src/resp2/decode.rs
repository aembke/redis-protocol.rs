//! Functions for decoding the RESP2 protocol into frames.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::resp2::types::*;
use crate::types::*;
use bytes::{Bytes, BytesMut};
use nom::bytes::streaming::{take as nom_take, take_until as nom_take_until};
use nom::combinator::{map as nom_map, map_res as nom_map_res, opt as nom_opt};
use nom::multi::count as nom_count;
use nom::number::streaming::be_u8;
use nom::sequence::terminated as nom_terminated;
use nom::{Err as NomErr, IResult};
use std::num::ParseIntError;
use std::str;

const NULL_LEN: isize = -1;

fn to_isize(s: &str) -> Result<isize, ParseIntError> {
  s.parse::<isize>()
}

fn to_i64(s: &str) -> Result<i64, ParseIntError> {
  s.parse::<i64>()
}

fn map_error(s: &str) -> Frame {
  Frame::Error(s.into())
}

fn isize_to_usize<'a>(s: isize) -> Result<usize, RedisParseError<&'a BytesMut>> {
  if s >= 0 {
    Ok(s as usize)
  } else {
    Err(RedisParseError::new_custom("isize_to_usize", "Invalid length."))
  }
}

fn d_read_to_crlf(input: &mut BytesMut) -> IResult<&mut BytesMut, &mut BytesMut, RedisParseError<&BytesMut>> {
  nom_terminated(nom_take_until(CRLF), nom_take(2_usize))(input)
}

fn d_read_to_crlf_s(input: &mut BytesMut) -> IResult<&mut BytesMut, &str, RedisParseError<&BytesMut>> {
  nom_map_res(d_read_to_crlf, str::from_utf8)(input)
}

fn d_read_prefix_len(input: &mut BytesMut) -> IResult<&mut BytesMut, isize, RedisParseError<&BytesMut>> {
  nom_map_res(d_read_to_crlf_s, to_isize)(input)
}

fn d_frame_type(input: &mut BytesMut) -> IResult<&mut BytesMut, FrameKind, RedisParseError<&BytesMut>> {
  let (input, byte) = be_u8(input)?;
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

fn d_parse_simplestring(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  let (input, data) = d_read_to_crlf_s(input)?;
  Ok((input, Frame::SimpleString(data.into())))
}

fn d_parse_integer(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  let (input, data) = nom_map_res(d_read_to_crlf_s, to_i64)(input)?;
  Ok((input, Frame::Integer(data)))
}

// assumes the '$-1\r\n' has been consumed already, since nulls look like bulk strings until the length prefix is parsed,
// and parsing the length prefix consumes the trailing \r\n in the underlying `terminated!` call
fn d_parse_null(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  Ok((input, Frame::Null))
}

fn d_parse_error(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  nom_map(d_read_to_crlf_s, map_error)(input)
}

fn d_parse_bulkstring(input: &mut BytesMut, len: usize) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input)?;
  Ok((input, Frame::BulkString(data.into())))
}

fn d_parse_bulkstring_or_null(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  let (input, len) = d_read_prefix_len(input)?;
  if len == NULL_LEN {
    d_parse_null(input)
  } else {
    d_parse_bulkstring(input, etry!(isize_to_usize(len)))
  }
}

fn d_parse_array_frames(
  input: &mut BytesMut,
  len: usize,
) -> IResult<&mut BytesMut, Vec<Frame>, RedisParseError<&BytesMut>> {
  nom_count(d_parse_frame, len)(input)
}

fn d_parse_array(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  let (input, len) = d_read_prefix_len(input)?;
  if len == NULL_LEN {
    d_parse_null(input)
  } else {
    let len = etry!(isize_to_usize(len));
    let (input, frames) = d_parse_array_frames(input, len)?;
    Ok((input, Frame::Array(frames)))
  }
}

fn d_parse_frame(input: &mut BytesMut) -> IResult<&mut BytesMut, Frame, RedisParseError<&BytesMut>> {
  let mut len = input.len();
  let (input, kind) = d_frame_type(input)?;

  // need to move the cursor buffer by 1 byte (len - input.len()) amount

  match kind {
    FrameKind::SimpleString => d_parse_simplestring(input),
    FrameKind::Error => d_parse_error(input),
    FrameKind::Integer => d_parse_integer(input),
    FrameKind::BulkString => d_parse_bulkstring_or_null(input),
    FrameKind::Array => d_parse_array(input),
    _ => e!(RedisParseError::new_custom("parse_frame", "Invalid frame kind.")),
  }
}

/// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
///
/// If the byte slice contains an incomplete frame then `None` is returned.
///
/// The returned frame will contain
pub fn decode(buf: &BytesMut) -> Result<Option<(Frame, usize)>, RedisProtocolError> {
  let len = buf.len();
  // operate on a shallow clone with a different cursor than `buf` since the parser will split the buffer while parsing,
  // and if a frame is later found to be incomplete we won't affect the caller's buffer cursor
  let mut buffer = buf.clone();

  match d_parse_frame(&mut buffer) {
    Ok((remaining, frame)) => Ok(Some((frame, len - remaining.len()))),
    Err(NomErr::Incomplete(_)) => Ok(None),
    Err(NomErr::Error(e)) => Err(e.into()),
    Err(NomErr::Failure(e)) => Err(e.into()),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::BytesMut;
  use std::str;

  const PADDING: &'static str = "FOOBARBAZ";

  fn pretty_print_panic(e: RedisProtocolError) {
    panic!("{:?}", e);
  }

  fn panic_no_decode() {
    panic!("Failed to decode bytes. None returned");
  }

  fn decode_and_verify_some(bytes: &mut BytesMut, expected: &(Option<Frame>, usize)) {
    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &mut BytesMut, expected: &(Option<Frame>, usize)) {
    bytes.extend_from_slice(PADDING.as_bytes());

    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &mut BytesMut) {
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
    let mut bytes: BytesMut = ":48293\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(Frame::SimpleString("string".into())), 9);
    let mut bytes: BytesMut = "+string\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (Some(Frame::SimpleString("string".into())), 9);
    let mut bytes: BytesMut = "+stri".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(Frame::BulkString("foo".into())), 9);
    let mut bytes: BytesMut = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (Some(Frame::BulkString("foo".into())), 9);
    let mut bytes: BytesMut = "$3\r\nfo".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
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
    let mut bytes: BytesMut = "*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let mut bytes: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();

    let expected = (
      Some(Frame::Array(vec![
        Frame::BulkString("Foo".into()),
        Frame::Null,
        Frame::BulkString("Bar".into()),
      ])),
      bytes.len(),
    );

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let mut bytes: BytesMut = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(Frame::Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
      )),
      bytes.len(),
    );

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let mut bytes: BytesMut = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(Frame::Error("MOVED 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let mut bytes: BytesMut = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(Frame::Error("ASK 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_incomplete() {
    let mut bytes: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar".into();
    decode_and_verify_none(&mut bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    let bytes: BytesMut = "foobarbazwibblewobble".into();
    let _ = decode(&bytes).map_err(|e| pretty_print_panic(e));
  }
}
