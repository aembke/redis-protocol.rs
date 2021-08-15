//! Functions for decoding the RESP2 protocol into frames.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::resp2::types::*;
use crate::types::*;
use nom::number::streaming::be_u8;
use nom::Err as NomError;
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
  Frame::Error(s.to_owned())
}

fn isize_to_usize<'a>(s: isize) -> Result<usize, RedisProtocolError> {
  if s >= 0 {
    Ok(s as usize)
  } else {
    Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid length.",
    ))
  }
}

named!(read_to_crlf<&[u8]>, terminated!(take_until!(CRLF), take!(2)));

named!(read_to_crlf_s<&str>, map_res!(read_to_crlf, str::from_utf8));

named!(read_prefix_len<isize>, map_res!(read_to_crlf_s, to_isize));

named!(
  frame_type<FrameKind>,
  switch!(be_u8,
    SIMPLESTRING_BYTE => value!(FrameKind::SimpleString) |
    ERROR_BYTE        => value!(FrameKind::Error) |
    INTEGER_BYTE      => value!(FrameKind::Integer) |
    BULKSTRING_BYTE   => value!(FrameKind::BulkString) |
    ARRAY_BYTE        => value!(FrameKind::Array)
  )
);

named!(
  parse_simplestring<Frame>,
  do_parse!(data: read_to_crlf_s >> (Frame::SimpleString(data.to_owned())))
);

named!(
  parse_integer<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_i64) >> (Frame::Integer(data)))
);

// assumes the '$-1\r\n' has been consumed already, since nulls look like bulk strings until the length prefix is parsed,
// and parsing the length prefix consumes the trailing \r\n in the underlying `terminated!` call
named!(parse_null<Frame>, do_parse!((Frame::Null)));

named!(parse_error<Frame>, map!(read_to_crlf_s, map_error));

named_args!(parse_bulkstring(len: isize) <Frame>,
  do_parse!(
    d: terminated!(take!(len), take!(2)) >>
    (Frame::BulkString(Vec::from(d)))
  )
);

named!(
  parse_bulkstring_or_null<Frame>,
  switch!(read_prefix_len,
    NULL_LEN => call!(parse_null) |
    len      => call!(parse_bulkstring, len)
  )
);

named_args!(parse_array_frames(len: usize) <Vec<Frame>>, count!(parse_frame, len));

named!(
  parse_array<Frame>,
  switch!(read_prefix_len,
    NULL_LEN => call!(parse_null) |
    len      => do_parse!(
      size: map_res!(value!(len), isize_to_usize) >>
      frames: call!(parse_array_frames, size) >>
      (Frame::Array(frames))
    )
  )
);

named!(
  parse_frame<Frame>,
  switch!(frame_type,
    FrameKind::SimpleString => call!(parse_simplestring) |
    FrameKind::Error        => call!(parse_error) |
    FrameKind::Integer      => call!(parse_integer) |
    FrameKind::BulkString   => call!(parse_bulkstring_or_null) |
    FrameKind::Array        => call!(parse_array)
  )
);

/// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
///
/// If the byte slice contains an incomplete frame then `None` is returned.
pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, RedisProtocolError> {
  let len = buf.len();

  match parse_frame(buf) {
    Ok((remaining, frame)) => Ok(Some((frame, len - remaining.len()))),
    Err(NomError::Incomplete(_)) => Ok(None),
    Err(e) => Err(e.into()),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
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
