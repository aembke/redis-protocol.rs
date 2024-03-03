use crate::decode_mut::frame::{DResult, Resp2IndexFrame};
use crate::decode_mut::utils::range_to_bytes;
use crate::resp2::decode::{isize_to_usize, NULL_LEN};
use crate::resp2::types::{
  Frame as Resp2Frame, FrameKind, ARRAY_BYTE, BULKSTRING_BYTE, ERROR_BYTE, INTEGER_BYTE, SIMPLESTRING_BYTE,
};
use crate::types::{RedisParseError, RedisProtocolError, RedisProtocolErrorKind, CRLF};
use crate::alloc::string::ToString;
use alloc::vec::Vec;
use bytes::{Bytes, BytesMut};
use bytes_utils::Str;
use nom::bytes::streaming::{take as nom_take, take_until as nom_take_until};
use nom::multi::count as nom_count;
use nom::number::streaming::be_u8;
use nom::sequence::terminated as nom_terminated;
use nom::{AsBytes, Err as NomErr};
use core::str;

pub fn to_isize(s: &[u8]) -> Result<isize, RedisParseError<&[u8]>> {
  str::from_utf8(s)?
    .parse::<isize>()
    .map_err(|_| RedisParseError::new_custom("to_isize", "Failed to parse as integer."))
}

pub fn to_i64(s: &[u8]) -> Result<i64, RedisParseError<&[u8]>> {
  str::from_utf8(s)?
    .parse::<i64>()
    .map_err(|_| RedisParseError::new_custom("to_i64", "Failed to parse as integer."))
}

// TODO optimize this using the nom counting interfaces
fn d_read_to_crlf(input: (&[u8], usize)) -> DResult<usize> {
  decode_log!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data.len()))
}

fn d_read_to_crlf_take(input: (&[u8], usize)) -> DResult<&[u8]> {
  decode_log!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", input.0);
  let (input_bytes, data) = nom_terminated(nom_take_until(CRLF.as_bytes()), nom_take(2_usize))(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data))
}

fn d_read_prefix_len(input: (&[u8], usize)) -> DResult<isize> {
  let (input, data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok((input, etry!(to_isize(&data))))
}

fn d_frame_type(input: (&[u8], usize)) -> DResult<FrameKind> {
  let (input_bytes, byte) = be_u8(input.0)?;
  decode_log!(
    input_bytes,
    "Reading frame type. Kind byte: {:?}, remaining: {:?}",
    byte,
    input_bytes
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

fn d_parse_simplestring(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok((
    (input, next_offset),
    Resp2IndexFrame::SimpleString {
      start: offset,
      end: offset + len,
    },
  ))
}

fn d_parse_integer(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_i64(&data));
  Ok(((input, next_offset), Resp2IndexFrame::Integer(parsed)))
}

// assumes the '$-1\r\n' has been consumed already, since nulls look like bulk strings until the length prefix is parsed,
// and parsing the length prefix consumes the trailing \r\n in the underlying `terminated!` call
fn d_parse_null(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  Ok((input, Resp2IndexFrame::Null))
}

fn d_parse_error(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok((
    (input, next_offset),
    Resp2IndexFrame::Error {
      start: offset,
      end: offset + len,
    },
  ))
}

fn d_parse_bulkstring(input: (&[u8], usize), len: usize) -> DResult<Resp2IndexFrame> {
  let offset = input.1;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize))(input.0)?;
  Ok((
    (input, offset + len + 2),
    Resp2IndexFrame::BulkString {
      start: offset,
      end: offset + data.len(),
    },
  ))
}

fn d_parse_bulkstring_or_null(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  decode_log!(input, "Parsing bulkstring, Length: {:?}, remaining: {:?}", len, input);

  if len == NULL_LEN {
    d_parse_null((input, offset))
  } else {
    d_parse_bulkstring((input, offset), etry!(isize_to_usize(len)))
  }
}

fn d_parse_array_frames(input: (&[u8], usize), len: usize) -> DResult<Vec<Resp2IndexFrame>> {
  decode_log!(
    input.0,
    _input,
    "Parsing array frames. Length: {:?}, remaining: {:?}",
    len,
    input.0
  );
  nom_count(d_parse_frame, len)(input)
}

fn d_parse_array(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  decode_log!(input, "Parsing array. Length: {:?}, remaining: {:?}", len, input);

  if len == NULL_LEN {
    d_parse_null((input, offset))
  } else {
    let len = etry!(isize_to_usize(len));
    let ((input, offset), frames) = d_parse_array_frames((input, offset), len)?;
    Ok(((input, offset), Resp2IndexFrame::Array(frames)))
  }
}

fn d_parse_frame(input: (&[u8], usize)) -> DResult<Resp2IndexFrame> {
  let ((input, offset), kind) = d_frame_type(input)?;
  decode_log!(input, "Parsed kind: {:?}, remaining: {:?}", kind, input);

  match kind {
    FrameKind::SimpleString => d_parse_simplestring((input, offset)),
    FrameKind::Error => d_parse_error((input, offset)),
    FrameKind::Integer => d_parse_integer((input, offset)),
    FrameKind::BulkString => d_parse_bulkstring_or_null((input, offset)),
    FrameKind::Array => d_parse_array((input, offset)),
    _ => e!(RedisParseError::new_custom("parse_frame", "Invalid frame kind.")),
  }
}

fn build_bytes_frame(buf: &Bytes, frame: Resp2IndexFrame) -> Result<Resp2Frame, RedisProtocolError> {
  let out = match frame {
    Resp2IndexFrame::Error { start, end } => {
      let bytes = range_to_bytes(buf, start, end)?;
      Resp2Frame::Error(Str::from_inner(bytes)?)
    }
    Resp2IndexFrame::SimpleString { start, end } => {
      let bytes = range_to_bytes(buf, start, end)?;
      Resp2Frame::SimpleString(bytes)
    }
    Resp2IndexFrame::BulkString { start, end } => {
      let bytes = range_to_bytes(buf, start, end)?;
      Resp2Frame::BulkString(bytes)
    }
    Resp2IndexFrame::Integer(i) => Resp2Frame::Integer(i),
    Resp2IndexFrame::Null => Resp2Frame::Null,
    Resp2IndexFrame::Array(frames) => {
      let mut out = Vec::with_capacity(frames.len());
      for frame in frames.into_iter() {
        out.push(build_bytes_frame(buf, frame)?);
      }
      Resp2Frame::Array(out)
    }
  };

  Ok(out)
}

fn freeze_parse(
  buf: &mut BytesMut,
  frame: Resp2IndexFrame,
  amt: usize,
) -> Result<(Resp2Frame, usize, Bytes), RedisProtocolError> {
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

/// Attempt to parse the contents of `buf`, returning the first valid frame, the number of bytes consumed, and the frozen consumed bytes.
///
/// If the byte slice contains an incomplete frame then `None` is returned.
///
/// Unlike `decode` this function works on a mutable `BytesMut` buffer in order to parse frames without copying the inner buffer contents
/// and will automatically split off the consumed bytes before returning. If an error or `None` is returned the buffer will not be modified.
#[cfg_attr(docsrs, doc(cfg(feature = "decode-mut")))]
pub fn decode_mut(buf: &mut BytesMut) -> Result<Option<(Resp2Frame, usize, Bytes)>, RedisProtocolError> {
  let (offset, len) = (0, buf.len());

  let (frame, amt) = match d_parse_frame((buf.as_bytes(), offset)) {
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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::resp2::decode::tests::*;
  use crate::resp2::types::Frame;
  use nom::AsBytes;

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
    let mut bytes: BytesMut = "foobarbazwibblewobble".into();
    let _ = decode_mut(&mut bytes).map_err(|e| pretty_print_panic(e));
  }
}
