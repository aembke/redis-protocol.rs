//! Functions for encoding Frames into the RESP2 protocol.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::resp2::types::*;
use crate::resp2::utils::{self as resp2_utils};
use crate::types::{RedisProtocolError, CRLF};
use crate::utils;
use crate::alloc::string::ToString;
use alloc::vec::Vec;
use bytes::BytesMut;
use cookie_factory::GenError;

fn gen_simplestring<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp2_utils::simplestring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleString.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_error<'a>(x: (&'a mut [u8], usize), data: &str) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp2_utils::error_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::Error.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_integer<'a>(x: (&'a mut [u8], usize), data: &i64) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp2_utils::integer_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::Integer.to_byte()) >> gen_slice!(data.to_string().as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_bulkstring<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp2_utils::bulkstring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BulkString.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_null(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
  encode_checks!(x, NULL.as_bytes().len());

  do_gen!(x, gen_slice!(NULL.as_bytes()))
}

fn gen_array<'a>(x: (&'a mut [u8], usize), data: &Vec<Frame>) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp2_utils::array_encode_len(data)?);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = match frame {
      Frame::SimpleString(ref s) => gen_simplestring(x, &s)?,
      Frame::BulkString(ref b) => gen_bulkstring(x, &b)?,
      Frame::Null => gen_null(x)?,
      Frame::Error(ref s) => gen_error(x, s)?,
      Frame::Array(ref frames) => gen_array(x, frames)?,
      Frame::Integer(ref i) => gen_integer(x, i)?,
    };
  }

  // no trailing CRLF here, the inner values add that
  Ok(x)
}

fn attempt_encoding(buf: &mut [u8], offset: usize, frame: &Frame) -> Result<usize, GenError> {
  match *frame {
    Frame::BulkString(ref b) => gen_bulkstring((buf, offset), b).map(|(_, l)| l),
    Frame::Null => gen_null((buf, offset)).map(|(_, l)| l),
    Frame::Array(ref frames) => gen_array((buf, offset), frames).map(|(_, l)| l),
    Frame::Error(ref s) => gen_error((buf, offset), s).map(|(_, l)| l),
    Frame::SimpleString(ref s) => gen_simplestring((buf, offset), s).map(|(_, l)| l),
    Frame::Integer(ref i) => gen_integer((buf, offset), i).map(|(_, l)| l),
  }
}

/// Attempt to encode a frame into `buf`, assuming a starting offset of 0.
///
/// The caller is responsible for extending the buffer if a `RedisProtocolErrorKind::BufferTooSmall` is returned.
pub fn encode(buf: &mut [u8], offset: usize, frame: &Frame) -> Result<usize, RedisProtocolError> {
  attempt_encoding(buf, offset, frame).map_err(|e| e.into())
}

/// Attempt to encode a frame into `buf`, extending the buffer as needed.
///
/// Returns the number of bytes encoded.
pub fn encode_bytes(buf: &mut BytesMut, frame: &Frame) -> Result<usize, RedisProtocolError> {
  let offset = buf.len();

  loop {
    match attempt_encoding(buf, offset, frame) {
      Ok(size) => return Ok(size),
      Err(e) => match e {
        GenError::BufferTooSmall(amt) => utils::zero_extend(buf, amt),
        _ => return Err(e.into()),
      },
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::*;

  const PADDING: &'static str = "foobar";

  fn encode_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = BytesMut::new();

    let len = match encode_bytes(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_non_empty(input: &Frame, expected: &str) {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = match encode_bytes(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };
    let padded = vec![PADDING, expected].join("");

    assert_eq!(buf, padded.as_bytes(), "padded buf contents match");
    assert_eq!(len, padded.as_bytes().len(), "padded expected len is correct");
  }

  fn encode_raw_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = Vec::from(&ZEROED_KB[0..expected.as_bytes().len()]);

    let len = match encode(&mut buf, 0, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  #[test]
  fn should_encode_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("LLEN".into()),
      Frame::BulkString("mylist".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("INCR".into()),
      Frame::BulkString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("BITCOUNT".into()),
      Frame::BulkString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("WATCH".into()),
      Frame::BulkString("WIBBLE".into()),
      Frame::BulkString("fooBARbaz".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("HSET".into()),
      Frame::BulkString("foo".into()),
      Frame::Null,
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("LLEN".into()),
      Frame::BulkString("mylist".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("INCR".into()),
      Frame::BulkString("mykey".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("BITCOUNT".into()),
      Frame::BulkString("mykey".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("WATCH".into()),
      Frame::BulkString("WIBBLE".into()),
      Frame::BulkString("fooBARbaz".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = Frame::Array(vec![
      Frame::BulkString("HSET".into()),
      Frame::BulkString("foo".into()),
      Frame::Null,
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_moved_error() {
    let expected = "-MOVED 3999 127.0.0.1:6381\r\n";
    let input = Frame::Error("MOVED 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_ask_error() {
    let expected = "-ASK 3999 127.0.0.1:6381\r\n";
    let input = Frame::Error("ASK 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_error() {
    let expected = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let input = Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_simplestring() {
    let expected = "+OK\r\n";
    let input = Frame::SimpleString("OK".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_integer() {
    let i1_expected = ":1000\r\n";
    let i1_input = Frame::Integer(1000);

    encode_and_verify_empty(&i1_input, i1_expected);
    encode_and_verify_non_empty(&i1_input, i1_expected);
  }

  #[test]
  fn should_encode_negative_integer() {
    let i2_expected = ":-1000\r\n";
    let i2_input = Frame::Integer(-1000);

    encode_and_verify_empty(&i2_input, i2_expected);
    encode_and_verify_non_empty(&i2_input, i2_expected);
  }
}
