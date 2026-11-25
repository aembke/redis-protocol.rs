//! Functions for encoding frames into the RESP2 protocol.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::{
  error::RedisProtocolError,
  gen::{check_line, GenError, GenOut},
  int2dec,
  resp2::types::*,
  types::CRLF,
};
use core::str;

#[cfg(feature = "bytes")]
use crate::utils;
#[cfg(feature = "bytes")]
use bytes::BytesMut;

fn gen_simplestring<W: GenOut>(x: W, data: &[u8]) -> Result<W, GenError> {
  check_line(data)?;
  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleString.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_error<W: GenOut>(x: W, data: &str) -> Result<W, GenError> {
  check_line(data.as_bytes())?;
  do_gen!(
    x,
    gen_be_u8!(FrameKind::Error.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_integer<W: GenOut>(x: W, data: i64, as_bulkstring: bool) -> Result<W, GenError> {
  let (buf, buf_padding) = int2dec::i64_to_digits(data);

  if as_bulkstring {
    // a more optimized way to encode an i64 as a BulkString, which is how Redis expects integers in practice
    let encoded_len = buf.len() - buf_padding;
    let (len, len_padding) = int2dec::u64_to_digits(encoded_len as u64);

    do_gen!(
      x,
      gen_be_u8!(FrameKind::BulkString.to_byte())
        >> gen_slice!(&len[len_padding ..])
        >> gen_slice!(CRLF.as_bytes())
        >> gen_slice!(&buf[buf_padding ..])
        >> gen_slice!(CRLF.as_bytes())
    )
  } else {
    do_gen!(
      x,
      gen_be_u8!(FrameKind::Integer.to_byte()) >> gen_slice!(&buf[buf_padding ..]) >> gen_slice!(CRLF.as_bytes())
    )
  }
}

fn gen_bulkstring<W: GenOut>(x: W, data: &[u8]) -> Result<W, GenError> {
  let (len, padding) = int2dec::u64_to_digits(data.len() as u64);
  // reserve the whole blob, so a large payload doesn't grow the buffer to exactly its size and then again for the
  // trailing CRLF, copying the payload twice. see benches/reserve.rs.
  let x = x.reserve(1 + len.len() - padding + 2 + data.len() + 2);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BulkString.to_byte())
      >> gen_slice!(&len[padding ..])
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_null<W: GenOut>(x: W) -> Result<W, GenError> {
  do_gen!(x, gen_slice!(NULL.as_bytes()))
}

fn gen_null_array<W: GenOut>(x: W) -> Result<W, GenError> {
  do_gen!(x, gen_slice!(NULL_ARRAY.as_bytes()))
}

// The array encoding logic requires using one of the public frame types, but they each potentially require some
// overhead to convert between. Converting from owned to bytes requires moving and adding a small ref counting layer
// whereas converting the other direction most likely requires copying the contents. We could introduce a layer in the
// middle that uses `&[u8]` and `&str` as the backing types, but even that would require an added `Vec` in the
// intermediate frame's `Array` variant. For now, I'm just going to duplicate this function to avoid these tradeoffs.

fn gen_owned_array<W: GenOut>(x: W, data: &[OwnedFrame], int_as_bulkstring: bool) -> Result<W, GenError> {
  let (len, padding) = int2dec::u64_to_digits(data.len() as u64);
  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte()) >> gen_slice!(&len[padding ..]) >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = match frame {
      OwnedFrame::SimpleString(s) => gen_simplestring(x, s)?,
      OwnedFrame::BulkString(b) => gen_bulkstring(x, b)?,
      OwnedFrame::Null => gen_null(x)?,
      OwnedFrame::NullArray => gen_null_array(x)?,
      OwnedFrame::Error(s) => gen_error(x, s)?,
      OwnedFrame::Array(frames) => gen_owned_array(x, frames, int_as_bulkstring)?,
      OwnedFrame::Integer(i) => gen_integer(x, *i, int_as_bulkstring)?,
    };
  }

  // the trailing CRLF is added by the last inner value
  Ok(x)
}

fn gen_borrowed_array<W: GenOut>(x: W, data: &[BorrowedFrame], int_as_bulkstring: bool) -> Result<W, GenError> {
  let (len, padding) = int2dec::u64_to_digits(data.len() as u64);
  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte()) >> gen_slice!(&len[padding ..]) >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = match frame {
      BorrowedFrame::SimpleString(s) => gen_simplestring(x, s)?,
      BorrowedFrame::BulkString(b) => gen_bulkstring(x, b)?,
      BorrowedFrame::Null => gen_null(x)?,
      BorrowedFrame::NullArray => gen_null_array(x)?,
      BorrowedFrame::Error(s) => gen_error(x, s)?,
      BorrowedFrame::Array(frames) => gen_borrowed_array(x, frames, int_as_bulkstring)?,
      BorrowedFrame::Integer(i) => gen_integer(x, *i, int_as_bulkstring)?,
    };
  }

  // the trailing CRLF is added by the last inner value
  Ok(x)
}

#[cfg(feature = "bytes")]
fn gen_bytes_array<W: GenOut>(x: W, data: &[BytesFrame], int_as_bulkstring: bool) -> Result<W, GenError> {
  let (len, padding) = int2dec::u64_to_digits(data.len() as u64);
  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte()) >> gen_slice!(&len[padding ..]) >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = match frame {
      BytesFrame::SimpleString(ref s) => gen_simplestring(x, s)?,
      BytesFrame::BulkString(ref b) => gen_bulkstring(x, b)?,
      BytesFrame::Null => gen_null(x)?,
      BytesFrame::NullArray => gen_null_array(x)?,
      BytesFrame::Error(ref s) => gen_error(x, s)?,
      BytesFrame::Array(ref frames) => gen_bytes_array(x, frames, int_as_bulkstring)?,
      BytesFrame::Integer(ref i) => gen_integer(x, *i, int_as_bulkstring)?,
    };
  }

  // the trailing CRLF is added by the last inner value
  Ok(x)
}

fn gen_owned_frame<W: GenOut>(x: W, frame: &OwnedFrame, int_as_bulkstring: bool) -> Result<W, GenError> {
  match frame {
    OwnedFrame::BulkString(b) => gen_bulkstring(x, b),
    OwnedFrame::Null => gen_null(x),
    OwnedFrame::NullArray => gen_null_array(x),
    OwnedFrame::Array(frames) => gen_owned_array(x, frames, int_as_bulkstring),
    OwnedFrame::Error(s) => gen_error(x, s),
    OwnedFrame::SimpleString(s) => gen_simplestring(x, s),
    OwnedFrame::Integer(i) => gen_integer(x, *i, int_as_bulkstring),
  }
}

fn gen_borrowed_frame<W: GenOut>(x: W, frame: &BorrowedFrame, int_as_bulkstring: bool) -> Result<W, GenError> {
  match frame {
    BorrowedFrame::BulkString(b) => gen_bulkstring(x, b),
    BorrowedFrame::Null => gen_null(x),
    BorrowedFrame::NullArray => gen_null_array(x),
    BorrowedFrame::Array(frames) => gen_borrowed_array(x, frames, int_as_bulkstring),
    BorrowedFrame::Error(s) => gen_error(x, s),
    BorrowedFrame::SimpleString(s) => gen_simplestring(x, s),
    BorrowedFrame::Integer(i) => gen_integer(x, *i, int_as_bulkstring),
  }
}

#[cfg(feature = "bytes")]
fn gen_bytes_frame<W: GenOut>(x: W, frame: &BytesFrame, int_as_bulkstring: bool) -> Result<W, GenError> {
  match frame {
    BytesFrame::BulkString(b) => gen_bulkstring(x, b),
    BytesFrame::Null => gen_null(x),
    BytesFrame::NullArray => gen_null_array(x),
    BytesFrame::Array(frames) => gen_bytes_array(x, frames, int_as_bulkstring),
    BytesFrame::Error(s) => gen_error(x, s),
    BytesFrame::SimpleString(s) => gen_simplestring(x, s),
    BytesFrame::Integer(i) => gen_integer(x, *i, int_as_bulkstring),
  }
}

/// Attempt to encode a frame into `buf`.
///
/// The caller is responsible for extending `buf` if a `BufferTooSmall` error is returned.
///
/// Returns the number of bytes encoded.
pub fn encode(buf: &mut [u8], frame: &OwnedFrame, int_as_bulkstring: bool) -> Result<usize, RedisProtocolError> {
  encode_checks!(buf, 0, frame.encode_len(int_as_bulkstring));
  gen_owned_frame((buf, 0), frame, int_as_bulkstring)
    .map(|(_, l)| l)
    .map_err(|e| e.into())
}

/// Attempt to encode a borrowed frame into `buf`.
///
/// The caller is responsible for extending `buf` if a `BufferTooSmall` error is returned.
///
/// Returns the number of bytes encoded.
pub fn encode_borrowed(
  buf: &mut [u8],
  frame: &BorrowedFrame,
  int_as_bulkstring: bool,
) -> Result<usize, RedisProtocolError> {
  encode_checks!(buf, 0, frame.encode_len(int_as_bulkstring));
  gen_borrowed_frame((buf, 0), frame, int_as_bulkstring)
    .map(|(_, l)| l)
    .map_err(|e| e.into())
}

/// Attempt to encode a frame into `buf`.
///
/// The caller is responsible for extending `buf` if a `BufferTooSmall` error is returned.
///
/// Returns the number of bytes encoded.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn encode_bytes(
  buf: &mut [u8],
  frame: &BytesFrame,
  int_as_bulkstring: bool,
) -> Result<usize, RedisProtocolError> {
  encode_checks!(buf, 0, frame.encode_len(int_as_bulkstring));
  gen_bytes_frame((buf, 0), frame, int_as_bulkstring)
    .map(|(_, l)| l)
    .map_err(|e| e.into())
}

/// Attempt to encode a frame at the end of `buf`, which grows as needed.
///
/// Returns the number of bytes encoded.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn extend_encode(
  buf: &mut BytesMut,
  frame: &BytesFrame,
  int_as_bulkstring: bool,
) -> Result<usize, RedisProtocolError> {
  // the buffer grows as needed, which benchmarks showed is faster than computing `encode_len` first
  let amt = utils::extend_with(buf, 0, |buf| gen_bytes_frame(buf, frame, int_as_bulkstring).map(|_| ()))?;
  // every test that encodes a frame also checks `encode_len`, which the slice-based functions rely on
  #[cfg(test)]
  assert_eq!(
    amt,
    frame.encode_len(int_as_bulkstring),
    "encoded length doesn't match encode_len"
  );
  Ok(amt)
}

/// Attempt to encode a borrowed frame at the end of `buf`, which grows as needed.
///
/// Returns the number of bytes encoded.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn extend_encode_borrowed(
  buf: &mut BytesMut,
  frame: &BorrowedFrame,
  int_as_bulkstring: bool,
) -> Result<usize, RedisProtocolError> {
  // the buffer grows as needed, which benchmarks showed is faster than computing `encode_len` first
  let amt = utils::extend_with(buf, 0, |buf| {
    gen_borrowed_frame(buf, frame, int_as_bulkstring).map(|_| ())
  })?;
  // every test that encodes a frame also checks `encode_len`, which the slice-based functions rely on
  #[cfg(test)]
  assert_eq!(
    amt,
    frame.encode_len(int_as_bulkstring),
    "encoded length doesn't match encode_len"
  );
  Ok(amt)
}

/// Other ways `extend_encode` could reserve space, kept so `benches/reserve.rs` can compare them with the current
/// approach. This is compiled only with `--cfg redis_protocol_bench` and isn't part of the public interface.
#[cfg(all(feature = "bytes", redis_protocol_bench))]
#[doc(hidden)]
pub mod reserve_strategies {
  use super::*;

  /// Reserve the exact encoded length before encoding. This was the behavior before 7.0.
  pub fn extend_encode_exact(
    buf: &mut BytesMut,
    frame: &BytesFrame,
    flag: bool,
  ) -> Result<usize, RedisProtocolError> {
    utils::extend_with(buf, frame.encode_len(flag), |buf| {
      gen_bytes_frame(buf, frame, flag).map(|_| ())
    })
  }

  /// Reserve a cheap over-estimate before encoding.
  pub fn extend_encode_estimate(
    buf: &mut BytesMut,
    frame: &BytesFrame,
    flag: bool,
  ) -> Result<usize, RedisProtocolError> {
    utils::extend_with(buf, estimate(frame), |buf| {
      gen_bytes_frame(buf, frame, flag).map(|_| ())
    })
  }

  /// Estimate the encoded length without counting digits. Every length prefix and integer is assumed to need 20
  /// digits.
  fn estimate(frame: &BytesFrame) -> usize {
    match frame {
      BytesFrame::BulkString(data) => data.len() + 25,
      BytesFrame::SimpleString(data) => data.len() + 3,
      BytesFrame::Error(data) => data.len() + 3,
      BytesFrame::Integer(_) => 30,
      BytesFrame::Null | BytesFrame::NullArray => 5,
      BytesFrame::Array(frames) => 23 + frames.iter().map(estimate).sum::<usize>(),
    }
  }
}

// Regression tests duplicated for each frame type.

#[cfg(test)]
mod owned_tests {
  use super::*;

  fn encode_and_verify_empty(input: &OwnedFrame, expected: &str) {
    let mut buf = vec![0; expected.len()];
    let len = encode(&mut buf, input, false).unwrap();

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_buf_and_verify_empty(input: &OwnedFrame, expected: &str) {
    let mut buf = vec![0; expected.as_bytes().len()];
    let len = encode(&mut buf, input, false).unwrap();

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  #[test]
  fn should_encode_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("LLEN".into()),
      OwnedFrame::BulkString("mylist".into()),
    ]);

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("INCR".into()),
      OwnedFrame::BulkString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("BITCOUNT".into()),
      OwnedFrame::BulkString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("WATCH".into()),
      OwnedFrame::BulkString("WIBBLE".into()),
      OwnedFrame::BulkString("fooBARbaz".into()),
    ]);

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("HSET".into()),
      OwnedFrame::BulkString("foo".into()),
      OwnedFrame::Null,
    ]);

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("LLEN".into()),
      OwnedFrame::BulkString("mylist".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("INCR".into()),
      OwnedFrame::BulkString("mykey".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("BITCOUNT".into()),
      OwnedFrame::BulkString("mykey".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("WATCH".into()),
      OwnedFrame::BulkString("WIBBLE".into()),
      OwnedFrame::BulkString("fooBARbaz".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = OwnedFrame::Array(vec![
      OwnedFrame::BulkString("HSET".into()),
      OwnedFrame::BulkString("foo".into()),
      OwnedFrame::Null,
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_moved_error() {
    let expected = "-MOVED 3999 127.0.0.1:6381\r\n";
    let input = OwnedFrame::Error("MOVED 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_ask_error() {
    let expected = "-ASK 3999 127.0.0.1:6381\r\n";
    let input = OwnedFrame::Error("ASK 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_error() {
    let expected = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let input = OwnedFrame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into());

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_simplestring() {
    let expected = "+OK\r\n";
    let input = OwnedFrame::SimpleString("OK".into());

    encode_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_integer() {
    let i1_expected = ":1000\r\n";
    let i1_input = OwnedFrame::Integer(1000);

    encode_and_verify_empty(&i1_input, i1_expected);
  }

  #[test]
  fn should_encode_negative_integer() {
    let i2_expected = ":-1000\r\n";
    let i2_input = OwnedFrame::Integer(-1000);

    encode_and_verify_empty(&i2_input, i2_expected);
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
mod bytes_tests {
  use super::*;

  const PADDING: &str = "foobar";

  fn encode_and_verify_empty(input: &BytesFrame, expected: &str) {
    let mut buf = BytesMut::new();
    let len = extend_encode(&mut buf, input, false).unwrap();

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_non_empty(input: &BytesFrame, expected: &str) {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = extend_encode(&mut buf, input, false).unwrap();
    let padded = [PADDING, expected].join("");

    assert_eq!(buf, padded.as_bytes(), "padded buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "padded expected len is correct");
  }

  fn encode_buf_and_verify_empty(input: &BytesFrame, expected: &str) {
    let mut buf = vec![0; expected.len()];
    let len = encode_bytes(&mut buf, input, false).unwrap();

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  #[test]
  fn should_encode_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("LLEN".into()),
      BytesFrame::BulkString("mylist".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("INCR".into()),
      BytesFrame::BulkString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("BITCOUNT".into()),
      BytesFrame::BulkString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("WATCH".into()),
      BytesFrame::BulkString("WIBBLE".into()),
      BytesFrame::BulkString("fooBARbaz".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("HSET".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::Null,
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("LLEN".into()),
      BytesFrame::BulkString("mylist".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("INCR".into()),
      BytesFrame::BulkString("mykey".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("BITCOUNT".into()),
      BytesFrame::BulkString("mykey".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("WATCH".into()),
      BytesFrame::BulkString("WIBBLE".into()),
      BytesFrame::BulkString("fooBARbaz".into()),
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_buf_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n$-1\r\n";
    let input = BytesFrame::Array(vec![
      BytesFrame::BulkString("HSET".into()),
      BytesFrame::BulkString("foo".into()),
      BytesFrame::Null,
    ]);

    encode_buf_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_moved_error() {
    let expected = "-MOVED 3999 127.0.0.1:6381\r\n";
    let input = BytesFrame::Error("MOVED 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_ask_error() {
    let expected = "-ASK 3999 127.0.0.1:6381\r\n";
    let input = BytesFrame::Error("ASK 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_reject_crlf_in_simple_strings_and_errors() {
    for frame in [
      BytesFrame::SimpleString("OK\r\n:1".into()),
      BytesFrame::Error("ERR a\nb".into()),
      BytesFrame::Array(vec![BytesFrame::Integer(1), BytesFrame::SimpleString("a\rb".into())]),
    ] {
      let mut buf = BytesMut::from("foo");
      assert!(extend_encode(&mut buf, &frame, false).is_err(), "{frame:?}");
      // a partially encoded frame is removed
      assert_eq!(buf, "foo");
    }
  }

  #[test]
  fn should_encode_error() {
    let expected = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let input = BytesFrame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_simplestring() {
    let expected = "+OK\r\n";
    let input = BytesFrame::SimpleString("OK".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_integer() {
    let i1_expected = ":1000\r\n";
    let i1_input = BytesFrame::Integer(1000);

    encode_and_verify_empty(&i1_input, i1_expected);
    encode_and_verify_non_empty(&i1_input, i1_expected);
  }

  #[test]
  fn should_encode_negative_integer() {
    let i2_expected = ":-1000\r\n";
    let i2_input = BytesFrame::Integer(-1000);

    encode_and_verify_empty(&i2_input, i2_expected);
    encode_and_verify_non_empty(&i2_input, i2_expected);
  }

  #[test]
  fn should_encode_integer_as_bulkstring() {
    let i1_expected = "$4\r\n1000\r\n";
    let i1_input = BytesFrame::Integer(1000);

    let mut buf = BytesMut::new();
    let len = extend_encode(&mut buf, &i1_input, true).unwrap();
    assert_eq!(buf, i1_expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, i1_expected.as_bytes().len(), "empty expected len is correct");
  }

  #[test]
  fn should_encode_negative_integer_as_bulkstring() {
    let i2_expected = "$5\r\n-1000\r\n";
    let i2_input = BytesFrame::Integer(-1000);

    let mut buf = BytesMut::new();
    let len = extend_encode(&mut buf, &i2_input, true).unwrap();
    assert_eq!(buf, i2_expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, i2_expected.as_bytes().len(), "empty expected len is correct");

    // test base10 overflow with `-` prefixes in the length prefix calculation
    let i2_expected = "$10\r\n-999999999\r\n";
    let i2_input = BytesFrame::Integer(-999999999);

    let mut buf = BytesMut::new();
    let len = extend_encode(&mut buf, &i2_input, true).unwrap();
    assert_eq!(buf, i2_expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, i2_expected.as_bytes().len(), "empty expected len is correct");
  }
}

#[cfg(test)]
mod null_array_tests {
  use super::*;
  use alloc::vec;

  #[test]
  fn should_encode_null_array() {
    let mut buf = vec![0; NULL_ARRAY.len()];
    let len = encode(&mut buf, &OwnedFrame::NullArray, false).unwrap();
    assert_eq!(&buf[.. len], b"*-1\r\n");
    assert_eq!(OwnedFrame::NullArray.encode_len(false), NULL_ARRAY.len());

    let mut buf = vec![0; NULL_ARRAY.len()];
    let len = encode_borrowed(&mut buf, &BorrowedFrame::NullArray, false).unwrap();
    assert_eq!(&buf[.. len], b"*-1\r\n");
  }

  #[test]
  fn should_encode_null_array_inside_array() {
    let frame = OwnedFrame::Array(vec![OwnedFrame::NullArray, OwnedFrame::Null]);
    let mut buf = vec![0; frame.encode_len(false)];
    let len = encode(&mut buf, &frame, false).unwrap();
    assert_eq!(&buf[.. len], b"*2\r\n*-1\r\n$-1\r\n");
  }

  #[test]
  #[cfg(feature = "bytes")]
  fn should_encode_bytes_null_array() {
    let mut buf = BytesMut::new();
    extend_encode(&mut buf, &BytesFrame::NullArray, false).unwrap();
    assert_eq!(&buf[..], b"*-1\r\n");
  }
}
