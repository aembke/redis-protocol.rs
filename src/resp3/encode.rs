//! Functions for encoding Frames into the RESP3 protocol.
//!
//! <https://github.com/antirez/RESP3/blob/master/spec.md>

use crate::resp3::types::*;
use crate::resp3::utils::{self as resp3_utils, BOOLEAN_ENCODE_LEN};
use crate::types::{RedisProtocolError, RedisProtocolErrorKind, CRLF};
use crate::utils;
use bytes::BytesMut;
use cookie_factory::GenError;
use std::collections::{HashMap, HashSet};

fn gen_simplestring<'a>(x: (&'a mut [u8], usize), data: &str) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::simplestring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleString.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_simpleerror<'a>(x: (&'a mut [u8], usize), data: &str) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::simplestring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleError.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_number<'a>(x: (&'a mut [u8], usize), data: &i64) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::number_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::Number.to_byte()) >> gen_slice!(data.to_string().as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_null(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
  encode_checks!(x, NULL.as_bytes().len());

  do_gen!(x, gen_slice!(NULL.as_bytes()))
}

fn gen_double<'a>(x: (&'a mut [u8], usize), data: &f64) -> Result<(&'a mut [u8], usize), GenError> {
  // NaN is checked here
  encode_checks!(x, resp3_utils::double_encode_len(data)?);
  let as_string = resp3_utils::f64_to_redis_string(data);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::Double.to_byte()) >> gen_slice!(as_string.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_boolean<'a>(x: (&'a mut [u8], usize), data: &bool) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, BOOLEAN_ENCODE_LEN);
  let data = if *data { BOOL_TRUE_BYTES } else { BOOL_FALSE_BYTES };

  do_gen!(x, gen_slice!(data.as_bytes()))
}

fn gen_bignumber<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::bignumber_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BigNumber.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_blobstring<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::blobstring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BlobString.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_bloberror<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::blobstring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BlobError.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_verbatimstring<'a>(
  x: (&'a mut [u8], usize),
  data: &str,
  format: &VerbatimStringFormat,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::verbatimstring_encode_len(data));

  do_gen!(
    x,
    gen_be_u8!(FrameKind::VerbatimString.to_byte())
      >> gen_slice!(data.as_bytes().len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(format.to_str().as_bytes())
      >> gen_be_u8!(VERBATIM_FORMAT_BYTE)
      >> gen_slice!(data.as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_array<'a>(x: (&'a mut [u8], usize), data: &Vec<Frame>) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::array_or_push_encode_len(data)?);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = attempt_encoding(x.0, x.1, frame)?;
  }

  Ok(x)
}

fn gen_map<'a>(x: (&'a mut [u8], usize), data: &HashMap<Frame, Frame>) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::map_encode_len(data)?);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Map.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for (key, value) in data.iter() {
    x = attempt_encoding(x.0, x.1, key)?;
    x = attempt_encoding(x.0, x.1, value)?;
  }

  Ok(x)
}

fn gen_set<'a>(x: (&'a mut [u8], usize), data: &HashSet<Frame>) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::set_encode_len(data)?);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Set.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = attempt_encoding(x.0, x.1, frame)?;
  }

  Ok(x)
}

fn gen_attribute<'a>(
  x: (&'a mut [u8], usize),
  data: &HashMap<Frame, Frame>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::map_encode_len(data)?);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Attribute.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for (key, value) in data.iter() {
    x = attempt_encoding(x.0, x.1, key)?;
    x = attempt_encoding(x.0, x.1, value)?;
  }

  Ok(x)
}

fn gen_push<'a>(x: (&'a mut [u8], usize), data: &Vec<Frame>) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::array_or_push_encode_len(data)?);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Push.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = attempt_encoding(x.0, x.1, frame)?;
  }

  Ok(x)
}

fn gen_hello<'a>(
  x: (&'a mut [u8], usize),
  version: &RespVersion,
  auth: &Option<Auth>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_checks!(x, resp3_utils::hello_encode_len(version, auth));

  let mut x = do_gen!(
    x,
    gen_slice!(HELLO.as_bytes()) >> gen_slice!(EMPTY_SPACE.as_bytes()) >> gen_be_u8!(version.to_byte())
  )?;

  if let Some(ref auth) = *auth {
    x = do_gen!(
      x,
      gen_slice!(EMPTY_SPACE.as_bytes())
        >> gen_slice!(AUTH.as_bytes())
        >> gen_slice!(EMPTY_SPACE.as_bytes())
        >> gen_slice!(auth.username.as_bytes())
        >> gen_slice!(EMPTY_SPACE.as_bytes())
        >> gen_slice!(auth.password.as_bytes())
    )?;
  }

  Ok(x)
}

fn attempt_encoding<'a>(buf: &'a mut [u8], offset: usize, frame: &Frame) -> Result<(&'a mut [u8], usize), GenError> {
  use crate::resp3::types::Frame::*;

  match *frame {
    Array(ref a) => gen_array((buf, offset), a),
    BlobString(ref b) => gen_blobstring((buf, offset), b),
    SimpleString(ref s) => gen_simplestring((buf, offset), s),
    SimpleError(ref s) => gen_simpleerror((buf, offset), s),
    Number(ref i) => gen_number((buf, offset), i),
    Null => gen_null((buf, offset)),
    Double(ref f) => gen_double((buf, offset), f),
    BlobError(ref b) => gen_bloberror((buf, offset), b),
    VerbatimString { ref data, ref format } => gen_verbatimstring((buf, offset), data, format),
    Boolean(ref b) => gen_boolean((buf, offset), b),
    Map(ref m) => gen_map((buf, offset), m),
    Set(ref s) => gen_set((buf, offset), s),
    Attribute(ref m) => gen_attribute((buf, offset), m),
    Push(ref a) => gen_push((buf, offset), a),
    Hello { ref version, ref auth } => gen_hello((buf, offset), version, auth),
    BigNumber(ref b) => gen_bignumber((buf, offset), b),
  }
}

/// Encoding functions for complete frames.
pub mod complete {
  use super::*;

  /// Attempt to encode a frame into `buf` at the provided `offset`.
  ///
  /// The caller is responsible for extending the buffer if a `RedisProtocolErrorKind::BufferTooSmall` is returned.
  pub fn encode(buf: &mut [u8], offset: usize, frame: &Frame) -> Result<usize, RedisProtocolError> {
    attempt_encoding(buf, offset, frame)
      .map(|(_, amt)| amt)
      .map_err(|e| e.into())
  }

  /// Attempt to encode a frame into `buf`, extending the buffer as needed.
  ///
  /// Returns the number of bytes encoded.
  pub fn encode_bytes(buf: &mut BytesMut, frame: &Frame) -> Result<usize, RedisProtocolError> {
    let offset = buf.len();

    loop {
      match attempt_encoding(buf, offset, frame) {
        Ok((_, amt)) => return Ok(amt),
        Err(GenError::BufferTooSmall(amt)) => utils::zero_extend(buf, amt),
        Err(e) => return Err(e.into()),
      }
    }
  }
}

/// Encoding functions for streaming blobs and aggregate types.
///
/// This interface is stateful due to the fact that the caller almost always needs to maintain some state outside of these functions. As a result it's up to the caller to manage the starting, middle, and ending state of data streams in order to call the correct functions in this module.
pub mod streaming {
  use super::*;

  fn gen_start_streaming_string(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
    encode_checks!(x, 4);

    do_gen!(
      x,
      gen_be_u8!(BLOB_STRING_BYTE) >> gen_be_u8!(STREAMED_LENGTH_BYTE) >> gen_slice!(CRLF.as_bytes())
    )
  }

  fn gen_streaming_string_chunk<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
    encode_checks!(x, resp3_utils::blobstring_encode_len(data));

    do_gen!(
      x,
      gen_be_u8!(CHUNKED_STRING_BYTE)
        >> gen_slice!(data.len().to_string().as_bytes())
        >> gen_slice!(CRLF.as_bytes())
        >> gen_slice!(data)
        >> gen_slice!(CRLF.as_bytes())
    )
  }

  fn gen_end_streaming_string(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
    encode_checks!(x, 4);

    do_gen!(x, gen_slice!(END_STREAM_STRING_BYTES.as_bytes()))
  }

  fn gen_start_streaming_aggregate_type<'a>(
    x: (&'a mut [u8], usize),
    kind: &FrameKind,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    if !kind.is_aggregate_type() {
      return Err(GenError::CustomError(3));
    }
    encode_checks!(x, 4);

    do_gen!(
      x,
      gen_be_u8!(kind.to_byte()) >> gen_be_u8!(STREAMED_LENGTH_BYTE) >> gen_slice!(CRLF.as_bytes())
    )
  }

  fn gen_end_streaming_aggregate_type(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
    encode_checks!(x, 3);

    do_gen!(x, gen_slice!(END_STREAM_AGGREGATE_BYTES.as_bytes()))
  }

  fn gen_streaming_inner_value_frame<'a>(
    x: (&'a mut [u8], usize),
    data: &Frame,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    attempt_encoding(x.0, x.1, data)
  }

  fn gen_streaming_inner_kv_pair_frames<'a>(
    x: (&'a mut [u8], usize),
    key: &Frame,
    value: &Frame,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    let x = attempt_encoding(x.0, x.1, key)?;
    attempt_encoding(x.0, x.1, value)
  }

  /// Encode the starting bytes for a streaming blob string.
  pub fn encode_start_streaming_string(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    gen_start_streaming_string((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the bytes making up one chunk of a streaming blob string.
  pub fn encode_streaming_string_chunk(
    buf: &mut [u8],
    offset: usize,
    data: &[u8],
  ) -> Result<usize, RedisProtocolError> {
    gen_streaming_string_chunk((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming blob string.
  pub fn encode_end_streaming_string(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    gen_end_streaming_string((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the starting bytes for a streaming aggregate type (array, set, or map).
  pub fn encode_start_streaming_aggregate_type(
    buf: &mut [u8],
    offset: usize,
    kind: &FrameKind,
  ) -> Result<usize, RedisProtocolError> {
    gen_start_streaming_aggregate_type((buf, offset), kind)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frame inside a streamed array or set.
  ///
  /// Use [encode_streaming_aggregate_type_inner_kv_pair] to encode a key-value pair inside a streaming map.
  pub fn encode_streaming_aggregate_type_inner_value(
    buf: &mut [u8],
    offset: usize,
    data: &Frame,
  ) -> Result<usize, RedisProtocolError> {
    gen_streaming_inner_value_frame((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frames that make up a key-value pair in a streamed map.
  pub fn encode_streaming_aggregate_type_inner_kv_pair<'a>(
    buf: &'a mut [u8],
    offset: usize,
    key: &Frame,
    value: &Frame,
  ) -> Result<usize, RedisProtocolError> {
    gen_streaming_inner_kv_pair_frames((buf, offset), key, value)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming aggregate type (array, set, or map).
  pub fn encode_end_streaming_aggregate_type(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    gen_end_streaming_aggregate_type((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// A wrapper function for automatically extending the input buffer while encoding frames with a different encoding function.
  ///
  /// ```rust
  /// extern crate bytes;
  ///
  /// use redis_protocol::resp3::encode::streaming::*;
  /// use redis_protocol::resp3::types::{Frame, FrameKind};
  /// use bytes::BytesMut;
  /// use redis_protocol::types::RedisProtocolError;
  ///
  /// fn example(buf: &mut BytesMut) -> Result<usize, RedisProtocolError> {
  ///   // in many cases the starting buffer wont be empty, so this example shows how to track the offset as well
  ///   let mut offset = buf.len();
  ///   let frames = vec![Frame::Number(1), Frame::Number(2)];
  ///
  ///   offset += extend_while_encoding(buf, |buf| {
  ///     encode_start_streaming_aggregate_type(buf, offset, &FrameKind::Array)
  ///   })?;
  ///
  ///   for frame in frames.iter() {
  ///     offset += extend_while_encoding(buf, |buf| {
  ///       encode_streaming_aggregate_type_inner_value(buf, offset, frame)
  ///     })?;
  ///   }
  ///
  ///   offset += extend_while_encoding(buf, |buf| {
  ///     encode_end_streaming_aggregate_type(buf, offset)
  ///   })?;
  ///
  ///   println!("New buffer size: {}", offset);
  ///   Ok(offset)
  /// }
  /// ```
  ///
  /// Or a somewhat more practical example...
  ///
  /// ```edition2018 no_run
  /// #[macro_use]
  /// extern crate tokio;
  /// extern crate redis_protocol;
  /// extern crate bytes;
  ///
  /// use redis_protocol::resp3::encode::streaming::*;
  /// use redis_protocol::resp3::types::{Frame, FrameKind};
  /// use redis_protocol::types::RedisProtocolError;
  /// use bytes::BytesMut;
  /// use std::future::Future;
  /// use tokio::net::TcpStream;
  /// use tokio::io::{AsyncWrite, AsyncWriteExt};
  /// use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
  ///
  /// async fn write_all(socket: &mut TcpStream, buf: &mut BytesMut, offset: &mut usize) -> Result<usize, RedisProtocolError> {
  ///   // normally you'd use the Sink interface with Feed or Send here, but we'll handwave that for this example and flush the socket each time
  ///   let mut written = 0;
  ///   while written < buf.len() {
  ///     written += socket.write(&buf[written..]).await.expect("Failed to write to socket.");
  ///   }
  ///   // we could just clear the buffer here, but normally you dont flush the socket each time so you have to split the buf instead
  ///   let _ = buf.split_to(written);
  ///   *offset = 0;
  ///
  ///   Ok(written)
  /// }
  ///
  ///
  /// async fn stream_array(socket: &mut TcpStream, mut rx: UnboundedReceiver<Frame>) -> Result<(), RedisProtocolError> {
  ///   let mut buf = BytesMut::new();
  ///   let mut offset = buf.len();
  ///
  ///   offset += extend_while_encoding(&mut buf, |buf| {
  ///     encode_start_streaming_aggregate_type(buf, offset, &FrameKind::Array)
  ///   })?;
  ///   let mut written = write_all(socket, &mut buf, &mut offset).await.expect("Failed to write to socket.");
  ///
  ///   loop {
  ///     let frame = match rx.recv().await {
  ///        Some(frame) => frame,
  ///        None => break
  ///     };
  ///
  ///     offset += extend_while_encoding(&mut buf, |buf| {
  ///       encode_streaming_aggregate_type_inner_value(buf, offset, &frame)
  ///     })?;
  ///     written += write_all(socket, &mut buf, &mut offset).await.expect("Failed to write to socket.");
  ///   }
  ///
  ///   offset = extend_while_encoding(&mut buf, |buf| {
  ///     encode_end_streaming_aggregate_type(buf, offset)
  ///   })?;
  ///   written += write_all(socket, &mut buf, &mut offset).await.expect("Failed to write to socket.");
  ///
  ///   println!("Streamed {} bytes to the socket.", written);
  ///   Ok(())  
  /// }
  ///
  /// fn generate_frames(tx: UnboundedSender<Frame>) -> Result<(), RedisProtocolError> {
  ///   // read from another socket or generate frames somehow
  ///   unimplemented!()
  /// }
  ///
  /// #[tokio::main]
  /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
  ///   let (tx, rx) = unbounded_channel();
  ///   let mut socket = TcpStream::connect("127.0.0.1:6379").await.expect("Failed to connect");
  ///   
  ///   generate_frames(tx);
  ///   stream_array(&mut socket, rx).await.expect("Failed to stream array.");
  ///
  ///   Ok(())
  /// }
  ///
  /// ```
  pub fn extend_while_encoding<F>(buf: &mut BytesMut, func: F) -> Result<usize, RedisProtocolError>
  where
    F: Fn(&mut BytesMut) -> Result<usize, RedisProtocolError>,
  {
    loop {
      match func(buf) {
        Ok(amt) => return Ok(amt),
        Err(err) => match err.kind() {
          RedisProtocolErrorKind::BufferTooSmall(amt) => utils::zero_extend(buf, *amt),
          _ => return Err(err),
        },
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::ZEROED_KB;

  const PADDING: &'static str = "foobar";

  fn empty_bytes() -> BytesMut {
    BytesMut::new()
  }

  fn encode_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = empty_bytes();

    let len = match complete::encode_bytes(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_non_empty(input: &Frame, expected: &str) {
    let mut buf = empty_bytes();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = match complete::encode_bytes(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };
    let padded = vec![PADDING, expected].join("");

    assert_eq!(buf, padded.as_bytes(), "padded buf contents match");
    assert_eq!(len, padded.as_bytes().len(), "padded expected len is correct");
  }

  fn encode_raw_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = Vec::from(&ZEROED_KB[0..expected.as_bytes().len()]);

    let len = match complete::encode(&mut buf, 0, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };

    assert_eq!(buf, expected.as_bytes(), "empty buf contents match");
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  // ------------- tests adapted from RESP2 --------------------------

  #[test]
  fn should_encode_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("LLEN".into()),
      Frame::BlobString("mylist".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("INCR".into()),
      Frame::BlobString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("BITCOUNT".into()),
      Frame::BlobString("mykey".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("WATCH".into()),
      Frame::BlobString("WIBBLE".into()),
      Frame::BlobString("fooBARbaz".into()),
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n_\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("HSET".into()),
      Frame::BlobString("foo".into()),
      Frame::Null,
    ]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("LLEN".into()),
      Frame::BlobString("mylist".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("INCR".into()),
      Frame::BlobString("mykey".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("BITCOUNT".into()),
      Frame::BlobString("mykey".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("WATCH".into()),
      Frame::BlobString("WIBBLE".into()),
      Frame::BlobString("fooBARbaz".into()),
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n_\r\n";
    let input = Frame::Array(vec![
      Frame::BlobString("HSET".into()),
      Frame::BlobString("foo".into()),
      Frame::Null,
    ]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_moved_error() {
    let expected = "-MOVED 3999 127.0.0.1:6381\r\n";
    let input = Frame::SimpleError("MOVED 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_ask_error() {
    let expected = "-ASK 3999 127.0.0.1:6381\r\n";
    let input = Frame::SimpleError("ASK 3999 127.0.0.1:6381".into());

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_error() {
    let expected = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let input = Frame::SimpleError("WRONGTYPE Operation against a key holding the wrong kind of value".into());

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
  fn should_encode_number() {
    let i1_expected = ":1000\r\n";
    let i1_input = Frame::Number(1000);

    encode_and_verify_empty(&i1_input, i1_expected);
    encode_and_verify_non_empty(&i1_input, i1_expected);
  }

  #[test]
  fn should_encode_negative_number() {
    let i2_expected = ":-1000\r\n";
    let i2_input = Frame::Number(-1000);

    encode_and_verify_empty(&i2_input, i2_expected);
    encode_and_verify_non_empty(&i2_input, i2_expected);
  }

  // ------------- end tests adapted from RESP2 --------------------------

  // TODO bool, double, bulk strings, verbatim strings, bulk errors, push, map, set, attributes
}
