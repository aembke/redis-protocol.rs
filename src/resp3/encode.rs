//! Functions for encoding Frames into the RESP3 protocol.
//!
//! <https://github.com/antirez/RESP3/blob/master/spec.md>

use crate::resp3::types::*;
use crate::resp3::utils::{self as resp3_utils};
use crate::types::{RedisProtocolError, RedisProtocolErrorKind, CRLF};
use crate::utils;
use crate::alloc::string::ToString;
use alloc::vec::Vec;
use bytes::BytesMut;
use cookie_factory::GenError;

macro_rules! encode_attributes (
  ($x:ident, $attributes:ident) => {
    if let Some(ref attributes) = *$attributes {
      $x = gen_attribute($x, attributes)?;
    }
  }
);

fn gen_simplestring<'a>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleString.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_simpleerror<'a>(
  mut x: (&'a mut [u8], usize),
  data: &str,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleError.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_number<'a>(
  mut x: (&'a mut [u8], usize),
  data: &i64,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::Number.to_byte()) >> gen_slice!(data.to_string().as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_null(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
  do_gen!(x, gen_slice!(NULL.as_bytes()))
}

fn gen_double<'a>(
  mut x: (&'a mut [u8], usize),
  data: &f64,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  let as_string = resp3_utils::f64_to_redis_string(data);
  do_gen!(
    x,
    gen_be_u8!(FrameKind::Double.to_byte()) >> gen_slice!(as_string.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_boolean<'a>(
  mut x: (&'a mut [u8], usize),
  data: &bool,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  let data = if *data { BOOL_TRUE_BYTES } else { BOOL_FALSE_BYTES };
  do_gen!(x, gen_slice!(data.as_bytes()))
}

fn gen_bignumber<'a>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BigNumber.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_blobstring<'a>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BlobString.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_bloberror<'a>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

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
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  format: &VerbatimStringFormat,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);
  let total_len = format.encode_len() + data.len();

  do_gen!(
    x,
    gen_be_u8!(FrameKind::VerbatimString.to_byte())
      >> gen_slice!(total_len.to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(format.to_str().as_bytes())
      >> gen_be_u8!(VERBATIM_FORMAT_BYTE)
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_array<'a>(
  mut x: (&'a mut [u8], usize),
  data: &Vec<Frame>,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

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

fn gen_map<'a>(
  mut x: (&'a mut [u8], usize),
  data: &FrameMap,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
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

fn gen_set<'a>(
  mut x: (&'a mut [u8], usize),
  data: &FrameSet,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
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

fn gen_attribute<'a>(x: (&'a mut [u8], usize), data: &FrameMap) -> Result<(&'a mut [u8], usize), GenError> {
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

fn gen_push<'a>(
  mut x: (&'a mut [u8], usize),
  data: &Vec<Frame>,
  attributes: &Option<Attributes>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
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

  let x = do_gen!(x, gen_slice!(CRLF.as_bytes()))?;
  Ok(x)
}

fn gen_chunked_string<'a>(x: (&'a mut [u8], usize), data: &[u8]) -> Result<(&'a mut [u8], usize), GenError> {
  if data.is_empty() {
    // signal the end of the chunked stream
    do_gen!(x, gen_slice!(END_STREAM_STRING_BYTES.as_bytes()))
  } else {
    do_gen!(
      x,
      gen_be_u8!(FrameKind::ChunkedString.to_byte())
        >> gen_slice!(data.len().to_string().as_bytes())
        >> gen_slice!(CRLF.as_bytes())
        >> gen_slice!(data)
        >> gen_slice!(CRLF.as_bytes())
    )
  }
}

fn attempt_encoding<'a>(buf: &'a mut [u8], offset: usize, frame: &Frame) -> Result<(&'a mut [u8], usize), GenError> {
  use crate::resp3::types::Frame::*;

  let x = (buf, offset);
  let total_size = resp3_utils::encode_len(frame)?;
  trace!("Attempting to encode {:?} with total size {}", frame.kind(), total_size);
  encode_checks!(x, total_size);

  match *frame {
    Array {
      ref data,
      ref attributes,
    } => gen_array(x, data, attributes),
    BlobString {
      ref data,
      ref attributes,
    } => gen_blobstring(x, data, attributes),
    SimpleString {
      ref data,
      ref attributes,
    } => gen_simplestring(x, data, attributes),
    SimpleError {
      ref data,
      ref attributes,
    } => gen_simpleerror(x, data, attributes),
    Number {
      ref data,
      ref attributes,
    } => gen_number(x, data, attributes),
    Null => gen_null(x),
    Double {
      ref data,
      ref attributes,
    } => gen_double(x, data, attributes),
    BlobError {
      ref data,
      ref attributes,
    } => gen_bloberror(x, data, attributes),
    VerbatimString {
      ref data,
      ref format,
      ref attributes,
    } => gen_verbatimstring(x, data, format, attributes),
    Boolean {
      ref data,
      ref attributes,
    } => gen_boolean(x, data, attributes),
    Map {
      ref data,
      ref attributes,
    } => gen_map(x, data, attributes),
    Set {
      ref data,
      ref attributes,
    } => gen_set(x, data, attributes),
    Push {
      ref data,
      ref attributes,
    } => gen_push(x, data, attributes),
    Hello { ref version, ref auth } => gen_hello(x, version, auth),
    BigNumber {
      ref data,
      ref attributes,
    } => gen_bignumber(x, data, attributes),
    ChunkedString(ref b) => gen_chunked_string(x, b),
  }
}

/// Encoding functions for complete frames.
///
/// ```edition2018 no_run
/// # extern crate bytes;
/// # extern crate tokio;
///
/// # use redis_protocol::resp3::encode::complete::*;
/// # use redis_protocol::resp3::types::{Frame, FrameKind};
/// # use bytes::BytesMut;
/// # use tokio::net::TcpStream;
/// # use redis_protocol::types::RedisProtocolError;
/// # use tokio::io::AsyncWriteExt;
/// # use std::convert::TryInto;
///
/// async fn example(socket: &mut TcpStream, buf: &mut BytesMut) -> Result<(), RedisProtocolError> {
///   // in many cases the starting buffer wont be empty, so this example shows how to track the offset as well
///   let frame = Frame::BlobString { data: "foobarbaz".into(), attributes: None };
///   let offset = encode_bytes(buf, &frame).expect("Failed to encode frame");
///   
///   let _ = socket.write_all(&buf).await.expect("Failed to write to socket");
///   let _ = buf.split_to(offset);
///   Ok(())
/// }
/// ```
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
///
/// Examples:
///
/// ```rust
/// # extern crate bytes;
///
/// use redis_protocol::resp3::encode::streaming::*;
/// use redis_protocol::resp3::types::{Frame, FrameKind};
/// use bytes::BytesMut;
/// use redis_protocol::types::RedisProtocolError;
///
/// fn example(buf: &mut BytesMut) -> Result<usize, RedisProtocolError> {
///   // in many cases the starting buffer wont be empty, so this example shows how to track the offset as well
///   let mut offset = buf.len();
///   let frames: Vec<Frame> = vec![1.into(), 2.into()];
///
///   offset += extend_while_encoding(buf, |buf| {
///     encode_start_aggregate_type(buf, offset, &FrameKind::Array)
///   })?;
///
///   for frame in frames.iter() {
///     offset += extend_while_encoding(buf, |buf| {
///       encode_aggregate_type_inner_value(buf, offset, frame)
///     })?;
///   }
///
///   offset += extend_while_encoding(buf, |buf| {
///     encode_end_aggregate_type(buf, offset)
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
/// # #[macro_use]
/// # extern crate tokio;
/// # extern crate redis_protocol;
/// # extern crate bytes;
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
/// async fn write_all(socket: &mut TcpStream, buf: &mut BytesMut) -> Result<usize, RedisProtocolError> {
///   let len = buf.len();
///   socket.write_all(&buf).await.expect("Failed to write to socket.");
///   // we could just clear the buffer here since we use `write_all`, but in many cases callers don't flush the socket on each `write` call.
///   // in those scenarios the caller should split the buffer based on the result from `write`.
///   let _ = buf.split_to(len);
///
///   Ok(len)
/// }
///
/// /// Start a new array stream, sending frames received from `rx` out to `socket` and ending the stream when `rx` closes.
/// async fn stream_array(socket: &mut TcpStream, mut rx: UnboundedReceiver<Frame>) -> Result<(), RedisProtocolError> {
///   let mut buf = BytesMut::new();
///
///   let _ = extend_while_encoding(&mut buf, |buf| {
///     encode_start_aggregate_type(buf, 0, &FrameKind::Array)
///   })?;
///   let mut written = write_all(socket, &mut buf).await.expect("Failed to write to socket.");
///
///   loop {
///     let frame = match rx.recv().await {
///        Some(frame) => frame,
///        // break out of the loop when the channel closes
///        None => break
///     };
///
///     let _ = extend_while_encoding(&mut buf, |buf| {
///       encode_aggregate_type_inner_value(buf, 0, &frame)
///     })?;
///     written += write_all(socket, &mut buf).await.expect("Failed to write to socket.");
///   }
///
///   let _ = extend_while_encoding(&mut buf, |buf| {
///     encode_end_aggregate_type(buf, 0)
///   })?;
///   written += write_all(socket, &mut buf).await.expect("Failed to write to socket.");
///
///   println!("Streamed {} bytes to the socket.", written);
///   Ok(())  
/// }
///
/// fn generate_frames(tx: UnboundedSender<Frame>) -> Result<(), RedisProtocolError> {
///   // read from another socket or somehow generate frames, writing them to `tx`
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
  pub fn encode_start_string(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    gen_start_streaming_string((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the bytes making up one chunk of a streaming blob string.
  ///
  /// If `data` is empty this will do the same thing as [encode_end_string] to signal that the streamed string is finished.
  pub fn encode_string_chunk(buf: &mut [u8], offset: usize, data: &[u8]) -> Result<usize, RedisProtocolError> {
    gen_streaming_string_chunk((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming blob string.
  pub fn encode_end_string(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    gen_end_streaming_string((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the starting bytes for a streaming aggregate type (array, set, or map).
  pub fn encode_start_aggregate_type(
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
  /// Use [encode_aggregate_type_inner_kv_pair] to encode a key-value pair inside a streaming map.
  pub fn encode_aggregate_type_inner_value(
    buf: &mut [u8],
    offset: usize,
    data: &Frame,
  ) -> Result<usize, RedisProtocolError> {
    gen_streaming_inner_value_frame((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frames that make up a key-value pair in a streamed map.
  pub fn encode_aggregate_type_inner_kv_pair<'a>(
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
  pub fn encode_end_aggregate_type(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    gen_end_streaming_aggregate_type((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// A wrapper function for automatically extending the input buffer while encoding frames with a different encoding function.
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
  use itertools::Itertools;
  use std::convert::TryInto;
  use std::str;

  const PADDING: &'static str = "foobar";

  fn empty_bytes() -> BytesMut {
    BytesMut::new()
  }

  fn create_attributes() -> (FrameMap, Vec<u8>) {
    let mut out = resp3_utils::new_map(None);
    let key = Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    };
    let value = Frame::Number {
      data: 42,
      attributes: None,
    };
    out.insert(key, value);
    let encoded = "|1\r\n+foo\r\n:42\r\n".to_owned().into_bytes();

    (out, encoded)
  }

  fn blobstring_array(data: Vec<&'static str>) -> Frame {
    let inner: Vec<Frame> = data
      .into_iter()
      .map(|s| (FrameKind::BlobString, s).try_into().unwrap())
      .collect();

    Frame::Array {
      data: inner,
      attributes: None,
    }
  }

  fn push_frame_to_array(frame: &mut Frame, inner: Frame) {
    if let Frame::Array { ref mut data, .. } = frame {
      data.push(inner);
    }
  }

  fn unordered_assert_eq(data: BytesMut, expected_start: BytesMut, expected_middle: &[&str]) {
    let mut exptected_permutations = vec![];
    for middle_permutation in expected_middle.iter().permutations(expected_middle.len()) {
      let mut expected = expected_start.clone();
      for middle in middle_permutation {
        expected.extend_from_slice(middle.as_bytes())
      }
      exptected_permutations.push(expected);
    }

    assert!(
      exptected_permutations.contains(&data),
      "No middle permutations matched: data {:?} needs to match with one of the following {:#?}",
      data,
      exptected_permutations
    );
  }

  fn encode_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = empty_bytes();

    let len = match complete::encode_bytes(&mut buf, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };

    assert_eq!(
      buf,
      expected.as_bytes(),
      "empty buf contents match {:?} == {:?}",
      str::from_utf8(&buf),
      expected
    );
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_empty_unordered(input: &Frame, expected_start: &str, expected_middle: &[&str]) {
    let mut buf = empty_bytes();

    let len = complete::encode_bytes(&mut buf, input).unwrap();

    unordered_assert_eq(buf, BytesMut::from(expected_start.as_bytes()), expected_middle);

    let expected_middle_len: usize = expected_middle.iter().map(|x| x.as_bytes().len()).sum();
    assert_eq!(
      len,
      expected_start.as_bytes().len() + expected_middle_len,
      "empty expected len is correct"
    );
  }

  fn encode_and_verify_non_empty(input: &Frame, expected: &str) {
    let mut buf = empty_bytes();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::encode_bytes(&mut buf, input).unwrap();
    let padded = vec![PADDING, expected].join("");

    assert_eq!(
      buf,
      padded.as_bytes(),
      "padded buf contents match {:?} == {:?}",
      str::from_utf8(&buf),
      expected
    );
    assert_eq!(len, padded.as_bytes().len(), "padded expected len is correct");
  }

  fn encode_and_verify_non_empty_unordered(input: &Frame, expected_start: &str, expected_middle: &[&str]) {
    let mut buf = empty_bytes();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::encode_bytes(&mut buf, input).unwrap();
    let expected_start_padded = vec![PADDING, expected_start].join("");

    unordered_assert_eq(buf, BytesMut::from(expected_start_padded.as_bytes()), expected_middle);

    let expected_middle_len: usize = expected_middle.iter().map(|x| x.as_bytes().len()).sum();
    assert_eq!(
      len,
      expected_start_padded.as_bytes().len() + expected_middle_len,
      "padded expected len is correct"
    );
  }

  fn encode_raw_and_verify_empty(input: &Frame, expected: &str) {
    let mut buf = Vec::from(&ZEROED_KB[0..expected.as_bytes().len()]);

    let len = match complete::encode(&mut buf, 0, input) {
      Ok(l) => l,
      Err(e) => panic!("{:?}", e),
    };

    assert_eq!(
      buf,
      expected.as_bytes(),
      "empty buf contents match {:?} == {:?}",
      str::from_utf8(&buf),
      expected
    );
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_empty_with_attributes(input: &Frame, expected: &str) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();
    let mut buf = empty_bytes();

    let len = complete::encode_bytes(&mut buf, &frame).unwrap();

    let mut expected_bytes = empty_bytes();
    expected_bytes.extend_from_slice(&encoded_attributes);
    expected_bytes.extend_from_slice(expected.as_bytes());

    assert_eq!(buf, expected_bytes, "non empty buf contents match with attrs");
    assert_eq!(
      len,
      expected.as_bytes().len() + encoded_attributes.len(),
      "non empty expected len is correct with attrs"
    );
  }

  fn encode_and_verify_empty_with_attributes_unordered(input: &Frame, expected_start: &str, expected_middle: &[&str]) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();
    let mut buf = empty_bytes();

    let len = complete::encode_bytes(&mut buf, &frame).unwrap();

    let mut expected_start_bytes = empty_bytes();
    expected_start_bytes.extend_from_slice(&encoded_attributes);
    expected_start_bytes.extend_from_slice(expected_start.as_bytes());
    unordered_assert_eq(buf, expected_start_bytes, expected_middle);

    let expected_middle_len: usize = expected_middle.iter().map(|x| x.as_bytes().len()).sum();
    assert_eq!(
      len,
      expected_start.as_bytes().len() + expected_middle_len + encoded_attributes.len(),
      "non empty expected len is correct with attrs"
    );
  }

  fn encode_and_verify_non_empty_with_attributes(input: &Frame, expected: &str) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();

    let mut buf = empty_bytes();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::encode_bytes(&mut buf, &frame).unwrap();
    let mut expected_bytes = empty_bytes();
    expected_bytes.extend_from_slice(PADDING.as_bytes());
    expected_bytes.extend_from_slice(&encoded_attributes);
    expected_bytes.extend_from_slice(expected.as_bytes());

    assert_eq!(buf, expected_bytes, "empty buf contents match with attrs");
    assert_eq!(
      len,
      expected.as_bytes().len() + encoded_attributes.len() + PADDING.as_bytes().len(),
      "empty expected len is correct with attrs"
    );
  }

  fn encode_and_verify_non_empty_with_attributes_unordered(
    input: &Frame,
    expected_start: &str,
    expected_middle: &[&str],
  ) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();

    let mut buf = empty_bytes();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::encode_bytes(&mut buf, &frame).unwrap();
    let mut expected_start_bytes = empty_bytes();
    expected_start_bytes.extend_from_slice(PADDING.as_bytes());
    expected_start_bytes.extend_from_slice(&encoded_attributes);
    expected_start_bytes.extend_from_slice(expected_start.as_bytes());
    unordered_assert_eq(buf, expected_start_bytes, expected_middle);

    let expected_middle_len: usize = expected_middle.iter().map(|x| x.as_bytes().len()).sum();
    assert_eq!(
      len,
      expected_start.as_bytes().len() + expected_middle_len + encoded_attributes.len() + PADDING.as_bytes().len(),
      "empty expected len is correct with attrs"
    );
  }

  // ------------- tests adapted from RESP2 --------------------------

  #[test]
  fn should_encode_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = blobstring_array(vec!["LLEN", "mylist"]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = blobstring_array(vec!["INCR", "mykey"]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = blobstring_array(vec!["BITCOUNT", "mykey"]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = blobstring_array(vec!["WATCH", "WIBBLE", "fooBARbaz"]);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n_\r\n";
    let mut input = blobstring_array(vec!["HSET", "foo"]);
    push_frame_to_array(&mut input, Frame::Null);

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_llen_req_example() {
    let expected = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let input = blobstring_array(vec!["LLEN", "mylist"]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_incr_req_example() {
    let expected = "*2\r\n$4\r\nINCR\r\n$5\r\nmykey\r\n";
    let input = blobstring_array(vec!["INCR", "mykey"]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_bitcount_req_example() {
    let expected = "*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n";
    let input = blobstring_array(vec!["BITCOUNT", "mykey"]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_bulk_string_test() {
    let expected = "*3\r\n$5\r\nWATCH\r\n$6\r\nWIBBLE\r\n$9\r\nfooBARbaz\r\n";
    let input = blobstring_array(vec!["WATCH", "WIBBLE", "fooBARbaz"]);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_raw_array_null_test() {
    let expected = "*3\r\n$4\r\nHSET\r\n$3\r\nfoo\r\n_\r\n";
    let mut input = blobstring_array(vec!["HSET", "foo"]);
    push_frame_to_array(&mut input, Frame::Null);

    encode_raw_and_verify_empty(&input, expected);
  }

  #[test]
  fn should_encode_moved_error() {
    let expected = "-MOVED 3999 127.0.0.1:6381\r\n";
    let input = (FrameKind::SimpleError, "MOVED 3999 127.0.0.1:6381")
      .try_into()
      .unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_ask_error() {
    let expected = "-ASK 3999 127.0.0.1:6381\r\n";
    let input = (FrameKind::SimpleError, "ASK 3999 127.0.0.1:6381").try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_error() {
    let expected = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let input = (
      FrameKind::SimpleError,
      "WRONGTYPE Operation against a key holding the wrong kind of value",
    )
      .try_into()
      .unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_simplestring() {
    let expected = "+OK\r\n";
    let input = (FrameKind::SimpleString, "OK").try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_number() {
    let expected = ":1000\r\n";
    let input: Frame = 1000.into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_negative_number() {
    let expected = ":-1000\r\n";
    let input: Frame = (-1000 as i64).into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  // ------------- end tests adapted from RESP2 --------------------------

  #[test]
  fn should_encode_bool_true() {
    let expected = BOOL_TRUE_BYTES;
    let input: Frame = true.into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bool_false() {
    let expected = BOOL_FALSE_BYTES;
    let input: Frame = false.into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_double_positive() {
    let expected = ",12.34567\r\n";
    let input: Frame = 12.34567.try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_double_negative() {
    let expected = ",-12.34567\r\n";
    let input: Frame = (-12.34567).try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  #[should_panic]
  fn should_not_encode_double_nan() {
    // force NaN into the frame avoiding try_into, which also checks for NaN
    let input = Frame::Double {
      data: f64::NAN,
      attributes: None,
    };
    let mut buf = empty_bytes();
    let _ = complete::encode_bytes(&mut buf, &input).unwrap();
  }

  #[test]
  fn should_encode_double_inf() {
    let expected = ",inf\r\n";
    let input: Frame = f64::INFINITY.try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_double_neg_inf() {
    let expected = ",-inf\r\n";
    let input: Frame = f64::NEG_INFINITY.try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bignumber() {
    let expected = "(3492890328409238509324850943850943825024385\r\n";
    let input: Frame = (
      FrameKind::BigNumber,
      "3492890328409238509324850943850943825024385".as_bytes().to_vec(),
    )
      .try_into()
      .unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_null() {
    let expected = "_\r\n";
    let input = Frame::Null;

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_blobstring() {
    let expected = "$9\r\nfoobarbaz\r\n";
    let input: Frame = (FrameKind::BlobString, "foobarbaz").try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bloberror() {
    let expected = "!21\r\nSYNTAX invalid syntax\r\n";
    let input: Frame = (FrameKind::BlobError, "SYNTAX invalid syntax").try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_verbatimstring_txt() {
    let expected = "=15\r\ntxt:Some string\r\n";
    let input = Frame::VerbatimString {
      format: VerbatimStringFormat::Text,
      data: "Some string".as_bytes().into(),
      attributes: None,
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_verbatimstring_mkd() {
    let expected = "=15\r\nmkd:Some string\r\n";
    let input = Frame::VerbatimString {
      format: VerbatimStringFormat::Markdown,
      data: "Some string".as_bytes().into(),
      attributes: None,
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_push_pubsub() {
    let expected = ">4\r\n+pubsub\r\n+message\r\n+somechannel\r\n+this is the message\r\n";
    let input = Frame::Push {
      data: vec![
        (FrameKind::SimpleString, "pubsub").try_into().unwrap(),
        (FrameKind::SimpleString, "message").try_into().unwrap(),
        (FrameKind::SimpleString, "somechannel").try_into().unwrap(),
        (FrameKind::SimpleString, "this is the message").try_into().unwrap(),
      ],
      attributes: None,
    };

    assert!(input.is_pubsub_message());
    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_push_keyspace_event() {
    let expected = ">4\r\n+pubsub\r\n+message\r\n+__keyspace@0__:mykey\r\n+del\r\n";
    let input = Frame::Push {
      data: vec![
        (FrameKind::SimpleString, "pubsub").try_into().unwrap(),
        (FrameKind::SimpleString, "message").try_into().unwrap(),
        (FrameKind::SimpleString, "__keyspace@0__:mykey").try_into().unwrap(),
        (FrameKind::SimpleString, "del").try_into().unwrap(),
      ],
      attributes: None,
    };

    assert!(input.is_pubsub_message());
    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_simple_set() {
    let expected_start = "~5\r\n";
    let expected_middle = ["+orange\r\n", "+apple\r\n", "#t\r\n", ":100\r\n", ":999\r\n"];
    let mut inner = resp3_utils::new_set(None);
    let v1: Frame = (FrameKind::SimpleString, "orange").try_into().unwrap();
    let v2: Frame = (FrameKind::SimpleString, "apple").try_into().unwrap();
    let v3: Frame = true.into();
    let v4: Frame = 100.into();
    let v5: Frame = 999.into();

    inner.insert(v1);
    inner.insert(v2);
    inner.insert(v3);
    inner.insert(v4);
    inner.insert(v5);
    let input = Frame::Set {
      data: inner,
      attributes: None,
    };

    encode_and_verify_empty_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_non_empty_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_empty_with_attributes_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_non_empty_with_attributes_unordered(&input, expected_start, &expected_middle);
  }

  #[test]
  fn should_encode_simple_map() {
    let expected_start = "%2\r\n";
    let expected_middle = ["+first\r\n:1\r\n", "+second\r\n:2\r\n"];
    let mut inner = resp3_utils::new_map(None);
    let k1: Frame = (FrameKind::SimpleString, "first").try_into().unwrap();
    let v1: Frame = 1.into();
    let k2: Frame = (FrameKind::SimpleString, "second").try_into().unwrap();
    let v2: Frame = 2.into();

    inner.insert(k1, v1);
    inner.insert(k2, v2);
    let input = Frame::Map {
      data: inner,
      attributes: None,
    };

    encode_and_verify_empty_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_non_empty_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_empty_with_attributes_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_non_empty_with_attributes_unordered(&input, expected_start, &expected_middle);
  }

  #[test]
  fn should_encode_nested_map() {
    let expected_start = "%2\r\n";
    let expected_middle = ["+first\r\n:1\r\n", "+second\r\n%1\r\n+third\r\n:3\r\n"];
    let mut inner = resp3_utils::new_map(None);
    let k1: Frame = (FrameKind::SimpleString, "first").try_into().unwrap();
    let v1: Frame = 1.into();
    let k2: Frame = (FrameKind::SimpleString, "second").try_into().unwrap();
    let k3: Frame = (FrameKind::SimpleString, "third").try_into().unwrap();
    let v3: Frame = 3.into();

    let mut v2_inner = resp3_utils::new_map(None);
    v2_inner.insert(k3, v3);
    let v2 = Frame::Map {
      data: v2_inner,
      attributes: None,
    };

    inner.insert(k1, v1);
    inner.insert(k2, v2);
    let input = Frame::Map {
      data: inner,
      attributes: None,
    };

    encode_and_verify_empty_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_non_empty_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_empty_with_attributes_unordered(&input, expected_start, &expected_middle);
    encode_and_verify_non_empty_with_attributes_unordered(&input, expected_start, &expected_middle);
  }

  #[test]
  fn should_encode_hello() {
    let expected = "HELLO 3\r\n";
    let input = Frame::Hello {
      version: RespVersion::RESP3,
      auth: None,
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);

    let expected = "HELLO 2\r\n";
    let input = Frame::Hello {
      version: RespVersion::RESP2,
      auth: None,
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_hello_with_auth() {
    let expected = "HELLO 3 AUTH default mypassword\r\n";
    let input = Frame::Hello {
      version: RespVersion::RESP3,
      auth: Some(Auth {
        username: "default".into(),
        password: "mypassword".into(),
      }),
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_streaming_blobstring() {
    let expected = "$?\r\n;2\r\nhe\r\n;4\r\nllow\r\n;1\r\no\r\n;3\r\nrld\r\n;0\r\n";
    let chunk1 = "he";
    let chunk2 = "llow";
    let chunk3 = "o";
    let chunk4 = "rld";

    let mut buf = BytesMut::new();
    let mut offset = 0;

    offset = streaming::extend_while_encoding(&mut buf, |buf| streaming::encode_start_string(buf, offset)).unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_string_chunk(buf, offset, chunk1.as_bytes())
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_string_chunk(buf, offset, chunk2.as_bytes())
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_string_chunk(buf, offset, chunk3.as_bytes())
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_string_chunk(buf, offset, chunk4.as_bytes())
    })
    .unwrap();
    let _ = streaming::extend_while_encoding(&mut buf, |buf| streaming::encode_end_string(buf, offset)).unwrap();

    assert_eq!(buf, expected);
  }

  #[test]
  fn should_encode_streaming_array() {
    let expected = "*?\r\n:1\r\n+foo\r\n#f\r\n$9\r\nfoobarbaz\r\n.\r\n";
    let chunk1 = Frame::Number {
      data: 1,
      attributes: None,
    };
    let chunk2 = Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    };
    let chunk3 = Frame::Boolean {
      data: false,
      attributes: None,
    };
    let chunk4 = Frame::BlobString {
      data: "foobarbaz".as_bytes().into(),
      attributes: None,
    };

    let mut buf = BytesMut::new();
    let mut offset = 0;

    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_start_aggregate_type(buf, offset, &FrameKind::Array)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk1)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk2)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk3)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk4)
    })
    .unwrap();
    let _ =
      streaming::extend_while_encoding(&mut buf, |buf| streaming::encode_end_aggregate_type(buf, offset)).unwrap();

    assert_eq!(buf, expected);
  }

  #[test]
  fn should_encode_streaming_set() {
    let expected = "~?\r\n:1\r\n+foo\r\n#f\r\n$9\r\nfoobarbaz\r\n.\r\n";
    let chunk1 = Frame::Number {
      data: 1,
      attributes: None,
    };
    let chunk2 = Frame::SimpleString {
      data: "foo".into(),
      attributes: None,
    };
    let chunk3 = Frame::Boolean {
      data: false,
      attributes: None,
    };
    let chunk4 = Frame::BlobString {
      data: "foobarbaz".as_bytes().into(),
      attributes: None,
    };

    let mut buf = BytesMut::new();
    let mut offset = 0;

    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_start_aggregate_type(buf, offset, &FrameKind::Set)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk1)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk2)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk3)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_value(buf, offset, &chunk4)
    })
    .unwrap();
    let _ =
      streaming::extend_while_encoding(&mut buf, |buf| streaming::encode_end_aggregate_type(buf, offset)).unwrap();

    assert_eq!(buf, expected);
  }

  #[test]
  fn should_encode_streaming_map() {
    let expected = "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n";
    let k1 = Frame::SimpleString {
      data: "a".into(),
      attributes: None,
    };
    let v1 = Frame::Number {
      data: 1,
      attributes: None,
    };
    let k2 = Frame::SimpleString {
      data: "b".into(),
      attributes: None,
    };
    let v2 = Frame::Number {
      data: 2,
      attributes: None,
    };

    let mut buf = BytesMut::new();
    let mut offset = 0;

    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_start_aggregate_type(buf, offset, &FrameKind::Map)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_kv_pair(buf, offset, &k1, &v1)
    })
    .unwrap();
    offset = streaming::extend_while_encoding(&mut buf, |buf| {
      streaming::encode_aggregate_type_inner_kv_pair(buf, offset, &k2, &v2)
    })
    .unwrap();
    let _ =
      streaming::extend_while_encoding(&mut buf, |buf| streaming::encode_end_aggregate_type(buf, offset)).unwrap();

    assert_eq!(buf, expected);
  }
}
