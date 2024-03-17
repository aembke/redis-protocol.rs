//! Functions for encoding Frames into the RESP3 protocol.
//!
//! <https://github.com/antirez/RESP3/blob/master/spec.md>

use crate::{
  error::{RedisProtocolError, RedisProtocolErrorKind},
  resp3::{
    types::*,
    utils::{self as resp3_utils},
  },
  types::CRLF,
  utils,
};
use cookie_factory::GenError;

#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};

enum BorrowedAttrs<'a> {
  Owned(&'a OwnedAttributes),
  #[cfg(feature = "bytes")]
  Bytes(&'a BytesAttributes),
}

impl<'a> From<&'a OwnedAttributes> for BorrowedAttrs<'a> {
  fn from(value: &'a OwnedAttributes) -> Self {
    BorrowedAttrs::Owned(value)
  }
}

#[cfg(feature = "bytes")]
impl<'a> From<&'a BytesAttributes> for BorrowedAttrs<'a> {
  fn from(value: &'a BytesAttributes) -> Self {
    BorrowedAttrs::Bytes(value)
  }
}

macro_rules! encode_attributes (
  ($x:ident, $attributes:ident) => {
    if let Some(attributes) = $attributes {
      let attributes: BorrowedAttrs = attributes.into();
      $x = match attributes {
        BorrowedAttrs::Owned(attrs) => gen_owned_attribute($x, attrs)?,
        #[cfg(feature = "bytes")]
        BorrowedAttrs::Bytes(attrs) => gen_bytes_attribute($x, attrs)?,
      };
    }
  }
);

fn gen_simplestring<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleString.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_simpleerror<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &str,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::SimpleError.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_number<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: i64,
  attributes: Option<A>,
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

fn gen_double<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: f64,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  let as_string = resp3_utils::f64_to_redis_string(data);
  do_gen!(
    x,
    gen_be_u8!(FrameKind::Double.to_byte()) >> gen_slice!(as_string.as_bytes()) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_boolean<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: bool,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  let data = if data { BOOL_TRUE_BYTES } else { BOOL_FALSE_BYTES };
  do_gen!(x, gen_slice!(data.as_bytes()))
}

fn gen_bignumber<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  do_gen!(
    x,
    gen_be_u8!(FrameKind::BigNumber.to_byte()) >> gen_slice!(data) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_blobstring<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: Option<A>,
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

fn gen_bloberror<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  attributes: Option<A>,
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

fn gen_verbatimstring<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[u8],
  format: &VerbatimStringFormat,
  attributes: Option<A>,
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

fn gen_owned_array<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[OwnedFrame],
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = gen_owned_frame(x.0, x.1, frame)?;
  }

  Ok(x)
}

#[cfg(feature = "bytes")]
fn gen_bytes_array<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[BytesFrame],
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Array.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = gen_bytes_frame(x.0, x.1, frame)?;
  }

  Ok(x)
}

fn gen_owned_map<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &FrameMap<OwnedFrame, OwnedFrame>,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Map.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for (key, value) in data.iter() {
    x = gen_owned_frame(x.0, x.1, key)?;
    x = gen_owned_frame(x.0, x.1, value)?;
  }

  Ok(x)
}

#[cfg(feature = "bytes")]
fn gen_bytes_map<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &FrameMap<BytesFrame, BytesFrame>,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Map.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for (key, value) in data.iter() {
    x = gen_bytes_frame(x.0, x.1, key)?;
    x = gen_bytes_frame(x.0, x.1, value)?;
  }

  Ok(x)
}

fn gen_owned_set<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &FrameSet<OwnedFrame>,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Set.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = gen_owned_frame(x.0, x.1, frame)?;
  }

  Ok(x)
}

#[cfg(feature = "bytes")]
fn gen_bytes_set<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &FrameSet<BytesFrame>,
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Set.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = gen_bytes_frame(x.0, x.1, frame)?;
  }

  Ok(x)
}

fn gen_owned_attribute<'a>(
  x: (&'a mut [u8], usize),
  data: &OwnedAttributes,
) -> Result<(&'a mut [u8], usize), GenError> {
  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Attribute.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for (key, value) in data.iter() {
    x = gen_owned_frame(x.0, x.1, key)?;
    x = gen_owned_frame(x.0, x.1, value)?;
  }

  Ok(x)
}

#[cfg(feature = "bytes")]
fn gen_bytes_attribute<'a>(
  x: (&'a mut [u8], usize),
  data: &BytesAttributes,
) -> Result<(&'a mut [u8], usize), GenError> {
  let mut x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Attribute.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for (key, value) in data.iter() {
    x = gen_bytes_frame(x.0, x.1, key)?;
    x = gen_bytes_frame(x.0, x.1, value)?;
  }

  Ok(x)
}

fn gen_owned_push<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[OwnedFrame],
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Push.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = gen_owned_frame(x.0, x.1, frame)?;
  }

  Ok(x)
}

#[cfg(feature = "bytes")]
fn gen_bytes_push<'a, 'b, A: Into<BorrowedAttrs<'b>>>(
  mut x: (&'a mut [u8], usize),
  data: &[BytesFrame],
  attributes: Option<A>,
) -> Result<(&'a mut [u8], usize), GenError> {
  encode_attributes!(x, attributes);

  x = do_gen!(
    x,
    gen_be_u8!(FrameKind::Push.to_byte())
      >> gen_slice!(data.len().to_string().as_bytes())
      >> gen_slice!(CRLF.as_bytes())
  )?;

  for frame in data.iter() {
    x = gen_bytes_frame(x.0, x.1, frame)?;
  }

  Ok(x)
}

fn gen_hello<'a>(
  x: (&'a mut [u8], usize),
  version: &RespVersion,
  username: Option<&str>,
  password: Option<&str>,
) -> Result<(&'a mut [u8], usize), GenError> {
  let mut x = do_gen!(
    x,
    gen_slice!(HELLO.as_bytes()) >> gen_slice!(EMPTY_SPACE.as_bytes()) >> gen_be_u8!(version.to_byte())
  )?;
  if username.is_some() || password.is_some() {
    x = do_gen!(x, gen_slice!(EMPTY_SPACE.as_bytes()) >> gen_slice!(AUTH.as_bytes()))?;
  }
  if let Some(username) = username {
    x = do_gen!(x, gen_slice!(EMPTY_SPACE.as_bytes()) >> gen_slice!(username.as_bytes()))?;
  }
  if let Some(password) = password {
    x = do_gen!(x, gen_slice!(EMPTY_SPACE.as_bytes()) >> gen_slice!(password.as_bytes()))?;
  }

  do_gen!(x, gen_slice!(CRLF.as_bytes()))
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

fn gen_owned_frame<'a>(
  buf: &'a mut [u8],
  offset: usize,
  frame: &OwnedFrame,
) -> Result<(&'a mut [u8], usize), GenError> {
  trace!("Encode {:?}, buf len: {}", frame.kind(), buf.len());
  let x = (buf, offset);

  match frame {
    OwnedFrame::Array { data, attributes } => gen_owned_array(x, &data, attributes.as_ref()),
    OwnedFrame::BlobString { data, attributes } => gen_blobstring(x, data, attributes.as_ref()),
    OwnedFrame::SimpleString { data, attributes } => gen_simplestring(x, data, attributes.as_ref()),
    OwnedFrame::SimpleError { data, attributes } => gen_simpleerror(x, data, attributes.as_ref()),
    OwnedFrame::Number { data, attributes } => gen_number(x, *data, attributes.as_ref()),
    OwnedFrame::Null => gen_null(x),
    OwnedFrame::Double { data, attributes } => gen_double(x, *data, attributes.as_ref()),
    OwnedFrame::BlobError { data, attributes } => gen_bloberror(x, data, attributes.as_ref()),
    OwnedFrame::VerbatimString {
      data,
      format,
      attributes,
    } => gen_verbatimstring(x, data, format, attributes.as_ref()),
    OwnedFrame::Boolean { data, attributes } => gen_boolean(x, *data, attributes.as_ref()),
    OwnedFrame::Map { data, attributes } => gen_owned_map(x, data, attributes.as_ref()),
    OwnedFrame::Set { data, attributes } => gen_owned_set(x, data, attributes.as_ref()),
    OwnedFrame::Push { data, attributes } => gen_owned_push(x, data, attributes.as_ref()),
    OwnedFrame::Hello {
      version,
      username,
      password,
    } => gen_hello(
      x,
      version,
      username.as_ref().map(|s| s.as_str()),
      password.as_ref().map(|s| s.as_str()),
    ),
    OwnedFrame::BigNumber { data, attributes } => gen_bignumber(x, data, attributes.as_ref()),
    OwnedFrame::ChunkedString(b) => gen_chunked_string(x, b),
  }
}

#[cfg(feature = "bytes")]
fn gen_bytes_frame<'a>(
  buf: &'a mut [u8],
  offset: usize,
  frame: &BytesFrame,
) -> Result<(&'a mut [u8], usize), GenError> {
  trace!("Encode {:?}, buf len: {}", frame.kind(), buf.len());
  let x = (buf, offset);

  match frame {
    BytesFrame::Array { data, attributes } => gen_bytes_array(x, data, attributes.as_ref()),
    BytesFrame::BlobString { data, attributes } => gen_blobstring(x, data, attributes.as_ref()),
    BytesFrame::SimpleString { data, attributes } => gen_simplestring(x, data, attributes.as_ref()),
    BytesFrame::SimpleError { data, attributes } => gen_simpleerror(x, data, attributes.as_ref()),
    BytesFrame::Number { data, attributes } => gen_number(x, *data, attributes.as_ref()),
    BytesFrame::Null => gen_null(x),
    BytesFrame::Double { data, attributes } => gen_double(x, *data, attributes.as_ref()),
    BytesFrame::BlobError { data, attributes } => gen_bloberror(x, data, attributes.as_ref()),
    BytesFrame::VerbatimString {
      data,
      format,
      attributes,
    } => gen_verbatimstring(x, data, format, attributes.as_ref()),
    BytesFrame::Boolean { data, attributes } => gen_boolean(x, *data, attributes.as_ref()),
    BytesFrame::Map { data, attributes } => gen_bytes_map(x, data, attributes.as_ref()),
    BytesFrame::Set { data, attributes } => gen_bytes_set(x, data, attributes.as_ref()),
    BytesFrame::Push { data, attributes } => gen_bytes_push(x, data, attributes.as_ref()),
    BytesFrame::Hello {
      version,
      username,
      password,
    } => gen_hello(
      x,
      version,
      username.as_ref().map(|s| s.as_ref()),
      password.as_ref().map(|s| s.as_ref()),
    ),
    BytesFrame::BigNumber { data, attributes } => gen_bignumber(x, data, attributes.as_ref()),
    BytesFrame::ChunkedString(b) => gen_chunked_string(x, b),
  }
}

/// Encoding functions for complete frames.
///
/// ## Examples
///
/// ### Using owned types:
///
/// ```rust
/// # use redis_protocol::resp3::encode::complete::*;
/// # use redis_protocol::resp3::types::{OwnedFrame, FrameKind, Resp3Frame};
/// use std::net::TcpStream;
/// # use std::io::Write;
/// fn example(socket: &mut TcpStream) {
///   // in many cases the starting buffer won't be empty, so this example shows how to track the offset as well
///   let frame = OwnedFrame::Array {
///     // send `HGET foo bar`
///     data: vec![
///       OwnedFrame::BlobString { data: "HGET".into(), attributes: None },
///       OwnedFrame::BlobString { data: "foo".into(), attributes: None },
///       OwnedFrame::BlobString { data: "bar".into(), attributes: None },
///     ],
///     attributes: None
///   };
///   let mut buf = Vec::with_capacity(frame.encode_len());
///   let amt = encode(&mut buf, &frame).expect("Failed to encode frame");
///   debug_assert_eq!(buf.len(), amt);
///
///   socket.write_all(&buf).expect("Failed to write to socket");
/// }
/// ```
///
/// ### Using bytes types with Tokio:
///
/// ```rust
/// # use redis_protocol::resp3::encode::complete::*;
/// # use redis_protocol::resp3::types::{BytesFrame, FrameKind};
/// # use bytes::BytesMut;
/// use tokio::net::TcpStream;
/// # use tokio::io::AsyncWriteExt;
/// async fn example(socket: &mut TcpStream, buf: &mut BytesMut) {
///   // in many cases the starting buffer won't be empty, so this example shows how to track the offset as well
///   let frame = BytesFrame::Array {
///     // send `HGET foo bar`
///     data: vec![
///       BytesFrame::BlobString { data: "HGET".into(), attributes: None },
///       BytesFrame::BlobString { data: "foo".into(), attributes: None },
///       BytesFrame::BlobString { data: "bar".into(), attributes: None },
///     ],
///     attributes: None
///   };
///   let amt = extend_encode(buf, &frame).expect("Failed to encode frame");
///
///   socket.write_all(&buf).await.expect("Failed to write to socket");
///   let _ = buf.split_to(amt);
/// }
/// ```
pub mod complete {
  use super::*;

  /// Attempt to encode a frame into `buf`.
  ///
  /// The caller is responsible for extending `buf` if a `BufferTooSmall` error is returned.
  pub fn encode(buf: &mut [u8], frame: &OwnedFrame) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, frame.encode_len());
    gen_owned_frame(buf, 0, frame).map(|(_, amt)| amt).map_err(|e| e.into())
  }

  /// Attempt to encode a frame into `buf`.
  ///
  /// The caller is responsible for extending `buf` if a `BufferTooSmall` error is returned.
  ///
  /// Returns the number of bytes encoded.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn encode_bytes(buf: &mut [u8], frame: &BytesFrame) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, frame.encode_len());
    gen_bytes_frame(buf, 0, frame).map(|(_, amt)| amt).map_err(|e| e.into())
  }

  /// Attempt to encode a frame into `buf`, extending the buffer as needed.
  ///
  /// Returns the number of bytes encoded.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn extend_encode(buf: &mut BytesMut, frame: &BytesFrame) -> Result<usize, RedisProtocolError> {
    let amt = frame.encode_len();
    let offset = buf.len();
    utils::zero_extend(buf, amt);

    gen_bytes_frame(buf, offset, frame)
      .map(|(_, amt)| amt)
      .map_err(|e| e.into())
  }
}

// TODO this wont work. need better way to extend before writing.
/// Encoding functions for streaming blobs and aggregate types.
///
/// ### Using `Bytes` and Tokio
///
/// Stream an array of frames via a Tokio unbounded channel.
///
/// ```rust
/// # use redis_protocol::{zero_extend, resp3::{encode::streaming::*, types::{BytesFrame, FrameKind, Resp3Frame}}, error::RedisProtocolError};
/// # use bytes::BytesMut;
/// # use std::{future::Future, time::Duration};
/// # use tokio::{net::TcpStream, time::sleep, io::{AsyncWrite, AsyncWriteExt}};
/// # use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
///
/// async fn write_all(socket: &mut TcpStream, buf: &mut BytesMut) -> usize {
///   let len = buf.len();
///   socket.write_all(&buf).await.expect("Failed to write to socket.");
///   // we could just clear the buffer here since we use `write_all`, but in many cases it's common to not flush the socket on
///   // each `write` call. in those scenarios the caller should split the buffer based on the result from `write`.
///   let _ = buf.split_to(len);
///   len
/// }
///
/// /// Start a new array stream, sending frames received from `rx` out to `socket` and ending the stream when `rx` closes.
/// async fn stream_array(socket: &mut TcpStream, mut rx: UnboundedReceiver<BytesFrame>) {
///   let mut buf = BytesMut::new();
///   let mut written = 0;
///
///   zero_extend(&mut buf, START_STREAM_ENCODE_LEN);
///   encode_start_aggregate_type(&mut buf, FrameKind::Array).unwrap();
///   written = write_all(socket, &mut buf).await;
///
///   while let Some(frame) = rx.recv().await {
///     zero_extend(&mut buf, frame.encode_len());
///     encode_bytes_aggregate_type_inner_value(&mut buf, &frame).unwrap();
///     written += write_all(socket, &mut buf).await;
///   }
///
///   zero_extend(&mut buf, END_STREAM_AGGREGATE_TYPE_ENCODE_LEN);
///   encode_end_aggregate_type(&mut buf).unwrap();
///   written += write_all(socket, &mut buf).await;
///
///   println!("Streamed {} bytes to the socket.", written);
/// }
///
/// async fn generate_frames(tx: UnboundedSender<BytesFrame>) {
///   // read from another socket or somehow generate frames, writing them to `tx`
///   sleep(Duration::from_secs(1)).await;
///   tx.send(BytesFrame::BlobString { data: "foo".into(), attributes: None }).unwrap();
///   sleep(Duration::from_secs(1)).await;
///   tx.send(BytesFrame::BlobString { data: "bar".into(), attributes: None }).unwrap();
///   sleep(Duration::from_secs(1)).await;
///   tx.send(BytesFrame::BlobString { data: "baz".into(), attributes: None }).unwrap();
/// }
///
/// #[tokio::main]
/// async fn main() {
///   let (tx, rx) = unbounded_channel();
///   let mut socket = TcpStream::connect("127.0.0.1:6379").await.expect("Failed to connect");
///
///   tokio::spawn(generate_frames(tx));
///   stream_array(&mut socket, rx).await;
/// }
/// ```
pub mod streaming {
  use super::*;

  /// Number of bytes needed to encode the prefix when starting a stream.
  pub const START_STREAM_ENCODE_LEN: usize = 4;
  /// Number of bytes needed to encode the terminating bytes after a blob string.
  pub const END_STREAM_STRING_ENCODE_LEN: usize = 4;
  /// Number of bytes needed to encode the terminating bytes after an aggregate type.
  pub const END_STREAM_AGGREGATE_TYPE_ENCODE_LEN: usize = 3;

  fn gen_start_streaming_string(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
    do_gen!(
      x,
      gen_be_u8!(BLOB_STRING_BYTE) >> gen_be_u8!(STREAMED_LENGTH_BYTE) >> gen_slice!(CRLF.as_bytes())
    )
  }

  fn gen_streaming_string_chunk<'a>(
    x: (&'a mut [u8], usize),
    data: &[u8],
  ) -> Result<(&'a mut [u8], usize), GenError> {
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
    do_gen!(x, gen_slice!(END_STREAM_STRING_BYTES.as_bytes()))
  }

  fn gen_start_streaming_aggregate_type(
    x: (&mut [u8], usize),
    kind: FrameKind,
  ) -> Result<(&mut [u8], usize), GenError> {
    do_gen!(
      x,
      gen_be_u8!(kind.to_byte()) >> gen_be_u8!(STREAMED_LENGTH_BYTE) >> gen_slice!(CRLF.as_bytes())
    )
  }

  fn gen_end_streaming_aggregate_type(x: (&mut [u8], usize)) -> Result<(&mut [u8], usize), GenError> {
    do_gen!(x, gen_slice!(END_STREAM_AGGREGATE_BYTES.as_bytes()))
  }

  fn gen_owned_streaming_inner_value_frame<'a>(
    x: (&'a mut [u8], usize),
    data: &OwnedFrame,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    gen_owned_frame(x.0, x.1, data)
  }

  fn gen_owned_streaming_inner_kv_pair_frames<'a>(
    x: (&'a mut [u8], usize),
    key: &OwnedFrame,
    value: &OwnedFrame,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    let x = gen_owned_frame(x.0, x.1, key)?;
    gen_owned_frame(x.0, x.1, value)
  }

  #[cfg(feature = "bytes")]
  fn gen_bytes_streaming_inner_value_frame<'a>(
    x: (&'a mut [u8], usize),
    data: &BytesFrame,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    gen_bytes_frame(x.0, x.1, data)
  }

  #[cfg(feature = "bytes")]
  fn gen_bytes_streaming_inner_kv_pair_frames<'a>(
    x: (&'a mut [u8], usize),
    key: &BytesFrame,
    value: &BytesFrame,
  ) -> Result<(&'a mut [u8], usize), GenError> {
    let x = gen_bytes_frame(x.0, x.1, key)?;
    gen_bytes_frame(x.0, x.1, value)
  }

  /// Encode the starting bytes in a streaming blob string.
  pub fn encode_start_string(buf: &mut [u8]) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, START_STREAM_ENCODE_LEN);

    gen_start_streaming_string((buf, 0))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the bytes making up one chunk of a streaming blob string.
  ///
  /// If `data` is empty this will do the same thing as [encode_end_string] to signal that the streamed string is
  /// finished.
  pub fn encode_string_chunk(buf: &mut [u8], data: &[u8]) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, resp3_utils::blobstring_encode_len(data));

    gen_streaming_string_chunk((buf, 0), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming blob string.
  pub fn encode_end_string(buf: &mut [u8]) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, END_STREAM_STRING_ENCODE_LEN);

    gen_end_streaming_string((buf, 0)).map(|(_, l)| l).map_err(|e| e.into())
  }

  /// Encode the starting bytes for a streaming aggregate type (array, set, or map).
  pub fn encode_start_aggregate_type(buf: &mut [u8], kind: FrameKind) -> Result<usize, RedisProtocolError> {
    if !kind.is_aggregate_type() {
      return Err(GenError::CustomError(3).into());
    }
    encode_checks!(buf, START_STREAM_ENCODE_LEN);

    gen_start_streaming_aggregate_type((buf, 0), kind)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frame inside a streamed array or set.
  ///
  /// Use [encode_owned_aggregate_type_inner_kv_pair] to encode a key-value pair inside a streaming map.
  pub fn encode_owned_aggregate_type_inner_value(
    buf: &mut [u8],
    data: &OwnedFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, data.encode_len());

    gen_owned_streaming_inner_value_frame((buf, 0), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frames that make up a key-value pair in a streamed map.
  pub fn encode_owned_aggregate_type_inner_kv_pair<'a>(
    buf: &'a mut [u8],
    key: &OwnedFrame,
    value: &OwnedFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, key.encode_len() + value.encode_len());

    gen_owned_streaming_inner_kv_pair_frames((buf, 0), key, value)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frame inside a streamed array or set.
  ///
  /// Use [encode_bytes_aggregate_type_inner_kv_pair] to encode a key-value pair inside a streaming map.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn encode_bytes_aggregate_type_inner_value(
    buf: &mut [u8],
    data: &BytesFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, data.encode_len());

    gen_bytes_streaming_inner_value_frame((buf, 0), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frames that make up a key-value pair in a streamed map.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn encode_bytes_aggregate_type_inner_kv_pair<'a>(
    buf: &'a mut [u8],
    key: &BytesFrame,
    value: &BytesFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, key.encode_len() + value.encode_len());

    gen_bytes_streaming_inner_kv_pair_frames((buf, 0), key, value)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming aggregate type (array, set, or map).
  pub fn encode_end_aggregate_type(buf: &mut [u8]) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, END_STREAM_AGGREGATE_TYPE_ENCODE_LEN);

    gen_end_streaming_aggregate_type((buf, 0))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }
}
