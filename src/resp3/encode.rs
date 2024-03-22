//! Functions for encoding Frames into the RESP3 protocol.
//!
//! <https://github.com/antirez/RESP3/blob/master/spec.md>

use crate::{
  error::RedisProtocolError,
  resp3::{
    types::*,
    utils::{self as resp3_utils},
  },
  types::CRLF,
  utils,
};
use cookie_factory::GenError;

#[cfg(feature = "bytes")]
use bytes::BytesMut;

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
    OwnedFrame::Array { data, attributes } => gen_owned_array(x, data, attributes.as_ref()),
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
      username.as_ref().map(|s| &**s),
      password.as_ref().map(|s| &**s),
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
    encode_checks!(buf, 0, frame.encode_len());
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
    encode_checks!(buf, 0, frame.encode_len());
    gen_bytes_frame(buf, 0, frame).map(|(_, amt)| amt).map_err(|e| e.into())
  }

  /// Attempt to encode a frame at the end of `buf`, extending the buffer before encoding.
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

/// Encoding functions for streaming blobs and aggregate types.
///
/// ### Using `Bytes` and Tokio
///
/// Stream an array of frames via a Tokio unbounded channel.
///
/// ```rust no_run
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
///   encode_start_aggregate_type(&mut buf, 0, FrameKind::Array).unwrap();
///   written += write_all(socket, &mut buf).await;
///
///   while let Some(frame) = rx.recv().await {
///     zero_extend(&mut buf, frame.encode_len());
///     encode_bytes_aggregate_type_inner_value(&mut buf, 0, &frame).unwrap();
///     written += write_all(socket, &mut buf).await;
///   }
///
///   zero_extend(&mut buf, END_STREAM_AGGREGATE_TYPE_ENCODE_LEN);
///   encode_end_aggregate_type(&mut buf, 0).unwrap();
///   written += write_all(socket, &mut buf).await;
///
///   println!("Streamed {} bytes to the socket.", written);
/// }
///
/// async fn generate_frames(tx: UnboundedSender<BytesFrame>) {
///   // read from another socket or somehow generate frames
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
///   let mut socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
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
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_start_string(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, START_STREAM_ENCODE_LEN);

    gen_start_streaming_string((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the bytes making up one chunk of a streaming blob string.
  ///
  /// If `data` is empty this will do the same thing as [encode_end_string] to signal that the streamed string is
  /// finished.
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_string_chunk(buf: &mut [u8], offset: usize, data: &[u8]) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, resp3_utils::blobstring_encode_len(data));

    gen_streaming_string_chunk((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming blob string.
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_end_string(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, END_STREAM_STRING_ENCODE_LEN);

    gen_end_streaming_string((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the starting bytes for a streaming aggregate type (array, set, or map).
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_start_aggregate_type(
    buf: &mut [u8],
    offset: usize,
    kind: FrameKind,
  ) -> Result<usize, RedisProtocolError> {
    if !kind.is_aggregate_type() {
      return Err(GenError::CustomError(3).into());
    }
    encode_checks!(buf, offset, START_STREAM_ENCODE_LEN);

    gen_start_streaming_aggregate_type((buf, offset), kind)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frame inside a streamed array or set.
  ///
  /// Use [encode_owned_aggregate_type_inner_kv_pair] to encode a key-value pair inside a streaming map.
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_owned_aggregate_type_inner_value(
    buf: &mut [u8],
    offset: usize,
    data: &OwnedFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, data.encode_len());

    gen_owned_streaming_inner_value_frame((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frames that make up a key-value pair in a streamed map.
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_owned_aggregate_type_inner_kv_pair(
    buf: &mut [u8],
    offset: usize,
    key: &OwnedFrame,
    value: &OwnedFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, key.encode_len() + value.encode_len());

    gen_owned_streaming_inner_kv_pair_frames((buf, offset), key, value)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frame inside a streamed array or set.
  ///
  /// Use [encode_bytes_aggregate_type_inner_kv_pair] to encode a key-value pair inside a streaming map.
  ///
  /// Returns the new offset in `buf`.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn encode_bytes_aggregate_type_inner_value(
    buf: &mut [u8],
    offset: usize,
    data: &BytesFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, data.encode_len());

    gen_bytes_streaming_inner_value_frame((buf, offset), data)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the inner frames that make up a key-value pair in a streamed map.
  ///
  /// Returns the new offset in `buf`.
  #[cfg(feature = "bytes")]
  #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
  pub fn encode_bytes_aggregate_type_inner_kv_pair(
    buf: &mut [u8],
    offset: usize,
    key: &BytesFrame,
    value: &BytesFrame,
  ) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, key.encode_len() + value.encode_len());

    gen_bytes_streaming_inner_kv_pair_frames((buf, offset), key, value)
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }

  /// Encode the terminating bytes at the end of a streaming aggregate type (array, set, or map).
  ///
  /// Returns the new offset in `buf`.
  pub fn encode_end_aggregate_type(buf: &mut [u8], offset: usize) -> Result<usize, RedisProtocolError> {
    encode_checks!(buf, offset, END_STREAM_AGGREGATE_TYPE_ENCODE_LEN);

    gen_end_streaming_aggregate_type((buf, offset))
      .map(|(_, l)| l)
      .map_err(|e| e.into())
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
mod tests {
  use super::*;
  use itertools::Itertools;
  use std::{convert::TryInto, str};

  const PADDING: &'static str = "foobar";

  fn create_attributes() -> (FrameMap<BytesFrame, BytesFrame>, Vec<u8>) {
    let mut out = resp3_utils::new_map(0);
    let key = BytesFrame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    };
    let value = BytesFrame::Number {
      data:       42,
      attributes: None,
    };
    out.insert(key, value);
    let encoded = "|1\r\n+foo\r\n:42\r\n".to_owned().into_bytes();

    (out, encoded)
  }

  fn blobstring_array(data: Vec<&'static str>) -> BytesFrame {
    let inner: Vec<BytesFrame> = data
      .into_iter()
      .map(|s| (FrameKind::BlobString, s).try_into().unwrap())
      .collect();

    BytesFrame::Array {
      data:       inner,
      attributes: None,
    }
  }

  fn push_frame_to_array(frame: &mut BytesFrame, inner: BytesFrame) {
    if let BytesFrame::Array { ref mut data, .. } = frame {
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

  fn encode_and_verify_empty(input: &BytesFrame, expected: &str) {
    let mut buf = BytesMut::new();
    let len = complete::extend_encode(&mut buf, input).unwrap();

    assert_eq!(
      buf,
      expected.as_bytes(),
      "empty buf contents match {:?} == {:?}",
      str::from_utf8(&buf),
      expected
    );
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_empty_unordered(input: &BytesFrame, expected_start: &str, expected_middle: &[&str]) {
    let mut buf = BytesMut::new();
    let len = complete::extend_encode(&mut buf, input).unwrap();

    unordered_assert_eq(buf, BytesMut::from(expected_start.as_bytes()), expected_middle);
    let expected_middle_len: usize = expected_middle.iter().map(|x| x.as_bytes().len()).sum();
    assert_eq!(
      len,
      expected_start.as_bytes().len() + expected_middle_len,
      "empty expected len is correct"
    );
  }

  fn encode_and_verify_non_empty(input: &BytesFrame, expected: &str) {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::extend_encode(&mut buf, input).unwrap();
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

  fn encode_and_verify_non_empty_unordered(input: &BytesFrame, expected_start: &str, expected_middle: &[&str]) {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::extend_encode(&mut buf, input).unwrap();
    let expected_start_padded = vec![PADDING, expected_start].join("");

    unordered_assert_eq(buf, BytesMut::from(expected_start_padded.as_bytes()), expected_middle);
    let expected_middle_len: usize = expected_middle.iter().map(|x| x.as_bytes().len()).sum();
    assert_eq!(
      len,
      expected_start_padded.as_bytes().len() + expected_middle_len,
      "padded expected len is correct"
    );
  }

  fn encode_raw_and_verify_empty(input: &BytesFrame, expected: &str) {
    let mut buf = vec![0; expected.as_bytes().len()];
    let len = complete::encode_bytes(&mut buf, input).unwrap();

    assert_eq!(
      buf,
      expected.as_bytes(),
      "empty buf contents match {:?} == {:?}",
      str::from_utf8(&buf),
      expected
    );
    assert_eq!(len, expected.as_bytes().len(), "empty expected len is correct");
  }

  fn encode_and_verify_empty_with_attributes(input: &BytesFrame, expected: &str) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();
    let mut buf = BytesMut::new();
    let len = complete::extend_encode(&mut buf, &frame).unwrap();

    let mut expected_bytes = BytesMut::new();
    expected_bytes.extend_from_slice(&encoded_attributes);
    expected_bytes.extend_from_slice(expected.as_bytes());

    assert_eq!(buf, expected_bytes, "non empty buf contents match with attrs");
    assert_eq!(
      len,
      expected.as_bytes().len() + encoded_attributes.len(),
      "non empty expected len is correct with attrs"
    );
  }

  fn encode_and_verify_empty_with_attributes_unordered(
    input: &BytesFrame,
    expected_start: &str,
    expected_middle: &[&str],
  ) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();
    let mut buf = BytesMut::new();
    let len = complete::extend_encode(&mut buf, &frame).unwrap();

    let mut expected_start_bytes = BytesMut::new();
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

  fn encode_and_verify_non_empty_with_attributes(input: &BytesFrame, expected: &str) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();

    let mut buf = BytesMut::new();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::extend_encode(&mut buf, &frame).unwrap();
    let mut expected_bytes = BytesMut::new();
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
    input: &BytesFrame,
    expected_start: &str,
    expected_middle: &[&str],
  ) {
    let (attributes, encoded_attributes) = create_attributes();
    let mut frame = input.clone();
    let _ = frame.add_attributes(attributes).unwrap();

    let mut buf = BytesMut::new();
    buf.extend_from_slice(PADDING.as_bytes());

    let len = complete::extend_encode(&mut buf, &frame).unwrap();
    let mut expected_start_bytes = BytesMut::new();
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
    push_frame_to_array(&mut input, BytesFrame::Null);

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
    push_frame_to_array(&mut input, BytesFrame::Null);

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
    let input: BytesFrame = 1000.into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_negative_number() {
    let expected = ":-1000\r\n";
    let input: BytesFrame = (-1000 as i64).into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  // ------------- end tests adapted from RESP2 --------------------------

  #[test]
  fn should_encode_bool_true() {
    let expected = BOOL_TRUE_BYTES;
    let input: BytesFrame = true.into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bool_false() {
    let expected = BOOL_FALSE_BYTES;
    let input: BytesFrame = false.into();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_double_positive() {
    let expected = ",12.34567\r\n";
    let input: BytesFrame = 12.34567.try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_double_negative() {
    let expected = ",-12.34567\r\n";
    let input: BytesFrame = (-12.34567).try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  #[should_panic]
  fn should_not_encode_double_nan() {
    let input = BytesFrame::Double {
      data:       f64::NAN,
      attributes: None,
    };
    let mut buf = BytesMut::new();
    let _ = complete::encode_bytes(&mut buf, &input).unwrap();
  }

  #[test]
  fn should_encode_double_inf() {
    let expected = ",inf\r\n";
    let input: BytesFrame = f64::INFINITY.try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_double_neg_inf() {
    let expected = ",-inf\r\n";
    let input: BytesFrame = f64::NEG_INFINITY.try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bignumber() {
    let expected = "(3492890328409238509324850943850943825024385\r\n";
    let input: BytesFrame = (
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
    let input = BytesFrame::Null;

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_blobstring() {
    let expected = "$9\r\nfoobarbaz\r\n";
    let input: BytesFrame = (FrameKind::BlobString, "foobarbaz").try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_bloberror() {
    let expected = "!21\r\nSYNTAX invalid syntax\r\n";
    let input: BytesFrame = (FrameKind::BlobError, "SYNTAX invalid syntax").try_into().unwrap();

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_verbatimstring_txt() {
    let expected = "=15\r\ntxt:Some string\r\n";
    let input = BytesFrame::VerbatimString {
      format:     VerbatimStringFormat::Text,
      data:       "Some string".as_bytes().into(),
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
    let input = BytesFrame::VerbatimString {
      format:     VerbatimStringFormat::Markdown,
      data:       "Some string".as_bytes().into(),
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
    let input = BytesFrame::Push {
      data:       vec![
        (FrameKind::SimpleString, "pubsub").try_into().unwrap(),
        (FrameKind::SimpleString, "message").try_into().unwrap(),
        (FrameKind::SimpleString, "somechannel").try_into().unwrap(),
        (FrameKind::SimpleString, "this is the message").try_into().unwrap(),
      ],
      attributes: None,
    };

    assert!(input.is_normal_pubsub_message());
    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_push_keyspace_event() {
    let expected = ">4\r\n+pubsub\r\n+message\r\n+__keyspace@0__:mykey\r\n+del\r\n";
    let input = BytesFrame::Push {
      data:       vec![
        (FrameKind::SimpleString, "pubsub").try_into().unwrap(),
        (FrameKind::SimpleString, "message").try_into().unwrap(),
        (FrameKind::SimpleString, "__keyspace@0__:mykey").try_into().unwrap(),
        (FrameKind::SimpleString, "del").try_into().unwrap(),
      ],
      attributes: None,
    };

    assert!(input.is_normal_pubsub_message());
    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
    encode_and_verify_empty_with_attributes(&input, expected);
    encode_and_verify_non_empty_with_attributes(&input, expected);
  }

  #[test]
  fn should_encode_simple_set() {
    let expected_start = "~5\r\n";
    let expected_middle = ["+orange\r\n", "+apple\r\n", "#t\r\n", ":100\r\n", ":999\r\n"];
    let mut inner = resp3_utils::new_set(0);
    let v1: BytesFrame = (FrameKind::SimpleString, "orange").try_into().unwrap();
    let v2: BytesFrame = (FrameKind::SimpleString, "apple").try_into().unwrap();
    let v3: BytesFrame = true.into();
    let v4: BytesFrame = 100.into();
    let v5: BytesFrame = 999.into();

    inner.insert(v1);
    inner.insert(v2);
    inner.insert(v3);
    inner.insert(v4);
    inner.insert(v5);
    let input = BytesFrame::Set {
      data:       inner,
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
    let mut inner = resp3_utils::new_map(0);
    let k1: BytesFrame = (FrameKind::SimpleString, "first").try_into().unwrap();
    let v1: BytesFrame = 1.into();
    let k2: BytesFrame = (FrameKind::SimpleString, "second").try_into().unwrap();
    let v2: BytesFrame = 2.into();

    inner.insert(k1, v1);
    inner.insert(k2, v2);
    let input = BytesFrame::Map {
      data:       inner,
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
    let mut inner = resp3_utils::new_map(0);
    let k1: BytesFrame = (FrameKind::SimpleString, "first").try_into().unwrap();
    let v1: BytesFrame = 1.into();
    let k2: BytesFrame = (FrameKind::SimpleString, "second").try_into().unwrap();
    let k3: BytesFrame = (FrameKind::SimpleString, "third").try_into().unwrap();
    let v3: BytesFrame = 3.into();

    let mut v2_inner = resp3_utils::new_map(0);
    v2_inner.insert(k3, v3);
    let v2 = BytesFrame::Map {
      data:       v2_inner,
      attributes: None,
    };

    inner.insert(k1, v1);
    inner.insert(k2, v2);
    let input = BytesFrame::Map {
      data:       inner,
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
    let input = BytesFrame::Hello {
      version:  RespVersion::RESP3,
      username: None,
      password: None,
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);

    let expected = "HELLO 2\r\n";
    let input = BytesFrame::Hello {
      version:  RespVersion::RESP2,
      username: None,
      password: None,
    };

    encode_and_verify_empty(&input, expected);
    encode_and_verify_non_empty(&input, expected);
  }

  #[test]
  fn should_encode_hello_with_auth() {
    let expected = "HELLO 3 AUTH default mypassword\r\n";
    let input = BytesFrame::Hello {
      version:  RespVersion::RESP3,
      username: Some("default".into()),
      password: Some("mypassword".into()),
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
    utils::zero_extend(&mut buf, expected.as_bytes().len());
    let mut offset = 0;

    offset = streaming::encode_start_string(&mut buf, offset).unwrap();
    offset = streaming::encode_string_chunk(&mut buf, offset, chunk1.as_bytes()).unwrap();
    offset = streaming::encode_string_chunk(&mut buf, offset, chunk2.as_bytes()).unwrap();
    offset = streaming::encode_string_chunk(&mut buf, offset, chunk3.as_bytes()).unwrap();
    offset = streaming::encode_string_chunk(&mut buf, offset, chunk4.as_bytes()).unwrap();
    offset = streaming::encode_end_string(&mut buf, offset).unwrap();

    assert_eq!(offset, expected.as_bytes().len());
    assert_eq!(buf, expected);
  }

  #[test]
  fn should_encode_streaming_array() {
    let expected = "*?\r\n:1\r\n+foo\r\n#f\r\n$9\r\nfoobarbaz\r\n.\r\n";
    let chunk1 = BytesFrame::Number {
      data:       1,
      attributes: None,
    };
    let chunk2 = BytesFrame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    };
    let chunk3 = BytesFrame::Boolean {
      data:       false,
      attributes: None,
    };
    let chunk4 = BytesFrame::BlobString {
      data:       "foobarbaz".as_bytes().into(),
      attributes: None,
    };

    let mut buf = BytesMut::new();
    utils::zero_extend(&mut buf, expected.as_bytes().len());
    let mut offset = 0;

    offset = streaming::encode_start_aggregate_type(&mut buf, offset, FrameKind::Array).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk1).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk2).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk3).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk4).unwrap();
    offset = streaming::encode_end_aggregate_type(&mut buf, offset).unwrap();

    assert_eq!(offset, expected.as_bytes().len());
    assert_eq!(buf, expected);
  }

  #[test]
  fn should_encode_streaming_set() {
    let expected = "~?\r\n:1\r\n+foo\r\n#f\r\n$9\r\nfoobarbaz\r\n.\r\n";
    let chunk1 = BytesFrame::Number {
      data:       1,
      attributes: None,
    };
    let chunk2 = BytesFrame::SimpleString {
      data:       "foo".into(),
      attributes: None,
    };
    let chunk3 = BytesFrame::Boolean {
      data:       false,
      attributes: None,
    };
    let chunk4 = BytesFrame::BlobString {
      data:       "foobarbaz".as_bytes().into(),
      attributes: None,
    };

    let mut buf = BytesMut::new();
    utils::zero_extend(&mut buf, expected.as_bytes().len());
    let mut offset = 0;

    offset = streaming::encode_start_aggregate_type(&mut buf, offset, FrameKind::Set).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk1).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk2).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk3).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_value(&mut buf, offset, &chunk4).unwrap();
    offset = streaming::encode_end_aggregate_type(&mut buf, offset).unwrap();

    assert_eq!(offset, expected.as_bytes().len());
    assert_eq!(buf, expected);
  }

  #[test]
  fn should_encode_streaming_map() {
    let expected = "%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n";
    let k1 = BytesFrame::SimpleString {
      data:       "a".into(),
      attributes: None,
    };
    let v1 = BytesFrame::Number {
      data:       1,
      attributes: None,
    };
    let k2 = BytesFrame::SimpleString {
      data:       "b".into(),
      attributes: None,
    };
    let v2 = BytesFrame::Number {
      data:       2,
      attributes: None,
    };

    let mut buf = BytesMut::new();
    utils::zero_extend(&mut buf, expected.as_bytes().len());
    let mut offset = 0;

    offset = streaming::encode_start_aggregate_type(&mut buf, offset, FrameKind::Map).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_kv_pair(&mut buf, offset, &k1, &v1).unwrap();
    offset = streaming::encode_bytes_aggregate_type_inner_kv_pair(&mut buf, offset, &k2, &v2).unwrap();
    offset = streaming::encode_end_aggregate_type(&mut buf, offset).unwrap();

    assert_eq!(offset, expected.as_bytes().len());
    assert_eq!(buf, expected);
  }
}
