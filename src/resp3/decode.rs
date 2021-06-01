//! Functions for decoding the RESP3 protocol into frames.
//!
//! <https://github.com/antirez/RESP3/blob/master/spec.md>

use crate::resp3::types::*;
use crate::resp3::utils as resp3_utils;
use crate::types::*;
use crate::utils;
use bytes::BytesMut;
use nom::bytes::complete::take as nom_take;
use nom::combinator::{map as nom_map, map_res as nom_map_res};
use nom::error::{context, Error, ErrorKind};
use nom::number::streaming::be_u8;
use nom::sequence::terminated as nom_terminated;
use nom::Err;
use nom::{Err as NomError, IResult};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::num::{ParseFloatError, ParseIntError};
use std::slice::Chunks;
use std::str;
use std::string::FromUtf8Error;
use types::RedisProtocolErrorKind::DecodeError;

#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};

fn non_streaming_error<T>(data: T, kind: FrameKind) -> Result<T, RedisParseError> {
  Err(RedisParseError::new(
    "non_streaming_error",
    format!("Cannot decode streaming {:?}", kind),
    None,
  ))
}

fn map_complete_frame(frame: Frame) -> DecodedFrame {
  DecodedFrame::Complete(frame)
}

fn unwrap_complete_frame(frame: DecodedFrame) -> Result<Frame, RedisParseError> {
  frame
    .into_complete_frame()
    .map_err(|e| RedisParseError::new("unwrap_complete_frame", format!("{:?}", e), None))
}

fn to_usize(s: &str) -> Result<usize, RedisParseError> {
  s.parse::<usize>().map_err(|e| RedisParseError {
    input: vec![s.as_bytes()],
    kind: vec![ErrorKind::MapRes],
    context: "to_usize",
    message: Some(format!("{:?}", e)),
  })
}

fn to_isize(s: &str) -> Result<isize, RedisParseError> {
  if s == "?" {
    Ok(-1)
  } else {
    s.parse::<isize>().map_err(|e| RedisParseError {
      input: vec![s.as_bytes()],
      kind: vec![ErrorKind::MapRes],
      context: "to_isize",
      message: Some(format!("{:?}", e)),
    })
  }
}

fn isize_to_usize(n: isize) -> Result<usize, RedisParseError> {
  if n.is_negative() {
    Err(RedisParseError::new("isize_to_usize", "Invalid prefix length.", None))
  } else {
    Ok(n as usize)
  }
}

fn to_i64(s: &str) -> Result<i64, RedisParseError> {
  s.parse::<i64>().map_err(|e| RedisParseError {
    input: vec![s.as_bytes()],
    kind: vec![ErrorKind::MapRes],
    context: "to_i64",
    message: Some(format!("{:?}", e)),
  })
}

fn to_f64(s: &str) -> Result<f64, RedisParseError> {
  s.parse::<f64>().map_err(|e| RedisParseError {
    input: vec![s.as_bytes()],
    kind: vec![ErrorKind::MapRes],
    context: "to_f64",
    message: Some(format!("{:?}", e)),
  })
}

fn to_bool(s: &str) -> Result<bool, RedisParseError> {
  match s.as_ref() {
    "t" => Ok(true),
    "f" => Ok(false),
    _ => Err(RedisParseError {
      input: vec![s.as_bytes()],
      kind: vec![ErrorKind::MapRes],
      context: "to_bool",
      message: Some("Invalid boolean value.".into()),
    }),
  }
}

fn to_string(d: &[u8]) -> Result<String, RedisParseError> {
  String::from_utf8(d.to_vec()).map_err(|e| RedisParseError {
    input: vec![d],
    kind: vec![ErrorKind::MapRes],
    context: "to_string",
    message: Some(format!("{:?}", e)),
  })
}

fn to_verbatimstring_format<'a>(s: &str) -> Result<VerbatimStringFormat, RedisParseError<'a>> {
  match s.as_ref() {
    "txt" => Ok(VerbatimStringFormat::Text),
    "mkd" => Ok(VerbatimStringFormat::Markdown),
    _ => Err(RedisParseError::new(
      "to_verbatimstring_format",
      "Invalid format.",
      None,
    )),
  }
}

fn to_map<'a>(mut data: Vec<Frame>) -> Result<FrameMap, RedisParseError<'a>> {
  if data.len() % 2 != 0 {
    return Err(RedisParseError::new("to_map", "Invalid hashmap frame length.", None));
  }

  let mut out = resp3_utils::new_map(Some(data.len() / 2));
  while data.len() >= 2 {
    let value = data.pop().unwrap();
    let key = data.pop().unwrap();

    out.insert(key, value);
  }

  Ok(out)
}

fn to_set<'a>(data: Vec<Frame>) -> Result<FrameSet, RedisParseError<'a>> {
  let mut out = resp3_utils::new_set(Some(data.len()));

  for frame in data.into_iter() {
    out.insert(frame);
  }

  Ok(out)
}

fn to_hello<'a>((version, auth): (u8, Option<(&str, &str)>)) -> Result<Frame, RedisParseError<'a>> {
  let version = match version {
    2 => RespVersion::RESP2,
    3 => RespVersion::RESP3,
    _ => {
      return Err(RedisParseError::new("parse_hello", "Invalid RESP version.", None));
    }
  };
  let auth = if let Some((username, password)) = auth {
    Some(Auth {
      username: Cow::Owned(username.to_owned()),
      password: Cow::Owned(password.to_owned()),
    })
  } else {
    None
  };

  Ok(Frame::Hello { version, auth })
}

fn attach_attributes<'a>(attributes: Attributes, mut frame: DecodedFrame) -> Result<DecodedFrame, RedisParseError<'a>> {
  if let Err(e) = frame.add_attributes(attributes) {
    Err(RedisParseError::new("attach_attributes", format!("{:?}", e), None))
  } else {
    Ok(frame)
  }
}

named!(read_to_crlf<&[u8]>, terminated!(take_until!(CRLF), take!(2)));

named!(read_to_crlf_s<&str>, map_res!(read_to_crlf, str::from_utf8));

named!(read_prefix_len<usize>, map_res!(read_to_crlf_s, to_usize));

named!(read_prefix_len_signed<isize>, map_res!(read_to_crlf_s, to_isize));

named!(
  frame_type<FrameKind>,
  switch!(be_u8,
    SIMPLE_STRING_BYTE   => value!(FrameKind::SimpleString) |
    SIMPLE_ERROR_BYTE    => value!(FrameKind::SimpleError) |
    NUMBER_BYTE          => value!(FrameKind::Number) |
    DOUBLE_BYTE          => value!(FrameKind::Double) |
    BLOB_STRING_BYTE     => value!(FrameKind::BlobString) |
    BLOB_ERROR_BYTE      => value!(FrameKind::BlobError) |
    VERBATIM_STRING_BYTE => value!(FrameKind::VerbatimString) |
    ARRAY_BYTE           => value!(FrameKind::Array) |
    NULL_BYTE            => value!(FrameKind::Null) |
    BOOLEAN_BYTE         => value!(FrameKind::Boolean) |
    MAP_BYTE             => value!(FrameKind::Map) |
    SET_BYTE             => value!(FrameKind::Set) |
    ATTRIBUTE_BYTE       => value!(FrameKind::Attribute) |
    PUSH_BYTE            => value!(FrameKind::Push) |
    BIG_NUMBER_BYTE      => value!(FrameKind::BigNumber) |
    CHUNKED_STRING_BYTE  => value!(FrameKind::ChunkedString) |
    END_STREAM_BYTE      => value!(FrameKind::EndStream)
  )
);

named!(
  parse_simplestring<Frame>,
  do_parse!(
    data: read_to_crlf_s
      >> (Frame::SimpleString {
        data: data.to_owned(),
        attributes: None
      })
  )
);

named!(
  parse_simpleerror<Frame>,
  do_parse!(
    data: read_to_crlf_s
      >> (Frame::SimpleError {
        data: data.to_owned(),
        attributes: None
      })
  )
);

named!(
  parse_number<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_i64) >> (Frame::Number { data, attributes: None }))
);

named!(
  parse_double<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_f64) >> (Frame::Double { data, attributes: None }))
);

named!(
  parse_boolean<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_bool) >> (Frame::Boolean { data, attributes: None }))
);

named!(parse_null<Frame>, do_parse!(read_to_crlf >> (Frame::Null)));

named_args!(
  parse_blobstring(len: usize)<Frame>,
  do_parse!(d: terminated!(take!(len), take!(2)) >> (Frame::BlobString{ data: Vec::from(d), attributes: None }))
);

named!(
  parse_bloberror<Frame>,
  do_parse!(
    len: read_prefix_len
      >> d: terminated!(take!(len), take!(2))
      >> (Frame::BlobError {
        data: Vec::from(d),
        attributes: None
      })
  )
);

named!(
  parse_verbatimstring<Frame>,
  do_parse!(
    len: read_prefix_len
      >> format_s: map_res!(terminated!(take!(3), take!(1)), str::from_utf8)
      >> format: map_res!(value!(format_s), to_verbatimstring_format)
      >> data: map_res!(terminated!(take!(len - 4), take!(2)), to_string)
      >> (Frame::VerbatimString {
        format,
        data,
        attributes: None
      })
  )
);

named!(
  parse_bignumber<Frame>,
  do_parse!(
    d: read_to_crlf
      >> (Frame::BigNumber {
        data: d.to_vec(),
        attributes: None
      })
  )
);

named_args!(
  parse_array_frames(len: usize) <Vec<Frame>>,
  count!(map_res!(parse_frame_or_attribute, unwrap_complete_frame), len)
);

named_args!(
  parse_kv_pairs(len: usize) <FrameMap>,
  map_res!(count!(map_res!(parse_frame_or_attribute, unwrap_complete_frame), len * 2), to_map)
);

named_args!(
  parse_array(len: usize)<Frame>,
  do_parse!(frames: call!(parse_array_frames, len) >> (Frame::Array { data: frames, attributes: None }))
);

named!(
  parse_push<Frame>,
  do_parse!(
    len: read_prefix_len
      >> frames: call!(parse_array_frames, len)
      >> (Frame::Push {
        data: frames,
        attributes: None
      })
  )
);

named_args!(
  parse_set(len: usize)<Frame>,
  do_parse!(frames: map_res!(call!(parse_array_frames, len), to_set) >> (Frame::Set { data: frames, attributes: None }))
);

named_args!(
  parse_map(len: usize)<Frame>,
  do_parse!(map: call!(parse_kv_pairs, len) >> (Frame::Map { data: map, attributes: None }))
);

named!(
  parse_attribute<Attributes>,
  do_parse!(len: read_prefix_len >> map: call!(parse_kv_pairs, len) >> (map))
);

named_args!(
  map_hello<'a>(version: u8, auth: Option<(&'a str, &'a str)>) <(u8, Option<(&'a str, &'a str)>)>,
  do_parse!((version, auth))
);

named_args!(
  map_attribute_and_frame<'a>(attributes: &'a mut Attributes, frame: DecodedFrame)<(&'a mut Attributes, DecodedFrame)>,
  do_parse!((attributes, frame))
);

named!(
  parse_hello<Frame>,
  do_parse!(
    hello: terminated!(take_until!(HELLO), take!(1))
      >> version: call!(be_u8)
      >> auth:
        opt!(do_parse!(
          auth: terminated!(take_until!(AUTH), take!(1))
            >> username: map_res!(terminated!(take_until!(EMPTY_SPACE), take!(1)), str::from_utf8)
            >> password: map_res!(terminated!(take_until!(EMPTY_SPACE), take!(1)), str::from_utf8)
            >> ((username, password))
        ))
      >> hello: map_res!(call!(map_hello, version, auth), to_hello)
      >> (hello)
  )
);

/// Check for a streaming variant of a frame, and if found then return the prefix bytes only, otherwise return the complete frame.
///
/// Only supported for arrays, sets, maps, and blob strings.
fn check_streaming(input: &[u8], kind: FrameKind) -> IResult<&[u8], DecodedFrame> {
  let (input, len) = read_prefix_len_signed(input)?;
  let (input, frame) = if len == -1 {
    (input, DecodedFrame::Streaming(StreamedFrame::new(kind)))
  } else {
    let len = isize_to_usize(len)?;
    let (input, frame) = match kind {
      FrameKind::Array => parse_array(input, len)?,
      FrameKind::Set => parse_set(input, len)?,
      FrameKind::Map => parse_map(input, len)?,
      _ => return Err(Err::Failure(Error::new(input, ErrorKind::Switch))),
    };

    (input, DecodedFrame::Complete(frame))
  };

  Ok((input, frame))
}

/*
named_args!(
  check_streaming(kind: FrameKind)<DecodedFrame>,
  do_parse!(
    len: read_prefix_len_signed >>
    frame: switch!(value!(len),
      // return streaming variant
      -1 => value!(DecodedFrame::Streaming(StreamedFrame::new(kind))) |
      // return complete variant
      _ => do_parse!(
        len: map_res!(value!(len), isize_to_usize) >>
        frame: switch!(value!(kind),
          FrameKind::Array => call!(parse_array, len) |
          FrameKind::Set => call!(parse_set, len) |
          FrameKind::Map => call!(parse_map, len)
        ) >>
        (DecodedFrame::Complete(frame))
      )
    ) >>
    (frame)
  )
);

 */

fn parse_chunked_string(input: &[u8]) -> IResult<&[u8], DecodedFrame> {
  let (input, len) = read_prefix_len(input)?;
  let (input, frame) = if len == 0 {
    let (input, _) = read_to_crlf(input)?;
    (input, Frame::new_end_stream())
  } else {
    let (input, contents) = nom_terminated(nom_take(len), nom_take(2_usize))(input)?;
    (input, Frame::ChunkedString(Vec::from(contents)))
  };

  Ok((input, DecodedFrame::Complete(frame)))
}

/*
named!(
  parse_chunked_string<DecodedFrame>,
  do_parse!(
    len: read_prefix_len
      >> frame:
        switch!(value!(len),
          0 => do_parse!(extra: read_to_crlf >> (Frame::new_end_stream())) |
          _ => do_parse!(d: terminated!(take!(len), take!(2)) >> (Frame::ChunkedString(Vec::from(d))))
        )
      >> (DecodedFrame::Complete(frame))
  )
);

 */

fn return_end_stream(input: &[u8]) -> IResult<&[u8], DecodedFrame> {
  let (input, _) = read_to_crlf(input)?;
  Ok((input, DecodedFrame::Complete(Frame::new_end_stream())))
}

/*
named!(
  return_end_stream<DecodedFrame>,
  do_parse!(extra: read_to_crlf >> (DecodedFrame::Complete(Frame::new_end_stream())))
);
 */

fn parse_non_attribute_frame(input: &[u8], kind: FrameKind) -> IResult<&[u8], DecodedFrame, RedisParseError> {
  let (input, frame) = match kind {
    FrameKind::Array => check_streaming(input, kind)?,
    FrameKind::BlobString => check_streaming(input, kind)?,
    FrameKind::Map => check_streaming(input, kind)?,
    FrameKind::Set => check_streaming(input, kind)?,
    FrameKind::SimpleString => nom_map(parse_simplestring, map_complete_frame)(input)?,
    FrameKind::SimpleError => nom_map(parse_simpleerror, map_complete_frame)(input)?,
    FrameKind::Number => nom_map(parse_number, map_complete_frame)(input)?,
    FrameKind::Null => nom_map(parse_null, map_complete_frame)(input)?,
    FrameKind::Double => nom_map(parse_double, map_complete_frame)(input)?,
    FrameKind::Boolean => nom_map(parse_boolean, map_complete_frame)(input)?,
    FrameKind::BlobError => nom_map(parse_bloberror, map_complete_frame)(input)?,
    FrameKind::VerbatimString => nom_map(parse_verbatimstring, map_complete_frame)(input)?,
    FrameKind::Push => nom_map(parse_push, map_complete_frame)(input)?,
    FrameKind::BigNumber => nom_map(parse_bignumber, map_complete_frame)(input)?,
    FrameKind::Hello => nom_map(parse_hello, map_complete_frame)(input)?,
    FrameKind::ChunkedString => parse_chunked_string(input)?,
    FrameKind::EndStream => return_end_stream(input)?,
    FrameKind::Attribute => {
      error!("Found unexpected attribute frame.");
      return Err(RedisParseError::new(
        "parse_non_attribute_frame",
        "Unexpected attribute frame.",
        None,
      ));
    }
  };

  Ok((input, frame))
}

/*
named_args!(
  parse_non_attribute_frame(kind: FrameKind)<DecodedFrame>,
  do_parse!(
      frame:
        switch!(value!(kind),
          FrameKind::Array          => call!(check_streaming, kind) |
          FrameKind::BlobString     => call!(check_streaming, kind) |
          FrameKind::SimpleString   => map!(call!(parse_simplestring), map_complete_frame) |
          FrameKind::SimpleError    => map!(call!(parse_simpleerror), map_complete_frame) |
          FrameKind::Number         => map!(call!(parse_number), map_complete_frame) |
          FrameKind::Null           => map!(call!(parse_null), map_complete_frame) |
          FrameKind::Double         => map!(call!(parse_double), map_complete_frame) |
          FrameKind::Boolean        => map!(call!(parse_boolean), map_complete_frame) |
          FrameKind::BlobError      => map!(call!(parse_bloberror), map_complete_frame) |
          FrameKind::VerbatimString => map!(call!(parse_verbatimstring), map_complete_frame) |
          FrameKind::Map            => call!(check_streaming, kind) |
          FrameKind::Set            => call!(check_streaming, kind) |
          FrameKind::Push           => map!(call!(parse_push), map_complete_frame) |
          FrameKind::BigNumber      => map!(call!(parse_bignumber), map_complete_frame) |
          FrameKind::Hello          => map!(call!(parse_hello), map_complete_frame) |
          FrameKind::ChunkedString  => call!(parse_chunked_string) |
          FrameKind::EndStream      => call!(return_end_stream)
        )
      >> (frame)
  )
);

 */

fn parse_attribute_and_frame(input: &[u8], kind: FrameKind) -> IResult<&[u8], DecodedFrame, RedisParseError> {
  let (input, attributes) = parse_attribute(input)?;
  let (input, next_frame) = parse_non_attribute_frame(input, kind)?;
  let frame = attach_attributes(attributes, next_frame)?;

  Ok((input, frame))
}

fn parse_frame_or_attribute(input: &[u8]) -> IResult<&[u8], DecodedFrame, RedisParseError> {
  let (input, kind) = frame_type(input)?;
  let (input, frame) = if let FrameKind::Attribute = kind {
    parse_attribute_and_frame(input, kind)?
  } else {
    parse_non_attribute_frame(input, kind)?
  };

  Ok((input, frame))
}

/*
named!(
  parse_frame_or_attribute<DecodedFrame>,
  do_parse!(
    kind: frame_type
      >> frame:
        switch!(value!(kind),
          // parse the attribute, then parse the next frame, then attach the attribute to the next frame, and return that frame
          FrameKind::Attribute => call!(parse_attribute_and_frame, kind) |
          _                    => call!(parse_non_attribute_frame, kind)
        )
      >> (frame)
  )
);
*/

/// Decoding functions for complete frames. **If a streamed frame is detected it will result in an error.**
///
/// Implement a [codec](https://docs.rs/tokio-util/0.6.6/tokio_util/codec/index.html) that only supports complete frames...
///
/// ```edition2018 no_run
/// # extern crate tokio_util;
/// # extern crate tokio;
/// # extern crate bytes;
///
/// # use redis_protocol::resp3::types::*;
/// # use redis_protocol::types::{RedisProtocolError, RedisProtocolErrorKind};
/// # use redis_protocol::resp3::decode::streaming::*;
/// # use redis_protocol::resp3::encode::complete::*;
/// # use bytes::BytesMut;
/// # use tokio_util::codec::{Decoder, Encoder};
/// # use std::collections::VecDeque;
///
/// pub struct RedisCodec {}
///
/// impl Encoder<Frame> for RedisCodec {
///   type Error = RedisProtocolError;
///
///   fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
///     // in this example we only show support for encoding complete frames
///     let _ = encode_bytes(dst, &item)?;
///     Ok(())
///   }
/// }
///
/// impl Decoder for RedisCodec {
///   type Item = Frame;
///   type Error = RedisProtocolError;
///
///   fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
///     if src.is_empty() {
///       return Ok(None);
///     }
///
///     if let Some((frame, amt)) = decode(&src)? {
///       // clear the buffer up to the amount decoded so the same bytes aren't repeatedly processed
///       let _ = src.split_to(amt);
///       
///       Ok(Some(frame.into_complete_frame()?))
///     }else{
///       Ok(None)
///     }
///   }
/// }
/// ```
///
pub mod complete {
  use super::*;
  use std::collections::VecDeque;

  /// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
  ///
  /// If the byte slice contains an incomplete frame then `None` is returned.
  pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, RedisProtocolError> {
    let len = buf.len();

    match parse_frame_or_attribute(buf) {
      Ok((remaining, frame)) => Ok(Some((frame.into_complete_frame()?, len - remaining.len()))),
      Err(NomError::Incomplete(_)) => Ok(None),
      Err(e) => Err(e.into()),
    }
  }
}

/// Decoding structs and functions that support streaming frames. The caller is responsible for managing any returned state for streaming frames.
///
/// Examples:
///
/// Implement a [codec](https://docs.rs/tokio-util/0.6.6/tokio_util/codec/index.html) that supports decoding streams...
///
/// ```edition2018 no_run
/// # extern crate tokio_util;
/// # extern crate tokio;
/// # extern crate bytes;
///
/// # use redis_protocol::resp3::types::*;
/// # use redis_protocol::types::{RedisProtocolError, RedisProtocolErrorKind};
/// # use redis_protocol::resp3::decode::streaming::*;
/// # use redis_protocol::resp3::encode::complete::*;
/// # use bytes::BytesMut;
/// # use tokio_util::codec::{Decoder, Encoder};
/// # use std::collections::VecDeque;
///
/// pub struct RedisCodec {
///   decoder_stream: Option<StreamedFrame>
/// }
///
/// impl Encoder<Frame> for RedisCodec {
///   type Error = RedisProtocolError;
///
///   fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
///     // in this example we only show support for encoding complete frames
///     let _ = encode_bytes(dst, &item)?;
///     Ok(())
///   }
/// }
///
/// impl Decoder for RedisCodec {
///   type Item = Frame;
///   type Error = RedisProtocolError;
///
///   fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
///     if src.is_empty() {
///       return Ok(None);
///     }
///
///     if let Some((frame, amt)) = decode(&src)? {
///       // clear the buffer up to the amount decoded so the same bytes aren't repeatedly processed
///       let _ = src.split_to(amt);
///
///       if self.decoder_stream.is_some() && frame.is_streaming() {
///         // it doesn't make sense to start a stream while inside another stream
///         return Err(RedisProtocolError::new(
///           RedisProtocolErrorKind::DecodeError,
///           "Cannot start a stream while already inside a stream."
///         ));
///       }
///       
///       let result = if let Some(ref mut streamed_frame) = self.decoder_stream {
///         // we started receiving streamed data earlier
///
///         // we already checked for streams within streams above
///         let frame = frame.into_complete_frame()?;
///         streamed_frame.add_frame(frame);
///
///         if streamed_frame.is_finished() {
///            // convert the inner stream buffer into the final output frame
///            Some(streamed_frame.into_frame()?)
///         }else{
///           // buffer the stream in memory until it completes
///           None
///         }
///       }else{
///         // we're not already in the middle of a streaming operation
///         if frame.is_streaming() {
///           // start a new stream, saving the internal buffer to the codec state
///           self.decoder_stream = Some(frame.into_streaming_frame()?);
///           // don't return anything to the caller until the stream finishes (shown above)
///           None
///         }else{
///           // we're not in the middle of a stream and we found a complete frame
///           Some(frame.into_complete_frame()?)
///         }
///       };
///
///       if result.is_some() {
///         // we're either done with the stream or we found a complete frame. either way clear the buffer.
///         let _ = self.decoder_stream.take();
///       }
///
///       Ok(result)
///     }else{
///       Ok(None)
///     }
///   }
/// }
/// ```
///
pub mod streaming {
  use super::*;
  use std::collections::VecDeque;

  /// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
  ///
  /// If the byte slice contains an incomplete frame then `None` is returned.
  pub fn decode(buf: &[u8]) -> Result<Option<(DecodedFrame, usize)>, RedisProtocolError> {
    let len = buf.len();

    match parse_frame_or_attribute(buf) {
      Ok((remaining, frame)) => Ok(Some((frame, len - remaining.len()))),
      Err(NomError::Incomplete(_)) => Ok(None),
      Err(e) => Err(e.into()),
    }
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
    panic!("Failed to decode bytes. None returned.")
  }

  fn decode_and_verify_some(bytes: &mut BytesMut, expected: &(Option<Frame>, usize)) {
    let (frame, len) = match complete::decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &mut BytesMut, expected: &(Option<Frame>, usize)) {
    bytes.extend_from_slice(PADDING.as_bytes());

    let (frame, len) = match complete::decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => return panic_no_decode(),
      Err(e) => return pretty_print_panic(e),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &mut BytesMut) {
    let (frame, len) = match complete::decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => return pretty_print_panic(e),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  // ----------------------- tests adapted from RESP2 ------------------------

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (
      Some(Frame::Number {
        data: 48293,
        attributes: None,
      }),
      8,
    );
    let mut bytes: BytesMut = ":48293\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (
      Some(Frame::SimpleString {
        data: "string".into(),
        attributes: None,
      }),
      9,
    );
    let mut bytes: BytesMut = "+string\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (
      Some(Frame::SimpleString {
        data: "string".into(),
        attributes: None,
      }),
      9,
    );
    let mut bytes: BytesMut = "+stri".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (
      Some(Frame::BlobString {
        data: "foo".into(),
        attributes: None,
      }),
      9,
    );
    let mut bytes: BytesMut = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (
      Some(Frame::BlobString {
        data: "foo".into(),
        attributes: None,
      }),
      9,
    );
    let mut bytes: BytesMut = "$3\r\nfo".into();

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(Frame::Array {
        data: vec![
          Frame::SimpleString {
            data: "Foo".into(),
            attributes: None,
          },
          Frame::SimpleString {
            data: "Bar".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
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
      Some(Frame::Array {
        data: vec![
          Frame::BlobString {
            data: "Foo".into(),
            attributes: None,
          },
          Frame::Null,
          Frame::BlobString {
            data: "Bar".into(),
            attributes: None,
          },
        ],
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let mut bytes: BytesMut = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(Frame::SimpleError {
        data: "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let mut bytes: BytesMut = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (
      Some(Frame::SimpleError {
        data: "MOVED 3999 127.0.0.1:6381".into(),
        attributes: None,
      }),
      bytes.len(),
    );

    decode_and_verify_some(&mut bytes, &expected);
    decode_and_verify_padded_some(&mut bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let mut bytes: BytesMut = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (
      Some(Frame::SimpleError {
        data: "ASK 3999 127.0.0.1:6381".into(),
        attributes: None,
      }),
      bytes.len(),
    );

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
    let _ = complete::decode(&bytes).map_err(|e| pretty_print_panic(e));
  }

  // ----------------- end tests adapted from RESP2 ------------------------

  // TODO bloberror, simpleerror, map, set, array, push, hello, boolean, number, double (inf, nan, negative, etc), bignumber, null, verbatimstring
  // TODO attributes, streaming
}
