//! Functions for decoding the RESP3 protocol into frames.
//!
//! <https://github.com/antirez/RESP3/blob/master/spec.md>

use crate::resp3::types::*;
use crate::resp3::utils as resp2_utils;
use crate::types::*;
use crate::utils;
use bytes::BytesMut;
use nom::number::streaming::be_u8;
use nom::Err as NomError;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::num::{ParseFloatError, ParseIntError};
use std::slice::Chunks;
use std::str;
use std::string::FromUtf8Error;

fn non_streaming_error<T>(data: T, kind: FrameKind) -> Result<T, RedisProtocolError> {
  Err(RedisProtocolError::new(
    RedisProtocolErrorKind::DecodeError,
    format!("Cannot decode streaming {:?}", kind),
  ))
}

fn map_complete_frame(frame: Frame) -> DecodedFrame {
  DecodedFrame::Complete(frame)
}

fn unwrap_complete_frame(frame: DecodedFrame) -> Result<Frame, RedisProtocolError> {
  frame.into_complete_frame()
}

fn to_usize(s: &str) -> Result<usize, ParseIntError> {
  s.parse::<usize>()
}

fn to_isize(s: &str) -> Result<isize, ParseIntError> {
  if s == "?" {
    Ok(-1)
  } else {
    s.parse::<isize>()
  }
}

fn isize_to_usize(n: isize) -> Result<usize, RedisProtocolError> {
  if n.is_negative() {
    Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid prefix length.",
    ))
  } else {
    Ok(n as usize)
  }
}

fn to_i64(s: &str) -> Result<i64, ParseIntError> {
  s.parse::<i64>()
}

fn to_f64(s: &str) -> Result<f64, ParseFloatError> {
  s.parse::<f64>()
}

fn to_bool(s: &str) -> Result<bool, RedisProtocolError> {
  match s.as_ref() {
    "t" => Ok(true),
    "f" => Ok(false),
    _ => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid boolean value.",
    )),
  }
}

fn to_string(d: &[u8]) -> Result<String, FromUtf8Error> {
  String::from_utf8(d.to_vec())
}

fn to_verbatimstring_format(s: &str) -> Result<VerbatimStringFormat, RedisProtocolError> {
  match s.as_ref() {
    "txt" => Ok(VerbatimStringFormat::Text),
    "mkd" => Ok(VerbatimStringFormat::Markdown),
    _ => Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid verbatim string format.",
    )),
  }
}

fn to_hashmap(mut data: Vec<Frame>) -> Result<HashMap<Frame, Frame>, RedisProtocolError> {
  if data.len() % 2 != 0 {
    return Err(RedisProtocolError::new(
      RedisProtocolErrorKind::DecodeError,
      "Invalid hashmap frame length.",
    ));
  }

  let mut out = HashMap::with_capacity(data.len() / 2);
  while data.len() >= 2 {
    let value = data.pop().unwrap();
    let key = data.pop().unwrap();

    out.insert(key, value);
  }

  Ok(out)
}

fn to_set(data: Vec<Frame>) -> Result<HashSet<Frame>, RedisProtocolError> {
  let mut out = HashSet::with_capacity(data.len());

  for frame in data.into_iter() {
    out.insert(frame);
  }

  Ok(out)
}

fn to_hello((version, auth): (u8, Option<(&str, &str)>)) -> Result<Frame, RedisProtocolError> {
  let version = match version {
    2 => RespVersion::RESP2,
    3 => RespVersion::RESP3,
    _ => {
      return Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Invalid RESP version.",
      ))
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
  do_parse!(data: read_to_crlf_s >> (Frame::SimpleString(data.to_owned())))
);

named!(
  parse_simpleerror<Frame>,
  do_parse!(data: read_to_crlf_s >> (Frame::SimpleError(data.to_owned())))
);

named!(
  parse_number<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_i64) >> (Frame::Number(data)))
);

named!(
  parse_double<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_f64) >> (Frame::Double(data)))
);

named!(
  parse_boolean<Frame>,
  do_parse!(data: map_res!(read_to_crlf_s, to_bool) >> (Frame::Boolean(data)))
);

named!(parse_null<Frame>, do_parse!(read_to_crlf >> (Frame::Null)));

named_args!(
  parse_blobstring(len: usize)<Frame>,
  do_parse!(d: terminated!(take!(len), take!(2)) >> (Frame::BlobString(Vec::from(d))))
);

named!(
  parse_bloberror<Frame>,
  do_parse!(len: read_prefix_len >> d: terminated!(take!(len), take!(2)) >> (Frame::BlobError(Vec::from(d))))
);

named!(
  parse_verbatimstring<Frame>,
  do_parse!(
    len: read_prefix_len
      >> format_s: map_res!(terminated!(take!(3), take!(1)), str::from_utf8)
      >> format: map_res!(value!(format_s), to_verbatimstring_format)
      >> data: map_res!(terminated!(take!(len - 4), take!(2)), to_string)
      >> (Frame::VerbatimString { format, data })
  )
);

named!(
  parse_bignumber<Frame>,
  do_parse!(d: read_to_crlf >> (Frame::BigNumber(d.to_vec())))
);

named_args!(
  parse_array_frames(len: usize) <Vec<Frame>>,
  count!(map_res!(parse_frame, unwrap_complete_frame), len)
);

named_args!(
  parse_kv_pairs(len: usize) <HashMap<Frame, Frame>>,
  map_res!(count!(map_res!(parse_frame, unwrap_complete_frame), len * 2), to_hashmap)
);

named_args!(
  parse_array(len: usize)<Frame>,
  do_parse!(frames: call!(parse_array_frames, len) >> (Frame::Array(frames)))
);

named!(
  parse_push<Frame>,
  do_parse!(len: read_prefix_len >> frames: call!(parse_array_frames, len) >> (Frame::Push(frames)))
);

named_args!(
  parse_set(len: usize)<Frame>,
  do_parse!(frames: map_res!(call!(parse_array_frames, len), to_set) >> (Frame::Set(frames)))
);

named_args!(
  parse_map(len: usize)<Frame>,
  do_parse!(map: call!(parse_kv_pairs, len) >> (Frame::Map(map)))
);

named!(
  parse_attribute<Frame>,
  do_parse!(len: read_prefix_len >> map: call!(parse_kv_pairs, len) >> (Frame::Attribute(map)))
);

named_args!(
  map_hello<'a>(version: u8, auth: Option<(&'a str, &'a str)>) <(u8, Option<(&'a str, &'a str)>)>,
  do_parse!((version, auth))
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

named!(
  return_end_stream<DecodedFrame>,
  do_parse!(extra: read_to_crlf >> (DecodedFrame::Complete(Frame::new_end_stream())))
);

named!(
  parse_frame<DecodedFrame>,
  do_parse!(
    kind: frame_type
      >> frame:
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
          FrameKind::Attribute      => map!(call!(parse_attribute), map_complete_frame) |
          FrameKind::Push           => map!(call!(parse_push), map_complete_frame) |
          FrameKind::BigNumber      => map!(call!(parse_bignumber), map_complete_frame) |
          FrameKind::Hello          => map!(call!(parse_hello), map_complete_frame) |
          FrameKind::ChunkedString  => call!(parse_chunked_string) |
          FrameKind::EndStream      => call!(return_end_stream)
        )
      >> (frame)
  )
);

/// Decoding functions for complete frames. **If a streamed frame is detected it will result in an error.**
///
/// **Note about attributes:**
///
/// Attributes can appear in buffers in different locations. They can either appear just before a frame, in which case [decode_with_leading_attributes](complete::decode_with_leading_attributes)
/// will work as expected, or they can appear within an aggregate type such as an array. The latter case is left to the caller to handle in that a `Frame::Array`
/// will be returned where one of the inner frames will be an attribute frame. This approach enables the caller to more easily link the attribute to the following
/// frame in a higher level library.
pub mod complete {
  use super::*;
  use std::collections::VecDeque;

  /// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
  ///
  /// If the byte slice contains an incomplete frame then `None` is returned.
  pub fn decode(buf: &[u8]) -> Result<Option<(Frame, usize)>, RedisProtocolError> {
    let len = buf.len();

    match parse_frame(buf) {
      Ok((remaining, frame)) => Ok(Some((frame.into_complete_frame()?, len - remaining.len()))),
      Err(NomError::Incomplete(_)) => Ok(None),
      Err(e) => Err(e.into()),
    }
  }

  /// Attempt to parse the contents of `buf`, returning the first valid non-attribute frame, the number of bytes consumed, and a separate optional array of attribute frames leading up to the first non-attribute frame.
  ///
  /// If the buffer contains an incomplete frame, or if the buffer only contains an attribute frame, then `None` is returned. Use [decode] if you want to parse individual attribute frames.
  pub fn decode_with_leading_attributes(
    buf: &[u8],
  ) -> Result<Option<(Frame, usize, VecDeque<Frame>)>, RedisProtocolError> {
    let len = buf.len();
    let mut parsed = 0;
    let mut attributes = VecDeque::new();

    // TODO examine this approach where attribute frames appear within a map.
    // this may result in an odd number of frames which will fail the decoder.

    while parsed < buf.len() {
      let (frame, amt) = match parse_frame(&buf[parsed..]) {
        Ok((remaining, frame)) => (frame.into_complete_frame()?, len - remaining.len()),
        Err(NomError::Incomplete(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
      };
      parsed += amt;

      if frame.is_attribute() {
        attributes.push_back(frame);
        continue;
      }

      return Ok(Some((frame, parsed, attributes)));
    }

    Ok(None)
  }
}

/// Decoding structs and functions that support streaming frames. The caller is responsible for managing any returned state for streaming frames.
///
/// **Note about attributes:**
///
/// Attributes can appear in buffers in different locations. They can either appear just before a frame, in which case [decode_with_leading_attributes](streaming::decode_with_leading_attributes)
/// will work as expected, or they can appear within an aggregate type such as an array. The latter case is left to the caller to handle in that a `Frame::Array`
/// will be returned where one of the inner frames will be an attribute frame. This approach enables the caller to more easily link the attribute to the following
/// frame in a higher level library.
pub mod streaming {
  use super::*;
  use std::collections::VecDeque;

  /// Attempt to parse the contents of `buf`, returning the first valid frame and the number of bytes consumed.
  ///
  /// If the byte slice contains an incomplete frame then `None` is returned.
  pub fn decode(buf: &[u8]) -> Result<Option<(DecodedFrame, usize)>, RedisProtocolError> {
    let len = buf.len();

    match parse_frame(buf) {
      Ok((remaining, frame)) => Ok(Some((frame, len - remaining.len()))),
      Err(NomError::Incomplete(_)) => Ok(None),
      Err(e) => Err(e.into()),
    }
  }

  /// Attempt to parse the contents of `buf`, returning the first valid non-attribute frame, the number of bytes consumed, and a separate optional array of attribute frames leading up to the first non-attribute frame.
  ///
  /// If the buffer contains an incomplete frame, or if the buffer only contains an attribute frame, then `None` is returned. Use [decode] if you want to parse individual attribute frames.
  ///
  /// TODO examples
  pub fn decode_with_leading_attributes(
    buf: &[u8],
  ) -> Result<Option<(DecodedFrame, usize, VecDeque<Frame>)>, RedisProtocolError> {
    let len = buf.len();
    let mut parsed = 0;
    let mut attributes = VecDeque::new();

    // TODO examine this approach where attribute frames appear within a map.
    // this may result in an odd number of frames which will fail the decoder.

    while parsed < buf.len() {
      let (frame, amt) = match parse_frame(&buf[parsed..]) {
        Ok((remaining, frame)) => (frame, len - remaining.len()),
        Err(NomError::Incomplete(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
      };
      parsed += amt;

      if frame.is_attribute() {
        attributes.push_back(frame.into_complete_frame()?);
        continue;
      }

      return Ok(Some((frame, parsed, attributes)));
    }

    Ok(None)
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
}
