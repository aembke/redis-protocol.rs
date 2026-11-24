//! Functions for decoding the RESP2 protocol into frames.
//!
//! <https://redis.io/topics/protocol#resp-protocol-description>

use crate::{
  error::{RedisParseError, RedisProtocolError},
  resp2::{types::*, utils::build_owned_frame},
  types::*,
  utils,
};
use alloc::vec::Vec;
use core::{mem::size_of, str};
use nom::{
  bytes::streaming::take as nom_take,
  number::streaming::be_u8,
  sequence::terminated as nom_terminated,
  Err as NomErr,
  Parser,
};

#[cfg(feature = "bytes")]
use crate::resp2::utils::{build_bytes_frame, freeze_parse};
#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};

pub(crate) const NULL_LEN: isize = -1;

fn to_isize(s: &[u8]) -> Result<isize, RedisParseError<&[u8]>> {
  str::from_utf8(s)?
    .parse::<isize>()
    .map_err(|_| RedisParseError::new_custom("to_isize", "Failed to parse as integer."))
}

fn to_i64(s: &[u8]) -> Result<i64, RedisParseError<&[u8]>> {
  str::from_utf8(s)?
    .parse::<i64>()
    .map_err(|_| RedisParseError::new_custom("to_i64", "Failed to parse as integer."))
}

fn d_read_to_crlf(input: (&[u8], usize)) -> DResult<'_, usize> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (data, input_bytes) = utils::find_crlf(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data.len()))
}

fn d_read_to_crlf_take(input: (&[u8], usize)) -> DResult<'_, &[u8]> {
  decode_log_str!(input.0, _input, "Parsing to CRLF. Remaining: {:?}", _input);
  let (data, input_bytes) = utils::find_crlf(input.0)?;
  Ok(((input_bytes, input.1 + data.len() + 2), data))
}

fn d_read_prefix_len(input: (&[u8], usize)) -> DResult<'_, isize> {
  let (input, data) = d_read_to_crlf_take(input)?;
  decode_log!("Reading prefix len. Data: {:?}", str::from_utf8(data));
  Ok((input, etry!(to_isize(data))))
}

fn d_frame_type(input: (&[u8], usize)) -> DResult<'_, FrameKind> {
  let (input_bytes, byte) = be_u8(input.0)?;
  decode_log_str!(
    input_bytes,
    _input,
    "Reading frame type. Kind byte: {:?}, remaining: {:?}",
    byte,
    _input
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

fn d_parse_simplestring(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::SimpleString((offset, offset + len))))
}

fn d_parse_integer(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, next_offset), data) = d_read_to_crlf_take(input)?;
  let parsed = etry!(to_i64(data));
  Ok(((input, next_offset), RangeFrame::Integer(parsed)))
}

// assumes the '$-1\r\n' has been consumed already, since nulls look like bulk strings until the length prefix is
// parsed, and parsing the length prefix consumes the trailing \r\n in the underlying `terminated!` call
fn d_parse_null(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  Ok((input, RangeFrame::Null))
}

fn d_parse_error(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let ((input, next_offset), len) = d_read_to_crlf(input)?;
  Ok(((input, next_offset), RangeFrame::Error((offset, offset + len))))
}

fn d_parse_bulkstring(input: (&[u8], usize), len: usize) -> DResult<'_, RangeFrame> {
  let offset = input.1;
  let (input, data) = nom_terminated(nom_take(len), nom_take(2_usize)).parse(input.0)?;
  Ok((
    (input, offset + len + 2),
    RangeFrame::BulkString((offset, offset + data.len())),
  ))
}

fn d_parse_bulkstring_or_null(input: (&[u8], usize)) -> DResult<'_, RangeFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  decode_log_str!(
    input,
    _input,
    "Parsing bulkstring, Length: {:?}, remaining: {:?}",
    len,
    _input
  );

  if len == NULL_LEN {
    d_parse_null((input, offset))
  } else {
    d_parse_bulkstring((input, offset), etry!(utils::isize_to_usize(len)))
  }
}

fn d_parse_array_frames(input: (&[u8], usize), len: usize, depth: usize) -> DResult<'_, Vec<RangeFrame>> {
  decode_log_str!(
    input.0,
    _input,
    "Parsing array frames. Length: {:?}, remaining: {:?}",
    len,
    _input
  );
  // mirror nom's `count`, which caps the initial allocation to avoid trusting the length prefix. this is a manual
  // loop in order to keep the stack frames on the recursive parsing path small.
  let mut frames = Vec::with_capacity(len.min(MAX_INITIAL_CAPACITY_BYTES / size_of::<RangeFrame>()));
  let mut input = input;
  for _ in 0 .. len {
    let (next, frame) = d_parse_frame(input, depth + 1)?;
    frames.push(frame);
    input = next;
  }

  Ok((input, frames))
}

fn d_parse_array(input: (&[u8], usize), depth: usize) -> DResult<'_, RangeFrame> {
  let ((input, offset), len) = d_read_prefix_len(input)?;
  decode_log_str!(
    input,
    _input,
    "Parsing array. Length: {:?}, remaining: {:?}",
    len,
    _input
  );

  if len == NULL_LEN {
    Ok(((input, offset), RangeFrame::NullArray))
  } else {
    let len = etry!(utils::isize_to_usize(len));
    let ((input, offset), frames) = d_parse_array_frames((input, offset), len, depth)?;
    Ok(((input, offset), RangeFrame::Array(frames)))
  }
}

fn d_parse_frame(input: (&[u8], usize), depth: usize) -> DResult<'_, RangeFrame> {
  if depth > MAX_DECODE_DEPTH {
    e!(RedisParseError::new_custom(
      "parse_frame",
      "Max frame nesting depth exceeded."
    ));
  }
  let ((input, offset), kind) = d_frame_type(input)?;
  decode_log_str!(input, _input, "Parsed kind: {:?}, remaining: {:?}", kind, _input);

  match kind {
    FrameKind::SimpleString => d_parse_simplestring((input, offset)),
    FrameKind::Error => d_parse_error((input, offset)),
    FrameKind::Integer => d_parse_integer((input, offset)),
    FrameKind::BulkString => d_parse_bulkstring_or_null((input, offset)),
    FrameKind::Array => d_parse_array((input, offset), depth),
    _ => e!(RedisParseError::new_custom("parse_frame", "Invalid frame kind.")),
  }
}

/// Attempt to the decode the contents of `buf`, returning frames that reference ranges into the provided buffer.
///
/// This is the generic interface behind the zero-copy interface and can be used to implement zero-copy
/// deserialization into other types.
pub fn decode_range(buf: &[u8]) -> Result<Option<(RangeFrame, usize)>, RedisProtocolError> {
  let (offset, _len) = (0, buf.len());

  match d_parse_frame((buf, offset), 0) {
    Ok(((_remaining, amt), frame)) => {
      #[cfg(feature = "std")]
      debug_assert_eq!(amt, _len - _remaining.len(), "returned offset doesn't match");
      Ok(Some((frame, amt)))
    },
    Err(NomErr::Incomplete(_)) => Ok(None),
    Err(NomErr::Error(e)) => Err(e.into()),
    Err(NomErr::Failure(e)) => Err(e.into()),
  }
}

/// Attempt to decode the contents of `buf`, returning the first valid frame and the number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<Option<(OwnedFrame, usize)>, RedisProtocolError> {
  let (frame, amt) = match decode_range(buf)? {
    Some(result) => result,
    None => return Ok(None),
  };

  Ok(Some((build_owned_frame(buf, &frame)?, amt)))
}

/// Attempt to decode the provided buffer without moving or copying the inner buffer contents.
///
/// The returned frame(s) will hold owned views into the original buffer via [slice](bytes::Bytes::slice).
///
/// Unlike [decode_bytes_mut], this function will not modify the input buffer.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn decode_bytes(buf: &Bytes) -> Result<Option<(BytesFrame, usize)>, RedisProtocolError> {
  let (frame, amt) = match decode_range(buf)? {
    Some(result) => result,
    None => return Ok(None),
  };

  Ok(Some((build_bytes_frame(buf, &frame)?, amt)))
}

/// Attempt to decode and [split](bytes::BytesMut::split_to) the provided buffer without moving or copying the inner
/// buffer contents.
///
/// The returned frame(s) will hold owned views into the original buffer.
///
/// This function is designed to work best with a [codec](tokio_util::codec) interface.
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn decode_bytes_mut(buf: &mut BytesMut) -> Result<Option<(BytesFrame, usize, Bytes)>, RedisProtocolError> {
  let (frame, amt) = match decode_range(&*buf)? {
    Some(result) => result,
    None => return Ok(None),
  };
  let (frame, buf) = freeze_parse(buf, &frame, amt)?;

  Ok(Some((frame, amt, buf)))
}

/// An incremental scanner that finds the end of the next frame in a buffer that is filled over several reads.
///
/// The decoding functions in this module don't keep any state between calls. When the buffer holds only part of a
/// frame they return `Ok(None)`, and the next call parses the buffer again from the start. If a large frame arrives
/// over many small reads (TLS, for example, usually yields one record per read), the total decoding cost grows with
/// the square of the frame size.
///
/// A `FrameScanner` remembers how far it got. Each call to [scan](Self::scan) reads only the bytes added since the
/// previous call, and bulk string contents are skipped by their length prefix. Call a decoding function when the
/// scanner reports that the buffer contains a complete frame.
///
/// The scanner uses the same parsing logic as the decoder, so the [Scan] value always matches what the decoder
/// would return for the same buffer. It never copies or stores the buffer contents.
///
/// ```rust
/// # use redis_protocol::{resp2::decode::{decode, FrameScanner}, types::Scan};
/// let mut scanner = FrameScanner::new();
/// let mut buf = b"*2\r\n$3\r\nfoo\r\n$3\r\nb".to_vec();
/// assert_eq!(scanner.scan(&buf), Scan::Incomplete);
///
/// // the next call only reads the new bytes
/// buf.extend_from_slice(b"ar\r\n");
/// assert_eq!(scanner.scan(&buf), Scan::Complete(buf.len()));
///
/// let (frame, amt) = decode(&buf).unwrap().unwrap();
/// assert_eq!(amt, 22);
/// ```
///
/// The scanner's offsets are relative to the start of the buffer, so the buffer must only grow between calls that
/// return [Scan::Incomplete]. The scanner resets itself when it returns [Scan::Complete] or [Scan::Invalid], at which
/// point the caller is expected to decode or discard the frame. Call [reset](Self::reset) if the buffer is cleared or
/// replaced in any other way.
#[derive(Clone, Debug, Default)]
pub struct FrameScanner {
  /// The offset of the next frame header to read.
  pos:      usize,
  /// The buffer length at the end of the last search for the CRLF that ends the next header.
  searched: usize,
  /// The number of inner frames still expected by each open array, innermost last.
  stack:    Vec<usize>,
}

impl FrameScanner {
  /// Create a new scanner.
  pub fn new() -> Self {
    Self::default()
  }

  /// Reset the scanner so that the next call to [scan](Self::scan) starts from the beginning of the buffer.
  pub fn reset(&mut self) {
    self.pos = 0;
    self.searched = 0;
    self.stack.clear();
  }

  fn finish(&mut self, result: Scan) -> Scan {
    self.reset();
    result
  }

  /// Whether the scanner has not read any part of the next frame.
  fn is_idle(&self) -> bool {
    self.pos == 0 && self.searched == 0
  }

  /// Decode the next frame in `buf` with `decode`, using the scanner to avoid parsing a partial frame more than once.
  ///
  /// If no part of the next frame has been scanned yet, `decode` runs right away, since the whole frame is usually
  /// already buffered. If it returns `Ok(None)`, the scanner reads the partial frame, and later calls only run
  /// `decode` once the frame is complete. This costs one parse per frame when frames arrive whole, and keeps the
  /// cost linear in the frame size when they don't.
  ///
  /// ```rust
  /// # use redis_protocol::resp2::decode::{decode, FrameScanner};
  /// let mut scanner = FrameScanner::new();
  /// let mut buf = b"$5\r\nhel".to_vec();
  /// assert!(scanner
  ///   .decode_with(&mut buf, |buf| decode(buf))
  ///   .unwrap()
  ///   .is_none());
  ///
  /// buf.extend_from_slice(b"lo\r\n");
  /// let (frame, amt) = scanner
  ///   .decode_with(&mut buf, |buf| decode(buf))
  ///   .unwrap()
  ///   .unwrap();
  /// buf.drain(.. amt);
  /// ```
  pub fn decode_with<B, T, E, F>(&mut self, buf: &mut B, decode: F) -> Result<Option<T>, E>
  where
    B: AsRef<[u8]> + ?Sized,
    F: FnOnce(&mut B) -> Result<Option<T>, E>,
  {
    if self.is_idle() {
      let result = decode(buf);
      if let Ok(None) = result {
        let _ = self.scan(buf.as_ref());
      }
      result
    } else if self.scan(buf.as_ref()) == Scan::Incomplete {
      Ok(None)
    } else {
      decode(buf)
    }
  }

  /// Continue scanning `buf`, which must contain the same bytes as the previous call, plus any newly received bytes.
  pub fn scan(&mut self, buf: &[u8]) -> Scan {
    if buf.len() < self.pos.max(self.searched) {
      // the buffer was changed without calling `reset`. start over rather than read the wrong offsets.
      self.reset();
    }

    loop {
      let depth = self.stack.len();
      if depth > MAX_DECODE_DEPTH {
        return self.finish(Scan::Invalid);
      }
      let input = (&buf[self.pos ..], self.pos);
      // the decoder rejects an invalid type byte before reading the rest of the line
      match d_frame_type(input) {
        Ok(_) => {},
        Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
        Err(_) => return self.finish(Scan::Invalid),
      };
      if !utils::line_is_buffered(buf, self.pos, &mut self.searched) {
        return Scan::Incomplete;
      }

      let next = if buf[self.pos] == ARRAY_BYTE {
        match d_read_prefix_len((&buf[self.pos + 1 ..], self.pos + 1)) {
          Ok(((_, next), len)) if len == NULL_LEN || len == 0 => next,
          Ok(((_, next), len)) if len > 0 => {
            self.pos = next;
            self.searched = next;
            self.stack.push(len as usize);
            continue;
          },
          Ok(_) | Err(NomErr::Error(_)) | Err(NomErr::Failure(_)) => return self.finish(Scan::Invalid),
          Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
        }
      } else {
        // leaf frames are parsed with the decoder's own logic, which only reads one header line plus any bulk string
        // contents, and never allocates
        match d_parse_frame(input, depth) {
          Ok(((_, next), _)) => next,
          Err(NomErr::Incomplete(_)) => return Scan::Incomplete,
          Err(_) => return self.finish(Scan::Invalid),
        }
      };
      self.pos = next;
      self.searched = next;

      // a frame ended, which may also end one or more of the arrays that contain it
      loop {
        match self.stack.last_mut() {
          None => {
            let amt = self.pos;
            return self.finish(Scan::Complete(amt));
          },
          Some(remaining) => {
            *remaining -= 1;
            if *remaining > 0 {
              break;
            }
            self.stack.pop();
          },
        }
      }
    }
  }
}

// Regression tests duplicated for each frame type.

#[cfg(test)]
mod owned_tests {
  use super::*;

  pub const PADDING: &str = "FOOBARBAZ";

  fn decode_and_verify_some(bytes: &[u8], expected: &(Option<OwnedFrame>, usize)) {
    let bytes = bytes.to_vec();

    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => panic!("Failed to decode bytes. None returned"),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_padded_some(bytes: &[u8], expected: &(Option<OwnedFrame>, usize)) {
    let mut bytes = bytes.to_vec();
    bytes.extend_from_slice(PADDING.as_bytes());

    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => panic!("Failed to decode bytes. None returned"),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
  }

  fn decode_and_verify_none(bytes: &[u8]) {
    let bytes = bytes.to_vec();
    let (frame, len) = match decode(&bytes) {
      Ok(Some((f, l))) => (Some(f), l),
      Ok(None) => (None, 0),
      Err(e) => panic!("{e:?}"),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (Some(OwnedFrame::Integer(48293)), 8);
    let bytes: Vec<u8> = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(OwnedFrame::SimpleString("string".into())), 9);
    let bytes: Vec<u8> = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (Some(OwnedFrame::SimpleString("string".into())), 9);
    let bytes: Vec<u8> = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(OwnedFrame::BulkString("foo".into())), 9);
    let bytes: Vec<u8> = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (Some(OwnedFrame::BulkString("foo".into())), 9);
    let bytes: Vec<u8> = "$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(OwnedFrame::Array(vec![
        OwnedFrame::SimpleString("Foo".into()),
        OwnedFrame::SimpleString("Bar".into()),
      ])),
      16,
    );
    let bytes: Vec<u8> = "*2\r\n+Foo\r\n+Bar\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_nulls() {
    let bytes: Vec<u8> = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();

    let expected = (
      Some(OwnedFrame::Array(vec![
        OwnedFrame::BulkString("Foo".into()),
        OwnedFrame::Null,
        OwnedFrame::BulkString("Bar".into()),
      ])),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_normal_error() {
    let bytes: Vec<u8> = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".into();
    let expected = (
      Some(OwnedFrame::Error(
        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
      )),
      bytes.len(),
    );

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_moved_error() {
    let bytes: Vec<u8> = "-MOVED 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(OwnedFrame::Error("MOVED 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let bytes: Vec<u8> = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(OwnedFrame::Error("ASK 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_incomplete() {
    let bytes: Vec<u8> = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar".into();
    decode_and_verify_none(&bytes);
  }

  #[test]
  #[should_panic]
  fn should_error_on_junk() {
    decode("foobarbazwibblewobble".as_bytes()).unwrap();
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
mod bytes_tests {
  use super::*;
  use nom::AsBytes;

  pub const PADDING: &str = "FOOBARBAZ";

  fn decode_and_verify_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    let total_len = bytes.len();

    let (frame, len, buf) = match decode_bytes_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => panic!("Failed to decode bytes. None returned"),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
    assert_eq!(buf.len(), expected.1, "output buffer len matched");
    assert_eq!(buf.len() + bytes.len(), total_len, "total len matched");
  }

  fn decode_and_verify_padded_some(bytes: &Bytes, expected: &(Option<BytesFrame>, usize)) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    bytes.extend_from_slice(PADDING.as_bytes());
    let total_len = bytes.len();

    let (frame, len, buf) = match decode_bytes_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => panic!("Failed to decode bytes. None returned"),
      Err(e) => panic!("{e:?}"),
    };

    assert_eq!(frame, expected.0, "decoded frame matched");
    assert_eq!(len, expected.1, "decoded frame len matched");
    assert_eq!(buf.len(), expected.1, "output buffer len matched");
    assert_eq!(buf.len() + bytes.len(), total_len, "total len matched");
  }

  fn decode_and_verify_none(bytes: &Bytes) {
    let mut bytes = BytesMut::from(bytes.as_bytes());
    let (frame, len, buf) = match decode_bytes_mut(&mut bytes) {
      Ok(Some((f, l, b))) => (Some(f), l, b),
      Ok(None) => (None, 0, Bytes::new()),
      Err(e) => panic!("{e:?}"),
    };

    assert!(frame.is_none());
    assert_eq!(len, 0);
    assert!(buf.is_empty());
  }

  #[test]
  fn should_decode_llen_res_example() {
    let expected = (Some(BytesFrame::Integer(48293)), 8);
    let bytes: Bytes = ":48293\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_simple_string() {
    let expected = (Some(BytesFrame::SimpleString("string".into())), 9);
    let bytes: Bytes = "+string\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_simple_string_incomplete() {
    let expected = (Some(BytesFrame::SimpleString("string".into())), 9);
    let bytes: Bytes = "+stri".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_bulk_string() {
    let expected = (Some(BytesFrame::BulkString("foo".into())), 9);
    let bytes: Bytes = "$3\r\nfoo\r\n".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  #[should_panic]
  fn should_decode_bulk_string_incomplete() {
    let expected = (Some(BytesFrame::BulkString("foo".into())), 9);
    let bytes: Bytes = "$3\r\nfo".into();

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_array_no_nulls() {
    let expected = (
      Some(BytesFrame::Array(vec![
        BytesFrame::SimpleString("Foo".into()),
        BytesFrame::SimpleString("Bar".into()),
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
      Some(BytesFrame::Array(vec![
        BytesFrame::BulkString("Foo".into()),
        BytesFrame::Null,
        BytesFrame::BulkString("Bar".into()),
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
      Some(BytesFrame::Error(
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
    let expected = (Some(BytesFrame::Error("MOVED 3999 127.0.0.1:6381".into())), bytes.len());

    decode_and_verify_some(&bytes, &expected);
    decode_and_verify_padded_some(&bytes, &expected);
  }

  #[test]
  fn should_decode_ask_error() {
    let bytes: Bytes = "-ASK 3999 127.0.0.1:6381\r\n".into();
    let expected = (Some(BytesFrame::Error("ASK 3999 127.0.0.1:6381".into())), bytes.len());

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
    decode_bytes_mut(&mut bytes).unwrap();
  }
}

#[cfg(test)]
mod depth_tests {
  use super::*;

  fn nested_array(depth: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(depth * 4 + 4);
    for _ in 0 .. depth {
      buf.extend_from_slice(b"*1\r\n");
    }
    buf.extend_from_slice(b"+e\r\n");
    buf
  }

  #[test]
  fn should_decode_at_max_depth() {
    let buf = nested_array(MAX_DECODE_DEPTH);
    let (_, amt) = decode(&buf).unwrap().unwrap();
    assert_eq!(amt, buf.len());
  }

  #[test]
  fn should_error_past_max_depth() {
    let buf = nested_array(MAX_DECODE_DEPTH + 1);
    assert!(decode(&buf).is_err());
  }

  #[test]
  fn should_error_on_incomplete_frame_past_max_depth() {
    let mut buf = nested_array(MAX_DECODE_DEPTH + 1);
    buf.truncate(buf.len() - 4);
    assert!(decode(&buf).is_err());
  }

  #[test]
  fn should_not_overflow_stack_with_deep_nesting() {
    // https://github.com/aembke/redis-protocol.rs/issues/45
    std::thread::Builder::new()
      .stack_size(1024 * 1024)
      .spawn(|| {
        let buf = nested_array(100_000);
        assert!(decode(&buf).is_err());
      })
      .unwrap()
      .join()
      .unwrap();
  }
}

#[cfg(test)]
mod null_array_tests {
  use super::*;

  #[test]
  fn should_decode_null_array() {
    // https://github.com/aembke/redis-protocol.rs/issues/44
    let (frame, amt) = decode(b"*-1\r\n").unwrap().unwrap();
    assert_eq!(frame, OwnedFrame::NullArray);
    assert_eq!(amt, 5);
    assert_eq!(frame.kind(), FrameKind::NullArray);
  }

  #[test]
  fn should_decode_null_bulkstring_as_null() {
    let (frame, _) = decode(b"$-1\r\n").unwrap().unwrap();
    assert_eq!(frame, OwnedFrame::Null);
  }

  #[test]
  fn should_decode_nested_null_array() {
    let (frame, amt) = decode(b"*2\r\n*-1\r\n$-1\r\n").unwrap().unwrap();
    assert_eq!(frame, OwnedFrame::Array(vec![OwnedFrame::NullArray, OwnedFrame::Null]));
    assert_eq!(amt, 14);
  }

  #[test]
  #[cfg(feature = "bytes")]
  fn should_decode_bytes_null_array() {
    let mut buf: BytesMut = "*-1\r\n".into();
    let (frame, amt, _) = decode_bytes_mut(&mut buf).unwrap().unwrap();
    assert_eq!(frame, BytesFrame::NullArray);
    assert_eq!(amt, 5);
    assert!(buf.is_empty());
  }

  #[test]
  #[cfg(feature = "resp3")]
  fn should_convert_null_array_to_resp3_null() {
    assert_eq!(
      OwnedFrame::NullArray.into_resp3(),
      crate::resp3::types::OwnedFrame::Null
    );
  }

  #[test]
  #[cfg(feature = "convert")]
  fn should_convert_null_array_to_none_or_empty() {
    use crate::convert::FromResp2;
    assert_eq!(
      Option::<alloc::string::String>::from_frame(OwnedFrame::NullArray).unwrap(),
      None
    );
    assert!(Vec::<alloc::string::String>::from_frame(OwnedFrame::NullArray)
      .unwrap()
      .is_empty());
  }
}

#[cfg(test)]
mod scan_tests {
  use super::*;
  use alloc::{format, vec::Vec};

  struct Rng(u64);
  impl Rng {
    fn next(&mut self) -> u64 {
      self.0 ^= self.0 << 13;
      self.0 ^= self.0 >> 7;
      self.0 ^= self.0 << 17;
      self.0
    }

    fn below(&mut self, n: u64) -> u64 {
      self.next() % n
    }
  }

  /// Bytes that must be skipped by length rather than parsed: CRLF and every type byte.
  const NOISE: &[u8] = b"ab\r\n*$:-+%|>~;._?H01";

  fn bulkstring(rng: &mut Rng, out: &mut Vec<u8>) {
    let len = rng.below(200) as usize;
    out.extend_from_slice(format!("${len}\r\n").as_bytes());
    for _ in 0 .. len {
      out.push(NOISE[rng.below(NOISE.len() as u64) as usize]);
    }
    out.extend_from_slice(b"\r\n");
  }

  fn frame(rng: &mut Rng, depth: u32, out: &mut Vec<u8>) {
    match rng.below(if depth == 0 { 7 } else { 10 }) {
      0 => out.extend_from_slice(b"+OK\r\n"),
      1 => out.extend_from_slice(b"-ERR wrong type\r\n"),
      2 => out.extend_from_slice(format!(":{}\r\n", rng.next() as i64).as_bytes()),
      3 => out.extend_from_slice(b"$-1\r\n"),
      4 => out.extend_from_slice(b"*-1\r\n"),
      5 | 6 => bulkstring(rng, out),
      7 => out.extend_from_slice(b"*0\r\n"),
      _ => {
        let len = 1 + rng.below(5);
        out.extend_from_slice(format!("*{len}\r\n").as_bytes());
        for _ in 0 .. len {
          frame(rng, depth - 1, out);
        }
      },
    }
  }

  fn decode_len(buf: &[u8]) -> Result<Option<usize>, RedisProtocolError> {
    decode_range(buf).map(|r| r.map(|(_, amt)| amt))
  }

  /// Feed `wire` to a scanner in random chunks, checking that each result matches the decoder's.
  fn check(rng: &mut Rng, wire: &[u8]) {
    let max_chunk = [1, 3, 16, 256, 4096][rng.below(5) as usize];
    let mut scanner = FrameScanner::new();
    let mut buf = Vec::new();
    let mut offset = 0;

    while offset < wire.len() {
      let end = (offset + 1 + rng.below(max_chunk) as usize).min(wire.len());
      buf.extend_from_slice(&wire[offset .. end]);
      offset = end;

      loop {
        match (scanner.scan(&buf), decode_len(&buf)) {
          (Scan::Incomplete, Ok(None)) => break,
          (Scan::Complete(amt), Ok(Some(expected))) if amt == expected => {
            buf.drain(.. amt);
          },
          (Scan::Invalid, Err(_)) => return,
          (scan, decoded) => panic!(
            "scan returned {:?}, decoder returned {:?}, buffer: {:?}",
            scan,
            decoded,
            String::from_utf8_lossy(&buf)
          ),
        }
      }
    }
  }

  #[test]
  fn should_match_decoder_on_chunked_input() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0 .. 5000 {
      let mut wire = Vec::new();
      for _ in 0 .. 1 + rng.below(3) {
        frame(&mut rng, 3, &mut wire);
      }
      check(&mut rng, &wire);
    }
  }

  #[test]
  fn should_match_decoder_on_corrupted_input() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    for _ in 0 .. 5000 {
      let mut wire = Vec::new();
      for _ in 0 .. 1 + rng.below(3) {
        frame(&mut rng, 3, &mut wire);
      }
      for _ in 0 .. 1 + rng.below(2) {
        let idx = rng.below(wire.len() as u64) as usize;
        wire[idx] = NOISE[rng.below(NOISE.len() as u64) as usize];
      }
      check(&mut rng, &wire);
    }
  }

  #[test]
  fn should_match_decoder_at_max_depth() {
    let mut rng = Rng(7);
    for depth in [
      MAX_DECODE_DEPTH - 1,
      MAX_DECODE_DEPTH,
      MAX_DECODE_DEPTH + 1,
      MAX_DECODE_DEPTH + 2,
    ] {
      for tail in [&b":1\r\n"[..], b"*0\r\n", b"*-1\r\n", b"*1\r\n"] {
        let mut wire = b"*1\r\n".repeat(depth);
        wire.extend_from_slice(tail);
        for _ in 0 .. 4 {
          check(&mut rng, &wire);
        }
      }
    }
  }

  #[test]
  fn should_read_each_byte_a_bounded_number_of_times() {
    // a large array of bulk strings, fed one byte at a time. the scanner's progress is visible through `pos`, which
    // should only move forward.
    let mut wire = b"*1000\r\n".to_vec();
    let mut rng = Rng(11);
    for _ in 0 .. 1000 {
      bulkstring(&mut rng, &mut wire);
    }
    let mut scanner = FrameScanner::new();
    let mut last_pos = 0;
    for end in 1 .. wire.len() {
      assert_eq!(scanner.scan(&wire[.. end]), Scan::Incomplete);
      assert!(scanner.pos >= last_pos);
      last_pos = scanner.pos;
    }
    assert_eq!(scanner.scan(&wire), Scan::Complete(wire.len()));
  }

  #[test]
  fn should_find_long_simple_strings_incrementally() {
    let mut wire = b"+".to_vec();
    wire.extend(core::iter::repeat_n(b'a', 10_000));
    wire.extend_from_slice(b"\r\n");
    let mut scanner = FrameScanner::new();
    for end in 1 .. wire.len() {
      assert_eq!(scanner.scan(&wire[.. end]), Scan::Incomplete);
      assert_eq!(scanner.searched, end);
    }
    assert_eq!(scanner.scan(&wire), Scan::Complete(wire.len()));
  }

  #[test]
  fn should_reset_after_complete_frames() {
    let mut scanner = FrameScanner::new();
    assert_eq!(scanner.scan(b"*2\r\n:1\r\n"), Scan::Incomplete);
    assert_eq!(scanner.scan(b"*2\r\n:1\r\n:2\r\n+OK\r\n"), Scan::Complete(12));
    assert_eq!(scanner.scan(b"+OK\r\n"), Scan::Complete(5));
    assert_eq!(scanner.scan(b"&1\r\n"), Scan::Invalid);
    assert_eq!(scanner.scan(b"+OK\r\n"), Scan::Complete(5));
  }

  #[test]
  fn should_restart_when_buffer_shrinks_without_reset() {
    let mut scanner = FrameScanner::new();
    assert_eq!(scanner.scan(b"*2\r\n$10\r\nabc"), Scan::Incomplete);
    assert_eq!(scanner.scan(b"+O"), Scan::Incomplete);
    assert_eq!(scanner.scan(b"+OK\r\n"), Scan::Complete(5));
  }
}
