use crate::{
  error::RedisProtocolError,
  resp2::types::{OwnedFrame, RangeFrame},
  utils::digits_in_number,
};
use alloc::{borrow::ToOwned, string::String, vec::Vec};

#[cfg(feature = "bytes")]
use crate::resp2::types::BytesFrame;
#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};
#[cfg(feature = "bytes")]
use bytes_utils::Str;

pub fn bulkstring_encode_len(b: &[u8]) -> usize {
  1 + digits_in_number(b.len()) + 2 + b.len() + 2
}

pub fn simplestring_encode_len(s: &[u8]) -> usize {
  1 + s.len() + 2
}

pub fn error_encode_len(s: &str) -> usize {
  1 + s.as_bytes().len() + 2
}

pub fn integer_encode_len(i: i64) -> usize {
  let prefix = if i < 0 { 1 } else { 0 };
  let as_usize = if i < 0 { -i as usize } else { i as usize };

  1 + digits_in_number(as_usize) + 2 + prefix
}

/// Move or copy the contents of `buf` based on the ranges in the provided frame.
pub fn build_owned_frame(buf: &[u8], frame: &RangeFrame) -> Result<OwnedFrame, RedisProtocolError> {
  Ok(match frame {
    RangeFrame::Error((start, end)) => OwnedFrame::Error(String::from_utf8(buf[*start .. *end].to_vec())?),
    RangeFrame::SimpleString((start, end)) => OwnedFrame::SimpleString(buf[*start .. *end].to_owned()),
    RangeFrame::BulkString((start, end)) => OwnedFrame::BulkString(buf[*start .. *end].to_owned()),
    RangeFrame::Integer(i) => OwnedFrame::Integer(*i),
    RangeFrame::Null => OwnedFrame::Null,
    RangeFrame::Array(frames) => {
      // FIXME is there a cleaner way to do this with results?
      let mut out = Vec::with_capacity(frames.len());
      for frame in frames.iter() {
        out.push(build_owned_frame(buf, frame)?);
      }
      OwnedFrame::Array(out)
    },
  })
}

/// Use the `Bytes` interface to create owned views into the provided buffer for each of the provided range frames.
#[cfg(feature = "bytes")]
pub fn build_bytes_frame(buf: &Bytes, frame: &RangeFrame) -> Result<BytesFrame, RedisProtocolError> {
  Ok(match frame {
    RangeFrame::Error((start, end)) => {
      let bytes = buf.slice(start .. end);
      BytesFrame::Error(Str::from_inner(bytes)?)
    },
    RangeFrame::SimpleString((start, end)) => {
      let bytes = buf.slice(start .. end);
      BytesFrame::SimpleString(bytes)
    },
    RangeFrame::BulkString((start, end)) => {
      let bytes = buf.slice(start .. end);
      BytesFrame::BulkString(bytes)
    },
    RangeFrame::Integer(i) => BytesFrame::Integer(*i),
    RangeFrame::Null => BytesFrame::Null,
    RangeFrame::Array(frames) => {
      // FIXME is there a cleaner way to do this with results?
      let mut out = Vec::with_capacity(frames.len());
      for frame in frames.iter() {
        out.push(build_bytes_frame(buf, frame)?);
      }
      BytesFrame::Array(out)
    },
  })
}

/// Split off and [freeze](bytes::BytesMut::freeze) `amt` bytes from `buf` and return owned views into the buffer
/// based on the provided range frame.
///
/// The returned `Bytes` represents the `Bytes` buffer that was sliced off `buf`. The returned frames hold
/// owned `Bytes` views to slices within this buffer.
#[cfg(feature = "bytes")]
pub fn freeze_parse(
  buf: &mut BytesMut,
  frame: &RangeFrame,
  amt: usize,
) -> Result<(BytesFrame, Bytes), RedisProtocolError> {
  let buffer = buf.split_to(amt).freeze();
  let frame = build_bytes_frame(&buffer, frame)?;
  Ok((frame, buffer))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_get_encode_len_simplestring() {
    let ss1 = "Ok";
    let ss2 = "FooBarBaz";
    let ss3 = "-&#$@9232";

    assert_eq!(simplestring_encode_len(ss1.as_bytes()), 5);
    assert_eq!(simplestring_encode_len(ss2.as_bytes()), 12);
    assert_eq!(simplestring_encode_len(ss3.as_bytes()), 12);
  }

  #[test]
  fn should_get_encode_len_error() {
    let e1 = "MOVED 3999 127.0.0.1:6381";
    let e2 = "ERR unknown command 'foobar'";
    let e3 = "WRONGTYPE Operation against a key holding the wrong kind of value";

    assert_eq!(error_encode_len(e1), 28);
    assert_eq!(error_encode_len(e2), 31);
    assert_eq!(error_encode_len(e3), 68);
  }

  #[test]
  fn should_get_encode_len_integer() {
    assert_eq!(integer_encode_len(38473), 8);
    assert_eq!(integer_encode_len(-74834), 9);
  }
}
