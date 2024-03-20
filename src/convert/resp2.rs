use crate::{
  error::RedisProtocolError,
  resp2::types::{OwnedFrame, Resp2Frame},
};
use alloc::vec::Vec;
use core::str;

#[cfg(feature = "bytes")]
use crate::resp2::types::BytesFrame;

macro_rules! to_signed_number(
  ($f:ty, $t:ty, $v:expr) => {
    match $v {
      $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
      $f::Integer(i) => Ok(i as $t),
      $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(s)
        .map_err(RedisProtocolError::from)
        .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
      $f::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
          $f::Integer(i) => Ok(i as $t),
          $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(s)
            .map_err(RedisProtocolError::from)
            .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
          $f::Null => Err(RedisProtocolError::new_decode("Cannot convert nil to number.")),
          _ => Err(RedisProtocolError::new_decode("Cannot convert to number."))
        }
      }else{
        Err(RedisProtocolError::new_decode("Cannot convert array to number."))
      }
      $f::Null => Err(RedisProtocolError::new_decode("Cannot convert nil to number.")),
      _ => Err(RedisProtocolError::new_decode("Cannot convert to number.")),
    }
  }
);

macro_rules! to_unsigned_number(
  ($f:ty, $t:ty, $v:expr) => {
    match $v {
      $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
      $f::Integer(i) => if i >= 0 {
        Ok(i as $t)
      }else{
        Err(RedisProtocolError::new_decode("Cannot convert from negative number"))
      },
      $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(s)
        .map_err(RedisProtocolError::from)
        .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
      $f::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
          $f::Integer(i) => if i >= 0 {
            Ok(i as $t)
          }else{
            Err(RedisProtocolError::new_decode("Cannot convert from negative number"))
          },
          $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(s)
            .map_err(RedisProtocolError::from)
            .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
          $f::Null => Err(RedisProtocolError::new_decode("Cannot convert nil to number.")),
          _ => Err(RedisProtocolError::new_decode("Cannot convert to number."))
        }
      }else{
        Err(RedisProtocolError::new_decode("Cannot convert array to number."))
      }
      $f::Null => Err(RedisProtocolError::new_decode("Cannot convert nil to number.")),
      _ => Err(RedisProtocolError::new_decode("Cannot convert to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl FromResp2<OwnedFrame> for $t {
      fn from_value(value: OwnedFrame) -> Result<$t, RedisError> {
        to_signed_number!(OwnedFrame, $t, value)
      }
    }

    #[cfg(feature = "bytes")]
    #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
    impl FromResp2<BytesFrame> for $t {
      fn from_value(value: BytesFrame) -> Result<$t, RedisError> {
        to_signed_number!(BytesFrame, $t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl FromResp2<OwnedFrame> for $t {
      fn from_value(value: OwnedFrame) -> Result<$t, RedisError> {
        to_unsigned_number!(OwnedFrame, $t, value)
      }
    }

    #[cfg(feature = "bytes")]
    #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
    impl FromResp2<BytesFrame> for $t {
      fn from_value(value: BytesFrame) -> Result<$t, RedisError> {
        to_unsigned_number!(BytesFrame, $t, value)
      }
    }
  }
);

/// A trait used to convert frames into various other types.
pub trait FromResp2<F: Resp2Frame>: Sized {
  /// Convert a frame to the destination type.
  fn from_frame(frame: F) -> Result<Self, RedisProtocolError>;

  /// Convert multiple frames to the destination type.
  fn from_frames(frames: Vec<F>) -> Result<Vec<Self>, RedisProtocolError> {
    frames.into_iter().map(Self::from_frame).collect()
  }

  // Optional functions that can be used to specialize converting into certain types. Currently we want to specialize
  // for `Vec<u8>` and tuples.

  #[doc(hidden)]
  fn is_tuple(&self) -> bool {
    false
  }

  #[doc(hidden)]
  fn from_owned_bytes(_: Vec<u8>) -> Option<Vec<Self>> {
    None
  }
}

impl FromResp2<OwnedFrame> for () {
  fn from_frame(_: OwnedFrame) -> Result<Self, RedisProtocolError> {
    Ok(())
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for () {
  fn from_frame(_: BytesFrame) -> Result<Self, RedisProtocolError> {
    Ok(_)
  }
}

impl FromResp2<OwnedFrame> for OwnedFrame {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    Ok(frame)
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for BytesFrame {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    Ok(frame)
  }
}
