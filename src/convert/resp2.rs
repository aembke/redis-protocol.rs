use crate::{
  error::RedisProtocolError,
  resp2::types::{OwnedFrame, Resp2Frame},
};
use alloc::{string::String, vec::Vec};
use core::str;

#[cfg(feature = "bytes")]
use crate::resp2::types::BytesFrame;
#[cfg(feature = "bytes")]
use bytes::Bytes;
#[cfg(feature = "bytes")]
use bytes_utils::Str;

macro_rules! to_signed_number(
  ($f:tt, $t:ty, $v:expr) => {
    match $v {
      $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
      $f::Integer(i) => Ok(i as $t),
      $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(&s)
        .map_err(RedisProtocolError::from)
        .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
      $f::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
          $f::Integer(i) => Ok(i as $t),
          $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(&s)
            .map_err(RedisProtocolError::from)
            .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
          $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
          _ => Err(RedisProtocolError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert array to number."))
      }
      $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
    }
  }
);

macro_rules! to_unsigned_number(
  ($f:tt, $t:ty, $v:expr) => {
    match $v {
      $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
      $f::Integer(i) => if i >= 0 {
        Ok(i as $t)
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert from negative number"))
      },
      $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(&s)
        .map_err(RedisProtocolError::from)
        .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
      $f::Array(mut a) => if a.len() == 1 {
        match a.pop().unwrap() {
          $f::Error(s) => s.parse::<$t>().map_err(|e| e.into()),
          $f::Integer(i) => if i >= 0 {
            Ok(i as $t)
          }else{
            Err(RedisProtocolError::new_parse("Cannot convert from negative number"))
          },
          $f::SimpleString(s) | $f::BulkString(s) => str::from_utf8(&s)
            .map_err(RedisProtocolError::from)
            .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
          $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
          _ => Err(RedisProtocolError::new_parse("Cannot convert to number."))
        }
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert array to number."))
      }
      $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl FromResp2<OwnedFrame> for $t {
      fn from_frame(value: OwnedFrame) -> Result<$t, RedisProtocolError> {
        to_signed_number!(OwnedFrame, $t, value)
      }
    }

    #[cfg(feature = "bytes")]
    #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
    impl FromResp2<BytesFrame> for $t {
      fn from_frame(value: BytesFrame) -> Result<$t, RedisProtocolError> {
        to_signed_number!(BytesFrame, $t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl FromResp2<OwnedFrame> for $t {
      fn from_frame(value: OwnedFrame) -> Result<$t, RedisProtocolError> {
        to_unsigned_number!(OwnedFrame, $t, value)
      }
    }

    #[cfg(feature = "bytes")]
    #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
    impl FromResp2<BytesFrame> for $t {
      fn from_frame(value: BytesFrame) -> Result<$t, RedisProtocolError> {
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

  // Optional functions that can be used to specialize converting into certain types. Currently, we want to specialize
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

impl_signed_number!(i8);
impl_signed_number!(i16);
impl_signed_number!(i32);
impl_signed_number!(i64);
impl_signed_number!(i128);
impl_signed_number!(isize);

impl FromResp2<OwnedFrame> for u8 {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    to_unsigned_number!(OwnedFrame, u8, frame)
  }

  fn from_owned_bytes(d: Vec<u8>) -> Option<Vec<Self>> {
    Some(d)
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for u8 {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    to_unsigned_number!(BytesFrame, u8, frame)
  }

  fn from_owned_bytes(d: Vec<u8>) -> Option<Vec<Self>> {
    Some(d)
  }
}

impl_unsigned_number!(u16);
impl_unsigned_number!(u32);
impl_unsigned_number!(u64);
impl_unsigned_number!(u128);
impl_unsigned_number!(usize);

impl FromResp2<OwnedFrame> for () {
  fn from_frame(_: OwnedFrame) -> Result<Self, RedisProtocolError> {
    Ok(())
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for () {
  fn from_frame(_: BytesFrame) -> Result<Self, RedisProtocolError> {
    Ok(())
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

impl FromResp2<OwnedFrame> for String {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    Ok(match frame {
      OwnedFrame::BulkString(b) | OwnedFrame::SimpleString(b) => String::from_utf8(b)?,
      OwnedFrame::Error(s) => s,
      OwnedFrame::Integer(i) => i.to_string(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for String {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    Ok(match frame {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => String::from_utf8(b.to_vec())?,
      BytesFrame::Error(s) => s.to_string(),
      BytesFrame::Integer(i) => i.to_string(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}

impl<T> FromResp2<OwnedFrame> for Option<T>
where
  T: FromResp2<OwnedFrame>,
{
  fn from_frame(frame: OwnedFrame) -> Result<Option<T>, RedisProtocolError> {
    debug_type!("FromResp(Option<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      OwnedFrame::Array(inner) => {
        if inner.is_empty() {
          Ok(None)
        } else {
          T::from_frame(OwnedFrame::Array(inner)).map(Some)
        }
      },
      OwnedFrame::Null => Ok(None),
      _ => T::from_frame(frame).map(Some),
    }
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<T> FromResp2<BytesFrame> for Option<T>
where
  T: FromResp2<BytesFrame>,
{
  fn from_frame(frame: BytesFrame) -> Result<Option<T>, RedisProtocolError> {
    debug_type!("FromResp(Option<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      BytesFrame::Array(inner) => {
        if inner.is_empty() {
          Ok(None)
        } else {
          T::from_frame(BytesFrame::Array(inner)).map(Some)
        }
      },
      BytesFrame::Null => Ok(None),
      _ => T::from_frame(frame).map(Some),
    }
  }
}
