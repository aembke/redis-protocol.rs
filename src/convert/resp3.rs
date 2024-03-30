use crate::{
  error::RedisProtocolError,
  resp3::types::{OwnedFrame, Resp3Frame},
};
use alloc::{
  format,
  string::{String, ToString},
  vec,
  vec::Vec,
};
use core::{
  hash::{BuildHasher, Hash},
  str,
};

#[cfg(feature = "bytes")]
use crate::resp3::types::BytesFrame;
#[cfg(feature = "bytes")]
use bytes::Bytes;
#[cfg(feature = "bytes")]
use bytes_utils::Str;

use crate::resp3::types::FrameKind;
#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};
#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

macro_rules! to_signed_number(
  ($f:tt, $t:ty, $v:expr) => {
    match $v {
      $f::SimpleError { data, .. } => data.parse::<$t>().map_err(|e| e.into()),
      $f::Number { data, .. } => Ok(data as $t),
      $f::Double { data, .. } => Ok(data as $t),
      $f::SimpleString { data, .. }
        | $f::BlobString { data, .. }
        | $f::VerbatimString { data, .. }
        | $f::BigNumber { data, .. }
        | $f::ChunkedString(data) => str::from_utf8(&data)
          .map_err(RedisProtocolError::from)
          .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
      $f::Array { mut data, .. } => if data.len() == 1 {
        match data.pop().unwrap() {
          $f::SimpleError { data, .. } => data.parse::<$t>().map_err(|e| e.into()),
          $f::Number { data, .. } => Ok(data as $t),
          $f::Double { data, .. } => Ok(data as $t),
          $f::SimpleString { data, .. }
           | $f::BlobString { data, .. }
           | $f::VerbatimString { data, .. }
           | $f::BigNumber { data, .. }
           | $f::ChunkedString(data) => str::from_utf8(&data)
              .map_err(RedisProtocolError::from)
              .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
          $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
          _ => Err(RedisProtocolError::new_parse("Cannot convert to number.")),
        }
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert array to number."))
      }
      $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
      _ => Err(RedisProtocolError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! to_unsigned_number(
  ($f:tt, $t:ty, $v:expr) => {
    match $v {
      $f::SimpleError { data, .. } => data.parse::<$t>().map_err(|e| e.into()),
      $f::Number { data, .. } => if data >= 0 {
        Ok(data as $t)
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert from negative number"))
      },
      $f::Double { data, .. } => if data.is_sign_positive() {
        Ok(data as $t)
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert from negative number"))
      },
      $f::SimpleString { data, .. }
        | $f::BlobString { data, .. }
        | $f::VerbatimString { data, .. }
        | $f::BigNumber { data, .. }
        | $f::ChunkedString(data) => str::from_utf8(&data)
          .map_err(RedisProtocolError::from)
          .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
      $f::Array { mut data, .. } => if data.len() == 1 {
        match data.pop().unwrap() {
          $f::SimpleError { data, .. } => data.parse::<$t>().map_err(|e| e.into()),
          $f::Number { data, .. } => if data >= 0 {
            Ok(data as $t)
          }else{
            Err(RedisProtocolError::new_parse("Cannot convert from negative number"))
          },
          $f::Double { data, .. } => if data.is_sign_positive() {
            Ok(data as $t)
          }else{
            Err(RedisProtocolError::new_parse("Cannot convert from negative number"))
          },
          $f::SimpleString { data, .. }
           | $f::BlobString { data, .. }
           | $f::VerbatimString { data, .. }
           | $f::BigNumber { data, .. }
           | $f::ChunkedString(data) => str::from_utf8(&data)
              .map_err(RedisProtocolError::from)
              .and_then(|s| s.parse::<$t>().map_err(RedisProtocolError::from)),
          $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
          _ => Err(RedisProtocolError::new_parse("Cannot convert to number.")),
        }
      }else{
        Err(RedisProtocolError::new_parse("Cannot convert array to number."))
      }
      $f::Null => Err(RedisProtocolError::new_parse("Cannot convert nil to number.")),
      _ => Err(RedisProtocolError::new_parse("Cannot convert to number.")),
    }
  }
);

macro_rules! impl_signed_number (
  ($t:ty) => {
    impl FromResp3<OwnedFrame> for $t {
      fn from_frame(value: OwnedFrame) -> Result<$t, RedisProtocolError> {
        check_single_vec_reply!(value);
        to_signed_number!(OwnedFrame, $t, value)
      }
    }

    #[cfg(feature = "bytes")]
    #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
    impl FromResp3<BytesFrame> for $t {
      fn from_frame(value: BytesFrame) -> Result<$t, RedisProtocolError> {
        check_single_vec_reply!(value);
        to_signed_number!(BytesFrame, $t, value)
      }
    }
  }
);

macro_rules! impl_unsigned_number (
  ($t:ty) => {
    impl FromResp3<OwnedFrame> for $t {
      fn from_frame(value: OwnedFrame) -> Result<$t, RedisProtocolError> {
        check_single_vec_reply!(value);
        to_unsigned_number!(OwnedFrame, $t, value)
      }
    }

    #[cfg(feature = "bytes")]
    #[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
    impl FromResp3<BytesFrame> for $t {
      fn from_frame(value: BytesFrame) -> Result<$t, RedisProtocolError> {
        check_single_vec_reply!(value);
        to_unsigned_number!(BytesFrame, $t, value)
      }
    }
  }
);

/// A trait used to convert frames into various other types.
pub trait FromResp3<F: Resp3Frame>: Sized {
  /// Convert a frame to the destination type.
  fn from_frame(frame: F) -> Result<Self, RedisProtocolError>;

  /// Convert multiple frames to the destination type.
  fn from_frames(frames: Vec<F>) -> Result<Vec<Self>, RedisProtocolError> {
    frames.into_iter().map(Self::from_frame).collect()
  }

  // Optional functions that can be used to specialize converting into certain types. Currently, we want to specialize
  // for `Vec<u8>` and tuples.

  #[doc(hidden)]
  fn is_tuple() -> bool {
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

impl FromResp3<OwnedFrame> for u8 {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);
    to_unsigned_number!(OwnedFrame, u8, frame)
  }

  fn from_owned_bytes(d: Vec<u8>) -> Option<Vec<Self>> {
    Some(d)
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for u8 {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);
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

impl FromResp3<OwnedFrame> for f64 {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);

    frame
      .as_f64()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for f64 {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);

    frame
      .as_f64()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}

impl FromResp3<OwnedFrame> for f32 {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);

    frame
      .as_f64()
      .map(|f| f as f32)
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for f32 {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);

    frame
      .as_f64()
      .map(|f| f as f32)
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}

impl FromResp3<OwnedFrame> for bool {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);

    frame
      .as_bool()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to bool"))
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for bool {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    check_single_vec_reply!(frame);

    frame
      .as_bool()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to bool"))
  }
}

impl FromResp3<OwnedFrame> for () {
  fn from_frame(_: OwnedFrame) -> Result<Self, RedisProtocolError> {
    Ok(())
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for () {
  fn from_frame(_: BytesFrame) -> Result<Self, RedisProtocolError> {
    Ok(())
  }
}

impl FromResp3<OwnedFrame> for OwnedFrame {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    Ok(frame)
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for BytesFrame {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    Ok(frame)
  }
}

impl FromResp3<OwnedFrame> for String {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp3(String): {:?}", frame);
    check_single_vec_reply!(frame);

    Ok(match frame {
      OwnedFrame::SimpleError { data, .. } => data,
      OwnedFrame::SimpleString { data, .. }
      | OwnedFrame::BlobString { data, .. }
      | OwnedFrame::BlobError { data, .. }
      | OwnedFrame::VerbatimString { data, .. }
      | OwnedFrame::BigNumber { data, .. }
      | OwnedFrame::ChunkedString(data) => String::from_utf8(data)?,
      OwnedFrame::Number { data, .. } => data.to_string(),
      OwnedFrame::Double { data, .. } => data.to_string(),
      OwnedFrame::Boolean { data, .. } => data.to_string(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for String {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp3(String): {:?}", frame);
    check_single_vec_reply!(frame);

    Ok(match frame {
      BytesFrame::SimpleError { data, .. } => data.to_string(),
      BytesFrame::SimpleString { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BlobError { data, .. }
      | BytesFrame::VerbatimString { data, .. }
      | BytesFrame::BigNumber { data, .. }
      | BytesFrame::ChunkedString(data) => String::from_utf8(data.to_vec())?,
      BytesFrame::Number { data, .. } => data.to_string(),
      BytesFrame::Double { data, .. } => data.to_string(),
      BytesFrame::Boolean { data, .. } => data.to_string(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for Str {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp3(Str): {:?}", frame);
    check_single_vec_reply!(frame);

    Ok(match frame {
      BytesFrame::SimpleError { data, .. } => data.to_string().into(),
      BytesFrame::SimpleString { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BlobError { data, .. }
      | BytesFrame::VerbatimString { data, .. }
      | BytesFrame::BigNumber { data, .. }
      | BytesFrame::ChunkedString(data) => Str::from_inner(data)?,
      BytesFrame::Number { data, .. } => data.to_string().into(),
      BytesFrame::Double { data, .. } => data.to_string().into(),
      BytesFrame::Boolean { data, .. } => data.to_string().into(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp3<BytesFrame> for Bytes {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp3(Bytes): {:?}", frame);
    check_single_vec_reply!(frame);

    Ok(match frame {
      BytesFrame::SimpleError { data, .. } => data.into_inner(),
      BytesFrame::SimpleString { data, .. }
      | BytesFrame::BlobString { data, .. }
      | BytesFrame::BlobError { data, .. }
      | BytesFrame::VerbatimString { data, .. }
      | BytesFrame::BigNumber { data, .. }
      | BytesFrame::ChunkedString(data) => data,
      BytesFrame::Number { data, .. } => data.to_string().into(),
      BytesFrame::Double { data, .. } => data.to_string().into(),
      BytesFrame::Boolean { data, .. } => data.to_string().into(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to bytes.")),
    })
  }
}

impl<T> FromResp3<OwnedFrame> for Option<T>
where
  T: FromResp3<OwnedFrame>,
{
  fn from_frame(frame: OwnedFrame) -> Result<Option<T>, RedisProtocolError> {
    debug_type!("FromResp3(Option<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      OwnedFrame::Array { data, attributes } => {
        if data.is_empty() {
          Ok(None)
        } else {
          T::from_frame(OwnedFrame::Array { data, attributes }).map(Some)
        }
      },
      OwnedFrame::Null => Ok(None),
      _ => T::from_frame(frame).map(Some),
    }
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<T> FromResp3<BytesFrame> for Option<T>
where
  T: FromResp3<BytesFrame>,
{
  fn from_frame(frame: BytesFrame) -> Result<Option<T>, RedisProtocolError> {
    debug_type!("FromResp3(Option<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      BytesFrame::Array { data, attributes } => {
        if data.is_empty() {
          Ok(None)
        } else {
          T::from_frame(BytesFrame::Array { data, attributes }).map(Some)
        }
      },
      BytesFrame::Null => Ok(None),
      _ => T::from_frame(frame).map(Some),
    }
  }
}

impl<T> FromResp3<OwnedFrame> for Vec<T>
where
  T: FromResp3<OwnedFrame>,
{
  fn from_frame(frame: OwnedFrame) -> Result<Vec<T>, RedisProtocolError> {
    debug_type!("FromResp3(Vec<{}>): {:?}", std::any::type_name::<T>(), frame);

    // TODO make a macro for the duplicated Vec<u8> variants
    match frame {
      OwnedFrame::BlobString { data, .. } => {
        // hacky way to check if T is bytes without consuming `string`
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::BlobString { data, attributes: None })?])
        }
      },
      OwnedFrame::BlobError { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::BlobError { data, attributes: None })?])
        }
      },
      OwnedFrame::SimpleString { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::SimpleString {
            data,
            attributes: None,
          })?])
        }
      },
      OwnedFrame::VerbatimString { data, format, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::VerbatimString {
            data,
            format,
            attributes: None,
          })?])
        }
      },
      OwnedFrame::BigNumber { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::BigNumber { data, attributes: None })?])
        }
      },
      OwnedFrame::SimpleError { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.into_bytes()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::SimpleError { data, attributes: None })?])
        }
      },
      OwnedFrame::ChunkedString(data) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::ChunkedString(data))?])
        }
      },
      OwnedFrame::Boolean { data, .. } => Ok(vec![T::from_frame(OwnedFrame::Boolean { data, attributes: None })?]),
      OwnedFrame::Number { data, .. } => Ok(vec![T::from_frame(OwnedFrame::Number { data, attributes: None })?]),
      OwnedFrame::Double { data, .. } => Ok(vec![T::from_frame(OwnedFrame::Double { data, attributes: None })?]),
      OwnedFrame::Null => Ok(Vec::new()),

      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => {
        if !data.is_empty() {
          let kind = data[0].kind();
          if matches!(kind, FrameKind::Array | FrameKind::Push) {
            data.into_iter().map(|x| T::from_frame(x)).collect()
          } else {
            T::from_frames(data)
          }
        } else {
          Ok(Vec::new())
        }
      },
      OwnedFrame::Set { data, .. } => data.into_iter().map(|x| T::from_frame(x)).collect(),
      OwnedFrame::Map { data, .. } => {
        let mut out = Vec::with_capacity(data.len() * 2);
        for (key, value) in data.into_iter() {
          out.push(T::from_frame(key)?);
          out.push(T::from_frame(value)?);
        }
        Ok(out)
      },
      _ => Err(RedisProtocolError::new_parse("Could not convert to array.")),
    }
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<T> FromResp3<BytesFrame> for Vec<T>
where
  T: FromResp3<BytesFrame>,
{
  fn from_frame(frame: BytesFrame) -> Result<Vec<T>, RedisProtocolError> {
    debug_type!("FromResp3(Vec<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      BytesFrame::BlobString { data, .. } => {
        // hacky way to check if T is bytes without consuming `string`
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::BlobString { data, attributes: None })?])
        }
      },
      BytesFrame::BlobError { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::BlobError { data, attributes: None })?])
        }
      },
      BytesFrame::SimpleString { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::SimpleString {
            data,
            attributes: None,
          })?])
        }
      },
      BytesFrame::VerbatimString { data, format, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::VerbatimString {
            data,
            format,
            attributes: None,
          })?])
        }
      },
      BytesFrame::BigNumber { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::BigNumber { data, attributes: None })?])
        }
      },
      BytesFrame::SimpleError { data, .. } => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.as_bytes().to_vec())
            .ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::SimpleError { data, attributes: None })?])
        }
      },
      BytesFrame::ChunkedString(data) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(data.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::ChunkedString(data))?])
        }
      },
      BytesFrame::Boolean { data, .. } => Ok(vec![T::from_frame(BytesFrame::Boolean { data, attributes: None })?]),
      BytesFrame::Number { data, .. } => Ok(vec![T::from_frame(BytesFrame::Number { data, attributes: None })?]),
      BytesFrame::Double { data, .. } => Ok(vec![T::from_frame(BytesFrame::Double { data, attributes: None })?]),
      BytesFrame::Null => Ok(Vec::new()),

      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => {
        if !data.is_empty() {
          let kind = data[0].kind();
          if matches!(kind, FrameKind::Array | FrameKind::Push) {
            data.into_iter().map(|x| T::from_frame(x)).collect()
          } else {
            T::from_frames(data)
          }
        } else {
          Ok(Vec::new())
        }
      },
      BytesFrame::Set { data, .. } => data.into_iter().map(|x| T::from_frame(x)).collect(),
      BytesFrame::Map { data, .. } => {
        let mut out = Vec::with_capacity(data.len() * 2);
        for (key, value) in data.into_iter() {
          out.push(T::from_frame(key)?);
          out.push(T::from_frame(value)?);
        }
        Ok(out)
      },
      _ => Err(RedisProtocolError::new_parse("Could not convert to array.")),
    }
  }
}

impl<T, const N: usize> FromResp3<OwnedFrame> for [T; N]
where
  T: FromResp3<OwnedFrame>,
{
  fn from_frame(value: OwnedFrame) -> Result<[T; N], RedisProtocolError> {
    debug_type!("FromResp3([{}; {}]): {:?}", std::any::type_name::<T>(), N, value);
    // use the `from_value` impl for Vec<T>
    let value: Vec<T> = value.convert()?;
    let len = value.len();

    value.try_into().map_err(|_| {
      RedisProtocolError::new_parse(format!("Failed to convert to array. Expected {}, found {}.", N, len))
    })
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<T, const N: usize> FromResp3<BytesFrame> for [T; N]
where
  T: FromResp3<BytesFrame>,
{
  fn from_frame(value: BytesFrame) -> Result<[T; N], RedisProtocolError> {
    debug_type!("FromResp3([{}; {}]): {:?}", std::any::type_name::<T>(), N, value);
    // use the `from_value` impl for Vec<T>
    let value: Vec<T> = value.convert()?;
    let len = value.len();

    value.try_into().map_err(|_| {
      RedisProtocolError::new_parse(format!("Failed to convert to array. Expected {}, found {}.", N, len))
    })
  }
}

impl<K, V, S> FromResp3<OwnedFrame> for HashMap<K, V, S>
where
  K: FromResp3<OwnedFrame> + Eq + Hash,
  V: FromResp3<OwnedFrame>,
  S: BuildHasher + Default,
{
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    debug_type!(
      "FromResp3(HashMap<{}, {}>): {:?}",
      std::any::type_name::<K>(),
      std::any::type_name::<V>(),
      frame
    );

    match frame {
      OwnedFrame::Array { mut data, .. } | OwnedFrame::Push { mut data, .. } => {
        if data.is_empty() {
          return Ok::<HashMap<K, V, S>, _>(HashMap::default());
        }

        if data.len() % 2 == 0 {
          let mut out = HashMap::default();
          out.reserve(data.len() / 2);

          #[allow(clippy::manual_while_let_some)]
          while !data.is_empty() {
            let value = data.pop().unwrap();
            let key = data.pop().unwrap();

            out.insert(K::from_frame(key)?, V::from_frame(value)?);
          }
          Ok(out)
        } else {
          Err(RedisProtocolError::new_parse("Expected even number of elements"))
        }
      },
      OwnedFrame::Map { data, .. } => data
        .into_iter()
        .map(|(k, v)| Ok((K::from_frame(k)?, V::from_frame(v)?)))
        .collect(),
      _ => Err(RedisProtocolError::new_parse("Cannot convert to map")),
    }
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<K, V, S> FromResp3<BytesFrame> for HashMap<K, V, S>
where
  K: FromResp3<BytesFrame> + Eq + Hash,
  V: FromResp3<BytesFrame>,
  S: BuildHasher + Default,
{
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!(
      "FromResp3(HashMap<{}, {}>): {:?}",
      std::any::type_name::<K>(),
      std::any::type_name::<V>(),
      frame
    );

    match frame {
      BytesFrame::Array { mut data, .. } | BytesFrame::Push { mut data, .. } => {
        if data.is_empty() {
          return Ok::<HashMap<K, V, S>, _>(HashMap::default());
        }

        if data.len() % 2 == 0 {
          let mut out = HashMap::default();
          out.reserve(data.len() / 2);

          #[allow(clippy::manual_while_let_some)]
          while !data.is_empty() {
            let value = data.pop().unwrap();
            let key = data.pop().unwrap();

            out.insert(K::from_frame(key)?, V::from_frame(value)?);
          }
          Ok(out)
        } else {
          Err(RedisProtocolError::new_parse("Expected even number of elements"))
        }
      },
      BytesFrame::Map { data, .. } => data
        .into_iter()
        .map(|(k, v)| Ok((K::from_frame(k)?, V::from_frame(v)?)))
        .collect(),
      _ => Err(RedisProtocolError::new_parse("Cannot convert to map")),
    }
  }
}

impl<V, S> FromResp3<OwnedFrame> for HashSet<V, S>
where
  V: FromResp3<OwnedFrame> + Hash + Eq,
  S: BuildHasher + Default,
{
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp3(HashSet<{}>): {:?}", std::any::type_name::<V>(), frame);

    match frame {
      OwnedFrame::Array { data, .. } | OwnedFrame::Push { data, .. } => data.into_iter().map(V::from_frame).collect(),
      OwnedFrame::Set { data, .. } => data.into_iter().map(V::from_frame).collect(),
      _ => Err(RedisProtocolError::new_parse("Cannot convert to set")),
    }
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<V, S> FromResp3<BytesFrame> for HashSet<V, S>
where
  V: FromResp3<BytesFrame> + Hash + Eq,
  S: BuildHasher + Default,
{
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp3(HashSet<{}>): {:?}", std::any::type_name::<V>(), frame);

    match frame {
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => data.into_iter().map(V::from_frame).collect(),
      BytesFrame::Set { data, .. } => data.into_iter().map(V::from_frame).collect(),
      _ => Err(RedisProtocolError::new_parse("Cannot convert to set")),
    }
  }
}

macro_rules! impl_from_resp3_tuple {
  () => ();
  ($($name:ident,)+) => (
    #[doc(hidden)]
    #[cfg(feature = "bytes")]
    impl<$($name: FromResp3<BytesFrame>),*> FromResp3<BytesFrame> for ($($name,)*) {
      fn is_tuple() -> bool {
        true
      }

      #[allow(non_snake_case, unused_variables)]
      fn from_frame(v: BytesFrame) -> Result<($($name,)*), RedisProtocolError> {
        let mut values: Vec<_> = match v {
          BytesFrame::Array { data, .. } => data,
          BytesFrame::Push { data, .. } => data,
          BytesFrame::Set { data, .. } => data.into_iter().collect(),
          _ => return Err(RedisProtocolError::new_parse("Could not convert to tuple."))
        };

        let mut n = 0;
        $(let $name = (); n += 1;)*
        debug_type!("FromResp3({}-tuple): {:?}", n, values);
        if values.len() != n {
          return Err(RedisProtocolError::new_parse(format!("Invalid tuple dimension. Expected {}, found {}.", n, values.len())));
        }

        values.reverse();
        Ok(($({let $name = (); values
          .pop()
          .ok_or(RedisProtocolError::new_parse("Expected value, found none."))?
          .convert()?
        },)*))
      }

      #[allow(non_snake_case, unused_variables)]
      fn from_frames(mut values: Vec<BytesFrame>) -> Result<Vec<($($name,)*)>, RedisProtocolError> {
        let mut n = 0;
        $(let $name = (); n += 1;)*
        debug_type!("FromResp3({}-tuple): {:?}", n, values);
        if values.len() % n != 0 {
          return Err(RedisProtocolError::new_parse(format!("Invalid tuple dimension. Expected {}, found {}.", n, values.len())));
        }

        let mut out = Vec::with_capacity(values.len() / n);
        // this would be cleaner if there were an owned `chunks` variant
        for chunk in values.chunks_exact_mut(n) {
          match chunk {
            [$($name),*] => out.push(($($name.take().convert()?),*),),
             _ => unreachable!(),
          }
        }

        Ok(out)
      }
    }

    #[doc(hidden)]
    impl<$($name: FromResp3<OwnedFrame>),*> FromResp3<OwnedFrame> for ($($name,)*) {
      fn is_tuple() -> bool {
        true
      }

      #[allow(non_snake_case, unused_variables)]
      fn from_frame(v: OwnedFrame) -> Result<($($name,)*), RedisProtocolError> {
        let mut values: Vec<_> = match v {
          OwnedFrame::Array { data, .. } => data,
          OwnedFrame::Push { data, .. } => data,
          OwnedFrame::Set { data, .. } => data.into_iter().collect(),
          _ => return Err(RedisProtocolError::new_parse("Could not convert to tuple."))
        };

        let mut n = 0;
        $(let $name = (); n += 1;)*
        debug_type!("FromResp3({}-tuple): {:?}", n, values);
        if values.len() != n {
          return Err(RedisProtocolError::new_parse(format!("Invalid tuple dimension. Expected {}, found {}.", n, values.len())));
        }

        values.reverse();
        Ok(($({let $name = (); values
          .pop()
          .ok_or(RedisProtocolError::new_parse("Expected value, found none."))?
          .convert()?
        },)*))
      }

      #[allow(non_snake_case, unused_variables)]
      fn from_frames(mut values: Vec<OwnedFrame>) -> Result<Vec<($($name,)*)>, RedisProtocolError> {
        let mut n = 0;
        $(let $name = (); n += 1;)*
        debug_type!("FromResp3({}-tuple): {:?}", n, values);
        if values.len() % n != 0 {
          return Err(RedisProtocolError::new_parse(format!("Invalid tuple dimension. Expected {}, found {}.", n, values.len())));
        }

        let mut out = Vec::with_capacity(values.len() / n);
        // this would be cleaner if there were an owned `chunks` variant
        for chunk in values.chunks_exact_mut(n) {
          match chunk {
            [$($name),*] => out.push(($($name.take().convert()?),*),),
             _ => unreachable!(),
          }
        }

        Ok(out)
      }
    }
    impl_from_resp3_peel!($($name,)*);
  )
}

macro_rules! impl_from_resp3_peel {
  ($name:ident, $($other:ident,)*) => (impl_from_resp3_tuple!($($other,)*);)
}

impl_from_resp3_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

// Regression tests duplicated for each frame type.
#[cfg(test)]
mod owned_tests {
  use super::*;

  #[test]
  fn should_convert_signed_numeric_types() {
    let _foo: i8 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i8 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i16 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i16 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i32 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i32 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i64 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i64 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i128 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i128 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: isize = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: isize = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: f32 = OwnedFrame::BlobString {
      data:       "123.5".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123.5);
    let _foo: f64 = OwnedFrame::BlobString {
      data:       "123.5".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123.5);
  }

  #[test]
  fn should_convert_unsigned_numeric_types() {
    let _foo: u8 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u8 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u16 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u16 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u32 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u32 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u64 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u64 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u128 = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u128 = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: usize = OwnedFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: usize = OwnedFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
  }

  #[test]
  fn should_return_not_found_with_null_number_types() {
    let result: Result<u8, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u16, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u32, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u64, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u128, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<usize, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i8, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i16, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i32, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i64, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i128, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<isize, _> = OwnedFrame::Null.convert();
    assert!(result.is_err());
  }

  #[test]
  fn should_convert_strings() {
    let _foo: String = OwnedFrame::BlobString {
      data:       "foo".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, "foo".to_owned());
  }

  #[test]
  fn should_convert_numbers_to_bools() {
    let foo: bool = OwnedFrame::Number {
      data:       0,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(!foo);
    let foo: bool = OwnedFrame::Number {
      data:       1,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(foo);
    let foo: bool = OwnedFrame::BlobString {
      data:       "0".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(!foo);
    let foo: bool = OwnedFrame::BlobString {
      data:       "1".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(foo);
  }

  #[test]
  fn should_convert_bytes() {
    let foo: Vec<u8> = OwnedFrame::BlobString {
      data:       "foo".as_bytes().to_vec(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
    let foo: Vec<u8> = OwnedFrame::BlobString {
      data:       "foo".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
    let foo: Vec<u8> = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::Number {
          data:       102,
          attributes: None,
        },
        OwnedFrame::Number {
          data:       111,
          attributes: None,
        },
        OwnedFrame::Number {
          data:       111,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
  }

  #[test]
  fn should_convert_arrays() {
    let foo: Vec<String> = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, vec!["a".to_owned(), "b".to_owned()]);
  }

  #[test]
  fn should_convert_hash_maps() {
    let foo: HashMap<String, u16> = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        OwnedFrame::Number {
          data:       1,
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
        OwnedFrame::Number {
          data:       2,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();

    let mut expected = HashMap::new();
    expected.insert("a".to_owned(), 1);
    expected.insert("b".to_owned(), 2);
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_hash_sets() {
    let foo: HashSet<String> = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();

    let mut expected = HashSet::new();
    expected.insert("a".to_owned());
    expected.insert("b".to_owned());
    assert_eq!(foo, expected);

    let foo: HashSet<String> = OwnedFrame::Set {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
      ]
      .into_iter()
      .collect(),
      attributes: None,
    }
    .convert()
    .unwrap();

    let mut expected = HashSet::new();
    expected.insert("a".to_owned());
    expected.insert("b".to_owned());
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_tuples() {
    let foo: (String, i64) = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        OwnedFrame::Number {
          data:       1,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, ("a".to_owned(), 1));
  }

  #[test]
  fn should_convert_array_tuples() {
    let foo: Vec<(String, i64)> = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        OwnedFrame::Number {
          data:       1,
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
        OwnedFrame::Number {
          data:       2,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, vec![("a".to_owned(), 1), ("b".to_owned(), 2)]);
  }

  #[test]
  fn should_handle_single_element_vector_to_scalar() {
    assert!(OwnedFrame::Array {
      data:       vec![],
      attributes: None,
    }
    .convert::<String>()
    .is_err());
    assert_eq!(
      OwnedFrame::Array {
        data:       vec![OwnedFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        }],
        attributes: None,
      }
      .convert::<String>(),
      Ok("foo".into())
    );
    assert!(OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "bar".into(),
          attributes: None,
        }
      ],
      attributes: None,
    }
    .convert::<String>()
    .is_err());

    assert_eq!(
      OwnedFrame::Array {
        data:       vec![],
        attributes: None,
      }
      .convert::<Option<String>>(),
      Ok(None)
    );
    assert_eq!(
      OwnedFrame::Array {
        data:       vec![OwnedFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        }],
        attributes: None,
      }
      .convert::<Option<String>>(),
      Ok(Some("foo".into()))
    );
    assert!(OwnedFrame::Array {
      data:       vec![
        OwnedFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        OwnedFrame::SimpleString {
          data:       "bar".into(),
          attributes: None,
        }
      ],
      attributes: None,
    }
    .convert::<Option<String>>()
    .is_err());
  }

  #[test]
  fn should_convert_null_to_empty_array() {
    assert_eq!(Vec::<String>::new(), OwnedFrame::Null.convert::<Vec<String>>().unwrap());
    assert_eq!(Vec::<u8>::new(), OwnedFrame::Null.convert::<Vec<u8>>().unwrap());
  }

  #[test]
  fn should_convert_to_fixed_arrays() {
    let foo: [i64; 2] = OwnedFrame::Array {
      data:       vec![
        OwnedFrame::Number {
          data:       1,
          attributes: None,
        },
        OwnedFrame::Number {
          data:       2,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, [1, 2]);

    assert!(OwnedFrame::Array {
      data:       vec![
        OwnedFrame::Number {
          data:       1,
          attributes: None,
        },
        OwnedFrame::Number {
          data:       2,
          attributes: None,
        }
      ],
      attributes: None,
    }
    .convert::<[i64; 3]>()
    .is_err());
    assert!(OwnedFrame::Array {
      data:       vec![],
      attributes: None,
    }
    .convert::<[i64; 3]>()
    .is_err());
  }
}

#[cfg(test)]
#[cfg(feature = "bytes")]
mod bytes_tests {
  use super::*;

  #[test]
  fn should_convert_signed_numeric_types() {
    let _foo: i8 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i8 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i16 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i16 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i32 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i32 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i64 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i64 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i128 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: i128 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: isize = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: isize = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: f32 = BytesFrame::BlobString {
      data:       "123.5".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123.5);
    let _foo: f64 = BytesFrame::BlobString {
      data:       "123.5".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123.5);
  }

  #[test]
  fn should_convert_unsigned_numeric_types() {
    let _foo: u8 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u8 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u16 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u16 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u32 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u32 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u64 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u64 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u128 = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: u128 = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: usize = BytesFrame::BlobString {
      data:       "123".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
    let _foo: usize = BytesFrame::Number {
      data:       123,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, 123);
  }

  #[test]
  fn should_return_not_found_with_null_number_types() {
    let result: Result<u8, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u16, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u32, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u64, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<u128, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<usize, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i8, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i16, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i32, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i64, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<i128, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
    let result: Result<isize, _> = BytesFrame::Null.convert();
    assert!(result.is_err());
  }

  #[test]
  fn should_convert_strings() {
    let _foo: String = BytesFrame::BlobString {
      data:       "foo".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, "foo".to_owned());
  }

  #[test]
  fn should_convert_bytes_util_str() {
    let _foo: Str = BytesFrame::BlobString {
      data:       "foo".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, Str::from("foo"));
  }

  #[test]
  fn should_convert_bytes_bytes() {
    let _foo: Bytes = BytesFrame::BlobString {
      data:       "foo".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(_foo, Bytes::from("foo"));
  }

  #[test]
  fn should_convert_numbers_to_bools() {
    let foo: bool = BytesFrame::Number {
      data:       0,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(!foo);
    let foo: bool = BytesFrame::Number {
      data:       1,
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(foo);
    let foo: bool = BytesFrame::BlobString {
      data:       "0".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(!foo);
    let foo: bool = BytesFrame::BlobString {
      data:       "1".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert!(foo);
  }

  #[test]
  fn should_convert_bytes() {
    let foo: Vec<u8> = BytesFrame::BlobString {
      data:       "foo".as_bytes().to_vec().into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
    let foo: Vec<u8> = BytesFrame::BlobString {
      data:       "foo".into(),
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
    let foo: Vec<u8> = BytesFrame::Array {
      data:       vec![
        BytesFrame::Number {
          data:       102,
          attributes: None,
        },
        BytesFrame::Number {
          data:       111,
          attributes: None,
        },
        BytesFrame::Number {
          data:       111,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, "foo".as_bytes().to_vec());
  }

  #[test]
  fn should_convert_arrays() {
    let foo: Vec<String> = BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, vec!["a".to_owned(), "b".to_owned()]);
  }

  #[test]
  fn should_convert_hash_maps() {
    let foo: HashMap<String, u16> = BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        BytesFrame::Number {
          data:       1,
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
        BytesFrame::Number {
          data:       2,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();

    let mut expected = HashMap::new();
    expected.insert("a".to_owned(), 1);
    expected.insert("b".to_owned(), 2);
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_hash_sets() {
    let foo: HashSet<String> = BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();

    let mut expected = HashSet::new();
    expected.insert("a".to_owned());
    expected.insert("b".to_owned());
    assert_eq!(foo, expected);

    let foo: HashSet<String> = BytesFrame::Set {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
      ]
      .into_iter()
      .collect(),
      attributes: None,
    }
    .convert()
    .unwrap();

    let mut expected = HashSet::new();
    expected.insert("a".to_owned());
    expected.insert("b".to_owned());
    assert_eq!(foo, expected);
  }

  #[test]
  fn should_convert_tuples() {
    let foo: (String, i64) = BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        BytesFrame::Number {
          data:       1,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, ("a".to_owned(), 1));
  }

  #[test]
  fn should_convert_array_tuples() {
    let foo: Vec<(String, i64)> = BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "a".into(),
          attributes: None,
        },
        BytesFrame::Number {
          data:       1,
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "b".into(),
          attributes: None,
        },
        BytesFrame::Number {
          data:       2,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, vec![("a".to_owned(), 1), ("b".to_owned(), 2)]);
  }

  #[test]
  fn should_handle_single_element_vector_to_scalar() {
    assert!(BytesFrame::Array {
      data:       vec![],
      attributes: None,
    }
    .convert::<String>()
    .is_err());
    assert_eq!(
      BytesFrame::Array {
        data:       vec![BytesFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        }],
        attributes: None,
      }
      .convert::<String>(),
      Ok("foo".into())
    );
    assert!(BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "bar".into(),
          attributes: None,
        }
      ],
      attributes: None,
    }
    .convert::<String>()
    .is_err());

    assert_eq!(
      BytesFrame::Array {
        data:       vec![],
        attributes: None,
      }
      .convert::<Option<String>>(),
      Ok(None)
    );
    assert_eq!(
      BytesFrame::Array {
        data:       vec![BytesFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        }],
        attributes: None,
      }
      .convert::<Option<String>>(),
      Ok(Some("foo".into()))
    );
    assert!(BytesFrame::Array {
      data:       vec![
        BytesFrame::SimpleString {
          data:       "foo".into(),
          attributes: None,
        },
        BytesFrame::SimpleString {
          data:       "bar".into(),
          attributes: None,
        }
      ],
      attributes: None,
    }
    .convert::<Option<String>>()
    .is_err());
  }

  #[test]
  fn should_convert_null_to_empty_array() {
    assert_eq!(Vec::<String>::new(), BytesFrame::Null.convert::<Vec<String>>().unwrap());
    assert_eq!(Vec::<u8>::new(), BytesFrame::Null.convert::<Vec<u8>>().unwrap());
  }

  #[test]
  fn should_convert_to_fixed_arrays() {
    let foo: [i64; 2] = BytesFrame::Array {
      data:       vec![
        BytesFrame::Number {
          data:       1,
          attributes: None,
        },
        BytesFrame::Number {
          data:       2,
          attributes: None,
        },
      ],
      attributes: None,
    }
    .convert()
    .unwrap();
    assert_eq!(foo, [1, 2]);

    assert!(BytesFrame::Array {
      data:       vec![
        BytesFrame::Number {
          data:       1,
          attributes: None,
        },
        BytesFrame::Number {
          data:       2,
          attributes: None,
        }
      ],
      attributes: None,
    }
    .convert::<[i64; 3]>()
    .is_err());
    assert!(BytesFrame::Array {
      data:       vec![],
      attributes: None,
    }
    .convert::<[i64; 3]>()
    .is_err());
  }
}
