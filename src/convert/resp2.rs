use crate::{
  error::RedisProtocolError,
  resp2::types::{OwnedFrame, Resp2Frame},
};
use alloc::{string::String, vec::Vec};
use core::{
  hash::{BuildHasher, Hash},
  str,
};

#[cfg(feature = "bytes")]
use crate::resp2::types::BytesFrame;
#[cfg(feature = "bytes")]
use bytes::Bytes;
#[cfg(feature = "bytes")]
use bytes_utils::Str;

#[cfg(feature = "hashbrown")]
use hashbrown::{HashMap, HashSet};
#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};
#[cfg(feature = "std")]
use std::collections::{HashMap, HashSet};

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

impl FromResp2<OwnedFrame> for f64 {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    frame
      .as_f64()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for f64 {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    frame
      .as_f64()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}

impl FromResp2<OwnedFrame> for f32 {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    frame
      .as_f64()
      .map(|f| f as f32)
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for f32 {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    frame
      .as_f64()
      .map(|f| f as f32)
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to f64"))
  }
}

impl FromResp2<OwnedFrame> for bool {
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    frame
      .as_bool()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to bool"))
  }
}
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for bool {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    frame
      .as_bool()
      .ok_or_else(|| RedisProtocolError::new_parse("Cannot convert to bool"))
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
    debug_type!("FromResp2(String): {:?}", frame);

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
    debug_type!("FromResp2(String): {:?}", frame);

    Ok(match frame {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => String::from_utf8(b.to_vec())?,
      BytesFrame::Error(s) => s.to_string(),
      BytesFrame::Integer(i) => i.to_string(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for Str {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp2(Str): {:?}", frame);

    Ok(match frame {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => Str::from_inner(b)?,
      BytesFrame::Error(s) => s,
      BytesFrame::Integer(i) => i.to_string().into(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to string.")),
    })
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl FromResp2<BytesFrame> for Bytes {
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp2(Bytes): {:?}", frame);

    Ok(match frame {
      BytesFrame::BulkString(b) | BytesFrame::SimpleString(b) => b,
      BytesFrame::Error(s) => s.into_inner(),
      BytesFrame::Integer(i) => i.to_string().into(),
      _ => return Err(RedisProtocolError::new_parse("Cannot convert to bytes.")),
    })
  }
}

impl<T> FromResp2<OwnedFrame> for Option<T>
where
  T: FromResp2<OwnedFrame>,
{
  fn from_frame(frame: OwnedFrame) -> Result<Option<T>, RedisProtocolError> {
    debug_type!("FromResp2(Option<{}>): {:?}", std::any::type_name::<T>(), frame);

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
    debug_type!("FromResp2(Option<{}>): {:?}", std::any::type_name::<T>(), frame);

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

impl<T> FromResp2<OwnedFrame> for Vec<T>
where
  T: FromResp2<OwnedFrame>,
{
  fn from_frame(frame: OwnedFrame) -> Result<Vec<T>, RedisProtocolError> {
    debug_type!("FromResp2(Vec<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      OwnedFrame::BulkString(buf) => {
        // hacky way to check if T is bytes without consuming `string`
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(buf).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::BulkString(buf))?])
        }
      },
      OwnedFrame::SimpleString(buf) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(buf).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::SimpleString(buf))?])
        }
      },
      OwnedFrame::Error(buf) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(buf.into_bytes()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(OwnedFrame::Error(buf))?])
        }
      },
      OwnedFrame::Array(values) => {
        if !values.is_empty() {
          if let OwnedFrame::Array(_) = &values[0] {
            values.into_iter().map(|x| T::from_frame(x)).collect()
          } else {
            T::from_frames(values)
          }
        } else {
          Ok(Vec::new())
        }
      },
      OwnedFrame::Integer(i) => Ok(vec![T::from_frame(OwnedFrame::Integer(i))?]),
      OwnedFrame::Null => Ok(Vec::new()),
    }
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<T> FromResp2<BytesFrame> for Vec<T>
where
  T: FromResp2<BytesFrame>,
{
  fn from_frame(frame: BytesFrame) -> Result<Vec<T>, RedisProtocolError> {
    debug_type!("FromResp2(Vec<{}>): {:?}", std::any::type_name::<T>(), frame);

    match frame {
      BytesFrame::BulkString(buf) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(buf.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::BulkString(buf))?])
        }
      },
      BytesFrame::SimpleString(buf) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(buf.to_vec()).ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::SimpleString(buf))?])
        }
      },
      BytesFrame::Error(buf) => {
        if T::from_owned_bytes(Vec::new()).is_some() {
          T::from_owned_bytes(buf.into_inner().to_vec())
            .ok_or(RedisProtocolError::new_parse("Could not convert to bytes."))
        } else {
          Ok(vec![T::from_frame(BytesFrame::Error(buf))?])
        }
      },
      BytesFrame::Array(values) => {
        if !values.is_empty() {
          if let BytesFrame::Array(_) = &values[0] {
            values.into_iter().map(|x| T::from_frame(x)).collect()
          } else {
            T::from_frames(values)
          }
        } else {
          Ok(Vec::new())
        }
      },
      BytesFrame::Integer(i) => Ok(vec![T::from_frame(BytesFrame::Integer(i))?]),
      BytesFrame::Null => Ok(Vec::new()),
    }
  }
}

impl<T, const N: usize> FromResp2<OwnedFrame> for [T; N]
where
  T: FromResp2<OwnedFrame>,
{
  fn from_frame(value: OwnedFrame) -> Result<[T; N], RedisProtocolError> {
    debug_type!("FromResp2([{}; {}]): {:?}", std::any::type_name::<T>(), N, value);
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
impl<T, const N: usize> FromResp2<BytesFrame> for [T; N]
where
  T: FromResp2<BytesFrame>,
{
  fn from_frame(value: BytesFrame) -> Result<[T; N], RedisProtocolError> {
    debug_type!("FromResp2([{}; {}]): {:?}", std::any::type_name::<T>(), N, value);
    // use the `from_value` impl for Vec<T>
    let value: Vec<T> = value.convert()?;
    let len = value.len();

    value.try_into().map_err(|_| {
      RedisProtocolError::new_parse(format!("Failed to convert to array. Expected {}, found {}.", N, len))
    })
  }
}

impl<K, V, S> FromResp2<OwnedFrame> for HashMap<K, V, S>
where
  K: FromResp2<OwnedFrame> + Eq + Hash,
  V: FromResp2<OwnedFrame>,
  S: BuildHasher + Default,
{
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    debug_type!(
      "FromResp2(HashMap<{}, {}>): {:?}",
      std::any::type_name::<K>(),
      std::any::type_name::<V>(),
      frame
    );

    if let OwnedFrame::Array(mut values) = frame {
      if values.is_empty() {
        return Ok::<HashMap<K, V, S>, _>(HashMap::default());
      }

      if values.len() % 2 == 0 {
        let mut out = HashMap::default();
        out.reserve(values.len() / 2);

        #[allow(clippy::manual_while_let_some)]
        while !values.is_empty() {
          let value = values.pop().unwrap();
          let key = values.pop().unwrap();

          out.insert(K::from_frame(key)?, V::from_frame(value)?);
        }
        Ok(out)
      } else {
        Err(RedisProtocolError::new_parse("Expected even number of elements"))
      }
    } else {
      Err(RedisProtocolError::new_parse("Cannot convert to map"))
    }
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<K, V, S> FromResp2<BytesFrame> for HashMap<K, V, S>
where
  K: FromResp2<BytesFrame> + Eq + Hash,
  V: FromResp2<BytesFrame>,
  S: BuildHasher + Default,
{
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!(
      "FromResp2(HashMap<{}, {}>): {:?}",
      std::any::type_name::<K>(),
      std::any::type_name::<V>(),
      frame
    );

    if let BytesFrame::Array(mut values) = frame {
      if values.is_empty() {
        return Ok::<HashMap<K, V, S>, _>(HashMap::default());
      }

      if values.len() % 2 == 0 {
        let mut out = HashMap::default();
        out.reserve(values.len() / 2);

        #[allow(clippy::manual_while_let_some)]
        while !values.is_empty() {
          let value = values.pop().unwrap();
          let key = values.pop().unwrap();

          out.insert(K::from_frame(key)?, V::from_frame(value)?);
        }
        Ok(out)
      } else {
        Err(RedisProtocolError::new_parse("Expected even number of elements"))
      }
    } else {
      Err(RedisProtocolError::new_parse("Cannot convert to map"))
    }
  }
}

// TODO indexmap

impl<V, S> FromResp2<OwnedFrame> for HashSet<V, S>
where
  V: FromResp2<OwnedFrame> + Hash + Eq,
  S: BuildHasher + Default,
{
  fn from_frame(frame: OwnedFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp2(HashSet<{}>): {:?}", std::any::type_name::<V>(), frame);

    if let OwnedFrame::Array(values) = frame {
      values.into_iter().map(V::from_frame).collect()
    } else {
      Err(RedisProtocolError::new_parse("Cannot convert to set"))
    }
  }
}

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
impl<V, S> FromResp2<BytesFrame> for HashSet<V, S>
where
  V: FromResp2<BytesFrame> + Hash + Eq,
  S: BuildHasher + Default,
{
  fn from_frame(frame: BytesFrame) -> Result<Self, RedisProtocolError> {
    debug_type!("FromResp2(HashSet<{}>): {:?}", std::any::type_name::<V>(), frame);

    if let BytesFrame::Array(values) = frame {
      values.into_iter().map(V::from_frame).collect()
    } else {
      Err(RedisProtocolError::new_parse("Cannot convert to set"))
    }
  }
}
