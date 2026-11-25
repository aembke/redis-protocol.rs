#![allow(unused_imports)]

#[cfg(feature = "resp2")]
mod resp2;
#[cfg(feature = "resp3")]
mod resp3;

#[cfg(feature = "resp2")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp2")))]
pub use resp2::*;
#[cfg(feature = "resp3")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp3")))]
pub use resp3::*;

#[cfg(any(feature = "resp2", feature = "resp3"))]
use crate::error::RedisProtocolError;
#[cfg(any(feature = "resp2", feature = "resp3"))]
use alloc::format;
#[cfg(any(feature = "resp2", feature = "resp3"))]
use core::{any::type_name, fmt::Display};

/// Map a UTF-8 error encountered while converting a frame to a `Parse` error.
///
/// The global `From` impls for UTF-8 errors are used by the decoders too, so conversion paths map these explicitly.
#[cfg(any(feature = "resp2", feature = "resp3"))]
pub(crate) fn utf8_error<E: Display>(e: E) -> RedisProtocolError {
  RedisProtocolError::new_parse(format!("Invalid UTF-8: {e}"))
}

/// Convert an integer to another integer type, returning an error instead of truncating.
#[cfg(any(feature = "resp2", feature = "resp3"))]
pub(crate) fn i64_to_int<T: TryFrom<i64>>(data: i64) -> Result<T, RedisProtocolError> {
  T::try_from(data).map_err(|_| {
    if data < 0 {
      RedisProtocolError::new_parse(format!(
        "Cannot convert from negative number {data} to {}.",
        type_name::<T>()
      ))
    } else {
      RedisProtocolError::new_parse(format!("Number {data} out of range for {}.", type_name::<T>()))
    }
  })
}

/// Convert a double to an integer type, returning an error unless the value is finite, has no fractional part, and
/// fits in the destination type. Negative zero is treated as zero.
#[cfg(feature = "resp3")]
pub(crate) fn f64_to_int<T: TryFrom<i128> + TryFrom<u128>>(data: f64) -> Result<T, RedisProtocolError> {
  // 2^127 and 2^128 are exactly representable as doubles
  const I128_BOUND: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;
  const U128_BOUND: f64 = 2.0 * I128_BOUND;

  let out_of_range =
    || RedisProtocolError::new_parse(format!("Double {data} out of range for {}.", type_name::<T>()));
  if !data.is_finite() {
    return Err(RedisProtocolError::new_parse(format!(
      "Cannot convert non-finite double {data} to {}.",
      type_name::<T>()
    )));
  }

  // `-0.0 >= 0.0`, so negative zero takes this branch
  let integral = if data >= 0.0 {
    if data >= U128_BOUND {
      return Err(out_of_range());
    }
    // truncation is exact for values in range, so this only differs from `data` if there is a fractional part
    let i = data as u128;
    (i as f64 == data).then(|| T::try_from(i).map_err(|_| out_of_range()))
  } else {
    if data < -I128_BOUND {
      return Err(out_of_range());
    }
    let i = data as i128;
    (i as f64 == data).then(|| T::try_from(i).map_err(|_| out_of_range()))
  };

  integral.unwrap_or_else(|| {
    Err(RedisProtocolError::new_parse(format!(
      "Cannot convert double {data} with a fractional part to {}.",
      type_name::<T>()
    )))
  })
}

#[cfg(test)]
#[cfg(any(feature = "resp2", feature = "resp3"))]
mod tests {
  use super::*;
  use crate::error::RedisProtocolErrorKind;

  #[test]
  fn should_check_integer_conversions() {
    assert_eq!(i64_to_int::<u8>(255), Ok(255));
    assert_eq!(
      i64_to_int::<u8>(256).unwrap_err().kind(),
      &RedisProtocolErrorKind::Parse
    );
    assert_eq!(i64_to_int::<u8>(-1).unwrap_err().kind(), &RedisProtocolErrorKind::Parse);
    assert_eq!(i64_to_int::<i128>(i64::MIN), Ok(i64::MIN as i128));
  }

  #[cfg(feature = "resp3")]
  #[test]
  fn should_check_double_conversions() {
    assert_eq!(f64_to_int::<u8>(-0.0), Ok(0));
    assert_eq!(f64_to_int::<i64>(i64::MIN as f64), Ok(i64::MIN));
    assert_eq!(f64_to_int::<i128>(i128::MIN as f64), Ok(i128::MIN));
    assert_eq!(f64_to_int::<u128>((1_u128 << 127) as f64), Ok(1 << 127));
    assert!(f64_to_int::<u128>(2.0 * (1_u128 << 127) as f64).is_err());
    assert!(f64_to_int::<i128>((1_u128 << 127) as f64).is_err());
    assert!(f64_to_int::<i64>(-(i64::MIN as f64)).is_err());
    assert!(f64_to_int::<i64>(0.5).is_err());
    assert!(f64_to_int::<i64>(f64::NAN).is_err());
    assert!(f64_to_int::<i64>(f64::INFINITY).is_err());
    assert_eq!(
      f64_to_int::<i64>(1.9).unwrap_err().kind(),
      &RedisProtocolErrorKind::Parse
    );
  }

  #[test]
  fn should_return_parse_error_from_str_to_f64() {
    assert_eq!(
      crate::str_to_f64("foo").unwrap_err().kind(),
      &RedisProtocolErrorKind::Parse
    );
  }
}
