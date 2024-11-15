// Adapted from https://github.com/lifthrasiir/rust-strconv/blob/master/src/int2dec/strategy/bcd_earlyexit.rs,
// but modified to return the amount of padding that should be removed.

use core::{
  marker::Copy,
  ops::{Div, Rem},
};

pub const NDIGITS64: usize = 20; // 1844 6744 0737 0955 1615
pub type Digit = u8;
pub type Digits64 = [Digit; NDIGITS64];

pub static TENS: &[u8] = b"00000000001111111111222222222233333333334444444444\
                           55555555556666666666777777777788888888889999999999";
pub static ONES: &[u8] = b"01234567890123456789012345678901234567890123456789\
                           01234567890123456789012345678901234567890123456789";

#[inline]
fn div_rem<T: Copy + Div<T, Output = T> + Rem<T, Output = T>>(x: T, y: T) -> (T, T) {
  (x / y, x % y)
}

#[inline]
fn padding(buf: &[u8], shiftr: usize) -> usize {
  if buf[0] != b'0' {
    return shiftr;
  }
  if buf[1] != b'0' {
    return shiftr + 1;
  }
  if buf[2] != b'0' {
    return shiftr + 2;
  }
  if buf[3] != b'0' {
    return shiftr + 3;
  }
  shiftr + 4
}

macro_rules! tens {
  ($i:expr) => {
    TENS[$i as usize]
  };
}
macro_rules! ones {
  ($i:expr) => {
    ONES[$i as usize]
  };
}

// http://homepage.cs.uiowa.edu/~jones/bcd/decimal.html#sixtyfour
#[inline]
pub fn u64_to_digits(n: u64) -> (Digits64, usize) {
  let mut buf: Digits64 = [b'0'; NDIGITS64];

  let n0 = (n & 0xffff) as u32;
  let n1 = ((n >> 16) & 0xffff) as u32;
  let n2 = ((n >> 32) & 0xffff) as u32;
  let n3 = ((n >> 48) & 0xffff) as u32;

  macro_rules! quad {
    ($d:expr, $i:expr) => {{
      let (qq, rr) = div_rem($d, 100);
      buf[$i] = tens!(qq);
      buf[$i + 1] = ones!(qq);
      buf[$i + 2] = tens!(rr);
      buf[$i + 3] = ones!(rr);
    }};
  }

  if n == 0 {
    return (buf, NDIGITS64 - 1);
  }

  let mut shiftr = NDIGITS64 - 4 - 1;
  let (c0, d0) = div_rem(656 * n3 + 7296 * n2 + 5536 * n1 + n0, 10000);
  quad!(d0, 16);
  if n <= 9999 {
    return (buf, padding(&buf[shiftr ..], shiftr));
  }

  shiftr -= 4;
  let (c1, d1) = div_rem(c0 + 7671 * n3 + 9496 * n2 + 6 * n1, 10000);
  quad!(d1, 12);
  if n <= 9999_9999 {
    return (buf, padding(&buf[shiftr ..], shiftr));
  }

  shiftr -= 4;
  let (c2, d2) = div_rem(c1 + 4749 * n3 + 42 * n2, 10000);
  quad!(d2, 8);
  if n <= 9999_9999_9999 {
    return (buf, padding(&buf[shiftr ..], shiftr));
  }

  shiftr -= 4;
  let (d4, d3) = div_rem(c2 + 281 * n3, 10000);
  quad!(d3, 4);
  if n <= 9999_9999_9999_9999 {
    return (buf, padding(&buf[shiftr ..], shiftr));
  }

  quad!(d4, 0);
  (buf, padding(&buf[0 ..], 0))
}

#[inline]
pub fn i64_to_digits(n: i64) -> (Digits64, usize) {
  // padding will always be >= 1 in this context
  let (mut buf, padding) = u64_to_digits(n.unsigned_abs());

  if n.is_negative() {
    buf[padding - 1] = b'-';
    (buf, padding - 1)
  } else {
    (buf, padding)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn test_data(s: &str) -> [u8; NDIGITS64] {
    let mut buf = [b'0'; NDIGITS64];
    for (i, c) in s.as_bytes().iter().rev().enumerate() {
      buf[NDIGITS64 - i - 1] = *c;
    }
    buf
  }

  #[test]
  fn should_convert_zero() {
    let (input, (expected, expected_padding)) = (0, ([b'0'; NDIGITS64], 19));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_max() {
    let (input, (expected, expected_padding)) = (u64::MAX, (test_data(u64::MAX.to_string().as_str()), 0));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_u64_1000() {
    let (input, (expected, expected_padding)) = (1000, (test_data("1000"), NDIGITS64 - 4));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_lt_4_dec() {
    let (input, (expected, expected_padding)) = (1, (test_data("1"), 19));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (12, (test_data("12"), 18));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (123, (test_data("123"), 17));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (1234, (test_data("1234"), 16));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_lt_8_dec() {
    let (input, (expected, expected_padding)) = (12345, (test_data("12345"), 15));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (123456, (test_data("123456"), 14));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (1234567, (test_data("1234567"), 13));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (12345678, (test_data("12345678"), 12));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_lt_12_dec() {
    let (input, (expected, expected_padding)) = (123456789, (test_data("123456789"), 11));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (1234567899, (test_data("1234567899"), 10));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (12345678999, (test_data("12345678999"), 9));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (123456789999, (test_data("123456789999"), 8));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_lt_16_dec() {
    let (input, (expected, expected_padding)) = (1234567899999, (test_data("1234567899999"), 7));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (12345678999999, (test_data("12345678999999"), 6));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (123456789999999, (test_data("123456789999999"), 5));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (1234567899999999, (test_data("1234567899999999"), 4));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_lt_20_dec() {
    let (input, (expected, expected_padding)) = (12345678999999999, (test_data("12345678999999999"), 3));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (123456789999999999, (test_data("123456789999999999"), 2));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (1234567899999999999, (test_data("1234567899999999999"), 1));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);

    let (input, (expected, expected_padding)) = (12345678999999999999, (test_data("12345678999999999999"), 0));
    let (actual, padding) = u64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_i64_zero() {
    let (input, (expected, expected_padding)) = (0, ([b'0'; NDIGITS64], 19));
    let (actual, padding) = i64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_i64_max() {
    let (input, (expected, expected_padding)) = (i64::MAX, (test_data(i64::MAX.to_string().as_str()), 1));
    let (actual, padding) = i64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_i64_min() {
    let (input, (expected, expected_padding)) = (i64::MIN, (test_data(i64::MIN.to_string().as_str()), 0));
    let (actual, padding) = i64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_i64_42() {
    let (input, (expected, expected_padding)) = (42, (test_data("42"), NDIGITS64 - 2));
    let (actual, padding) = i64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }

  #[test]
  fn should_convert_i64_neg_42() {
    let (input, (expected, expected_padding)) = (-42, (test_data("-42"), NDIGITS64 - 3));
    let (actual, padding) = i64_to_digits(input);
    assert_eq!(actual, expected);
    assert_eq!(padding, expected_padding);
  }
}
