//! Minimal serialization helpers used by the encoders.
//!
//! These replace the subset of the legacy `cookie-factory` macro interface previously used by this crate.

/// Errors returned by the internal serialization functions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GenError {
  /// The output buffer is too small. The argument is the number of additional bytes required.
  BufferTooSmall(usize),
  /// Allocated for custom errors.
  #[cfg_attr(not(feature = "resp3"), allow(dead_code))]
  CustomError(u32),
}

/// Copy `data` into `buf` at `offset`, returning the buffer and the next offset.
#[inline]
pub(crate) fn gen_slice<'a>(
  (buf, offset): (&'a mut [u8], usize),
  data: &[u8],
) -> Result<(&'a mut [u8], usize), GenError> {
  let end = offset + data.len();
  if end > buf.len() {
    return Err(GenError::BufferTooSmall(end - buf.len()));
  }

  buf[offset .. end].copy_from_slice(data);
  Ok((buf, end))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn should_write_slices_in_sequence() {
    let mut buf = [0u8; 6];
    let x = (&mut buf[..], 0);
    let res: Result<_, GenError> = do_gen!(x, gen_be_u8!(b'+') >> gen_slice!(b"OK") >> gen_slice!(b"\r\n"));
    let (_, offset) = res.unwrap();

    assert_eq!(offset, 5);
    assert_eq!(&buf, b"+OK\r\n\0");
  }

  #[test]
  fn should_error_when_buffer_too_small() {
    let mut buf = [0u8; 3];
    let x = (&mut buf[..], 0);
    let res: Result<_, GenError> = do_gen!(x, gen_be_u8!(b'+') >> gen_slice!(b"OK") >> gen_slice!(b"\r\n"));

    assert_eq!(res.unwrap_err(), GenError::BufferTooSmall(2));
  }
}
