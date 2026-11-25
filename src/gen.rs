//! Minimal serialization helpers used by the encoders.
//!
//! These replace the subset of the legacy `cookie-factory` macro interface previously used by this crate.

#[cfg(feature = "bytes")]
use bytes::BytesMut;

/// Errors returned by the internal serialization functions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GenError {
  /// The output buffer is too small. The argument is the number of additional bytes required.
  BufferTooSmall(usize),
  /// Allocated for custom errors.
  #[cfg_attr(not(feature = "resp3"), allow(dead_code))]
  CustomError(u32),
}

/// A destination for encoded bytes.
pub(crate) trait GenOut: Sized {
  /// Append `data`, returning the destination for the next write.
  fn gen_slice(self, data: &[u8]) -> Result<Self, GenError>;

  /// Make room for at least `amt` more bytes, if the destination can grow.
  #[inline]
  fn reserve(self, _amt: usize) -> Self {
    self
  }
}

/// A slice and the offset of the next write, which fails if the slice is too small.
impl GenOut for (&mut [u8], usize) {
  #[inline]
  fn gen_slice(self, data: &[u8]) -> Result<Self, GenError> {
    let (buf, offset) = self;
    let end = offset + data.len();
    if end > buf.len() {
      return Err(GenError::BufferTooSmall(end - buf.len()));
    }

    buf[offset .. end].copy_from_slice(data);
    Ok((buf, end))
  }
}

/// A growable buffer. Bytes are appended directly, so the buffer doesn't need to be zero-filled before encoding.
#[cfg(feature = "bytes")]
impl GenOut for &mut BytesMut {
  #[inline]
  fn gen_slice(self, data: &[u8]) -> Result<Self, GenError> {
    self.extend_from_slice(data);
    Ok(self)
  }

  #[inline]
  fn reserve(self, amt: usize) -> Self {
    BytesMut::reserve(self, amt);
    self
  }
}

/// Write `data` to `x`, returning the destination for the next write.
#[inline]
pub(crate) fn gen_slice<W: GenOut>(x: W, data: &[u8]) -> Result<W, GenError> {
  x.gen_slice(data)
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
