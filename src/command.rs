//! Encode commands directly from their arguments.
//!
//! A command is an array of blob strings in both RESP2 and RESP3, so these are re-exported from both encoding
//! modules.

use crate::{
  error::RedisProtocolError,
  gen::{GenError, GenOut, COMMAND_LENGTH},
  int2dec,
  types::CRLF,
  utils::digits_in_usize,
};

#[cfg(feature = "bytes")]
use crate::utils;
#[cfg(feature = "bytes")]
use bytes::BytesMut;

const ARRAY_BYTE: u8 = b'*';
const BLOB_STRING_BYTE: u8 = b'$';

/// Write one command argument as a blob string.
pub(crate) fn gen_command_arg<W: GenOut>(x: W, data: &[u8]) -> Result<W, GenError> {
  let (len, padding) = int2dec::u64_to_digits(data.len() as u64);
  // reserve the whole blob, so a large payload isn't copied twice. see benches/reserve.rs.
  let x = x.reserve(1 + len.len() - padding + 2 + data.len() + 2);
  do_gen!(
    x,
    gen_be_u8!(BLOB_STRING_BYTE)
      >> gen_slice!(&len[padding ..])
      >> gen_slice!(CRLF.as_bytes())
      >> gen_slice!(data)
      >> gen_slice!(CRLF.as_bytes())
  )
}

/// Write the array header for a command with `len` arguments.
pub(crate) fn gen_command_header<W: GenOut>(x: W, len: usize) -> Result<W, GenError> {
  let (digits, padding) = int2dec::u64_to_digits(len as u64);
  do_gen!(
    x,
    gen_be_u8!(ARRAY_BYTE) >> gen_slice!(&digits[padding ..]) >> gen_slice!(CRLF.as_bytes())
  )
}

fn gen_command<W, I>(x: W, args: I) -> Result<W, GenError>
where
  W: GenOut,
  I: Iterator + ExactSizeIterator,
  I::Item: AsRef<[u8]>,
{
  let len = args.len();
  let mut x = gen_command_header(x, len)?;
  let mut count = 0;
  for arg in args {
    if count == len {
      // the iterator reported the wrong length, so the array header is wrong
      return Err(GenError::CustomError(COMMAND_LENGTH));
    }
    x = gen_command_arg(x, arg.as_ref())?;
    count += 1;
  }

  if count == len {
    Ok(x)
  } else {
    Err(GenError::CustomError(COMMAND_LENGTH))
  }
}

/// Returns the number of bytes needed to encode a command with the provided arguments.
pub fn command_encode_len<I>(args: I) -> usize
where
  I: IntoIterator,
  I::Item: AsRef<[u8]>,
{
  let (count, args_len) = args.into_iter().fold((0, 0), |(count, total), arg| {
    let len = arg.as_ref().len();
    (count + 1, total + 1 + digits_in_usize(len) + 2 + len + 2)
  });
  1 + digits_in_usize(count) + 2 + args_len
}

/// Encode a command, such as `SET foo bar`, from its arguments into `buf`.
///
/// This writes the same bytes as encoding an array of blob strings, without building the frames first. Integer and
/// other arguments should be formatted as strings, since Redis only accepts blob strings in a command.
///
/// The caller is responsible for extending `buf` if a `BufferTooSmall` error is returned. See
/// [command_encode_len].
///
/// Returns the number of bytes encoded.
///
/// ```rust
/// # #[cfg(feature = "resp2")] {
/// # use redis_protocol::resp2::encode::{command_encode_len, encode_command};
/// let args = ["SET", "foo", "bar"];
/// let mut buf = vec![0; command_encode_len(&args)];
/// let amt = encode_command(&mut buf, &args).unwrap();
/// assert_eq!(
///   &buf[.. amt],
///   b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
/// );
///
/// // arguments with different types can be passed as byte slices
/// let value = 42.to_string();
/// let args: [&[u8]; 3] = [b"SET", &[0, 1, 2], value.as_bytes()];
/// let mut buf = vec![0; command_encode_len(&args)];
/// encode_command(&mut buf, &args).unwrap();
/// # }
/// ```
pub fn encode_command<I>(buf: &mut [u8], args: I) -> Result<usize, RedisProtocolError>
where
  I: IntoIterator,
  I::IntoIter: ExactSizeIterator + Clone,
  I::Item: AsRef<[u8]>,
{
  let args = args.into_iter();
  encode_checks!(buf, 0, command_encode_len(args.clone()));
  gen_command((buf, 0), args).map(|(_, l)| l).map_err(|e| e.into())
}

/// Encode a command, such as `SET foo bar`, from its arguments at the end of `buf`, which grows as needed.
///
/// This writes the same bytes as encoding an array of blob strings, without building the frames first. Integer and
/// other arguments should be formatted as strings, since Redis only accepts blob strings in a command.
///
/// Returns the number of bytes encoded.
///
/// ```rust
/// # #[cfg(feature = "resp2")] {
/// # use redis_protocol::{bytes::BytesMut, resp2::encode::extend_encode_command};
/// let mut buf = BytesMut::new();
/// extend_encode_command(&mut buf, ["GET", "foo"]).unwrap();
/// assert_eq!(&buf[..], b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
/// # }
/// ```
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub fn extend_encode_command<I>(buf: &mut BytesMut, args: I) -> Result<usize, RedisProtocolError>
where
  I: IntoIterator,
  I::IntoIter: ExactSizeIterator,
  I::Item: AsRef<[u8]>,
{
  utils::extend_with(buf, 0, |buf| gen_command(buf, args.into_iter()).map(|_| ()))
}

#[cfg(test)]
mod tests {
  use super::*;
  use alloc::{vec, vec::Vec};

  /// An iterator that reports the wrong length.
  #[derive(Clone)]
  struct WrongLen {
    items:    Vec<&'static str>,
    reported: usize,
  }

  impl Iterator for WrongLen {
    type Item = &'static str;

    fn next(&mut self) -> Option<Self::Item> {
      if self.items.is_empty() {
        None
      } else {
        Some(self.items.remove(0))
      }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
      (self.reported, Some(self.reported))
    }
  }

  impl ExactSizeIterator for WrongLen {}

  #[test]
  fn should_encode_commands() {
    let args = ["SET", "foo", "", "0123456789"];
    let expected = b"*4\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n$10\r\n0123456789\r\n";
    assert_eq!(command_encode_len(args), expected.len());

    let mut buf = vec![0; expected.len()];
    assert_eq!(encode_command(&mut buf, args).unwrap(), expected.len());
    assert_eq!(&buf[..], expected);

    let mut buf = vec![0; expected.len() - 1];
    assert_eq!(
      encode_command(&mut buf, args).unwrap_err(),
      RedisProtocolError::buffer_too_small(1)
    );

    let empty: [&[u8]; 0] = [];
    assert_eq!(command_encode_len(empty), 4);
    let mut buf = vec![0; 4];
    assert_eq!(encode_command(&mut buf, empty).unwrap(), 4);
    assert_eq!(&buf[..], b"*0\r\n");
  }

  #[test]
  #[cfg(feature = "bytes")]
  fn should_extend_encode_commands() {
    let mut buf = BytesMut::from(&b"foo"[..]);
    let args = vec![b"GET".to_vec(), b"bar\r\nbaz".to_vec()];
    let amt = extend_encode_command(&mut buf, &args).unwrap();
    assert_eq!(amt, command_encode_len(&args));
    assert_eq!(&buf[..], b"foo*2\r\n$3\r\nGET\r\n$8\r\nbar\r\nbaz\r\n");

    // the output matches an encoded array of blob strings
    #[cfg(feature = "resp2")]
    {
      use crate::resp2::{encode::extend_encode, types::BytesFrame};
      let frame = BytesFrame::Array(args.iter().map(|a| BytesFrame::BulkString(a.clone().into())).collect());
      let mut expected = BytesMut::new();
      extend_encode(&mut expected, &frame, false).unwrap();
      assert_eq!(&buf[3 ..], &expected[..]);
    }
  }

  #[test]
  fn should_reject_iterators_with_the_wrong_length() {
    for reported in [1, 3] {
      let args = WrongLen {
        items: vec!["GET", "foo"],
        reported,
      };
      let mut buf = vec![0; 64];
      assert!(encode_command(&mut buf, args.clone()).is_err());

      #[cfg(feature = "bytes")]
      {
        let mut buf = BytesMut::from(&b"foo"[..]);
        assert!(extend_encode_command(&mut buf, args).is_err());
        // the buffer is left unchanged
        assert_eq!(&buf[..], b"foo");
      }
    }
  }
}
