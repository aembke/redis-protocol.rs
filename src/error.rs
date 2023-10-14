use alloc::{borrow::Cow, format, string::String};
use bytes_utils::string::Utf8Error as BytesUtf8Error;
use cookie_factory::GenError;
use core::{fmt, fmt::Debug, str::Utf8Error};
use nom::{
  error::{ErrorKind, FromExternalError, ParseError},
  Err as NomError,
  Needed,
};

#[cfg(feature = "std")]
use std::io::Error as IoError;

/// The kind of error without any associated data.
#[derive(Debug)]
pub enum RedisProtocolErrorKind {
  /// An error that occurred while encoding data.
  EncodeError,
  /// An error indicating that the provided buffer needs to be extended by the inner `usize` bytes before encoding
  /// can continue.
  BufferTooSmall(usize),
  /// An error that occurred while decoding data.
  DecodeError,
  #[cfg(feature = "std")]
  /// An IO error.
  IO(IoError),
  /// An unknown error, or an error that can occur during encoding or decoding.
  Unknown,
}

impl PartialEq for RedisProtocolErrorKind {
  fn eq(&self, other: &Self) -> bool {
    use self::RedisProtocolErrorKind::*;

    match *self {
      EncodeError => match *other {
        EncodeError => true,
        _ => false,
      },
      DecodeError => match *other {
        DecodeError => true,
        _ => false,
      },
      BufferTooSmall(amt) => match *other {
        BufferTooSmall(_amt) => amt == amt,
        _ => false,
      },
      #[cfg(feature = "std")]
      IO(_) => match *other {
        IO(_) => true,
        _ => false,
      },
      Unknown => match *other {
        Unknown => true,
        _ => false,
      },
    }
  }
}

impl Eq for RedisProtocolErrorKind {}

impl RedisProtocolErrorKind {
  pub fn to_str(&self) -> &'static str {
    use self::RedisProtocolErrorKind::*;

    match *self {
      EncodeError => "Encode Error",
      DecodeError => "Decode Error",
      Unknown => "Unknown Error",
      #[cfg(feature = "std")]
      IO(_) => "IO Error",
      BufferTooSmall(_) => "Buffer too small",
    }
  }
}

/// The default error type used with all external functions in this library.
#[derive(Debug, Eq, PartialEq)]
pub struct RedisProtocolError {
  desc: Cow<'static, str>,
  kind: RedisProtocolErrorKind,
}

impl RedisProtocolError {
  pub fn buffer_too_small(amt: usize) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::BufferTooSmall(amt), "")
  }

  pub fn new<S: Into<Cow<'static, str>>>(kind: RedisProtocolErrorKind, desc: S) -> Self {
    RedisProtocolError {
      kind,
      desc: desc.into(),
    }
  }

  pub fn description(&self) -> &str {
    self.desc.borrow()
  }

  pub fn new_empty() -> Self {
    RedisProtocolError {
      kind: RedisProtocolErrorKind::Unknown,
      desc: "".into(),
    }
  }

  pub fn to_string(&self) -> String {
    format!("{}: {}", self.kind.to_str(), self.desc)
  }

  pub fn kind(&self) -> &RedisProtocolErrorKind {
    &self.kind
  }
}

impl fmt::Display for RedisProtocolError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}: {}", self.kind.to_str(), self.desc)
  }
}

impl From<GenError> for RedisProtocolError {
  fn from(e: GenError) -> Self {
    match e {
      GenError::CustomError(i) => match i {
        1 => RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "Invalid frame kind."),
        2 => RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "Cannot encode NaN."),
        3 => RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "Cannot stream non aggregate type."),
        _ => RedisProtocolError::new_empty(),
      },
      GenError::InvalidOffset => RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "Invalid offset."),
      GenError::BufferTooSmall(b) => RedisProtocolError::buffer_too_small(b),
      _ => RedisProtocolError::new_empty(),
    }
  }
}

impl From<NomError<nom::error::Error<&[u8]>>> for RedisProtocolError {
  fn from(e: NomError<nom::error::Error<&[u8]>>) -> Self {
    if let NomError::Incomplete(Needed::Size(ref s)) = e {
      RedisProtocolError {
        kind: RedisProtocolErrorKind::BufferTooSmall(s.get()),
        desc: Cow::Borrowed(""),
      }
    } else {
      let desc = match e {
        NomError::Failure(inner) => format!("Failure: {:?}", inner.code),
        NomError::Error(inner) => format!("Error: {:?}", inner.code),
        _ => format!("{:?}", e),
      };

      RedisProtocolError {
        kind: RedisProtocolErrorKind::DecodeError,
        desc: Cow::Owned(desc),
      }
    }
  }
}

impl From<NomError<&[u8]>> for RedisProtocolError {
  fn from(e: NomError<&[u8]>) -> Self {
    if let NomError::Incomplete(Needed::Size(ref s)) = e {
      RedisProtocolError {
        kind: RedisProtocolErrorKind::BufferTooSmall(s.get()),
        desc: Cow::Borrowed(""),
      }
    } else {
      RedisProtocolError {
        kind: RedisProtocolErrorKind::DecodeError,
        desc: Cow::Owned(format!("{:?}", e)),
      }
    }
  }
}

#[cfg(feature = "std")]
impl From<IoError> for RedisProtocolError {
  fn from(e: IoError) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::IO(e), "IO Error")
  }
}

impl<I> From<RedisParseError<I>> for RedisProtocolError
where
  I: Debug,
{
  fn from(e: RedisParseError<I>) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::DecodeError, format!("{:?}", e))
  }
}

impl<B> From<BytesUtf8Error<B>> for RedisProtocolError {
  fn from(e: BytesUtf8Error<B>) -> Self {
    e.utf8_error().into()
  }
}

impl From<Utf8Error> for RedisProtocolError {
  fn from(e: Utf8Error) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::DecodeError, format!("{}", e))
  }
}

/// A struct defining parse errors when decoding frames.
pub enum RedisParseError<I> {
  Custom {
    context: &'static str,
    message: Cow<'static, str>,
  },
  Incomplete(Needed),
  Nom(I, ErrorKind),
}

impl<I> fmt::Debug for RedisParseError<I>
where
  I: Debug,
{
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      RedisParseError::Custom {
        ref context,
        ref message,
      } => write!(f, "{}: {}", context, message),
      RedisParseError::Nom(input, kind) => write!(f, "{:?} at {:?}", kind, input),
      RedisParseError::Incomplete(ref needed) => write!(f, "Incomplete({:?})", needed),
    }
  }
}

impl<I> RedisParseError<I> {
  pub fn new_custom<S: Into<Cow<'static, str>>>(ctx: &'static str, message: S) -> Self {
    RedisParseError::Custom {
      context: ctx,
      message: message.into(),
    }
  }

  pub fn into_nom_error(self) -> nom::Err<RedisParseError<I>> {
    match self {
      RedisParseError::Incomplete(n) => nom::Err::Incomplete(n),
      _ => nom::Err::Failure(self),
    }
  }
}

impl<I> ParseError<I> for RedisParseError<I> {
  fn from_error_kind(input: I, kind: ErrorKind) -> Self {
    RedisParseError::Nom(input, kind)
  }

  fn append(_: I, _: ErrorKind, other: Self) -> Self {
    other
  }
}

impl<I, O> ParseError<(I, O)> for RedisParseError<I> {
  fn from_error_kind(input: (I, O), kind: ErrorKind) -> Self {
    RedisParseError::Nom(input.0, kind)
  }

  fn append(_: (I, O), _: ErrorKind, other: Self) -> Self {
    other
  }
}

impl<I, E> FromExternalError<I, E> for RedisParseError<I> {
  fn from_external_error(input: I, kind: ErrorKind, _e: E) -> Self {
    RedisParseError::Nom(input, kind)
  }
}

impl<I, O, E> FromExternalError<(I, O), E> for RedisParseError<I> {
  fn from_external_error(input: (I, O), kind: ErrorKind, _e: E) -> Self {
    RedisParseError::Nom(input.0, kind)
  }
}

impl<I> From<nom::Err<RedisParseError<I>>> for RedisParseError<I> {
  fn from(e: NomError<RedisParseError<I>>) -> Self {
    match e {
      NomError::Incomplete(n) => RedisParseError::Incomplete(n),
      NomError::Failure(e) | NomError::Error(e) => e,
    }
  }
}

impl<B, I> From<BytesUtf8Error<B>> for RedisParseError<I> {
  fn from(e: BytesUtf8Error<B>) -> Self {
    e.utf8_error().into()
  }
}

impl<I> From<Utf8Error> for RedisParseError<I> {
  fn from(e: Utf8Error) -> Self {
    RedisParseError::new_custom("parse_utf8", format!("{}", e))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use core::num::NonZeroUsize;

  #[test]
  fn should_cast_from_nom_incomplete() {
    let n: NomError<&[u8]> = NomError::Incomplete(Needed::Size(NonZeroUsize::new(10).unwrap()));
    let e = RedisProtocolError::from(n);

    assert_eq!(e.kind(), &RedisProtocolErrorKind::BufferTooSmall(10));
  }
}
