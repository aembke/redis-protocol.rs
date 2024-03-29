use alloc::{borrow::Cow, format, string::FromUtf8Error};
use cookie_factory::GenError;
use core::{borrow::Borrow, fmt, fmt::Debug, str::Utf8Error};
use nom::{
  error::{ErrorKind, FromExternalError, ParseError},
  Err as NomError,
  Needed,
};

#[cfg(feature = "bytes")]
use bytes_utils::string::Utf8Error as BytesUtf8Error;
#[cfg(feature = "std")]
use std::error::Error;
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
  /// An error parsing a value or converting between types.
  Parse,
}

impl PartialEq for RedisProtocolErrorKind {
  fn eq(&self, other: &Self) -> bool {
    use self::RedisProtocolErrorKind::*;

    match *self {
      EncodeError => matches!(other, EncodeError),
      DecodeError => matches!(other, DecodeError),
      BufferTooSmall(amt) => match other {
        BufferTooSmall(_amt) => amt == *_amt,
        _ => false,
      },
      #[cfg(feature = "std")]
      IO(_) => matches!(other, IO(_)),
      Unknown => matches!(other, Unknown),
      Parse => matches!(other, Parse),
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
      Parse => "Parse Error",
      #[cfg(feature = "std")]
      IO(_) => "IO Error",
      BufferTooSmall(_) => "Buffer too small",
    }
  }
}

/// The default error type used with all external functions in this library.
#[derive(Debug, Eq, PartialEq)]
pub struct RedisProtocolError {
  details: Cow<'static, str>,
  kind:    RedisProtocolErrorKind,
}

impl RedisProtocolError {
  pub fn buffer_too_small(amt: usize) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::BufferTooSmall(amt), "")
  }

  pub fn new<S: Into<Cow<'static, str>>>(kind: RedisProtocolErrorKind, desc: S) -> Self {
    RedisProtocolError {
      kind,
      details: desc.into(),
    }
  }

  #[cfg(feature = "convert")]
  pub(crate) fn new_parse<S: Into<Cow<'static, str>>>(desc: S) -> Self {
    RedisProtocolError {
      kind:    RedisProtocolErrorKind::Parse,
      details: desc.into(),
    }
  }

  pub fn details(&self) -> &str {
    self.details.borrow()
  }

  pub fn new_empty() -> Self {
    RedisProtocolError {
      kind:    RedisProtocolErrorKind::Unknown,
      details: "".into(),
    }
  }

  pub fn kind(&self) -> &RedisProtocolErrorKind {
    &self.kind
  }
}

impl fmt::Display for RedisProtocolError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}: {}", self.kind.to_str(), self.details)
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
    if let NomError::Incomplete(Needed::Size(s)) = e {
      RedisProtocolError {
        kind:    RedisProtocolErrorKind::BufferTooSmall(s.get()),
        details: Cow::Borrowed(""),
      }
    } else {
      let desc = match e {
        NomError::Failure(inner) => format!("Failure: {:?}", inner.code),
        NomError::Error(inner) => format!("Error: {:?}", inner.code),
        _ => format!("{:?}", e),
      };

      RedisProtocolError {
        kind:    RedisProtocolErrorKind::DecodeError,
        details: Cow::Owned(desc),
      }
    }
  }
}

impl From<NomError<&[u8]>> for RedisProtocolError {
  fn from(e: NomError<&[u8]>) -> Self {
    if let NomError::Incomplete(Needed::Size(s)) = e {
      RedisProtocolError {
        kind:    RedisProtocolErrorKind::BufferTooSmall(s.get()),
        details: Cow::Borrowed(""),
      }
    } else {
      RedisProtocolError {
        kind:    RedisProtocolErrorKind::DecodeError,
        details: Cow::Owned(format!("{:?}", e)),
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

impl From<FromUtf8Error> for RedisProtocolError {
  fn from(e: FromUtf8Error) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::Unknown, format!("{:?}", e))
  }
}

#[cfg(feature = "bytes")]
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
      RedisParseError::Custom { context, message } => write!(f, "{}: {}", context, message),
      RedisParseError::Nom(input, kind) => write!(f, "{:?} at {:?}", kind, input),
      RedisParseError::Incomplete(needed) => write!(f, "Incomplete({:?})", needed),
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

#[cfg(feature = "bytes")]
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

#[cfg(feature = "std")]
impl std::error::Error for RedisProtocolError {
  fn description(&self) -> &str {
    self.details()
  }

  fn source(&self) -> Option<&(dyn Error + 'static)> {
    match self.kind {
      RedisProtocolErrorKind::IO(ref e) => Some(e),
      _ => None,
    }
  }
}

#[cfg(feature = "convert")]
impl From<core::num::ParseIntError> for RedisProtocolError {
  fn from(value: core::num::ParseIntError) -> Self {
    RedisProtocolError::new_parse(format!("{:?}", value))
  }
}

#[cfg(feature = "convert")]
impl From<core::num::ParseFloatError> for RedisProtocolError {
  fn from(value: core::num::ParseFloatError) -> Self {
    RedisProtocolError::new_parse(format!("{:?}", value))
  }
}
