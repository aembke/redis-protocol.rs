use crate::resp2::types::Frame as Resp2Frame;
use crate::resp3::types::Frame as Resp3Frame;
use cookie_factory::GenError;
use nom::error::{ContextError, ErrorKind, ParseError};
use nom::{Err as NomError, Needed};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fmt;
use std::io::Error as IoError;
use std::str;
use utils;

/// Terminating bytes between frames.
pub const CRLF: &'static str = "\r\n";

/// The kind of error without any associated data.
#[derive(Debug)]
pub enum RedisProtocolErrorKind {
  /// An error that occurred while encoding data.
  EncodeError,
  /// An error indicating that the provided buffer needs to be extended by the inner `usize` bytes before encoding can continue.
  BufferTooSmall(usize),
  /// An error that occurred while decoding data.
  DecodeError,
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

impl From<IoError> for RedisProtocolError {
  fn from(e: IoError) -> Self {
    RedisProtocolError::new(RedisProtocolErrorKind::IO(e), "IO Error")
  }
}

/// A struct defining parse errors when decoding frames.
pub struct RedisParseError<'a> {
  pub input: Vec<&'a [u8]>,
  pub context: &'static str,
  pub message: Option<String>,
  pub kind: Vec<ErrorKind>,
  pub needed: Option<usize>,
}

impl<'a> RedisParseError<'a> {
  pub fn new<S: Into<String>>(ctx: &'static str, message: S, kind: Option<ErrorKind>) -> Self {
    let kind = kind.map(|k| vec![k]).unwrap_or(Vec::new());

    RedisParseError {
      input: Vec::new(),
      context: ctx,
      message: Some(message.into()),
      kind,
      needed: None,
    }
  }
}

impl<'a> ParseError<&'a [u8]> for RedisParseError<'a> {
  fn from_error_kind(input: &'a [u8], kind: ErrorKind) -> Self {
    RedisParseError {
      input: vec![input],
      kind: vec![kind],
      context: "Parse Error",
      message: None,
      needed: None,
    }
  }

  fn append(input: &'a [u8], kind: ErrorKind, mut other: Self) -> Self {
    other.input.push(input);
    other.kind.push(kind);

    other
  }
}

impl<'a> ContextError<&'a [u8]> for RedisParseError<'a> {
  fn add_context(input: &'a [u8], ctx: &'static str, mut other: Self) -> Self {
    other.context = ctx;
    other.input.push(input);
    other
  }
}

impl<'a> From<NomError<nom::error::Error<&'a [u8]>>> for RedisParseError<'a> {
  fn from(e: NomError<nom::error::Error<&[u8]>>) -> Self {
    let (ctx, kind, input) = match e {
      NomError::Failure(inner) => ("failure", inner.code, inner.input),
      NomError::Error(inner) => ("error", inner.code, inner.input),
      NomError::Incomplete(needed) => {
        let needed = match needed {
          Needed::Unknown => None,
          Needed::Size(s) => Some(s.get()),
        };

        return RedisParseError {
          input: vec![],
          kind: vec![],
          context: "incomplete",
          message: None,
          needed,
        };
      }
    };

    RedisParseError {
      context: ctx,
      kind: vec![kind],
      input: vec![input],
      message: Some("Parse Error".into()),
      needed: None,
    }
  }
}

impl<'a> From<RedisParseError<'a>> for RedisProtocolError {
  fn from(e: RedisParseError<'a>) -> Self {
    let message = match e.message {
      Some(ref msg) => msg,
      None => "Parse Error",
    };

    RedisProtocolError {
      kind: RedisProtocolErrorKind::DecodeError,
      desc: Cow::Owned(format!("{}: {}", e.context, message)),
    }
  }
}

/// A cluster redirection message.
///
/// <https://redis.io/topics/cluster-spec#redirection-and-resharding>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Redirection {
  Moved { slot: u16, server: String },
  Ask { slot: u16, server: String },
}

impl Redirection {
  pub fn to_resp2_frame(&self) -> Resp2Frame {
    let inner = match *self {
      Redirection::Moved { ref slot, ref server } => format!("MOVED {} {}", slot, server),
      Redirection::Ask { ref slot, ref server } => format!("ASK {} {}", slot, server),
    };

    Resp2Frame::Error(inner)
  }

  pub fn to_resp3_frame(&self) -> Resp3Frame {
    let inner = match *self {
      Redirection::Moved { ref slot, ref server } => format!("MOVED {} {}", slot, server),
      Redirection::Ask { ref slot, ref server } => format!("ASK {} {}", slot, server),
    };

    Resp3Frame::SimpleError {
      data: inner,
      attributes: None,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::num::NonZeroUsize;
  use utils::ZEROED_KB;

  // gotta pad those coveralls stats...
  #[test]
  fn should_create_empty_error() {
    let e = RedisProtocolError::new_empty();

    assert_eq!(e.description(), "");
    assert_eq!(e.kind(), &RedisProtocolErrorKind::Unknown);
  }

  #[test]
  fn should_create_encode_error() {
    let e = RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "foo");

    assert_eq!(e.description(), "foo");
    assert_eq!(e.kind(), &RedisProtocolErrorKind::EncodeError);
  }

  #[test]
  fn should_create_decode_error() {
    let e = RedisProtocolError::new(RedisProtocolErrorKind::DecodeError, "foo");

    assert_eq!(e.description(), "foo");
    assert_eq!(e.kind(), &RedisProtocolErrorKind::DecodeError);
  }

  #[test]
  fn should_create_buf_too_small_error() {
    let e = RedisProtocolError::new(RedisProtocolErrorKind::BufferTooSmall(10), "foo");

    assert_eq!(e.description(), "foo");
    assert_eq!(e.kind(), &RedisProtocolErrorKind::BufferTooSmall(10));
  }

  #[test]
  fn should_cast_from_nom_incomplete() {
    let n: NomError<&[u8]> = NomError::Incomplete(Needed::Size(NonZeroUsize::new(10).unwrap()));
    let e = RedisProtocolError::from(n);

    assert_eq!(e.kind(), &RedisProtocolErrorKind::BufferTooSmall(10));
  }

  #[test]
  fn should_print_error_kinds() {
    assert_eq!(RedisProtocolErrorKind::EncodeError.to_str(), "Encode Error");
    assert_eq!(RedisProtocolErrorKind::DecodeError.to_str(), "Decode Error");
    assert_eq!(RedisProtocolErrorKind::Unknown.to_str(), "Unknown Error");
    assert_eq!(RedisProtocolErrorKind::BufferTooSmall(10).to_str(), "Buffer too small");
  }
}
