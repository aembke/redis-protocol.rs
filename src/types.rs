use crate::{error::RedisParseError, resp2::types::Frame as Resp2Frame, resp3::types::Frame as Resp3Frame};
use alloc::{format, string::String, vec::Vec};
use core::{
  borrow::Borrow,
  fmt::{Debug, Display},
  str::FromStr,
};
use nom::IResult;

/// Terminating bytes between frames.
pub const CRLF: &'static str = "\r\n";

// TODO format - (remaining, amt parsed so far), result type (usually bytes), error
///
pub type DResult<'a, T> = IResult<(&'a [u8], usize), T, RedisParseError<&'a [u8]>>;

/// A cluster redirection message.
///
/// <https://redis.io/topics/cluster-spec#redirection-and-resharding>
#[derive(Clone, Debug)]
pub enum Redirection<S: Debug + Display + FromStr> {
  Moved { slot: u16, server: S },
  Ask { slot: u16, server: S },
}

impl<S: Debug + Display + FromStr> Redirection<S> {
  ///
  pub fn from_str(s: &str) -> Option<Self<S>> {
    let mut parts = s.split(" ");
    let is_moved = match parts.next() {
      Some("MOVED") => true,
      Some("ASK") => false,
      _ => return None,
    };
    let slot = parts.next().and_then(|s| s.parse::<u16>().ok())?;
    let server = parts.next().and_then(|s| S::from_str(s).ok())?;

    Some(if is_moved {
      Redirection::Moved { slot, server }
    } else {
      Redirection::Ask { slot, server }
    })
  }

  ///
  pub fn to_resp2_frame(&self) -> Resp2Frame {
    let inner = match *self {
      Redirection::Moved { ref slot, ref server } => format!("MOVED {} {}", slot, server),
      Redirection::Ask { ref slot, ref server } => format!("ASK {} {}", slot, server),
    };

    Resp2Frame::Error(inner.into())
  }

  ///
  pub fn to_resp3_frame(&self) -> Resp3Frame {
    let inner = match *self {
      Redirection::Moved { ref slot, ref server } => format!("MOVED {} {}", slot, server),
      Redirection::Ask { ref slot, ref server } => format!("ASK {} {}", slot, server),
    };

    Resp3Frame::SimpleError {
      data:       inner.into(),
      attributes: None,
    }
  }
}
