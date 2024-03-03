use crate::{
  error::{RedisProtocolError, RedisProtocolErrorKind},
  resp2::types::Frame as Resp2Frame,
  resp3::types::Frame as Resp3Frame,
};
use alloc::{format, string::String};

/// Terminating bytes between frames.
pub const CRLF: &str = "\r\n";
/// The number of slots in a Redis cluster.
pub const REDIS_CLUSTER_SLOTS: u16 = 16384;
/// The prefix on normal pubsub messages.
///
/// In RESP2 this may be the first inner frame, and in RESP3 it may be the second inner frame.
pub const PUBSUB_PREFIX: &str = "message";
/// The prefix on pubsub messages from a pattern matching subscription.
///
/// In RESP2 this may be the first inner frame, and in RESP3 it may be the second inner frame.
pub const PATTERN_PUBSUB_PREFIX: &str = "pmessage";
/// The prefix on pubsub messages from a shard channel subscription.
///
/// In RESP2 this may be the first inner frame, and in RESP3 it may be the second inner frame.
pub const SHARD_PUBSUB_PREFIX: &str = "smessage";
/// Prefix on RESP3 push pubsub messages.
///
/// In RESP3 this is the first inner frame in a push pubsub message.
pub const PUBSUB_PUSH_PREFIX: &str = "pubsub";

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

    Resp2Frame::Error(inner.into())
  }

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

impl TryFrom<&str> for Redirection {
  type Error = RedisProtocolError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    if value.starts_with("MOVED") {
      let parts: Vec<&str> = value.split(" ").collect();
      if parts.len() == 3 {
        let slot = unwrap_return!(parts[1].parse::<u16>().ok());
        let server = parts[2].to_owned();

        Ok(Redirection::Moved { slot, server })
      } else {
        Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Invalid cluster redirection.",
        ))
      }
    } else if value.starts_with("ASK") {
      let parts: Vec<&str> = value.split(" ").collect();
      if parts.len() == 3 {
        let slot = unwrap_return!(parts[1].parse::<u16>().ok());
        let server = parts[2].to_owned();

        Ok(Redirection::Ask { slot, server })
      } else {
        Err(RedisProtocolError::new(
          RedisProtocolErrorKind::Unknown,
          "Invalid cluster redirection.",
        ))
      }
    } else {
      Err(RedisProtocolError::new(
        RedisProtocolErrorKind::Unknown,
        "Invalid cluster redirection.",
      ))
    }
  }
}
