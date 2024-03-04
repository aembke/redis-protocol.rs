use crate::error::{RedisParseError, RedisProtocolError, RedisProtocolErrorKind};
use alloc::{format, string::String};
use nom::IResult;

/// Alternative alias for `std::ops::Range`.
pub(crate) type _Range = (usize, usize);
/// A wrapper type for the nom result type that stores range offsets alongside the buffer.
pub(crate) type DResult<'a, T> = IResult<(&'a [u8], usize), T, RedisParseError<&'a [u8]>>;

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
