use crate::error::RedisParseError;
use nom::IResult;

/// Alternative alias for `std::ops::Range`.
pub(crate) type _Range = (usize, usize);
/// A wrapper type for the nom result type that stores range offsets alongside the buffer.
pub(crate) type DResult<'a, T> = IResult<(&'a [u8], usize), T, RedisParseError<&'a [u8]>>;

/// The maximum nesting depth of aggregate frames (arrays, maps, sets, attributes, etc.) supported by the decoders.
///
/// Frames nested more deeply than this return a parse error rather than recursing further. This protects callers from
/// stack overflows when decoding untrusted input.
pub const MAX_DECODE_DEPTH: usize = 128;

/// The maximum initial capacity, in bytes, of the buffer used to store the inner frames of an aggregate type.
pub(crate) const MAX_INITIAL_CAPACITY_BYTES: usize = 65536;

/// The result of scanning a buffer for a complete frame.
///
/// See the `FrameScanner` types in the [resp2](crate::resp2::decode::FrameScanner) and
/// [resp3](crate::resp3::decode::FrameScanner) decoding modules.
#[cfg(any(feature = "resp2", feature = "resp3"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Scan {
  /// The buffer does not contain a complete frame yet. Decoding it now would return `Ok(None)`.
  Incomplete,
  /// The first `n` bytes of the buffer contain a complete frame. Decoding it now would return that frame.
  Complete(usize),
  /// The buffer does not start with a valid frame. Decoding it now would return an error.
  Invalid,
  /// The next frame is larger than the scanner's maximum frame size, or would be once it's complete.
  TooLarge,
}

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
