use utils;

use std::borrow::Cow;
use std::fmt;
use std::str;

use std::borrow::Borrow;
use std::error::Error;

use cookie_factory::GenError;

use nom::{Context, Err as NomError, Needed};

const PUBSUB_PREFIX: &'static str = "message";

pub const SIMPLESTRING_BYTE: u8 = b'+';
pub const ERROR_BYTE: u8 = b'-';
pub const INTEGER_BYTE: u8 = b':';
pub const BULKSTRING_BYTE: u8 = b'$';
pub const ARRAY_BYTE: u8 = b'*';

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisProtocolErrorKind {
    /// An error that occurred while encoding data.
    EncodeError,
    /// An error indicating that the provided buffer needs to be extended by the inner `usize` bytes before encoding can continue.
    BufferTooSmall(usize),
    /// An error that occurred while decoding data.
    DecodeError,
    /// An unknown error, or an error that can occur during encoding or decoding.
    Unknown,
}

impl RedisProtocolErrorKind {
    pub fn to_str(&self) -> &'static str {
        use self::RedisProtocolErrorKind::*;

        match *self {
            EncodeError => "Encode Error",
            DecodeError => "Decode Error",
            Unknown => "Unknown Error",
            BufferTooSmall(_) => "Buffer too small",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisProtocolError<'a> {
    desc: Cow<'static, str>,
    kind: RedisProtocolErrorKind,
    context: Option<&'a [u8]>,
}

impl<'a> RedisProtocolError<'a> {
    pub fn new<S: Into<Cow<'static, str>>>(kind: RedisProtocolErrorKind, desc: S) -> Self {
        RedisProtocolError {
            kind,
            desc: desc.into(),
            context: None,
        }
    }

    pub fn new_empty() -> Self {
        RedisProtocolError {
            kind: RedisProtocolErrorKind::Unknown,
            desc: "".into(),
            context: None,
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}: {}", self.kind.to_str(), self.desc)
    }

    pub fn kind(&self) -> &RedisProtocolErrorKind {
        &self.kind
    }

    /// Attempt to read the underlying data on which the encoding or decoding error occurred.
    pub fn context(&self) -> Option<&[u8]> {
        match self.context {
            Some(ref c) => Some(c),
            None => None,
        }
    }
}

impl<'a> fmt::Display for RedisProtocolError<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.kind.to_str(), self.desc)
    }
}

impl<'a> Error for RedisProtocolError<'a> {
    fn description(&self) -> &str {
        self.desc.borrow()
    }
}

// yikes
impl<'a> From<GenError> for RedisProtocolError<'a> {
    fn from(e: GenError) -> Self {
        match e {
            GenError::CustomError(i) => match i {
                1 => RedisProtocolError::new(
                    RedisProtocolErrorKind::EncodeError,
                    "Invalid frame kind.",
                ),
                _ => RedisProtocolError::new_empty(),
            },
            GenError::InvalidOffset => {
                RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid offset.")
            }
            GenError::BufferTooSmall(b) => RedisProtocolError::new(
                RedisProtocolErrorKind::BufferTooSmall(b),
                format!("Need {} more bytes", b),
            ),
            _ => RedisProtocolError::new_empty(),
        }
    }
}

impl<'a> From<NomError<&'a [u8]>> for RedisProtocolError<'a> {
    fn from(e: NomError<&'a [u8]>) -> Self {
        if let NomError::Incomplete(Needed::Size(ref s)) = e {
            RedisProtocolError {
                kind: RedisProtocolErrorKind::BufferTooSmall(*s),
                desc: Cow::Owned(format!("{:?}", e)),
                context: None,
            }
        } else {
            let context = match e {
                NomError::Failure(Context::Code(i, _)) => Some(i),
                NomError::Error(Context::Code(i, _)) => Some(i),
                _ => None,
            };

            RedisProtocolError {
                kind: RedisProtocolErrorKind::Unknown,
                desc: Cow::Owned(format!("{:?}", e)),
                context,
            }
        }
    }
}

/// An enum representing the kind of a Frame without references to any inner data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameKind {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Moved,
    Ask,
    Null,
}

impl FrameKind {
    pub fn from_byte(d: u8) -> Option<FrameKind> {
        use self::FrameKind::*;

        match d {
            SIMPLESTRING_BYTE => Some(SimpleString),
            ERROR_BYTE => Some(Error),
            INTEGER_BYTE => Some(Integer),
            BULKSTRING_BYTE => Some(BulkString),
            ARRAY_BYTE => Some(Array),
            _ => None,
        }
    }

    pub fn to_byte(&self) -> u8 {
        use self::FrameKind::*;

        match *self {
            SimpleString => SIMPLESTRING_BYTE,
            Error | Moved | Ask => ERROR_BYTE,
            Integer => INTEGER_BYTE,
            BulkString | Null => BULKSTRING_BYTE,
            Array => ARRAY_BYTE,
        }
    }
}

/// An enum representing a Frame of data. Frames are recursively defined to account for arrays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<Frame>),
    Moved(String),
    Ask(String),
    Null,
}

impl Frame {
    /// Whether or not the frame is an error.
    pub fn is_error(&self) -> bool {
        match self.kind() {
            FrameKind::Error | FrameKind::Moved | FrameKind::Ask => true,
            _ => false,
        }
    }

    /// Whether or not the frame represents a message on a publish-subscribe channel.
    pub fn is_pubsub_message(&self) -> bool {
        if let Frame::Array(ref frames) = *self {
            frames.len() == 3
                && frames[0].kind() == FrameKind::BulkString
                && frames[0]
                    .as_str()
                    .map(|s| s == PUBSUB_PREFIX)
                    .unwrap_or(false)
        } else {
            false
        }
    }

    /// Read the `FrameKind` value for this frame.
    pub fn kind(&self) -> FrameKind {
        match *self {
            Frame::SimpleString(_) => FrameKind::SimpleString,
            Frame::Error(_) => FrameKind::Error,
            Frame::Integer(_) => FrameKind::Integer,
            Frame::BulkString(_) => FrameKind::BulkString,
            Frame::Array(_) => FrameKind::Array,
            Frame::Moved(_) => FrameKind::Moved,
            Frame::Ask(_) => FrameKind::Ask,
            Frame::Null => FrameKind::Null,
        }
    }

    /// Attempt to read the frame value as a string slice.
    pub fn as_str(&self) -> Option<&str> {
        match *self {
            Frame::BulkString(ref b) => str::from_utf8(b).ok(),
            Frame::SimpleString(ref s) => Some(s),
            Frame::Error(ref s) => Some(s),
            _ => None,
        }
    }

    /// Whether or not the frame is a simple string or bulk string.
    pub fn is_string(&self) -> bool {
        match *self {
            Frame::SimpleString(_) | Frame::BulkString(_) => true,
            _ => false,
        }
    }

    /// Whether or not the frame is Null.
    pub fn is_null(&self) -> bool {
        match *self {
            Frame::Null => true,
            _ => false,
        }
    }

    /// Whether or not the frame is an array of frames.
    pub fn is_array(&self) -> bool {
        match *self {
            Frame::Array(_) => true,
            _ => false,
        }
    }

    /// Whether or not the frame is an integer.
    pub fn is_integer(&self) -> bool {
        match *self {
            Frame::Integer(_) => true,
            _ => false,
        }
    }

    /// Whether or not the framed is a a Moved or Ask error.
    pub fn is_moved_or_ask_error(&self) -> bool {
        match *self {
            Frame::Moved(_) | Frame::Ask(_) => true,
            _ => false,
        }
    }

    // Copy and read the inner value as a string, if possible.
    pub fn to_string(&self) -> Option<String> {
        match *self {
            Frame::SimpleString(ref s) => Some(s.clone()),
            Frame::BulkString(ref b) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        }
    }

    /// Attempt to parse the frame as a publish-subscribe message, returning the `(channel, message)` tuple
    /// if successful, or the original frame if the inner data is not a publish-subscribe message.
    pub fn parse_as_pubsub(self) -> Result<(String, String), Self> {
        if self.is_pubsub_message() {
            // if `is_pubsub_message` returns true but this panics then there's a bug in `is_pubsub_message`, so this fails loudly
            let (message, channel, _) = match self {
                Frame::Array(mut frames) => (
                    utils::opt_frame_to_string_panic(
                        frames.pop(),
                        "Expected pubsub payload. This is a bug.",
                    ),
                    utils::opt_frame_to_string_panic(
                        frames.pop(),
                        "Expected pubsub channel. This is a bug.",
                    ),
                    utils::opt_frame_to_string_panic(
                        frames.pop(),
                        "Expected pubsub message kind. This is a bug.",
                    ),
                ),
                _ => panic!("Unreachable 1. This is a bug."),
            };

            Ok((channel, message))
        } else {
            Err(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utils::ZEROED_KB;

    use nom::ErrorKind as NomErrorKind;

    #[test]
    fn should_parse_pubsub_message() {
        let frame = Frame::Array(vec![
            Frame::BulkString("message".into()),
            Frame::BulkString("foo".into()),
            Frame::BulkString("bar".into()),
        ]);

        let (channel, message) = frame.parse_as_pubsub().expect("Expected pubsub frames");

        assert_eq!(channel, "foo");
        assert_eq!(message, "bar");
    }

    #[test]
    #[should_panic]
    fn should_fail_parsing_non_pubsub_message() {
        let frame = Frame::Array(vec![
            Frame::BulkString("baz".into()),
            Frame::BulkString("foo".into()),
        ]);

        frame.parse_as_pubsub().expect("Expected non pubsub frames");
    }

    // gotta pad those coveralls stats...
    #[test]
    fn should_create_empty_error() {
        let e = RedisProtocolError::new_empty();
        let _s = e.to_string();

        assert_eq!(e.description(), "");
        assert_eq!(e.kind(), &RedisProtocolErrorKind::Unknown);
        assert_eq!(e.context(), None);
    }

    #[test]
    fn should_create_encode_error() {
        let e = RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "foo");
        let _s = e.to_string();

        assert_eq!(e.description(), "foo");
        assert_eq!(e.kind(), &RedisProtocolErrorKind::EncodeError);
        assert_eq!(e.context(), None);
    }

    #[test]
    fn should_create_decode_error() {
        let e = RedisProtocolError::new(RedisProtocolErrorKind::DecodeError, "foo");
        let _s = e.to_string();

        assert_eq!(e.description(), "foo");
        assert_eq!(e.kind(), &RedisProtocolErrorKind::DecodeError);
        assert_eq!(e.context(), None);
    }

    #[test]
    fn should_create_buf_too_small_error() {
        let e = RedisProtocolError::new(RedisProtocolErrorKind::BufferTooSmall(10), "foo");
        let _s = e.to_string();

        assert_eq!(e.description(), "foo");
        assert_eq!(e.kind(), &RedisProtocolErrorKind::BufferTooSmall(10));
        assert_eq!(e.context(), None);
    }

    #[test]
    fn should_cast_from_nom_failure() {
        let n = NomError::Failure(Context::Code(&ZEROED_KB[0..10], NomErrorKind::Custom(1)));
        let e = RedisProtocolError::from(n);

        assert_eq!(e.context(), Some(&ZEROED_KB[0..10]))
    }

    #[test]
    fn should_cast_from_nom_error() {
        let n = NomError::Error(Context::Code(&ZEROED_KB[0..10], NomErrorKind::Custom(1)));
        let e = RedisProtocolError::from(n);

        assert_eq!(e.context(), Some(&ZEROED_KB[0..10]))
    }

    #[test]
    fn should_cast_from_nom_incomplete() {
        let n = NomError::Incomplete(Needed::Size(10));
        let e = RedisProtocolError::from(n);

        assert_eq!(e.kind(), &RedisProtocolErrorKind::BufferTooSmall(10));
    }

    #[test]
    fn should_check_frame_types() {
        let f = Frame::Null;
        assert!(f.is_null());
        assert!(!f.is_string());
        assert!(!f.is_error());
        assert!(!f.is_array());
        assert!(!f.is_integer());
        assert!(!f.is_moved_or_ask_error());

        let f = Frame::BulkString("foo".as_bytes().to_vec());
        assert!(!f.is_null());
        assert!(f.is_string());
        assert!(!f.is_error());
        assert!(!f.is_array());
        assert!(!f.is_integer());
        assert!(!f.is_moved_or_ask_error());

        let f = Frame::SimpleString("foo".into());
        assert!(!f.is_null());
        assert!(f.is_string());
        assert!(!f.is_error());
        assert!(!f.is_array());
        assert!(!f.is_integer());
        assert!(!f.is_moved_or_ask_error());

        let f = Frame::Error("foo".into());
        assert!(!f.is_null());
        assert!(!f.is_string());
        assert!(f.is_error());
        assert!(!f.is_array());
        assert!(!f.is_integer());
        assert!(!f.is_moved_or_ask_error());

        let f = Frame::Array(vec![Frame::SimpleString("foo".into())]);
        assert!(!f.is_null());
        assert!(!f.is_string());
        assert!(!f.is_error());
        assert!(f.is_array());
        assert!(!f.is_integer());
        assert!(!f.is_moved_or_ask_error());

        let f = Frame::Integer(10);
        assert!(!f.is_null());
        assert!(!f.is_string());
        assert!(!f.is_error());
        assert!(!f.is_array());
        assert!(f.is_integer());
        assert!(!f.is_moved_or_ask_error());

        let f = Frame::Moved("foo".into());
        assert!(!f.is_null());
        assert!(!f.is_string());
        assert!(f.is_error());
        assert!(!f.is_array());
        assert!(!f.is_integer());
        assert!(f.is_moved_or_ask_error());

        let f = Frame::Ask("foo".into());
        assert!(!f.is_null());
        assert!(!f.is_string());
        assert!(f.is_error());
        assert!(!f.is_array());
        assert!(!f.is_integer());
        assert!(f.is_moved_or_ask_error());
    }

    #[test]
    fn should_decode_frame_kind_byte() {
        assert_eq!(
            FrameKind::from_byte(SIMPLESTRING_BYTE),
            Some(FrameKind::SimpleString)
        );
        assert_eq!(FrameKind::from_byte(ERROR_BYTE), Some(FrameKind::Error));
        assert_eq!(
            FrameKind::from_byte(BULKSTRING_BYTE),
            Some(FrameKind::BulkString)
        );
        assert_eq!(FrameKind::from_byte(INTEGER_BYTE), Some(FrameKind::Integer));
        assert_eq!(FrameKind::from_byte(ARRAY_BYTE), Some(FrameKind::Array));
    }

    #[test]
    fn should_encode_frame_kind_byte() {
        assert_eq!(FrameKind::SimpleString.to_byte(), SIMPLESTRING_BYTE);
        assert_eq!(FrameKind::Error.to_byte(), ERROR_BYTE);
        assert_eq!(FrameKind::BulkString.to_byte(), BULKSTRING_BYTE);
        assert_eq!(FrameKind::Integer.to_byte(), INTEGER_BYTE);
        assert_eq!(FrameKind::Array.to_byte(), ARRAY_BYTE);
    }

    #[test]
    fn should_cast_from_gen_error() {
        let g = GenError::CustomError(0);
        let e = RedisProtocolError::new_empty();
        assert_eq!(e, RedisProtocolError::from(g));

        let g = GenError::CustomError(1);
        let e = RedisProtocolError::new(RedisProtocolErrorKind::EncodeError, "Invalid frame kind.");
        assert_eq!(e, RedisProtocolError::from(g));

        let g = GenError::BufferTooSmall(10);
        let e = RedisProtocolError::from(g);
        assert_eq!(e.kind(), &RedisProtocolErrorKind::BufferTooSmall(10));

        let g = GenError::InvalidOffset;
        let e = RedisProtocolError::new(RedisProtocolErrorKind::Unknown, "Invalid offset.");
        assert_eq!(e, RedisProtocolError::from(g));
    }

    #[test]
    fn should_print_error_kinds() {
        assert_eq!(RedisProtocolErrorKind::EncodeError.to_str(), "Encode Error");
        assert_eq!(RedisProtocolErrorKind::DecodeError.to_str(), "Decode Error");
        assert_eq!(RedisProtocolErrorKind::Unknown.to_str(), "Unknown Error");
        assert_eq!(
            RedisProtocolErrorKind::BufferTooSmall(10).to_str(),
            "Buffer too small"
        );
    }

}
