//! Redis Protocol
//!
//! Structs and functions for implementing the [Redis protocol](https://redis.io/topics/protocol), built on [nom](https://github.com/Geal/nom) and designed to work easily with [Tokio](https://github.com/tokio-rs/tokio).
//!
//!
//! ## Examples
//!
//! ```rust
//! extern crate redis_protocol;
//! extern crate bytes;
//!
//! use redis_protocol::prelude::*;
//! use bytes::BytesMut;
//!
//! fn main() {
//!   let frame = Frame::BulkString("foobar".into());
//!   let mut buf = BytesMut::new();
//!
//!   let len = match encode_bytes(&mut buf, &frame) {
//!     Ok(l) => l,
//!     Err(e) => panic!("Error encoding frame: {:?}", e)
//!   };
//!   println!("Encoded {} bytes into buffer with contents {:?}", len, buf);
//!
//!   let buf: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();
//!   let (frame, consumed) = match decode_bytes(&buf) {
//!     Ok((f, c)) => (f, c),
//!     Err(e) => panic!("Error parsing bytes: {:?}", e)
//!   };
//!
//!   if let Some(frame) = frame {
//!     println!("Parsed frame {:?} and consumed {} bytes", frame, consumed);
//!   }else{
//!     println!("Incomplete frame, parsed {} bytes", consumed);
//!   }
//!
//!   let key = "foobarbaz";
//!   println!("Hash slot for {}: {}", key, redis_keyslot(key));
//! }
//! ```
//!
//! Or use `decode()` and `encode()` to interact with slices directly.
//!

#[macro_use]
extern crate log;
extern crate pretty_env_logger;

extern crate bytes;
extern crate crc16;

#[macro_use]
extern crate cookie_factory;
#[macro_use]
extern crate nom;

mod utils;

/// Error and Frame types.
pub mod types;
/// Encoding functions for BytesMut and slices.
pub mod encode;
/// Decoding functions for BytesMut and slices.
pub mod decode;

/// Shorthand for `use`'ing `types`, `encode`, `decode`, etc.
pub mod prelude {
  pub use types::*;
  pub use encode::*;
  pub use decode::*;

  pub use utils::redis_keyslot;
}

pub use utils::{
  redis_keyslot,
  ZEROED_KB,
  CRLF,
  NULL
};

#[doc(hidden)]
pub use utils::digits_in_number;