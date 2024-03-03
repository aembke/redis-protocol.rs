#![cfg_attr(docsrs, deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![cfg_attr(all(not(test), not(feature = "std")), no_std)]

//! # Redis Protocol
//!
//! Structs and functions for implementing the [RESP2](https://redis.io/topics/protocol) and [RESP3](https://github.com/antirez/RESP3/blob/master/spec.md) protocol.
//!
//! ## Examples
//!
//! ```rust
//! # extern crate redis_protocol;
//! # extern crate bytes;
//!
//! use redis_protocol::resp2::prelude::*;
//! use bytes::{Bytes, BytesMut};
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
//!   let buf: Bytes = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();
//!   let (frame, consumed) = match decode(&buf) {
//!     Ok(Some((f, c))) => (f, c),
//!     Ok(None) => panic!("Incomplete frame."),
//!     Err(e) => panic!("Error parsing bytes: {:?}", e)
//!   };
//!   println!("Parsed frame {:?} and consumed {} bytes", frame, consumed);
//!
//!   let key = "foobarbaz";
//!   println!("Hash slot for {}: {}", key, redis_keyslot(key.as_bytes()));
//! }
//! ```
//!
//! Note: if callers are not using the `index-map` feature then substitute `std::collections::HashMap` for any `IndexMap` types in these docs. `rustdoc` doesn't have a great way to show type substitutions based on feature flags.

extern crate alloc;
extern crate core;

#[macro_use]
extern crate log;
#[macro_use]
extern crate cookie_factory;

#[cfg(test)]
extern crate pretty_env_logger;

#[cfg(feature = "index-map")]
extern crate indexmap;

#[macro_use]
pub(crate) mod utils;
pub(crate) mod nom_bytes;

#[cfg(feature = "decode-mut")]
mod decode_mut;
/// Types and functions for implementing the RESP2 protocol.
pub mod resp2;
/// Types and functions for implementing the RESP3 protocol.
pub mod resp3;
/// Error types and general redis protocol types.
pub mod types;

pub use utils::{digits_in_number, redis_keyslot, resp2_frame_to_resp3, ZEROED_KB};
