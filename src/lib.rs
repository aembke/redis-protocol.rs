#![allow(clippy::needless_doctest_main)]
#![allow(clippy::unnecessary_fallible_conversions)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::iter_kv_map)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::vec_init_then_push)]
#![allow(clippy::while_let_on_iterator)]
#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]
#![allow(clippy::needless_as_bytes)]
// these suggest APIs that are newer than the minimum supported Rust version
#![allow(unknown_lints)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::chunks_exact_to_as_chunks)]
#![cfg_attr(docsrs, deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![cfg_attr(all(not(test), not(feature = "std")), no_std)]
// most of the internal utilities are only used by the protocol implementations
#![cfg_attr(
  not(any(feature = "resp2", feature = "resp3")),
  allow(dead_code, unused_macros, unused_imports)
)]
#![doc = include_str!("../README.md")]

extern crate alloc;
extern crate core;

#[cfg(all(
  feature = "resp3",
  not(any(feature = "std", feature = "hashbrown", feature = "index-map"))
))]
compile_error!("The `resp3` feature requires one of the `std`, `hashbrown`, or `index-map` features.");

#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub extern crate bytes;
#[cfg(feature = "bytes")]
#[cfg_attr(docsrs, doc(cfg(feature = "bytes")))]
pub extern crate bytes_utils;
#[cfg(feature = "codec")]
#[cfg_attr(docsrs, doc(cfg(feature = "codec")))]
pub extern crate tokio_util;

#[macro_use]
mod macros;
#[cfg(any(feature = "resp2", feature = "resp3"))]
mod command;
/// Error types.
pub mod error;
#[cfg(any(feature = "resp2", feature = "resp3"))]
mod gen;
mod int2dec;
mod utils;

///  A RESP2 interface.
#[cfg(feature = "resp2")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp2")))]
pub mod resp2;
/// A RESP3 interface.
#[cfg(feature = "resp3")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp3")))]
pub mod resp3;
/// Common types across RESP versions.
pub mod types;

/// Zero-copy RESP2 and RESP3 [codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html) interfaces.
#[cfg(feature = "codec")]
#[cfg_attr(docsrs, doc(cfg(feature = "codec")))]
pub mod codec;

/// Traits for converting between frame types.
#[cfg(feature = "convert")]
#[cfg_attr(docsrs, doc(cfg(feature = "convert")))]
pub mod convert;

#[cfg(feature = "bytes")]
pub use utils::zero_extend;
pub use utils::{digits_in_i64, digits_in_usize, redis_keyslot, str_to_f64};
