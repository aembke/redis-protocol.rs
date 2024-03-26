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
#![cfg_attr(docsrs, deny(rustdoc::broken_intra_doc_links))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]
#![cfg_attr(all(not(test), not(feature = "std")), no_std)]
#![doc = include_str!("../README.md")]

extern crate alloc;
extern crate core;

#[macro_use]
extern crate log;
#[macro_use]
extern crate cookie_factory;

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
/// Error types.
pub mod error;
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
pub use utils::{digits_in_number, redis_keyslot, str_to_f64};
