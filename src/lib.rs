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

#[cfg(feature = "zero-copy")]
#[cfg_attr(docsrs, doc(cfg(feature = "zero-copy")))]
pub extern crate bytes;
#[cfg(feature = "zero-copy")]
#[cfg_attr(docsrs, doc(cfg(feature = "zero-copy")))]
pub extern crate bytes_utils;
#[cfg(feature = "codec")]
#[cfg_attr(docsrs, doc(cfg(feature = "codec")))]
pub extern crate tokio_util;

#[macro_use]
mod macros;
/// Error types.
pub mod error;
mod utils;

#[cfg(feature = "routing")]
pub use utils::redis_keyslot;

/// A cluster routing interface.
#[cfg(feature = "routing")]
#[cfg_attr(docsrs, doc(cfg(feature = "routing")))]
pub mod routing;

///  A RESP2 interface.
pub mod resp2;
/// A RESP3 interface.
pub mod resp3;
/// Common types across RESP versions.
pub mod types;

pub use utils::{digits_in_number, str_to_f64};
