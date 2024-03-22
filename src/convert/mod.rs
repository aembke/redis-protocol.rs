#[cfg(feature = "resp2")]
mod resp2;
#[cfg(feature = "resp3")]
mod resp3;

#[cfg(feature = "resp2")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp2")))]
pub use resp2::*;
#[cfg(feature = "resp3")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp3")))]
pub use resp3::*;
