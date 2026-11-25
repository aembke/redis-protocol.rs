macro_rules! encode_checks(
  ($buf:ident, $offset:expr, $required:expr) => {
    let required = $required;
    // an offset past the end of the buffer leaves no room
    let remaining = $buf.len().saturating_sub($offset);

    if required > remaining {
      return Err(RedisProtocolError::from(crate::gen::GenError::BufferTooSmall(required - remaining)));
    }
  }
);

#[cfg(feature = "convert")]
macro_rules! debug_type(
  ($($arg:tt)*) => {
    #[cfg(all(feature="decode-logs", feature = "std"))]
    log::trace!($($arg)*);
  }
);

macro_rules! e (
  ($err:expr) => {
    return Err(RedisParseError::from($err).into_nom_error())
  }
);

macro_rules! etry (
  ($expr:expr) => {
    match $expr {
      Ok(result) => result,
      Err(e) => return Err(RedisParseError::from(e).into_nom_error())
    }
  }
);

macro_rules! decode_log(
  ($($arg:tt)*) => (
    #[cfg(feature = "decode-logs")]
    {
      if log::log_enabled!(log::Level::Trace) {
        log::trace!($($arg)*)
      }
    }
  );
);

macro_rules! decode_log_str(
  ($buf:expr, $name:ident, $($arg:tt)*) => (
    #[cfg(feature = "decode-logs")]
    {
      if log::log_enabled!(log::Level::Trace) {
        if let Ok(s) = core::str::from_utf8(&$buf) {
          let $name = s;
          log::trace!($($arg)*)
        }else{
          let $name = $buf;
          log::trace!($($arg)*)
        }
      }
    }
  )
);

#[cfg(feature = "convert")]
macro_rules! check_single_vec_reply(
  ($v:expr) => {
    if $v.is_single_element_vec() {
      return Self::from_frame($v.pop_or_take());
    }
  };
  ($t:ty, $v:expr) => {
    if $v.is_single_element_vec() {
      return $t::from_frame($v.pop_or_take());
    }
  }
);

/// Write a single byte. Equivalent to the legacy `cookie-factory` macro with the same name.
#[cfg(any(feature = "resp2", feature = "resp3"))]
macro_rules! gen_be_u8(
  ($i:expr, $val:expr) => (
    crate::gen::gen_slice($i, &[$val])
  );
);

/// Write a byte slice. Equivalent to the legacy `cookie-factory` macro with the same name.
#[cfg(any(feature = "resp2", feature = "resp3"))]
macro_rules! gen_slice(
  ($i:expr, $val:expr) => (
    crate::gen::gen_slice($i, $val)
  );
);

/// Apply a sequence of generators separated by `>>`, short-circuiting on the first error. Equivalent to the subset of
/// the legacy `cookie-factory` macro with the same name used by this crate.
#[cfg(any(feature = "resp2", feature = "resp3"))]
macro_rules! do_gen(
  (__impl $i:expr, $submac:ident!( $($args:tt)* )) => (
    $submac!($i, $($args)*)
  );
  (__impl $i:expr, $submac:ident!( $($args:tt)* ) >> $($rest:tt)*) => (
    match $submac!($i, $($args)*) {
      Ok(next) => do_gen!(__impl next, $($rest)*),
      Err(e) => Err(e),
    }
  );
  ($i:expr, $($rest:tt)*) => (
    do_gen!(__impl $i, $($rest)*)
  );
);
