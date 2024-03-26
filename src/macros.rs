macro_rules! encode_checks(
  ($buf:ident, $offset:expr, $required:expr) => {
    let required = $required;
    let remaining = $buf.len() - $offset;

    if required > remaining {
      return Err(RedisProtocolError::from(cookie_factory::GenError::BufferTooSmall(required - remaining)));
    }
  }
);

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
      if log_enabled!(log::Level::Trace) {
        trace!($($arg)*)
      }
    }
  );
);

macro_rules! decode_log_str(
  ($buf:expr, $name:ident, $($arg:tt)*) => (
    #[cfg(feature = "decode-logs")]
    {
      if log_enabled!(log::Level::Trace) {
        if let Some(s) = core::str::from_utf8(&$buf).ok() {
          let $name = s;
          trace!($($arg)*)
        }else{
          let $name = $buf;
          trace!($($arg)*)
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
