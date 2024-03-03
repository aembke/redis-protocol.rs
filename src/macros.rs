macro_rules! unwrap_return(
  ($expr:expr) => {
    match $expr {
      Some(val) => val,
      None => return None,
    }
  };
);

macro_rules! encode_checks(
  ($x:ident, $required:expr) => {
    let _ = crate::utils::check_offset(&$x)?;
    let required = $required;
    let remaining = $x.0.len() - $x.1;

    if remaining < required {
      return Err(cookie_factory::GenError::BufferTooSmall(required - remaining));
    }
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

#[cfg(feature = "decode-logs")]
macro_rules! decode_log(
  ($buf:ident, $($arg:tt)*) => (
    if log_enabled!(log::Level::Trace) {
      if let Some(s) = std::str::from_utf8(&$buf).ok() {
        let $buf = s;
        trace!($($arg)*)
      }else{
        trace!($($arg)*)
      }
    }
  );
  ($buf:expr, $name:ident, $($arg:tt)*) => (
    if log_enabled!(log::Level::Trace) {
      if let Some(s) = std::str::from_utf8(&$buf).ok() {
        let $name = s;
        trace!($($arg)*)
      }else{
        trace!($($arg)*)
      }
    }
  );
  ($($arg:tt)*) => (
    if log_enabled!(log::Level::Trace) {
      trace!($($arg)*)
    }
  );
);

#[cfg(not(feature = "decode-logs"))]
macro_rules! decode_log(
  ($buf:ident, $($arg:tt)*) => ();
  ($($arg:tt)*) => ();
);
