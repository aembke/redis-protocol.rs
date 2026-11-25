#![no_main]

//! Feeds the input to the RESP3 codec in chunks, the way `tokio_util::codec::Framed` does, and checks that it never
//! panics.

use libfuzzer_sys::fuzz_target;
use redis_protocol::{bytes::BytesMut, codec::Resp3, tokio_util::codec::Decoder};

fuzz_target!(|data: &[u8]| {
  let _ = pretty_env_logger::try_init();
  let (chunk, wire) = match data.split_first() {
    Some((chunk, wire)) => (*chunk as usize + 1, wire),
    None => return,
  };
  let mut codec = Resp3::default();
  let mut buf = BytesMut::new();

  for bytes in wire.chunks(chunk) {
    buf.extend_from_slice(bytes);
    loop {
      match codec.decode(&mut buf) {
        Ok(Some(_)) => continue,
        Ok(None) => break,
        // `Framed` stops reading after an error
        Err(_) => return,
      }
    }
  }
  // the end of the stream
  while let Ok(Some(_)) = codec.decode_eof(&mut buf) {}
});
