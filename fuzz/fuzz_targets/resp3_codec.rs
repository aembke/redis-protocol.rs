#![no_main]

//! Feeds the input to the RESP3 codec in chunks, the way `tokio_util::codec::Framed` does, and checks that it never
//! panics or returns a frame larger than its maximum frame size.

use libfuzzer_sys::fuzz_target;
use redis_protocol::{bytes::BytesMut, codec::Resp3, tokio_util::codec::Decoder};

fuzz_target!(|data: &[u8]| {
  let _ = pretty_env_logger::try_init();
  let (chunk, max, wire) = match data {
    [chunk, max, wire @ ..] => (*chunk as usize + 1, *max as usize, wire),
    _ => return,
  };
  // a limit of 0 means there's no limit
  let mut codec = if max == 0 {
    Resp3::default()
  } else {
    Resp3::default().with_max_frame_size(max)
  };
  let mut buf = BytesMut::new();

  for bytes in wire.chunks(chunk) {
    buf.extend_from_slice(bytes);
    loop {
      let buffered = buf.len();
      match codec.decode(&mut buf) {
        Ok(Some(_)) => {
          // a frame larger than the limit is an error
          assert!(
            max == 0 || buffered - buf.len() <= max,
            "decoded a frame larger than the limit"
          );
          continue;
        },
        Ok(None) => break,
        // `Framed` stops reading after an error
        Err(_) => return,
      }
    }
  }
  // the end of the stream
  while let Ok(Some(_)) = codec.decode_eof(&mut buf) {}
});
