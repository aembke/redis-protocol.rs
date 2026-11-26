#![no_main]

//! Checks that the frame scanner agrees with the decoder at every step while the input arrives in chunks, with and
//! without a maximum frame size.

use libfuzzer_sys::fuzz_target;
use redis_protocol::{resp3::decode::FrameScanner, types::Scan};

fuzz_target!(|data: &[u8]| {
  let (chunk, max, wire) = match data {
    [chunk, max, wire @ ..] => (*chunk as usize + 1, *max as usize, wire),
    _ => return,
  };
  // a limit of 0 means there's no limit
  let max = if max == 0 { None } else { Some(max) };
  let mut scanner = FrameScanner::new();
  scanner.set_max_frame_size(max);
  let mut buf = Vec::new();

  for bytes in wire.chunks(chunk) {
    buf.extend_from_slice(bytes);
    loop {
      let decoded = redis_protocol::resp3::decode::streaming::decode_range(&buf).map(|r| r.map(|(_, amt)| amt));
      let too_large = |amt: usize| max.is_some_and(|max| amt > max);
      match (scanner.scan(&buf), decoded) {
        (Scan::Incomplete, Ok(None)) if !too_large(buf.len()) => break,
        (Scan::Complete(amt), Ok(Some(expected))) if amt == expected && !too_large(amt) => {
          buf.drain(.. amt);
        },
        (Scan::Invalid, Err(_)) => return,
        // a frame is too large once it's complete, or once more than the limit is buffered
        (Scan::TooLarge, Ok(Some(amt))) if too_large(amt) => return,
        (Scan::TooLarge, Ok(None)) if too_large(buf.len()) => return,
        (scan, decoded) => panic!("scan returned {:?}, decoder returned {:?}", scan, decoded),
      }
    }
  }
});
