#![no_main]

//! Checks that the frame scanner agrees with the decoder at every step while the input arrives in chunks.

use libfuzzer_sys::fuzz_target;
use redis_protocol::{resp3::decode::FrameScanner, types::Scan};

fuzz_target!(|data: &[u8]| {
  let (chunk, wire) = match data.split_first() {
    Some((chunk, wire)) => (*chunk as usize + 1, wire),
    None => return,
  };
  let mut scanner = FrameScanner::new();
  let mut buf = Vec::new();

  for bytes in wire.chunks(chunk) {
    buf.extend_from_slice(bytes);
    loop {
      let decoded = redis_protocol::resp3::decode::streaming::decode_range(&buf).map(|r| r.map(|(_, amt)| amt));
      match (scanner.scan(&buf), decoded) {
        (Scan::Incomplete, Ok(None)) => break,
        (Scan::Complete(amt), Ok(Some(expected))) if amt == expected => {
          buf.drain(.. amt);
        },
        (Scan::Invalid, Err(_)) => return,
        (scan, decoded) => panic!("scan returned {:?}, decoder returned {:?}", scan, decoded),
      }
    }
  }
});
