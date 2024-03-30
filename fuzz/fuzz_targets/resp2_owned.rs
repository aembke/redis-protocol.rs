#![no_main]

use libfuzzer_sys::fuzz_target;
extern crate redis_protocol;

fuzz_target!(|data: &[u8]| {
  pretty_env_logger::try_init();
  let _ = redis_protocol::resp2::decode::decode(data);
});
