#![no_main]

use libfuzzer_sys::fuzz_target;
extern crate redis_protocol;
use redis_protocol::bytes::Bytes;

fuzz_target!(|data: &[u8]| {
  pretty_env_logger::try_init();
  let buf: Bytes = data.to_vec().into();
  let _ = redis_protocol::resp3::decode::complete::decode_bytes(&buf);
});
