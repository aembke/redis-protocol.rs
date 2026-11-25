#![no_main]

//! Checks that every frame the decoder accepts can be encoded and decoded again without changing.

use libfuzzer_sys::fuzz_target;
use redis_protocol::{
  bytes::{Bytes, BytesMut},
  resp2::{
    decode::decode_bytes,
    encode::{encode_bytes, extend_encode},
    types::Resp2Frame,
  },
};

fuzz_target!(|data: &[u8]| {
  let _ = pretty_env_logger::try_init();
  let buf = Bytes::copy_from_slice(data);
  let frame = match decode_bytes(&buf) {
    Ok(Some((frame, _))) => frame,
    _ => return,
  };

  for int_as_bulkstring in [false, true] {
    let encode_len = frame.encode_len(int_as_bulkstring);
    let mut extended = BytesMut::new();
    let amt = extend_encode(&mut extended, &frame, int_as_bulkstring).expect("failed to encode decoded frame");
    assert_eq!(amt, encode_len, "extend_encode length doesn't match encode_len");
    assert_eq!(
      extended.len(),
      encode_len,
      "extend_encode buffer length doesn't match encode_len"
    );

    let mut sliced = vec![0; encode_len];
    let amt = encode_bytes(&mut sliced, &frame, int_as_bulkstring).expect("failed to encode into slice");
    assert_eq!(amt, encode_len, "encode_bytes length doesn't match encode_len");
    assert_eq!(&sliced[..], &extended[..], "slice and BytesMut encodings differ");

    // integers are encoded as bulk strings with `int_as_bulkstring`, so only the default encoding round-trips
    if !int_as_bulkstring {
      let (decoded, amt) = decode_bytes(&extended.freeze())
        .expect("failed to decode encoded frame")
        .expect("encoded frame is incomplete");
      assert_eq!(amt, encode_len, "decoded length doesn't match encoded length");
      assert_eq!(decoded, frame, "frame changed after round-trip");
    }
  }
});
