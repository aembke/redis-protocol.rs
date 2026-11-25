#![no_main]

//! Checks that every frame the decoder accepts can be encoded and decoded again without changing.

use libfuzzer_sys::fuzz_target;
use redis_protocol::{
  bytes::{Bytes, BytesMut},
  resp3::{
    decode::complete::decode_bytes,
    encode::complete::{encode_bytes, extend_encode},
    types::{BytesFrame, Resp3Frame},
  },
};

/// Whether the frame, or anything nested inside it, can't be expected to round-trip exactly.
fn skip(frame: &BytesFrame) -> bool {
  let attributes = frame
    .attributes()
    .map(|attrs| attrs.iter().any(|(k, v)| skip(k) || skip(v)))
    .unwrap_or(false);

  attributes
    || match frame {
      // `nan != nan`, so a frame containing a NaN never equals itself
      BytesFrame::Double { data, .. } => data.is_nan(),
      // HELLO isn't length-prefixed, so the encoded form isn't always parsed back the same way (e.g. arguments with
      // spaces). it's also only sent by clients.
      BytesFrame::Hello { .. } => true,
      // chunks only exist inside a streamed string, which the complete decoder doesn't return
      BytesFrame::ChunkedString(_) => true,
      BytesFrame::Array { data, .. } | BytesFrame::Push { data, .. } => data.iter().any(skip),
      BytesFrame::Set { data, .. } => data.iter().any(skip),
      BytesFrame::Map { data, .. } => data.iter().any(|(k, v)| skip(k) || skip(v)),
      _ => false,
    }
}

fuzz_target!(|data: &[u8]| {
  let _ = pretty_env_logger::try_init();
  let buf = Bytes::copy_from_slice(data);
  let frame = match decode_bytes(&buf) {
    Ok(Some((frame, _))) => frame,
    _ => return,
  };
  if skip(&frame) {
    return;
  }

  for int_as_blobstring in [false, true] {
    let encode_len = frame.encode_len(int_as_blobstring);
    let mut extended = BytesMut::new();
    let amt = extend_encode(&mut extended, &frame, int_as_blobstring).expect("failed to encode decoded frame");
    assert_eq!(amt, encode_len, "extend_encode length doesn't match encode_len");
    assert_eq!(
      extended.len(),
      encode_len,
      "extend_encode buffer length doesn't match encode_len"
    );

    // the slice and `BytesMut` interfaces should write the same bytes. this has to use the same frame since map and
    // set iteration order can differ between two otherwise equal frames.
    let mut sliced = vec![0; encode_len];
    let amt = encode_bytes(&mut sliced, &frame, int_as_blobstring).expect("failed to encode into slice");
    assert_eq!(amt, encode_len, "encode_bytes length doesn't match encode_len");
    assert_eq!(&sliced[..], &extended[..], "slice and BytesMut encodings differ");

    if !int_as_blobstring {
      let (decoded, amt) = decode_bytes(&extended.freeze())
        .expect("failed to decode encoded frame")
        .expect("encoded frame is incomplete");
      assert_eq!(amt, encode_len, "decoded length doesn't match encoded length");
      assert_eq!(decoded, frame, "frame changed after round-trip");
    }
  }
});
