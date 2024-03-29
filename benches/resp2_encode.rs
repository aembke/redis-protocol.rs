use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};
use redis_protocol::resp2::{
  decode,
  types::{BytesFrame, FrameKind, OwnedFrame, Resp2Frame},
};

fn rand_chars(len: usize) -> String {
  rand::thread_rng()
    .sample_iter(&Alphanumeric)
    .take(len)
    .map(char::from)
    .collect()
}

fn rand_bytes_array(len: usize, null_every: usize, str_len: usize) -> Vec<BytesFrame> {
  (0 .. len)
    .map(|i| {
      if i + 1 % null_every == 0 {
        // curious about the impact of forcing a branch sometimes
        BytesFrame::Null
      } else {
        BytesFrame::BulkString(rand_chars(str_len).into())
      }
    })
    .collect()
}

fn rand_owned_array(len: usize, null_every: usize, str_len: usize) -> Vec<OwnedFrame> {
  (0 .. len)
    .map(|i| {
      if i + 1 % null_every == 0 {
        OwnedFrame::Null
      } else {
        OwnedFrame::BulkString(rand_chars(str_len).into_bytes())
      }
    })
    .collect()
}
