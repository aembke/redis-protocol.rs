use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};
use redis_protocol::resp2::{
  decode,
  types::{FrameKind, OwnedFrame, Resp2Frame},
};

#[cfg(feature = "bytes")]
use bytes::{BufMut, Bytes, BytesMut};
#[cfg(feature = "bytes")]
use redis_protocol::resp2::types::BytesFrame;
