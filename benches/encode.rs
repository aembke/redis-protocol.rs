use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use redis_protocol::{
  resp2::{encode as resp2, types::Resp2Frame as _},
  resp3::{encode::complete as resp3, types::Resp3Frame as _},
};
use std::hint::black_box;

mod common;

#[cfg(feature = "bytes")]
use bytes::BytesMut;

fn resp2_encode(c: &mut Criterion) {
  let mut group = c.benchmark_group("resp2_encode");

  for (name, frame) in common::resp2_frames() {
    let len = frame.encode_len(false);
    group.throughput(Throughput::Bytes(len as u64));

    let mut buf = vec![0; len];
    group.bench_function(format!("owned/{name}"), |b| {
      b.iter(|| resp2::encode(black_box(&mut buf), black_box(&frame), false).unwrap())
    });
    #[cfg(feature = "bytes")]
    {
      let frame = frame.clone().into_bytes_frame();
      let mut buf = BytesMut::with_capacity(len);
      group.bench_function(format!("bytes/{name}"), |b| {
        b.iter(|| {
          buf.clear();
          resp2::extend_encode(black_box(&mut buf), black_box(&frame), false).unwrap()
        })
      });
    }
  }

  group.finish();
}

fn resp3_encode(c: &mut Criterion) {
  let mut group = c.benchmark_group("resp3_encode");

  for (name, frame) in common::resp3_frames() {
    let len = frame.encode_len(false);
    group.throughput(Throughput::Bytes(len as u64));

    let mut buf = vec![0; len];
    group.bench_function(format!("owned/{name}"), |b| {
      b.iter(|| resp3::encode(black_box(&mut buf), black_box(&frame), false).unwrap())
    });
    #[cfg(feature = "bytes")]
    {
      let frame = frame.clone().into_bytes_frame();
      let mut buf = BytesMut::with_capacity(len);
      group.bench_function(format!("bytes/{name}"), |b| {
        b.iter(|| {
          buf.clear();
          resp3::extend_encode(black_box(&mut buf), black_box(&frame), false).unwrap()
        })
      });
    }
  }

  group.finish();
}

criterion_group!(benches, resp2_encode, resp3_encode);
criterion_main!(benches);
