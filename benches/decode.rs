use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use redis_protocol::{resp2::decode as resp2, resp3::decode::complete as resp3};
use std::hint::black_box;

mod common;

#[cfg(feature = "bytes")]
use bytes::{Bytes, BytesMut};

fn resp2_decode(c: &mut Criterion) {
  let mut group = c.benchmark_group("resp2_decode");

  for (name, frame) in common::resp2_frames() {
    let buf = common::encode_resp2(&frame);
    group.throughput(Throughput::Bytes(buf.len() as u64));

    group.bench_function(format!("range/{name}"), |b| {
      b.iter(|| resp2::decode_range(black_box(&buf)).unwrap())
    });
    group.bench_function(format!("owned/{name}"), |b| {
      b.iter(|| resp2::decode(black_box(&buf)).unwrap())
    });
    #[cfg(feature = "bytes")]
    {
      let bytes = Bytes::from(buf.clone());
      group.bench_function(format!("bytes/{name}"), |b| {
        b.iter(|| resp2::decode_bytes(black_box(&bytes)).unwrap())
      });
      group.bench_function(format!("bytes_mut/{name}"), |b| {
        b.iter_batched(
          || BytesMut::from(&buf[..]),
          |mut buf| resp2::decode_bytes_mut(&mut buf).unwrap(),
          BatchSize::SmallInput,
        )
      });
    }
  }

  group.finish();
}

fn resp3_decode(c: &mut Criterion) {
  let mut group = c.benchmark_group("resp3_decode");

  for (name, frame) in common::resp3_frames() {
    let buf = common::encode_resp3(&frame);
    group.throughput(Throughput::Bytes(buf.len() as u64));

    group.bench_function(format!("range/{name}"), |b| {
      b.iter(|| resp3::decode_range(black_box(&buf)).unwrap())
    });
    group.bench_function(format!("owned/{name}"), |b| {
      b.iter(|| resp3::decode(black_box(&buf)).unwrap())
    });
    #[cfg(feature = "bytes")]
    {
      let bytes = Bytes::from(buf.clone());
      group.bench_function(format!("bytes/{name}"), |b| {
        b.iter(|| resp3::decode_bytes(black_box(&bytes)).unwrap())
      });
      group.bench_function(format!("bytes_mut/{name}"), |b| {
        b.iter_batched(
          || BytesMut::from(&buf[..]),
          |mut buf| resp3::decode_bytes_mut(&mut buf).unwrap(),
          BatchSize::SmallInput,
        )
      });
    }
  }

  group.finish();
}

criterion_group!(benches, resp2_decode, resp3_decode);
criterion_main!(benches);
