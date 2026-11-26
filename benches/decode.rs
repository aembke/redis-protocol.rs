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

/// Feed `wire` into a buffer `chunk` bytes at a time, the way a codec reads from a socket, and decode every frame.
/// `ready` reports whether the buffered bytes should be decoded, which is always the case without a scanner.
fn feed(
  wire: &[u8],
  chunk: usize,
  mut ready: impl FnMut(&[u8]) -> bool,
  decode: impl Fn(&[u8]) -> Option<usize>,
) -> usize {
  let mut buf = Vec::with_capacity(wire.len());
  let (mut start, mut frames) = (0, 0);
  for bytes in wire.chunks(chunk) {
    buf.extend_from_slice(bytes);
    while ready(&buf[start ..]) {
      match decode(&buf[start ..]) {
        Some(amt) => {
          start += amt;
          frames += 1;
        },
        None => break,
      }
    }
  }
  frames
}

/// Like [feed], but with a function that decodes the next frame if it's ready.
fn feed_with(wire: &[u8], chunk: usize, mut decode: impl FnMut(&mut &[u8]) -> Option<usize>) -> usize {
  let mut buf = Vec::with_capacity(wire.len());
  let (mut start, mut frames) = (0, 0);
  for bytes in wire.chunks(chunk) {
    buf.extend_from_slice(bytes);
    while let Some(amt) = decode(&mut &buf[start ..]) {
      start += amt;
      frames += 1;
    }
  }
  frames
}

/// Inputs for the chunked benchmarks: one large array reply, and many small replies.
fn chunked_inputs(large: Vec<u8>, small: Vec<u8>) -> Vec<(&'static str, Vec<u8>, usize)> {
  let small = small.repeat(1000);
  let small_len = small.len();
  vec![
    ("large_16kb_reads", large.clone(), 16 * 1024),
    ("large_1400b_reads", large, 1400),
    ("small_replies_buffered", small, small_len),
  ]
}

fn resp2_chunked(c: &mut Criterion) {
  use redis_protocol::{resp2::types::OwnedFrame, types::Scan};
  let mut group = c.benchmark_group("resp2_chunked");
  let large = common::encode_resp2(&OwnedFrame::Array(
    (0 .. 10_000)
      .map(|_| OwnedFrame::BulkString(common::payload(100)))
      .collect(),
  ));
  let small = common::encode_resp2(&OwnedFrame::BulkString(common::payload(10)));
  let decode = |buf: &[u8]| resp2::decode_range(buf).unwrap().map(|(_, amt)| amt);

  for (name, wire, chunk) in chunked_inputs(large, small) {
    group.throughput(Throughput::Bytes(wire.len() as u64));
    group.bench_function(format!("decoder/{name}"), |b| {
      b.iter(|| feed(black_box(&wire), chunk, |buf| !buf.is_empty(), decode))
    });
    group.bench_function(format!("scanner/{name}"), |b| {
      b.iter(|| {
        let mut scanner = resp2::FrameScanner::new();
        feed(
          black_box(&wire),
          chunk,
          |buf| scanner.scan(buf) != Scan::Incomplete,
          decode,
        )
      })
    });
    group.bench_function(format!("decode_with/{name}"), |b| {
      b.iter(|| {
        let mut scanner = resp2::FrameScanner::new();
        feed_with(black_box(&wire), chunk, |buf| {
          scanner
            .decode_with(buf, |buf: &mut &[u8]| resp2::decode_range(buf))
            .unwrap()
            .map(|(_, amt)| amt)
        })
      })
    });
  }

  group.finish();
}

fn resp3_chunked(c: &mut Criterion) {
  use redis_protocol::{
    resp3::{decode::FrameScanner, types::OwnedFrame},
    types::Scan,
  };
  let mut group = c.benchmark_group("resp3_chunked");
  let large = common::encode_resp3(&OwnedFrame::Array {
    data:       (0 .. 10_000)
      .map(|_| OwnedFrame::BlobString {
        data:       common::payload(100),
        attributes: None,
      })
      .collect(),
    attributes: None,
  });
  let small = common::encode_resp3(&OwnedFrame::BlobString {
    data:       common::payload(10),
    attributes: None,
  });
  let decode = |buf: &[u8]| resp3::decode_range(buf).unwrap().map(|(_, amt)| amt);

  for (name, wire, chunk) in chunked_inputs(large, small) {
    group.throughput(Throughput::Bytes(wire.len() as u64));
    group.bench_function(format!("decoder/{name}"), |b| {
      b.iter(|| feed(black_box(&wire), chunk, |buf| !buf.is_empty(), decode))
    });
    group.bench_function(format!("scanner/{name}"), |b| {
      b.iter(|| {
        let mut scanner = FrameScanner::new();
        feed(
          black_box(&wire),
          chunk,
          |buf| scanner.scan(buf) != Scan::Incomplete,
          decode,
        )
      })
    });
    group.bench_function(format!("decode_with/{name}"), |b| {
      b.iter(|| {
        let mut scanner = FrameScanner::new();
        feed_with(black_box(&wire), chunk, |buf| {
          scanner
            .decode_with(buf, |buf: &mut &[u8]| resp3::decode_range(buf))
            .unwrap()
            .map(|(_, amt)| amt)
        })
      })
    });
  }

  group.finish();
}

criterion_group!(benches, resp2_decode, resp3_decode, resp2_chunked, resp3_chunked);
criterion_main!(benches);
