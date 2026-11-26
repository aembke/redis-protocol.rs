//! Compares the ways `extend_encode` could reserve space in the output buffer.
//!
//! The alternatives are only compiled with a cfg flag, so they stay out of the public interface:
//!
//! ```text
//! RUSTFLAGS="--cfg redis_protocol_bench" cargo bench --features bytes --bench reserve
//! ```
//!
//! ## Strategies
//!
//! * `exact`: reserve `encode_len()` bytes, then encode. This was the behavior before 7.0. Computing the length walks
//!   the whole frame once before encoding walks it again, counting the digits of every length prefix and formatting
//!   every double.
//! * `estimate`: reserve a cheap over-estimate (20 digits for every number, 32 bytes for every double), then encode.
//!   This still walks the frame twice.
//! * `grow`: encode without reserving, letting `BytesMut` grow as it's written. This is what `extend_encode` does.
//!
//! ## Scenarios
//!
//! * `single/*/warm`: one frame into a reused buffer that already has room for it.
//! * `single/*/cold`: one frame into a new 8 KiB buffer, the size tokio's `Framed` starts with.
//! * `workload/*/steady`: 99 small commands and 1 large command, cleared at `Framed`'s 8 KiB flush boundary, into a
//!   buffer that kept its capacity from earlier runs. This is a long-lived connection.
//! * `workload/*/fresh`: the same, starting from a new 8 KiB buffer. This is the first time a connection sees a frame
//!   that large.
//!
//! ## Results
//!
//! Measured in November 2026 for 7.0, on an AMD Ryzen 9 7950X with rustc 1.100 and glibc 2.35. Changes are relative
//! to `exact`, and negative is faster.
//!
//! | Scenario                                             | `exact`  | `estimate`     | `grow`         |
//! |------------------------------------------------------|----------|----------------|----------------|
//! | small commands (GET, SET 64B, HSET 5 fields), warm   | 37-159ns | -1% to -7%     | -20% to -27%   |
//! | small commands, cold                                 | 42-188ns | -4% to -6%     | -12% to -23%   |
//! | MSET 1000 keys, 100 and 1000 element arrays, maps    | 1.2-26µs | -6% to +18%    | -15% to -24% * |
//! | arrays with doubles (`array/100_mixed`)              | 2.5µs    | -27% to -29%   | -38% to -39%   |
//! | 64 KiB and 1 MiB blobs, warm                         | 0.9-186µs| -1% to +2%     | -1% to +4%     |
//! | 64 KiB and 1 MiB blobs, cold                         | 8-335µs  | -15% to +34% * | -3% to +12% *  |
//! | 99 small + SET 64 KiB or MSET 1000 keys, steady      | 8-35µs   | -4% to -6%     | -20% to -24%   |
//! | 99 small + SET 64 KiB or MSET 1000 keys, fresh       | 11-33µs  | -6% to +5%     | -3% to -21%    |
//! | 99 small + SET 1 MiB, steady and fresh               | 41-265µs | -1% to -4%     | -4% to +0%     |
//!
//! \* One run of cold `array/1000x32B` measured `grow` at +1%, where the previous run measured -18%. Cold 64 KiB
//! blobs measured `estimate` at +34% and `grow` at +12%, but `grow` was -4% to -1% and `estimate` -1% to +1% when
//! re-run. See the caveats below.
//!
//! ## Conclusions
//!
//! * `grow` is 12-39% faster for small frames and for frames with many elements, which is most of what a client
//!   encodes. Walking the frame to compute its length costs about as much as encoding it, and frames with doubles
//!   cost the most because each double is formatted twice.
//! * For large blobs, copying the payload dominates and the three strategies are within a few percent of each other.
//! * `estimate` keeps most of the cost of `exact`, because walking the frame is the expensive part rather than
//!   counting digits. It only helps frames with doubles.
//! * `grow` needs the blob encoders to reserve each blob, including its trailing CRLF, before writing it. Without
//!   that, a large payload grows the buffer to exactly its size and the CRLF grows it again, copying the payload
//!   twice. That made `grow` 74-142% slower than `exact` for 64 KiB and 1 MiB blobs in a cold buffer.
//!
//! ## Caveats
//!
//! * Large allocations depend on the allocator's history. glibc serves large allocations with `mmap` until earlier
//!   frees raise its threshold, and growing an `mmap` allocation is a cheap `mremap` rather than a copy. Run alone,
//!   each large cold case took 250-270µs for every strategy, which hid the double copy above. In the run that showed
//!   it, the large cold cases ran after the others, as in a long-lived process, and `exact` took 80-110µs. In the run
//!   for the table above, the same cases took 210-335µs. Compare strategies within the same run, and treat the
//!   absolute times for large cold frames as approximate.
//! * The `cold` cases include allocating a new 8 KiB buffer, and that cost varies between runs. Frames that fit in it
//!   never grow it, so for those rows `cold` measures the same thing as `warm` with more noise.
//! * The workloads clear the buffer at the flush boundary, where `FramedWrite` would write it to the socket and
//!   advance it. A partially written buffer that still holds data can make growing it cost more than shown here.

#[cfg(redis_protocol_bench)]
mod common;

#[cfg(not(redis_protocol_bench))]
fn main() {
  eprintln!("This benchmark needs RUSTFLAGS=\"--cfg redis_protocol_bench\". See benches/reserve.rs.");
}

#[cfg(redis_protocol_bench)]
fn main() {
  bench::main();
}

#[cfg(redis_protocol_bench)]
mod bench {
  use super::common;
  use bytes::{Bytes, BytesMut};
  use criterion::{criterion_group, BatchSize, Criterion};
  use redis_protocol::{
    error::RedisProtocolError,
    resp2::{
      encode::{extend_encode as r2_grow, reserve_strategies as r2},
      types::BytesFrame as F2,
    },
    resp3::{
      encode::{complete::extend_encode as r3_grow, reserve_strategies as r3},
      types::{BytesFrame as F3, Resp3Frame as _},
    },
  };
  use std::hint::black_box;

  type EncodeResult = Result<usize, RedisProtocolError>;
  type Enc3 = fn(&mut BytesMut, &F3, bool) -> EncodeResult;
  type Enc2 = fn(&mut BytesMut, &F2, bool) -> EncodeResult;

  const V3: [(&str, Enc3); 3] = [
    ("exact", r3::extend_encode_exact),
    ("estimate", r3::extend_encode_estimate),
    ("grow", r3_grow),
  ];
  const V2: [(&str, Enc2); 3] = [
    ("exact", r2::extend_encode_exact),
    ("estimate", r2::extend_encode_estimate),
    ("grow", r2_grow),
  ];

  /// Tokio's Framed starts with, and flushes at, 8 KiB.
  const FRAMED_CAPACITY: usize = 8 * 1024;

  fn payload(len: usize) -> Bytes {
    common::payload(len).into()
  }

  fn cmd3(args: Vec<Bytes>) -> F3 {
    F3::Array {
      data:       args
        .into_iter()
        .map(|data| F3::BlobString { data, attributes: None })
        .collect(),
      attributes: None,
    }
  }

  fn cmd2(args: Vec<Bytes>) -> F2 {
    F2::Array(args.into_iter().map(F2::BulkString).collect())
  }

  /// Command-shaped frames, which is what a client encodes.
  fn commands() -> Vec<(&'static str, Vec<Bytes>)> {
    let key = |i: usize| Bytes::from(format!("key:{i:012}"));
    let mut mset = vec![Bytes::from_static(b"MSET")];
    for i in 0 .. 1000 {
      mset.push(key(i));
      mset.push(payload(32));
    }
    vec![
      ("get", vec![Bytes::from_static(b"GET"), key(1)]),
      ("set_64B", vec![Bytes::from_static(b"SET"), key(1), payload(64)]),
      ("hset_5x16B", {
        let mut v = vec![Bytes::from_static(b"HSET"), key(1)];
        for i in 0 .. 5 {
          v.push(Bytes::from(format!("f{i}")));
          v.push(payload(16));
        }
        v
      }),
      ("set_64KiB", vec![
        Bytes::from_static(b"SET"),
        key(1),
        payload(64 * 1024),
      ]),
      ("set_1MiB", vec![
        Bytes::from_static(b"SET"),
        key(1),
        payload(1024 * 1024),
      ]),
      ("mset_1000x32B", mset),
    ]
  }

  /// 99 small commands then one large command.
  fn workload(large: &[Bytes]) -> Vec<Vec<Bytes>> {
    let small = commands();
    let mut out = Vec::with_capacity(100);
    for i in 0 .. 99 {
      out.push(small[i % 3].1.clone());
    }
    out.push(large.to_vec());
    out
  }

  /// Encode each frame, clearing the buffer once it reaches Framed's flush boundary, like FramedWrite does.
  fn run_stream<T>(buf: &mut BytesMut, frames: &[T], enc: fn(&mut BytesMut, &T, bool) -> EncodeResult) {
    for frame in frames {
      enc(buf, black_box(frame), false).unwrap();
      if buf.len() >= FRAMED_CAPACITY {
        black_box(&buf[..]);
        buf.clear();
      }
    }
    buf.clear();
  }

  fn single_frames(c: &mut Criterion) {
    let mut group = c.benchmark_group("single");
    let mut frames3: Vec<(String, F3)> = commands()
      .into_iter()
      .map(|(n, a)| (format!("cmd/{n}"), cmd3(a)))
      .collect();
    for (n, f) in common::resp3_frames() {
      frames3.push((n.to_string(), f.into_bytes_frame()));
    }
    for (name, frame) in &frames3 {
      let len = frame.encode_len(false);
      for (vname, enc) in V3 {
        let mut warm = BytesMut::with_capacity(len);
        group.bench_function(format!("resp3/warm/{vname}/{name}"), |b| {
          b.iter(|| {
            warm.clear();
            enc(&mut warm, black_box(frame), false).unwrap()
          })
        });
        group.bench_function(format!("resp3/cold/{vname}/{name}"), |b| {
          b.iter_batched_ref(
            || BytesMut::with_capacity(FRAMED_CAPACITY),
            |buf| enc(buf, black_box(frame), false).unwrap(),
            BatchSize::SmallInput,
          )
        });
      }
    }
    group.finish();
  }

  fn workloads(c: &mut Criterion) {
    let mut group = c.benchmark_group("workload");
    for (large_name, large) in commands().into_iter().skip(3) {
      let frames = workload(&large);
      let f3: Vec<F3> = frames.iter().cloned().map(cmd3).collect();
      let f2: Vec<F2> = frames.iter().cloned().map(cmd2).collect();

      for (vname, enc) in V3 {
        // a long-lived connection: the buffer kept its capacity from earlier batches
        let mut steady = BytesMut::with_capacity(FRAMED_CAPACITY);
        run_stream(&mut steady, &f3, enc);
        group.bench_function(format!("resp3/steady/{vname}/99_small+{large_name}"), |b| {
          b.iter(|| run_stream(&mut steady, &f3, enc))
        });
        // the first time this connection sees a frame this large
        group.bench_function(format!("resp3/fresh/{vname}/99_small+{large_name}"), |b| {
          b.iter_batched_ref(
            || BytesMut::with_capacity(FRAMED_CAPACITY),
            |buf| run_stream(buf, &f3, enc),
            BatchSize::SmallInput,
          )
        });
      }
      for (vname, enc) in V2 {
        let mut steady = BytesMut::with_capacity(FRAMED_CAPACITY);
        run_stream(&mut steady, &f2, enc);
        group.bench_function(format!("resp2/steady/{vname}/99_small+{large_name}"), |b| {
          b.iter(|| run_stream(&mut steady, &f2, enc))
        });
        group.bench_function(format!("resp2/fresh/{vname}/99_small+{large_name}"), |b| {
          b.iter_batched_ref(
            || BytesMut::with_capacity(FRAMED_CAPACITY),
            |buf| run_stream(buf, &f2, enc),
            BatchSize::SmallInput,
          )
        });
      }
    }
    group.finish();
  }

  criterion_group!(benches, single_frames, workloads);

  pub fn main() {
    benches();
    Criterion::default().configure_from_args().final_summary();
  }
}
