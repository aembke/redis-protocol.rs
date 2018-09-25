#![feature(test)]

extern crate test;
extern crate rand;
extern crate redis_protocol;
extern crate bytes;

#[macro_use]
extern crate lazy_static;

use rand::Rng;

use redis_protocol::prelude::*;
use bytes::BytesMut;
use bytes::BufMut;

pub fn rand_chars(len: usize) -> String {
  rand::thread_rng()
    .gen_ascii_chars()
    .take(len)
    .collect()
}

fn bulkstring_bytes(len: usize) -> BytesMut {
  let digits = redis_protocol::digits_in_number(len);
  let mut v = BytesMut::with_capacity(1 + digits + 2 + len + 2);
  let s = rand_chars(len);

  v.put_u8(b'$');
  v.extend_from_slice(len.to_string().as_bytes());
  v.extend_from_slice(redis_protocol::CRLF.as_bytes());
  v.extend_from_slice(s.as_bytes());
  v.extend_from_slice(redis_protocol::CRLF.as_bytes());
  v
}

#[cfg(test)]
mod tests {
  use super::*;
  use test::{Bencher, black_box};

  #[bench]
  fn bench_decode_1kb_bulkstring(b: &mut Bencher) {
    let buf = bulkstring_bytes(1024);

    b.iter(|| {
      black_box(decode_bytes(&buf));
    });
  }

  #[bench]
  fn bench_decode_10kb_bulkstring(b: &mut Bencher) {
    let buf = bulkstring_bytes(10 * 1024);

    b.iter(|| {
      black_box(decode_bytes(&buf));
    });
  }

  #[bench]
  fn bench_decode_100kb_bulkstring(b: &mut Bencher) {
    let buf = bulkstring_bytes(100 * 1024);

    b.iter(|| {
      black_box(decode_bytes(&buf));
    });
  }

  #[bench]
  fn bench_decode_1mb_bulkstring(b: &mut Bencher) {
    let buf = bulkstring_bytes(1024 * 1024);

    b.iter(|| {
      black_box(decode_bytes(&buf));
    });
  }

  #[bench]
  fn bench_decode_10mb_bulkstring(b: &mut Bencher) {
    let buf = bulkstring_bytes(10 * 1024 * 1024);

    b.iter(|| {
      black_box(decode_bytes(&buf));
    });
  }

}