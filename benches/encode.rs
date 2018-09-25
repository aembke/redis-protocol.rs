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

pub fn rand_chars(len: usize) -> String {
  rand::thread_rng()
    .gen_ascii_chars()
    .take(len)
    .collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use test::{Bencher, black_box};

  #[bench]
  fn bench_encode_1kb_bulkstring(b: &mut Bencher) {
    let f = Frame::BulkString(rand_chars(1024).into_bytes());

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    });
  }

  #[bench]
  fn bench_encode_10kb_bulkstring(b: &mut Bencher) {
    let f = Frame::BulkString(rand_chars(10 * 1024).into_bytes());

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    });
  }

  #[bench]
  fn bench_encode_100kb_bulkstring(b: &mut Bencher) {
    let f = Frame::BulkString(rand_chars(100 * 1024).into_bytes());

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    });
  }

  #[bench]
  fn bench_encode_1mb_bulkstring(b: &mut Bencher) {
    let f = Frame::BulkString(rand_chars(1024 * 1024).into_bytes());

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    });
  }

  #[bench]
  fn bench_encode_10mb_bulkstring(b: &mut Bencher) {
    let f = Frame::BulkString(rand_chars(10 * 1024 * 1024).into_bytes());

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    });
  }

}