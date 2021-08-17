#![feature(test)]

extern crate test;

use bytes::BufMut;
use bytes::BytesMut;
use rand::Rng;

pub fn rand_chars(len: usize) -> String {
  rand::thread_rng().gen_ascii_chars().take(len).collect()
}

#[cfg(test)]
mod tests {
  use super::*;
  use test::{black_box, Bencher};

  use redis_protocol::redis_keyslot;

  #[bench]
  fn bench_redis_keyslot_random_32b(b: &mut Bencher) {
    let k = rand_chars(32);

    b.iter(|| {
      black_box(redis_keyslot(&k));
    });
  }

  #[bench]
  fn bench_redis_keyslot_random_64b(b: &mut Bencher) {
    let k = rand_chars(64);

    b.iter(|| {
      black_box(redis_keyslot(&k));
    });
  }

  #[bench]
  fn bench_redis_keyslot_random_128b(b: &mut Bencher) {
    let k = rand_chars(128);

    b.iter(|| {
      black_box(redis_keyslot(&k));
    });
  }

  #[bench]
  fn bench_redis_keyslot_random_256b(b: &mut Bencher) {
    let k = rand_chars(256);

    b.iter(|| {
      black_box(redis_keyslot(&k));
    });
  }

  #[bench]
  fn bench_redis_keyslot_random_512b(b: &mut Bencher) {
    let k = rand_chars(512);

    b.iter(|| {
      black_box(redis_keyslot(&k));
    });
  }

  #[bench]
  fn bench_redis_keyslot_random_1kb(b: &mut Bencher) {
    let k = rand_chars(1024);

    b.iter(|| {
      black_box(redis_keyslot(&k));
    });
  }
}
