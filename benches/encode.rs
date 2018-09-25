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

pub fn rand_array(len: usize, null_every: usize, str_len: usize) -> Vec<Frame> {
  let mut v = Vec::with_capacity(len);

  for i in 0..len {
    if i+1 % null_every == 0 {
      v.push(Frame::Null);
    }else{
      v.push(Frame::BulkString(rand_chars(str_len).into_bytes()));
    }
  }

  v
}

#[cfg(test)]
mod tests {
  use super::*;
  use test::{Bencher, black_box};

  // bulkstring encoding

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


  // array encoding

  #[bench]
  fn bench_encode_array_len_10_no_nulls_1k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(10, 11, 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_100_no_nulls_1k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(100, 101, 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_1000_no_nulls_1k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(1000, 1001, 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_10_no_nulls_10k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(10, 11, 10 * 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_100_no_nulls_10k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(100, 101, 10 * 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_1000_no_nulls_10k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(1000, 1001, 10 * 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_10_half_nulls_1k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(10, 2, 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_100_half_nulls_1k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(100, 2, 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_1000_half_nulls_1k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(1000, 2, 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_10_half_nulls_10k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(10, 2, 10 * 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_100_half_nulls_10k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(100, 2, 10 * 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

  #[bench]
  fn bench_encode_array_len_1000_half_nulls_10k_values(b: &mut Bencher) {
    let f = Frame::Array(rand_array(1000, 2, 10 * 1024));

    b.iter(|| {
      let mut b = BytesMut::new();
      black_box(encode_bytes(&mut b, &f));
    })
  }

}