#![feature(test)]

extern crate bytes;
extern crate rand;
extern crate redis_protocol;
extern crate test;

#[macro_use]
extern crate lazy_static;

use rand::Rng;

use bytes::BufMut;
use bytes::BytesMut;
use redis_protocol::prelude::*;

pub fn rand_chars(len: usize) -> String {
    rand::thread_rng().gen_ascii_chars().take(len).collect()
}

fn bulkstring_bytes(len: usize, buf: Option<BytesMut>) -> BytesMut {
    let digits = redis_protocol::digits_in_number(len);
    let mut v = buf.unwrap_or(BytesMut::with_capacity(1 + digits + 2 + len + 2));
    let s = rand_chars(len);

    v.put_u8(b'$');
    v.extend_from_slice(len.to_string().as_bytes());
    v.extend_from_slice(redis_protocol::CRLF.as_bytes());
    v.extend_from_slice(s.as_bytes());
    v.extend_from_slice(redis_protocol::CRLF.as_bytes());
    v
}

pub fn encode_null(buf: Option<BytesMut>) -> BytesMut {
    let mut v = buf.unwrap_or(BytesMut::with_capacity(5));
    v.extend_from_slice(redis_protocol::NULL.as_bytes());
    v
}

pub fn rand_array(len: usize, null_every: usize, str_len: usize) -> BytesMut {
    let arr_len_digits = redis_protocol::digits_in_number(len);
    let str_len_digits = redis_protocol::digits_in_number(str_len);

    let mut buf = BytesMut::with_capacity(
        1 + arr_len_digits + 2 + (len * (1 + str_len_digits + 2 + str_len + 2)),
    );

    (0..len).fold(buf, |mut buf, i| {
        if i + 1 % null_every == 0 {
            encode_null(Some(buf))
        } else {
            bulkstring_bytes(str_len, Some(buf))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    // bulkstring decoding

    #[bench]
    fn bench_decode_1kb_bulkstring(b: &mut Bencher) {
        let buf = bulkstring_bytes(1024, None);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_10kb_bulkstring(b: &mut Bencher) {
        let buf = bulkstring_bytes(10 * 1024, None);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_100kb_bulkstring(b: &mut Bencher) {
        let buf = bulkstring_bytes(100 * 1024, None);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_1mb_bulkstring(b: &mut Bencher) {
        let buf = bulkstring_bytes(1024 * 1024, None);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_10mb_bulkstring(b: &mut Bencher) {
        let buf = bulkstring_bytes(10 * 1024 * 1024, None);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    // array decoding

    #[bench]
    fn bench_decode_array_len_10_no_nulls_1k_values(b: &mut Bencher) {
        let buf = rand_array(10, 11, 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_100_no_nulls_1k_values(b: &mut Bencher) {
        let buf = rand_array(100, 101, 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_1000_no_nulls_1k_values(b: &mut Bencher) {
        let buf = rand_array(1000, 1001, 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_10_no_nulls_10k_values(b: &mut Bencher) {
        let buf = rand_array(10, 11, 10 * 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_100_no_nulls_10k_values(b: &mut Bencher) {
        let buf = rand_array(100, 101, 10 * 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_1000_no_nulls_10k_values(b: &mut Bencher) {
        let buf = rand_array(1000, 1001, 10 * 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_10_half_nulls_1k_values(b: &mut Bencher) {
        let buf = rand_array(10, 2, 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_100_half_nulls_1k_values(b: &mut Bencher) {
        let buf = rand_array(100, 2, 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_1000_half_nulls_1k_values(b: &mut Bencher) {
        let buf = rand_array(1000, 2, 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_10_half_nulls_10k_values(b: &mut Bencher) {
        let buf = rand_array(10, 2, 10 * 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_100_half_nulls_10k_values(b: &mut Bencher) {
        let buf = rand_array(100, 2, 10 * 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

    #[bench]
    fn bench_decode_array_len_1000_half_nulls_10k_values(b: &mut Bencher) {
        let buf = rand_array(1000, 2, 10 * 1024);

        b.iter(|| {
            black_box(decode_bytes(&buf));
        });
    }

}
