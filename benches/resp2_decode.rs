use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};
use redis_protocol::{
  resp2::{
    decode,
    types::{FrameKind, OwnedFrame, Resp2Frame, NULL},
  },
  types::CRLF,
};

#[cfg(feature = "bytes")]
use bytes::{BufMut, Bytes, BytesMut};
#[cfg(feature = "bytes")]
use redis_protocol::resp2::types::BytesFrame;

#[cfg(feature = "bytes")]
fn gen_bulkstring_bytes(len: usize, buf: Option<BytesMut>) -> BytesMut {
  let digits = redis_protocol::digits_in_number(len);
  let mut v = buf.unwrap_or_else(|| BytesMut::with_capacity(1 + digits + 2 + len + 2));

  v.put_u8(b'$');
  v.extend_from_slice(len.to_string().as_bytes());
  v.extend_from_slice(CRLF.as_bytes());
  v.extend(rand::thread_rng().sample_iter(&Alphanumeric).take(len));
  v.extend_from_slice(CRLF.as_bytes());
  v
}

#[cfg(feature = "bytes")]
fn gen_null_bytes(buf: Option<BytesMut>) -> BytesMut {
  let mut v = buf.unwrap_or_else(|| BytesMut::with_capacity(5));
  v.extend_from_slice(NULL.as_bytes());
  v
}

#[cfg(feature = "bytes")]
fn gen_array_bytes(len: usize, null_every: usize, str_len: usize) -> BytesMut {
  let arr_len_digits = redis_protocol::digits_in_number(len);
  let str_len_digits = redis_protocol::digits_in_number(str_len);
  let buf = BytesMut::with_capacity(1 + arr_len_digits + 2 + (len * (1 + str_len_digits + 2 + str_len + 2)));

  (0 .. len).fold(buf, |buf, i| {
    if i + 1 % null_every == 0 {
      gen_null_bytes(Some(buf))
    } else {
      gen_bulkstring_bytes(str_len, Some(buf))
    }
  })
}

fn gen_bulkstring_owned(len: usize, buf: Option<Vec<u8>>) -> Vec<u8> {
  let digits = redis_protocol::digits_in_number(len);
  let mut v = buf.unwrap_or_else(|| Vec::with_capacity(1 + digits + 2 + len + 2));

  v.push(b'$');
  v.extend_from_slice(len.to_string().as_bytes());
  v.extend_from_slice(CRLF.as_bytes());
  v.extend(rand::thread_rng().sample_iter(&Alphanumeric).take(len));
  v.extend_from_slice(CRLF.as_bytes());
  v
}

fn gen_null_owned(buf: Option<Vec<u8>>) -> Vec<u8> {
  let mut v = buf.unwrap_or_else(|| Vec::with_capacity(5));
  v.extend_from_slice(NULL.as_bytes());
  v
}

fn gen_array_owned(len: usize, null_every: usize, str_len: usize) -> Vec<u8> {
  let arr_len_digits = redis_protocol::digits_in_number(len);
  let str_len_digits = redis_protocol::digits_in_number(str_len);
  let buf = Vec::with_capacity(1 + arr_len_digits + 2 + (len * (1 + str_len_digits + 2 + str_len + 2)));

  (0 .. len).fold(buf, |buf, i| {
    if i + 1 % null_every == 0 {
      gen_null_owned(Some(buf))
    } else {
      gen_bulkstring_owned(str_len, Some(buf))
    }
  })
}

struct OwnedInput {
  pub _1kb_bs:                 Vec<u8>,
  pub _10kb_bs:                Vec<u8>,
  pub _100kb_bs:               Vec<u8>,
  pub _1mb_bs:                 Vec<u8>,
  pub _10mb_bs:                Vec<u8>,
  pub _10_no_nulls_1k_bs:      Vec<u8>,
  pub _100_no_nulls_1k_bs:     Vec<u8>,
  pub _1000_no_nulls_1k_bs:    Vec<u8>,
  pub _10_no_nulls_10k_bs:     Vec<u8>,
  pub _100_no_nulls_10k_bs:    Vec<u8>,
  pub _1000_no_nulls_10k_bs:   Vec<u8>,
  pub _10_half_nulls_1k_bs:    Vec<u8>,
  pub _100_half_nulls_1k_bs:   Vec<u8>,
  pub _1000_half_nulls_1k_bs:  Vec<u8>,
  pub _10_half_nulls_10k_bs:   Vec<u8>,
  pub _100_half_nulls_10k_bs:  Vec<u8>,
  pub _1000_half_nulls_10k_bs: Vec<u8>,
}

impl Default for OwnedInput {
  fn default() -> Self {
    OwnedInput {
      _1kb_bs:                 gen_bulkstring_owned(1024, None),
      _10kb_bs:                gen_bulkstring_owned(10 * 1024, None),
      _100kb_bs:               gen_bulkstring_owned(100 * 1024, None),
      _1mb_bs:                 gen_bulkstring_owned(1_000 * 1024, None),
      _10mb_bs:                gen_bulkstring_owned(10_000 * 1024, None),
      _10_no_nulls_1k_bs:      gen_array_owned(10, 11, 1024),
      _100_no_nulls_1k_bs:     gen_array_owned(100, 101, 1024),
      _1000_no_nulls_1k_bs:    gen_array_owned(100, 1001, 1024),
      _10_no_nulls_10k_bs:     gen_array_owned(10, 11, 10 * 1024),
      _100_no_nulls_10k_bs:    gen_array_owned(100, 101, 10 * 1024),
      _1000_no_nulls_10k_bs:   gen_array_owned(1000, 1001, 10 * 1024),
      _10_half_nulls_1k_bs:    gen_array_owned(10, 2, 1024),
      _100_half_nulls_1k_bs:   gen_array_owned(100, 2, 1024),
      _1000_half_nulls_1k_bs:  gen_array_owned(1000, 2, 1024),
      _10_half_nulls_10k_bs:   gen_array_owned(10, 2, 10 * 1024),
      _100_half_nulls_10k_bs:  gen_array_owned(100, 2, 10 * 1024),
      _1000_half_nulls_10k_bs: gen_array_owned(1000, 2, 10 * 1024),
    }
  }
}

#[cfg(feature = "bytes")]
struct BytesInput {
  pub _1kb_bs:                 Bytes,
  pub _10kb_bs:                Bytes,
  pub _100kb_bs:               Bytes,
  pub _1mb_bs:                 Bytes,
  pub _10mb_bs:                Bytes,
  pub _10_no_nulls_1k_bs:      Bytes,
  pub _100_no_nulls_1k_bs:     Bytes,
  pub _1000_no_nulls_1k_bs:    Bytes,
  pub _10_no_nulls_10k_bs:     Bytes,
  pub _100_no_nulls_10k_bs:    Bytes,
  pub _1000_no_nulls_10k_bs:   Bytes,
  pub _10_half_nulls_1k_bs:    Bytes,
  pub _100_half_nulls_1k_bs:   Bytes,
  pub _1000_half_nulls_1k_bs:  Bytes,
  pub _10_half_nulls_10k_bs:   Bytes,
  pub _100_half_nulls_10k_bs:  Bytes,
  pub _1000_half_nulls_10k_bs: Bytes,
}

#[cfg(feature = "bytes")]
impl Default for BytesInput {
  fn default() -> Self {
    BytesInput {
      _1kb_bs:                 gen_bulkstring_bytes(1024, None).freeze(),
      _10kb_bs:                gen_bulkstring_bytes(10 * 1024, None).freeze(),
      _100kb_bs:               gen_bulkstring_bytes(100 * 1024, None).freeze(),
      _1mb_bs:                 gen_bulkstring_bytes(1_000 * 1024, None).freeze(),
      _10mb_bs:                gen_bulkstring_bytes(10_000 * 1024, None).freeze(),
      _10_no_nulls_1k_bs:      gen_array_bytes(10, 11, 1024).freeze(),
      _100_no_nulls_1k_bs:     gen_array_bytes(100, 101, 1024).freeze(),
      _1000_no_nulls_1k_bs:    gen_array_bytes(100, 1001, 1024).freeze(),
      _10_no_nulls_10k_bs:     gen_array_bytes(10, 11, 10 * 1024).freeze(),
      _100_no_nulls_10k_bs:    gen_array_bytes(100, 101, 10 * 1024).freeze(),
      _1000_no_nulls_10k_bs:   gen_array_bytes(1000, 1001, 10 * 1024).freeze(),
      _10_half_nulls_1k_bs:    gen_array_bytes(10, 2, 1024).freeze(),
      _100_half_nulls_1k_bs:   gen_array_bytes(100, 2, 1024).freeze(),
      _1000_half_nulls_1k_bs:  gen_array_bytes(1000, 2, 1024).freeze(),
      _10_half_nulls_10k_bs:   gen_array_bytes(10, 2, 10 * 1024).freeze(),
      _100_half_nulls_10k_bs:  gen_array_bytes(100, 2, 10 * 1024).freeze(),
      _1000_half_nulls_10k_bs: gen_array_bytes(1000, 2, 10 * 1024).freeze(),
    }
  }
}

#[cfg(feature = "bytes")]
struct BytesMutInput {
  pub _1kb_bs:                 BytesMut,
  pub _10kb_bs:                BytesMut,
  pub _100kb_bs:               BytesMut,
  pub _1mb_bs:                 BytesMut,
  pub _10mb_bs:                BytesMut,
  pub _10_no_nulls_1k_bs:      BytesMut,
  pub _100_no_nulls_1k_bs:     BytesMut,
  pub _1000_no_nulls_1k_bs:    BytesMut,
  pub _10_no_nulls_10k_bs:     BytesMut,
  pub _100_no_nulls_10k_bs:    BytesMut,
  pub _1000_no_nulls_10k_bs:   BytesMut,
  pub _10_half_nulls_1k_bs:    BytesMut,
  pub _100_half_nulls_1k_bs:   BytesMut,
  pub _1000_half_nulls_1k_bs:  BytesMut,
  pub _10_half_nulls_10k_bs:   BytesMut,
  pub _100_half_nulls_10k_bs:  BytesMut,
  pub _1000_half_nulls_10k_bs: BytesMut,
}

#[cfg(feature = "bytes")]
impl Default for BytesMutInput {
  fn default() -> Self {
    BytesMutInput {
      _1kb_bs:                 gen_bulkstring_bytes(1024, None),
      _10kb_bs:                gen_bulkstring_bytes(10 * 1024, None),
      _100kb_bs:               gen_bulkstring_bytes(100 * 1024, None),
      _1mb_bs:                 gen_bulkstring_bytes(1_000 * 1024, None),
      _10mb_bs:                gen_bulkstring_bytes(10_000 * 1024, None),
      _10_no_nulls_1k_bs:      gen_array_bytes(10, 11, 1024),
      _100_no_nulls_1k_bs:     gen_array_bytes(100, 101, 1024),
      _1000_no_nulls_1k_bs:    gen_array_bytes(100, 1001, 1024),
      _10_no_nulls_10k_bs:     gen_array_bytes(10, 11, 10 * 1024),
      _100_no_nulls_10k_bs:    gen_array_bytes(100, 101, 10 * 1024),
      _1000_no_nulls_10k_bs:   gen_array_bytes(1000, 1001, 10 * 1024),
      _10_half_nulls_1k_bs:    gen_array_bytes(10, 2, 1024),
      _100_half_nulls_1k_bs:   gen_array_bytes(100, 2, 1024),
      _1000_half_nulls_1k_bs:  gen_array_bytes(1000, 2, 1024),
      _10_half_nulls_10k_bs:   gen_array_bytes(10, 2, 10 * 1024),
      _100_half_nulls_10k_bs:  gen_array_bytes(100, 2, 10 * 1024),
      _1000_half_nulls_10k_bs: gen_array_bytes(1000, 2, 10 * 1024),
    }
  }
}

fn owned(c: &mut Criterion) {
  // TODO look into criterion more to see how this can be parameterized
  // also try mixing in integers and other types
  let input = OwnedInput::default();

  c.bench_function("decode 1kb bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._1kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 10kb bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._10kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 100kb bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._100kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 1mb bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._1mb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 10mb bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._10mb_bs)).unwrap().unwrap())
  });

  c.bench_function("decode array - 10 no nulls 1k bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._10_no_nulls_1k_bs)).unwrap().unwrap())
  });
  c.bench_function("decode array - 100 no nulls 1k bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._100_no_nulls_1k_bs)).unwrap().unwrap())
  });
  c.bench_function("decode array - 1000 no nulls 1k bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._1000_no_nulls_1k_bs)).unwrap().unwrap())
  });

  c.bench_function("decode array - 10 no nulls 10k bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._10_no_nulls_10k_bs)).unwrap().unwrap())
  });
  c.bench_function("decode array - 100 no nulls 10k bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._100_no_nulls_10k_bs)).unwrap().unwrap())
  });
  c.bench_function("decode array - 1000 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode(black_box(&input._1000_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 half nulls 1k bulk string", |b| {
    b.iter(|| decode::decode(black_box(&input._10_half_nulls_1k_bs)).unwrap().unwrap())
  });
  c.bench_function("decode array - 100 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode(black_box(&input._100_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode(black_box(&input._1000_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode(black_box(&input._10_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode(black_box(&input._100_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode(black_box(&input._1000_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
}

#[cfg(feature = "bytes")]
fn bytes(c: &mut Criterion) {
  let input = BytesInput::default();

  c.bench_function("decode 1kb bulk string", |b| {
    b.iter(|| decode::decode_bytes(black_box(&input._1kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 10kb bulk string", |b| {
    b.iter(|| decode::decode_bytes(black_box(&input._10kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 100kb bulk string", |b| {
    b.iter(|| decode::decode_bytes(black_box(&input._100kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 1mb bulk string", |b| {
    b.iter(|| decode::decode_bytes(black_box(&input._1mb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 10mb bulk string", |b| {
    b.iter(|| decode::decode_bytes(black_box(&input._10mb_bs)).unwrap().unwrap())
  });

  c.bench_function("decode array - 10 no nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._10_no_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 no nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._100_no_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 no nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._1000_no_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._10_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._100_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._1000_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._10_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._100_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._1000_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._10_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._100_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_bytes(black_box(&input._1000_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
}

#[cfg(feature = "bytes")]
fn bytes_mut(c: &mut Criterion) {
  let mut input = BytesMutInput::default();

  c.bench_function("decode 1kb bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._1kb_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode 10kb bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._10kb_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode 100kb bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._100kb_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode 1mb bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._1mb_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode 10mb bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._10mb_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });

  c.bench_function("decode array - 10 no nulls 1k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._10_no_nulls_1k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 100 no nulls 1k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._100_no_nulls_1k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 1000 no nulls 1k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._1000_no_nulls_1k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });

  c.bench_function("decode array - 10 no nulls 10k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._10_no_nulls_10k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 100 no nulls 10k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._100_no_nulls_10k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 1000 no nulls 10k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._1000_no_nulls_10k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });

  c.bench_function("decode array - 10 half nulls 1k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._10_half_nulls_1k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 100 half nulls 1k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._100_half_nulls_1k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 1000 half nulls 1k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._1000_half_nulls_1k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });

  c.bench_function("decode array - 10 half nulls 10k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._10_half_nulls_10k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 100 half nulls 10k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._100_half_nulls_10k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
  c.bench_function("decode array - 1000 half nulls 10k bulk string", |b| {
    b.iter_batched_ref(|| {
      let mut buf = input._1000_half_nulls_10k_bs.clone();
      decode::decode_bytes_mut(&mut buf).unwrap().unwrap();
    })
  });
}

fn range(c: &mut Criterion) {
  let input = OwnedInput::default();

  c.bench_function("decode 1kb bulk string", |b| {
    b.iter(|| decode::decode_range(black_box(&input._1kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 10kb bulk string", |b| {
    b.iter(|| decode::decode_range(black_box(&input._10kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 100kb bulk string", |b| {
    b.iter(|| decode::decode_range(black_box(&input._100kb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 1mb bulk string", |b| {
    b.iter(|| decode::decode_range(black_box(&input._1mb_bs)).unwrap().unwrap())
  });
  c.bench_function("decode 10mb bulk string", |b| {
    b.iter(|| decode::decode_range(black_box(&input._10mb_bs)).unwrap().unwrap())
  });

  c.bench_function("decode array - 10 no nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._10_no_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 no nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._100_no_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 no nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._1000_no_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._10_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._100_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 no nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._1000_no_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._10_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._100_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 half nulls 1k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._1000_half_nulls_1k_bs))
        .unwrap()
        .unwrap()
    })
  });

  c.bench_function("decode array - 10 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._10_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 100 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._100_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
  c.bench_function("decode array - 1000 half nulls 10k bulk string", |b| {
    b.iter(|| {
      decode::decode_range(black_box(&input._1000_half_nulls_10k_bs))
        .unwrap()
        .unwrap()
    })
  });
}

criterion_group!(bench_owned, owned);
criterion_group!(bench_range, range);
#[cfg(feature = "bytes")]
criterion_group!(bench_bytes, bytes);
#[cfg(feature = "bytes")]
criterion_group!(bench_bytes_mut, bytes_mut);

#[cfg(feature = "bytes")]
criterion_main!(bench_owned, bench_range, bench_bytes, bench_bytes_mut);
#[cfg(not(feature = "bytes"))]
criterion_main!(bench_owned, bench_range);
