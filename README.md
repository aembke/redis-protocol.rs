Redis Protocol
==============

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/aembke/redis-protocol.rs/tree/main.svg?style=svg)](https://circleci.com/gh/aembke/redis-protocol.rs/tree/main)
[![Crates.io](https://img.shields.io/crates/v/redis-protocol.svg)](https://crates.io/crates/redis-protocol)
[![API docs](https://docs.rs/redis-protocol/badge.svg)](https://docs.rs/redis-protocol)

A Rust implementation of the [Redis protocol](https://redis.io/topics/protocol).

## Features

* Owned and zero-copy [Bytes](https://docs.rs/bytes/latest/bytes/struct.Bytes.html)-based parsing interfaces.
* Supports RESP2 and RESP3 frames, including streaming frames.
* Publish-subscribe message utilities.
* A cluster [hash slot](https://redis.io/docs/reference/cluster-spec/#key-distribution-model) interface.
* RESP2 and RESP3 [codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html) interfaces.
* Utility functions for converting from RESP2 to RESP3.
* Traits for converting frames into other types.

## Examples

```rust
use redis_protocol::resp2::{
  decode::decode,
  encode::encode,
  types::{OwnedFrame as Frame, Resp2Frame}
};

fn main() {
  let frame = Frame::BulkString("foobar".into());
  let mut buf = vec![0; frame.encode_len(false)];

  let len = encode(&mut buf, &frame, false).expect("Error encoding frame");
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);

  // ["Foo", nil, "Bar"]
  let buf: &[u8] = b"*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n";
  match decode(&buf).unwrap() {
    Some((frame, amt)) => println!("Parsed {:?} and read {} bytes", frame, amt),
    None => println!("Incomplete frame."),
  };
}
```

## Build Features

| Name          | Default | Description                                                                                                                                  |
|---------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `std`         | x       | Enable stdlib features and most dependency default features.                                                                                 |
| `resp2`       | x       | Enable the RESP2 interface.                                                                                                                  |
| `resp3`       | x       | Enable the RESP3 interface.                                                                                                                  |
| `bytes`       |         | Enable the zero-copy parsing interface via [Bytes](https://crates.io/crates/bytes) types.                                                    |
| `decode-logs` |         | Enable extra debugging TRACE logs during the frame decoding process.                                                                         |
| `codec`       |         | Enable a RESP2 and RESP3 [Tokio codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html) interface.                             |
| `convert`     |         | Enable the `FromResp2` and `FromResp3` trait interfaces.                                                                                     |
| `index-map`   |         | Use [IndexMap](https://crates.io/crates/indexmap) types instead of `HashMap`. This is useful for testing and may also be useful for callers. |

## no_std

`no_std` builds are supported by disabling the `std` feature. However, a few optional dependencies must be activated as
a substitute.

```TOML
redis-protocol = { version = "X.X.X", default-features = false, features = ["libm", "hashbrown", "alloc"] }
```

## Decoding

Both RESP2 and RESP3 interfaces support 3 different `Frame` interfaces. These interfaces are designed to support
different use cases:

* `OwnedFrame` types use `core` container types to implement owned frame variants. This is the easiest frame
  interface to use, but often requires moving or copying the underlying buffer contents when decoding.
* `BytesFrame` types use [Bytes](https://docs.rs/bytes/1.5.0/bytes/struct.Bytes.html) as the backing container.
  The `bytes` feature flag enables this frame type and a set of associated functions that avoid moving or
  copying `BytesMut` contents.
* `RangeFrame` types represent ranges into an associated buffer and are typically used to implement forms of zero-copy
  parsing. This is the lowest level interface.

### RESP2 `OwnedFrame` Decoding Example

Simple array decoding example adapted from the tests

```rust
use redis_protocol::resp2::{
  decode::decode,
  types::{OwnedFrame, Resp2Frame}
};

fn should_decode_array() {
  // ["Foo", nil, "Bar"]
  let buf: &[u8] = b"*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n";

  let (frame, amt) = decode(&buf).unwrap().unwrap();
  assert_eq!(frame, OwnedFrame::Array(vec![
    OwnedFrame::BulkString("Foo".into()),
    OwnedFrame::Null,
    OwnedFrame::BulkString("Bar".into())
  ]));
  assert_eq!(amt, buf.len());
}
```

### RESP2 `BytesFrame` Decoding Example

Array decoding example adapted from the tests

```rust
use redis_protocol::resp2::{
  decode::decode_bytes_mut,
  types::{BytesFrame, Resp2Frame}
};
use bytes::BytesMut;

fn should_decode_array_no_nulls() {
  let expected = (
    BytesFrame::Array(vec![
      BytesFrame::SimpleString("Foo".into()),
      BytesFrame::SimpleString("Bar".into()),
    ]),
    16,
  );
  let mut bytes: BytesMut = "*2\r\n+Foo\r\n+Bar\r\n".into();
  let total_len = bytes.len();

  let (frame, amt, buf) = match decode_bytes_mut(&mut bytes) {
    Ok(Some(result)) => result,
    Ok(None) => panic!("Expected complete frame"),
    Err(e) => panic!("{:?}", e)
  };

  assert_eq!(frame, expected.0, "decoded frame matched");
  assert_eq!(amt, expected.1, "decoded frame len matched");
  assert_eq!(buf.len(), expected.1, "output buffer len matched");
  assert_eq!(buf.len() + bytes.len(), total_len, "total len matched");
}
```

### RESP2 `RangeFrame` Decoding Example

Implement a custom borrowed frame type that can only represent BulkString and SimpleString

```rust
use redis_protocol::resp2::{
  decode::decode_range,
  types::RangeFrame
};
use std::str;

enum MyBorrowedFrame<'a> {
  BulkString(&'a [u8]),
  SimpleString(&'a str),
}

fn decode_borrowed(buf: &[u8]) -> Option<MyBorrowedFrame> {
  match decode_range(buf).ok()? {
    Some((RangeFrame::BulkString((i, j)), _)) => {
      Some(MyBorrowedFrame::BulkString(&buf[i..j]))
    }
    Some((RangeFrame::SimpleString((i, j)), _)) => {
      let parsed = str::from_utf8(&buf[i..j]).ok()?;
      Some(MyBorrowedFrame::SimpleString(parsed))
    }
    _ => None,
  }
}
```

## Encoding

Similar to the decoding interface, there are 3 frame types available to callers for encoding use cases.

* `OwnedFrame` - See above. Callers that use this interface often have to allocate additional containers on the heap,
  and often have to move or copy the contents into these frame types from other types, such as `RedisValue` in `fred`.
* `BytesFrame` - See above. This frame type often avoids the need to copy or move the contents from upstream types such
  as `RedisValue`, but still requires callers to allocate containers on the heap (i.e. the `Vec` in the `Array`
  variant).
* `BorrowedFrame` - This frame type serves a similar purpose to the `RangeFrame` type in the decoding interface in that
  it can be used when callers want to entirely avoid heap allocations. In practice this often requires the caller to
  create some intermediate reference-based types, but significantly outperforms other approaches that require
  allocations.

### Options

All encoding interfaces expose an additional parameter, `int_as_bulkstring` for RESP2 or `int_as_blobstring` for RESP3,
that can be used to encode integers as `BulkString` or `BlobString` using a more optimized code path that avoids heap
allocations.

In practice Redis expects all incoming frames to be an array of blob or bulk strings. This option can be used to
automatically convert integers to byte arrays without additional allocations.

### Buffer Management

In some cases this library can automatically extend buffers while encoding. The `extend_encode_*` family of functions
operate on `BytesMut` types and can automatically resize buffers while encoding, whereas the `encode_*` family of
functions require the caller to allocate the correct amount beforehand.

```rust
use redis_protocol::resp2::{
  encode,
  types::{OwnedFrame, BytesFrame, BorrowedFrame, Resp2Frame}
};
use bytes::BytesMut;

fn main() {
  // using OwnedFrame variants, manually allocating the buffer
  let frame = OwnedFrame::BulkString("foobar".into());
  let mut buf = vec![0; frame.encode_len(false)];
  let len = encode::encode(&mut buf, &frame, false).expect("Error encoding frame");
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);

  // using BytesFrame variants
  let frame = BytesFrame::BulkString("foobar".into());
  let mut buf = BytesMut::new();
  let len = encode::extend_encode(&mut buf, &frame, false).expect("Error encoding frame");
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);

  // using BorrowedFrame variants
  let value: String = "foobar".into();
  let frame = BorrowedFrame::BulkString(value.as_bytes());
  let mut buf = BytesMut::new();
  let len = encode::extend_encode_borrowed(&mut buf, &frame, false).expect("Error encoding frame");
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);
}

```