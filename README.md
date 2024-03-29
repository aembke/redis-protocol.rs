Redis Protocol
==============

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/aembke/redis-protocol.rs/tree/main.svg?style=svg)](https://circleci.com/gh/aembke/redis-protocol.rs/tree/main)
[![Crates.io](https://img.shields.io/crates/v/redis-protocol.svg)](https://crates.io/crates/redis-protocol)
[![API docs](https://docs.rs/redis-protocol/badge.svg)](https://docs.rs/redis-protocol)

Structs and functions for implementing the [Redis protocol](https://redis.io/topics/protocol). 

## Install

With [cargo edit](https://github.com/killercup/cargo-edit).

```
cargo add redis-protocol
```

## Features

* Supports RESP2 and RESP3, including streaming frames.
* Parse publish-subscribe messages.
* Support cluster redirection errors.
* Implements cluster key hashing.
* Utility functions for converting from RESP2 to RESP3.

This library relies heavily on the `Bytes` interface to implement parsing logic such that buffer contents never need to move or be copied. 

## Examples

```rust
use redis_protocol::resp2::prelude::*;
use bytes::{Bytes, BytesMut};

fn main() {
  let frame = Frame::BulkString("foobar".into());
  let mut buf = BytesMut::new();
  
  let len = match encode_bytes(&mut buf, &frame) {
    Ok(l) => l,
    Err(e) => panic!("Error encoding frame: {:?}", e)
  };
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);
  
  let buf: Bytes = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();
  let (frame, consumed) = match decode(&buf) {
    Ok(Some((f, c))) => (f, c),
    Ok(None) => panic!("Incomplete frame."),
    Err(e) => panic!("Error parsing bytes: {:?}", e)
  };
  println!("Parsed frame {:?} and consumed {} bytes", frame, consumed);
  
  let key = "foobarbaz";
  println!("Hash slot for {}: {}", key, redis_keyslot(key));
}
```

## Decode Logs

Use the `decode-logs` feature to enable special TRACE logs during the Frame decoding process.

## Decoding `BytesMut`

Using the default decoder interface with `BytesMut` can be challenging if the goal is to avoid copying or moving the buffer contents. 

To better support this use case (such as the [codec](https://docs.rs/tokio-util/0.6.6/tokio_util/codec/index.html) interface) this library supports a `decode-mut` feature flag that can parse `BytesMut` without copying or moving the buffer contents.

## IndexMap

Enable the `index-map` feature to use [IndexMap](https://crates.io/crates/indexmap) instead of `HashMap` and `HashSet`. This is useful for testing and may also be useful to callers.

## no_std

`no_std` builds are supported by disabling the `std` feature. However, a few optional dependencies must be activated as a substitute.

````TOML
redis-protocol = { version="X.X.X", default-features = false, features = ["libm", "hashbrown", "alloc"] }
````