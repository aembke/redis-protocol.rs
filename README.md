Redis Protocol
==============

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/aembke/redis-protocol.rs/tree/main.svg?style=svg)](https://circleci.com/gh/aembke/redis-protocol.rs/tree/main)
[![Crates.io](https://img.shields.io/crates/v/redis-protocol.svg)](https://crates.io/crates/redis-protocol)
[![API docs](https://docs.rs/redis-protocol/badge.svg)](https://docs.rs/redis-protocol)

A Rust implementation of the [Redis protocol](https://redis.io/topics/protocol). 

## Features

* Owned and zero-copy borrowed or [Bytes](https://docs.rs/bytes/latest/bytes/struct.Bytes.html)-based parsing interfaces. 
* Supports RESP2 and RESP3 frames, including streaming frames.
* Publish-subscribe message utilities.
* Cluster key hashing.
* Cluster routing.
* RESP2 and RESP3 [codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html) interfaces. 
* Utility functions for converting from RESP2 to RESP3.
* Utility traits for converting frames into other types. 

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

## Build Features 

| Name          | Default | Description                                                                                                                                  |
|---------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `std`         | x       | Enable stdlib features and most dependency default features.                                                                                 |
| `decode-logs` |         | Enable extra debugging TRACE logs during the frame decoding process.                                                                         |
| `alloc`       |         | Enable `nom/alloc` for use in `no_std` builds.                                                                                               |
| `libm`        |         | Enable `libm` utils for use in `no_std` builds.                                                                                              |
| `hashbrown`   |         | Enable `hashbrown` types for use in `no_std` builds.                                                                                         |
| `routing`     |         | Enable a cluster routing interface.                                                                                                          |
| `codec`       |         | Enable a RESP2 and RESP3 [Tokio codec](https://docs.rs/tokio-util/latest/tokio_util/codec/index.html) interface.                             |
| `convert`     |         | Enable the `FromResp2` and `FromResp3` trait interfaces.                                                                                     |
| `index-map`   |         | Use [IndexMap](https://crates.io/crates/indexmap) types instead of `HashMap`. This is useful for testing and may also be useful for callers. |

## no_std

`no_std` builds are supported by disabling the `std` feature. However, a few optional dependencies must be activated as a substitute.

````TOML
redis-protocol = { version="X.X.X", default-features = false, features = ["libm", "hashbrown", "alloc"] }
````