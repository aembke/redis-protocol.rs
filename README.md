Redis Protocol
==============

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://travis-ci.org/aembke/redis-protocol.rs.svg?branch=master)](https://travis-ci.org/aembke/redis-protocol.rs)
[![Crates.io](https://img.shields.io/crates/v/redis-protocol.svg)](https://crates.io/crates/redis-protocol)
[![API docs](https://docs.rs/redis-protocol/badge.svg)](https://docs.rs/redis-protocol)

Structs and functions for implementing the [Redis protocol](https://redis.io/topics/protocol), built on [nom](https://github.com/Geal/nom) and designed to work easily with [Tokio](https://github.com/tokio-rs/tokio).

## Install

With [cargo edit](https://github.com/killercup/cargo-edit).

```
cargo add redis-protocol
```

## Features

* Supports RESP2 and RESP3, including streaming frames.
* Encode and decode with `BytesMut` or slices.
* Parse publish-subscribe messages.
* Support cluster redirection errors.
* Implements cluster key hashing.

## Examples

```rust
extern crate redis_protocol;
extern crate bytes;

use redis_protocol::resp2::prelude::*;
use bytes::BytesMut;

fn main() {
  let frame = Frame::BulkString("foobar".into());
  let mut buf = BytesMut::new();
  
  let len = match encode_bytes(&mut buf, &frame) {
    Ok(l) => l,
    Err(e) => panic!("Error encoding frame: {:?}", e)
  };
  println!("Encoded {} bytes into buffer with contents {:?}", len, buf);
  
  let buf: BytesMut = "*3\r\n$3\r\nFoo\r\n$-1\r\n$3\r\nBar\r\n".into();
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

## IndexMap

Enable the `index-map` feature to use [IndexMap](https://crates.io/crates/indexmap) instead of `HashMap` and `HashSet`. This is useful for testing and may also be useful to callers.

## Tests

To run the unit tests:

```
cargo test --features index-map
```
