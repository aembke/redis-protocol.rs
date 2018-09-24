Redis Protocol
==============

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://travis-ci.org/aembke/redis-protocol.rs.svg?branch=master)](https://travis-ci.org/aembke/redis-protocol.rs)
[![Crates.io](https://img.shields.io/crates/v/redis-protocol.svg)](https://crates.io/crates/redis-protocol)
[![Coverage Status](https://coveralls.io/repos/github/aembke/redis-protocol.rs/badge.svg?branch=master)](https://coveralls.io/github/aembke/redis-protocol.rs?branch=master)
[![API docs](https://docs.rs/redis-protocol/badge.svg)](https://docs.rs/redis-protocol)

Structs and functions for implementing the [Redis protocol](https://redis.io/topics/protocol), built on [nom](https://github.com/Geal/nom) and designed to work easily with [Tokio](https://github.com/tokio-rs/tokio).

## Install

With [cargo edit](https://github.com/killercup/cargo-edit).

```
cargo add redis-protocol
```

## Features

* Encode and decode with `BytesMut` or slices.
* Parse publish-subscribe messages.
* Support `MOVED` and `ASK` errors.
* Implements cluster key hashing.

## Examples

```rust
extern crate redis_protocol;
extern crate bytes;

use redis_protocol::prelude::*;
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
  let (frame, consumed) = match decode_bytes(&buf) {
    Ok((f, c)) => (f, c),
    Err(e) => panic!("Error parsing bytes: {:?}", e)
  };
  
  if let Some(frame) = frame {
    println!("Parsed frame {:?} and consumed {} bytes", frame, consumed);
  }else{
    println!("Incomplete frame, parsed {} bytes", consumed);
  }
  
  let key = "foobarbaz";
  println!("Hash slot for {}: {}", key, redis_keyslot(key));
}
```

See the [encode](/src/encode.rs) and [decode](/src/decode.rs) tests for more examples.

## Tests

To run the unit tests:

```
cargo test
```
