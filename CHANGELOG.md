# Changelog

## 7.0.0

### Upgrading from 6.x

* `extend_encode` and `extend_encode_borrowed` return the number of bytes written rather than the buffer length.
* RESP3 maps and sets are `Vec`s in wire order. `FrameMap<K, V>` is `Vec<(K, V)>` and `FrameSet<T>` is `Vec<T>`.
  Convert frames to a `HashMap`, `IndexMap`, etc. to look up keys.
* RESP3 attributes and `Hello` credentials are boxed. Use `OwnedFrame::hello` or `BytesFrame::hello` to build `Hello`
  frames.
* The frame enums, `FrameKind`, `RedisProtocolErrorKind`, and several other enums are `#[non_exhaustive]`.
* `RedisParseError` and the nom error conversions are no longer public.
* Remove the `alloc`, `libm`, `hashbrown`, and `index-map` features. `codec` implies `std`.
* The MSRV is 1.88.

### Changes

* Add `FrameScanner`, used by the codecs, so frames that arrive over many reads decode in linear time
* Add a maximum frame size to the scanners and codecs
* Add `encode_command` and `extend_encode_command` to encode commands without building frames
* Add `BytesMut` versions of the RESP3 streaming encoders
* Add a RESP2 `NullArray` frame
* Add `RangeFrame::to_owned_frame` and `RangeFrame::to_bytes_frame`
* Encode `HELLO` as an array of blob strings, so credentials and names can contain spaces
* Support any RESP3 frame as a map key or set member, and any verbatim string format
* Decode RESP3 `$-1` and `*-1` as `Null`
* Reject malformed frames that could desync the stream, such as blobs without a trailing CRLF or mismatched stream
  terminators
* Return an error when encoding simple strings, errors, or big numbers that contain CR or LF
* Limit the decoder nesting depth to prevent stack overflows
* Make `FromResp2` and `FromResp3` integer conversions checked
* Fix several RESP3 codec panics and edge cases with streamed frames
* Remove the `cookie-factory` dependency and upgrade `nom` to 8
* Change the license to `MIT OR Apache-2.0`

### Benchmarks vs 6.0.0

Decoding and encoding `Bytes` frames on x86_64:

| Benchmark                                         |   6.0.0 |   7.0.0 | Change |
|---------------------------------------------------|--------:|--------:|-------:|
| RESP2 decode, 1 KiB bulk string                   |   36 ns |   31 ns |   -14% |
| RESP2 decode, array of 100 32 B strings           | 2.52 µs | 2.62 µs |    +4% |
| RESP3 decode, array of 100 32 B strings           | 8.79 µs | 5.35 µs |   -39% |
| RESP3 decode, map with 50 pairs                   | 12.6 µs | 6.17 µs |   -51% |
| RESP2 encode, array of 100 32 B strings           | 2.42 µs |  855 ns |   -65% |
| RESP3 encode, map with 50 pairs                   | 2.47 µs |  942 ns |   -62% |
| RESP2 codec, 10,000 element array in 16 KiB reads | 1.86 ms |  358 µs |   -81% |
| RESP3 codec, 10,000 element array in 16 KiB reads | 8.12 ms |  761 µs |   -91% |

`encode_command` is about twice as fast as building and encoding an array of blob strings.

## 6.0.0

* Add `BorrowedFrame` types for encoding use cases
* Change integer encoding to avoid allocations
* Add automatic integer -> BulkString/BlobString type conversion during encoding

## 5.0.1

* Fix codec & resp2 feature flags
* Fix resp3 pubsub frame checks

## 5.0.0

* Remove `decode-mut` FF, move to `bytes` FF
* Add range-based frame types
* Add separate `Owned` and `Bytes` frame variants
* Add codec interfaces
* Add `FromResp2` and `FromResp3` type conversion traits
* Add fuzzing
* Criterion benchmark scaffolding
* Lots of refactoring
* Fix CI
