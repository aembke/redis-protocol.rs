#[cfg(feature = "resp3")]
mod resp3 {
  use crate::{
    error::{RedisProtocolError, RedisProtocolErrorKind},
    resp3::{
      decode::{streaming::decode_bytes_mut as resp3_decode, FrameScanner},
      encode::complete::{extend_encode as resp3_encode, extend_encode_borrowed as resp3_encode_borrowed},
      types::{BorrowedFrame, BytesFrame as Resp3Frame, FrameKind, StreamedFrame},
    },
    utils,
  };
  use bytes::BytesMut;
  use tokio_util::codec::{Decoder, Encoder};
  /// Encode a redis command string (`SET foo bar NX`, etc) into a RESP3 blob string array.
  pub fn resp3_encode_command(cmd: &str) -> Resp3Frame {
    Resp3Frame::Array {
      data:       cmd
        .split(' ')
        .map(|s| Resp3Frame::BlobString {
          data:       s.as_bytes().to_vec().into(),
          attributes: None,
        })
        .collect(),
      attributes: None,
    }
  }

  /// A framed codec for complete and streaming/chunked RESP3 frames.
  ///
  /// ```rust
  /// use futures::{SinkExt, StreamExt};
  /// use redis_protocol::{
  ///   codec::{resp3_encode_command, Resp3},
  ///   resp3::types::{BytesFrame, Resp3Frame, RespVersion},
  /// };
  /// use tokio::net::TcpStream;
  /// use tokio_util::codec::Framed;
  ///
  /// // send `HELLO 3 AUTH foo bar` then `GET foo`
  /// async fn example() {
  ///   let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
  ///   let mut framed = Framed::new(socket, Resp3::default());
  ///
  ///   let hello = BytesFrame::hello(RespVersion::RESP3, Some(("foo", "bar")), None);
  ///   // or use the shorthand, but this likely only works for simple use cases
  ///   let get_foo = resp3_encode_command("GET foo");
  ///
  ///   // `Framed` implements both `Sink` and `Stream`
  ///   let _ = framed.send(hello).await.unwrap();
  ///   let response = framed.next().await.unwrap();
  ///   println!("HELLO response: {:?}", response);
  ///
  ///   let _ = framed.send(get_foo).await.unwrap();
  ///   let response = framed.next().await.unwrap();
  ///   println!("GET foo: {:?}", response);
  /// }
  /// ```
  #[derive(Clone, Debug, Default)]
  pub struct Resp3 {
    streaming:         Option<StreamedFrame<Resp3Frame>>,
    /// The number of bytes in the current streamed frame.
    streamed_len:      usize,
    scanner:           FrameScanner,
    int_as_blobstring: bool,
  }

  impl Resp3 {
    /// Create a new codec with the provided flag describing whether the encoder logic should send integers as blob
    /// strings.
    pub fn new(int_as_blobstring: bool) -> Resp3 {
      Resp3 {
        int_as_blobstring,
        streaming: None,
        streamed_len: 0,
        scanner: FrameScanner::new(),
      }
    }

    /// Return a decoding error for frames larger than `max` bytes, rather than buffering them.
    ///
    /// There's no limit by default. The limit includes any nested frames, and applies to the total size of a streamed
    /// frame.
    pub fn with_max_frame_size(mut self, max: usize) -> Resp3 {
      self.scanner.set_max_frame_size(Some(max));
      self
    }
  }

  impl Encoder<Resp3Frame> for Resp3 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: Resp3Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
      resp3_encode(dst, &item, self.int_as_blobstring).map(|_| ())
    }
  }

  impl Encoder<BorrowedFrame<'_>> for Resp3 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: BorrowedFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
      resp3_encode_borrowed(dst, &item, self.int_as_blobstring).map(|_| ())
    }
  }

  impl Decoder for Resp3 {
    type Error = RedisProtocolError;
    type Item = Resp3Frame;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
      loop {
        let buffered = src.len();
        let result = self.decode_next(src).inspect_err(|_| {
          // the frame that caused the error may be partially consumed, so start over with the next frame
          self.streaming = None;
          self.streamed_len = 0;
          self.scanner.reset();
        });
        match result? {
          Some(frame) => return Ok(Some(frame)),
          // the decoder consumed part of a streamed frame. `Framed` waits for more data after `None`, so keep
          // decoding in case the rest of the stream is already buffered.
          None if !src.is_empty() && src.len() < buffered => continue,
          None => return Ok(None),
        }
      }
    }
  }

  impl Resp3 {
    /// Decode the next frame, which may be part of a streamed frame.
    fn decode_next(&mut self, src: &mut BytesMut) -> Result<Option<Resp3Frame>, RedisProtocolError> {
      if src.is_empty() {
        return Ok(None);
      }
      // the type byte of the next frame, which is consumed along with the frame
      let type_byte = src[0];
      // a decoding attempt parses the buffer from the start, so the scanner avoids repeated attempts on partial
      // frames
      let (parsed, amt) = match self.scanner.decode_with(src, resp3_decode)? {
        Some((f, amt, _)) => (f, amt),
        None => return Ok(None),
      };

      if self.streaming.is_some() && parsed.is_streaming() {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Cannot start a stream while already inside a stream.",
        ));
      }

      if let Some(ref mut state) = self.streaming {
        // we started receiving streamed data earlier
        let frame = parsed.into_complete_frame()?;
        check_streamed_frame(state.kind, type_byte, &frame)?;
        self.streamed_len = self.streamed_len.saturating_add(amt);
        if let Some(max) = self.scanner.max_frame_size() {
          if self.streamed_len > max {
            return Err(utils::frame_too_large(max));
          }
        }
        state.add_frame(frame);

        if state.is_finished() {
          // clear the stream before building the frame so an error doesn't leave a stale stream behind
          let mut state = self.streaming.take().expect("missing streamed frame");
          Ok(Some(state.take()?))
        } else {
          Ok(None)
        }
      } else if parsed.is_streaming() {
        self.streaming = Some(parsed.into_streaming_frame()?);
        self.streamed_len = amt;
        Ok(None)
      } else {
        match parsed.into_complete_frame()? {
          Resp3Frame::ChunkedString(_) => Err(RedisProtocolError::new(
            RedisProtocolErrorKind::DecodeError,
            "Unexpected chunked string outside of a streamed frame.",
          )),
          frame => Ok(Some(frame)),
        }
      }
    }
  }

  /// Check that a frame can be part of a stream of type `kind`.
  ///
  /// Streamed strings only contain chunks and end with `;0`, and streamed aggregates don't contain chunks and end
  /// with `.`. Both terminators decode to the same frame, so this checks the type byte of the frame.
  fn check_streamed_frame(kind: FrameKind, type_byte: u8, frame: &Resp3Frame) -> Result<(), RedisProtocolError> {
    let is_valid = match (kind, frame) {
      (FrameKind::BlobString, Resp3Frame::ChunkedString(_)) => type_byte == FrameKind::ChunkedString.to_byte(),
      (FrameKind::BlobString, _) => false,
      (_, Resp3Frame::ChunkedString(data)) => data.is_empty() && type_byte == FrameKind::EndStream.to_byte(),
      _ => true,
    };

    if is_valid {
      Ok(())
    } else if kind == FrameKind::BlobString {
      Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected a chunk or `;0` in a streamed string.",
      ))
    } else {
      Err(RedisProtocolError::new(
        RedisProtocolErrorKind::DecodeError,
        "Expected a frame or `.` in a streamed aggregate.",
      ))
    }
  }

  #[cfg(test)]
  mod tests {
    use super::*;

    #[test]
    fn should_decode_buffered_streamed_frames() {
      let bytes = b"$?\r\n;4\r\nHell\r\n;6\r\no worl\r\n;1\r\nd\r\n;0\r\n*?\r\n:1\r\n+two\r\n.\r\n_\r\n";
      let expected = [
        Resp3Frame::BlobString {
          data:       "Hello world".into(),
          attributes: None,
        },
        Resp3Frame::Array {
          data:       vec![
            Resp3Frame::Number {
              data:       1,
              attributes: None,
            },
            Resp3Frame::SimpleString {
              data:       "two".into(),
              attributes: None,
            },
          ],
          attributes: None,
        },
        Resp3Frame::Null,
      ];

      // decode after each read, like `Framed`, which only calls `decode` again after `None` once more data arrives
      for chunk_size in [1, 2, 5, bytes.len()] {
        let mut codec = Resp3::default();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        for chunk in bytes.chunks(chunk_size) {
          buf.extend_from_slice(chunk);
          while let Some(frame) = codec.decode(&mut buf).unwrap() {
            frames.push(frame);
          }
        }

        assert!(buf.is_empty(), "chunk size {chunk_size}");
        assert_eq!(frames, expected, "chunk size {chunk_size}");
      }
    }

    #[test]
    fn should_reject_frames_larger_than_the_limit() {
      let mut codec = Resp3::default().with_max_frame_size(17);
      let mut buf = BytesMut::from(&b"$10\r\n0123456789\r\n"[..]);
      assert!(codec.decode(&mut buf).unwrap().is_some());
      let mut buf = BytesMut::from(&b"$11\r\n01234567890\r\n"[..]);
      assert!(codec.decode(&mut buf).is_err());

      // the limit applies to the whole streamed frame, not each chunk
      let mut codec = Resp3::default().with_max_frame_size(32);
      let mut buf = BytesMut::from(&b"$?\r\n;4\r\nabcd\r\n;4\r\nefgh\r\n;0\r\n"[..]);
      assert!(codec.decode(&mut buf).unwrap().is_some());
      let mut buf = BytesMut::from(&b"$?\r\n;4\r\nabcd\r\n;4\r\nefgh\r\n;4\r\nijkl\r\n;0\r\n"[..]);
      assert!(codec.decode(&mut buf).is_err());
      let mut buf = BytesMut::from(&b"$?\r\n;4\r\nabcd\r\n;4\r\nefgh\r\n;0\r\n"[..]);
      assert!(codec.decode(&mut buf).unwrap().is_some());
    }

    #[test]
    fn should_return_errors_for_invalid_streamed_frames() {
      let ok = Resp3Frame::SimpleString {
        data:       "OK".into(),
        attributes: None,
      };

      for bytes in [
        // an aggregate as a set member or map key
        &b"~?\r\n*0\r\n.\r\n"[..],
        b"%?\r\n*1\r\n:1\r\n:2\r\n.\r\n",
        // a map with a key and no value
        b"%?\r\n+a\r\n.\r\n",
        // a frame other than a chunk in a streamed string
        b"$?\r\n:1\r\n;0\r\n",
        // a streamed frame with the wrong terminator
        b"$?\r\n;3\r\nfoo\r\n.\r\n",
        b"*?\r\n:1\r\n;0\r\n",
        // a chunk in a streamed aggregate
        b"*?\r\n;3\r\nfoo\r\n.\r\n",
        // chunks outside of a streamed frame
        b";3\r\nfoo\r\n",
        b".\r\n",
      ] {
        let mut codec = Resp3::default();
        let mut buf = BytesMut::from(bytes);
        assert!(codec.decode(&mut buf).is_err(), "{bytes:?}");

        // the codec can be used again after an error
        buf.clear();
        buf.extend_from_slice(b"+OK\r\n");
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(ok.clone()), "{bytes:?}");
      }
    }
  }
}

#[cfg(feature = "resp2")]
mod resp2 {
  use crate::{
    error::RedisProtocolError,
    resp2::{
      decode::{decode_bytes_mut as resp2_decode, FrameScanner},
      encode::{extend_encode as resp2_encode, extend_encode_borrowed as resp2_encode_borrowed},
      types::{BorrowedFrame, BytesFrame as Resp2Frame},
    },
  };
  use bytes::BytesMut;
  use tokio_util::codec::{Decoder, Encoder};
  /// Encode a redis command string (`SET foo bar NX`, etc) into a RESP2 bulk string array.
  pub fn resp2_encode_command(cmd: &str) -> Resp2Frame {
    Resp2Frame::Array(
      cmd
        .split(' ')
        .map(|s| Resp2Frame::BulkString(s.as_bytes().to_vec().into()))
        .collect(),
    )
  }

  /// A framed RESP2 codec.
  ///
  /// ```rust
  /// use futures::{SinkExt, StreamExt};
  /// use redis_protocol::{
  ///   codec::{resp2_encode_command, Resp2},
  ///   resp2::types::{BytesFrame, Resp2Frame},
  /// };
  /// use tokio::net::TcpStream;
  /// use tokio_util::codec::Framed;
  ///
  /// async fn example() {
  ///   let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
  ///   let mut framed = Framed::new(socket, Resp2::default());
  ///
  ///   let auth = resp2_encode_command("AUTH foo bar");
  ///   let get_foo = resp2_encode_command("GET foo");
  ///
  ///   let _ = framed.send(auth).await.unwrap();
  ///   let response = framed.next().await.unwrap().unwrap();
  ///   assert_eq!(response.as_str().unwrap(), "OK");
  ///
  ///   let _ = framed.send(get_foo).await.unwrap();
  ///   let response = framed.next().await.unwrap().unwrap();
  ///   assert_eq!(response, BytesFrame::Null);
  /// }
  /// ```
  #[derive(Clone, Debug, Default)]
  pub struct Resp2 {
    scanner:           FrameScanner,
    int_as_bulkstring: bool,
  }

  impl Resp2 {
    /// Create a new codec with the provided flag describing whether the encoder logic should send integers as blob
    /// strings.
    pub fn new(int_as_bulkstring: bool) -> Resp2 {
      Resp2 {
        int_as_bulkstring,
        scanner: FrameScanner::new(),
      }
    }

    /// Return a decoding error for frames larger than `max` bytes, rather than buffering them.
    ///
    /// There's no limit by default. The limit includes any nested frames.
    pub fn with_max_frame_size(mut self, max: usize) -> Resp2 {
      self.scanner.set_max_frame_size(Some(max));
      self
    }
  }

  impl Encoder<Resp2Frame> for Resp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: Resp2Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
      resp2_encode(dst, &item, self.int_as_bulkstring).map(|_| ())
    }
  }

  impl Encoder<BorrowedFrame<'_>> for Resp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: BorrowedFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
      resp2_encode_borrowed(dst, &item, self.int_as_bulkstring).map(|_| ())
    }
  }

  impl Decoder for Resp2 {
    type Error = RedisProtocolError;
    type Item = Resp2Frame;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
      if src.is_empty() {
        return Ok(None);
      }
      // a decoding attempt parses the buffer from the start, so the scanner avoids repeated attempts on partial
      // frames
      let parsed = match self.scanner.decode_with(src, resp2_decode)? {
        Some((frame, _, _)) => frame,
        None => return Ok(None),
      };

      Ok(Some(parsed))
    }
  }
  #[cfg(test)]
  mod tests {
    use super::*;

    #[test]
    fn should_reject_frames_larger_than_the_limit() {
      let mut codec = Resp2::default().with_max_frame_size(17);
      let mut buf = BytesMut::from(&b"$10\r\n0123456789\r\n"[..]);
      assert!(codec.decode(&mut buf).unwrap().is_some());

      // a partial frame is rejected once more than the limit is buffered
      let mut buf = BytesMut::from(&b"$100\r\n0123"[..]);
      assert_eq!(codec.decode(&mut buf).unwrap(), None);
      buf.extend_from_slice(b"456789012345");
      assert!(codec.decode(&mut buf).is_err());
    }
  }
}

#[cfg(feature = "resp3")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp3")))]
pub use resp3::*;

#[cfg(feature = "resp2")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp2")))]
pub use resp2::*;
