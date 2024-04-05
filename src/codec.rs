#[cfg(feature = "resp3")]
mod resp3 {
  use crate::{
    error::{RedisProtocolError, RedisProtocolErrorKind},
    resp3::{
      decode::streaming::decode_bytes_mut as resp3_decode,
      encode::complete::extend_encode as resp3_encode,
      types::{BytesFrame as Resp3Frame, StreamedFrame},
    },
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
  ///   let hello = Resp3Frame::Hello {
  ///     version:  RespVersion::RESP3,
  ///     username: Some("foo".into()),
  ///     password: Some("bar".into()),
  ///   };
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
  #[derive(Debug, Default)]
  pub struct Resp3 {
    streaming: Option<StreamedFrame<Resp3Frame>>,
  }

  impl Encoder<Resp3Frame> for Resp3 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: Resp3Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
      resp3_encode(dst, &item).map(|_| ()).map_err(RedisProtocolError::from)
    }
  }

  impl Decoder for Resp3 {
    type Error = RedisProtocolError;
    type Item = Resp3Frame;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
      if src.is_empty() {
        return Ok(None);
      }
      let parsed = match resp3_decode(src)? {
        Some((f, _, _)) => f,
        None => return Ok(None),
      };

      if self.streaming.is_some() && parsed.is_streaming() {
        return Err(RedisProtocolError::new(
          RedisProtocolErrorKind::DecodeError,
          "Cannot start a stream while already inside a stream.",
        ));
      }

      let result = if let Some(ref mut state) = self.streaming {
        // we started receiving streamed data earlier
        state.add_frame(parsed.into_complete_frame()?);

        if state.is_finished() {
          Some(state.take()?)
        } else {
          None
        }
      } else {
        // we're processing a complete frame or starting a new streamed frame
        if parsed.is_streaming() {
          self.streaming = Some(parsed.into_streaming_frame()?);
          None
        } else {
          // we're not in the middle of a stream and we found a complete frame
          Some(parsed.into_complete_frame()?)
        }
      };

      if result.is_some() {
        let _ = self.streaming.take();
      }
      Ok(result)
    }
  }
}

#[cfg(feature = "resp2")]
mod resp2 {
  use crate::{
    error::RedisProtocolError,
    resp2::{
      decode::decode_bytes_mut as resp2_decode,
      encode::extend_encode as resp2_encode,
      types::BytesFrame as Resp2Frame,
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
  pub struct Resp2;

  impl Encoder<Resp2Frame> for Resp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: Resp2Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
      resp2_encode(dst, &item).map(|_| ()).map_err(RedisProtocolError::from)
    }
  }

  impl Decoder for Resp2 {
    type Error = RedisProtocolError;
    type Item = Resp2Frame;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
      if src.is_empty() {
        return Ok(None);
      }
      let parsed = match resp2_decode(src)? {
        Some((frame, _, _)) => frame,
        None => return Ok(None),
      };

      Ok(Some(parsed))
    }
  }
}

#[cfg(feature = "resp3")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp3")))]
pub use resp3::*;

#[cfg(feature = "resp2")]
#[cfg_attr(docsrs, doc(cfg(feature = "resp2")))]
pub use resp2::*;
