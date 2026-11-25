use futures::{SinkExt, StreamExt};
use redis_protocol::{
  codec::{resp2_encode_command, resp3_encode_command, Resp2, Resp3},
  resp2::types::{
    BorrowedFrame as Resp2BorrowedFrame,
    BytesFrame as Resp2BytesFrame,
    FrameKind as Resp2FrameKind,
    Resp2Frame,
  },
  resp3::types::{
    BorrowedFrame as Resp3BorrowedFrame,
    BytesFrame as Resp3BytesFrame,
    FrameKind as Resp3FrameKind,
    FrameMap,
    Resp3Frame,
    RespVersion,
  },
};
use std::env;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

fn read_env_var(name: &str) -> Option<String> {
  env::var_os(name).and_then(|s| s.into_string().ok())
}

fn read_redis_centralized_host() -> String {
  read_env_var("FRED_REDIS_CENTRALIZED_HOST").unwrap_or("redis-main".into())
}

fn read_redis_centralized_port() -> u16 {
  read_env_var("FRED_REDIS_CENTRALIZED_PORT")
    .and_then(|s| s.parse::<u16>().ok())
    .unwrap_or(6383)
}

fn read_redis_password() -> String {
  read_env_var("REDIS_PASSWORD").expect("Failed to read REDIS_PASSWORD env")
}

fn read_redis_username() -> String {
  read_env_var("REDIS_USERNAME").expect("Failed to read REDIS_USERNAME env")
}

/// A key that's only used by one test, so tests can run in parallel and failed runs don't leave state behind that
/// breaks later runs.
fn test_key(test: &str) -> String {
  format!("redis-protocol:tests:{test}")
}

async fn resp2_del(socket: &mut Framed<TcpStream, Resp2>, key: &str) {
  socket.send(resp2_encode_command(&format!("DEL {key}"))).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.kind(), Resp2FrameKind::Integer);
}

async fn resp3_del(socket: &mut Framed<TcpStream, Resp3>, key: &str) {
  socket.send(resp3_encode_command(&format!("DEL {key}"))).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.kind(), Resp3FrameKind::Number);
}

async fn connect_resp2(int_as_bulkstring: bool) -> Framed<TcpStream, Resp2> {
  let addr = format!("{}:{}", read_redis_centralized_host(), read_redis_centralized_port());
  debug!("Connecting to {addr}");
  let socket = TcpStream::connect(addr).await.unwrap();
  Framed::new(socket, Resp2::new(int_as_bulkstring))
}

async fn connect_resp3(int_as_blobstring: bool) -> Framed<TcpStream, Resp3> {
  let addr = format!("{}:{}", read_redis_centralized_host(), read_redis_centralized_port());
  debug!("Connecting to {addr}");
  let socket = TcpStream::connect(addr).await.unwrap();
  Framed::new(socket, Resp3::new(int_as_blobstring))
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_ping() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2(false).await;

  let ping = Resp2BytesFrame::Array(vec![Resp2BytesFrame::BulkString("PING".into())]);
  socket.send(ping).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "PONG")
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_borrowed_ping_incr() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2(true).await;
  let key = test_key("should_use_resp2_codec_borrowed_ping_incr");
  resp2_del(&mut socket, &key).await;

  let inner = vec![Resp2BorrowedFrame::BulkString("PING".as_bytes())];
  let ping = Resp2BorrowedFrame::Array(&inner);
  socket.send(ping).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "PONG");

  let inner = vec![
    Resp2BorrowedFrame::BulkString("INCRBY".as_bytes()),
    Resp2BorrowedFrame::BulkString(key.as_bytes()),
    Resp2BorrowedFrame::BulkString("2".as_bytes()),
  ];
  let incr = Resp2BorrowedFrame::Array(&inner);
  socket.send(incr).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response, Resp2BytesFrame::Integer(2));

  resp2_del(&mut socket, &key).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_get_set() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2(false).await;
  let key = test_key("should_use_resp2_codec_get_set");
  resp2_del(&mut socket, &key).await;

  let get = resp2_encode_command(&format!("GET {key}"));
  socket.send(get).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.kind(), Resp2FrameKind::Null);

  let set = resp2_encode_command(&format!("SET {key} bar"));
  socket.send(set).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let get = resp2_encode_command(&format!("GET {key}"));
  socket.send(get).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "bar");

  let del = resp2_encode_command(&format!("DEL {key}"));
  socket.send(del).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_hgetall() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2(false).await;
  let key = test_key("should_use_resp2_codec_hgetall");
  resp2_del(&mut socket, &key).await;

  let hmset = resp2_encode_command(&format!("HMSET {key} a b c d"));
  socket.send(hmset).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let hgetall = resp2_encode_command(&format!("HGETALL {key}"));
  socket.send(hgetall).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(
    response,
    Resp2BytesFrame::Array(vec![
      Resp2BytesFrame::BulkString("a".into()),
      Resp2BytesFrame::BulkString("b".into()),
      Resp2BytesFrame::BulkString("c".into()),
      Resp2BytesFrame::BulkString("d".into()),
    ])
  );

  let del = resp2_encode_command(&format!("DEL {key}"));
  socket.send(del).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp3_codec_hello() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp3(false).await;

  let hello = Resp3BytesFrame::Hello {
    version: RespVersion::RESP3,
    auth:    None,
    setname: None,
  };
  socket.send(hello).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert!(!matches!(
    response.kind(),
    Resp3FrameKind::SimpleError | Resp3FrameKind::BlobError
  ));

  let hello = Resp3BytesFrame::Hello {
    version: RespVersion::RESP3,
    auth:    Some(Box::new((read_redis_username().into(), read_redis_password().into()))),
    setname: None,
  };
  socket.send(hello).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert!(!matches!(
    response.kind(),
    Resp3FrameKind::SimpleError | Resp3FrameKind::BlobError
  ));

  let hello = Resp3BytesFrame::Hello {
    version: RespVersion::RESP3,
    auth:    Some(Box::new((read_redis_username().into(), read_redis_password().into()))),
    setname: Some("baz".into()),
  };
  socket.send(hello).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert!(!matches!(
    response.kind(),
    Resp3FrameKind::SimpleError | Resp3FrameKind::BlobError
  ));
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp3_codec_get_set() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp3(false).await;

  let hello = Resp3BytesFrame::Hello {
    version: RespVersion::RESP3,
    auth:    None,
    setname: None,
  };
  socket.send(hello).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert!(!matches!(
    response.kind(),
    Resp3FrameKind::SimpleError | Resp3FrameKind::BlobError
  ));
  let key = test_key("should_use_resp3_codec_get_set");
  resp3_del(&mut socket, &key).await;

  let get = resp3_encode_command(&format!("GET {key}"));
  socket.send(get).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.kind(), Resp3FrameKind::Null);

  let set = resp3_encode_command(&format!("SET {key} bar"));
  socket.send(set).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let get = resp3_encode_command(&format!("GET {key}"));
  socket.send(get).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "bar");

  let del = resp3_encode_command(&format!("DEL {key}"));
  socket.send(del).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp3_codec_hgetall() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp3(false).await;

  let hello = Resp3BytesFrame::Hello {
    version: RespVersion::RESP3,
    auth:    None,
    setname: None,
  };
  socket.send(hello).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert!(!matches!(
    response.kind(),
    Resp3FrameKind::SimpleError | Resp3FrameKind::BlobError
  ));
  let key = test_key("should_use_resp3_codec_hgetall");
  resp3_del(&mut socket, &key).await;

  let hmset = resp3_encode_command(&format!("HMSET {key} a b c d"));
  socket.send(hmset).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let mut expected = FrameMap::new();
  expected.insert(
    Resp3BytesFrame::BlobString {
      data:       "a".into(),
      attributes: None,
    },
    Resp3BytesFrame::BlobString {
      data:       "b".into(),
      attributes: None,
    },
  );
  expected.insert(
    Resp3BytesFrame::BlobString {
      data:       "c".into(),
      attributes: None,
    },
    Resp3BytesFrame::BlobString {
      data:       "d".into(),
      attributes: None,
    },
  );
  let expected = Resp3BytesFrame::Map {
    data:       expected,
    attributes: None,
  };

  let hgetall = resp3_encode_command(&format!("HGETALL {key}"));
  socket.send(hgetall).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response, expected);

  let del = resp3_encode_command(&format!("DEL {key}"));
  socket.send(del).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}
