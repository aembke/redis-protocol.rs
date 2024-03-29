use futures::{SinkExt, StreamExt};
use redis_protocol::{
  codec::{resp2_encode_command, resp3_encode_command, Resp2, Resp3},
  resp2::types::{BytesFrame as Resp2BytesFrame, FrameKind as Resp2FrameKind, Resp2Frame},
  resp3::types::{BytesFrame as Resp3BytesFrame, FrameKind as Resp3FrameKind, FrameMap, Resp3Frame, RespVersion},
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

async fn connect_resp2() -> Framed<TcpStream, Resp2> {
  let addr = format!("{}:{}", read_redis_centralized_host(), read_redis_centralized_port());
  debug!("Connecting to {}", addr);
  let socket = TcpStream::connect(addr).await.unwrap();
  Framed::new(socket, Resp2)
}

async fn connect_resp3() -> Framed<TcpStream, Resp3> {
  let addr = format!("{}:{}", read_redis_centralized_host(), read_redis_centralized_port());
  debug!("Connecting to {}", addr);
  let socket = TcpStream::connect(addr).await.unwrap();
  Framed::new(socket, Resp3::default())
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_ping() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2().await;

  let ping = Resp2BytesFrame::Array(vec![Resp2BytesFrame::BulkString("PING".into())]);
  socket.send(ping).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "PONG")
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_get_set() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2().await;

  let get_foo = resp2_encode_command("GET foo");
  socket.send(get_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.kind(), Resp2FrameKind::Null);

  let set_foo = resp2_encode_command("SET foo bar");
  socket.send(set_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let get_foo = resp2_encode_command("GET foo");
  socket.send(get_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "bar");

  let del_foo = resp2_encode_command("DEL foo");
  socket.send(del_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp2_codec_hgetall() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp2().await;

  let hmset = resp2_encode_command("HMSET foo a b c d");
  socket.send(hmset).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let hgetall = resp2_encode_command("HGETALL foo");
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

  let del_foo = resp2_encode_command("DEL foo");
  socket.send(del_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp3_codec_hello() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp3().await;

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
    auth:    Some((read_redis_username().into(), read_redis_password().into())),
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
    auth:    Some((read_redis_username().into(), read_redis_password().into())),
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
  let mut socket = connect_resp3().await;

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

  let get_foo = resp3_encode_command("GET foo");
  socket.send(get_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.kind(), Resp3FrameKind::Null);

  let set_foo = resp3_encode_command("SET foo bar");
  socket.send(set_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "OK");

  let get_foo = resp3_encode_command("GET foo");
  socket.send(get_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.as_str().unwrap(), "bar");

  let del_foo = resp3_encode_command("DEL foo");
  socket.send(del_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}

#[tokio::test(flavor = "multi_thread")]
async fn should_use_resp3_codec_hgetall() {
  let _ = pretty_env_logger::try_init();
  let mut socket = connect_resp3().await;

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

  let hmset = resp3_encode_command("HMSET foo a b c d");
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

  let hgetall = resp3_encode_command("HGETALL foo");
  socket.send(hgetall).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response, expected);

  let del_foo = resp3_encode_command("DEL foo");
  socket.send(del_foo).await.unwrap();
  let response = socket.next().await.unwrap().unwrap();
  assert_eq!(response.to_string().unwrap(), "1");
}
