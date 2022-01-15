use crate::decode_mut::utils::hash_tuple;
use crate::resp3::types::{Auth, FrameKind, RespVersion, VerbatimStringFormat, NULL};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resp2IndexFrame {
  SimpleString { start: usize, end: usize },
  Error { start: usize, end: usize },
  Integer(i64),
  BulkString { start: usize, end: usize },
  Array(Vec<Resp2IndexFrame>),
  Null,
}

#[derive(Clone, Debug)]
pub enum Resp3IndexFrame {
  BlobString {
    data: (usize, usize),
    attributes: Option<(usize, usize)>,
  },
  BlobError {
    data: (usize, usize),
    attributes: Option<(usize, usize)>,
  },
  SimpleString {
    data: (usize, usize),
    attributes: Option<(usize, usize)>,
  },
  SimpleError {
    data: (usize, usize),
    attributes: Option<(usize, usize)>,
  },
  Boolean {
    data: bool,
    attributes: Option<(usize, usize)>,
  },
  Null,
  Number {
    data: i64,
    attributes: Option<(usize, usize)>,
  },
  Double {
    data: f64,
    attributes: Option<(usize, usize)>,
  },
  BigNumber {
    data: (usize, usize),
    attributes: Option<(usize, usize)>,
  },
  VerbatimString {
    data: (usize, usize),
    attributes: Option<(usize, usize)>,
    format: VerbatimStringFormat,
  },
  Array {
    data: Vec<Resp3IndexFrame>,
    attributes: Option<(usize, usize)>,
  },
  Map {
    data: HashMap<Resp3IndexFrame, Resp3IndexFrame>,
    attributes: Option<(usize, usize)>,
  },
  Set {
    data: HashSet<Resp3IndexFrame>,
    attributes: Option<(usize, usize)>,
  },
  Push {
    data: Vec<Resp3IndexFrame>,
    attributes: Option<(usize, usize)>,
  },
  Hello {
    version: RespVersion,
    auth: Option<Auth>,
  },
  ChunkedString((usize, usize)),
}

impl PartialEq for Resp3IndexFrame {
  fn eq(&self, other: &Self) -> bool {
    use self::Resp3IndexFrame::*;

    match *self {
      ChunkedString(ref b) => match *other {
        ChunkedString(ref _b) => b == _b,
        _ => false,
      },
      Array {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Array {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      BlobString {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          BlobString {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      SimpleString {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          SimpleString {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      SimpleError {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          SimpleError {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Number {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Number {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Null => match *other {
        Null => true,
        _ => false,
      },
      Boolean {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Boolean {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Double {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Double {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      BlobError {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          BlobError {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      VerbatimString {
        ref data,
        ref format,
        ref attributes,
      } => {
        let (_data, _format, _attributes) = (data, format, attributes);
        match *other {
          VerbatimString {
            ref data,
            ref format,
            ref attributes,
          } => _data == data && _format == format && attributes == _attributes,
          _ => false,
        }
      }
      Map {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Map {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Set {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Set {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Push {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          Push {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
      Hello { ref version, ref auth } => {
        let (_version, _auth) = (version, auth);
        match *other {
          Hello { ref version, ref auth } => _version == version && _auth == auth,
          _ => false,
        }
      }
      BigNumber {
        ref data,
        ref attributes,
      } => {
        let (_data, _attributes) = (data, attributes);
        match *other {
          BigNumber {
            ref data,
            ref attributes,
          } => data == _data && attributes == _attributes,
          _ => false,
        }
      }
    }
  }
}

impl Eq for Resp3IndexFrame {}

impl Hash for Resp3IndexFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    use self::Resp3IndexFrame::*;
    self.kind().hash_prefix().hash(state);

    match *self {
      BlobString { ref data, .. } => hash_tuple(state, data),
      SimpleString { ref data, .. } => hash_tuple(state, data),
      SimpleError { ref data, .. } => hash_tuple(state, data),
      Number { ref data, .. } => data.hash(state),
      Null => NULL.hash(state),
      Double { ref data, .. } => data.to_string().hash(state),
      Boolean { ref data, .. } => data.hash(state),
      BlobError { ref data, .. } => hash_tuple(state, data),
      VerbatimString {
        ref data, ref format, ..
      } => {
        format.hash(state);
        hash_tuple(state, data);
      }
      ChunkedString(ref data) => hash_tuple(state, data),
      BigNumber { ref data, .. } => hash_tuple(state, data),
      _ => panic!("Invalid RESP3 data type to use as hash key."),
    };
  }
}

impl Resp3IndexFrame {
  /// Read the associated `FrameKind`.
  pub fn kind(&self) -> FrameKind {
    use self::Resp3IndexFrame::*;

    match *self {
      Array { .. } => FrameKind::Array,
      BlobString { .. } => FrameKind::BlobString,
      SimpleString { .. } => FrameKind::SimpleString,
      SimpleError { .. } => FrameKind::SimpleError,
      Number { .. } => FrameKind::Number,
      Null => FrameKind::Null,
      Double { .. } => FrameKind::Double,
      BlobError { .. } => FrameKind::BlobError,
      VerbatimString { .. } => FrameKind::VerbatimString,
      Boolean { .. } => FrameKind::Boolean,
      Map { .. } => FrameKind::Map,
      Set { .. } => FrameKind::Set,
      Push { .. } => FrameKind::Push,
      Hello { .. } => FrameKind::Hello,
      BigNumber { .. } => FrameKind::BigNumber,
      ChunkedString(ref inner) => {
        if inner.1 - inner.0 == 0 {
          FrameKind::EndStream
        } else {
          FrameKind::ChunkedString
        }
      }
    }
  }
}
