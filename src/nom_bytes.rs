use bytes::buf::{Chain, IntoIter, Reader, Take};
use bytes::{Buf, Bytes, BytesMut};
use nom::{
  AsBytes, FindSubstring, InputIter, InputLength, InputTake, InputTakeAtPosition, Needed, Offset, Slice,
  UnspecializedInput,
};
use std::fmt::{Debug, Display};
use std::io::IoSlice;
use std::iter::Enumerate;
use std::ops::{Deref, DerefMut, Range, RangeFrom, RangeFull, RangeTo};
#[cfg(feature = "decode-logs")]
use std::str;

/// A wrapper type for `BytesMut` that implements the Nom traits necessary to operate on BytesMut slices directly with nom functions.
///
/// This allows for the caller to read from BytesMut without copying any of the data as its being read. It also allows for parsing
/// functions to return Frame types that contain "owned" views into sections of the buffer without copying or even moving any of the
/// underlying data.
///
/// To do this we implement custom slicing functions that rely on the BytesMut `split_to` function and the fact that cloning a
/// `BytesMut` simply increments a ref count in order to create a new view into the buffer.
///
/// What this means in practice - given the following input buffer:
///
/// "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
///
/// and the corresponding frames in RESP2:
///
/// ```rust ignore
/// Frame::Array(vec![
///   Frame::BulkString("foo".into(),
///   Frame::BulkString("bar".into()
/// ])
/// ```
///
/// If the above frames were produced by parsing a `BytesMut` containing the above input bytes then the `BytesMut` inside the first
/// `BulkString` would simply be a shallow view into the input buffer starting at index 7, etc. This works because the relatively cheap
/// `Clone` impl on `BytesMut` allows for removing any lifetimes and the issues that come with using those for this kind of use case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NomBytes(Bytes);

impl NomBytes {
  pub fn into_bytes(self) -> Bytes {
    self.0
  }
}

impl From<Bytes> for NomBytes {
  fn from(buf: Bytes) -> Self {
    NomBytes(buf)
  }
}

impl<'a> From<&'a Bytes> for NomBytes {
  fn from(buf: &'a Bytes) -> Self {
    NomBytes(buf.clone())
  }
}

impl Deref for NomBytes {
  type Target = Bytes;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for NomBytes {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl AsRef<NomBytes> for NomBytes {
  fn as_ref(&self) -> &NomBytes {
    &self
  }
}

impl InputTake for NomBytes {
  fn take(&self, count: usize) -> Self {
    // operate on a shallow copy and split off the bytes to create a new shallow window into the bytes
    self.0.clone().split_to(count).into()
  }

  fn take_split(&self, count: usize) -> (Self, Self) {
    let mut right = self.clone();
    let left = right.split_to(count);
    // i wish this was at least called out in the docs
    (right, left.into())
  }
}

impl InputLength for NomBytes {
  fn input_len(&self) -> usize {
    self.0.len()
  }
}

impl<'a> FindSubstring<&'a [u8]> for NomBytes {
  fn find_substring(&self, substr: &'a [u8]) -> Option<usize> {
    decode_log!(
      "FindSubstring in {:?}. Offset: {:?}, len: {}",
      str::from_utf8(&self).ok(),
      self.0.as_bytes().find_substring(substr),
      self.len()
    );
    self.0.as_bytes().find_substring(substr)
  }
}

impl UnspecializedInput for NomBytes {}

impl InputIter for NomBytes {
  type Item = u8;
  type Iter = Enumerate<Self::IterElem>;
  type IterElem = IntoIter<Bytes>;

  fn iter_indices(&self) -> Self::Iter {
    self.iter_elements().enumerate()
  }

  fn iter_elements(&self) -> Self::IterElem {
    self.0.clone().into_iter()
  }

  fn position<P>(&self, predicate: P) -> Option<usize>
  where
    P: Fn(Self::Item) -> bool,
  {
    self.0.as_bytes().position(predicate)
  }

  fn slice_index(&self, count: usize) -> Result<usize, Needed> {
    self.0.as_bytes().slice_index(count)
  }
}

impl Offset for NomBytes {
  fn offset(&self, second: &Self) -> usize {
    self.0.as_bytes().offset(second.0.as_bytes())
  }
}

impl Slice<Range<usize>> for NomBytes {
  fn slice(&self, range: Range<usize>) -> Self {
    let mut buf = self.clone();
    let _ = buf.split_to(range.start);
    buf.split_to(range.end).into()
  }
}

impl Slice<RangeTo<usize>> for NomBytes {
  fn slice(&self, range: RangeTo<usize>) -> Self {
    let mut buf = self.clone();
    buf.split_to(range.end).into()
  }
}

impl Slice<RangeFrom<usize>> for NomBytes {
  fn slice(&self, range: RangeFrom<usize>) -> Self {
    let mut buf = self.clone();
    let _ = buf.split_to(range.start);
    buf
  }
}

impl Slice<RangeFull> for NomBytes {
  fn slice(&self, _: RangeFull) -> Self {
    self.clone()
  }
}
