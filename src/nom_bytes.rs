use bytes::buf::IntoIter;
use bytes::BytesMut;
use nom::{AsBytes, Needed};
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
pub(crate) struct NomBytesMut {
  inner: BytesMut,
}

impl NomBytesMut {
  pub fn into_bytes(self) -> BytesMut {
    self.inner
  }
}

impl From<BytesMut> for NomBytesMut {
  fn from(buf: BytesMut) -> Self {
    NomBytesMut { inner: buf }
  }
}

impl<'a> From<&'a BytesMut> for NomBytesMut {
  fn from(buf: &'a BytesMut) -> Self {
    NomBytesMut { inner: buf.clone() }
  }
}

impl Deref for NomBytesMut {
  type Target = BytesMut;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl DerefMut for NomBytesMut {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}

impl AsRef<NomBytesMut> for NomBytesMut {
  fn as_ref(&self) -> &NomBytesMut {
    &self
  }
}

impl nom::InputTake for NomBytesMut {
  fn take(&self, count: usize) -> Self {
    // operate on a shallow copy and split off the bytes to create a new shallow window into the bytes
    self.clone().split_to(count).into()
  }

  fn take_split(&self, count: usize) -> (Self, Self) {
    let mut right = self.clone();
    let left = right.split_to(count);
    // i wish this was at least called out in the docs
    (right, left.into())
  }
}

impl nom::InputLength for NomBytesMut {
  fn input_len(&self) -> usize {
    self.inner.len()
  }
}

impl<'a> nom::FindSubstring<&'a [u8]> for NomBytesMut {
  fn find_substring(&self, substr: &'a [u8]) -> Option<usize> {
    decode_log!(
      "FindSubstring in {:?}. Offset: {:?}, len: {}",
      str::from_utf8(&self).ok(),
      self.inner.as_bytes().find_substring(substr),
      self.len()
    );
    self.inner.as_bytes().find_substring(substr)
  }
}

impl nom::UnspecializedInput for NomBytesMut {}

impl nom::InputIter for NomBytesMut {
  type Item = u8;
  type Iter = Enumerate<Self::IterElem>;
  type IterElem = IntoIter<BytesMut>;

  fn iter_indices(&self) -> Self::Iter {
    self.iter_elements().enumerate()
  }

  fn iter_elements(&self) -> Self::IterElem {
    self.inner.clone().into_iter()
  }

  fn position<P>(&self, predicate: P) -> Option<usize>
  where
    P: Fn(Self::Item) -> bool,
  {
    self.inner.as_bytes().position(predicate)
  }

  fn slice_index(&self, count: usize) -> Result<usize, Needed> {
    self.inner.as_bytes().slice_index(count)
  }
}

impl nom::Offset for NomBytesMut {
  fn offset(&self, second: &Self) -> usize {
    self.inner.as_bytes().offset(second.inner.as_bytes())
  }
}

impl nom::Slice<Range<usize>> for NomBytesMut {
  fn slice(&self, range: Range<usize>) -> Self {
    let mut buf = self.clone();
    let _ = buf.split_to(range.start);
    buf.split_to(range.end).into()
  }
}

impl nom::Slice<RangeTo<usize>> for NomBytesMut {
  fn slice(&self, range: RangeTo<usize>) -> Self {
    let mut buf = self.clone();
    buf.split_to(range.end).into()
  }
}

impl nom::Slice<RangeFrom<usize>> for NomBytesMut {
  fn slice(&self, range: RangeFrom<usize>) -> Self {
    let mut buf = self.clone();
    let _ = buf.split_to(range.start);
    buf
  }
}

impl nom::Slice<RangeFull> for NomBytesMut {
  fn slice(&self, _: RangeFull) -> Self {
    self.clone()
  }
}
