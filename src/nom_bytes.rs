use bytes::buf::IntoIter;
use bytes::BytesMut;
use nom::bitvec::view::AsBits;
use nom::error::{ErrorKind, ParseError};
use nom::{AsBytes, CompareResult, IResult, InputLength, Needed};
use std::iter::{Copied, Enumerate, Map};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Range, RangeFrom, RangeFull, RangeTo};
use std::slice::Iter;
use std::str;

#[derive(Eq, PartialEq, Debug, Clone)]
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
    // why...
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
    buf.split_to(range.end + 1).into()
  }
}

impl nom::Slice<RangeTo<usize>> for NomBytesMut {
  fn slice(&self, range: RangeTo<usize>) -> Self {
    let mut buf = self.clone();
    buf.split_to(range.end + 1).into()
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
