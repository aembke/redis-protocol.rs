use bytes::buf::IntoIter;
use bytes::Bytes;
use nom::{AsBytes, FindSubstring, InputIter, InputLength, InputTake, Needed, Offset, Slice, UnspecializedInput};
use core::iter::Enumerate;
use core::ops::{Deref, DerefMut, Range, RangeFrom, RangeFull, RangeTo};
#[cfg(feature = "decode-logs")]
use core::str;

/// A wrapper type for `Bytes` that implements the Nom traits necessary to operate on Bytes slices directly with nom functions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NomBytes(Bytes);

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
    self.0.slice(range).into()
  }
}

impl Slice<RangeTo<usize>> for NomBytes {
  fn slice(&self, range: RangeTo<usize>) -> Self {
    self.0.slice(0..range.end).into()
  }
}

impl Slice<RangeFrom<usize>> for NomBytes {
  fn slice(&self, range: RangeFrom<usize>) -> Self {
    self.0.slice(range.start..).into()
  }
}

impl Slice<RangeFull> for NomBytes {
  fn slice(&self, _: RangeFull) -> Self {
    self.clone()
  }
}
