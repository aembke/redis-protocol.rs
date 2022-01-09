use bytes::BytesMut;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct NomBytesMut {
  inner: BytesMut,
}

impl From<BytesMut> for NomBytesMut {
  fn from(d: BytesMut) -> Self {
    NomBytesMut { inner: d }
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

impl nom::InputTake for NomBytesMut {
  fn take(&self, count: usize) -> Self {
    // operate on a shallow copy and split off the bytes to create a new shallow window into the bytes
    self.clone().split_to(count).into()
  }
  fn take_split(&self, count: usize) -> (Self, Self) {
    let mut right = self.clone();
    let left = right.split_to(count);
    (left.into(), right)
  }
}
