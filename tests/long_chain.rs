//! Round-trip tests for `.olean` files with very long chains of objects.
//!
//! A linked list with a million cells is a chain of a million objects, each
//! pointing to the next.  These tests build such `.olean` files in memory,
//! compress and decompress them, and compare the result with the original
//! bytes, on the 2 MiB stack of a test thread.

use std::io::Cursor;

/// Object bases are multiples of 64 KiB.
const BASE: u64 = 1 << 16;
/// Size of the version 2 `.olean` header.
const HEADER_SIZE: usize = 96;

const TAG_ARRAY: u8 = 246;
const TAG_STRING: u8 = 249;
const TAG_THUNK: u8 = 251;

const LEN: u64 = 1_000_000;

/// A boxed scalar.
fn boxed(n: u64) -> u64 { (n << 1) | 1 }

struct OleanBuilder {
  buf: Vec<u8>,
}

impl OleanBuilder {
  fn new() -> Self {
    let mut buf = vec![];
    buf.extend_from_slice(b"olean");
    buf.push(2); // version
    buf.push(1); // flags: GMP
    let mut lean_version = [0u8; 33];
    lean_version[..6].copy_from_slice(b"4.34.1");
    buf.extend_from_slice(&lean_version);
    buf.extend_from_slice(&[b'0'; 40]); // githash
    buf.extend_from_slice(&BASE.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // root, set by `finish`
    assert_eq!(buf.len(), HEADER_SIZE);
    OleanBuilder { buf }
  }

  fn ptr(&self) -> u64 { BASE + self.buf.len() as u64 }

  fn u64(&mut self, n: u64) { self.buf.extend_from_slice(&n.to_le_bytes()) }

  fn header(&mut self, tag: u8, cs_sz: u16, num_fields: u8) {
    self.buf.extend_from_slice(&0u32.to_le_bytes()); // rc
    self.buf.extend_from_slice(&cs_sz.to_le_bytes());
    self.buf.push(num_fields);
    self.buf.push(tag);
  }

  /// Appends a constructor object with object fields `fields` and 8-byte
  /// scalar fields `scalars`.
  fn ctor(&mut self, tag: u8, fields: &[u64], scalars: &[u64]) -> u64 {
    let ptr = self.ptr();
    let cs_sz = 8 * (1 + fields.len() + scalars.len());
    self.header(tag, cs_sz as u16, fields.len() as u8);
    for &f in fields.iter().chain(scalars) {
      self.u64(f)
    }
    ptr
  }

  fn array(&mut self, elems: &[u64]) -> u64 {
    let ptr = self.ptr();
    self.header(TAG_ARRAY, 1, 0);
    self.u64(elems.len() as u64);
    self.u64(elems.len() as u64);
    for &e in elems {
      self.u64(e)
    }
    ptr
  }

  fn thunk(&mut self, value: u64) -> u64 {
    let ptr = self.ptr();
    self.header(TAG_THUNK, 3 << 3, 0);
    self.u64(value);
    self.u64(0);
    ptr
  }

  /// Appends a string object of ASCII characters, laid out as Lean's
  /// compactor writes it.
  fn string(&mut self, s: &str) -> u64 {
    let ptr = self.ptr();
    self.header(TAG_STRING, 1, 0);
    let size = s.len() as u64 + 1;
    self.u64(size);
    self.u64(size);
    self.u64(s.len() as u64);
    self.buf.extend_from_slice(s.as_bytes());
    self.buf.push(0);
    self.buf.resize(self.buf.len().next_multiple_of(8), 0);
    ptr
  }

  fn finish(mut self, root: u64) -> Vec<u8> {
    self.buf[HEADER_SIZE - 8..HEADER_SIZE].copy_from_slice(&root.to_le_bytes());
    self.buf
  }
}

/// Compresses and decompresses `olean`, checks that this gives back the same
/// bytes, and returns the size of the lgz stream.
fn roundtrip(olean: &[u8]) -> usize {
  let mut lgz = vec![];
  leangz::lgz::compress(&[olean], &mut lgz);
  let (buf, ranges) = leangz::lgz::decompress(Cursor::new(&lgz), 1);
  assert_eq!(ranges.len(), 1);
  assert!(buf[ranges[0].clone()] == *olean, "round trip differs");
  lgz.len()
}

/// `List.range LEN`: a cons cell (tag 1) holds the head and the tail.
#[test]
fn test_long_list_roundtrip() {
  let mut b = OleanBuilder::new();
  let mut list = boxed(0); // List.nil
  for i in (0..LEN).rev() {
    list = b.ctor(1, &[boxed(i), list], &[]);
  }
  roundtrip(&b.finish(list));
}

/// Arrays nested `LEN` deep, each holding the next.
#[test]
fn test_nested_arrays_roundtrip() {
  let mut b = OleanBuilder::new();
  let mut array = b.array(&[]);
  for _ in 0..LEN {
    array = b.array(&[array]);
  }
  roundtrip(&b.finish(array));
}

/// A chain of `LEN` thunks, each holding the next.
#[test]
fn test_nested_thunks_roundtrip() {
  let mut b = OleanBuilder::new();
  let mut thunk = boxed(0);
  for _ in 0..LEN {
    thunk = b.thunk(thunk);
  }
  roundtrip(&b.finish(thunk));
}

/// A `Name` with `LEN` components, which the compressor encodes in exprish
/// mode.  All components share one string, which it encodes as a backref.
#[test]
fn test_long_name_roundtrip() {
  let mut b = OleanBuilder::new();
  let s = b.string("a");
  // `str_hash` reads its argument as `u64` words, so it needs it aligned
  let s_hash = leangz::lgz::str_hash(&zerocopy::IntoBytes::as_bytes(&[b'a' as u64])[..1]);
  let mut name = boxed(0); // Name.anonymous
  let mut hash = leangz::lgz::NAME_ANON_HASH;
  for _ in 0..LEN {
    hash = leangz::lgz::mix_hash(hash, s_hash);
    name = b.ctor(1, &[name, s], &[hash]); // Name.str
  }
  // two bytes per component: NAME_STR and the backref to the string, where
  // a normal constructor would take at least ten
  assert!(roundtrip(&b.finish(name)) < 3 * LEN as usize, "not encoded in exprish mode");
}
