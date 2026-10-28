//! Round-trip tests for Lean strings containing NUL bytes.
//!
//! Lean strings may contain U+0000 (for example the literal `"a\x00b"`), and
//! the `.olean` stores such a string with its interior NUL bytes followed by
//! the usual NUL terminator. These tests build minimal `.olean` files in
//! memory, compress and decompress them, and compare the result with the
//! original bytes.

use std::io::Cursor;

/// Object bases are multiples of 64 KiB.
const BASE: u64 = 1 << 16;
/// Size of the version 2 `.olean` header.
const HEADER_SIZE: usize = 96;

const TAG_ARRAY: u8 = 246;
const TAG_STRING: u8 = 249;

// lgz opcodes, as in `src/lgz.rs`
const STRING: u8 = 0x30;
const SIZED_STRING: u8 = 0xb0;
const UINT0: u8 = 0xd0;
const INT1: u8 = 0xf8;
const INT2: u8 = 0xf9;
const SAVE: u8 = 0xff;
const EXPRISH: u8 = 0x80;
const EXPRISH_NAME_STR: u8 = 0x00;
const EXPRISH_EXPR_LIT: u8 = 0x0b;

/// The lgz encoding of a nonnegative integer below 2^15.
fn uint(n: usize) -> Vec<u8> {
  match n {
    0..32 => vec![UINT0 + n as u8],
    32..128 => vec![INT1, n as u8],
    128..32768 => [&[INT2][..], &(n as u16).to_le_bytes()].concat(),
    _ => unimplemented!(),
  }
}

/// `leangz::lgz::str_hash` reads its argument as `u64` words, so it needs
/// the bytes 8-byte aligned, as they are in an `.olean`.
fn str_hash(bytes: &[u8]) -> u64 {
  let mut words = vec![0u64; bytes.len() / 8 + 1];
  let aligned = zerocopy::IntoBytes::as_mut_bytes(&mut words[..]);
  aligned[..bytes.len()].copy_from_slice(bytes);
  leangz::lgz::str_hash(&aligned[..bytes.len()])
}

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

  fn header(&mut self, tag: u8) { self.header_with(tag, 1, 0) }

  fn header_with(&mut self, tag: u8, cs_sz: u16, other: u8) {
    self.buf.extend_from_slice(&0u32.to_le_bytes()); // rc
    self.buf.extend_from_slice(&cs_sz.to_le_bytes());
    self.buf.push(other); // num_fields
    self.buf.push(tag);
  }

  /// Appends a constructor object with object fields `fields` and 8-byte
  /// scalar fields `scalars`.
  fn ctor(&mut self, tag: u8, fields: &[u64], scalars: &[u64]) -> u64 {
    let ptr = self.ptr();
    let cs_sz = 8 * (1 + fields.len() + scalars.len());
    self.header_with(tag, cs_sz as u16, fields.len() as u8);
    for &f in fields.iter().chain(scalars) {
      self.u64(f)
    }
    ptr
  }

  fn u64(&mut self, n: u64) { self.buf.extend_from_slice(&n.to_le_bytes()) }

  fn pad(&mut self) { self.buf.resize(self.buf.len().next_multiple_of(8), 0) }

  /// Appends a string object laid out as Lean's compactor writes it: `size`
  /// and `capacity` count the bytes including the terminator, `length` counts
  /// the code points excluding it.
  fn string(&mut self, s: &str) -> u64 {
    let ptr = self.ptr();
    self.header(TAG_STRING);
    let size = s.len() as u64 + 1;
    self.u64(size);
    self.u64(size);
    self.u64(s.chars().count() as u64);
    self.buf.extend_from_slice(s.as_bytes());
    self.buf.push(0);
    self.pad();
    ptr
  }

  fn array(&mut self, elems: &[u64]) -> u64 {
    let ptr = self.ptr();
    self.header(TAG_ARRAY);
    self.u64(elems.len() as u64);
    self.u64(elems.len() as u64);
    for &e in elems {
      self.u64(e)
    }
    ptr
  }

  fn finish(mut self, root: u64) -> Vec<u8> {
    self.buf[HEADER_SIZE - 8..HEADER_SIZE].copy_from_slice(&root.to_le_bytes());
    self.buf
  }
}

/// Compresses `olean`, checks that decompressing gives it back, and returns
/// the lgz stream.
fn roundtrip(olean: &[u8]) -> Vec<u8> {
  let mut compressed = vec![];
  leangz::lgz::compress(&[olean], &mut compressed);
  let (buf, ranges) = leangz::lgz::decompress(Cursor::new(&compressed), 1);
  assert_eq!(ranges.len(), 1);
  assert_eq!(olean, &buf[ranges[0].clone()]);
  compressed
}

fn single_string(s: &str) -> Vec<u8> {
  let mut b = OleanBuilder::new();
  let root = b.string(s);
  b.finish(root)
}

#[test]
fn test_strings_with_nul_roundtrip() {
  for s in [
    "\0",
    "\0abc",
    "abc\0",
    "a\0b",
    "\0\0",
    "a\0\0b\0c\0",
    "\u{e9}\0\u{65e5}\u{672c}\0\u{1f600}",
    "\0\u{1f600}\0",
    "a fairly long string, longer than thirty-two bytes\0with a NUL in it",
    &"a string longer than 127 bytes, whose length takes two bytes\0".repeat(3),
  ] {
    let compressed = roundtrip(&single_string(s));
    let mut expected = vec![SIZED_STRING];
    expected.extend_from_slice(&uint(s.len()));
    expected.extend_from_slice(s.as_bytes());
    assert!(compressed.ends_with(&expected), "unexpected encoding of {s:?}");
  }
}

#[test]
fn test_strings_without_nul_keep_encoding() {
  for s in ["", "Lean", "\u{e9}\u{65e5}\u{1f600}"] {
    let compressed = roundtrip(&single_string(s));
    let mut expected = vec![STRING];
    expected.extend_from_slice(s.as_bytes());
    expected.push(0);
    assert!(compressed.ends_with(&expected), "unexpected encoding of {s:?}");
  }
}

/// A string referenced twice is written once and then referenced by a
/// backref, which must work for the new encoding as well.
#[test]
fn test_shared_string_with_nul_roundtrip() {
  let mut b = OleanBuilder::new();
  let s1 = b.string("x\0y");
  let s2 = b.string("plain");
  let root = b.array(&[s1, s2, s1, s2]);
  let compressed = roundtrip(&b.finish(root));
  assert!(compressed.windows(2).any(|w| w == [SAVE, SIZED_STRING]));
}

/// A `Name.str` whose string contains NUL keeps the exprish encoding: the
/// compressor checks the name's cached hash, which covers the NUL bytes.
#[test]
fn test_name_with_nul_roundtrip() {
  let s = "a\0b";
  let mut b = OleanBuilder::new();
  let sp = b.string(s);
  // `Name.str .anonymous s`, with `.anonymous` the boxed scalar 0
  let hash = leangz::lgz::mix_hash(leangz::lgz::NAME_ANON_HASH, str_hash(s.as_bytes()));
  let root = b.ctor(1, &[1, sp], &[hash]);
  let compressed = roundtrip(&b.finish(root));
  let mut expected = vec![EXPRISH, EXPRISH_NAME_STR, UINT0, SIZED_STRING];
  expected.extend_from_slice(&uint(s.len()));
  expected.extend_from_slice(s.as_bytes());
  assert!(compressed.ends_with(&expected));
}

/// So does the string literal `Expr.lit (.strVal s)` that `"a\x00b"` in Lean
/// source elaborates to.
#[test]
fn test_string_literal_with_nul_roundtrip() {
  let s = "a\0b";
  let mut b = OleanBuilder::new();
  let sp = b.string(s);
  let lit = b.ctor(1, &[sp], &[]); // Literal.strVal
                                   // Expr.lit's cached data: the low 32 bits of mix_hash(3, hash of the literal)
  let data = leangz::lgz::mix_hash(3, str_hash(s.as_bytes())) & 0xffff_ffff;
  let root = b.ctor(9, &[lit], &[data]);
  let compressed = roundtrip(&b.finish(root));
  let mut expected = vec![EXPRISH, EXPRISH_EXPR_LIT];
  expected.extend_from_slice(&[1 << 4 | 1 << 1, SIZED_STRING]); // packed Literal.strVal
  expected.extend_from_slice(&uint(s.len()));
  expected.extend_from_slice(s.as_bytes());
  assert!(compressed.ends_with(&expected));
}
