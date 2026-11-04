//! Round-trip tests for constructor objects that have the shape of an `Expr`
//! constructor without being expressions.
//!
//! The compressor recognises `Expr.lit` and `Expr.const` by their shape (tag,
//! number of object fields, number of scalar words) and then computes the
//! hash they would cache from their fields. These tests build minimal
//! `.olean` files in memory holding user values of those shapes, as Lean
//! code can produce them, and check that they round-trip.

use std::io::Cursor;
use std::sync::mpsc;
use std::time::Duration;

/// Object bases are multiples of 64 KiB.
const BASE: u64 = 1 << 16;
/// Size of the version 2 `.olean` header.
const HEADER_SIZE: usize = 96;

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

  /// Appends a constructor object with object fields `fields` and 8-byte
  /// scalar fields `scalars`, and returns its address.
  fn ctor(&mut self, tag: u8, fields: &[u64], scalars: &[u64]) -> u64 {
    let ptr = BASE + self.buf.len() as u64;
    let cs_sz = 8 * (1 + fields.len() + scalars.len());
    self.buf.extend_from_slice(&0u32.to_le_bytes()); // rc
    self.buf.extend_from_slice(&(cs_sz as u16).to_le_bytes());
    self.buf.push(fields.len() as u8);
    self.buf.push(tag);
    for &f in fields.iter().chain(scalars) {
      self.buf.extend_from_slice(&f.to_le_bytes())
    }
    ptr
  }

  fn finish(mut self, root: u64) -> Vec<u8> {
    self.buf[HEADER_SIZE - 8..HEADER_SIZE].copy_from_slice(&root.to_le_bytes());
    self.buf
  }
}

/// A boxed scalar, as Lean represents small `Nat`s and enum-like
/// constructors.
fn boxed(n: u64) -> u64 { (n << 1) | 1 }

/// Compresses and decompresses `olean` on another thread, and checks that
/// this gives back `olean` within 10 seconds, so that a compressor that
/// loops fails the test instead of hanging the test suite.
fn roundtrip(olean: Vec<u8>) {
  let (tx, rx) = mpsc::channel();
  std::thread::spawn(move || {
    let mut compressed = vec![];
    leangz::lgz::compress(&[&olean], &mut compressed);
    let (buf, ranges) = leangz::lgz::decompress(Cursor::new(&compressed), 1);
    let _ = tx.send(olean == buf[ranges[0].clone()]);
  });
  match rx.recv_timeout(Duration::from_secs(10)) {
    Ok(same) => assert!(same, "the round trip changed the .olean"),
    Err(mpsc::RecvTimeoutError::Timeout) => panic!("the round trip did not finish in 10 s"),
    Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the round trip panicked"),
  }
}

/// A tag 9 constructor with one object field and one scalar word, the shape
/// of `Expr.lit`, whose object field is a tag 0 constructor without object
/// fields, the shape of a boxed `UInt64`.  In Lean,
/// `inductive T (α : Type) | c0 | c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | c9 (a : α) (y : UInt64)`
/// gives this for `(.c9 1000000 7 : T UInt64)`.
#[test]
fn test_lit_shape_roundtrip() {
  let mut b = OleanBuilder::new();
  let a = b.ctor(0, &[], &[1000000]);
  let root = b.ctor(9, &[a], &[7]);
  roundtrip(b.finish(root));
}

/// A tag 4 constructor with two object fields and one scalar word, the shape
/// of `Expr.const`, whose first field is the boxed 0 (which reads as
/// `Name.anonymous`) and whose second field, where `Expr.const` has its list
/// of universe levels, is a tag 1 constructor with one object field, the
/// shape of `some x`.  In Lean,
/// `inductive U (α β : Type) | c0 | c1 | c2 | c3 | c4 (a : α) (b : β) (y : UInt64)`
/// gives this for `(.c4 0 (some 5) 7 : U Nat (Option Nat))`.
#[test]
fn test_const_shape_roundtrip() {
  let mut b = OleanBuilder::new();
  let some5 = b.ctor(1, &[boxed(5)], &[]);
  let root = b.ctor(4, &[boxed(0), some5], &[7]);
  roundtrip(b.finish(root));
}
