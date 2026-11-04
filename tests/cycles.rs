//! Compressing `.olean` files whose objects point to each other in a cycle,
//! which Lean never writes: the compressor stops with a panic instead of
//! running forever.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::time::Duration;

/// Object bases are multiples of 64 KiB.
const BASE: u64 = 1 << 16;
/// Size of the version 2 `.olean` header.
const HEADER_SIZE: usize = 96;

fn header() -> Vec<u8> {
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
  buf
}

/// Appends a constructor object with object fields `fields` and 8-byte
/// scalar fields `scalars`.
fn ctor(buf: &mut Vec<u8>, tag: u8, fields: &[u64], scalars: &[u64]) -> u64 {
  let ptr = BASE + buf.len() as u64;
  let cs_sz = 8 * (1 + fields.len() + scalars.len());
  buf.extend_from_slice(&0u32.to_le_bytes()); // rc
  buf.extend_from_slice(&(cs_sz as u16).to_le_bytes());
  buf.push(fields.len() as u8);
  buf.push(tag);
  for &f in fields.iter().chain(scalars) {
    buf.extend_from_slice(&f.to_le_bytes());
  }
  ptr
}

fn finish(mut buf: Vec<u8>, root: u64) -> Vec<u8> {
  buf[HEADER_SIZE - 8..HEADER_SIZE].copy_from_slice(&root.to_le_bytes());
  buf
}

/// Compresses `olean` on another thread and checks that it panics within
/// a few seconds.
fn assert_compress_panics(olean: Vec<u8>) {
  let (tx, rx) = mpsc::channel();
  std::thread::spawn(move || {
    let r = catch_unwind(AssertUnwindSafe(|| leangz::lgz::compress(&[&olean], &mut vec![])));
    let _ = tx.send(r.is_err());
  });
  match rx.recv_timeout(Duration::from_secs(10)) {
    Ok(panicked) => assert!(panicked, "compressed a cyclic object graph"),
    Err(_) => panic!("compress still running after 10 s"),
  }
}

/// A list cell whose tail is itself.
#[test]
fn test_cyclic_list() {
  let mut buf = header();
  let cell = BASE + buf.len() as u64;
  ctor(&mut buf, 1, &[1, cell], &[]);
  assert_compress_panics(finish(buf, cell));
}

/// An `Expr.const` whose universe level list is a cell whose tail is itself,
/// which the compressor follows to recompute the hash.
#[test]
fn test_cyclic_level_list() {
  let mut buf = header();
  let cell = BASE + buf.len() as u64;
  ctor(&mut buf, 1, &[1, cell], &[]); // List.cons Level.zero <itself>
  let root = ctor(&mut buf, 4, &[1, cell], &[0]); // Expr.const Name.anonymous <cell>
  assert_compress_panics(finish(buf, root));
}
