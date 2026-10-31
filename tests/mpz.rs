//! Round-trip tests for big numbers (MPZ objects), in both the GMP layout and
//! Lean's own layout, with either sign.
//!
//! Lean stores an `Int` or `Nat` that does not fit in a boxed scalar as an
//! MPZ object. These tests build minimal `.olean` files in memory, compress
//! and decompress them, and compare the result with the original bytes.

use std::io::Cursor;

/// Object bases are multiples of 64 KiB.
const BASE: u64 = 1 << 16;
/// Size of the version 2 `.olean` header.
const HEADER_SIZE: usize = 96;
const TAG_MPZ: u8 = 250;

fn olean(use_gmp: bool, object: impl FnOnce(u64, &mut Vec<u8>)) -> Vec<u8> {
  let mut buf = vec![];
  buf.extend_from_slice(b"olean");
  buf.push(2); // version
  buf.push(use_gmp as u8); // flags
  let mut lean_version = [0u8; 33];
  lean_version[..6].copy_from_slice(b"4.34.1");
  buf.extend_from_slice(&lean_version);
  buf.extend_from_slice(&[b'0'; 40]); // githash
  let root = BASE + HEADER_SIZE as u64;
  buf.extend_from_slice(&BASE.to_le_bytes());
  buf.extend_from_slice(&root.to_le_bytes());
  assert_eq!(buf.len(), HEADER_SIZE);
  object(root, &mut buf);
  buf
}

fn header(buf: &mut Vec<u8>, cs_sz: u16) {
  buf.extend_from_slice(&0u32.to_le_bytes()); // rc
  buf.extend_from_slice(&cs_sz.to_le_bytes());
  buf.push(0); // other
  buf.push(TAG_MPZ);
}

/// An MPZ object in the GMP layout (`__mpz_struct`): allocated limbs, signed
/// size, pointer to the limbs, limbs.
fn gmp_mpz(negative: bool, limbs: &[u64]) -> Vec<u8> {
  olean(true, |ptr, buf| {
    let n = limbs.len();
    header(buf, ((n + 3) << 3) as u16);
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    let size = if negative { -(n as i32) } else { n as i32 };
    buf.extend_from_slice(&size.to_le_bytes());
    buf.extend_from_slice(&(ptr + 24).to_le_bytes());
    limbs.iter().for_each(|l| buf.extend_from_slice(&l.to_le_bytes()));
  })
}

/// An MPZ object in Lean's own layout: sign, number of 32-bit digits, pointer
/// to the digits, digits padded to 8 bytes.
fn lean_mpz(negative: bool, digits: &[u32]) -> Vec<u8> {
  olean(false, |ptr, buf| {
    let n = digits.len();
    header(buf, ((n + 8) << 2) as u16);
    buf.extend_from_slice(&(negative as u64).to_le_bytes());
    buf.extend_from_slice(&(n as u64).to_le_bytes());
    buf.extend_from_slice(&(ptr + 32).to_le_bytes());
    digits.iter().for_each(|d| buf.extend_from_slice(&d.to_le_bytes()));
    buf.resize(buf.len().next_multiple_of(8), 0);
  })
}

fn roundtrip(olean: &[u8]) {
  let mut compressed = vec![];
  leangz::lgz::compress(&[olean], &mut compressed);
  let (buf, ranges) = leangz::lgz::decompress(Cursor::new(&compressed), 1);
  assert_eq!(ranges.len(), 1);
  assert_eq!(olean, &buf[ranges[0].clone()]);
}

#[test]
fn test_gmp_mpz_roundtrip() {
  for negative in [false, true] {
    roundtrip(&gmp_mpz(negative, &[5_000_000_000]));
    roundtrip(&gmp_mpz(negative, &[0, 1]));
    roundtrip(&gmp_mpz(negative, &[u64::MAX, u64::MAX, 7]));
  }
}

#[test]
fn test_lean_mpz_roundtrip() {
  for negative in [false, true] {
    roundtrip(&lean_mpz(negative, &[0x2a05f200, 1]));
    roundtrip(&lean_mpz(negative, &[0, 0, 1]));
    roundtrip(&lean_mpz(negative, &[u32::MAX, u32::MAX, u32::MAX, 7]));
  }
}
