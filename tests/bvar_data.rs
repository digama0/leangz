//! The compressor computes the cached data of `Expr.bvar` for every
//! constructor of that shape (tag 0, one object field, one scalar word),
//! whatever its field holds.  This test builds a minimal `.olean` holding
//! such a constructor whose field is a number with all ones in its low 32
//! bits, as `structure S where n : Nat; y : UInt64` gives for
//! `⟨4294967295, 7⟩`, and checks that it round-trips.  Without
//! `wrapping_add` in `expr_bvar_data` it overflows, which panics in a build
//! with overflow checks such as `cargo test`'s.

use std::io::Cursor;

/// Object bases are multiples of 64 KiB.
const BASE: u64 = 1 << 16;
/// Size of the version 2 `.olean` header.
const HEADER_SIZE: u64 = 96;

#[test]
fn test_bvar_shape_roundtrip() {
  let mut olean = vec![];
  olean.extend_from_slice(b"olean");
  olean.push(2); // version
  olean.push(1); // flags: GMP
  let mut lean_version = [0u8; 33];
  lean_version[..6].copy_from_slice(b"4.34.1");
  olean.extend_from_slice(&lean_version);
  olean.extend_from_slice(&[b'0'; 40]); // githash
  olean.extend_from_slice(&BASE.to_le_bytes());
  olean.extend_from_slice(&(BASE + HEADER_SIZE).to_le_bytes()); // root
                                                                // the root: a tag 0 constructor with one object field and one scalar word
  olean.extend_from_slice(&0u32.to_le_bytes()); // rc
  olean.extend_from_slice(&24u16.to_le_bytes()); // cs_sz
  olean.push(1); // num_fields
  olean.push(0); // tag
  olean.extend_from_slice(&((0xffff_ffffu64 << 1) | 1).to_le_bytes()); // boxed 2^32 - 1
  olean.extend_from_slice(&7u64.to_le_bytes());

  let mut compressed = vec![];
  leangz::lgz::compress(&[&olean], &mut compressed);
  let (buf, ranges) = leangz::lgz::decompress(Cursor::new(&compressed), 1);
  assert_eq!(olean, &buf[ranges[0].clone()]);
}
