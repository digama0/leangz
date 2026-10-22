//! Integration tests for small files whose contents look like a number.
//!
//! `pack` stores a short file that parses as a decimal `u64` as the number
//! itself, and `unpack` writes the number back in decimal. Lake's `.hash` files
//! hold 16 zero-padded hex digits, so these tests check that a hash made only of
//! decimal digits comes back byte-identical, leading zeros included.

use std::fs;
use std::io::Cursor;

/// Packs `Attr.trace` with the given files and unpacks the archive into a fresh
/// directory, returning each file's unpacked contents.
fn roundtrip(files: &[(&str, &str)]) -> Vec<String> {
  let packed = tempfile::tempdir().expect("Failed to create a directory");
  fs::copy(
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/module/Attr.trace"),
    packed.path().join("Attr.trace"),
  )
  .expect("Failed to copy the .trace file");
  for (name, contents) in files {
    fs::write(packed.path().join(name), contents).expect("Failed to write a file");
  }

  let mut ltar = Vec::new();
  leangz::ltar::pack(
    &[packed.path().to_owned()],
    &mut ltar,
    "Attr.trace",
    files.iter().map(|(name, _)| name.to_string()),
    true,
    false,
  )
  .expect("Failed to pack");

  let unpacked = tempfile::tempdir().expect("Failed to create a directory");
  leangz::ltar::unpack(&[unpacked.path().to_owned()], Cursor::new(ltar), true, None, false)
    .unwrap_or_else(|e| panic!("Failed to unpack: {e}"));
  files
    .iter()
    .map(|(name, _)| {
      fs::read_to_string(unpacked.path().join(name)).expect("Failed to read an unpacked file")
    })
    .collect()
}

/// A file that parses as a number but is not written the way the number is,
/// with leading zeros like a Lake hash made only of decimal digits, or with a
/// `+` sign, unpacks unchanged.
#[test]
fn test_non_canonical_numbers_roundtrip() {
  let files = [
    ("Attr.olean.hash", "0482915573019264"),
    ("Attr.ilean.hash", "0000000000000001"),
    ("Attr.c.hash", "+12345"),
  ];
  for ((name, packed), unpacked) in files.iter().zip(roundtrip(&files)) {
    assert_eq!(*packed, unpacked, "{name} changed in the round trip");
  }
}

/// A hash with hex letters, and a decimal number written as Rust writes it, as
/// older Lake versions wrote their hashes, come back unchanged too.
#[test]
fn test_other_number_files_roundtrip() {
  let files = [
    ("Attr.c.hash", "3cca31ffa2f465ef"),
    ("Attr.ir.hash", "12345678901234567890"),
    ("Attr.bc.hash", "0"),
  ];
  for ((name, packed), unpacked) in files.iter().zip(roundtrip(&files)) {
    assert_eq!(*packed, unpacked, "{name} changed in the round trip");
  }
}
