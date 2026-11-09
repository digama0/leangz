//! `.olean` files that `lgz` does not support are packed without it, and unpack unchanged.

use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

const MODULE: [&str; 3] = ["ExprHash.olean", "ExprHash.olean.server", "ExprHash.olean.private"];

fn fixture(file: &str) -> Vec<u8> {
  fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/module").join(file)).unwrap()
}

/// Packs the `ExprHash` module (with `.olean` replaced by `olean`) and unpacks it again.
fn roundtrip(olean: &[u8]) {
  let src = tempfile::tempdir().unwrap();
  let dst = tempfile::tempdir().unwrap();
  fs::write(src.path().join("ExprHash.trace"), "0").unwrap();
  fs::write(src.path().join(MODULE[0]), olean).unwrap();
  for file in &MODULE[1..] {
    fs::write(src.path().join(file), fixture(file)).unwrap();
  }

  let mut ltar = vec![];
  let args = MODULE.iter().map(|s| s.to_string());
  leangz::ltar::pack(&[src.path().to_owned()], &mut ltar, "ExprHash.trace", args, true, false)
    .unwrap();
  let dst_dirs: [PathBuf; 1] = [dst.path().to_owned()];
  leangz::ltar::unpack(&dst_dirs, Cursor::new(&ltar), true, None, false)
    .map_err(|e| e.to_string())
    .unwrap();

  for file in MODULE {
    assert_eq!(
      fs::read(src.path().join(file)).unwrap(),
      fs::read(dst.path().join(file)).unwrap(),
      "{file} does not match after pack/unpack"
    );
  }
}

#[test]
fn test_supported_module_roundtrip() { roundtrip(&fixture(MODULE[0])) }

/// The closure section of a version 4 `.olean` with closures is not empty. Here it claims one
/// closure offset; `lgz` must not be used for it, or for the other files of the module.
#[test]
fn test_closures_module_roundtrip() {
  let mut olean = fixture(MODULE[0]);
  let len = olean.len();
  olean[len - 8] = 1;
  assert!(!leangz::lgz::is_supported(&olean));
  roundtrip(&olean)
}
