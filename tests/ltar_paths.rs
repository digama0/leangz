//! Unpacking .ltar files whose file names would leave the target directory:
//! leantar refuses them, and leaves no file outside the target directory.

use leangz::ltar::{unpack, UnpackError};
use std::io::Cursor;
use std::path::Path;

const COMPRESSION_HASH_JSON: u8 = 3;
const COMPRESSION_LGZ_MODULE: u8 = 5;

/// A version 4 archive with the trace file `trace`, followed by `entries`.
fn ltar(trace: &str, entries: &[u8]) -> Vec<u8> {
  let mut t = b"LTR4".to_vec();
  t.push(1); // with a depHash
  t.extend_from_slice(&7u64.to_le_bytes());
  t.extend_from_slice(trace.as_bytes());
  t.push(0);
  t.push(COMPRESSION_HASH_JSON);
  t.extend_from_slice(&7u64.to_le_bytes());
  t.extend_from_slice(entries);
  t
}

/// An entry of the first base directory, written as a JSON trace.
fn entry(path: &str) -> Vec<u8> {
  let mut e = vec![0];
  e.extend_from_slice(path.as_bytes());
  e.push(0);
  e.push(COMPRESSION_HASH_JSON);
  e.extend_from_slice(&7u64.to_le_bytes());
  e
}

/// Every file below `dir`, relative to it.
fn files(dir: &Path) -> Vec<String> {
  let mut out = vec![];
  let mut todo = vec![dir.to_owned()];
  while let Some(d) = todo.pop() {
    for e in std::fs::read_dir(d).unwrap() {
      let p = e.unwrap().path();
      if p.is_dir() {
        todo.push(p)
      } else {
        out.push(p.strip_prefix(dir).unwrap().display().to_string())
      }
    }
  }
  out.sort();
  out
}

/// Unpacks `archive` into `base` inside a fresh temporary directory, and
/// returns the result with the files that the directory holds afterwards.
fn unpack_in_temp_dir(archive: &[u8]) -> (Result<u64, UnpackError>, Vec<String>) {
  let dir = tempfile::tempdir().unwrap();
  let base = dir.path().join("base");
  std::fs::create_dir(&base).unwrap();
  let r = unpack(&[base], Cursor::new(archive), false, None, false);
  (r, files(dir.path()))
}

#[test]
fn test_safe_paths() {
  let entries = [entry("a/b.json"), entry("./c.json")].concat();
  let (r, files) = unpack_in_temp_dir(&ltar("x.trace", &entries));
  assert!(r.is_ok());
  assert_eq!(files, ["base/a/b.json", "base/c.json", "base/x.trace"]);
}

#[test]
fn test_unsafe_paths() {
  let outside = tempfile::tempdir().unwrap();
  let absolute = outside.path().join("absolute.json");
  let absolute = absolute.to_str().unwrap();
  let unsafe_paths = [
    "../up.json",
    "a/../../up.json",
    "..",
    absolute,
    "/up.json",
    "\\up.json",
    "C:up.json",
    "C:\\up.json",
    "\\\\?\\C:\\up.json",
    ".",
    "./",
    "a/.",
    "a/",
  ];
  for path in unsafe_paths {
    // as an entry after a safe one, as the trace file, and as a part of a module
    let module = [
      &[0][..],
      b"m.olean\0",
      &[COMPRESSION_LGZ_MODULE, 0],
      b"m.olean.server\0",
      &[0],
      path.as_bytes(),
      &[0],
    ]
    .concat();
    for archive in [
      ltar("x.trace", &[entry("ok.json"), entry(path)].concat()),
      ltar(path, &[]),
      ltar("x.trace", &module),
    ] {
      let (r, files) = unpack_in_temp_dir(&archive);
      assert!(matches!(r, Err(UnpackError::UnsafePath(p)) if p == path), "{path} was accepted");
      assert!(files.iter().all(|f| f.starts_with("base")), "{path}: wrote {files:?}");
    }
  }
  assert!(!Path::new(absolute).exists());
}

/// Packing refuses such names too, so that it cannot write an archive that
/// unpacking would refuse.
#[test]
fn test_pack_unsafe_paths() {
  let dir = tempfile::tempdir().unwrap();
  std::fs::write(dir.path().join("x.trace"), "7").unwrap();
  let base = [dir.path().to_owned()];
  let pack = |trace: &str, file: &str| {
    leangz::ltar::pack(&base, &mut vec![], trace, [file.to_string()], true, false)
  };
  assert_eq!(pack("../x.trace", "a").unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
  assert_eq!(pack("x.trace", "../a").unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
}
