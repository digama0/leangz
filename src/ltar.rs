use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufRead, Seek, Write};
use std::path::{Path, PathBuf};

use crate::lgz;

#[cfg(feature = "zstd")]
const COMPRESSION_LEVEL: i32 = 19;
#[cfg(all(feature = "zstd", feature = "zstd-dict"))]
const DICT_V1: &[u8] = include_bytes!("../dict/v1.dict");

const COMPRESSION_ZSTD: u8 = 0;
const COMPRESSION_LGZ: u8 = 1;
const COMPRESSION_HASH_PLAIN: u8 = 2;
const COMPRESSION_HASH_JSON: u8 = 3;

pub enum UnpackError {
  IOError(io::Error),
  InvalidUtf8(std::str::Utf8Error),
  BadLtar,
  UnsupportedCompression(u8),
}

impl std::fmt::Display for UnpackError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      UnpackError::IOError(e) => e.fmt(f),
      UnpackError::InvalidUtf8(e) => e.fmt(f),
      UnpackError::BadLtar => write!(f, "bad .ltar file"),
      UnpackError::UnsupportedCompression(c) => write!(f, "unsupported compression {c}"),
    }
  }
}

impl From<io::Error> for UnpackError {
  fn from(v: io::Error) -> Self { Self::IOError(v) }
}
impl From<std::str::Utf8Error> for UnpackError {
  fn from(v: std::str::Utf8Error) -> Self { Self::InvalidUtf8(v) }
}

#[derive(Clone, Copy)]
enum LtarVersion {
  V1,
  V2,
}

impl TryFrom<&str> for Hash {
  type Error = std::num::ParseIntError;
  fn try_from(value: &str) -> Result<Self, Self::Error> { value.parse().map(Hash) }
}
#[derive(Deserialize)]
#[serde(try_from = "&str")]
struct Hash(u64);
impl Serialize for Hash {
  fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
  where S: serde::Serializer {
    ser.serialize_str(&self.0.to_string())
  }
}

#[derive(Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum Level {
  Trace,
  Info,
  Warning,
  Error,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Message {
  message: String,
  level: Level,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuildTraceV2 {
  log: Vec<Message>,
  dep_hash: Hash,
}

enum BuildTrace {
  V1(u64),
  V2(BuildTraceV2),
  Bad,
  Missing,
}
impl BuildTrace {
  fn hash(&self) -> Option<u64> {
    match self {
      BuildTrace::V1(n) => Some(*n),
      BuildTrace::V2(b) => Some(b.dep_hash.0),
      _ => None,
    }
  }
}

fn read_trace_file(trace_path: &Path) -> Result<BuildTrace, io::Error> {
  let res = match std::fs::read_to_string(trace_path) {
    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BuildTrace::Missing),
    res => res?,
  };
  Ok(if let Ok(n) = res.parse::<u64>() {
    BuildTrace::V1(n)
  } else {
    match serde_json::de::from_str(&res) {
      Ok(b) => BuildTrace::V2(b),
      _ => BuildTrace::Bad,
    }
  })
}

pub fn unpack<R: BufRead>(
  basedir: &Path, mut tarfile: R, force: bool, verbose: bool,
) -> Result<u64, UnpackError> {
  let version = {
    let mut buf = [0; 4];
    tarfile.read_exact(&mut buf)?;
    match &buf {
      b"LTAR" => LtarVersion::V1,
      b"LTR2" => LtarVersion::V2,
      _ => return Err(UnpackError::BadLtar),
    }
  };
  let mut buf = vec![];
  let mut rollback = vec![];
  let result = (|| {
    let trace = tarfile.read_u64::<LE>()?;
    let read_cstr = |buf: &mut Vec<_>, tarfile: &mut R| -> Result<bool, UnpackError> {
      buf.clear();
      tarfile.read_until(0, buf)?;
      Ok(buf.pop().is_some())
    };
    let read_cstr_path =
      |buf: &mut Vec<_>, tarfile: &mut R| -> Result<Option<Option<PathBuf>>, UnpackError> {
        Ok(match read_cstr(buf, tarfile)? {
          true if buf.is_empty() => Some(None),
          true => Some(Some(basedir.join(std::str::from_utf8(buf)?))),
          false => None,
        })
      };
    let trace_path =
      read_cstr_path(&mut buf, &mut tarfile)?.flatten().ok_or(UnpackError::BadLtar)?;
    if !force {
      let b = read_trace_file(&trace_path)?;
      if Some(trace) == b.hash() {
        if verbose {
          println!("not unpacking because the trace matches\n{}", trace_path.display());
        }
        return Ok(trace)
      }
    }
    let mut cached_create_dir_all = {
      // Unpacked files tend to reuse the same folders for multiple files,
      // so cache the last directory so that we don't call the OS so much
      // for `create_dir_all`. See #3
      let mut prefix: Option<PathBuf> = None;
      move |dir: &Path| {
        if !prefix.as_deref().is_some_and(|d| d == dir) {
          std::fs::create_dir_all(dir)?;
          prefix = Some(dir.to_owned());
        }
        Ok::<_, io::Error>(())
      }
    };
    #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
    let dict = zstd::dict::DecoderDictionary::copy(DICT_V1);
    cached_create_dir_all(trace_path.parent().ok_or(UnpackError::BadLtar)?)?;
    match version {
      LtarVersion::V1 => {
        std::fs::write(&trace_path, format!("{trace}"))?;
        rollback.push(trace_path);
      }
      LtarVersion::V2 =>
        unpack_one(&mut tarfile, verbose, trace_path, &mut buf, &mut rollback, &dict)?,
    }
    while let Some(path) = read_cstr_path(&mut buf, &mut tarfile)? {
      if let Some(path) = path {
        cached_create_dir_all(path.parent().ok_or(UnpackError::BadLtar)?)?;
        unpack_one(&mut tarfile, verbose, path, &mut buf, &mut rollback, &dict)?;
      } else if read_cstr(&mut buf, &mut tarfile)? {
        if verbose {
          println!("comment: {}", std::str::from_utf8(&buf)?);
        }
      } else {
        return Err(UnpackError::BadLtar)
      }
    }
    Ok(trace)
  })();
  if result.is_err() {
    for file in rollback {
      let _ = std::fs::remove_file(file);
    }
  }
  result
}

fn unpack_one<R: BufRead>(
  tarfile: &mut R, verbose: bool, path: PathBuf, buf: &mut Vec<u8>, rollback: &mut Vec<PathBuf>,
  dict: &zstd::dict::DecoderDictionary<'_>,
) -> Result<(), UnpackError> {
  let compression = tarfile.read_u8()?;
  if verbose {
    println!("copying {}, compression = {compression}", path.display());
  }
  match compression {
    COMPRESSION_ZSTD => {
      buf.clear();
      buf.resize(tarfile.read_u64::<LE>()? as usize, 0);
      tarfile.read_exact(buf)?;
      let reader = std::io::Cursor::new(&**buf);
      #[cfg(feature = "zstd")]
      let reader = zstd::stream::Decoder::new(reader)?;
      let mut file = File::create(&path)?;
      rollback.push(path);
      std::io::copy(&mut { reader }, &mut file)?;
    }
    COMPRESSION_LGZ => {
      buf.clear();
      buf.resize(tarfile.read_u64::<LE>()? as usize, 0);
      tarfile.read_exact(buf)?;
      let reader = std::io::Cursor::new(&**buf);
      #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
      let reader = zstd::stream::Decoder::with_prepared_dictionary(reader, dict)?;
      #[cfg(all(feature = "zstd", not(feature = "zstd-dict")))]
      let reader = zstd::stream::Decoder::new(reader)?;
      let mut file = File::create(&path)?;
      rollback.push(path);
      file.write_all(&lgz::decompress(reader))?;
    }
    COMPRESSION_HASH_PLAIN => {
      std::fs::write(&path, format!("{}", tarfile.read_u64::<LE>()?))?;
      rollback.push(path);
    }
    COMPRESSION_HASH_JSON => {
      let b = BuildTraceV2 { log: vec![], dep_hash: Hash(tarfile.read_u64::<LE>()?) };
      std::fs::write(&path, serde_json::to_vec(&b).unwrap())?;
      rollback.push(path);
    }
    compression => return Err(UnpackError::UnsupportedCompression(compression)),
  }
  Ok(())
}

pub fn pack(
  basedir: &Path, mut tarfile: impl Write, trace_path: &str,
  args: impl IntoIterator<Item = String>, verbose: bool,
) -> io::Result<()> {
  let (version, trace) = match read_trace_file(&basedir.join(trace_path))? {
    BuildTrace::Missing => panic!("expected .trace file"),
    BuildTrace::Bad => panic!("bad .trace file"),
    BuildTrace::V1(n) => (LtarVersion::V1, BuildTraceV2 { log: vec![], dep_hash: Hash(n) }),
    BuildTrace::V2(mut b) => {
      b.log.retain(|it| it.level >= Level::Info);
      (LtarVersion::V2, b)
    }
  };
  tarfile.write_all(match version {
    LtarVersion::V1 => b"LTAR",
    LtarVersion::V2 => b"LTR2",
  })?;
  tarfile.write_u64::<LE>(trace.dep_hash.0)?;
  tarfile.write_all(trace_path.as_bytes())?;
  tarfile.write_u8(0)?;
  #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
  let dict_v1 = zstd::dict::EncoderDictionary::copy(DICT_V1, COMPRESSION_LEVEL);
  if let LtarVersion::V2 = version {
    if trace.log.is_empty() {
      tarfile.write_u8(COMPRESSION_HASH_JSON)?;
      tarfile.write_u64::<LE>(trace.dep_hash.0)?;
    } else {
      pack_zstd(&serde_json::to_vec(&trace)?, &mut tarfile)?;
    }
  }
  let mut it = args.into_iter();
  while let Some(file) = it.next() {
    if file == "-c" {
      let comment = it.next().expect("expected comment argument");
      tarfile.write_u8(0)?;
      tarfile.write_all(comment.as_bytes())?;
      tarfile.write_u8(0)?;
      continue
    }
    tarfile.write_all(file.as_bytes())?;
    tarfile.write_u8(0)?;
    let path = basedir.join(file);
    if verbose {
      println!("compressing {}", path.display());
    }
    let mmap = unsafe { Mmap::map(&File::open(path)?)? };
    if mmap.get(..5) == Some(b"olean") {
      let mut buf = vec![];
      tarfile.write_u8(COMPRESSION_LGZ)?;
      #[cfg(feature = "zstd")]
      {
        #[cfg(feature = "zstd-dict")]
        let mut enc = zstd::stream::Encoder::with_prepared_dictionary(&mut buf, &dict_v1)?;
        #[cfg(not(feature = "zstd-dict"))]
        let mut enc = zstd::stream::Encoder::new(&mut buf, COMPRESSION_LEVEL)?;
        lgz::compress(&mmap, &mut enc);
        enc.finish()?;
      }
      #[cfg(not(feature = "zstd"))]
      lgz::compress(&mmap, &mut buf);
      tarfile.write_u64::<LE>(buf.len() as u64)?;
      tarfile.write_all(&buf)?;
    } else if let Some(n) = (|| {
      if mmap.len() > 20 {
        return None
      }
      std::str::from_utf8(&mmap).ok()?.parse::<u64>().ok()
    })() {
      tarfile.write_u8(COMPRESSION_HASH_PLAIN)?;
      tarfile.write_u64::<LE>(n)?;
    } else {
      pack_zstd(&mmap, &mut tarfile)?;
    }
  }
  Ok(())
}

fn pack_zstd(data: &[u8], tarfile: &mut impl Write) -> io::Result<()> {
  tarfile.write_u8(COMPRESSION_ZSTD)?;
  let mut buf = vec![];
  #[cfg(feature = "zstd")]
  {
    let mut enc = zstd::stream::Encoder::new(&mut buf, COMPRESSION_LEVEL)?;
    enc.write_all(data)?;
    enc.finish()?;
  }
  #[cfg(not(feature = "zstd"))]
  buf.extend_from_slice(data);
  tarfile.write_u64::<LE>(buf.len() as u64)?;
  tarfile.write_all(&buf)?;
  Ok(())
}

pub fn comments<R: BufRead + Seek>(mut tarfile: R) -> Result<Vec<String>, UnpackError> {
  let mut buf = vec![0; 4];
  tarfile.read_exact(&mut buf)?;
  if buf != *b"LTAR" {
    return Err(UnpackError::BadLtar)
  }
  tarfile.read_u64::<LE>()?;
  let read_cstr = |buf: &mut Vec<_>, tarfile: &mut R| -> Result<bool, UnpackError> {
    buf.clear();
    tarfile.read_until(0, buf)?;
    Ok(buf.pop().is_some())
  };
  if !read_cstr(&mut buf, &mut tarfile)? {
    return Err(UnpackError::BadLtar)
  }
  let mut comments = vec![];
  while read_cstr(&mut buf, &mut tarfile)? {
    if buf.is_empty() {
      if !read_cstr(&mut buf, &mut tarfile)? {
        return Err(UnpackError::BadLtar)
      }
      comments.push(std::str::from_utf8(&buf)?.to_string());
      continue
    }
    let len = match tarfile.read_u8()? {
      COMPRESSION_ZSTD | COMPRESSION_LGZ => tarfile.read_u64::<LE>()?,
      COMPRESSION_HASH_PLAIN | COMPRESSION_HASH_JSON => 8,
      compression => return Err(UnpackError::UnsupportedCompression(compression)),
    };
    tarfile.seek(io::SeekFrom::Current(len as _))?;
  }
  Ok(comments)
}
