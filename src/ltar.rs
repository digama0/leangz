use byteorder::{ReadBytesExt, WriteBytesExt, LE};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fs::File;
use std::io::{self, BufRead, ErrorKind, Seek, Write};
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
const COMPRESSION_HASH_OUTPUT: u8 = 4;

pub enum UnpackError {
  IOError(io::Error),
  InvalidUtf8(std::str::Utf8Error),
  BadLtar,
  NotEnoughPaths(u8),
  UnsupportedCompression(u8),
}

impl std::fmt::Display for UnpackError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      UnpackError::IOError(e) => e.fmt(f),
      UnpackError::InvalidUtf8(e) => e.fmt(f),
      UnpackError::BadLtar => write!(f, "bad .ltar file"),
      UnpackError::NotEnoughPaths(n) => write!(f, "not enough base paths (expected > {n})"),
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

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
enum LtarVersion {
  V1,
  V2,
  V3,
}

impl TryFrom<Cow<'_, str>> for Hash {
  type Error = std::num::ParseIntError;
  fn try_from(value: Cow<'_, str>) -> Result<Self, Self::Error> { value.parse().map(Hash) }
}

#[derive(Deserialize)]
#[serde(try_from = "Cow<'_, str>")]
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

#[derive(Serialize)]
struct ModuleOutputHashes {
  #[serde(rename = "o")]
  olean: Vec<Hash>,
  #[serde(rename = "i")]
  ilean: Hash,
  #[serde(skip_serializing_if = "Option::is_none")]
  ir: Option<Hash>,
  c: Hash,
  #[serde(rename = "b", skip_serializing_if = "Option::is_none")]
  bc: Option<Hash>,
}

const OUTPUT_HASH_END: u8 = 0;
const OUTPUT_HASH_OLEAN: u8 = 1;
const OUTPUT_HASH_IR: u8 = 4;
const OUTPUT_HASH_BC: u8 = 5;

fn assert_none<T, E>(i: Option<T>, e: E) -> Result<(), E> {
  match i {
    Some(_) => Err(e),
    None => Ok(()),
  }
}

impl TryFrom<&serde_json::Value> for ModuleOutputHashes {
  type Error = ();
  fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
    let (mut o, mut ilean, mut ir, mut c, mut bc) = (None, None, None, None, None);
    for (key, val) in value.as_object().ok_or(())? {
      match &**key {
        "o" => assert_none(o.replace(serde_json::from_value(val.clone()).map_err(|_| ())?), ())?,
        "i" =>
          assert_none(ilean.replace(serde_json::from_value(val.clone()).map_err(|_| ())?), ())?,
        "ir" => assert_none(ir.replace(serde_json::from_value(val.clone()).map_err(|_| ())?), ())?,
        "c" => assert_none(c.replace(serde_json::from_value(val.clone()).map_err(|_| ())?), ())?,
        "b" => assert_none(bc.replace(serde_json::from_value(val.clone()).map_err(|_| ())?), ())?,
        _ => return Err(()),
      }
    }
    let olean: Vec<Hash> = o.ok_or(())?;
    if olean.is_empty() {
      return Err(())
    }
    Ok(Self { olean, ilean: ilean.ok_or(())?, ir, c: c.ok_or(())?, bc })
  }
}

enum Outputs {
  LeanModule(ModuleOutputHashes),
  Other(serde_json::Value),
}
impl Serialize for Outputs {
  fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
  where S: serde::Serializer {
    match self {
      Outputs::LeanModule(m) => m.serialize(ser),
      Outputs::Other(value) => value.serialize(ser),
    }
  }
}
impl<'de> Deserialize<'de> for Outputs {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where D: serde::Deserializer<'de> {
    let val: serde_json::Value = Deserialize::deserialize(deserializer)?;
    match (&val).try_into() {
      Ok(m) => Ok(Outputs::LeanModule(m)),
      Err(_) => Ok(Outputs::Other(val)),
    }
  }
}

const fn true_fn() -> bool { true }
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuildTraceV2 {
  #[serde(skip_serializing_if = "Vec::is_empty")]
  log: Vec<Message>,
  dep_hash: Hash,
  #[serde(skip_serializing_if = "Option::is_none")]
  outputs: Option<Outputs>,
  #[allow(dead_code)]
  #[serde(skip_serializing, default = "true_fn")]
  synthetic: bool,
}

impl BuildTraceV2 {
  fn from_hash(hash: u64, outputs: Option<Outputs>) -> Self {
    Self { log: vec![], dep_hash: Hash(hash), outputs, synthetic: true }
  }
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

fn get_version<R: BufRead>(tarfile: &mut R) -> Result<LtarVersion, UnpackError> {
  let mut buf = [0; 4];
  tarfile.read_exact(&mut buf)?;
  Ok(match &buf {
    b"LTAR" => LtarVersion::V1,
    b"LTR2" => LtarVersion::V2,
    b"LTR3" => LtarVersion::V3,
    _ => return Err(UnpackError::BadLtar),
  })
}

pub fn unpack<R: BufRead + Seek>(
  basedir: &[PathBuf], mut tarfile: R, force: bool, verbose: bool,
) -> Result<u64, UnpackError> {
  let version = get_version(&mut tarfile)?;
  let mut buf = vec![];
  let mut rollback = vec![];
  let result = (|| {
    let trace = tarfile.read_u64::<LE>()?;
    let read_cstr = |buf: &mut Vec<_>, tarfile: &mut R| -> Result<bool, UnpackError> {
      buf.clear();
      tarfile.read_until(0, buf)?;
      Ok(buf.pop().is_some())
    };
    let read_cstr_path = |pathidx: bool,
                          buf: &mut Vec<_>,
                          tarfile: &mut R|
     -> Result<Option<Option<PathBuf>>, UnpackError> {
      let pathidx = if !pathidx || version < LtarVersion::V3 {
        0
      } else {
        match tarfile.read_u8() {
          Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
          idx => idx?,
        }
      };
      Ok(match read_cstr(buf, tarfile)? {
        true if buf.is_empty() => Some(None),
        true => Some(Some(
          basedir
            .get(pathidx as usize)
            .ok_or(UnpackError::NotEnoughPaths(pathidx))?
            .join(std::str::from_utf8(buf)?),
        )),
        false => None,
      })
    };
    let trace_path =
      read_cstr_path(false, &mut buf, &mut tarfile)?.flatten().ok_or(UnpackError::BadLtar)?;
    let mut skip = false;
    if !force {
      let b = read_trace_file(&trace_path)?;
      skip = Some(trace) == b.hash();
      if skip && basedir.len() == 1 {
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
        if prefix.as_deref().is_none_or(|d| d != dir) {
          std::fs::create_dir_all(dir)?;
          prefix = Some(dir.to_owned());
        }
        Ok::<_, io::Error>(())
      }
    };
    #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
    let dict = zstd::dict::DecoderDictionary::copy(DICT_V1);
    if skip && matches!(std::fs::exists(&trace_path), Ok(true)) {
      skip_one(&mut tarfile, verbose, trace_path)?
    } else {
      cached_create_dir_all(trace_path.parent().ok_or(UnpackError::BadLtar)?)?;
      if version < LtarVersion::V2 {
        std::fs::write(&trace_path, format!("{trace}"))?;
        rollback.push(trace_path);
      } else {
        unpack_one(
          &mut tarfile,
          verbose,
          trace_path,
          &mut buf,
          &mut rollback,
          #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
          &dict,
        )?
      }
    }
    while let Some(path) = read_cstr_path(true, &mut buf, &mut tarfile)? {
      if let Some(path) = path {
        if skip && matches!(std::fs::exists(&path), Ok(true)) {
          skip_one(&mut tarfile, verbose, path)?
        } else {
          cached_create_dir_all(path.parent().ok_or(UnpackError::BadLtar)?)?;
          unpack_one(
            &mut tarfile,
            verbose,
            path,
            &mut buf,
            &mut rollback,
            #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
            &dict,
          )?;
        }
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
  #[cfg(all(feature = "zstd", feature = "zstd-dict"))] dict: &zstd::dict::DecoderDictionary<'_>,
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
      let b = BuildTraceV2::from_hash(tarfile.read_u64::<LE>()?, None);
      std::fs::write(&path, serde_json::to_vec(&b).unwrap())?;
      rollback.push(path);
    }
    COMPRESSION_HASH_OUTPUT => {
      let hash = tarfile.read_u64::<LE>()?;
      let mut m = ModuleOutputHashes {
        olean: vec![Hash(tarfile.read_u64::<LE>()?)],
        ilean: Hash(tarfile.read_u64::<LE>()?),
        ir: None,
        c: Hash(tarfile.read_u64::<LE>()?),
        bc: None,
      };
      loop {
        match tarfile.read_u8()? {
          OUTPUT_HASH_END => break,
          OUTPUT_HASH_OLEAN => m.olean.push(Hash(tarfile.read_u64::<LE>()?)),
          OUTPUT_HASH_IR =>
            assert_none(m.ir.replace(Hash(tarfile.read_u64::<LE>()?)), UnpackError::BadLtar)?,
          OUTPUT_HASH_BC =>
            assert_none(m.bc.replace(Hash(tarfile.read_u64::<LE>()?)), UnpackError::BadLtar)?,
          _ => return Err(UnpackError::BadLtar),
        }
      }
      let b = BuildTraceV2::from_hash(hash, Some(Outputs::LeanModule(m)));
      std::fs::write(&path, serde_json::to_vec(&b).unwrap())?;
      rollback.push(path);
    }
    compression => return Err(UnpackError::UnsupportedCompression(compression)),
  }
  Ok(())
}

fn skip_one<R: BufRead + Seek>(
  tarfile: &mut R, verbose: bool, path: PathBuf,
) -> Result<(), UnpackError> {
  let compression = tarfile.read_u8()?;
  if verbose {
    println!("skipping {}, compression = {compression}", path.display());
  }
  match compression {
    COMPRESSION_ZSTD | COMPRESSION_LGZ => {
      let len = tarfile.read_u64::<LE>()?;
      tarfile.seek_relative(len.try_into().unwrap())?;
    }
    COMPRESSION_HASH_PLAIN | COMPRESSION_HASH_JSON => {
      tarfile.read_u64::<LE>()?;
    }
    compression => return Err(UnpackError::UnsupportedCompression(compression)),
  }
  Ok(())
}

pub fn pack(
  basedirs: &[PathBuf], mut tarfile: impl Write, trace_path: &str,
  args: impl IntoIterator<Item = String>, verbose: bool,
) -> io::Result<()> {
  let (version, trace) = match read_trace_file(&basedirs[0].join(trace_path))? {
    BuildTrace::Missing => panic!("expected .trace file"),
    BuildTrace::Bad => panic!("bad .trace file"),
    BuildTrace::V1(n) => (LtarVersion::V1, BuildTraceV2::from_hash(n, None)),
    BuildTrace::V2(mut b) => {
      b.log.retain(|it| it.level >= Level::Info);
      (if basedirs.len() == 1 { LtarVersion::V2 } else { LtarVersion::V3 }, b)
    }
  };
  tarfile.write_all(match version {
    LtarVersion::V1 => b"LTAR",
    LtarVersion::V2 => b"LTR2",
    LtarVersion::V3 => b"LTR3",
  })?;
  tarfile.write_u64::<LE>(trace.dep_hash.0)?;
  tarfile.write_all(trace_path.as_bytes())?;
  tarfile.write_u8(0)?;
  #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
  let dict_v1 = zstd::dict::EncoderDictionary::copy(DICT_V1, COMPRESSION_LEVEL);
  if version >= LtarVersion::V2 {
    match trace {
      BuildTraceV2 { log, dep_hash, outputs: None, synthetic: _ } if log.is_empty() => {
        tarfile.write_u8(COMPRESSION_HASH_JSON)?;
        tarfile.write_u64::<LE>(dep_hash.0)?;
      }
      BuildTraceV2 { log, dep_hash, outputs: Some(Outputs::LeanModule(m)), synthetic: _ }
        if log.is_empty() =>
      {
        tarfile.write_u8(COMPRESSION_HASH_OUTPUT)?;
        tarfile.write_u64::<LE>(dep_hash.0)?;
        let mut oleans = m.olean.iter();
        tarfile.write_u64::<LE>(oleans.next().unwrap().0)?;
        tarfile.write_u64::<LE>(m.ilean.0)?;
        tarfile.write_u64::<LE>(m.c.0)?;
        for olean in oleans {
          tarfile.write_u8(OUTPUT_HASH_OLEAN)?;
          tarfile.write_u64::<LE>(olean.0)?;
        }
        if let Some(ir) = &m.ir {
          tarfile.write_u8(OUTPUT_HASH_IR)?;
          tarfile.write_u64::<LE>(ir.0)?;
        }
        if let Some(bc) = &m.bc {
          tarfile.write_u8(OUTPUT_HASH_BC)?;
          tarfile.write_u64::<LE>(bc.0)?;
        }
        tarfile.write_u8(OUTPUT_HASH_END)?;
      }
      _ => pack_zstd(&serde_json::to_vec(&trace)?, &mut tarfile)?,
    };
  }
  let mut it = args.into_iter();
  while let Some(mut file) = it.next() {
    if file == "-c" {
      let comment = it.next().expect("expected comment argument");
      tarfile.write_u8(0)?;
      tarfile.write_all(comment.as_bytes())?;
      tarfile.write_u8(0)?;
      continue
    }
    let mut idx = 0u8;
    if file == "-i" {
      idx = it.next().expect("expected index argument").parse().expect("expected number");
      file = it.next().expect("expected file");
    }
    if version >= LtarVersion::V3 {
      tarfile.write_u8(idx)?;
    } else {
      assert!(idx == 0);
    }
    tarfile.write_all(file.as_bytes())?;
    tarfile.write_u8(0)?;
    let path = basedirs.get(idx as usize).expect("not enough parent paths").join(file);
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
  let version = get_version(&mut tarfile)?;
  let mut buf = vec![];
  tarfile.read_u64::<LE>()?;
  let read_cstr = |buf: &mut Vec<_>, tarfile: &mut R| -> Result<bool, UnpackError> {
    buf.clear();
    tarfile.read_until(0, buf)?;
    Ok(buf.pop().is_some())
  };
  fn skip_one<R: BufRead + Seek>(pathidx: bool, tarfile: &mut R) -> Result<(), UnpackError> {
    if pathidx {
      tarfile.read_u8()?;
    }
    let len = match tarfile.read_u8()? {
      COMPRESSION_ZSTD | COMPRESSION_LGZ => tarfile.read_u64::<LE>()?,
      COMPRESSION_HASH_PLAIN | COMPRESSION_HASH_JSON => 8,
      COMPRESSION_HASH_OUTPUT => {
        tarfile.seek(io::SeekFrom::Current(32))?;
        loop {
          match tarfile.read_u8()? {
            OUTPUT_HASH_END => return Ok(()),
            OUTPUT_HASH_OLEAN | OUTPUT_HASH_IR | OUTPUT_HASH_BC => tarfile.read_u64::<LE>()?,
            _ => return Err(UnpackError::BadLtar),
          };
        }
      }
      compression => return Err(UnpackError::UnsupportedCompression(compression)),
    };
    tarfile.seek(io::SeekFrom::Current(len as _))?;
    Ok(())
  }
  if !read_cstr(&mut buf, &mut tarfile)? {
    return Err(UnpackError::BadLtar)
  }
  if version >= LtarVersion::V2 {
    skip_one(false, &mut tarfile)?
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
    skip_one(version >= LtarVersion::V3, &mut tarfile)?;
  }
  Ok(comments)
}
