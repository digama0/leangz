use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LE;
use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::lgz;

const COMPRESSION_LEVEL: i32 = 19;
const DICT_V1: &[u8] = include_bytes!("../dict/v1.dict");

pub enum UnpackError {
  IOError(io::Error),
  InvalidUtf8(std::str::Utf8Error),
  BadLtar,
  BadTrace,
  UnsupportedCompression(u8),
}

impl std::fmt::Display for UnpackError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      UnpackError::IOError(e) => e.fmt(f),
      UnpackError::InvalidUtf8(e) => e.fmt(f),
      UnpackError::BadLtar => write!(f, "bad .ltar file"),
      UnpackError::BadTrace => write!(f, "bad .trace file"),
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

pub fn unpack<R: BufRead>(
  basedir: &Path, mut tarfile: R, force: bool, verbose: bool,
) -> Result<u64, UnpackError> {
  let mut buf = vec![0; 4];
  tarfile.read_exact(&mut buf)?;
  if buf != *b"LTAR" {
    return Err(UnpackError::BadLtar)
  }
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
  let trace_path = read_cstr_path(&mut buf, &mut tarfile)?.flatten().ok_or(UnpackError::BadLtar)?;
  if !force {
    match std::fs::read_to_string(&trace_path) {
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
      res =>
        if trace == res?.parse::<u64>().map_err(|_| UnpackError::BadTrace)? {
          return Ok(trace)
        },
    }
  }
  let prefix = trace_path.parent().ok_or(UnpackError::BadLtar)?;
  std::fs::create_dir_all(prefix)?;
  std::fs::write(trace_path, format!("{trace}"))?;
  let dict = zstd::dict::DecoderDictionary::copy(DICT_V1);
  while let Some(path) = read_cstr_path(&mut buf, &mut tarfile)? {
    let Some(path) = path else {
      if !read_cstr(&mut buf, &mut tarfile)? {
        return Err(UnpackError::BadLtar)
      }
      if verbose {
        println!("comment: {}", std::str::from_utf8(&buf)?);
      }
      continue
    };
    if verbose {
      println!("copying {}", path.display());
    }
    let compression = tarfile.read_u8()?;
    buf.clear();
    buf.resize(tarfile.read_u64::<LE>()? as usize, 0);
    tarfile.read_exact(&mut buf)?;
    let reader = std::io::Cursor::new(&*buf);
    let prefix = path.parent().ok_or(UnpackError::BadLtar)?;
    std::fs::create_dir_all(prefix)?;
    match compression {
      0 => {
        let mut dec = zstd::stream::Decoder::new(reader)?;
        std::io::copy(&mut dec, &mut File::create(path)?)?;
      }
      1 => {
        let dec = zstd::stream::Decoder::with_prepared_dictionary(reader, &dict)?;
        std::fs::write(path, &lgz::decompress(dec))?;
      }
      _ => return Err(UnpackError::UnsupportedCompression(compression)),
    }
  }
  Ok(trace)
}

pub fn pack(
  basedir: &Path, mut tarfile: impl Write, trace_path: &str,
  args: impl IntoIterator<Item = String>, verbose: bool,
) -> io::Result<()> {
  let trace = std::fs::read_to_string(basedir.join(trace_path))?;
  let trace = trace.parse::<u64>().expect("expected .trace file");
  tarfile.write_all(b"LTAR")?;
  tarfile.write_u64::<LE>(trace)?;
  tarfile.write_all(trace_path.as_bytes())?;
  tarfile.write_u8(0)?;
  let dict_v1 = zstd::dict::EncoderDictionary::copy(DICT_V1, COMPRESSION_LEVEL);
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
    let mut buf = vec![];
    let mut enc;
    if mmap.get(..16) == Some(b"oleanfile!!!!!!!") {
      tarfile.write_u8(1)?;
      enc = zstd::stream::Encoder::with_prepared_dictionary(&mut buf, &dict_v1)?;
      lgz::compress(&mmap, &mut enc);
    } else {
      tarfile.write_u8(0)?;
      enc = zstd::stream::Encoder::new(&mut buf, COMPRESSION_LEVEL)?;
      enc.write_all(&mmap)?;
    }
    enc.finish()?;
    tarfile.write_u64::<LE>(buf.len() as u64)?;
    tarfile.write_all(&buf)?;
  }
  Ok(())
}
