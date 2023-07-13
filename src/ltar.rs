use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LE;
use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::Write;
use std::path::Path;

use crate::lgz;

const COMPRESSION_LEVEL: i32 = 19;
const DICT_V1: &[u8] = include_bytes!("../dict/v1.dict");

pub fn unpack<R: BufRead>(
  basedir: &Path, mut tarfile: R, force: bool, verbose: bool,
) -> io::Result<()> {
  let mut buf = vec![0; 4];
  let is_ltar = match tarfile.read_exact(&mut buf) {
    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => false,
    e => {
      e.unwrap();
      buf == *b"LTAR"
    }
  };
  if !is_ltar {
    return Err(io::Error::new(io::ErrorKind::InvalidData, "not a valid .ltar file"))
  }
  let trace = tarfile.read_u64::<LE>().unwrap();
  let read_cstr = |buf: &mut Vec<_>, tarfile: &mut R| {
    buf.clear();
    tarfile.read_until(0, buf).unwrap();
    buf.pop().map(|_| basedir.join(std::str::from_utf8(buf).unwrap()))
  };
  let trace_path = read_cstr(&mut buf, &mut tarfile).unwrap();
  if !force {
    match std::fs::read_to_string(&trace_path) {
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
      res =>
        if trace == res.unwrap().parse::<u64>().expect("expected .trace file") {
          return Ok(())
        },
    }
  }
  let prefix = trace_path.parent().unwrap();
  std::fs::create_dir_all(prefix).unwrap();
  std::fs::write(trace_path, format!("{trace}")).unwrap();
  let dict = zstd::dict::DecoderDictionary::copy(DICT_V1);
  while let Some(path) = read_cstr(&mut buf, &mut tarfile) {
    if verbose {
      println!("copying {}", path.display());
    }
    let compression = tarfile.read_u8().unwrap();
    buf.clear();
    buf.resize(tarfile.read_u64::<LE>().unwrap() as usize, 0);
    tarfile.read_exact(&mut buf).unwrap();
    let reader = std::io::Cursor::new(&*buf);
    let prefix = path.parent().unwrap();
    std::fs::create_dir_all(prefix).unwrap();
    match compression {
      0 => {
        let mut dec = zstd::stream::Decoder::new(reader).unwrap();
        std::io::copy(&mut dec, &mut File::create(path).unwrap()).unwrap();
      }
      1 => {
        let dec = zstd::stream::Decoder::with_prepared_dictionary(reader, &dict).unwrap();
        std::fs::write(path, &lgz::decompress(dec)).unwrap();
      }
      _ => panic!("unsupported compression {compression}"),
    }
  }
  Ok(())
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
  for file in args {
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
