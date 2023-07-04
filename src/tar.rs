use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LE;
#[cfg(feature = "debug")]
use leangz::STATS;
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
#[cfg(feature = "debug")]
use std::sync::atomic::Ordering;

const COMPRESSION_LEVEL: i32 = 19;
const DICT_V1: &[u8] = include_bytes!("../dict/v1.dict");

fn main() {
  let help = || panic!("usage: leantar [-v] [-d|-x] [-C BASEDIR] OUT.ltar FILE.trace [FILE ...]");
  let mut do_decompress = false;
  let mut verbose = false;
  let mut basedir = None;
  let mut args = std::env::args();
  args.next();
  let mut args = args.peekable();
  while let Some(arg) = args.peek() {
    match &**arg {
      "-v" => {
        verbose = true;
        args.next();
      }
      "-d" | "-x" => {
        do_decompress = true;
        args.next();
      }
      "-C" => {
        args.next();
        if basedir.replace(args.next().unwrap_or_else(help)).is_some() {
          help();
        }
      }
      _ => break,
    }
  }
  let basedir = PathBuf::from(basedir.unwrap_or_else(|| format!(".")));
  if do_decompress {
    args.collect::<Vec<_>>().into_par_iter().for_each(|tarfile| {
      let mut tarfile = BufReader::new(File::open(tarfile).unwrap());
      let mut buf = vec![0; 4];
      tarfile.read_exact(&mut buf).unwrap();
      assert!(buf == *b"LTAR");
      let trace = tarfile.read_u64::<LE>().unwrap();
      let read_cstr = |buf: &mut Vec<_>, tarfile: &mut BufReader<File>| {
        buf.clear();
        tarfile.read_until(0, buf).unwrap();
        buf.pop().map(|_| basedir.join(std::str::from_utf8(buf).unwrap()))
      };
      let trace_path = read_cstr(&mut buf, &mut tarfile).unwrap();
      match std::fs::read_to_string(&trace_path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        res =>
          if trace == res.unwrap().parse::<u64>().expect("expected .trace file") {
            return
          },
      }
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
        match compression {
          0 => {
            let mut dec = zstd::stream::Decoder::new(reader).unwrap();
            std::io::copy(&mut dec, &mut File::create(path).unwrap()).unwrap();
          }
          1 => {
            let dec = zstd::stream::Decoder::with_prepared_dictionary(reader, &dict).unwrap();
            std::fs::write(path, &leangz::decompress(dec)).unwrap();
          }
          _ => panic!("unsupported compression {compression}"),
        }
      }
    })
  } else {
    let tarfile = args.next().unwrap_or_else(help);
    let trace_path = args.next().unwrap_or_else(help);
    let trace = std::fs::read_to_string(basedir.join(&trace_path)).unwrap();
    let trace = trace.parse::<u64>().expect("expected .trace file");
    let mut tarfile = BufWriter::new(File::create(tarfile).unwrap());
    tarfile.write_all(b"LTAR").unwrap();
    tarfile.write_u64::<LE>(trace).unwrap();
    tarfile.write_all(trace_path.as_bytes()).unwrap();
    tarfile.write_u8(0).unwrap();
    let dict_v1 = zstd::dict::EncoderDictionary::copy(DICT_V1, COMPRESSION_LEVEL);
    for file in args {
      tarfile.write_all(file.as_bytes()).unwrap();
      tarfile.write_u8(0).unwrap();
      let path = basedir.join(file);
      if verbose {
        println!("compressing {}", path.display());
      }
      let mmap = unsafe { Mmap::map(&File::open(path).unwrap()).unwrap() };
      let mut buf = vec![];
      let mut enc;
      if mmap.get(..16) == Some(b"oleanfile!!!!!!!") {
        tarfile.write_u8(1).unwrap();
        enc = zstd::stream::Encoder::with_prepared_dictionary(&mut buf, &dict_v1).unwrap();
        leangz::compress(&mmap, &mut enc);
      } else {
        tarfile.write_u8(0).unwrap();
        enc = zstd::stream::Encoder::new(&mut buf, COMPRESSION_LEVEL).unwrap();
        enc.write_all(&mmap).unwrap();
      }
      enc.finish().unwrap();
      tarfile.write_u64::<LE>(buf.len() as u64).unwrap();
      tarfile.write_all(&buf).unwrap();
    }
  }
}
