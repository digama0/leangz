#[cfg(feature = "debug")]
use leangz::lgz::STATS;
use memmap2::Mmap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Write;
#[cfg(feature = "debug")]
use std::sync::atomic::Ordering;

fn main() {
  let help = || panic!("usage: leangz FILE.olean");
  let mut do_decompress = false;
  #[cfg(feature = "debug")]
  let mut do_selftest = false;
  #[cfg(feature = "debug")]
  let mut do_stats = false;
  #[cfg(feature = "zstd-train")]
  let mut do_training = false;
  let mut verbose = false;
  let mut outfile = None;
  let mut args = std::env::args();
  args.next();
  let mut args = args.peekable();
  while let Some(arg) = args.peek() {
    match &**arg {
      "-v" => {
        verbose = true;
        args.next();
      }
      #[cfg(feature = "debug")]
      "-t" => {
        do_selftest = true;
        args.next();
      }
      #[cfg(feature = "zstd-train")]
      "--train" => {
        do_training = true;
        args.next();
      }
      #[cfg(feature = "debug")]
      "-s" => {
        do_stats = true;
        args.next();
      }
      "-d" | "-x" => {
        do_decompress = true;
        args.next();
      }
      "-o" => {
        args.next();
        if outfile.replace(args.next().unwrap_or_else(help)).is_some() {
          help();
        }
      }
      _ => break,
    }
  }
  #[cfg(feature = "debug")]
  if do_selftest {
    let compressor = Compressor::new();
    let decompressor = Decompressor::new();
    let mut lgzs = 0;
    let mut oleans = 0;
    for file in args {
      println!("{file}");
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      let mut lgzfile = vec![];
      compressor.compress(&mmap, std::io::Cursor::new(&mut lgzfile));
      let oleanfile = decompressor.decompress(std::io::Cursor::new(&lgzfile));
      assert!(*mmap == *oleanfile);
      lgzs += lgzfile.len();
      oleans += oleanfile.len();
      println!(
        "{} / {} = {:.6}",
        oleanfile.len(),
        lgzfile.len(),
        oleanfile.len() as f64 / lgzfile.len() as f64
      );
    }
    println!("{} / {} = {:.6}", oleans, lgzs, oleans as f64 / lgzs as f64);
    return
  }
  #[cfg(feature = "zstd-train")]
  if do_training {
    let mut samples = vec![];
    let mut sizes = vec![];
    let mut max_size = 0;
    for file in args {
      if verbose {
        println!("{file}");
      }
      let start = samples.len();
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      leangz::compress(&mmap, std::io::Cursor::new(&mut samples));
      let size = samples.len() - start;
      sizes.push(size);
      max_size = max_size.max(size);
    }
    println!("starting training");
    let outfile = outfile.take().unwrap_or_else(|| format!("src/lgz.dict"));
    let dict = zstd::dict::from_continuous(&samples, &sizes, max_size).unwrap();
    std::fs::write(outfile, dict).unwrap();
    return
  }
  if do_decompress {
    let decompressor = Decompressor::new();
    for file in args {
      if verbose {
        println!("{file}");
      }
      let outfile = outfile.take().unwrap_or_else(|| format!("{file}.olean"));
      let file = std::io::BufReader::new(File::open(file).unwrap());
      std::fs::write(outfile, decompressor.decompress(file)).unwrap()
    }
  } else {
    let compressor = Compressor::new();
    let mut lgzs = 0;
    let mut oleans = 0;
    #[cfg(feature = "debug")]
    if do_stats {
      for file in args {
        let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
        let mut w = leangz::lgz::WithPosition { r: std::io::sink(), pos: 0 };
        compressor.compress(&mmap, &mut w);
        oleans += mmap.len();
        lgzs += w.pos;
      }
      for (i, (n, e)) in STATS.normal.iter().zip(&STATS.exprish).enumerate() {
        println!(
          "{i:x}: normal {} exprish {}",
          n.load(Ordering::Relaxed),
          e.load(Ordering::Relaxed)
        );
      }
      println!("{} / {} = {:.6}", oleans, lgzs, oleans as f64 / lgzs as f64);
      return
    }
    for file in args {
      if verbose {
        println!("{file}");
      }
      let outfile = outfile.take().unwrap_or_else(|| format!("{file}.lgz"));
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      oleans += mmap.len();
      let outfile = BufWriter::new(File::create(outfile).unwrap());
      let mut w = leangz::lgz::WithPosition { r: outfile, pos: 0 };
      compressor.compress(&mmap, &mut w);
      lgzs += w.pos;
    }
    println!("{} / {} = {:.6}", oleans, lgzs, oleans as f64 / lgzs as f64);
  }
}

#[cfg(feature = "zstd")]
const COMPRESSION_LEVEL: i32 = 19;

struct Compressor<'a> {
  #[cfg(feature = "zstd-dict")]
  dict: zstd::dict::EncoderDictionary<'a>,
  _phantom: &'a (),
}

#[cfg(feature = "zstd-dict")]
const ZSTD_DICT_V1: &[u8] = include_bytes!("../dict/v1.dict");

impl Compressor<'_> {
  fn new() -> Self {
    Compressor {
      #[cfg(feature = "zstd-dict")]
      dict: zstd::dict::EncoderDictionary::copy(ZSTD_DICT_V1, COMPRESSION_LEVEL),
      _phantom: &(),
    }
  }

  fn compress(&self, olean: &[u8], outfile: impl Write) {
    #[cfg(feature = "flate2")]
    let outfile = flate2::write::GzEncoder::new(outfile, flate2::Compression::new(7));
    #[cfg(all(feature = "zstd", not(feature = "zstd-dict")))]
    let outfile = zstd::stream::Encoder::new(outfile, COMPRESSION_LEVEL).unwrap();
    #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
    let outfile = zstd::stream::Encoder::with_prepared_dictionary(outfile, &self.dict).unwrap();
    let mut outfile = outfile;
    leangz::lgz::compress(olean, &mut outfile);
    #[cfg(any(feature = "flate2", feature = "zstd"))]
    outfile.finish().unwrap();
    #[cfg(not(any(feature = "flate2", feature = "zstd")))]
    outfile.flush().unwrap();
  }
}

struct Decompressor<'a> {
  #[cfg(feature = "zstd-dict")]
  dict: zstd::dict::DecoderDictionary<'a>,
  _phantom: &'a (),
}

impl Decompressor<'_> {
  fn new() -> Self {
    Decompressor {
      #[cfg(feature = "zstd-dict")]
      dict: zstd::dict::DecoderDictionary::copy(ZSTD_DICT_V1),
      _phantom: &(),
    }
  }
  fn decompress(&self, infile: impl BufRead) -> Vec<u8> {
    #[cfg(feature = "flate2")]
    let infile = flate2::read::GzDecoder::new(infile);
    #[cfg(all(feature = "zstd", not(feature = "zstd-dict")))]
    let infile = zstd::stream::Decoder::new(infile).unwrap();
    #[cfg(all(feature = "zstd", feature = "zstd-dict"))]
    let infile = zstd::stream::Decoder::with_prepared_dictionary(infile, &self.dict).unwrap();
    leangz::lgz::decompress(infile)
  }
}
