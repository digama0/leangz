#[cfg(feature = "debug")]
use leangz::STATS;
use memmap2::Mmap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Read;
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
    let mut lgzs = 0;
    let mut oleans = 0;
    for file in args {
      println!("{file}");
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      let mut lgzfile = vec![];
      compress(&mmap, std::io::Cursor::new(&mut lgzfile));
      let oleanfile = decompress(std::io::Cursor::new(&lgzfile));
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
  #[cfg(feature = "debug")]
  if do_stats {
    let mut lgzs = 0;
    let mut oleans = 0;
    for file in args {
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      let mut out = leangz::WithPosition { r: std::io::sink(), pos: 0 };
      compress(&mmap, &mut out);
      lgzs += mmap.len();
      oleans += out.pos;
    }
    for (i, (n, e)) in STATS.normal.iter().zip(&STATS.exprish).enumerate() {
      println!("{i:x}: normal {} exprish {}", n.load(Ordering::Relaxed), e.load(Ordering::Relaxed));
    }
    println!("{} / {} = {:.6}", lgzs, oleans, lgzs as f64 / oleans as f64);
    return
  }
  if do_decompress {
    for file in args {
      if verbose {
        println!("{file}");
      }
      let outfile = outfile.take().unwrap_or_else(|| format!("{file}.olean"));
      let file = File::open(file).unwrap();
      std::fs::write(outfile, decompress(file)).unwrap()
    }
  } else {
    for file in args {
      if verbose {
        println!("{file}");
      }
      let outfile = outfile.take().unwrap_or_else(|| format!("{file}.lgz"));
      let mmap = unsafe { Mmap::map(&File::open(file).unwrap()).unwrap() };
      compress(&mmap, BufWriter::new(File::create(outfile).unwrap()));
    }
  }
}

fn compress(olean: &[u8], outfile: impl Write) {
  #[cfg(feature = "flate2")]
  let outfile = flate2::write::GzEncoder::new(outfile, flate2::Compression::new(7));
  #[cfg(feature = "zstd")]
  let outfile = zstd::stream::Encoder::new(outfile, 19).unwrap();
  let mut outfile = outfile;
  leangz::compress(olean, &mut outfile);
  #[cfg(any(feature = "flate2", feature = "zstd"))]
  outfile.finish().unwrap();
  #[cfg(not(any(feature = "flate2", feature = "zstd")))]
  outfile.flush().unwrap();
}

fn decompress(infile: impl Read) -> Vec<u8> {
  #[cfg(feature = "flate2")]
  let infile = flate2::read::GzDecoder::new(infile);
  #[cfg(feature = "zstd")]
  let infile = zstd::stream::Decoder::new(infile).unwrap();
  leangz::decompress(infile)
}
