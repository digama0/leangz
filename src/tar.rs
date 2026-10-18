use leangz::ltar;
use leangz::ltar::UnpackError;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

/// Parses a JSON record's `base`: a string, or a list of strings or nulls.
/// Entry `i` overrides basedir `i` (`-C`); null keeps it.
fn parse_base(j: &serde_json::Map<String, serde_json::Value>) -> Vec<Option<PathBuf>> {
  match j.get("base") {
    None => vec![],
    Some(b) => match b.as_array() {
      Some(arr) => arr
        .iter()
        .map(|v| {
          if v.is_null() {
            None::<PathBuf>
          } else {
            Some(v.as_str().expect("expected string or null").into())
          }
        })
        .collect(),
      None => vec![Some(b.as_str().expect("expected string or array").into())],
    },
  }
}

/// `basedirs` with a record's `base` (see `parse_base`) applied.
fn apply_base<'a>(
  basedirs: &'a Vec<PathBuf>, base: &[Option<PathBuf>], file: &str,
) -> Cow<'a, Vec<PathBuf>> {
  let mut basedirs = Cow::Borrowed(basedirs);
  for (i, basedir2) in base.iter().enumerate() {
    if let Some(basedir2) = basedir2 {
      let basedirs = basedirs.to_mut();
      if let Some(b) = basedirs.get_mut(i) {
        b.clone_from(basedir2)
      } else {
        assert_eq!(i, basedirs.len(), "{file}: missing basedir {}", basedirs.len());
        basedirs.push(basedir2.clone())
      }
    }
  }
  basedirs
}

fn main() {
  let help = || {
    eprintln!(
      "leantar {} lean (de)compression utility\
      \n\
      \nusage:\
      \n* leantar [OPTS] {{-d,-x}} [FILE.ltar ...]\
      \n  Decompress each file FILE.ltar into the current directory.\
      \n  * If one of the arguments is '-', then additional files are read from stdin,\
      \n    one filename per line.\
      \n  * With '-j -', stdin is instead read as JSON: a list of records of the form\
      \n      {{ file: string, base?: string | [string], hash?: string }}\
      \n    where \"foo\" is interpreted like {{\"file\": \"foo\"}}.\
      \n    * file: the ltar file to decompress\
      \n    * base: the extraction base (overrides -C)\
      \n    * hash: a (hex) depHash to write into the unpacked .trace, replacing the\
      \n      one stored in the .ltar (required if the file was packed with -s)\
      \n\
      \n* leantar [OPTS] OUT.ltar FILE.trace [(FILE | -c COMMENT) ...]\
      \n  Compress files FILE.trace and FILE ... into OUT.ltar\
      \n  * -c COMMENT will add a comment to the file which can be recovered using -k\
      \n\
      \n* leantar [OPTS] -p -\
      \n  Compress many archives, reading stdin as JSON: a list of records of the form\
      \n      {{ out: string, trace: string, files?: [string], base?: string | [string] }}\
      \n  each like `leantar [-C DIR ...] OUT.ltar FILE.trace FILE ...` (files take the\
      \n  same -i and -c forms), packed in parallel (--jobs), preparing the dictionary\
      \n  and compression contexts once per thread.\
      \n    * base: overrides -C as for -x -j: entry i replaces the i-th -C directory\
      \n      (null keeps it), and a string is a one-entry list\
      \n\
      \n* leantar [OPTS] -k FILE.ltar\
      \n  Unpack comments in FILE.ltar\
      \n\
      \ngeneral options:\
      \n  --version   Prints the version and exits\
      \n  --help      Prints the help and exits\
      \n  -v          Show verbose information\
      \n  -C <DIR>    Use DIR instead of current dir as extraction base\
      \n              (can be overridden per-file with the JSON input)\
      \n\
      \ncompress opts:\
      \n  -s          Strip depHash from .ltar output\
      \n  --level <N> zstd compression level (default: 19); unpacks the same\
      \n  --jobs <N>  With -p, pack with N threads (default: num CPUs)\
      \n\
      \ndecompress opts:\
      \n  -f          Always unpack even if a matching trace file exists\
      \n  -j          Read the '-' stdin input as JSON (default: one filename per line)\
      \n  -r, --delete-corrupted\
      \n              Delete input FILE.ltar files if they fail parsing\
      \n  --jobs <N>  Unpack files with N threads (default: num CPUs)",
      env!("CARGO_PKG_VERSION")
    )
  };
  let help_err = |s: &str| {
    eprintln!("error: {s}");
    help();
    std::process::exit(1)
  };
  let mut do_decompress = false;
  let mut do_show_comments = false;
  let mut verbose = false;
  let mut force = false;
  let mut from_stdin = false;
  let mut json_stdin = false;
  let mut delete_corrupted = false;
  let mut include_hash = true;
  let mut do_pack_json = false;
  let mut num_jobs: Option<usize> = None;
  let mut level: Option<i32> = None;
  let mut basedirs = vec![];
  let mut args = std::env::args();
  args.next();
  let mut args = args.peekable();
  while let Some(arg) = args.peek() {
    match &**arg {
      "--version" => {
        println!("leantar {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
      }
      "--help" => {
        help();
        std::process::exit(0)
      }
      "-v" => {
        verbose = true;
        args.next();
      }
      "-f" => {
        force = true;
        args.next();
      }
      "-j" => {
        json_stdin = true;
        args.next();
      }
      "-r" | "--delete-corrupted" => {
        delete_corrupted = true;
        args.next();
      }
      "--jobs" => {
        args.next();
        let jobs = args
          .next()
          .unwrap_or_else(|| help_err("--jobs missing argument"))
          .parse::<usize>()
          .unwrap();
        num_jobs = Some(jobs);
      }
      "-d" | "-x" => {
        do_decompress = true;
        args.next();
      }
      "-C" => {
        args.next();
        basedirs
          .push(PathBuf::from(args.next().unwrap_or_else(|| help_err("-C missing argument"))));
      }
      "-k" => {
        do_show_comments = true;
        args.next();
      }
      "-s" => {
        include_hash = false;
        args.next();
      }
      "-p" => {
        do_pack_json = true;
        args.next();
      }
      "--level" => {
        args.next();
        level = Some(
          args
            .next()
            .unwrap_or_else(|| help_err("--level missing argument"))
            .parse::<i32>()
            .unwrap_or_else(|_| {
              help_err("--level expects a number");
              unreachable!()
            }),
        );
      }
      _ => break,
    }
  }
  // Packing recurses over an .olean's objects, deeper than a worker thread's
  // default stack allows; the main thread, which packs a single archive, has
  // the process's.
  let mut pool = ThreadPoolBuilder::new();
  if let Some(jobs) = num_jobs {
    pool = pool.num_threads(jobs)
  }
  if do_pack_json {
    pool = pool.stack_size(256 << 20)
  }
  pool.build_global().unwrap();
  if basedirs.is_empty() {
    basedirs.push(".".into())
  }
  if do_pack_json {
    if args.next().as_deref() != Some("-") {
      help_err("-p expects '-'");
    }
    let str = std::io::read_to_string(std::io::stdin()).unwrap();
    let strings = |v: Option<&serde_json::Value>| -> Vec<String> {
      v.map(|v| {
        v.as_array()
          .expect("expected array")
          .iter()
          .map(|s| s.as_str().expect("expected string").to_owned())
          .collect()
      })
      .unwrap_or_default()
    };
    let jobs: Vec<_> = serde_json::from_str::<Vec<serde_json::Value>>(&str)
      .unwrap()
      .into_iter()
      .map(|j| {
        let j = j.as_object().expect("expected object");
        let out = j["out"].as_str().expect("expected string").to_owned();
        let trace = j["trace"].as_str().expect("expected string").to_owned();
        let base = apply_base(&basedirs, &parse_base(j), &out).into_owned();
        (out, trace, strings(j.get("files")), base)
      })
      .collect();
    let error = AtomicBool::new(false);
    jobs.into_par_iter().for_each_init(
      || ltar::Compressors::new(level).unwrap(),
      |comp, (out, trace, files, base)| {
        if verbose {
          println!("packing {out}");
        }
        let res = (|| -> io::Result<()> {
          let mut temp = leangz::TempFile::new(out.clone().into())?;
          ltar::pack_with(&base, BufWriter::new(&mut *temp), &trace, files, include_hash, verbose,
            Some(comp))?;
          temp.save().map(drop)
        })();
        if let Err(e) = res {
          eprintln!("{out}: {e}");
          error.store(true, std::sync::atomic::Ordering::Relaxed)
        }
      },
    );
    std::process::exit(error.into_inner() as i32);
  } else if do_show_comments {
    let file = args.next().unwrap_or_else(|| help_err("expected FILE.ltar"));
    let tarfile = match File::open(&file) {
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
        eprintln!("{file} not found");
        std::process::exit(1);
      }
      e => BufReader::new(e.unwrap()),
    };
    match ltar::comments(tarfile) {
      Err(e) => {
        eprintln!("{e}");
        std::process::exit(1);
      }
      Ok(comments) =>
        for comment in comments {
          println!("{comment}")
        },
    }
  } else if do_decompress {
    struct Arg {
      base: Vec<Option<PathBuf>>,
      file: String,
      hash: Option<u64>,
    }
    fn from_file(file: String) -> Arg { Arg { base: vec![], file, hash: None } }
    let mut args_vec = vec![];
    for arg in args {
      if arg == "-" {
        assert!(!from_stdin, "two stdin inputs");
        from_stdin = true;
        if json_stdin {
          let str = std::io::read_to_string(std::io::stdin()).unwrap();
          for j in serde_json::from_str::<Vec<serde_json::Value>>(&str).unwrap() {
            args_vec.push(if let serde_json::Value::String(s) = j {
              from_file(s)
            } else {
              let j = j.as_object().expect("expected object");
              let file = j["file"].as_str().expect("expected string");
              let base = parse_base(j);
              let hash = j.get("hash").filter(|v| !v.is_null()).map(|value| {
                value
                  .as_str()
                  .and_then(|s| u64::from_str_radix(s, 16).ok())
                  .expect("expected hex hash")
              });
              Arg { base, file: file.into(), hash }
            })
          }
        } else {
          for arg in std::io::stdin().lines().map(|arg| arg.unwrap()) {
            args_vec.push(from_file(arg))
          }
        }
      } else {
        args_vec.push(from_file(arg))
      }
    }

    let mut error = AtomicBool::new(false);
    let fail = || error.store(true, std::sync::atomic::Ordering::Relaxed);
    args_vec.into_par_iter().for_each(|Arg { base, file, hash }| {
      if verbose {
        println!("unpacking {file}");
      }
      let basedirs = apply_base(&basedirs, &base, &file);
      let tarfile = match File::open(&file) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
          eprintln!("{file} not found");
          return fail()
        }
        e => BufReader::new(e.unwrap()),
      };
      if let Err(e) = ltar::unpack(&basedirs, tarfile, force, hash, verbose) {
        if matches!(&e, UnpackError::IOError(e)
          if matches!(e.kind(), io::ErrorKind::UnexpectedEof))
        {
          if delete_corrupted {
            eprintln!("{file}: removing corrupted file");
            let _ = std::fs::remove_file(file);
          } else {
            eprintln!("{file}: file is corrupted, try deleting or redownloading it");
            fail()
          }
        } else {
          eprintln!("{file}: {e}");
          fail()
        }
      }
    });
    std::process::exit(*error.get_mut() as i32);
  } else {
    let tarfile = args.next().unwrap_or_else(|| help_err("expected OUT.ltar"));
    let trace_path = args.next().unwrap_or_else(|| help_err("expected FILE.trace"));
    let mut temp = leangz::TempFile::new(tarfile.into()).unwrap();
    let mut comp = level.map(|level| ltar::Compressors::new(Some(level)).unwrap());
    ltar::pack_with(&basedirs, BufWriter::new(&mut *temp), &trace_path, args, include_hash, verbose,
      comp.as_mut())
    .unwrap();
    temp.save().unwrap();
  }
}
