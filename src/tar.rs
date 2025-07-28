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

fn main() {
  let help = || {
    eprintln!(
      "leantar {} lean (de)compression utility\
      \n\
      \nusage:\
      \n* leantar [OPTS] {{-d,-x}} [FILE.ltar ...]\
      \n  Decompress each file FILE.ltar into the current directory.\
      \n  * If one of the arguments is '-', then additional files are read from stdin.\
      \n\
      \n* leantar [OPTS] OUT.ltar FILE.trace [(FILE | -c COMMENT) ...]\
      \n  Compress files FILE.trace and FILE ... into OUT.ltar\
      \n  * -c COMMENT will add a comment to the file which can be recovered using -k\
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
      \ndecompress opts:\
      \n  -f          Always unpack even if a matching trace file exists\
      \n  -j          Expect stdin input in JSON format\
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
  let mut json_stdin = true;
  let mut delete_corrupted = true;
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
        ThreadPoolBuilder::new().num_threads(jobs).build_global().unwrap();
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
      _ => break,
    }
  }
  if basedirs.is_empty() {
    basedirs.push(".".into())
  }
  if do_show_comments {
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
    let mut args_vec = vec![];
    for arg in args {
      if arg == "-" {
        assert!(!from_stdin, "two stdin inputs");
        from_stdin = true;
        if json_stdin {
          let str = std::io::read_to_string(std::io::stdin()).unwrap();
          for j in serde_json::from_str::<Vec<serde_json::Value>>(&str).unwrap() {
            args_vec.push(if let serde_json::Value::String(s) = j {
              (vec![], s)
            } else {
              let j = j.as_object().expect("expected object");
              let file = j["file"].as_str().expect("expected string");
              let base = match j.get("base") {
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
              };
              (base, file.into())
            })
          }
        } else {
          for arg in std::io::stdin().lines().map(|arg| arg.unwrap()) {
            args_vec.push((vec![], arg))
          }
        }
      } else {
        args_vec.push((vec![], arg))
      }
    }

    let mut error = AtomicBool::new(false);
    let fail = || error.store(true, std::sync::atomic::Ordering::Relaxed);
    args_vec.into_par_iter().for_each(|(basedirs2, file)| {
      if verbose {
        println!("unpacking {file}");
      }
      let mut basedirs = Cow::Borrowed(&basedirs);
      for (i, basedir2) in basedirs2.iter().enumerate() {
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
      let tarfile = match File::open(&file) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
          eprintln!("{file} not found");
          return fail()
        }
        e => BufReader::new(e.unwrap()),
      };
      if let Err(e) = ltar::unpack(&basedirs, tarfile, force, verbose) {
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
    let tarfile = BufWriter::new(File::create(tarfile).unwrap());
    ltar::pack(&basedirs, tarfile, &trace_path, args, verbose).unwrap()
  }
}
