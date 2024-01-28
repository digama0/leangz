use leangz::ltar;
use leangz::ltar::UnpackError;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

fn main() {
  let help = || panic!("usage: leantar [-v] [-d|-x] [-C BASEDIR] OUT.ltar FILE.trace [FILE ...]");
  let mut do_decompress = false;
  let mut do_show_comments = false;
  let mut verbose = false;
  let mut force = false;
  let mut from_stdin = false;
  let mut json_stdin = true;
  let mut delete_corrupted = true;
  let mut basedir = None;
  let mut args = std::env::args();
  args.next();
  let mut args = args.peekable();
  while let Some(arg) = args.peek() {
    match &**arg {
      "--version" => {
        println!("leantar {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
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
        let jobs = args.next().unwrap_or_else(help).parse::<usize>().unwrap();
        ThreadPoolBuilder::new().num_threads(jobs).build_global().unwrap();
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
      "-k" => {
        do_show_comments = true;
        args.next();
      }
      _ => break,
    }
  }
  let basedir = PathBuf::from(basedir.unwrap_or_else(|| ".".into()));
  if do_show_comments {
    let file = args.next().unwrap_or_else(help);
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
              (None, s)
            } else {
              let j = j.as_object().expect("expected object");
              let file = j["file"].as_str().expect("expected string");
              let base = j.get("base").map(|b| b.as_str().expect("expected string").into());
              (base, file.into())
            })
          }
        } else {
          for arg in std::io::stdin().lines().map(|arg| arg.unwrap()) {
            args_vec.push((None, arg))
          }
        }
      } else {
        args_vec.push((None, arg))
      }
    }

    let mut error = AtomicBool::new(false);
    let fail = || error.store(true, std::sync::atomic::Ordering::Relaxed);
    args_vec.into_par_iter().for_each(|(basedir2, file)| {
      if verbose {
        println!("unpacking {file}");
      }
      let basedir = basedir2.as_ref().unwrap_or(&basedir);
      let tarfile = match File::open(&file) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
          eprintln!("{file} not found");
          return fail()
        }
        e => BufReader::new(e.unwrap()),
      };
      if let Err(e) = ltar::unpack(basedir, tarfile, force, verbose) {
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
    let tarfile = args.next().unwrap_or_else(help);
    let trace_path = args.next().unwrap_or_else(help);
    let tarfile = BufWriter::new(File::create(tarfile).unwrap());
    ltar::pack(&basedir, tarfile, &trace_path, args, verbose).unwrap()
  }
}
