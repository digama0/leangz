//! Benchmark tool for leantar decompression profiling
//!
//! Usage:
//!   leantar-benchmark [OPTIONS] <CACHE_DIR>
//!
//! This tool profiles the decompression of .ltar files and produces a detailed
//! breakdown of where time is spent.

use leangz::ltar;
use leangz::ltar::UnpackError;
use leangz::profile::{self, ProfileSnapshot};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

fn main() {
  let mut args = std::env::args();
  args.next(); // skip binary name

  let mut cache_dir: Option<PathBuf> = None;
  let mut output_dir: Option<PathBuf> = None;
  let mut json_output = false;
  let mut dry_run = false;
  let mut num_threads: Option<usize> = None;
  let mut limit: Option<usize> = None;
  let mut manifest_file: Option<PathBuf> = None;
  let mut create_manifest: Option<String> = None;
  let mut sweep_mode = false;

  while let Some(arg) = args.next() {
    match arg.as_str() {
      "--help" | "-h" => {
        print_help();
        std::process::exit(0);
      }
      "--json" => json_output = true,
      "--dry-run" | "-n" => dry_run = true,
      "--sweep" => sweep_mode = true,
      "--threads" | "-j" => {
        num_threads = Some(
          args
            .next()
            .expect("--threads requires argument")
            .parse()
            .expect("--threads requires number"),
        );
      }
      "--limit" | "-l" => {
        limit = Some(
          args.next().expect("--limit requires argument").parse().expect("--limit requires number"),
        );
      }
      "--output" | "-o" => {
        output_dir = Some(PathBuf::from(args.next().expect("--output requires argument")));
      }
      "--manifest" | "-m" => {
        manifest_file = Some(PathBuf::from(args.next().expect("--manifest requires argument")));
      }
      "--create-manifest" => {
        create_manifest =
          Some(args.next().expect("--create-manifest requires a mathlib tag or commit"));
      }
      arg if arg.starts_with('-') => {
        eprintln!("Unknown option: {}", arg);
        std::process::exit(1);
      }
      dir => {
        if cache_dir.is_some() {
          eprintln!("Multiple cache directories specified");
          std::process::exit(1);
        }
        cache_dir = Some(PathBuf::from(dir));
      }
    }
  }

  // Handle --sweep mode: run benchmark with different thread counts
  if sweep_mode {
    run_sweep(manifest_file, cache_dir, limit);
    std::process::exit(0);
  }

  // Handle --create-manifest mode
  if let Some(ref tag) = create_manifest {
    create_manifest_for_tag(tag);
    std::process::exit(0);
  }

  // Get the benchmarks directory - check current directory first, then relative to binary
  let benchmarks_dir = {
    let cwd_benchmarks = PathBuf::from("benchmarks");
    if cwd_benchmarks.exists() {
      cwd_benchmarks
    } else {
      let exe_path = std::env::current_exe().expect("Failed to get executable path");
      exe_path
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent())
        .map(|p| p.join("benchmarks"))
        .unwrap_or(cwd_benchmarks)
    }
  };

  // Auto-detect manifest if not specified
  let manifest_file = manifest_file.or_else(|| {
    if !benchmarks_dir.exists() {
      return None;
    }
    let manifests: Vec<PathBuf> = std::fs::read_dir(&benchmarks_dir)
      .ok()?
      .filter_map(|e| e.ok())
      .filter(|e| e.path().extension().map(|x| x == "manifest").unwrap_or(false))
      .map(|e| e.path())
      .collect();

    match manifests.len() {
      0 => None,
      1 => {
        eprintln!("Using manifest: {}", manifests[0].display());
        Some(manifests[0].clone())
      }
      _ => {
        eprintln!("Error: Multiple manifests found in {}:", benchmarks_dir.display());
        for m in &manifests {
          eprintln!("  - {}", m.file_name().unwrap().to_string_lossy());
        }
        eprintln!();
        eprintln!("Please specify one with -m/--manifest");
        std::process::exit(1);
      }
    }
  });

  // Require a manifest
  let manifest_file = manifest_file.unwrap_or_else(|| {
    eprintln!("Error: No benchmark manifest found.");
    eprintln!();
    eprintln!("To create a manifest for a specific mathlib version:");
    eprintln!("  leantar-benchmark --create-manifest v4.26.0-rc1");
    eprintln!();
    eprintln!("This will clone mathlib, download cache files, and save");
    eprintln!("the manifest to benchmarks/mathlib-<TAG>.manifest");
    eprintln!();
    eprintln!("Or specify an existing manifest with -m/--manifest");
    std::process::exit(1);
  });

  // Default cache directory to ~/.cache/mathlib
  let cache_dir = cache_dir.unwrap_or_else(|| {
    std::env::var("HOME")
      .map(|h| PathBuf::from(h).join(".cache/mathlib"))
      .unwrap_or_else(|_| PathBuf::from("~/.cache/mathlib"))
  });

  // Use temp dir if no output specified and not dry-run
  let output_dir = output_dir.unwrap_or_else(|| {
    if dry_run {
      PathBuf::from("/dev/null")
    } else {
      std::env::temp_dir().join("leantar-benchmark")
    }
  });

  // Configure thread pool
  if let Some(threads) = num_threads {
    ThreadPoolBuilder::new()
      .num_threads(threads)
      .build_global()
      .expect("Failed to configure thread pool");
  }

  let manifest_name = manifest_file
    .file_name()
    .map(|n| n.to_string_lossy().to_string())
    .unwrap_or_else(|| "unknown".to_string());

  eprintln!("Manifest:    {}", manifest_name);
  eprintln!("Cache:       {}", cache_dir.display());
  eprintln!("Output:      {}", output_dir.display());
  eprintln!("Threads:     {}", rayon::current_num_threads());
  if dry_run {
    eprintln!("Dry run:     yes");
  }
  if let Some(l) = limit {
    eprintln!("File limit:  {}", l);
  }
  eprintln!();

  // Read file hashes from manifest (one per line)
  let file = File::open(&manifest_file).expect("Failed to open manifest file");
  let reader = BufReader::new(file);
  let mut ltar_files: Vec<PathBuf> = reader
    .lines()
    .map_while(Result::ok)
    .filter(|line| !line.is_empty() && !line.starts_with('#'))
    .map(|hash| {
      // Support both bare hashes and full paths
      if hash.contains('/') || hash.contains('\\') {
        PathBuf::from(hash)
      } else {
        cache_dir.join(format!("{}.ltar", hash.trim_end_matches(".ltar")))
      }
    })
    .collect();

  ltar_files.sort();

  if let Some(l) = limit {
    ltar_files.truncate(l);
  }

  eprintln!("Found {} .ltar files", ltar_files.len());

  if ltar_files.is_empty() {
    eprintln!("No .ltar files found!");
    std::process::exit(1);
  }

  // Create output directory
  if !dry_run {
    std::fs::create_dir_all(&output_dir).expect("Failed to create output directory");
  }

  // Reset profiling counters
  profile::reset();

  // Run decompression
  let mut error = AtomicBool::new(false);
  let fail = || error.store(true, Ordering::Relaxed);

  eprintln!("Starting decompression...");
  let start = Instant::now();

  let basedirs = vec![output_dir.clone()];

  ltar_files.into_par_iter().for_each(|file| {
    let tarfile = match File::open(&file) {
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
        eprintln!("{} not found", file.display());
        return fail();
      }
      Err(e) => {
        eprintln!("{}: {}", file.display(), e);
        return fail();
      }
      Ok(f) => BufReader::new(f),
    };

    // Use force=false to skip files that already exist
    // Use verbose=false to reduce output
    if let Err(e) = ltar::unpack(&basedirs, tarfile, false, false) {
      if matches!(&e, UnpackError::IOError(e)
                if matches!(e.kind(), io::ErrorKind::UnexpectedEof))
      {
        eprintln!("{}: corrupted file", file.display());
      } else {
        eprintln!("{}: {}", file.display(), e);
      }
      fail();
    }
  });

  let wall_time = start.elapsed();

  // Capture profiling results
  let mut snapshot = ProfileSnapshot::capture();
  snapshot.wall_time_us = wall_time.as_micros() as usize;

  eprintln!();

  if json_output {
    snapshot.print_json();
  } else {
    snapshot.print_report();
  }

  // Cleanup temp directory
  if !dry_run && output_dir.starts_with(std::env::temp_dir()) {
    eprintln!();
    eprintln!("Cleaning up temporary output directory...");
    let _ = std::fs::remove_dir_all(&output_dir);
  }

  if *error.get_mut() {
    std::process::exit(1);
  }
}

fn print_help() {
  eprintln!(
    r#"leantar-benchmark - Profile leantar decompression performance

USAGE:
    leantar-benchmark [OPTIONS]

OPTIONS:
    -h, --help                   Print help information
    -j, --threads <N>            Number of threads to use (default: num CPUs)
    -l, --limit <N>              Only process first N .ltar files (sorted by name)
    -o, --output <DIR>           Output directory (default: temp directory)
    -n, --dry-run                Don't actually write files (benchmark overhead only)
    -m, --manifest <FILE>        Manifest file to use (default: auto-detect from benchmarks/)
    --create-manifest <TAG>      Create manifest for mathlib tag/commit and exit
    --sweep                      Test different thread counts and show comparison
    --json                       Output results in JSON format
    <CACHE_DIR>                  Cache directory (default: ~/.cache/mathlib)

EXAMPLES:
    # Run benchmark with defaults (uses single manifest from benchmarks/)
    leantar-benchmark

    # Find optimal thread count for your system
    leantar-benchmark --sweep

    # Create a reproducible manifest for a specific mathlib version
    leantar-benchmark --create-manifest v4.26.0-rc1

    # Use 4 threads and limit to 100 files
    leantar-benchmark -j 4 -l 100

    # Use specific manifest and cache directory
    leantar-benchmark -m benchmarks/mathlib-v4.26.0-rc1.manifest ~/.cache/mathlib

    # Output JSON for programmatic consumption
    leantar-benchmark --json > results.json
"#
  );
}

/// Run benchmark with different thread counts to find optimal parallelism
fn run_sweep(manifest_file: Option<PathBuf>, cache_dir: Option<PathBuf>, limit: Option<usize>) {
  let num_cpus = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);

  // Thread counts to test: 1, 2, 4, 6, 8, 12, 16, up to num_cpus
  let mut thread_counts: Vec<usize> =
    vec![1, 2, 4, 6, 8, 12, 16, 24, 32].into_iter().filter(|&n| n <= num_cpus).collect();
  if !thread_counts.contains(&num_cpus) {
    thread_counts.push(num_cpus);
  }
  thread_counts.sort();
  thread_counts.dedup();

  eprintln!("=== Thread Count Sweep ===");
  eprintln!("Testing thread counts: {:?}", thread_counts);
  eprintln!("(This will take a few minutes...)");
  eprintln!();

  let exe = std::env::current_exe().expect("Failed to get executable path");

  let mut results: Vec<(usize, f64)> = Vec::new();

  for &threads in &thread_counts {
    eprint!("Testing {} threads... ", threads);

    let mut cmd = Command::new(&exe);
    cmd.arg("-j").arg(threads.to_string());
    cmd.arg("--json");

    if let Some(ref m) = manifest_file {
      cmd.arg("-m").arg(m);
    }
    if let Some(ref c) = cache_dir {
      cmd.arg(c);
    }
    if let Some(l) = limit {
      cmd.arg("-l").arg(l.to_string());
    }

    let output = cmd.output().expect("Failed to run benchmark");

    if !output.status.success() {
      eprintln!("FAILED");
      continue;
    }

    // Parse JSON output to get wall time
    let stdout = String::from_utf8_lossy(&output.stdout);
    if let Some(wall_ms) = parse_wall_time_from_json(&stdout) {
      let wall_s = wall_ms as f64 / 1000.0;
      eprintln!("{:.2}s", wall_s);
      results.push((threads, wall_s));
    } else {
      eprintln!("(parse error)");
    }
  }

  if results.is_empty() {
    eprintln!("No successful benchmark runs!");
    return;
  }

  // Find best result
  let (best_threads, best_time) =
    results.iter().min_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).unwrap();

  eprintln!();
  eprintln!("=== Results ===");
  eprintln!();
  eprintln!("{:>8} {:>10} {:>10}", "Threads", "Time", "vs Best");
  eprintln!("{:>8} {:>10} {:>10}", "-------", "--------", "-------");

  for (threads, time) in &results {
    let ratio = time / best_time;
    let indicator = if threads == best_threads { " <-- best" } else { "" };
    eprintln!("{:>8} {:>9.2}s {:>9.2}x{}", threads, time, ratio, indicator);
  }

  eprintln!();
  eprintln!("Optimal thread count: {} ({:.2}s)", best_threads, best_time);

  // Show speedup vs max threads
  if let Some((_, max_time)) = results.iter().find(|(t, _)| *t == num_cpus) {
    if *best_time < *max_time {
      let speedup = max_time / best_time;
      eprintln!("Speedup vs {} threads: {:.1}x", num_cpus, speedup);
    }
  }
}

fn parse_wall_time_from_json(json: &str) -> Option<usize> {
  // Simple JSON parsing for wall_time_ms field
  for line in json.lines() {
    let line = line.trim();
    if line.starts_with("\"wall_time_ms\"") {
      // "wall_time_ms": 12345,
      let parts: Vec<&str> = line.split(':').collect();
      if parts.len() >= 2 {
        let value = parts[1].trim().trim_end_matches(',');
        return value.parse().ok();
      }
    }
  }
  None
}

/// Create a benchmark manifest for a specific mathlib tag/commit.
///
/// This function:
/// 1. Clones mathlib at the specified tag to a temp directory
/// 2. Downloads the cache files to a temp cache directory
/// 3. Lists all downloaded .ltar files
/// 4. Writes the manifest to benchmarks/mathlib-<tag>.manifest
fn create_manifest_for_tag(tag: &str) {
  use std::io::Write;

  let temp_dir = std::env::temp_dir().join(format!("mathlib-manifest-{}", tag));
  let cache_dir = std::env::temp_dir().join(format!("mathlib-cache-{}", tag));

  // Use current working directory for benchmarks/
  let cwd = std::env::current_dir().expect("Failed to get current directory");
  let benchmarks_dir = cwd.join("benchmarks");

  eprintln!("Creating manifest for mathlib {}", tag);
  eprintln!("Clone dir: {}", temp_dir.display());
  eprintln!("Cache dir: {}", cache_dir.display());
  eprintln!();

  // Clean up any existing temp directories
  let _ = std::fs::remove_dir_all(&temp_dir);
  let _ = std::fs::remove_dir_all(&cache_dir);
  std::fs::create_dir_all(&cache_dir).expect("Failed to create cache directory");

  // Clone mathlib at the specified tag
  eprintln!("Cloning mathlib4 at {}...", tag);
  let status = Command::new("git")
    .args([
      "clone",
      "--depth",
      "1",
      "--branch",
      tag,
      "https://github.com/leanprover-community/mathlib4.git",
      temp_dir.to_str().unwrap(),
    ])
    .status()
    .expect("Failed to run git clone");

  if !status.success() {
    eprintln!("Failed to clone mathlib4 at tag {}", tag);
    std::process::exit(1);
  }

  // Download cache files
  eprintln!("Downloading cache files...");
  let status = Command::new("lake")
    .args(["exe", "cache", "get"])
    .current_dir(&temp_dir)
    .env("MATHLIB_CACHE_DIR", &cache_dir)
    .status()
    .expect("Failed to run lake exe cache get");

  if !status.success() {
    eprintln!("Failed to download cache files");
    std::process::exit(1);
  }

  // List all .ltar files in the cache directory
  eprintln!("Collecting .ltar files...");
  let mut ltar_files: Vec<String> = std::fs::read_dir(&cache_dir)
    .expect("Failed to read cache directory")
    .filter_map(|e| e.ok())
    .filter(|e| e.path().extension().map(|x| x == "ltar").unwrap_or(false))
    .filter_map(|e| e.path().file_stem().map(|s| s.to_string_lossy().to_string()))
    .collect();

  ltar_files.sort();

  eprintln!("Found {} .ltar files", ltar_files.len());

  // Create benchmarks directory if it doesn't exist
  std::fs::create_dir_all(&benchmarks_dir).expect("Failed to create benchmarks directory");

  // Write manifest file
  let manifest_path = benchmarks_dir.join(format!("mathlib-{}.manifest", tag));
  let mut file = std::fs::File::create(&manifest_path).expect("Failed to create manifest file");

  writeln!(file, "# Benchmark manifest for mathlib {}", tag).unwrap();
  writeln!(file, "# Generated by leantar-benchmark --create-manifest {}", tag).unwrap();
  writeln!(file, "# {} files", ltar_files.len()).unwrap();
  writeln!(file, "#").unwrap();

  for hash in &ltar_files {
    writeln!(file, "{}", hash).unwrap();
  }

  eprintln!();
  eprintln!("Manifest written to: {}", manifest_path.display());
  eprintln!("Files in manifest: {}", ltar_files.len());

  // Clean up temp directories
  eprintln!();
  eprintln!("Cleaning up temporary directories...");
  let _ = std::fs::remove_dir_all(&temp_dir);
  let _ = std::fs::remove_dir_all(&cache_dir);

  eprintln!("Done!");
}
