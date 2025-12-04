//! Profiling infrastructure for leantar decompression
//!
//! This module provides detailed timing breakdown for identifying performance bottlenecks.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Macro to create atomic counters
macro_rules! atomic_counter {
  ($name:ident) => {
    static $name: AtomicUsize = AtomicUsize::new(0);
  };
}

// Time counters (in microseconds)
atomic_counter!(READ_LTAR_US); // Reading compressed data from .ltar file
atomic_counter!(ZSTD_INIT_US); // Initializing zstd decoder
atomic_counter!(DECOMPRESS_US); // Actual decompression (zstd + lgz)
atomic_counter!(FILE_CREATE_US); // File::create() calls
atomic_counter!(FILE_WRITE_US); // write_all() calls
atomic_counter!(FILE_CLOSE_US); // Explicit drop(file) / close
atomic_counter!(ROLLBACK_PUSH_US); // rollback.push(path) operations
atomic_counter!(LOOP_OVERHEAD_US); // Loop iteration overhead
atomic_counter!(MISC_US); // Other/unaccounted time

// Count counters
atomic_counter!(LTAR_FILE_COUNT); // Number of .ltar files processed
atomic_counter!(OUTPUT_FILE_COUNT); // Number of output files written
atomic_counter!(ZSTD_COUNT); // Files using plain ZSTD compression
atomic_counter!(LGZ_COUNT); // Files using LGZ compression
atomic_counter!(LGZ_MODULE_COUNT); // Files using LGZ_MODULE compression
atomic_counter!(HASH_PLAIN_COUNT); // Files using HASH_PLAIN
atomic_counter!(HASH_JSON_COUNT); // Files using HASH_JSON
atomic_counter!(HASH_OUTPUT_COUNT); // Files using HASH_OUTPUT

// Size counters (in bytes)
atomic_counter!(COMPRESSED_BYTES); // Total compressed bytes read
atomic_counter!(DECOMPRESSED_BYTES); // Total decompressed bytes written

/// Helper to add duration to atomic counter
#[inline]
pub fn add_duration(counter: &AtomicUsize, d: Duration) {
  counter.fetch_add(d.as_micros() as usize, Ordering::Relaxed);
}

/// Helper to increment count
#[inline]
pub fn inc(counter: &AtomicUsize) { counter.fetch_add(1, Ordering::Relaxed); }

/// Helper to add value
#[inline]
pub fn add(counter: &AtomicUsize, val: usize) { counter.fetch_add(val, Ordering::Relaxed); }

/// Scoped timer that adds elapsed time to a counter on drop
pub struct ScopedTimer<'a> {
  counter: &'a AtomicUsize,
  start: Instant,
}

impl<'a> ScopedTimer<'a> {
  #[inline]
  pub fn new(counter: &'a AtomicUsize) -> Self { Self { counter, start: Instant::now() } }

  /// Stop the timer and return elapsed duration (also adds to counter)
  #[inline]
  pub fn stop(self) -> Duration {
    let elapsed = self.start.elapsed();
    add_duration(self.counter, elapsed);
    std::mem::forget(self); // Don't run Drop
    elapsed
  }
}

impl Drop for ScopedTimer<'_> {
  fn drop(&mut self) { add_duration(self.counter, self.start.elapsed()); }
}

// Public accessors for the counters
pub mod counters {
  use super::*;

  pub fn read_ltar() -> &'static AtomicUsize { &READ_LTAR_US }
  pub fn zstd_init() -> &'static AtomicUsize { &ZSTD_INIT_US }
  pub fn decompress() -> &'static AtomicUsize { &DECOMPRESS_US }
  pub fn file_create() -> &'static AtomicUsize { &FILE_CREATE_US }
  pub fn file_write() -> &'static AtomicUsize { &FILE_WRITE_US }
  pub fn file_close() -> &'static AtomicUsize { &FILE_CLOSE_US }
  pub fn rollback_push() -> &'static AtomicUsize { &ROLLBACK_PUSH_US }
  pub fn loop_overhead() -> &'static AtomicUsize { &LOOP_OVERHEAD_US }
  pub fn misc() -> &'static AtomicUsize { &MISC_US }

  pub fn ltar_files() -> &'static AtomicUsize { &LTAR_FILE_COUNT }
  pub fn output_files() -> &'static AtomicUsize { &OUTPUT_FILE_COUNT }
  pub fn zstd_files() -> &'static AtomicUsize { &ZSTD_COUNT }
  pub fn lgz_files() -> &'static AtomicUsize { &LGZ_COUNT }
  pub fn lgz_module_files() -> &'static AtomicUsize { &LGZ_MODULE_COUNT }
  pub fn hash_plain_files() -> &'static AtomicUsize { &HASH_PLAIN_COUNT }
  pub fn hash_json_files() -> &'static AtomicUsize { &HASH_JSON_COUNT }
  pub fn hash_output_files() -> &'static AtomicUsize { &HASH_OUTPUT_COUNT }

  pub fn compressed_bytes() -> &'static AtomicUsize { &COMPRESSED_BYTES }
  pub fn decompressed_bytes() -> &'static AtomicUsize { &DECOMPRESSED_BYTES }
}

/// Reset all counters to zero
pub fn reset() {
  READ_LTAR_US.store(0, Ordering::Relaxed);
  ZSTD_INIT_US.store(0, Ordering::Relaxed);
  DECOMPRESS_US.store(0, Ordering::Relaxed);
  FILE_CREATE_US.store(0, Ordering::Relaxed);
  FILE_WRITE_US.store(0, Ordering::Relaxed);
  FILE_CLOSE_US.store(0, Ordering::Relaxed);
  ROLLBACK_PUSH_US.store(0, Ordering::Relaxed);
  LOOP_OVERHEAD_US.store(0, Ordering::Relaxed);
  MISC_US.store(0, Ordering::Relaxed);

  LTAR_FILE_COUNT.store(0, Ordering::Relaxed);
  OUTPUT_FILE_COUNT.store(0, Ordering::Relaxed);
  ZSTD_COUNT.store(0, Ordering::Relaxed);
  LGZ_COUNT.store(0, Ordering::Relaxed);
  LGZ_MODULE_COUNT.store(0, Ordering::Relaxed);
  HASH_PLAIN_COUNT.store(0, Ordering::Relaxed);
  HASH_JSON_COUNT.store(0, Ordering::Relaxed);
  HASH_OUTPUT_COUNT.store(0, Ordering::Relaxed);

  COMPRESSED_BYTES.store(0, Ordering::Relaxed);
  DECOMPRESSED_BYTES.store(0, Ordering::Relaxed);
}

/// Get a snapshot of all profile data
#[derive(Debug, Clone)]
pub struct ProfileSnapshot {
  // Times in microseconds
  pub read_ltar_us: usize,
  pub zstd_init_us: usize,
  pub decompress_us: usize,
  pub file_create_us: usize,
  pub file_write_us: usize,
  pub file_close_us: usize,
  pub rollback_push_us: usize,
  pub loop_overhead_us: usize,
  pub misc_us: usize,

  // Counts
  pub ltar_file_count: usize,
  pub output_file_count: usize,
  pub zstd_count: usize,
  pub lgz_count: usize,
  pub lgz_module_count: usize,
  pub hash_plain_count: usize,
  pub hash_json_count: usize,
  pub hash_output_count: usize,

  // Sizes
  pub compressed_bytes: usize,
  pub decompressed_bytes: usize,

  // Wall time (set externally)
  pub wall_time_us: usize,
  pub thread_count: usize,
}

impl ProfileSnapshot {
  /// Capture current profile data
  pub fn capture() -> Self {
    Self {
      read_ltar_us: READ_LTAR_US.load(Ordering::Relaxed),
      zstd_init_us: ZSTD_INIT_US.load(Ordering::Relaxed),
      decompress_us: DECOMPRESS_US.load(Ordering::Relaxed),
      file_create_us: FILE_CREATE_US.load(Ordering::Relaxed),
      file_write_us: FILE_WRITE_US.load(Ordering::Relaxed),
      file_close_us: FILE_CLOSE_US.load(Ordering::Relaxed),
      rollback_push_us: ROLLBACK_PUSH_US.load(Ordering::Relaxed),
      loop_overhead_us: LOOP_OVERHEAD_US.load(Ordering::Relaxed),
      misc_us: MISC_US.load(Ordering::Relaxed),

      ltar_file_count: LTAR_FILE_COUNT.load(Ordering::Relaxed),
      output_file_count: OUTPUT_FILE_COUNT.load(Ordering::Relaxed),
      zstd_count: ZSTD_COUNT.load(Ordering::Relaxed),
      lgz_count: LGZ_COUNT.load(Ordering::Relaxed),
      lgz_module_count: LGZ_MODULE_COUNT.load(Ordering::Relaxed),
      hash_plain_count: HASH_PLAIN_COUNT.load(Ordering::Relaxed),
      hash_json_count: HASH_JSON_COUNT.load(Ordering::Relaxed),
      hash_output_count: HASH_OUTPUT_COUNT.load(Ordering::Relaxed),

      compressed_bytes: COMPRESSED_BYTES.load(Ordering::Relaxed),
      decompressed_bytes: DECOMPRESSED_BYTES.load(Ordering::Relaxed),

      wall_time_us: 0,
      thread_count: rayon::current_num_threads(),
    }
  }

  /// Total tracked time (all categories)
  pub fn total_tracked_us(&self) -> usize {
    self.read_ltar_us
      + self.zstd_init_us
      + self.decompress_us
      + self.file_create_us
      + self.file_write_us
      + self.file_close_us
      + self.rollback_push_us
      + self.loop_overhead_us
      + self.misc_us
  }

  /// Total write-related time
  pub fn total_write_us(&self) -> usize {
    self.file_create_us + self.file_write_us + self.file_close_us
  }

  /// Ideal parallel time (total tracked / thread count)
  pub fn ideal_parallel_us(&self) -> usize { self.total_tracked_us() / self.thread_count.max(1) }

  /// Print detailed report
  pub fn print_report(&self) {
    let total = self.total_tracked_us().max(1);
    let wall = self.wall_time_us.max(1);
    let ltar_count = self.ltar_file_count.max(1);
    let out_count = self.output_file_count.max(1);

    eprintln!();
    eprintln!("=== PROFILE ===");
    eprintln!();
    eprintln!("Wall time:      {:>8.2}s", wall as f64 / 1_000_000.0);
    eprintln!(
      "Total tracked:  {:>8.2}s  ({:.1}x wall, {} threads)",
      total as f64 / 1_000_000.0,
      total as f64 / wall as f64,
      self.thread_count
    );
    eprintln!();

    eprintln!(
      "Files: {} .ltar -> {} output ({:.2} MB -> {:.2} MB, {:.1}x)",
      self.ltar_file_count,
      self.output_file_count,
      self.compressed_bytes as f64 / 1_000_000.0,
      self.decompressed_bytes as f64 / 1_000_000.0,
      self.decompressed_bytes as f64 / self.compressed_bytes.max(1) as f64
    );
    eprintln!();

    eprintln!("Time breakdown (cumulative across threads):");
    eprintln!();

    let print_time = |name: &str, us: usize, per_file: usize| {
      let pct = us * 100 / total;
      eprintln!(
        "  {:16} {:>7.2}s  {:>3}%  ({:>6}µs/file)",
        name,
        us as f64 / 1_000_000.0,
        pct,
        us / per_file
      );
    };

    eprintln!("I/O Read:");
    print_time("Read .ltar", self.read_ltar_us, ltar_count);

    eprintln!("Decompression:");
    print_time("ZSTD init", self.zstd_init_us, ltar_count);
    print_time("Decompress", self.decompress_us, ltar_count);

    eprintln!("I/O Write:");
    print_time("File create", self.file_create_us, out_count);
    print_time("File write", self.file_write_us, out_count);
    print_time("File close", self.file_close_us, out_count);

    eprintln!("Overhead:");
    print_time("Rollback push", self.rollback_push_us, out_count);
    print_time("Loop overhead", self.loop_overhead_us, out_count);
    print_time("Misc", self.misc_us, ltar_count);

    let write_total = self.total_write_us();
    let read_pct = self.read_ltar_us * 100 / total;
    let decomp_pct = (self.zstd_init_us + self.decompress_us) * 100 / total;
    let write_pct = write_total * 100 / total;
    let overhead_pct = (self.rollback_push_us + self.loop_overhead_us + self.misc_us) * 100 / total;

    eprintln!();
    eprintln!(
      "Summary: Read {}% | Decompress {}% | Write {}% | Overhead {}%",
      read_pct, decomp_pct, write_pct, overhead_pct
    );

    let efficiency =
      if wall > 0 { (total as f64 / self.thread_count as f64) / wall as f64 * 100.0 } else { 0.0 };
    eprintln!("Parallelism efficiency: {:.1}%", efficiency);
  }

  /// Print JSON output for programmatic consumption
  pub fn print_json(&self) {
    println!("{}", serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string()));
  }
}

impl serde::Serialize for ProfileSnapshot {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where S: serde::Serializer {
    use serde::ser::SerializeStruct;
    let mut s = serializer.serialize_struct("ProfileSnapshot", 20)?;

    s.serialize_field("wall_time_ms", &(self.wall_time_us / 1000))?;
    s.serialize_field("thread_count", &self.thread_count)?;

    s.serialize_field("read_ltar_ms", &(self.read_ltar_us / 1000))?;
    s.serialize_field("zstd_init_ms", &(self.zstd_init_us / 1000))?;
    s.serialize_field("decompress_ms", &(self.decompress_us / 1000))?;
    s.serialize_field("file_create_ms", &(self.file_create_us / 1000))?;
    s.serialize_field("file_write_ms", &(self.file_write_us / 1000))?;
    s.serialize_field("file_close_ms", &(self.file_close_us / 1000))?;
    s.serialize_field("rollback_push_ms", &(self.rollback_push_us / 1000))?;
    s.serialize_field("loop_overhead_ms", &(self.loop_overhead_us / 1000))?;
    s.serialize_field("misc_ms", &(self.misc_us / 1000))?;

    s.serialize_field("ltar_file_count", &self.ltar_file_count)?;
    s.serialize_field("output_file_count", &self.output_file_count)?;
    s.serialize_field("zstd_count", &self.zstd_count)?;
    s.serialize_field("lgz_count", &self.lgz_count)?;
    s.serialize_field("lgz_module_count", &self.lgz_module_count)?;
    s.serialize_field("hash_plain_count", &self.hash_plain_count)?;
    s.serialize_field("hash_json_count", &self.hash_json_count)?;
    s.serialize_field("hash_output_count", &self.hash_output_count)?;

    s.serialize_field("compressed_bytes", &self.compressed_bytes)?;
    s.serialize_field("decompressed_bytes", &self.decompressed_bytes)?;

    s.end()
  }
}
