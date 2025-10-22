//! Integration tests for module compression/decompression.
//!
//! These tests verify that the module system correctly compresses and
//! decompresses sets of related .olean files (.olean, .olean.server, .olean.private).

use std::fs;
use std::io::Cursor;

/// Integration test for module compression/decompression
///
/// Tests that compressing and decompressing a set of .olean module files
/// (.olean, .olean.server, .olean.private) produces byte-identical results.
#[test]
fn test_module_roundtrip() {
    // Load original files
    let test_data_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/module");

    let olean = fs::read(format!("{}/Attr.olean", test_data_dir))
        .expect("Failed to read .olean file");
    let server = fs::read(format!("{}/Attr.olean.server", test_data_dir))
        .expect("Failed to read .olean.server file");
    let private = fs::read(format!("{}/Attr.olean.private", test_data_dir))
        .expect("Failed to read .olean.private file");

    println!("Original sizes:");
    println!("  .olean:         {} bytes", olean.len());
    println!("  .olean.server:  {} bytes", server.len());
    println!("  .olean.private: {} bytes", private.len());

    // Compress the three files together
    let mut compressed = Vec::new();
    leangz::lgz::compress(&[&olean, &server, &private], &mut compressed);

    println!("Compressed size: {} bytes", compressed.len());
    println!("Compression ratio: {:.2}%",
        (compressed.len() as f64 / (olean.len() + server.len() + private.len()) as f64) * 100.0);

    // Decompress
    let (decompressed_buf, ranges) = leangz::lgz::decompress(Cursor::new(&compressed), 3);

    assert_eq!(ranges.len(), 3, "Expected 3 ranges for the 3 files");

    // Extract each file from the decompressed buffer
    let decompressed_olean = &decompressed_buf[ranges[0].clone()];
    let decompressed_server = &decompressed_buf[ranges[1].clone()];
    let decompressed_private = &decompressed_buf[ranges[2].clone()];

    println!("\nDecompressed sizes:");
    println!("  .olean:         {} bytes", decompressed_olean.len());
    println!("  .olean.server:  {} bytes", decompressed_server.len());
    println!("  .olean.private: {} bytes", decompressed_private.len());

    // Compare each file
    assert_eq!(
        olean, decompressed_olean,
        ".olean file does not match after compression/decompression"
    );
    println!("✓ .olean matches");

    assert_eq!(
        server, decompressed_server,
        ".olean.server file does not match after compression/decompression \
         (expected {} bytes, got {} bytes)",
        server.len(), decompressed_server.len()
    );
    println!("✓ .olean.server matches");

    assert_eq!(
        private, decompressed_private,
        ".olean.private file does not match after compression/decompression \
         (expected {} bytes, got {} bytes)",
        private.len(), decompressed_private.len()
    );
    println!("✓ .olean.private matches");

    println!("\nAll files match perfectly!");
}

/// Simpler test with just the main olean file (should pass)
#[test]
fn test_single_olean_roundtrip() {
    let test_data_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/module");

    let olean = fs::read(format!("{}/Attr.olean", test_data_dir))
        .expect("Failed to read .olean file");

    println!("Original .olean size: {} bytes", olean.len());

    // Compress single file
    let mut compressed = Vec::new();
    leangz::lgz::compress(&[&olean], &mut compressed);

    println!("Compressed size: {} bytes", compressed.len());

    // Decompress
    let (decompressed_buf, ranges) = leangz::lgz::decompress(Cursor::new(&compressed), 1);

    assert_eq!(ranges.len(), 1, "Expected 1 range for 1 file");

    let decompressed = &decompressed_buf[ranges[0].clone()];

    println!("Decompressed size: {} bytes", decompressed.len());

    assert_eq!(
        olean, decompressed,
        "Single .olean file does not match after compression/decompression"
    );

    println!("✓ Single .olean file matches perfectly");
}
