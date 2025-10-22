# Module Compression Test

This directory contains test data for the module compression feature.

## Test Files

- `Attr.olean` - Main olean file (48K)
- `Attr.olean.server` - Language server information (2.3K)
- `Attr.olean.private` - Private declarations (46K)
- `Attr.trace` - Build trace file

These files are from the Plausible project.

## Running the Test

```bash
cargo test --test module_compression
```

Or with output visible:

```bash
cargo test --test module_compression -- --nocapture
```

## Test Structure

The test is located in `tests/module_compression.rs` and includes two tests:

1. **`test_single_olean_roundtrip`** - Tests single file compression (should pass)
2. **`test_module_roundtrip`** - Tests module (3-file) compression (currently fails due to bug)

## Expected Behavior

All three files should compress together and then decompress to be byte-identical to the originals.
