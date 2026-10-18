# Module Compression Test

This directory contains test data for the module compression feature.

## Test Files

- `Attr.olean` - Main olean file (48K)
- `Attr.olean.server` - Language server information (2.3K)
- `Attr.olean.private` - Private declarations (46K)
- `Attr.trace` - Build trace file

These files are from the Plausible project.

- `ScalarArray.olean` - A module whose `.olean` holds a `ByteArray`, i.e. a scalar array object (2.2K)

`ScalarArray.olean` was built with Lean 4.34.1 (`lean -o`) from two files, `ScalarArrayExt.lean`:

```lean
import Lean
open Lean

initialize bytesExt : SimplePersistentEnvExtension ByteArray (Array ByteArray) ←
  registerSimplePersistentEnvExtension {
    addEntryFn := Array.push
    addImportedFn := Array.flatten
  }
```

and `ScalarArray.lean`:

```lean
import ScalarArrayExt
open Lean

run_cmd modifyEnv (bytesExt.addEntry · ⟨#[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]⟩)
```

- `WideScalarArray.olean` - A module whose `.olean` holds two `FloatArray`s, i.e. scalar arrays with element size 8 (2.3K)

`WideScalarArray.olean` was built with Lean 4.34.0 (`lean -o`) from two files, `WideScalarArrayExt.lean`:

```lean
import Lean
open Lean

initialize floatsExt : SimplePersistentEnvExtension FloatArray (Array FloatArray) ←
  registerSimplePersistentEnvExtension {
    addEntryFn := Array.push
    addImportedFn := Array.flatten
  }
```

and `WideScalarArray.lean`:

```lean
import WideScalarArrayExt
open Lean

run_cmd modifyEnv (floatsExt.addEntry · ⟨#[1.5]⟩)
run_cmd modifyEnv (floatsExt.addEntry · ⟨#[1.5, 2.5, 3.5]⟩)
```

## Running the Test

```bash
cargo test --test module_compression
```

Or with output visible:

```bash
cargo test --test module_compression -- --nocapture
```

## Test Structure

The test is located in `tests/module_compression.rs` and includes four tests:

1. **`test_single_olean_roundtrip`** - Tests single file compression
2. **`test_module_roundtrip`** - Tests module (3-file) compression
3. **`test_scalar_array_roundtrip`** - Tests single file compression of an `.olean` holding a scalar array
4. **`test_wide_scalar_array_roundtrip`** - Tests single file compression of an `.olean` holding scalar arrays with 8-byte elements

## Expected Behavior

Every file should decompress to be byte-identical to the original, including when the three `Attr` files are compressed together.
