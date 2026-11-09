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

- `ExprHash.olean`, `ExprHash.olean.server`, `ExprHash.olean.private` - A module in the version 4 `.olean` format, whose `Expr` data uses the hashes of leanprover/lean4#16107 (29K, 3.6K, 34K)

`ExprHash` was built with a Lean 4.37.0 prerelease (`lean -o`) from `ExprHash.lean`, which exercises every `Expr` constructor whose hash changed, including a dependent and a non-dependent `let`:

```lean
module

/-! Exercises every `Expr` constructor whose cached hash changed in leanprover/lean4#16107. -/

public structure Point where
  x : Nat
  y : Nat

-- `Expr.proj` (projection function bodies), `Expr.app`, `Expr.lam`
public def Point.sum (p : Point) : Nat := p.1 + p.2

-- `Expr.forallE`
public def Pred : Type := (n : Nat) → n = n → Prop

-- `Expr.letE`, dependent and non-dependent
public def lets (n : Nat) : Nat :=
  let a := n + 1
  have b := a * 2
  a + b

-- a dependent `Expr.letE`: the body only typechecks with the value of `n`
public def depLet : Fin 2 := let n := 2; (⟨0, by decide⟩ : Fin n)

-- `Expr.mdata` from tactic proofs
public theorem sum_comm (p : Point) : p.sum = p.y + p.x := by
  unfold Point.sum
  omega

def priv (n : Nat) : Nat := (fun k => k + n) n

theorem priv_eq (n : Nat) : priv n = n + n := rfl
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

The test is located in `tests/module_compression.rs` and includes six tests:

1. **`test_single_olean_roundtrip`** - Tests single file compression
2. **`test_module_roundtrip`** - Tests module (3-file) compression
3. **`test_scalar_array_roundtrip`** - Tests single file compression of an `.olean` holding a scalar array
4. **`test_wide_scalar_array_roundtrip`** - Tests single file compression of an `.olean` holding scalar arrays with 8-byte elements
5. **`test_v4_module_roundtrip`** - Tests module compression of version 4 `.olean`s, and that their `Expr` hashes are recomputed
6. **`test_v4_closures_unsupported`** - Tests that a version 4 `.olean` with closures is reported as unsupported

`tests/ltar_fallback.rs` packs and unpacks the `ExprHash` module with `leantar`, once as is and once with a closure in its `.olean`, which must then be packed without `lgz`.

## Expected Behavior

Every file should decompress to be byte-identical to the original, including when the three `Attr` files are compressed together.
