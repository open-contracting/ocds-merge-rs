# Changelog

## 0.1.6 (2026-06-30)

- feat: Add `NonStringNonNumberIdValueError`, raised when an object in an array has an `id` value that is a boolean, array or object, instead of panicking. A null `id` is treated as a missing `id`. A number `id` that isn't an integer (a float or out-of-range integer) is used as a string for the purpose of matching objects across releases (the original value is preserved in the merged output).
- perf: Reuse the path buffer when flattening, for faster merging with fewer allocations.

## 0.1.5 (2026-02-18)

- Upgrade pythonize 0.28.

## 0.1.4 (2026-02-11)

- fix: Fix large integers from Python to Rust.

## 0.1.3 (2026-01-07)

- feat: Add RepeatedDateValueWarning for when releases lack a stable order.

## 0.1.2 (2025-12-19)

- feat: Add attributes to errors and warnings in Python.

## 0.1.1 (2025-12-19)

- feat: Enable the `arbitrary_precision` feature on the `serde_json` crate.

## 0.1.0 (2025-08-31)

First release.
