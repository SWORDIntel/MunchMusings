"""Compatibility shim for the legacy root-level unittest entrypoint.

Canonical bootstrap tests live in ``tests/test_bootstrap.py``.
This module intentionally exposes no tests so ``unittest discover`` does not
run the same suite twice when it scans the repository root.
"""

from __future__ import annotations

import unittest


def load_tests(loader: unittest.TestLoader, tests: unittest.TestSuite, pattern: str | None) -> unittest.TestSuite:
    return loader.suiteClass()


if __name__ == "__main__":
    raise SystemExit(unittest.main(module="tests.test_bootstrap"))
