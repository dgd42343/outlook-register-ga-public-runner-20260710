#!/usr/bin/env python3
"""Evaluate bounded exact-hash samples for the HSProtect client asset."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys


def _identity(path: Path) -> dict:
    if not path.is_file():
        return {"present": False, "length": 0, "sha256": ""}
    body = path.read_bytes()
    return {
        "present": True,
        "length": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def evaluate_gate(expected: str, sample_paths: list[Path]) -> dict:
    samples = [_identity(path) for path in sample_paths]
    stable_pairs = []
    expected_pairs = []
    for index, (previous, current) in enumerate(zip(samples, samples[1:]), start=1):
        stable = (
            previous["present"]
            and current["present"]
            and previous["length"] == current["length"]
            and previous["sha256"] == current["sha256"]
        )
        if stable:
            pair = [index, index + 1]
            stable_pairs.append(pair)
            if current["sha256"] == expected:
                expected_pairs.append(pair)

    admitted = bool(expected_pairs)
    present_count = sum(bool(sample["present"]) for sample in samples)
    distinct = sorted(
        {sample["sha256"] for sample in samples if sample["sha256"]}
    )
    if admitted and expected_pairs[0] == [1, 2]:
        reason = "pinned_asset_admitted"
    elif admitted:
        reason = "pinned_asset_admitted_after_bounded_resample"
    elif present_count < 2:
        reason = "asset_fetch_failed"
    elif len(distinct) > 1:
        reason = "asset_changed_during_gate"
    else:
        reason = "asset_sha256_mismatch"

    first = samples[0] if samples else _identity(Path(""))
    second = samples[1] if len(samples) > 1 else _identity(Path(""))
    return {
        "schema": 1,
        "asset": "client.hsprotect.net/PXzC5j78di/main.min.js",
        "expected_sha256": expected,
        "first_sha256": first["sha256"] or None,
        "first_length": first["length"],
        "second_sha256": second["sha256"] or None,
        "second_length": second["length"],
        "sample_count": len(samples),
        "samples": [
            {
                "index": index,
                "present": sample["present"],
                "length": sample["length"],
                "sha256": sample["sha256"] or None,
            }
            for index, sample in enumerate(samples, start=1)
        ],
        "distinct_sha256": distinct,
        "stable": bool(stable_pairs),
        "stable_pairs": stable_pairs,
        "matched_pair": expected_pairs[0] if expected_pairs else None,
        "admitted": admitted,
        "reason": reason,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", required=True)
    parser.add_argument("--sample", action="append", required=True)
    parser.add_argument("--safe-output", required=True)
    parser.add_argument("--github-output", required=True)
    args = parser.parse_args(argv)

    expected = args.expected.strip().lower()
    if len(expected) != 64 or any(char not in "0123456789abcdef" for char in expected):
        parser.error("--expected must be a lowercase SHA-256")
    if not 2 <= len(args.sample) <= 4:
        parser.error("--sample must be supplied 2-4 times")

    safe = evaluate_gate(expected, [Path(path) for path in args.sample])
    Path(args.safe_output).write_text(json.dumps(safe, indent=2), encoding="utf-8")
    actual = expected if safe["admitted"] else ""
    with Path(args.github_output).open("a", encoding="utf-8") as handle:
        handle.write(f"admitted={str(safe['admitted']).lower()}\n")
        handle.write(f"actual_sha256={actual}\n")
        handle.write(f"reason={safe['reason']}\n")
    print(json.dumps(safe))
    if safe["admitted"]:
        return 0
    print(
        "live HSProtect client asset did not produce two consecutive pinned samples",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
