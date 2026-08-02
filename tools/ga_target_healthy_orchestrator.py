#!/usr/bin/env python3
"""Dispatch GA batches until a target number of Graph-healthy accounts exists.

Only public-safe verdict artifacts are downloaded.  Workflow conclusions are
not treated as business outcomes because the child workflow intentionally fails
its goal job when any non-IP path loss is present.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Any, Iterable


ACTIVE_STATUSES = {"queued", "in_progress", "waiting", "pending", "requested"}
RUN_ID_PATTERN = re.compile(r"/actions/runs/(\d+)(?:\b|/)")


class OrchestratorError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)


def run_gh(
    arguments: list[str], *, check: bool = True, timeout: float | None = None
) -> subprocess.CompletedProcess[str]:
    process = subprocess.run(
        ["gh", *arguments],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    if check and process.returncode != 0:
        message = (process.stderr or process.stdout or "unknown gh error").strip()
        raise OrchestratorError(
            f"gh {arguments[0]} failed with exit {process.returncode}: {message[-1200:]}"
        )
    return process


def gh_json(arguments: list[str], *, timeout: float | None = None) -> Any:
    process = run_gh(arguments, timeout=timeout)
    try:
        return json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise OrchestratorError("gh returned invalid JSON") from exc


def parse_run_id(text: str) -> int | None:
    match = RUN_ID_PATTERN.search(text)
    return int(match.group(1)) if match else None


def list_child_runs(repo: str, workflow: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = gh_json(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            workflow,
            "--limit",
            str(limit),
            "--json",
            "databaseId,status,conclusion,createdAt,updatedAt,url,headSha",
        ]
    )
    if not isinstance(rows, list):
        raise OrchestratorError("gh run list returned a non-list payload")
    return rows


def ensure_child_idle(repo: str, workflow: str) -> None:
    active = [
        row
        for row in list_child_runs(repo, workflow)
        if str(row.get("status") or "") in ACTIVE_STATUSES
    ]
    if active:
        ids = ",".join(str(row.get("databaseId")) for row in active[:10])
        raise OrchestratorError(f"child workflow already has active runs: {ids}")


def dispatch_child(
    *, repo: str, workflow: str, ref: str, slots: int, batch_marker: str
) -> int:
    before = {
        int(row["databaseId"])
        for row in list_child_runs(repo, workflow)
        if row.get("databaseId") is not None
    }
    slot_json = json.dumps(list(range(1, slots + 1)), separators=(",", ":"))
    process = run_gh(
        [
            "workflow",
            "run",
            workflow,
            "--repo",
            repo,
            "--ref",
            ref,
            "-f",
            f"node_slots_json={slot_json}",
            "-f",
            f"orchestration_id={batch_marker}",
        ]
    )
    run_id = parse_run_id((process.stdout or "") + "\n" + (process.stderr or ""))
    if run_id is not None:
        return run_id

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        time.sleep(3)
        candidates = [
            row
            for row in list_child_runs(repo, workflow)
            if int(row.get("databaseId") or 0) not in before
        ]
        if candidates:
            return max(int(row["databaseId"]) for row in candidates)
    raise OrchestratorError("dispatched child run could not be discovered")


def wait_for_child(
    *, repo: str, run_id: int, poll_seconds: int, timeout_minutes: int
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_minutes * 60
    previous = ""
    while time.monotonic() < deadline:
        row = gh_json(
            [
                "run",
                "view",
                str(run_id),
                "--repo",
                repo,
                "--json",
                "status,conclusion,createdAt,startedAt,updatedAt,url,headSha",
            ]
        )
        status = str(row.get("status") or "")
        if status != previous:
            print(f"child_run={run_id} status={status}", flush=True)
            previous = status
        if status == "completed":
            return row
        time.sleep(poll_seconds)

    run_gh(["run", "cancel", str(run_id), "--repo", repo], check=False)
    raise OrchestratorError(
        f"child run {run_id} exceeded {timeout_minutes} minute orchestration timeout"
    )


def download_safe_verdicts(
    *, repo: str, run_id: int, destination: Path, expected_slots: int
) -> list[dict[str, Any]]:
    verdict_root = destination / "safe-verdicts"
    for retry in range(1, 6):
        if verdict_root.exists():
            shutil.rmtree(verdict_root)
        verdict_root.mkdir(parents=True)
        process = run_gh(
            [
                "run",
                "download",
                str(run_id),
                "--repo",
                repo,
                "-p",
                "ga-safe-verdict-*",
                "-D",
                str(verdict_root),
            ],
            check=False,
            timeout=180,
        )
        files = sorted(verdict_root.rglob("*.json"))
        if process.returncode == 0 and len(files) == expected_slots:
            rows = []
            for path in files:
                try:
                    row = json.loads(path.read_text(encoding="utf-8-sig"))
                except (OSError, json.JSONDecodeError) as exc:
                    raise OrchestratorError(
                        f"invalid safe verdict JSON in child run {run_id}"
                    ) from exc
                if not isinstance(row, dict):
                    raise OrchestratorError("safe verdict is not an object")
                rows.append(row)
            return rows
        if retry < 5:
            print(
                f"child_run={run_id} verdicts={len(files)}/{expected_slots} "
                f"download_retry={retry}",
                flush=True,
            )
            time.sleep(10)
    raise OrchestratorError(
        f"child run {run_id} did not publish {expected_slots} safe verdicts"
    )


def parse_timestamp(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def unique_values(rows: Iterable[dict[str, Any]], key: str) -> list[Any]:
    values = {json.dumps(row.get(key), sort_keys=True) for row in rows}
    return [json.loads(value) for value in sorted(values)]


def summarize_verdicts(
    *,
    rows: list[dict[str, Any]],
    expected_slots: int,
    batch_marker: str,
    run_info: dict[str, Any],
) -> dict[str, Any]:
    if len(rows) != expected_slots:
        raise OrchestratorError(
            f"expected {expected_slots} verdicts, received {len(rows)}"
        )
    attempts = []
    for row in rows:
        try:
            attempts.append(int(row.get("attempt")))
        except (TypeError, ValueError) as exc:
            raise OrchestratorError("safe verdict contains an invalid attempt") from exc
    if sorted(attempts) != list(range(1, expected_slots + 1)):
        raise OrchestratorError("safe verdict attempts are incomplete or duplicated")
    markers = {str(row.get("orchestration_id") or "") for row in rows}
    if markers != {batch_marker}:
        raise OrchestratorError(
            f"safe verdict orchestration marker mismatch: {sorted(markers)}"
        )

    skipped = [row for row in rows if row.get("category") == "ip_skipped"]
    live = [row for row in rows if row.get("category") != "ip_skipped"]
    accepted = [row for row in live if row.get("accepted_result0") is True]
    strict = [row for row in live if row.get("strict_success") is True]
    healthy = [
        row
        for row in strict
        if row.get("graph_import_ok") is True
        and row.get("account_lifecycle") == "graph_healthy"
    ]
    fresh = [
        row
        for row in live
        if row.get("fresh_rechallenge_policy_skipped") is True
        or row.get("post_success_rechallenge") is True
        or row.get("fresh_rechallenge_absolute_timed_out") is True
        or row.get("fresh_rechallenge_idle_timed_out") is True
    ]
    risk = [row for row in live if row.get("explicit_riskblock") is True]
    # Live slots that entered the captcha but the collector returned -1 without
    # an accepted result0.  A high ratio signals the shared-risk collector cliff.
    collector_cliff_live = [
        row
        for row in live
        if row.get("accepted_result0") is not True
        and row.get("explicit_riskblock") is not True
        and row.get("probe_timed_out") is not True
        and row.get("category") == "technical_failure"
    ]
    waits = [
        int(wait)
        for row in rows
        for wait in (row.get("coordinator_final_wait_ms") or [])
    ]
    gaps = sorted(
        {
            int(gap)
            for row in rows
            for gap in (row.get("coordinator_final_gap_ms") or [])
        }
    )
    created = parse_timestamp(str(run_info["createdAt"]))
    updated = parse_timestamp(str(run_info["updatedAt"]))
    duration_minutes = (updated - created).total_seconds() / 60

    return {
        "run_id": int(run_info.get("databaseId") or 0),
        "url": str(run_info.get("url") or ""),
        "conclusion": str(run_info.get("conclusion") or ""),
        "head_sha": str(run_info.get("headSha") or ""),
        "orchestration_id": batch_marker,
        "duration_minutes": round(duration_minutes, 3),
        "dispatched": len(rows),
        "skipped": len(skipped),
        "live": len(live),
        "accepted_result0": len(accepted),
        "strict_create_account": len(strict),
        "graph_healthy": len(healthy),
        "fresh_challenge": len(fresh),
        "explicit_riskblock": len(risk),
        "probe_timeout": sum(bool(row.get("probe_timed_out")) for row in live),
        "collector_cliff_live": len(collector_cliff_live),
        "collector_cliff_rate": len(collector_cliff_live) / len(live) if live else 0.0,
        "graph_import_attempts_gt1": sum(
            int(row.get("graph_import_attempts") or 0) > 1 for row in rows
        ),
        "categories": dict(collections.Counter(row.get("category") for row in rows)),
        "live_accepted_rate": len(accepted) / len(live) if live else 0,
        "live_strict_rate": len(strict) / len(live) if live else 0,
        "accepted_to_strict_rate": len(strict) / len(accepted) if accepted else 0,
        "graph_after_strict_rate": len(healthy) / len(strict) if strict else 0,
        "graph_healthy_per_min": len(healthy) / duration_minutes
        if duration_minutes > 0
        else 0,
        "coordinator_final_reservations": len(waits),
        "coordinator_final_wait_ms_total": sum(waits),
        "coordinator_final_wait_ms_max": max(waits, default=0),
        "coordinator_final_gap_ms": gaps,
        "observed_config": {
            "variant": unique_values(rows, "variant"),
            "ads_profile_policy": unique_values(rows, "ads_profile_policy"),
            "fresh_session_restart_policy": unique_values(
                rows, "fresh_session_restart_policy"
            ),
            "pre_first_hold_warmup_policy": unique_values(
                rows, "pre_first_hold_warmup_policy"
            ),
            "pre_first_hold_warmup_ms": unique_values(
                rows, "pre_first_hold_warmup_ms"
            ),
            "signup_country_policy": unique_values(rows, "signup_country_policy"),
            "signup_country_code": unique_values(rows, "signup_country_code"),
            "signup_dob_policy": unique_values(rows, "signup_dob_policy"),
            "signup_dob_mode": unique_values(rows, "signup_dob_mode"),
            "email_domain_policy": unique_values(rows, "email_domain_policy"),
            "email_domain": unique_values(rows, "email_domain"),
            "coordinator_mode": unique_values(rows, "coordinator_mode"),
            "max_parallel": unique_values(rows, "max_parallel"),
            "runtime_mode": unique_values(rows, "runtime_mode"),
            "probe_timeout_minutes": unique_values(rows, "probe_timeout_minutes"),
            "job_timeout_minutes": unique_values(rows, "job_timeout_minutes"),
        },
    }


# Cliff threshold: when >25% of live slots in the previous batch returned
# collector -1 without an accepted result0, the shared-risk window is closing.
# Back off to avoid pouring jobs into a throttled lane.  Two consecutive clean
# batches (cliff_rate < 5%) are required before growing again.
CLIFF_SHRINK_THRESHOLD = 0.15
CLIFF_GROW_THRESHOLD = 0.05
CLIFF_SHRINK_FACTOR = 0.5
CLIFF_GROW_FACTOR = 1.3


def compute_next_batch_size(
    *,
    target: int,
    achieved: int,
    dispatched: int,
    max_dispatched: int,
    batch_slots: int,
    min_batch_slots: int,
    prev_cliff_rate: float = 0.0,
    consecutive_clean: int = 0,
) -> int:
    budget = max_dispatched - dispatched
    if budget <= 0 or achieved >= target:
        return 0

    # Effective cap starts at the configured batch_slots, but shrinks when the
    # previous batch hit the collector cliff.  Recovery requires two clean
    # batches in a row before the cap grows back toward batch_slots.
    effective_cap = batch_slots
    if prev_cliff_rate >= CLIFF_SHRINK_THRESHOLD:
        effective_cap = max(min_batch_slots, int(batch_slots * CLIFF_SHRINK_FACTOR))
    elif prev_cliff_rate < CLIFF_GROW_THRESHOLD and consecutive_clean >= 2:
        effective_cap = min(batch_slots, int(batch_slots * CLIFF_GROW_FACTOR))

    if dispatched == 0 or achieved == 0:
        return min(effective_cap, budget)
    remaining = target - achieved
    observed_rate = achieved / dispatched
    conservative_rate = max(observed_rate * 0.85, 0.05)
    estimated = math.ceil(remaining / conservative_rate)
    planned = max(min_batch_slots, estimated)
    return min(effective_cap, budget, planned)


def cumulative_summary(state: dict[str, Any]) -> None:
    batches = state["batches"]
    dispatched = sum(int(row["dispatched"]) for row in batches)
    live = sum(int(row["live"]) for row in batches)
    accepted = sum(int(row["accepted_result0"]) for row in batches)
    strict = sum(int(row["strict_create_account"]) for row in batches)
    healthy = sum(int(row["graph_healthy"]) for row in batches)
    duration = sum(float(row["duration_minutes"]) for row in batches)
    state["totals"] = {
        "batches": len(batches),
        "dispatched": dispatched,
        "skipped": sum(int(row["skipped"]) for row in batches),
        "live": live,
        "accepted_result0": accepted,
        "strict_create_account": strict,
        "graph_healthy": healthy,
        "duration_minutes_sum": round(duration, 3),
        "raw_graph_healthy_rate": healthy / dispatched if dispatched else 0,
        "live_accepted_rate": accepted / live if live else 0,
        "live_strict_rate": strict / live if live else 0,
        "accepted_to_strict_rate": strict / accepted if accepted else 0,
        "graph_after_strict_rate": healthy / strict if strict else 0,
        "graph_healthy_per_child_run_minute": healthy / duration if duration else 0,
    }
    state["achieved_graph_healthy"] = healthy
    state["success"] = healthy >= int(state["target_graph_healthy"])


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--workflow", default="ctf-ga-own-ip-pool.yml")
    parser.add_argument("--ref", default="main")
    parser.add_argument("--target-graph-healthy", type=int, required=True)
    parser.add_argument("--batch-slots", type=int, default=50)
    parser.add_argument("--min-batch-slots", type=int, default=5)
    parser.add_argument("--max-dispatched", type=int, default=400)
    parser.add_argument("--poll-seconds", type=int, default=20)
    parser.add_argument("--child-timeout-minutes", type=int, default=45)
    parser.add_argument("--orchestration-id", default="")
    parser.add_argument("--artifact-root", type=Path, default=Path("Results/ga-target"))
    parser.add_argument(
        "--summary", type=Path, default=Path("Results/target-healthy-summary.json")
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not args.repo:
        parser.error("--repo or GITHUB_REPOSITORY is required")
    if args.target_graph_healthy < 1:
        parser.error("--target-graph-healthy must be positive")
    if not 1 <= args.batch_slots <= 256:
        parser.error("--batch-slots must be between 1 and 256")
    if not 1 <= args.min_batch_slots <= args.batch_slots:
        parser.error("--min-batch-slots must be between 1 and --batch-slots")
    if args.max_dispatched < 1:
        parser.error("--max-dispatched must be positive")
    if args.poll_seconds < 5:
        parser.error("--poll-seconds must be at least 5")
    if args.child_timeout_minutes < 5:
        parser.error("--child-timeout-minutes must be at least 5")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    orchestration_id = args.orchestration_id or (
        "ga-target-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    state: dict[str, Any] = {
        "schema": 1,
        "orchestration_id": orchestration_id,
        "repo": args.repo,
        "child_workflow": args.workflow,
        "child_ref": args.ref,
        "target_graph_healthy": args.target_graph_healthy,
        "batch_slots_max": args.batch_slots,
        "min_batch_slots": args.min_batch_slots,
        "max_dispatched": args.max_dispatched,
        "started_at_utc": utc_now(),
        "finished_at_utc": None,
        "success": False,
        "achieved_graph_healthy": 0,
        "batches": [],
        "totals": {},
        "error": None,
    }
    cumulative_summary(state)
    write_json_atomic(args.summary, state)

    try:
        run_gh(["auth", "status"], timeout=30)
        run_gh(["api", f"repos/{args.repo}", "--silent"], timeout=30)
        ensure_child_idle(args.repo, args.workflow)
        prev_cliff_rate = 0.0
        consecutive_clean = 0
        if args.dry_run:
            state["dry_run"] = True
            state["planned_first_batch_slots"] = compute_next_batch_size(
                target=args.target_graph_healthy,
                achieved=0,
                dispatched=0,
                max_dispatched=args.max_dispatched,
                batch_slots=args.batch_slots,
                min_batch_slots=args.min_batch_slots,
            )
            state["finished_at_utc"] = utc_now()
            write_json_atomic(args.summary, state)
            print(json.dumps(state, ensure_ascii=False, indent=2))
            return 0

        while not state["success"]:
            totals = state["totals"]
            slots = compute_next_batch_size(
                target=args.target_graph_healthy,
                achieved=int(totals["graph_healthy"]),
                dispatched=int(totals["dispatched"]),
                max_dispatched=args.max_dispatched,
                batch_slots=args.batch_slots,
                min_batch_slots=args.min_batch_slots,
                prev_cliff_rate=prev_cliff_rate,
                consecutive_clean=consecutive_clean,
            )
            if slots <= 0:
                break
            if state["batches"]:
                ensure_child_idle(args.repo, args.workflow)
            batch_number = len(state["batches"]) + 1
            batch_marker = f"{orchestration_id}-b{batch_number:03d}"
            print(
                f"dispatch batch={batch_number} slots={slots} "
                f"healthy={totals['graph_healthy']}/{args.target_graph_healthy}",
                flush=True,
            )
            run_id = dispatch_child(
                repo=args.repo,
                workflow=args.workflow,
                ref=args.ref,
                slots=slots,
                batch_marker=batch_marker,
            )
            run_info = wait_for_child(
                repo=args.repo,
                run_id=run_id,
                poll_seconds=args.poll_seconds,
                timeout_minutes=args.child_timeout_minutes,
            )
            run_info["databaseId"] = run_id
            batch_root = args.artifact_root / f"batch-{batch_number:03d}-run-{run_id}"
            rows = download_safe_verdicts(
                repo=args.repo,
                run_id=run_id,
                destination=batch_root,
                expected_slots=slots,
            )
            batch = summarize_verdicts(
                rows=rows,
                expected_slots=slots,
                batch_marker=batch_marker,
                run_info=run_info,
            )
            state["batches"].append(batch)
            batch_cliff_rate = float(batch.get("collector_cliff_rate") or 0.0)
            if batch_cliff_rate >= CLIFF_SHRINK_THRESHOLD:
                prev_cliff_rate = batch_cliff_rate
                consecutive_clean = 0
            else:
                prev_cliff_rate = batch_cliff_rate
                if batch_cliff_rate < CLIFF_GROW_THRESHOLD:
                    consecutive_clean += 1
                else:
                    consecutive_clean = 0
            cumulative_summary(state)
            write_json_atomic(batch_root / "batch-summary.json", batch)
            write_json_atomic(args.summary, state)
            print(
                f"complete batch={batch_number} run={run_id} "
                f"strict={batch['strict_create_account']} "
                f"graph_healthy={batch['graph_healthy']} "
                f"cumulative={state['achieved_graph_healthy']}/"
                f"{args.target_graph_healthy}",
                flush=True,
            )

        state["finished_at_utc"] = utc_now()
        cumulative_summary(state)
        write_json_atomic(args.summary, state)
        if state["success"]:
            print(
                f"target achieved graph_healthy={state['achieved_graph_healthy']} "
                f"dispatched={state['totals']['dispatched']}",
                flush=True,
            )
            return 0
        state["error"] = "max_dispatched_exhausted_before_target"
        write_json_atomic(args.summary, state)
        print(
            f"target not achieved graph_healthy={state['achieved_graph_healthy']}/"
            f"{args.target_graph_healthy} dispatched={state['totals']['dispatched']}/"
            f"{args.max_dispatched}",
            file=sys.stderr,
        )
        return 2
    except (OrchestratorError, subprocess.TimeoutExpired, OSError) as exc:
        state["finished_at_utc"] = utc_now()
        state["error"] = str(exc)[:2000]
        cumulative_summary(state)
        write_json_atomic(args.summary, state)
        print(f"orchestration failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
