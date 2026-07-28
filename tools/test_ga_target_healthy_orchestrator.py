import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tools.ga_target_healthy_orchestrator import (
    OrchestratorError,
    compute_next_batch_size,
    dispatch_child,
    parse_args,
    parse_run_id,
    summarize_verdicts,
)


def verdict(attempt, category, *, marker="target-b001", **overrides):
    row = {
        "attempt": str(attempt),
        "orchestration_id": marker,
        "category": category,
        "accepted_result0": False,
        "strict_success": False,
        "graph_import_ok": False,
        "account_lifecycle": "not_created",
        "fresh_rechallenge_policy_skipped": False,
        "post_success_rechallenge": False,
        "fresh_rechallenge_absolute_timed_out": False,
        "fresh_rechallenge_idle_timed_out": False,
        "explicit_riskblock": False,
        "probe_timed_out": False,
        "graph_import_attempts": 0,
        "coordinator_final_wait_ms": [],
        "coordinator_final_gap_ms": [],
        "variant": "online_ads_ga_production_fast_fail",
        "ads_profile_policy": "round_robin",
        "fresh_session_restart_policy": "off",
        "pre_first_hold_warmup_policy": "fixed_input",
        "pre_first_hold_warmup_ms": None,
        "signup_country_policy": "source_default",
        "signup_country_code": None,
        "signup_dob_policy": "source_default",
        "signup_dob_mode": None,
        "email_domain_policy": "source_default",
        "email_domain": None,
        "coordinator_mode": "final_only",
        "max_parallel": 20,
        "runtime_mode": "prebuilt",
        "probe_timeout_minutes": 18,
        "job_timeout_minutes": 30,
    }
    row.update(overrides)
    return row


class ParseRunIdTests(unittest.TestCase):
    def test_extracts_actions_run_url(self):
        self.assertEqual(
            parse_run_id("https://github.com/a/b/actions/runs/29417330058"),
            29417330058,
        )

    def test_missing_url_returns_none(self):
        self.assertIsNone(parse_run_id("workflow dispatched"))


class DispatchDefaultsTests(unittest.TestCase):
    def test_production_variant_defaults_to_offline_ads_pool(self):
        args = parse_args(
            ["--repo", "a/b", "--target-graph-healthy", "1", "--dry-run"]
        )
        self.assertEqual(args.variant, "offline_ads_pool_balanced_shuffle_v1")
        self.assertEqual(args.runner, "ubuntu-24.04")
        self.assertEqual(args.batch_slots, 100)

    def test_accepts_one_hundred_slot_rolling_matrix(self):
        args = parse_args(
            [
                "--repo",
                "a/b",
                "--target-graph-healthy",
                "1",
                "--batch-slots",
                "100",
                "--min-batch-slots",
                "100",
                "--duration-minutes",
                "300",
                "--dry-run",
            ]
        )
        self.assertEqual(args.batch_slots, 100)
        self.assertEqual(args.duration_minutes, 300)

    def test_rejects_child_matrices_larger_than_one_hundred_slots(self):
        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--repo",
                    "a/b",
                    "--target-graph-healthy",
                    "1",
                    "--batch-slots",
                    "101",
                    "--dry-run",
                ]
            )

    def test_rejects_negative_duration(self):
        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--repo",
                    "a/b",
                    "--target-graph-healthy",
                    "1",
                    "--duration-minutes",
                    "-1",
                    "--dry-run",
                ]
            )

    @patch("tools.ga_target_healthy_orchestrator.run_gh")
    @patch("tools.ga_target_healthy_orchestrator.list_child_runs", return_value=[])
    def test_dispatch_pins_requested_variant(self, _list_child_runs, run_gh):
        run_gh.return_value = SimpleNamespace(
            stdout="https://github.com/a/b/actions/runs/29417330058", stderr=""
        )

        run_id = dispatch_child(
            repo="a/b",
            workflow="ctf-ga-own-ip-pool.yml",
            ref="production",
            variant="offline_ads_pool_ga_fresh_rechallenge",
            runner="ubuntu-24.04",
            slots=3,
            batch_marker="batch-1",
        )

        self.assertEqual(run_id, 29417330058)
        arguments = run_gh.call_args.args[0]
        self.assertIn("variant=offline_ads_pool_ga_fresh_rechallenge", arguments)
        self.assertIn("runner=ubuntu-24.04", arguments)


class AdaptiveBatchTests(unittest.TestCase):
    def test_first_batch_uses_configured_cap(self):
        self.assertEqual(
            compute_next_batch_size(
                target=100,
                achieved=0,
                dispatched=0,
                max_dispatched=400,
                batch_slots=50,
                min_batch_slots=5,
            ),
            50,
        )

    def test_backfill_uses_observed_rate_with_margin(self):
        # 20 healthy / 50 slots, 10 remain.  Conservative rate is 0.34,
        # therefore ceil(10 / 0.34) = 30.
        self.assertEqual(
            compute_next_batch_size(
                target=30,
                achieved=20,
                dispatched=50,
                max_dispatched=200,
                batch_slots=50,
                min_batch_slots=5,
            ),
            30,
        )

    def test_budget_is_hard_cap(self):
        self.assertEqual(
            compute_next_batch_size(
                target=100,
                achieved=20,
                dispatched=95,
                max_dispatched=100,
                batch_slots=50,
                min_batch_slots=5,
            ),
            5,
        )


class VerdictSummaryTests(unittest.TestCase):
    def setUp(self):
        self.run_info = {
            "databaseId": 123,
            "url": "https://github.com/a/b/actions/runs/123",
            "conclusion": "failure",
            "headSha": "abc",
            "createdAt": "2026-07-15T10:00:00Z",
            "updatedAt": "2026-07-15T10:10:00Z",
        }

    def test_counts_only_graph_healthy_as_output(self):
        rows = [
            verdict(
                1,
                "strict_success",
                accepted_result0=True,
                strict_success=True,
                graph_import_ok=True,
                account_lifecycle="graph_healthy",
                graph_import_attempts=1,
                coordinator_final_wait_ms=[1000, 2000],
                coordinator_final_gap_ms=[12000],
            ),
            verdict(
                2,
                "post_proof_rechallenge",
                accepted_result0=True,
                fresh_rechallenge_policy_skipped=True,
                coordinator_final_wait_ms=[3000],
                coordinator_final_gap_ms=[12000],
            ),
            verdict(3, "ip_skipped"),
            verdict(4, "ip_riskblock", explicit_riskblock=True),
        ]
        summary = summarize_verdicts(
            rows=rows,
            expected_slots=4,
            batch_marker="target-b001",
            run_info=self.run_info,
        )
        self.assertEqual(summary["dispatched"], 4)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["live"], 3)
        self.assertEqual(summary["accepted_result0"], 2)
        self.assertEqual(summary["strict_create_account"], 1)
        self.assertEqual(summary["graph_healthy"], 1)
        self.assertEqual(summary["fresh_challenge"], 1)
        self.assertEqual(summary["explicit_riskblock"], 1)
        self.assertEqual(summary["coordinator_final_reservations"], 3)
        self.assertEqual(summary["coordinator_final_wait_ms_total"], 6000)
        self.assertEqual(summary["coordinator_final_gap_ms"], [12000])
        self.assertEqual(
            summary["observed_config"]["pre_first_hold_warmup_policy"],
            ["fixed_input"],
        )
        self.assertEqual(
            summary["observed_config"]["pre_first_hold_warmup_ms"], [None]
        )
        self.assertEqual(
            summary["observed_config"]["signup_country_policy"], ["source_default"]
        )
        self.assertEqual(summary["observed_config"]["signup_country_code"], [None])
        self.assertEqual(
            summary["observed_config"]["signup_dob_policy"], ["source_default"]
        )
        self.assertEqual(summary["observed_config"]["signup_dob_mode"], [None])
        self.assertEqual(
            summary["observed_config"]["email_domain_policy"], ["source_default"]
        )
        self.assertEqual(summary["observed_config"]["email_domain"], [None])
        self.assertAlmostEqual(summary["graph_healthy_per_min"], 0.1)

    def test_counts_backend_deferred_separately_from_graph_healthy(self):
        rows = [
            verdict(
                1,
                "strict_success",
                strict_success=True,
                graph_import_ok=False,
                graph_retry_scheduled=True,
                account_lifecycle="created_graph_deferred",
            )
        ]
        summary = summarize_verdicts(
            rows=rows,
            expected_slots=1,
            batch_marker="target-b001",
            run_info=self.run_info,
        )

        self.assertEqual(summary["graph_healthy"], 0)
        self.assertEqual(summary["graph_deferred"], 1)

    def test_rejects_wrong_orchestration_marker(self):
        rows = [verdict(1, "ip_skipped", marker="wrong")]
        with self.assertRaises(OrchestratorError):
            summarize_verdicts(
                rows=rows,
                expected_slots=1,
                batch_marker="expected",
                run_info=self.run_info,
            )

    def test_rejects_duplicate_attempts(self):
        rows = [verdict(1, "ip_skipped"), verdict(1, "ip_skipped")]
        with self.assertRaises(OrchestratorError):
            summarize_verdicts(
                rows=rows,
                expected_slots=2,
                batch_marker="target-b001",
                run_info=self.run_info,
            )


if __name__ == "__main__":
    unittest.main()
