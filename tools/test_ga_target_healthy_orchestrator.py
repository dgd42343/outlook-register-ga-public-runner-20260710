import unittest

from tools.ga_target_healthy_orchestrator import (
    OrchestratorError,
    compute_next_batch_size,
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
