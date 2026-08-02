import unittest

from tools.ga_target_healthy_orchestrator import (
    CLIFF_GROW_FACTOR,
    CLIFF_GROW_THRESHOLD,
    CLIFF_SHRINK_FACTOR,
    CLIFF_SHRINK_THRESHOLD,
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


class CliffAdaptiveBatchTests(unittest.TestCase):
    """Verify the orchestrator backs off when collector_minus1 (cliff) spikes."""

    def test_high_cliff_rate_shrinks_batch(self):
        # batch_slots=60, cliff_rate=0.35 (> 0.25 threshold) -> 60*0.5=30
        shrunk = compute_next_batch_size(
            target=100,
            achieved=10,
            dispatched=50,
            max_dispatched=500,
            batch_slots=60,
            min_batch_slots=5,
            prev_cliff_rate=0.35,
            consecutive_clean=0,
        )
        self.assertLessEqual(shrunk, int(60 * CLIFF_SHRINK_FACTOR))
        self.assertGreaterEqual(shrunk, 5)

    def test_low_cliff_rate_alone_does_not_grow(self):
        # One clean batch is not enough; need two consecutive.
        result = compute_next_batch_size(
            target=100,
            achieved=10,
            dispatched=50,
            max_dispatched=500,
            batch_slots=60,
            min_batch_slots=5,
            prev_cliff_rate=0.02,
            consecutive_clean=1,
        )
        # Without 2 consecutive clean, effective_cap stays at batch_slots=60
        self.assertLessEqual(result, 60)

    def test_two_clean_batches_enable_growth(self):
        result = compute_next_batch_size(
            target=100,
            achieved=10,
            dispatched=50,
            max_dispatched=500,
            batch_slots=60,
            min_batch_slots=5,
            prev_cliff_rate=0.02,
            consecutive_clean=2,
        )
        # 60 * 1.3 = 78, but capped by batch_slots=60
        self.assertLessEqual(result, 60)

    def test_growth_helps_when_batch_cap_is_low(self):
        # If previously shrunk (effective cap was 30), two clean batches
        # should grow toward 30*1.3=39.
        result = compute_next_batch_size(
            target=100,
            achieved=10,
            dispatched=50,
            max_dispatched=500,
            batch_slots=30,
            min_batch_slots=5,
            prev_cliff_rate=0.02,
            consecutive_clean=2,
        )
        self.assertGreaterEqual(result, min(30, int(30 * CLIFF_GROW_FACTOR)))

    def test_cliff_does_not_override_hard_budget(self):
        result = compute_next_batch_size(
            target=100,
            achieved=99,
            dispatched=99,
            max_dispatched=100,
            batch_slots=60,
            min_batch_slots=5,
            prev_cliff_rate=0.50,
            consecutive_clean=0,
        )
        self.assertLessEqual(result, 1)

    def test_zero_cliff_first_batch_unchanged(self):
        result = compute_next_batch_size(
            target=100,
            achieved=0,
            dispatched=0,
            max_dispatched=500,
            batch_slots=60,
            min_batch_slots=5,
            prev_cliff_rate=0.0,
            consecutive_clean=0,
        )
        self.assertEqual(result, 60)


class CliffVerdictSummaryTests(unittest.TestCase):
    """Verify summarize_verdicts detects collector_minus1 live slots."""

    def setUp(self):
        self.run_info = {
            "databaseId": 42,
            "url": "https://github.com/a/b/actions/runs/42",
            "conclusion": "failure",
            "headSha": "abc",
            "createdAt": "2026-07-29T10:00:00Z",
            "updatedAt": "2026-07-29T10:10:00Z",
        }

    def test_collector_minus1_detected_as_cliff(self):
        rows = [
            verdict(1, "strict_success", accepted_result0=True, strict_success=True,
                    graph_import_ok=True, account_lifecycle="graph_healthy"),
            verdict(2, "technical_failure", accepted_result0=False),
            verdict(3, "technical_failure", accepted_result0=False),
            verdict(4, "technical_failure", accepted_result0=False),
            verdict(5, "ip_skipped"),
        ]
        summary = summarize_verdicts(
            rows=rows,
            expected_slots=5,
            batch_marker="target-b001",
            run_info=self.run_info,
        )
        # 4 live, 3 of which are cliff (technical_failure, no accepted_result0)
        self.assertEqual(summary["live"], 4)
        self.assertEqual(summary["collector_cliff_live"], 3)
        self.assertAlmostEqual(summary["collector_cliff_rate"], 0.75)

    def test_riskblock_not_counted_as_cliff(self):
        rows = [
            verdict(1, "ip_riskblock", explicit_riskblock=True),
            verdict(2, "ip_riskblock", explicit_riskblock=True),
            verdict(3, "technical_failure", accepted_result0=False),
            verdict(4, "ip_skipped"),
        ]
        summary = summarize_verdicts(
            rows=rows,
            expected_slots=4,
            batch_marker="target-b001",
            run_info=self.run_info,
        )
        # 3 live, but only 1 is cliff (riskblock excluded)
        self.assertEqual(summary["collector_cliff_live"], 1)
        self.assertAlmostEqual(summary["collector_cliff_rate"], 1.0 / 3.0)

    def test_probe_timeout_not_counted_as_cliff(self):
        rows = [
            verdict(1, "technical_failure", accepted_result0=False, probe_timed_out=True),
            verdict(2, "technical_failure", accepted_result0=False),
            verdict(3, "ip_skipped"),
        ]
        summary = summarize_verdicts(
            rows=rows,
            expected_slots=3,
            batch_marker="target-b001",
            run_info=self.run_info,
        )
        # 2 live, 1 cliff (probe_timed_out excluded)
        self.assertEqual(summary["collector_cliff_live"], 1)
        self.assertAlmostEqual(summary["collector_cliff_rate"], 0.5)


if __name__ == "__main__":
    unittest.main()
