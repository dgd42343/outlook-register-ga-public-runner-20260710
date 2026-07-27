from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ctf-ga-service-abuse-auto.yml"


class ServiceAbuseWorkflowContractTests(unittest.TestCase):
    def test_approach_then_outer_treatment_and_private_source_are_pinned(self):
        source = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn(
            'NATURAL_HANDLER_INTERNAL_APPROACH_THEN_OUTER: "true"',
            source,
        )
        self.assertEqual(
            source.count("8d80679c07e6d6b204bd4a0975f63e4ee7068af9"),
            2,
        )
        self.assertNotIn("4a10139bc1b39ff605f48080392fccc21592f425", source)

    def test_controller_overlay_identity_follows_pinned_private_commit(self):
        source = WORKFLOW.read_text(encoding="utf-8")
        start = source.index("      - name: Apply 875b057 pre-press guard overlay")
        end = source.index("\n      - name:", start + 1)
        apply_step = source[start:end]

        self.assertIn(
            'overlay_blob=$(git -C "$GITHUB_WORKSPACE" rev-parse',
            apply_step,
        )
        self.assertIn('overlay_sha=$(sha256sum "$overlay"', apply_step)
        self.assertEqual(apply_step.count("hash-object --no-filters"), 2)
        self.assertIn('echo "derived_blob=$overlay_blob"', apply_step)
        self.assertIn('echo "derived_sha256=$overlay_sha"', apply_step)

        # Only the immutable base controller may be repeated here. The mutable
        # overlay identity comes from the already-pinned private source commit.
        self.assertEqual(
            re.findall(r"\b[0-9a-f]{40}\b", apply_step),
            [
                "dbf139e1c63cdbabb08262b75c797244cddb4a15",
                "dbf139e1c63cdbabb08262b75c797244cddb4a15",
            ],
        )
        self.assertEqual(re.findall(r"\b[0-9a-f]{64}\b", apply_step), [])


if __name__ == "__main__":
    unittest.main()
