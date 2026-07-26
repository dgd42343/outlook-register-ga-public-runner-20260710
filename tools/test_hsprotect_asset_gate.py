import hashlib
from pathlib import Path

from hsprotect_asset_gate import evaluate_gate, main


def _write(path: Path, body: bytes) -> Path:
    path.write_bytes(body)
    return path


def test_first_exact_pair_is_admitted_without_resample(tmp_path):
    body = b"expected asset"
    expected = hashlib.sha256(body).hexdigest()
    safe = evaluate_gate(
        expected,
        [_write(tmp_path / "1", body), _write(tmp_path / "2", body)],
    )
    assert safe["admitted"] is True
    assert safe["reason"] == "pinned_asset_admitted"
    assert safe["matched_pair"] == [1, 2]


def test_transient_variant_is_recovered_by_consecutive_exact_pair(tmp_path):
    wanted = b"expected asset"
    other = b"same-family transient variant"
    expected = hashlib.sha256(wanted).hexdigest()
    safe = evaluate_gate(
        expected,
        [
            _write(tmp_path / "1", other),
            _write(tmp_path / "2", wanted),
            _write(tmp_path / "3", wanted),
        ],
    )
    assert safe["admitted"] is True
    assert safe["reason"] == "pinned_asset_admitted_after_bounded_resample"
    assert safe["matched_pair"] == [2, 3]
    assert len(safe["distinct_sha256"]) == 2


def test_alternating_variants_remain_fail_closed(tmp_path):
    wanted = b"expected asset"
    other = b"other asset"
    expected = hashlib.sha256(wanted).hexdigest()
    safe = evaluate_gate(
        expected,
        [
            _write(tmp_path / "1", other),
            _write(tmp_path / "2", wanted),
            _write(tmp_path / "3", other),
            _write(tmp_path / "4", wanted),
        ],
    )
    assert safe["admitted"] is False
    assert safe["reason"] == "asset_changed_during_gate"
    assert safe["matched_pair"] is None


def test_stable_wrong_asset_remains_fail_closed_and_writes_outputs(tmp_path):
    wanted = b"expected asset"
    other = b"wrong stable asset"
    expected = hashlib.sha256(wanted).hexdigest()
    safe_path = tmp_path / "safe.json"
    github_output = tmp_path / "github-output.txt"
    code = main(
        [
            "--expected",
            expected,
            "--sample",
            str(_write(tmp_path / "1", other)),
            "--sample",
            str(_write(tmp_path / "2", other)),
            "--safe-output",
            str(safe_path),
            "--github-output",
            str(github_output),
        ]
    )
    assert code == 2
    assert '"reason": "asset_sha256_mismatch"' in safe_path.read_text(encoding="utf-8")
    assert "admitted=false" in github_output.read_text(encoding="utf-8")


def test_workflow_fetches_at_most_four_samples_and_uses_gate_tool():
    workflow = (
        Path(__file__).parents[1]
        / ".github/workflows/ctf-ga-service-abuse-auto.yml"
    ).read_text(encoding="utf-8")
    assert "for attempt in 1 2 3 4; do" in workflow
    assert 'if [ "$current_sha" = "$HSPROTECT_MAIN_ASSET_SHA256" ]' in workflow
    assert '[ "$previous_sha" = "$HSPROTECT_MAIN_ASSET_SHA256" ]' in workflow
    assert "tools/hsprotect_asset_gate.py" in workflow
