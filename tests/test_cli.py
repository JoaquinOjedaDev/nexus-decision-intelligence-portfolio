from pathlib import Path

from typer.testing import CliRunner

from nexus.cli import app

runner = CliRunner()
ROOT = Path(__file__).resolve().parents[1]


def test_version_command() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_doctor_command() -> None:
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    assert "sku-location-week" in result.stdout


def test_contract_validation_command() -> None:
    result = runner.invoke(app, ["contracts", "validate"])
    assert result.exit_code == 0
    assert '"validated": 6' in result.stdout


def test_storage_proof_command(tmp_path) -> None:
    result = runner.invoke(app, ["storage", "prove", "--artifact-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert '"raw_rows": 5' in result.stdout
    assert (tmp_path / "storage-proof-manifest.json").is_file()


def test_r0_verify_command() -> None:
    result = runner.invoke(app, ["r0", "verify", "--skip-dbt"])
    assert result.exit_code == 0
    assert "required R0 files" in result.stdout
    assert "contract examples" in result.stdout
    assert "storage proof" in result.stdout


def test_r1_demo_command(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "r1",
            "demo",
            "--source-dir",
            str(ROOT / "fixtures" / "r1"),
            "--artifact-dir",
            str(tmp_path / "demo"),
        ],
    )

    assert result.exit_code == 0
    assert "NEXUS R1 closed loop" in result.stdout
    assert "recommended quantity=47" in result.stdout
    assert "absolute error=2" in result.stdout


def test_r1_verify_command(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "r1",
            "verify",
            "--source-dir",
            str(ROOT / "fixtures" / "r1"),
            "--artifact-dir",
            str(tmp_path / "verification"),
        ],
    )

    assert result.exit_code == 0
    assert "NEXUS R1 verification" in result.stdout
    assert "deterministic fingerprint" in result.stdout


def test_r2c_plan_makes_full_scale_and_opt_in_explicit() -> None:
    result = runner.invoke(app, ["r2c", "plan", "--profile", "full"])

    assert result.exit_code == 0
    assert '"series_count": 30490' in result.stdout
    assert '"demand_rows": 59181090' in result.stdout
    assert '"requires_explicit_opt_in": true' in result.stdout
    assert '"executable_by_default": false' in result.stdout


def test_r2c_generate_writes_a_manifest_bound_smoke_package(tmp_path: Path) -> None:
    output_dir = tmp_path / "source"
    result = runner.invoke(
        app,
        [
            "r2c",
            "generate",
            "--profile",
            "smoke",
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0
    assert '"classification": "synthetic"' in result.stdout
    assert '"demand_rows": 3360' in result.stdout
    assert (output_dir / "profile-manifest.json").is_file()


def test_r2c_rejects_an_unknown_profile() -> None:
    result = runner.invoke(app, ["r2c", "plan", "--profile", "unknown"])

    assert result.exit_code != 0
    assert "profile must be one of" in result.output + result.stderr


def test_r2c_full_commands_require_opt_in_before_writing(tmp_path: Path) -> None:
    source = tmp_path / "full-source"
    verification = tmp_path / "full-verification"
    generated = runner.invoke(
        app,
        ["r2c", "generate", "--profile", "full", "--output-dir", str(source)],
    )
    verified = runner.invoke(
        app,
        [
            "r2c",
            "verify",
            "--profile",
            "full",
            "--artifact-root",
            str(verification),
        ],
    )
    assert generated.exit_code != 0
    assert verified.exit_code != 0
    assert not source.exists()
    assert not verification.exists()
