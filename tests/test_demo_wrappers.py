import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_terminal_wrapper_rejects_ambiguous_output_argument() -> None:
    result = subprocess.run(
        [str(ROOT / "demo-terminal-bench.sh"), "--output", "/tmp/ignored.json"],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 2
    assert "Set OUTPUT=" in result.stderr
