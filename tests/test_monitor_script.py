from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path


class TestMonitorScript(unittest.TestCase):
    def test_beb_monitor_script_remains_read_only(self) -> None:
        script_path = Path(__file__).resolve().parents[1] / "beb-monitor.sh"
        script = script_path.read_text(encoding="utf-8")
        lower_script = script.lower()

        forbidden_fragments = (
            "docker compose restart",
            "docker compose stop",
            "docker compose start",
            "docker restart",
            "docker stop",
            "docker start",
            "insert into",
            "update ",
            "delete from",
            "vacuum",
            "mosquitto_pub",
            "rm ",
            "rm -",
            "remove-item",
            "> .env",
            ">> .env",
            "sed -i",
        )
        for fragment in forbidden_fragments:
            with self.subTest(fragment=fragment):
                self.assertNotIn(fragment, lower_script)

    def test_beb_monitor_script_has_valid_bash_syntax(self) -> None:
        bash = shutil.which("bash")
        if bash is None:
            self.skipTest("bash is unavailable")

        script_path = Path(__file__).resolve().parents[1] / "beb-monitor.sh"
        result = subprocess.run(
            [bash, "-n", str(script_path)],
            text=True,
            capture_output=True,
            check=False,
        )
        combined_output = f"{result.stdout}\n{result.stderr}".replace("\x00", "")
        if "Access is denied" in combined_output or "E_ACCESSDENIED" in combined_output:
            self.skipTest("bash is installed but blocked by local OS policy")

        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
