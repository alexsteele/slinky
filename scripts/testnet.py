#!/usr/bin/env python3

"""Run a disposable local Slinky node through the v0 CLI."""

from __future__ import annotations

import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


class TestNet:
    def __init__(self) -> None:
        self.root_dir = Path(__file__).resolve().parent.parent
        self.sandbox_dir = Path(tempfile.mkdtemp(prefix="slinky-testnet-"))
        self.node_dir = self.sandbox_dir / "node-1"
        self.sync_root = self.sandbox_dir / "sync"
        self.config_path = self.node_dir / "config.toml"
        self.log_path = self.sandbox_dir / "slinky.log"
        self.binary = self.root_dir / "target" / "debug" / "slinky"
        self.process: subprocess.Popen[bytes] | None = None
        self.keep_sandbox = False

    def run_checked(self, cmd: list[str]) -> None:
        print("+", " ".join(str(part) for part in cmd), flush=True)
        subprocess.run(cmd, cwd=self.root_dir, check=True)

    def setup(self) -> None:
        print(f"testnet root: {self.sandbox_dir}", flush=True)
        self.node_dir.mkdir(parents=True, exist_ok=True)
        self.sync_root.mkdir(parents=True, exist_ok=True)

        hello_path = self.sync_root / "hello.txt"
        hello_path.write_text("hello\n")

        self.run_checked(["cargo", "build"])
        self.run_checked(
            [
                str(self.binary),
                "--config",
                str(self.config_path),
                "setup",
                "--sync-root",
                str(self.sync_root),
            ]
        )

    def start(self) -> None:
        with self.log_path.open("w") as log_file:
            cmd = [
                str(self.binary),
                "--config",
                str(self.config_path),
                "start",
                "--foreground",
            ]
            print("+", " ".join(cmd), flush=True)
            self.process = subprocess.Popen(
                cmd,
                cwd=self.root_dir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )

        time.sleep(2.0)

    def apply_step(self, label: str, pause: float = 1.0) -> None:
        print(f"* {label}", flush=True)
        time.sleep(pause)

    def run(self) -> None:
        hello_path = self.sync_root / "hello.txt"
        docs_path = self.sync_root / "docs"
        notes_path = docs_path / "notes.txt"
        renamed_path = docs_path / "notes-renamed.txt"

        self.apply_step("modify hello.txt")
        hello_path.write_text("hello again\n")

        self.apply_step("append hello.txt")
        with hello_path.open("a") as handle:
            handle.write("and again\n")

        self.apply_step("create docs directory")
        docs_path.mkdir(exist_ok=True)

        self.apply_step("create docs/notes.txt")
        notes_path.write_text("notes\n")

        self.apply_step("rename docs/notes.txt")
        notes_path.rename(renamed_path)

        self.apply_step("delete hello.txt")
        hello_path.unlink()

        self.apply_step("delete docs directory")
        shutil.rmtree(docs_path)

        time.sleep(2.0)

    def stop(self) -> None:
        if self.process is None or self.process.poll() is not None:
            return

        self.process.send_signal(signal.SIGINT)
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    def close(self) -> None:
        self.stop()
        if not self.keep_sandbox:
            shutil.rmtree(self.sandbox_dir, ignore_errors=True)

    def main(self) -> int:
        try:
            self.setup()
            self.start()
            self.run()
            self.stop()

            if self.process and self.process.returncode not in (0, 130, -signal.SIGINT):
                raise RuntimeError(f"slinky exited with status {self.process.returncode}")

            print("testnet run complete", flush=True)
            print(f"log file: {self.log_path}", flush=True)
            return 0
        except Exception:
            self.keep_sandbox = True
            print(
                f"testnet failed; sandbox preserved at {self.sandbox_dir}",
                file=sys.stderr,
            )
            raise
        finally:
            self.close()


def main() -> int:
    return TestNet().main()


if __name__ == "__main__":
    raise SystemExit(main())
