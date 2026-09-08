from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


class GitPromotionError(RuntimeError):
    """Safe error raised by the restricted configuration-versioning boundary."""


@dataclass(frozen=True)
class GitPromotionResult:
    commit_sha: str
    push_status: str


class GitConfigAdapter:
    """Version exactly one promoted config inside one fixed repository."""

    def __init__(
        self,
        repository_root: Path,
        *,
        executable: str = "git",
        push_enabled: bool = False,
        remote: str = "origin",
        branch: str = "main",
    ) -> None:
        self.repository_root = repository_root.resolve()
        self.executable = executable
        self.push_enabled = push_enabled
        self.remote = remote
        self.branch = branch

    def ensure_clean(self) -> None:
        result = self._run(["status", "--porcelain", "--untracked-files=all"])
        if result.stdout.strip():
            raise GitPromotionError(
                "Repository has tracked or untracked project changes; approval is blocked."
            )

    def commit_config(self, config_path: Path, dataset: str) -> GitPromotionResult:
        relative = self._relative_config(config_path)
        try:
            self._run(["add", "--", relative])
            self._run(
                [
                    "-c",
                    "user.name=ETL Control Center",
                    "-c",
                    "user.email=etl-control-center@localhost",
                    "commit",
                    "-m",
                    f"Approve dataset configuration: {dataset}",
                    "--",
                    relative,
                ]
            )
            commit_sha = self._run(["rev-parse", "HEAD"]).stdout.strip()
        except GitPromotionError:
            self._restore_index(relative)
            raise

        push_status = "DISABLED"
        if self.push_enabled:
            try:
                self._run(["push", self.remote, f"HEAD:{self.branch}"])
                push_status = "SUCCEEDED"
            except GitPromotionError:
                push_status = "FAILED"
        return GitPromotionResult(commit_sha=commit_sha, push_status=push_status)

    def recover_config_commit(self, config_path: Path, dataset: str) -> GitPromotionResult | None:
        """Return the exact approval commit after an interrupted database finalize."""
        relative = self._relative_config(config_path)
        commit_sha = self._run(["rev-parse", "HEAD"]).stdout.strip()
        subject = self._run(["log", "-1", "--pretty=%s"]).stdout.strip()
        changed = {
            line.strip()
            for line in self._run(
                ["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]
            ).stdout.splitlines()
            if line.strip()
        }
        if subject != f"Approve dataset configuration: {dataset}" or changed != {relative}:
            return None
        self.ensure_clean()
        push_status = "DISABLED"
        if self.push_enabled:
            try:
                self._run(["push", self.remote, f"HEAD:{self.branch}"])
                push_status = "SUCCEEDED"
            except GitPromotionError:
                push_status = "FAILED"
        return GitPromotionResult(commit_sha=commit_sha, push_status=push_status)

    def _relative_config(self, config_path: Path) -> str:
        resolved = config_path.resolve()
        try:
            relative = resolved.relative_to(self.repository_root)
        except ValueError as exc:
            raise GitPromotionError("Approved config is outside the fixed repository.") from exc
        if len(relative.parts) != 2 or relative.parts[0] != "configs":
            raise GitPromotionError("Only one top-level configs/<dataset>.yaml file may be staged.")
        return relative.as_posix()

    def _restore_index(self, relative: str) -> None:
        subprocess.run(
            [
                self.executable,
                "-c",
                f"safe.directory={self.repository_root}",
                "-c",
                "core.autocrlf=true",
                "restore",
                "--staged",
                "--",
                relative,
            ],
            cwd=self.repository_root,
            check=False,
            capture_output=True,
            text=True,
            shell=False,
            timeout=30,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )

    def _run(self, arguments: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                [
                    self.executable,
                    "-c",
                    f"safe.directory={self.repository_root}",
                    "-c",
                    "core.autocrlf=true",
                    *arguments,
                ],
                cwd=self.repository_root,
                check=True,
                capture_output=True,
                text=True,
                shell=False,
                timeout=30,
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise GitPromotionError("Git configuration versioning failed.") from exc
