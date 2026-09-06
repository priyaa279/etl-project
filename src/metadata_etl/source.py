from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path

from metadata_etl.errors import ExtractionError


@dataclass(frozen=True)
class RawArtifact:
    path: Path
    sha256: str
    bytes_copied: int


def preserve_raw_copy(source: Path, raw_root: Path, dataset: str, run_id: str) -> RawArtifact:
    """Copy source bytes before parsing so the original input remains reproducible."""
    destination_dir = raw_root / dataset / run_id.lower()
    destination = destination_dir / source.name
    try:
        destination_dir.mkdir(parents=True, exist_ok=False)
        shutil.copy2(source, destination)
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    except OSError as exc:
        raise ExtractionError(f"Could not preserve raw source at {destination}: {exc}") from exc
    return RawArtifact(destination, digest, destination.stat().st_size)
