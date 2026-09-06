from pathlib import Path

from metadata_etl.source import preserve_raw_copy


def test_raw_copy_is_byte_identical(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_bytes(b"id,name\n001,Ada\n")

    artifact = preserve_raw_copy(source, tmp_path / "raw", "example", "RUN_TEST")

    assert artifact.path.read_bytes() == source.read_bytes()
    assert artifact.bytes_copied == len(source.read_bytes())
    assert len(artifact.sha256) == 64
