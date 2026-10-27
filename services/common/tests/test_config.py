import shutil
from pathlib import Path

import pytest

from marketpulse_common.config import ConfigError, load_config

REPO_CONFIG = Path(__file__).resolve().parents[3] / "config"


def test_repo_config_loads_and_is_consistent():
    c = load_config(REPO_CONFIG)
    assert len(c.instruments) == 11
    assert {i.currency for i in c.instruments} == {"USD", "INR"}
    assert all(e in {i.entity_id for i in c.instruments} for e in c.entities)


def test_unknown_entity_reference_fails(tmp_path):
    shutil.copytree(REPO_CONFIG, tmp_path / "c")
    p = tmp_path / "c" / "entities.yaml"
    p.write_text(p.read_text() + "  ghost:\n    strong_terms: [\"Ghost\"]\n")
    with pytest.raises(ConfigError, match="unknown entity_id"):
        load_config(tmp_path / "c")


def test_ambiguous_term_without_context_fails(tmp_path):
    shutil.copytree(REPO_CONFIG, tmp_path / "c")
    p = tmp_path / "c" / "entities.yaml"
    p.write_text("entities:\n  aapl:\n    context_terms: [\"Apple\"]\n")
    with pytest.raises(ConfigError, match="context_required"):
        load_config(tmp_path / "c")


def test_missing_dir():
    with pytest.raises(ConfigError, match="missing config file"):
        load_config(Path("/nonexistent-dir"))
