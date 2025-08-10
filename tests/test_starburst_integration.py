import os
import pytest

from reconciliation.framework import ReconciliationFramework

REQUIRED_ENV = ["STARBURST_HOST", "STARBURST_PORT", "STARBURST_USER", "STARBURST_PASSWORD"]

def _has_starburst_env():
    return all(os.getenv(k) for k in REQUIRED_ENV)

try:
    import pystarburst  # type: ignore
    HAS_PYSTARBURST = True
except Exception:
    HAS_PYSTARBURST = False

pytestmark = pytest.mark.skipif(not (_has_starburst_env() and HAS_PYSTARBURST), reason="Starburst env or library not available")


def test_starburst_smoke(monkeypatch, tmp_path):
    """Smoke test for Starburst connection if environment is configured.
    Creates a minimal config that selects from a table with optional filter.
    """
    # Build a minimal config using env variables
    catalog = os.getenv("STARBURST_CATALOG", "hive")
    database = os.getenv("STARBURST_DATABASE", "default")
    table = os.getenv("STARBURST_TABLE", "some_small_table")

    config_content = f"""
source:
  catalog: "{catalog}"
  database: "{database}"
  table: "{table}"

target:
  catalog: "{catalog}"
  database: "{database}"
  table: "{table}"

columns:
  exclude: []
  mappings: []

settings:
  key_fields: []
"""
    # key_fields must not be empty - so provide a placeholder column likely to exist (id)
    config_content = config_content.replace("key_fields: []", "key_fields:\n  - id")

    cfg = tmp_path / "sb_config.yaml"
    cfg.write_text(config_content)

    fw = ReconciliationFramework(str(cfg), connection_type='starburst')

    # Should be able to run until load; errors will surface normally if table doesn't exist
    report = fw.run_reconciliation()
    assert report is not None
