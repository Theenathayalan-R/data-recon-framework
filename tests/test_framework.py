import pytest
from pyspark.sql import SparkSession
import os
from reconciliation.framework import ReconciliationFramework

@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .appName("Framework Tests")
            .master("local[*]")
            .getOrCreate())

@pytest.fixture
def test_config_dir(tmp_path):
    """Create test configuration files."""
    # Create main config
    config_content = """
source:
  path: "tests/data/test_source.json"
  multiline: true

target:
  path: "tests/data/test_target.json"
  multiline: true

columns:
  exclude:
    - updated_timestamp
    - etl_batch_id
  mappings:
    - source_field: id
      target_field: id
      comparison_type: exact
      tolerance: 0
    - source_field: amount
      target_field: value
      comparison_type: numeric
      tolerance: 0.1

settings:
  record_count_threshold: 0.99
  key_fields:
    - id
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    return str(tmp_path)

def test_framework_initialization(test_config_dir):
    """Test framework initialization."""
    config_path = os.path.join(test_config_dir, "test_config.yaml")
    framework = ReconciliationFramework(config_path, connection_type="json")
    
    assert framework.config is not None
    assert framework.connection_manager is not None

def test_framework_reconciliation(test_config_dir):
    """Test full reconciliation process."""
    config_path = os.path.join(test_config_dir, "test_config.yaml")
    framework = ReconciliationFramework(config_path, connection_type="json")
    
    report = framework.run_reconciliation()
    
    assert report is not None
    assert report.overall_status in ["PASSED", "FAILED"]
    assert report.record_count_comparison is not None
    assert len(report.field_comparisons) > 0

def test_framework_results_writing(test_config_dir, tmp_path):
    """Test writing reconciliation results."""
    config_path = os.path.join(test_config_dir, "test_config.yaml")
    framework = ReconciliationFramework(config_path, connection_type="json")
    
    report = framework.run_reconciliation()
    results_path = os.path.join(tmp_path, "results.json")
    
    framework.write_results(report, results_path)
    assert os.path.exists(results_path)
