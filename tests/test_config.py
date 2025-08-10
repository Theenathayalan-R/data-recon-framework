import pytest
import os
from reconciliation.config import Config

@pytest.fixture
def sample_config_path(tmp_path):
    """Create a sample config file for testing."""
    config_content = """
source:
  path: "data/source.json"
  multiline: true

target:
  path: "data/target.json"
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

settings:
  record_count_threshold: 0.99
  field_match_threshold: 0.95
  batch_size: 100000
  key_fields:
    - id
"""
    config_file = tmp_path / "test_config.yaml"
    config_file.write_text(config_content)
    return str(config_file)

def test_config_loading(sample_config_path):
    """Test loading configuration from YAML."""
    config = Config(sample_config_path)
    
    # Test source config
    assert config.source_config["path"] == "data/source.json"
    assert config.source_config["multiline"] is True
    
    # Test target config
    assert config.target_config["path"] == "data/target.json"
    assert config.target_config["multiline"] is True
    
    # Test excluded columns
    assert "updated_timestamp" in config.excluded_columns
    assert "etl_batch_id" in config.excluded_columns
    
    # Test column mappings
    assert len(config.column_mappings) > 0
    mapping = config.column_mappings[0]
    assert mapping.source_field == "id"
    assert mapping.target_field == "id"
    assert mapping.comparison_type == "exact"
    assert mapping.tolerance == 0
    
    # Test settings
    assert config.record_count_threshold == 0.99
    assert "id" in config.key_fields

def test_config_validation(tmp_path):
    """Test configuration validation."""
    invalid_config = """
source:
  multiline: true

target:
  path: "data/target.json"
  multiline: true
"""
    config_file = tmp_path / "invalid_config.yaml"
    config_file.write_text(invalid_config)
    
    with pytest.raises(ValueError) as exc_info:
        Config(str(config_file))
    assert "Missing required configuration section" in str(exc_info.value)
