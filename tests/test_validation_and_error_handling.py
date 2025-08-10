"""Tests for validation logic and error handling throughout the framework.

This module contains tests for:
- Configuration validation and error handling
- Constants usage verification
- Enhanced error messages and exception handling
- JSON file loading error scenarios
- Performance monitoring integration
"""

import pytest
import tempfile
import os
from reconciliation.framework import ReconciliationFramework
from reconciliation.constants import STATUS_FAILED, STATUS_PASSED


def test_enhanced_error_handling():
    """Test enhanced error handling in configuration."""
    # Test invalid sample_fraction
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
source:
  path: "tests/data/test_source.json"
  multiline: true
target:
  path: "tests/data/test_target.json"
  multiline: true
settings:
  sample_fraction: 2.0  # Invalid value > 1
  key_fields:
    - id
""")
        f.flush()
        
        with pytest.raises(ValueError, match="sample_fraction must be between 0 and 1"):
            framework = ReconciliationFramework(f.name, connection_type='json')
        
        os.unlink(f.name)


def test_enhanced_error_handling_empty_key_fields():
    """Test error handling for empty key_fields."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
source:
  path: "tests/data/test_source.json"
  multiline: true
target:
  path: "tests/data/test_target.json"
  multiline: true
settings:
  key_fields: []  # Empty key fields
""")
        f.flush()
        
        with pytest.raises(ValueError, match="key_fields must not be empty"):
            framework = ReconciliationFramework(f.name, connection_type='json')
        
        os.unlink(f.name)


def test_enhanced_error_handling_invalid_threshold():
    """Test error handling for invalid record count threshold."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
source:
  path: "tests/data/test_source.json"
  multiline: true
target:
  path: "tests/data/test_target.json"
  multiline: true
settings:
  record_count_threshold: 1.5  # Invalid value > 1
  key_fields:
    - id
""")
        f.flush()
        
        with pytest.raises(ValueError, match="record_count_threshold must be between 0 and 1"):
            framework = ReconciliationFramework(f.name, connection_type='json')
        
        os.unlink(f.name)


def test_constants_usage():
    """Test that constants are properly used throughout the framework."""
    from reconciliation.constants import (
        STATUS_PASSED, STATUS_FAILED, STATUS_SUCCESS, STATUS_ERROR,
        DEFAULT_TOP_K_EXAMPLES, DEFAULT_SAMPLE_FRACTION
    )
    
    # Test constants are properly defined
    assert STATUS_PASSED == 'PASSED'
    assert STATUS_FAILED == 'FAILED'
    assert STATUS_SUCCESS == 'Success'
    assert STATUS_ERROR == 'Error'
    assert DEFAULT_TOP_K_EXAMPLES == 10
    assert DEFAULT_SAMPLE_FRACTION == 1.0


def test_enhanced_json_error_handling():
    """Test enhanced error handling for JSON file loading."""
    from reconciliation.connections import JSONConnectionManager
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
    manager = JSONConnectionManager(spark)
    
    # Test file not found
    with pytest.raises(FileNotFoundError, match="JSON file not found"):
        manager.load_dataset("nonexistent_file.json")


def test_performance_monitoring_integration():
    """Test that performance monitoring doesn't break normal operation."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
source:
  path: "tests/data/test_source.json"
  multiline: true
target:
  path: "tests/data/test_target.json"
  multiline: true
settings:
  key_fields:
    - id
  top_k_examples: 5
  sample_fraction: 1.0
""")
        f.flush()
        
        try:
            framework = ReconciliationFramework(f.name, connection_type='json')
            report = framework.run_reconciliation()
            
            # Verify the report is generated successfully
            assert report is not None
            assert report.overall_status in [STATUS_PASSED, STATUS_FAILED]
            assert hasattr(report, 'timestamp')
            assert hasattr(report, 'record_count_comparison')
            
        finally:
            os.unlink(f.name)
