from typing import Dict, Any
import pytest
from pyspark.sql import SparkSession
import os
import json
from reconciliation.connections import JSONConnectionManager, StarburstConnectionManager

@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .appName("Reconciliation Tests")
            .master("local[*]")
            .getOrCreate())

@pytest.fixture
def test_data_path():
    """Get the test data directory path."""
    return os.path.join(os.path.dirname(__file__), '..', 'data')

@pytest.fixture
def config_path():
    """Get the config directory path."""
    return os.path.join(os.path.dirname(__file__), '..', 'config')

@pytest.fixture
def json_manager(spark):
    """Create a JSONConnectionManager for testing."""
    return JSONConnectionManager(spark)

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return [
        {"id": 1, "value": 100.0},
        {"id": 2, "value": 200.0},
        {"id": 3, "value": 300.0}
    ]

def test_json_load_dataset(spark, json_manager, test_data_path):
    """Test loading a dataset from JSON."""
    # Load the source dataset
    df = json_manager.load_dataset(
        path=os.path.join(test_data_path, "source.json")
    )
    
    # Verify the dataframe
    assert df.count() == 3
    assert "id" in df.columns
    assert "amount" in df.columns
    assert "customer_name" in df.columns

def test_json_write_dataset(spark, json_manager, tmp_path, sample_data):
    """Test writing a dataset to JSON."""
    # Create a test dataframe
    test_df = spark.createDataFrame(sample_data)
    output_path = str(tmp_path / "test_output.json")
    
    # Write the dataframe
    json_manager.write_dataset(test_df, output_path)
    
    # Verify the output
    result_df = json_manager.load_dataset(output_path)
    assert result_df.count() == len(sample_data)
