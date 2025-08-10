import pytest
from pyspark.sql import SparkSession
from reconciliation import ReconciliationFramework
import yaml
import os

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (SparkSession.builder
            .appName("ReconciliationTest")
            .master("local[*]")
            .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def sample_config(tmp_path):
    """Create a sample JSON-based configuration file for testing."""
    config_data = {
        'source': {
            'path': os.path.join('tests', 'data', 'test_source.json'),
            'multiline': True
        },
        'target': {
            'path': os.path.join('tests', 'data', 'test_target.json'),
            'multiline': True
        },
        'columns': {
            'exclude': [],
            'mappings': [
                {
                    'source_field': 'id',
                    'target_field': 'id',
                    'comparison_type': 'exact',
                    'tolerance': 0
                },
                {
                    'source_field': 'amount',
                    'target_field': 'value',
                    'comparison_type': 'numeric',
                    'tolerance': 0.01
                }
            ]
        },
        'settings': {
            'record_count_threshold': 0.99,
            'key_fields': ['id']
        }
    }
    config_path = tmp_path / "test_config.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)
    return str(config_path)

@pytest.fixture
def sample_source_data(spark):
    """Create sample source data for testing."""
    return spark.createDataFrame([
        (1, 100.0),
        (2, 200.0),
        (3, 300.0)
    ], ["id", "value"])

@pytest.fixture
def sample_target_data(spark):
    """Create sample target data for testing."""
    return spark.createDataFrame([
        (1, 100.0),
        (2, 200.1),  # Slight difference within tolerance
        (3, 350.0)   # Significant difference
    ], ["id", "value"])

def test_record_count_comparison(spark):
    """Test record count comparison functionality."""
    from reconciliation.comparators import RecordCountComparator
    
    # Create test data
    source_data = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    target_data = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    
    comparator = RecordCountComparator(0.99)
    result = comparator.compare(source_data, target_data)
    assert result.match_percentage == 1.0
    assert result.threshold_met is True

def test_field_value_comparison(spark, sample_source_data, sample_target_data):
    """Test field value comparison functionality."""
    from reconciliation.comparators import FieldComparator
    from reconciliation.models import ColumnMapping
    
    mapping = ColumnMapping(
        source_field='value',
        target_field='value',
        comparison_type='numeric',
        tolerance=0.01
    )
    
    comparator = FieldComparator(['id'])
    results = comparator.compare(sample_source_data, sample_target_data, mapping)
    
    # Should have comparison results
    assert len(results) > 0
    assert all(hasattr(result, 'status') for result in results)

def test_end_to_end_reconciliation(sample_config):
    """Test complete end-to-end reconciliation process using JSON files."""
    framework = ReconciliationFramework(sample_config, connection_type='json')
    
    results = framework.run_reconciliation()
    
    # Check that results contain expected keys
    assert hasattr(results, 'record_count_comparison')
    assert hasattr(results, 'field_comparisons')
    assert hasattr(results, 'overall_status')
    assert results.overall_status in ['PASSED', 'FAILED']

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
