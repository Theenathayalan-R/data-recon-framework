import pytest
from pyspark.sql import SparkSession
from reconciliation.comparators import RecordCountComparator, FieldComparator, ColumnMatcher

@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .appName("Comparators Tests")
            .master("local[*]")
            .getOrCreate())

@pytest.fixture
def source_data(spark):
    """Create sample source data."""
    data = [
        (1, "A", 100.0),
        (2, "B", 200.0),
        (3, "C", 300.0)
    ]
    return spark.createDataFrame(data, ["id", "name", "amount"])

@pytest.fixture
def target_data(spark):
    """Create sample target data."""
    data = [
        (1, "A", 100.0),
        (2, "B", 200.1),  # Small difference
        (3, "D", 300.0)   # Name mismatch
    ]
    return spark.createDataFrame(data, ["id", "name", "value"])

def test_record_count_comparator(source_data, target_data):
    """Test record count comparison."""
    comparator = RecordCountComparator(threshold=1.0)
    result = comparator.compare(source_data, target_data)
    
    assert result.source_count == 3
    assert result.target_count == 3
    assert result.threshold_met is True

def test_field_comparator(source_data, target_data):
    """Test field comparison."""
    from reconciliation.models import ColumnMapping
    
    comparator = FieldComparator(key_fields=["id"])
    mapping = ColumnMapping(
        source_field="amount",
        target_field="value",
        comparison_type="numeric",
        tolerance=0.1
    )
    results = comparator.compare(source_data, target_data, mapping)
    
    assert len(results) > 0
    assert all(r.source_field == "amount" for r in results)
    assert all(r.target_field == "value" for r in results)

def test_column_matcher(source_data, target_data):
    """Test column matching."""
    from reconciliation.models import ColumnMapping
    
    matcher = ColumnMatcher(excluded_columns=[])
    configured_mappings = [
        ColumnMapping(source_field="amount", target_field="value", comparison_type="numeric", tolerance=0.1)
    ]
    mappings = matcher.get_matching_columns(source_data, target_data, configured_mappings)
    
    assert len(mappings) > 0
    assert any(m.source_field == "amount" and m.target_field == "value" for m in mappings)
    assert any(m.source_field == "id" and m.target_field == "id" for m in mappings)
