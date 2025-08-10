import pytest
from pyspark.sql import SparkSession
from reconciliation.comparators import RecordCountComparator, FieldComparator, ColumnMatcher

@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    spark = (SparkSession.builder
            .appName("Comparators Tests")
            .master("local[*]")
            .getOrCreate())
    yield spark
    spark.stop()

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

def test_record_count_comparator_zero_counts(spark):
    """Test record count comparator when both sides are empty (no division by zero)."""
    empty1 = spark.createDataFrame([], schema="id INT")
    empty2 = spark.createDataFrame([], schema="id INT")
    comparator = RecordCountComparator(threshold=1.0)
    result = comparator.compare(empty1, empty2)
    assert result.match_percentage == 1.0
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

def test_field_comparator_requires_keys(source_data, target_data):
    """Ensure comparator fails when key_fields are empty."""
    from reconciliation.models import ColumnMapping
    comparator = FieldComparator(key_fields=[])
    mapping = ColumnMapping(
        source_field="amount",
        target_field="value",
        comparison_type="numeric",
        tolerance=0.0
    )
    with pytest.raises(ValueError):
        comparator.compare(source_data, target_data, mapping)

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

# New tests for datetime comparator

def test_datetime_comparator_timezone_and_minutes_tolerance(spark):
    from reconciliation.models import ColumnMapping
    # Source times are naive local times in America/New_York; target times are naive UTC wall times
    src_rows = [
        (1, "2025-08-01 10:00:00"),  # EDT -> 14:00:00Z
        (2, "2025-12-01 09:00:00"),  # EST -> 14:00:00Z
        (3, "2025-08-01 08:00:00"),  # EDT -> 12:00:00Z
    ]
    tgt_rows = [
        (1, "2025-08-01 14:00:30"),  # 30s diff -> within 2 minutes
        (2, "2025-12-01 14:00:00"),  # exact match
        (3, "2025-08-01 12:05:00"),  # 5 minutes diff -> outside 2 minutes
    ]
    source_df = spark.createDataFrame(src_rows, ["id", "event_time_src"])
    target_df = spark.createDataFrame(tgt_rows, ["id", "event_time_tgt"])

    comparator = FieldComparator(key_fields=["id"])
    mapping = ColumnMapping(
        source_field="event_time_src",
        target_field="event_time_tgt",
        comparison_type="datetime",
        tolerance=2,
        datetime_tolerance_unit="minutes",
        source_format="yyyy-MM-dd HH:mm:ss",
        source_timezone="America/New_York",
        target_format="yyyy-MM-dd HH:mm:ss",
        target_timezone="UTC",
    )

    results = comparator.compare(source_df, target_df, mapping)
    # Expect one error example (id=3 ~300s diff) due to error-priority sampling
    assert len(results) == 1
    err = results[0]
    assert err.status == "Error"
    assert err.keys is not None and err.keys.get("id") == 3
    assert err.metric_value is not None and abs(err.metric_value - 300.0) < 1.0


def test_datetime_comparator_seconds_tolerance_iso(spark):
    from reconciliation.models import ColumnMapping
    src = [(1, "2025-08-01T10:00:00Z")]
    tgt = [(1, "2025-08-01T10:00:20Z")]
    s_df = spark.createDataFrame(src, ["id", "ts_s"])
    t_df = spark.createDataFrame(tgt, ["id", "ts_t"])

    comparator = FieldComparator(key_fields=["id"])
    mapping = ColumnMapping(
        source_field="ts_s",
        target_field="ts_t",
        comparison_type="datetime",
        tolerance=30,
        datetime_tolerance_unit="seconds",
    )

    results = comparator.compare(s_df, t_df, mapping)
    # With no errors, comparator returns successes up to top_k
    assert len(results) >= 1
    assert all(r.status == "Success" for r in results)
    assert any(r.metric_value is not None and abs(r.metric_value - 20.0) < 1.0 for r in results)
