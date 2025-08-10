import pytest
from reconciliation.models import ComparisonResult, RecordCountResult, ReconciliationReport
from datetime import datetime, timezone

def test_comparison_result():
    """Test ComparisonResult model."""
    result = ComparisonResult(
        source_field="amount",
        target_field="value",
        status="Success",
        message="Test message",
        metric_value=0.95,
        comparison_type="numeric",
        tolerance=0.01
    )
    
    assert result.source_field == "amount"
    assert result.target_field == "value"
    assert result.status == "Success"
    assert result.metric_value == 0.95
    assert result.comparison_type == "numeric"

def test_record_count_result():
    """Test RecordCountResult model."""
    result = RecordCountResult(
        source_count=100,
        target_count=98,
        match_percentage=0.98,
        threshold_met=False
    )
    
    assert result.source_count == 100
    assert result.target_count == 98
    assert result.match_percentage == 0.98
    assert not result.threshold_met

def test_reconciliation_report():
    """Test ReconciliationReport model."""
    record_count = RecordCountResult(
        source_count=100,
        target_count=98,
        match_percentage=0.98,
        threshold_met=False
    )
    
    field_comparison = ComparisonResult(
        source_field="amount",
        target_field="value",
        status="Success",
        message="Test comparison",
        metric_value=0.95,
        comparison_type="numeric",
        tolerance=0.01
    )
    
    report = ReconciliationReport(
        record_count_comparison=record_count,
        field_comparisons={"amount_vs_value": [field_comparison]},
        overall_status="FAILED",
        timestamp=datetime.now(timezone.utc).isoformat(),
        source_details={"path": "data/source.json"},
        target_details={"path": "data/target.json"}
    )
    
    assert report.overall_status == "FAILED"
    assert len(report.field_comparisons) == 1
    assert report.record_count_comparison.match_percentage == 0.98
