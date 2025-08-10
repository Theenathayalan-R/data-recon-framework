from dataclasses import dataclass
from typing import Dict, Any, Optional, List

@dataclass
class ComparisonResult:
    """Result of a single field comparison."""
    source_field: str
    target_field: str
    status: str
    message: str
    metric_value: Optional[float]
    comparison_type: str
    tolerance: float

@dataclass
class RecordCountResult:
    """Result of record count comparison."""
    source_count: int
    target_count: int
    match_percentage: float
    threshold_met: bool

@dataclass
class ReconciliationReport:
    """Complete reconciliation report."""
    record_count_comparison: RecordCountResult
    field_comparisons: Dict[str, List[ComparisonResult]]
    overall_status: str
    timestamp: str  # ISO format timestamp
    source_details: Dict[str, Any]
    target_details: Dict[str, Any]

@dataclass
class ColumnMapping:
    """Configuration for column mapping."""
    source_field: str
    target_field: str
    comparison_type: str
    tolerance: float = 0.0
