from dataclasses import dataclass, field
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
    # Optional enriched context for triage
    source_value: Optional[Any] = None
    target_value: Optional[Any] = None
    keys: Optional[Dict[str, Any]] = None

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
    mismatch_counts: Dict[str, int] = field(default_factory=dict)
    unmatched_source_count: int = 0
    unmatched_target_count: int = 0
    unmatched_source_examples: List[Dict[str, Any]] = field(default_factory=list)
    unmatched_target_examples: List[Dict[str, Any]] = field(default_factory=list)
    # Aggregated stats per field mapping
    field_aggregates: Dict[str, Dict[str, float]] = field(default_factory=dict)

@dataclass
class ColumnMapping:
    """Configuration for column mapping."""
    source_field: str
    target_field: str
    comparison_type: str
    tolerance: float = 0.0
    # The following options are used for datetime comparisons and ignored otherwise
    # Parse formats, e.g., 'yyyy-MM-dd HH:mm:ss' or ISO8601; if omitted, Spark will infer when possible
    source_format: Optional[str] = None
    target_format: Optional[str] = None
    # Time zones for naive datetimes (e.g., 'UTC', 'America/New_York').
    # Only set if your input strings/values do NOT include timezone information.
    source_timezone: Optional[str] = None
    target_timezone: Optional[str] = None
    # Unit in which tolerance is expressed: 'seconds', 'minutes', 'hours', or 'milliseconds'
    datetime_tolerance_unit: str = 'seconds'
