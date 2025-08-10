"""Constants for the reconciliation framework."""

from enum import Enum

# Enums
class OverallStatus(Enum):
    PASSED = 'PASSED'
    FAILED = 'FAILED'

class FieldStatus(Enum):
    Success = 'Success'
    Error = 'Error'

class ComparisonType(Enum):
    EXACT = 'exact'
    NUMERIC = 'numeric'
    DATETIME = 'datetime'  # New type for date/timestamp comparisons

# Default configuration values
DEFAULT_RECORD_COUNT_THRESHOLD = 1.0
DEFAULT_TOP_K_EXAMPLES = 10
DEFAULT_SAMPLE_FRACTION = 1.0

# Status constants (backward compatibility)
STATUS_PASSED = OverallStatus.PASSED.value
STATUS_FAILED = OverallStatus.FAILED.value
STATUS_SUCCESS = FieldStatus.Success.value
STATUS_ERROR = FieldStatus.Error.value

# Comparison types (backward compatibility)
COMPARISON_TYPE_EXACT = ComparisonType.EXACT.value
COMPARISON_TYPE_NUMERIC = ComparisonType.NUMERIC.value
COMPARISON_TYPE_DATETIME = ComparisonType.DATETIME.value  # Backward-compatible constant for datetime comparisons

# Connection types
CONNECTION_TYPE_JSON = 'json'
CONNECTION_TYPE_STARBURST = 'starburst'

# Logging format
LOGGING_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Performance thresholds
LARGE_DATASET_THRESHOLD = 1000000  # Row count threshold for caching
PERFORMANCE_LOG_THRESHOLD = 30.0   # Seconds threshold for performance logging
