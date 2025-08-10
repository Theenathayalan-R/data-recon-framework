# Data Reconciliation Framework

A robust, production-ready data reconciliation framework built with PySpark for comparing datasets across different sources. It provides configurable comparison strategies, tolerance levels, and comprehensive reporting that includes both accuracy (field-level mismatches) and completeness (unmatched records) checks. Features modern Python packaging, comprehensive testing, and enterprise-grade reliability.

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/pyspark-3.3+-orange.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Version](https://img.shields.io/badge/version-1.1.0-brightgreen.svg)](./pyproject.toml)

## 🚀 Features

### Core Capabilities
- Multi-Source Support: Connect to Starburst databases or local JSON files.
- Completeness Checks: Identifies and reports records present in one source but not the other.
- Accuracy Checks: Performs field-by-field value comparisons for matched records.
- Flexible Comparisons:
  - exact: string/structural equality
  - numeric: numeric tolerance with absolute difference
  - datetime: time-aware comparison with format parsing, timezone normalization, and tolerance in configurable units
- Smart Column Mapping: Map fields between source and target, even if they have different names.
- Comprehensive Reporting: Detailed JSON reports with overall status, mismatch counts, per-field aggregates, and examples.
- Production Ready: Logging, error handling, and a command-line interface.
- Modern Python Packaging: `pyproject.toml` with optional extras for dev and Starburst.

### Architecture
- Modular design with pluggable components for connections and comparisons.
- Type safety with dataclasses and type hints.
- Extensible to add new connection types and comparison strategies.
- Comprehensive test suite using pytest.

### 🎯 Enterprise Features (v1.1.0)
- Enhanced Validation with context-aware error messages
- Performance Monitoring and intelligent caching
- Constants Management for maintainability
- Robust Error Handling with actionable guidance
- Memory Optimization and performance logging
- Deterministic Sampling for reproducible examples
- Null-Safe Joins for correct completeness metrics
- New: Datetime comparator with format/timezone support and tolerance units

## 📦 Installation

### Prerequisites
- Python 3.8+ (tested up to 3.13)
- Java 8+ (required for PySpark)

### Quick Setup

1.  Clone and create a virtual environment:

    ```bash
    git clone <repository-url>
    cd recon_framework
    python -m venv .venv
    source .venv/bin/activate
    ```

2.  Install the package:

    ```bash
    # Core features (JSON support, all comparison logic)
    pip install -e .

    # Optional: with Starburst connector
    pip install -e .[starburst]

    # Optional: development tools (pytest, black, mypy, etc.)
    pip install -e .[dev]
    ```

3.  Verify with tests (optional):

    ```bash
    python run_tests.py
    ```

## 🏃 Quick Start

### From the Command Line

```bash
recon-framework --config config/json_recon_config.yaml --connection json --output results.json
```

### From a Python Script

```python
from reconciliation import ReconciliationFramework

framework = ReconciliationFramework(
    config_path='config/json_recon_config.yaml',
    connection_type='json'
)
report = framework.run_reconciliation()
framework.write_results(report, 'reconciliation_results.json')
print(f"Reconciliation complete. Status: {report.overall_status}")
```

## ⏱️ Datetime Comparison

The datetime comparison type normalizes both sides to UTC timestamps, then compares the absolute time difference against a tolerance in your chosen unit.

- Normalization:
  - Parses strings using Spark `to_timestamp` with optional `source_format`/`target_format`.
  - If a timezone is specified (e.g., `America/New_York`), values are interpreted as wall-clock times in that timezone and converted to UTC via `to_utc_timestamp`.
  - ISO-8601 values with offsets (e.g., `Z`, `+02:00`) are handled without needing timezones.
- Metric: Absolute time difference in seconds (reported as `metric_value`).
- Tolerance Units: `milliseconds`, `seconds` (default), `minutes`, `hours`.

Example YAML configuration:

```yaml
columns:
  mappings:
    - source_field: event_time_src
      target_field: event_time_tgt
      comparison_type: datetime
      tolerance: 2
      datetime_tolerance_unit: minutes
      # Optional parse formats
      source_format: "yyyy-MM-dd HH:mm:ss"
      target_format: "yyyy-MM-dd HH:mm:ss"
      # Optional timezones for naive values
      source_timezone: "America/New_York"
      target_timezone: "UTC"
```

Notes:
- If your strings already include timezone offsets, omit the timezone fields.
- If columns are already TimestampType, formats are ignored and values are compared directly.

## ⚙️ Configuration

- JSON mode: Use `path` and `multiline` in `source`/`target`.
- Starburst mode: Use `catalog`, `database`, `table`, and optional `filter_condition`.
- Key Fields: `key_fields` are required for field-level and completeness comparisons.
- Optional settings: `sample_seed`, `logging_level`, `logging_format`, `join_hints.broadcast` (source|target|none).

## Enums

- OverallStatus: PASSED | FAILED
- FieldStatus: Success | Error
- ComparisonType: EXACT | NUMERIC | DATETIME

## 📜 Report Structure

The output `results.json` contains:
- overall_status: PASSED or FAILED
- record_count_comparison: counts and match percentage
- completeness:
  - unmatched_source_count, unmatched_target_count
  - unmatched_source_examples, unmatched_target_examples
- field_comparisons: detailed results per compared field
- mismatch_counts: error counts per field
- field_aggregates: per-field totals, matches, errors, match_rate

## 🧪 Testing

```bash
python run_tests.py
pytest tests/ -v
```

## CI

A GitHub Actions workflow at `.github/workflows/ci.yml` runs tests on pushes and PRs.

## Changelog

### v1.1.0 (Latest)
- Added datetime comparison type with format/timezone options and tolerance units
- Centralized constants module
- Enhanced configuration validation with detailed error messages
- Performance monitoring and intelligent caching
- Improved error handling
- Expanded test suite
- Deterministic sampling and null-safe joins
- Per-field aggregates in the report

### v1.0.0 
- Initial production release
- Core reconciliation with PySpark
- JSON and Starburst connectivity
- Comprehensive reporting and comparison algorithms

## License

MIT License. See `LICENSE`.
