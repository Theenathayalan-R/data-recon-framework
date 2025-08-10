# Data Reconciliation Framework

A robust, production-ready data reconciliation framework built with PySpark for comparing datasets across different sources with configurable comparison strategies, tolerance levels, and comprehensive reporting. Features modern Python packaging, comprehensive testing, and enterprise-grade reliability.

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/pyspark-4.0+-orange.svg)](https://spark.apache.org/)
[![Tests](https://img.shields.io/badge/tests-16%2F16%20passing-green.svg)](./tests/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Version](https://img.shields.io/badge/version-1.0.0-brightgreen.svg)](./pyproject.toml)

## 🚀 Features

### Core Capabilities
- **Multi-Source Support**: Connect to Starburst databases or local JSON files
- **Flexible Comparisons**: Record count and field-by-field value comparisons  
- **Configurable Tolerances**: Set numeric tolerances and comparison types (exact, numeric)
- **Smart Column Mapping**: Map fields between source and target with different names
- **Comprehensive Reporting**: Detailed reports with overall status and field-level results
- **Production Ready**: Logging, error handling, and JSON-based result outputs
- **Modern Python Packaging**: Uses pyproject.toml for dependency management
- **Git Version Control**: Clean repository with comprehensive .gitignore
- **VS Code Integration**: Workspace configuration and protective file exclusions

### Architecture
- **Modular Design**: Clean separation of concerns with pluggable components
- **Type Safety**: Full type hints and validation throughout codebase
- **Extensible**: Easy to add new connection types and comparison strategies
- **Test Coverage**: Comprehensive unit and integration tests (16 tests)
- **Enterprise Ready**: Production-grade logging, monitoring, and error handling

## 📦 Installation

### Prerequisites
- Python 3.13+ (compatible with 3.8+)
- Java 8+ (for PySpark)
- Git (for version control)

### Quick Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd recon_framework
```

2. **Create and activate virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install the package**
```bash
# Install in development mode (recommended)
pip install -e .

# All dependencies are managed in pyproject.toml
# No separate requirements.txt needed - modern Python packaging!
```

4. **Verify installation**
```bash
# Run comprehensive test suite
python run_tests.py

# Or run individual test components
pytest tests/ -v

# Quick functionality test
python -c "
from reconciliation import ReconciliationFramework
framework = ReconciliationFramework('config/json_recon_config.yaml', 'json')
print('✅ Framework ready for use!')
"
```

5. **Run your first reconciliation**
```bash
# Execute sample reconciliation
python -c "
from reconciliation import ReconciliationFramework
framework = ReconciliationFramework('config/json_recon_config.yaml', 'json')
results = framework.run_reconciliation()
print(f'Status: {results.overall_status}')
print(f'Records: {results.record_count_comparison.source_count} vs {results.record_count_comparison.target_count}')
"
```

## 🏃 Quick Start

### JSON File Reconciliation (Local Testing)

```python
from reconciliation import ReconciliationFramework

# Initialize framework with JSON configuration
framework = ReconciliationFramework(
    config_path='config/json_recon_config.yaml', 
    connection_type='json'
)

# Run reconciliation
report = framework.run_reconciliation()

# Check results
print(f"Overall Status: {report.overall_status}")
print(f"Records: {report.record_count_comparison.source_count} vs {report.record_count_comparison.target_count}")
print(f"Match Rate: {report.record_count_comparison.match_percentage * 100:.1f}%")

# Save results
framework.write_results(report, 'reconciliation_results.json')
```

### Starburst Database Reconciliation

```python
from reconciliation import ReconciliationFramework

# Initialize framework with Starburst configuration
framework = ReconciliationFramework(
    config_path='config/prod_recon_config.yaml', 
    connection_type='starburst'
)

# Run reconciliation
report = framework.run_reconciliation()

# Process results
if report.overall_status == 'PASSED':
    print("✅ Reconciliation passed!")
else:
    print("❌ Reconciliation failed - check field comparisons")
    for field, results in report.field_comparisons.items():
        failed_results = [r for r in results if r.status == 'Error']
        if failed_results:
            print(f"  {field}: {len(failed_results)} mismatches")
```

## ⚙️ Configuration

### Main Configuration File (YAML)

Create your reconciliation configuration in YAML format:

```yaml
# Source Dataset Configuration
source:
  # For JSON files
  path: "data/source.json"
  multiline: true
  
  # For Starburst databases
  # catalog_name: "my_catalog"
  # database_name: "my_database"  
  # table_name: "source_table"
  # filter_condition: "date >= '2025-01-01'"  # Optional

# Target Dataset Configuration
target:
  # For JSON files
  path: "data/target.json"
  multiline: true
  
  # For Starburst databases
  # catalog_name: "my_catalog"
  # database_name: "my_database"
  # table_name: "target_table"
  # filter_condition: "date >= '2025-01-01'"  # Optional

# Column Configuration
columns:
  # Columns to exclude from comparison
  exclude:
    - updated_timestamp
    - etl_batch_id
    - created_by
  
  # Explicit field mappings with comparison strategies
  mappings:
    - source_field: id
      target_field: id
      comparison_type: exact
      tolerance: 0
    - source_field: amount
      target_field: value
      comparison_type: numeric
      tolerance: 0.01  # Allow 1 cent difference
    - source_field: customer_name
      target_field: customer_name
      comparison_type: exact
      tolerance: 0
    - source_field: balance
      target_field: account_balance
      comparison_type: numeric
      tolerance: 0.001  # Allow 0.1% difference

# Reconciliation Settings
settings:
  record_count_threshold: 0.99    # Require 99% record count match
  key_fields:                     # Fields used for joining datasets
    - id
    - customer_id
```

### Starburst Connection Configuration

Create `config/starburst_config.json` for database connections:

```json
{
    "host": "your-starburst-cluster.com",
    "port": 8080,
    "user": "your-username",
    "password": "your-password",
    "catalog": "default",
    "schema": "public",
    "ssl_verify": false,
    "timeout": 300
}
```

### Environment Variables

For security, use environment variables for sensitive data:

```bash
export STARBURST_HOST=your-cluster.com
export STARBURST_PORT=8080
export STARBURST_USER=your-username
export STARBURST_PASSWORD=your-password
```

## 📁 Project Structure

```
recon_framework/
├── README.md                      # This comprehensive documentation  
├── LICENSE                        # MIT License
├── pyproject.toml                 # Modern Python project configuration (primary)
├── setup.py                       # Legacy compatibility (minimal)
├── run_tests.py                   # Comprehensive test runner script
├── .gitignore                     # Comprehensive Git ignore patterns
├── .vscode/                       # VS Code workspace configuration
│   └── settings.json             # Python environment and exclusions
│
├── config/                        # Configuration files and templates
│   ├── json_recon_config.yaml    # JSON file reconciliation config
│   ├── recon_config_template.yaml # Template for new configurations
│   └── starburst_config.json     # Database connection settings template
│
├── data/                          # Sample data for testing and examples
│   ├── source.json               # Sample source dataset (3 records)
│   └── target.json               # Sample target dataset (3 records)
│
├── src/reconciliation/           # Main source code package
│   ├── __init__.py              # Package initialization and exports
│   ├── framework.py             # ReconciliationFramework main class
│   ├── models.py                # Data models and type definitions
│   ├── config.py                # Configuration loading and validation
│   ├── connections.py           # Connection managers (JSON, Starburst)
│   └── comparators.py           # Comparison algorithms and logic
│
└── tests/                       # Comprehensive test suite (16 tests)
    ├── test_framework.py        # End-to-end framework tests (3 tests)
    ├── test_models.py           # Data model validation tests (3 tests)
    ├── test_config.py           # Configuration parsing tests (2 tests)
    ├── test_connections.py      # Connection manager tests (2 tests)
    ├── test_comparators.py      # Comparison algorithm tests (3 tests)
    ├── test_reconciliation.py   # Integration workflow tests (3 tests)
    └── data/                    # Test-specific data files
        ├── test_source.json     # Test source dataset
        └── test_target.json     # Test target dataset
```

## 🔧 Components Deep Dive

### 1. ReconciliationFramework (`framework.py`)
Main orchestrator class that coordinates all components:

```python
class ReconciliationFramework:
    def __init__(self, config_path: str, connection_type: str = "starburst")
    def run_reconciliation(self) -> ReconciliationReport
    def write_results(self, report: ReconciliationReport, output_path: str)
```

### 2. Data Models (`models.py`)
Structured data representations:

- **`ComparisonResult`**: Individual field comparison outcome
- **`RecordCountResult`**: Record count comparison results  
- **`ReconciliationReport`**: Complete reconciliation report
- **`ColumnMapping`**: Field mapping configuration

### 3. Connection Managers (`connections.py`)
Handle data source connectivity:

- **`JSONConnectionManager`**: Local JSON file processing
- **`StarburstConnectionManager`**: Starburst database connections
- **`ConnectionManager`**: Abstract base class for extensibility

### 4. Comparators (`comparators.py`)
Core comparison algorithms:

- **`RecordCountComparator`**: Validates record count matches
- **`FieldComparator`**: Performs field-by-field value comparisons
- **`ColumnMatcher`**: Intelligent column mapping between datasets

### 5. Configuration Manager (`config.py`)
Loads and validates YAML configurations with error handling.

## 🔍 Comparison Types

### Exact Comparison
Perfect string/value matching with no tolerance:

```yaml
- source_field: customer_id
  target_field: customer_id
  comparison_type: exact
  tolerance: 0
```

### Numeric Comparison
Numerical comparison with configurable tolerance:

```yaml
- source_field: amount
  target_field: value
  comparison_type: numeric
  tolerance: 0.01  # Allow 1 cent difference
```

## 📊 Sample Data and Results

### Input Data Examples

**Source Data** (`data/source.json`):
```json
[
    {
        "id": 1,
        "customer_name": "John Doe",
        "amount": 1000.00,
        "transaction_date": "2025-08-01",
        "status": "completed",
        "updated_timestamp": "2025-08-01T10:00:00Z",
        "etl_batch_id": "batch_001"
    },
    {
        "id": 2,
        "customer_name": "Jane Smith", 
        "amount": 1500.50,
        "transaction_date": "2025-08-01",
        "status": "completed",
        "updated_timestamp": "2025-08-01T10:05:00Z",
        "etl_batch_id": "batch_001"
    }
]
```

**Target Data** (`data/target.json`):
```json
[
    {
        "id": 1,
        "customer_name": "John Doe",
        "value": 1000.00,  # Note: amount -> value mapping
        "transaction_date": "2025-08-01",
        "status": "completed",
        "updated_timestamp": "2025-08-01T10:30:00Z",  # Different timestamp (excluded)
        "etl_batch_id": "batch_002"  # Different batch (excluded)
    },
    {
        "id": 2,
        "customer_name": "Jane Smith",
        "value": 1500.49,  # Small difference within tolerance
        "transaction_date": "2025-08-01", 
        "status": "pending",  # Status mismatch!
        "updated_timestamp": "2025-08-01T10:35:00Z",
        "etl_batch_id": "batch_002"
    }
]
```

### Console Output Example
```
🎯 Final End-to-End Reconciliation Test
==================================================
✅ Reconciliation completed successfully!
📊 Overall Status: FAILED
📈 Record Count Match: 100.00%
📋 Source Records: 3
📋 Target Records: 3
🔍 Total Field Comparisons: 15
🔍 Field Mappings: 5 different field pairs
✅ Successful field comparisons: 14
❌ Failed field comparisons: 1

Failed comparisons details:
  • status → status: Values differ

🎉 End-to-End test completed successfully!
🚀 Framework is fully operational and ready for production!
```

### JSON Results Output
```json
{
  "timestamp": "2025-08-10T14:08:31.377Z",
  "overall_status": "FAILED",
  "source_details": {
    "path": "data/source.json",
    "multiline": true
  },
  "target_details": {
    "path": "data/target.json", 
    "multiline": true
  },
  "record_count_comparison": {
    "source_count": 2,
    "target_count": 2,
    "match_percentage": 1.0,
    "threshold_met": true
  },
  "field_comparisons": {
    "status vs status": [
      {
        "source_field": "status",
        "target_field": "status",
        "status": "Success", 
        "message": "Exact match",
        "metric_value": 0,
        "comparison_type": "exact",
        "tolerance": 0
      },
      {
        "source_field": "status",
        "target_field": "status",
        "status": "Error",
        "message": "Values differ", 
        "metric_value": 1,
        "comparison_type": "exact",
        "tolerance": 0
      }
    ]
  }
}
```

## 🧪 Testing

### Run All Tests
```bash
# Run the comprehensive test suite
python run_tests.py

# Or use pytest directly
pytest tests/ -v

# Run with coverage
pytest --cov=reconciliation tests/
```

### Test Categories

1. **Unit Tests**: Individual component testing
   - Model validation
   - Configuration parsing
   - Connection management
   - Comparison algorithms

2. **Integration Tests**: End-to-end workflows
   - Complete reconciliation process
   - JSON file processing
   - Result generation and output

3. **Configuration Tests**: YAML parsing and validation
   - Valid configuration loading
   - Error handling for invalid configs
   - Environment variable support

### Sample Test Output
```
🚀 Data Reconciliation Framework - Test Suite
============================================================

============================================================
Running: Unit Tests (pytest)
Command: PYTHONPATH=/path/to/src python -m pytest tests/ -v --tb=short
============================================================
✅ Unit Tests (pytest) - PASSED
============================= test session starts ==============================
platform darwin -- Python 3.13.5, pytest-8.4.1, pluggy-1.6.0
cachedir: .pytest_cache
rootdir: /path/to/recon_framework
configfile: pyproject.toml
plugins: spark-0.8.0
collecting ... collected 16 items

tests/test_comparators.py::test_record_count_comparator PASSED     [  6%]
tests/test_comparators.py::test_field_comparator PASSED           [ 12%]
tests/test_comparators.py::test_column_matcher PASSED             [ 18%]
tests/test_config.py::test_config_loading PASSED                  [ 25%]
tests/test_config.py::test_config_validation PASSED               [ 31%]
tests/test_connections.py::test_json_load_dataset PASSED          [ 37%]
tests/test_connections.py::test_json_write_dataset PASSED         [ 43%]
tests/test_framework.py::test_framework_initialization PASSED     [ 50%]
tests/test_framework.py::test_framework_reconciliation PASSED     [ 56%]
tests/test_framework.py::test_framework_results_writing PASSED    [ 62%]
tests/test_models.py::test_comparison_result PASSED               [ 68%]
tests/test_models.py::test_record_count_result PASSED             [ 75%]
tests/test_models.py::test_reconciliation_report PASSED           [ 81%]
tests/test_reconciliation.py::test_record_count_comparison PASSED [ 87%]
tests/test_reconciliation.py::test_field_value_comparison PASSED  [ 93%]
tests/test_reconciliation.py::test_end_to_end_reconciliation PASSED [100%]

============================== 16 passed in 7.44s ==============================

============================================================
Running: Local JSON Reconciliation Test
============================================================
✅ Local JSON Reconciliation Test - PASSED  
Status: FAILED (expected due to intentional test data mismatch)
Records: 3 vs 3

============================================================
Running: Configuration Validation Test
============================================================
✅ Configuration Validation Test - PASSED
Config loaded with 5 mappings
Key fields: ['id']
Threshold: 0.99

📊 TEST SUMMARY
============================================================
Total Tests: 3
Passed: 3
Failed: 0

🎉 ALL TESTS PASSED! Framework is ready for use.
```

## 🔧 Advanced Usage

### Custom Comparison Logic

Extend the framework with custom comparators:

```python
from reconciliation.comparators import FieldComparator
from reconciliation.models import ComparisonResult

class CustomDateComparator(FieldComparator):
    def compare_dates(self, source_df, target_df, mapping):
        # Custom date comparison logic
        # Handle different date formats, timezones, etc.
        pass
```

### Multiple Environment Configuration

Create environment-specific configs:

```
config/
├── dev_recon_config.yaml
├── staging_recon_config.yaml  
├── prod_recon_config.yaml
└── starburst_configs/
    ├── dev_starburst.json
    ├── staging_starburst.json
    └── prod_starburst.json
```

### Batch Processing

Process multiple reconciliations:

```python
import os
from reconciliation import ReconciliationFramework

configs = [
    'config/daily_reconciliation.yaml',
    'config/monthly_reconciliation.yaml', 
    'config/year_end_reconciliation.yaml'
]

for config_path in configs:
    framework = ReconciliationFramework(config_path)
    report = framework.run_reconciliation()
    
    output_name = os.path.basename(config_path).replace('.yaml', '_results.json')
    framework.write_results(report, f'results/{output_name}')
    
    print(f"{config_path}: {report.overall_status}")
```

## 📋 Best Practices

### Configuration Management
1. **Version Control**: Store configs in version control
2. **Environment Separation**: Use separate configs per environment
3. **Sensitive Data**: Use environment variables for passwords
4. **Documentation**: Comment complex mapping logic

### Performance Optimization
1. **Filter Early**: Use `filter_condition` to reduce data volume
2. **Key Selection**: Choose efficient join keys
3. **Batch Size**: Configure appropriate Spark settings
4. **Resource Allocation**: Adjust Spark executor settings

### Error Handling
1. **Validation**: Validate configs before execution
2. **Logging**: Monitor logs for debugging
3. **Alerts**: Set up alerts for failed reconciliations
4. **Retry Logic**: Implement retry for transient failures

### Testing Strategy
1. **Sample Data**: Use representative test datasets
2. **Edge Cases**: Test boundary conditions
3. **Performance**: Validate with large datasets
4. **Regression**: Maintain test suite for changes

## 🚀 Production Deployment

### Environment Setup
```bash
# Production environment
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
export PYTHONPATH=/opt/recon_framework/src

# Starburst credentials
export STARBURST_HOST=prod-cluster.company.com
export STARBURST_USER=recon_service_account
export STARBURST_PASSWORD=$(cat /etc/secrets/starburst_password)
```

### Scheduling with Cron
```bash
# Daily reconciliation at 2 AM
0 2 * * * /opt/recon_framework/run_daily_recon.sh

# Weekly summary on Sundays
0 6 * * 0 /opt/recon_framework/run_weekly_summary.sh
```

### Monitoring and Alerting
```python
# Integration with monitoring systems
def run_with_monitoring():
    try:
        framework = ReconciliationFramework('config/prod.yaml')
        report = framework.run_reconciliation()
        
        if report.overall_status == 'FAILED':
            send_alert(f"Reconciliation failed: {report}")
        
        # Log metrics
        log_metrics({
            'status': report.overall_status,
            'source_count': report.record_count_comparison.source_count,
            'target_count': report.record_count_comparison.target_count,
            'field_failures': sum(1 for results in report.field_comparisons.values() 
                                for r in results if r.status == 'Error')
        })
        
    except Exception as e:
        send_alert(f"Reconciliation error: {e}")
        raise
```

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b feature-name`
3. **Make** changes and add comprehensive tests
4. **Run** the test suite: `python run_tests.py`
5. **Ensure** all tests pass and coverage is maintained
6. **Submit** a pull request with detailed description

### Development Setup
```bash
# Development environment
git clone <repository-url>
cd recon_framework
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install  # If using pre-commit hooks
```

## 📋 Dependencies

### Core Dependencies
- **PySpark 4.0+**: Distributed data processing engine with PyDeequ integration
- **PyYAML 6.0.2+**: YAML configuration file parsing and validation
- **pandas 2.0+**: Data manipulation and analysis support
- **python-dotenv 1.0+**: Environment variable management

### Optional Dependencies
- **pystarburst 0.10+**: Starburst database connectivity and SQL execution
- **pydeequ 1.5+**: Data quality validation and profiling capabilities

### Development Dependencies
- **pytest 8.0+**: Comprehensive testing framework
- **pytest-spark 0.8+**: PySpark testing utilities and fixtures
- **pytest-cov 4.0+**: Test coverage reporting and analysis

## 🆕 Recent Improvements (v1.0.0)

### Modern Python Packaging
- ✅ **pyproject.toml**: Centralized project configuration
- ✅ **Dependency Management**: Single source of truth for all dependencies
- ✅ **Version Management**: Semantic versioning with automated package building

### Enhanced Development Experience  
- ✅ **VS Code Integration**: Workspace configuration with Python environment setup
- ✅ **Git Version Control**: Clean repository with comprehensive .gitignore patterns
- ✅ **File Protection**: Automatic exclusion of temporary and cache files

### Production Readiness
- ✅ **Comprehensive Testing**: 16 tests covering all components with 100% pass rate
- ✅ **Error Handling**: Robust error handling with detailed logging and reporting
- ✅ **Documentation**: Extensive README with examples and best practices
- ✅ **Monitoring**: Built-in logging and metrics for production monitoring

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support and Documentation

### Getting Help
- **Documentation**: This README and inline code documentation
- **Examples**: Check the `data/` and `config/` directories for samples
- **Tests**: Review `tests/` directory for usage patterns
- **Issues**: Report bugs and feature requests via GitHub issues

### Performance Guidelines
- **Small Datasets** (< 1M records): Local mode works well
- **Medium Datasets** (1M - 100M records): Use cluster mode with 4-8 executors
- **Large Datasets** (> 100M records): Optimize Spark settings and consider partitioning

### Troubleshooting
- **Memory Issues**: Increase Spark driver/executor memory
- **Connection Timeouts**: Check network connectivity and firewall settings
- **Schema Mismatches**: Verify column names and data types
- **Performance Issues**: Review join keys and data distribution

---

**Built with ❤️ for reliable data reconciliation workflows**

**Version 1.0.0** - Production Ready Framework with Modern Python Packaging
