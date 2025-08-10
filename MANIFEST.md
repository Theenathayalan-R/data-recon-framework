## Data Reconciliation Framework - File Manifest

### Core Source Code (src/reconciliation/)
- `__init__.py` - Package initialization with exports
- `framework.py` - Main ReconciliationFramework orchestrator class
- `models.py` - Data models (ComparisonResult, ReconciliationReport, etc.)
- `config.py` - YAML configuration loading and validation with enhanced error handling
- `connections.py` - Connection managers (JSON, Starburst) with improved error messages
- `comparators.py` - Comparison algorithms (RecordCount, Field, Column) with performance monitoring
- `constants.py` - Centralized configuration constants and defaults

### Configuration Files (config/)
- `starburst_config.json` - Database connection configuration
- `json_recon_config.yaml` - JSON file reconciliation configuration
- `recon_config_template.yaml` - Template for new configurations

### Sample Data (data/)
- `source.json` - Sample source dataset for testing
- `target.json` - Sample target dataset for testing

### Test Suite (tests/)
- `test_framework.py` - Framework integration tests (3 tests)
- `test_models.py` - Data model tests (3 tests)
- `test_config.py` - Configuration loading tests (2 tests)
- `test_connections.py` - Connection manager tests (2 tests)
- `test_comparators.py` - Comparison algorithm tests (5 tests)
- `test_reconciliation.py` - End-to-end reconciliation tests (3 tests)
- `test_validation_and_error_handling.py` - Enhanced validation and error handling tests (6 tests)
- `data/` - Test data files
  - `test_source.json` - Test source data
  - `test_target.json` - Test target data

### Build & Configuration
- `setup.py` - Legacy compatibility (minimal)
- `pyproject.toml` - Modern Python project configuration (primary)
- `run_tests.py` - Test runner script
- `.gitignore` - Git ignore patterns

### Documentation
- `README.md` - Comprehensive project documentation
- `LICENSE` - MIT License
- `MANIFEST.md` - This file manifest

### Dependency Management
**Modern Approach**: All dependencies are managed in `pyproject.toml`
- Core dependencies in `[project.dependencies]`
- Development dependencies in `[project.optional-dependencies.dev]`
- No separate `requirements.txt` file needed

### Total Test Count: 24 tests across 7 test files
### Framework Status: Production Ready ✅ (v1.1.0 with Enterprise Features)
