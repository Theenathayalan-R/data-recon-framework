import yaml
from typing import Dict, List, Optional, Any
import os
from .models import ColumnMapping
from .constants import (
    DEFAULT_RECORD_COUNT_THRESHOLD, 
    DEFAULT_TOP_K_EXAMPLES, 
    DEFAULT_SAMPLE_FRACTION,
    LOGGING_FORMAT,
)

class Config:
    """Configuration manager for reconciliation framework."""
    
    def __init__(self, config_path: str):
        """Initialize configuration from YAML file."""
        self.config_data = self._load_config(config_path)
        self._validate_config()

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Store config path for better error messages
        config['_config_path'] = config_path
        return config

    def _validate_config(self):
        """Validate required configuration sections."""
        required_sections = ['source', 'target', 'settings']
        for section in required_sections:
            if section not in self.config_data:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Ensure columns section exists with at least exclude or mappings
        if 'columns' not in self.config_data:
            self.config_data['columns'] = {'exclude': [], 'mappings': []}

    @property
    def source_config(self) -> Dict:
        """Get source dataset configuration."""
        return self.config_data['source']

    @property
    def target_config(self) -> Dict:
        """Get target dataset configuration."""
        return self.config_data['target']

    @property
    def excluded_columns(self) -> List[str]:
        """Get list of columns to exclude from comparison."""
        return self.config_data.get('columns', {}).get('exclude', [])

    @property
    def column_mappings(self) -> List[ColumnMapping]:
        """Get configured column mappings."""
        mappings = self.config_data.get('columns', {}).get('mappings', [])
        return [ColumnMapping(**m) for m in mappings]

    @property
    def settings(self) -> Dict:
        """Get reconciliation settings."""
        return self.config_data['settings']

    def get_setting(self, key: str, default: Optional[Any] = None) -> Any:
        """Get a specific setting value."""
        return self.settings.get(key, default)

    @property
    def record_count_threshold(self) -> float:
        """Get record count match threshold with validation."""
        val = float(self.get_setting('record_count_threshold', DEFAULT_RECORD_COUNT_THRESHOLD))
        if not 0 < val <= 1:
            raise ValueError(
                f"record_count_threshold must be between 0 and 1, got {val}. "
                f"Check your config file: {self.config_data.get('_config_path', 'unknown')}"
            )
        return val

    @property
    def key_fields(self) -> List[str]:
        """Get key fields for joining datasets with validation."""
        fields = self.get_setting('key_fields', [])
        if not fields:
            raise ValueError(
                "key_fields must not be empty for reconciliation. "
                f"Check your config file: {self.config_data.get('_config_path', 'unknown')}"
            )
        return fields

    @property
    def top_k_examples(self) -> int:
        """Max examples to include per field (errors first) with validation."""
        val = int(self.get_setting('top_k_examples', DEFAULT_TOP_K_EXAMPLES))
        if val <= 0:
            raise ValueError(f"top_k_examples must be positive, got {val}")
        return val

    @property
    def sample_fraction(self) -> float:
        """Fraction to sample before top-k selection with validation."""
        try:
            val = float(self.get_setting('sample_fraction', DEFAULT_SAMPLE_FRACTION))
            if not 0 < val <= 1:
                raise ValueError(f"sample_fraction must be between 0 and 1, got {val}")
            return val
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid sample_fraction value: {e}")

    @property
    def sample_seed(self) -> int:
        """Deterministic sampling seed for reproducibility."""
        try:
            return int(self.get_setting('sample_seed', 42))
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid sample_seed value: {e}")

    @property
    def logging_level(self) -> str:
        """Logging level (e.g., DEBUG, INFO, WARNING)."""
        val = str(self.get_setting('logging_level', 'INFO')).upper()
        # Basic validation
        if val not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError(f"Invalid logging_level: {val}")
        return val

    @property
    def logging_format(self) -> str:
        """Logging format string."""
        return str(self.get_setting('logging_format', LOGGING_FORMAT))

    @property
    def join_hints(self) -> Dict[str, Any]:
        """Optional join hints, e.g., {'broadcast': 'source'|'target'|'none'}"""
        hints = self.get_setting('join_hints', {}) or {}
        b = hints.get('broadcast', 'none')
        if b not in {'source', 'target', 'none'}:
            raise ValueError("join_hints.broadcast must be one of: source, target, none")
        return {'broadcast': b}
