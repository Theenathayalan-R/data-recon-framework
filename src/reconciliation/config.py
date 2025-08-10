import yaml
from typing import Dict, List, Optional, Any
import os
from .models import ColumnMapping

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
        """Get record count match threshold."""
        return float(self.get_setting('record_count_threshold', 1.0))

    @property
    def key_fields(self) -> List[str]:
        """Get key fields for joining datasets."""
        return self.get_setting('key_fields', [])
