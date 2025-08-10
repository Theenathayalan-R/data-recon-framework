from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod
import os
import json
from .constants import CONNECTION_TYPE_JSON, CONNECTION_TYPE_STARBURST

try:
    import pystarburst  # type: ignore
    STARBURST_AVAILABLE = True
except Exception:
    pystarburst = None  # type: ignore
    STARBURST_AVAILABLE = False

class ConnectionManager(ABC):
    """Abstract base class for all connection managers."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def load_dataset(self, *args, **kwargs) -> DataFrame:
        """Load a dataset."""
        pass

    @abstractmethod
    def write_dataset(self, df: DataFrame, *args, **kwargs):
        """Write a dataset."""
        pass

class JSONConnectionManager(ConnectionManager):
    """Manages connections to JSON files for local testing."""

    def load_dataset(self, path: str, multiline: bool = True, **kwargs) -> DataFrame:
        """Load dataset from JSON file with enhanced error handling.
        
        Args:
            path: Path to the JSON file
            multiline: Whether the JSON file contains one record per line (False) 
                      or the entire file is a JSON array (True)
                      
        Raises:
            FileNotFoundError: If the JSON file doesn't exist
            Exception: If JSON parsing fails
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"JSON file not found: {path}")
            
        try:
            return self.spark.read.option("multiline", str(multiline).lower()).json(path)
        except Exception as e:
            raise Exception(f"Failed to load JSON file {path}: {str(e)}")

    def write_dataset(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """Write dataset to JSON file."""
        df.write.mode(mode).json(path)

class StarburstConnectionManager(ConnectionManager):
    """Manages connections to Starburst."""

    def __init__(self, spark: SparkSession, config_path: Optional[str] = None):
        super().__init__(spark)
        # Default to top-level config/starburst_config.json
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'config',
            'starburst_config.json'
        )
        self.connection_config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load Starburst connection configuration from JSON file, fallback to env.
        Avoid writing credentials to disk by default.
        """
        config: Dict[str, Any] = {}
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
        except Exception:
            config = {}

        # Env fallbacks
        config.setdefault('host', os.getenv('STARBURST_HOST'))
        port_str = os.getenv('STARBURST_PORT')
        if 'port' not in config or config.get('port') is None:
            config['port'] = int(port_str) if port_str else None
        config.setdefault('user', os.getenv('STARBURST_USER'))
        config.setdefault('password', os.getenv('STARBURST_PASSWORD'))

        # Validate required fields
        required_fields = ['host', 'port', 'user', 'password']
        missing = [k for k in required_fields if not config.get(k)]
        if missing:
            raise ValueError(
                f"Missing Starburst config fields: {missing}. "
                f"Supply via {self.config_path} or environment variables "
                f"(STARBURST_HOST, STARBURST_PORT, STARBURST_USER, STARBURST_PASSWORD)."
            )

        return config

    def load_dataset(self, catalog: str, database: str, table: str, 
                    filter_condition: Optional[str] = None) -> DataFrame:
        """Load dataset from Starburst catalog."""
        if not STARBURST_AVAILABLE:
            raise NotImplementedError("Starburst connection not available. Install with extras: pip install recon_framework[starburst]")
        
        query = f"SELECT * FROM {catalog}.{database}.{table}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        return self.spark.read.format("starburst").option("query", query).load()

    def write_dataset(self, df: DataFrame, table: str, mode: str = "append"):
        """Write dataset to Starburst."""
        if not STARBURST_AVAILABLE:
            raise NotImplementedError("Starburst connection not available. Install with extras: pip install recon_framework[starburst]")
        
        df.write.format("starburst").option("table", table).mode(mode).save()
