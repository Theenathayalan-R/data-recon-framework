from typing import Optional, Dict, Any, Union
from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod
import os
import json

try:
    from pystarburst import StarburstConnection
    STARBURST_AVAILABLE = True
except ImportError:
    StarburstConnection = Any  # type: ignore
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
        """Load dataset from JSON file.
        
        Args:
            path: Path to the JSON file
            multiline: Whether the JSON file contains one record per line (False) 
                      or the entire file is a JSON array (True)
        """
        return self.spark.read.option("multiline", str(multiline).lower()) \
                   .json(path)

    def write_dataset(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """Write dataset to JSON file."""
        df.write.mode(mode).json(path)

class StarburstConnectionManager(ConnectionManager):
    """Manages connections to Starburst."""

    def __init__(self, spark: SparkSession, config_path: Optional[str] = None):
        super().__init__(spark)
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'config',
            'starburst_config.json'
        )
        self.connection_config = self._load_config()
        self.connection = None if not STARBURST_AVAILABLE else None

    def _load_config(self) -> Dict[str, Any]:
        """Load Starburst connection configuration from JSON file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                
            # Validate required fields
            required_fields = ['host', 'port', 'user', 'password']
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                raise ValueError(f"Missing required fields in config: {missing_fields}")
                
            # Ensure port is integer
            config['port'] = int(config['port'])
            
            return config
        except FileNotFoundError:
            # Create default config if file doesn't exist
            default_config = {
                'host': os.getenv('STARBURST_HOST', 'localhost'),
                'port': int(os.getenv('STARBURST_PORT', '8080')),
                'user': os.getenv('STARBURST_USER', 'test'),
                'password': os.getenv('STARBURST_PASSWORD', 'test')
            }
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(default_config, f, indent=4)
            return default_config

    def connect(self):
        """Establish connection to Starburst."""
        if not STARBURST_AVAILABLE:
            raise NotImplementedError("Starburst connection not available.")
        
        if self.connection is None:
            self.connection = StarburstConnection(**self.connection_config)

    def load_dataset(self, catalog: str, database: str, table: str, 
                    filter_condition: Optional[str] = None) -> DataFrame:
        """Load dataset from Starburst catalog."""
        if not STARBURST_AVAILABLE:
            raise NotImplementedError("Starburst connection not available.")
        
        query = f"SELECT * FROM {catalog}.{database}.{table}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        return self.spark.read.format("starburst").option("query", query).load()

    def write_dataset(self, df: DataFrame, table: str, mode: str = "append"):
        """Write dataset to Starburst."""
        if not STARBURST_AVAILABLE:
            raise NotImplementedError("Starburst connection not available.")
        
        df.write.format("starburst") \
            .option("table", table) \
            .mode(mode) \
            .save()
