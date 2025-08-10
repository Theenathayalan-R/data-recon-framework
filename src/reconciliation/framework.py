from pyspark.sql import SparkSession
from datetime import datetime, timezone
from typing import Dict, Optional
import logging
import os

from .config import Config
from .models import ReconciliationReport, ColumnMapping
from .comparators import RecordCountComparator, FieldComparator, ColumnMatcher
from .connections import StarburstConnectionManager, JSONConnectionManager, ConnectionManager

class ReconciliationFramework:
    """Main reconciliation framework class."""

    def __init__(self, config_path: str, connection_type: str = "starburst"):
        """Initialize the reconciliation framework.
        
        Args:
            config_path: Path to the YAML config file
            connection_type: Type of connection manager to use ('starburst' or 'json')
        """
        self.config_path = config_path
        self.config = Config(config_path)
        self.spark = self._initialize_spark()
        self.connection_manager = self._create_connection_manager(connection_type)
        self.logger = self._setup_logging()
        
        # Initialize comparators
        self.record_comparator = RecordCountComparator(
            threshold=self.config.record_count_threshold
        )
        self.field_comparator = FieldComparator(
            key_fields=self.config.key_fields
        )
        self.column_matcher = ColumnMatcher(
            excluded_columns=self.config.excluded_columns
        )

    def _create_connection_manager(self, connection_type: str) -> ConnectionManager:
        """Create appropriate connection manager based on type."""
        if connection_type == "starburst":
            # Look for starburst_config.json in the config directory
            config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config')
            starburst_config = os.path.join(config_dir, 'starburst_config.json')
            return StarburstConnectionManager(self.spark, config_path=starburst_config)
        elif connection_type == "json":
            return JSONConnectionManager(self.spark)
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")
        
        # Initialize comparators
        self.record_comparator = RecordCountComparator(
            threshold=self.config.record_count_threshold
        )
        self.field_comparator = FieldComparator(
            key_fields=self.config.key_fields
        )
        self.column_matcher = ColumnMatcher(
            excluded_columns=self.config.excluded_columns
        )

    def _initialize_spark(self) -> SparkSession:
        """Initialize Spark session with necessary configurations."""
        return (SparkSession.builder
                .appName("Data Reconciliation Framework")
                .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3")
                .getOrCreate())

    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)

    def run_reconciliation(self) -> ReconciliationReport:
        """Run the complete reconciliation process."""
        if self.connection_manager is None:
            raise ValueError("Starburst connection manager not initialized")
            
        # Load datasets
        source_df = self.connection_manager.load_dataset(**self.config.source_config)
        target_df = self.connection_manager.load_dataset(**self.config.target_config)

        # Compare record counts
        count_comparison = self.record_comparator.compare(source_df, target_df)
        self.logger.info(f"Record count comparison: {count_comparison}")

        # Get columns to compare
        mappings = self.column_matcher.get_matching_columns(
            source_df, target_df, self.config.column_mappings
        )
        
        # Compare field values
        field_comparisons = {}
        for mapping in mappings:
            comparison_results = self.field_comparator.compare(source_df, target_df, mapping)
            key = f"{mapping.source_field} vs {mapping.target_field}"
            field_comparisons[key] = comparison_results
            self.logger.info(f"Field comparison {key}: {comparison_results}")

        # Generate final report
        report = ReconciliationReport(
            record_count_comparison=count_comparison,
            field_comparisons=field_comparisons,
            overall_status=self._determine_overall_status(count_comparison, field_comparisons),
            timestamp=datetime.now(timezone.utc).isoformat(),
            source_details=self.config.source_config,
            target_details=self.config.target_config
        )

        return report

    def _determine_overall_status(self, count_comparison, field_comparisons) -> str:
        """Determine overall reconciliation status."""
        if not count_comparison.threshold_met:
            return 'FAILED'

        for comparison_results in field_comparisons.values():
            if any(result.status == 'Error' for result in comparison_results):
                return 'FAILED'

        return 'PASSED'

    def write_results(self, report: ReconciliationReport, output_path: str):
        """Write reconciliation results to output file."""
        import json
        
        # Convert report to dictionary for JSON serialization
        result_dict = {
            'timestamp': report.timestamp,
            'overall_status': report.overall_status,
            'source_details': report.source_details,
            'target_details': report.target_details,
            'record_count_comparison': {
                'source_count': report.record_count_comparison.source_count,
                'target_count': report.record_count_comparison.target_count,
                'match_percentage': report.record_count_comparison.match_percentage,
                'threshold_met': report.record_count_comparison.threshold_met
            },
            'field_comparisons': {
                k: [
                    {
                        'source_field': r.source_field,
                        'target_field': r.target_field,
                        'status': r.status,
                        'message': r.message,
                        'metric_value': r.metric_value,
                        'comparison_type': r.comparison_type,
                        'tolerance': r.tolerance
                    } for r in v
                ] for k, v in report.field_comparisons.items()
            }
        }
        
        # Write to JSON file
        with open(output_path, 'w') as f:
            json.dump(result_dict, f, indent=2)
