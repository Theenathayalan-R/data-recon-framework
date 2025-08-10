from pyspark.sql import SparkSession
from datetime import datetime, timezone
from typing import Dict, Optional
import logging
import os
import time
from functools import reduce

from .config import Config
from .models import ReconciliationReport, ColumnMapping
from .comparators import RecordCountComparator, FieldComparator, ColumnMatcher
from .connections import StarburstConnectionManager, JSONConnectionManager, ConnectionManager
from .constants import (
    STATUS_PASSED, STATUS_FAILED, LOGGING_FORMAT, 
    LARGE_DATASET_THRESHOLD, PERFORMANCE_LOG_THRESHOLD
)
from pyspark.sql import functions as F

class ReconciliationFramework:
    """Main reconciliation framework class."""

    def __init__(self, config_path: str, connection_type: str = "starburst"):
        # Initialize config first to use settings for logging
        self.config_path = config_path
        self.connection_type = connection_type
        self.config = Config(config_path)
        self.logger = self._setup_logging()
        self.spark = self._initialize_spark(connection_type)
        self.connection_manager = self._create_connection_manager(connection_type)
        
        # Initialize comparators with config-driven sampling seed
        self.record_comparator = RecordCountComparator(
            threshold=self.config.record_count_threshold
        )
        self.field_comparator = FieldComparator(
            key_fields=self.config.key_fields,
            top_k=self.config.top_k_examples,
            sample_fraction=self.config.sample_fraction,
            sample_seed=self.config.sample_seed,
        )
        self.column_matcher = ColumnMatcher(
            excluded_columns=self.config.excluded_columns
        )

    def _create_connection_manager(self, connection_type: str) -> ConnectionManager:
        """Create appropriate connection manager based on type."""
        # Resolve top-level config directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_dir = os.path.join(os.path.dirname(project_root), 'config')
        starburst_config = os.path.join(config_dir, 'starburst_config.json')

        if connection_type == "starburst":
            return StarburstConnectionManager(self.spark, config_path=starburst_config)
        elif connection_type == "json":
            return JSONConnectionManager(self.spark)
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")

    def _initialize_spark(self, connection_type: str) -> SparkSession:
        """Initialize Spark session with necessary configurations."""
        builder = SparkSession.builder.appName("Data Reconciliation Framework")
        # Default to local master for JSON/local mode to improve DX
        if connection_type == 'json':
            builder = builder.master("local[*]")
        return builder.getOrCreate()

    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration with enhanced formatting."""
        logging.basicConfig(
            level=getattr(logging, self.config.logging_level, logging.INFO),
            format=self.config.logging_format
        )
        return logging.getLogger(__name__)

    def run_reconciliation(self) -> ReconciliationReport:
        """Run the complete reconciliation process with performance monitoring."""
        start_time = time.time()
        
        if self.connection_manager is None:
            raise ValueError("Connection manager not initialized")
            
        # Load datasets with timing
        load_start = time.time()
        source_df = self.connection_manager.load_dataset(**self.config.source_config)
        target_df = self.connection_manager.load_dataset(**self.config.target_config)
        load_time = time.time() - load_start
        
        if load_time > PERFORMANCE_LOG_THRESHOLD:
            self.logger.info(f"Dataset loading completed in {load_time:.2f}s")

        # Determine mappings and build a single joined DataFrame
        mappings = self.column_matcher.get_matching_columns(
            source_df, target_df, self.config.column_mappings
        )
        
        # Columns required: join keys + all mapped fields
        s_needed = set(self.config.key_fields + [m.source_field for m in mappings])
        t_needed = set(self.config.key_fields + [m.target_field for m in mappings])
        s_sel = [F.col(c).alias(f"s_{c}") for c in s_needed if c in source_df.columns]
        t_sel = [F.col(c).alias(f"t_{c}") for c in t_needed if c in target_df.columns]
        s_pruned = source_df.select(*s_sel)
        t_pruned = target_df.select(*t_sel)

        # Build single full_outer join on key fields with timing (null-safe join)
        join_start = time.time()
        join_cond = None
        for k in self.config.key_fields:
            left = s_pruned[f"s_{k}"] if f"s_{k}" in s_pruned.columns else F.lit(None)
            right = t_pruned[f"t_{k}"] if f"t_{k}" in t_pruned.columns else F.lit(None)
            c = left.eqNullSafe(right)
            join_cond = c if join_cond is None else (join_cond & c)
        
        # Apply optional broadcast hints
        broadcast = self.config.join_hints.get('broadcast', 'none')
        s_join = s_pruned
        t_join = t_pruned
        if broadcast == 'source':
            s_join = F.broadcast(s_pruned)
        elif broadcast == 'target':
            t_join = F.broadcast(t_pruned)
        
        joined = s_join.join(t_join, on=join_cond, how='full_outer')
        
        # Cache for large datasets or if sampling will be used
        should_cache_pre = self.config.sample_fraction < 1.0
        if should_cache_pre:
            joined = joined.cache()
        
        dataset_size = joined.count()
        if (not should_cache_pre) and dataset_size > LARGE_DATASET_THRESHOLD:
            joined = joined.cache()
            self.logger.info(f"Cached joined DataFrame with {dataset_size:,} rows")
        
        join_time = time.time() - join_start
        if join_time > PERFORMANCE_LOG_THRESHOLD:
            self.logger.info(f"Join operation completed in {join_time:.2f}s")

        # Completeness metrics: unmatched keys on each side (all key cols null on opposite side)
        t_null_all = reduce(lambda a, b: a & b, [joined[f"t_{k}"].isNull() for k in self.config.key_fields])
        s_null_all = reduce(lambda a, b: a & b, [joined[f"s_{k}"].isNull() for k in self.config.key_fields])
        left_only_df = joined.where(t_null_all)
        right_only_df = joined.where(s_null_all)
        
        unmatched_source_count = left_only_df.count()
        unmatched_target_count = right_only_df.count()

        # Get examples of unmatched keys
        unmatched_source_examples = [
            {k: row[f"s_{k}"] for k in self.config.key_fields}
            for row in left_only_df.select(*[f"s_{k}" for k in self.config.key_fields]).limit(self.config.top_k_examples).collect()
        ]
        unmatched_target_examples = [
            {k: row[f"t_{k}"] for k in self.config.key_fields}
            for row in right_only_df.select(*[f"t_{k}" for k in self.config.key_fields]).limit(self.config.top_k_examples).collect()
        ]

        # Record count comparator
        count_comparison = self.record_comparator.compare(source_df, target_df)
        self.logger.info(f"Record count comparison: {count_comparison}")

        # Compute field comparisons using single joined DF
        field_comparisons, mismatch_counts, aggregates = self.field_comparator.compute(joined, mappings)

        # Determine overall status using aggregate errors
        overall_status = STATUS_FAILED if (not count_comparison.threshold_met or any(v > 0 for v in mismatch_counts.values())) else STATUS_PASSED

        report = ReconciliationReport(
            record_count_comparison=count_comparison,
            field_comparisons=field_comparisons,
            overall_status=overall_status,
            timestamp=datetime.now(timezone.utc).isoformat(),
            source_details=self.config.source_config,
            target_details=self.config.target_config,
            mismatch_counts=mismatch_counts,
            unmatched_source_count=unmatched_source_count,
            unmatched_target_count=unmatched_target_count,
            unmatched_source_examples=unmatched_source_examples,
            unmatched_target_examples=unmatched_target_examples,
            field_aggregates=aggregates,
        )

        # Log completeness and performance summary
        self.logger.info(f"Unmatched source records (left_only): {unmatched_source_count}")
        self.logger.info(f"Unmatched target records (right_only): {unmatched_target_count}")
        
        total_time = time.time() - start_time
        if total_time > PERFORMANCE_LOG_THRESHOLD:
            self.logger.info(f"Total reconciliation completed in {total_time:.2f}s")

        return report

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
            'completeness': {
                'unmatched_source_count': report.unmatched_source_count,
                'unmatched_target_count': report.unmatched_target_count,
                'unmatched_source_examples': report.unmatched_source_examples,
                'unmatched_target_examples': report.unmatched_target_examples
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
                        'tolerance': r.tolerance,
                        'keys': r.keys,
                        'source_value': r.source_value,
                        'target_value': r.target_value
                    } for r in v
                ] for k, v in report.field_comparisons.items()
            },
            'mismatch_counts': report.mismatch_counts,
            'field_aggregates': report.field_aggregates,
        }
        
        # Write to JSON file
        with open(output_path, 'w') as f:
            json.dump(result_dict, f, indent=2)


def main():
    """CLI entry point: run reconciliation using provided config and connection type."""
    import argparse
    parser = argparse.ArgumentParser(description='Run data reconciliation framework')
    parser.add_argument('--config', required=True, help='Path to YAML config file')
    parser.add_argument('--connection', default='json', choices=['json', 'starburst'], help='Connection type')
    parser.add_argument('--output', help='Path to write JSON results (optional)')
    args = parser.parse_args()

    framework = ReconciliationFramework(args.config, connection_type=args.connection)
    report = framework.run_reconciliation()

    if args.output:
        framework.write_results(report, args.output)
        print(f"Results written to {args.output}")
    else:
        # Print concise summary to stdout
        print(f"Overall: {report.overall_status}")
        print(f"Source: {report.record_count_comparison.source_count} | Target: {report.record_count_comparison.target_count}")
        print(f"Unmatched Source: {report.unmatched_source_count} | Unmatched Target: {report.unmatched_target_count}")


if __name__ == '__main__':
    main()
