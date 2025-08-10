from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .models import ComparisonResult, RecordCountResult, ColumnMapping

class RecordCountComparator:
    """Compares record counts between source and target datasets."""

    def __init__(self, threshold: float = 1.0):
        self.threshold = threshold

    def compare(self, source_df: DataFrame, target_df: DataFrame) -> RecordCountResult:
        """Compare record counts between datasets."""
        source_count = source_df.count()
        target_count = target_df.count()
        match_percentage = min(source_count, target_count) / max(source_count, target_count)
        
        return RecordCountResult(
            source_count=source_count,
            target_count=target_count,
            match_percentage=match_percentage,
            threshold_met=match_percentage >= self.threshold
        )

class FieldComparator:
    """Compares individual fields between source and target datasets."""

    def __init__(self, key_fields: List[str]):
        self.key_fields = key_fields

    def compare(self, source_df: DataFrame, target_df: DataFrame, 
               mapping: ColumnMapping) -> List[ComparisonResult]:
        """Compare field values between datasets."""
        # Rename columns to avoid ambiguity
        source_columns = [col(c).alias(f"source_{c}") for c in source_df.columns]
        target_columns = [col(c).alias(f"target_{c}") for c in target_df.columns]
        
        source_df = source_df.select(*source_columns)
        target_df = target_df.select(*target_columns)
        
        # Update field names to match renamed columns
        source_field_qual = f"source_{mapping.source_field}"
        target_field_qual = f"target_{mapping.target_field}"
        
        # Join datasets on key fields (with renamed columns)
        join_conditions = [
            source_df[f"source_{k}"] == target_df[f"target_{k}"] 
            for k in self.key_fields
        ]
        join_condition = join_conditions[0] if join_conditions else None
        
        # Join with renamed columns
        comparison_df = source_df.join(
            target_df,
            on=join_condition,
            how='full_outer'
        )

        # Select and compare values
        comparison_values = comparison_df.select(
            comparison_df[source_field_qual].alias('source_val'),
            comparison_df[target_field_qual].alias('target_val')
        ).collect()
        
        results = []
        for row in comparison_values:
            # Handle different data types
            if mapping.comparison_type == 'numeric':
                try:
                    s_val = float(row['source_val']) if row['source_val'] is not None else None
                    t_val = float(row['target_val']) if row['target_val'] is not None else None
                except (ValueError, TypeError):
                    s_val = None
                    t_val = None
            else:
                s_val = str(row['source_val']) if row['source_val'] is not None else None
                t_val = str(row['target_val']) if row['target_val'] is not None else None
            
            if s_val is None or t_val is None:
                status = 'Error'
                message = 'Null value found'
                metric_value = None
            elif mapping.comparison_type == 'numeric':
                if isinstance(s_val, (int, float)) and isinstance(t_val, (int, float)):
                    diff = abs(float(s_val) - float(t_val))
                    status = 'Success' if diff <= mapping.tolerance else 'Error'
                    message = f'Difference: {diff}'
                    metric_value = diff
                else:
                    status = 'Error'
                    message = 'Invalid numeric values'
                    metric_value = None
            else:  # exact comparison
                status = 'Success' if s_val == t_val else 'Error'
                message = 'Exact match' if status == 'Success' else 'Values differ'
                metric_value = 0 if status == 'Success' else 1
            
            results.append(ComparisonResult(
                source_field=mapping.source_field,
                target_field=mapping.target_field,
                status=status,
                message=message,
                metric_value=metric_value,
                comparison_type=mapping.comparison_type,
                tolerance=mapping.tolerance
            ))
        
        return results

class ColumnMatcher:
    """Matches columns between source and target datasets."""

    def __init__(self, excluded_columns: List[str]):
        self.excluded_columns = set(excluded_columns)

    def get_matching_columns(self, source_df: DataFrame, target_df: DataFrame, 
                           configured_mappings: List[ColumnMapping]) -> List[ColumnMapping]:
        """Get list of columns to compare."""
        # Get all source columns except excluded ones
        source_columns = set(source_df.columns) - self.excluded_columns
        target_columns = set(target_df.columns)
        
        mappings = []
        configured_source_fields = {m.source_field for m in configured_mappings}
        
        # Add configured mappings first
        mappings.extend([
            m for m in configured_mappings 
            if m.source_field not in self.excluded_columns
        ])
        
        # Add automatic mappings for remaining columns
        for col_name in source_columns - configured_source_fields:
            if col_name in target_columns:  # Only compare if column exists in both
                mappings.append(ColumnMapping(
                    source_field=col_name,
                    target_field=col_name,
                    comparison_type='exact'
                ))
        
        return mappings
