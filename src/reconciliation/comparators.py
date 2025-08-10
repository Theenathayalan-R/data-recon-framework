from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from .models import ComparisonResult, RecordCountResult, ColumnMapping
from .constants import STATUS_SUCCESS, STATUS_ERROR, COMPARISON_TYPE_NUMERIC, COMPARISON_TYPE_EXACT, COMPARISON_TYPE_DATETIME

class RecordCountComparator:
    """Compares record counts between source and target datasets."""

    def __init__(self, threshold: float = 1.0):
        self.threshold = threshold

    def compare(self, source_df: DataFrame, target_df: DataFrame) -> RecordCountResult:
        """Compare record counts between datasets."""
        source_count = source_df.count()
        target_count = target_df.count()
        max_count = max(source_count, target_count)
        match_percentage = 1.0 if max_count == 0 else min(source_count, target_count) / max_count
        
        return RecordCountResult(
            source_count=source_count,
            target_count=target_count,
            match_percentage=match_percentage,
            threshold_met=match_percentage >= self.threshold
        )

class FieldComparator:
    """Compares fields using a single joined DataFrame for scalability."""

    def __init__(self, key_fields: List[str], top_k: int = 10, sample_fraction: float = 1.0, sample_seed: int = 42):
        self.key_fields = key_fields
        self.top_k = top_k
        self.sample_fraction = sample_fraction
        self.sample_seed = sample_seed

    def _datetime_diff_seconds(self, s_val, t_val, mapping: ColumnMapping):
        # Normalize to timestamp type with optional formats and timezones
        def to_ts(col, fmt, tz):
            if tz:
                # Parse to timestamp (may apply session TZ), then extract wall-clock and reinterpret as tz
                parsed = F.to_timestamp(col, fmt) if fmt else F.to_timestamp(col)
                # If still null, try simple cast
                parsed = F.when(parsed.isNull(), col.cast('timestamp')).otherwise(parsed)
                # Get wall-clock components as string and rebuild a timestamp without offsets
                wall = F.date_format(parsed, "yyyy-MM-dd HH:mm:ss")
                wall_ts = F.to_timestamp(wall, "yyyy-MM-dd HH:mm:ss")
                # Now interpret wall_ts as time in tz and convert to UTC
                return F.to_utc_timestamp(wall_ts, tz)
            else:
                # No tz specified: rely on parser/cast so ISO8601 with Z/+hh:mm works
                parsed = F.to_timestamp(col, fmt) if fmt else F.to_timestamp(col)
                return F.when(parsed.isNull(), col.cast('timestamp')).otherwise(parsed)

        s_ts = to_ts(s_val, mapping.source_format, mapping.source_timezone)
        t_ts = to_ts(t_val, mapping.target_format, mapping.target_timezone)
        # Difference in seconds
        diff_seconds = F.abs(F.unix_timestamp(s_ts) - F.unix_timestamp(t_ts)).cast('double')
        return s_ts, t_ts, diff_seconds

    def _severity_columns(self, joined: DataFrame, mapping: ColumnMapping):
        s_val = joined[f"s_{mapping.source_field}"]
        t_val = joined[f"t_{mapping.target_field}"]
        if mapping.comparison_type == COMPARISON_TYPE_NUMERIC:
            s_cast = s_val.cast('double')
            t_cast = t_val.cast('double')
            diff = F.abs(s_cast - t_cast)
            status_col = F.when(s_cast.isNull() | t_cast.isNull(), F.lit(STATUS_ERROR)) \
                         .when(diff <= F.lit(mapping.tolerance), F.lit(STATUS_SUCCESS)) \
                         .otherwise(F.lit(STATUS_ERROR))
            message_col = F.when(s_cast.isNull() | t_cast.isNull(), F.lit('Null value found')) \
                           .otherwise(F.concat(F.lit('Difference: '), F.format_number(diff, 6)))
            metric_col = F.when(s_cast.isNull() | t_cast.isNull(), F.lit(None)) \
                          .otherwise(diff)
            sev = F.when(metric_col.isNull(), F.lit(0.0)).otherwise(-metric_col)
        elif mapping.comparison_type == COMPARISON_TYPE_DATETIME:
            s_ts, t_ts, diff_seconds = self._datetime_diff_seconds(s_val, t_val, mapping)
            # Convert tolerance to seconds based on unit
            unit = (mapping.datetime_tolerance_unit or 'seconds').lower()
            tol_seconds = F.lit(mapping.tolerance) * F.when(F.lit(unit == 'milliseconds'), F.lit(0.001)) \
                                                       .when(F.lit(unit == 'seconds'), F.lit(1.0)) \
                                                       .when(F.lit(unit == 'minutes'), F.lit(60.0)) \
                                                       .when(F.lit(unit == 'hours'), F.lit(3600.0)) \
                                                       .otherwise(F.lit(1.0))
            status_col = F.when(s_ts.isNull() | t_ts.isNull(), F.lit(STATUS_ERROR)) \
                         .when(diff_seconds <= tol_seconds, F.lit(STATUS_SUCCESS)) \
                         .otherwise(F.lit(STATUS_ERROR))
            message_col = F.when(s_ts.isNull() | t_ts.isNull(), F.lit('Null or unparsable datetime')) \
                           .otherwise(F.concat(F.lit('Time diff seconds: '), F.format_number(diff_seconds, 6)))
            metric_col = F.when(s_ts.isNull() | t_ts.isNull(), F.lit(None)) \
                          .otherwise(diff_seconds)
            sev = F.when(metric_col.isNull(), F.lit(0.0)).otherwise(-metric_col)
        else:
            status_col = F.when(s_val.isNull() | t_val.isNull(), F.lit(STATUS_ERROR)) \
                         .when(s_val == t_val, F.lit(STATUS_SUCCESS)) \
                         .otherwise(F.lit(STATUS_ERROR))
            message_col = F.when(s_val.isNull() | t_val.isNull(), F.lit('Null value found')) \
                           .when(s_val == t_val, F.lit('Exact match')) \
                           .otherwise(F.lit('Values differ'))
            metric_col = F.when(s_val.isNull() | t_val.isNull(), F.lit(None)) \
                          .when(s_val == t_val, F.lit(0.0)) \
                          .otherwise(F.lit(1.0))
            sev = F.when(metric_col.isNull(), F.lit(0.0)).otherwise(-metric_col)
        return s_val, t_val, status_col, message_col, metric_col, sev

    def compute(self, joined: DataFrame, mappings: List[ColumnMapping]) -> Tuple[Dict[str, List[ComparisonResult]], Dict[str, int], Dict[str, Dict[str, float]]]:
        """Compute comparisons for all mappings using one joined DataFrame.
        
        This method performs field-by-field comparisons on a pre-joined DataFrame,
        supporting both exact and numeric comparisons with configurable tolerances.
        It efficiently processes all mappings in a single pass for optimal performance.
        
        Args:
            joined: Pre-joined DataFrame with s_ and t_ prefixed columns from source/target
            mappings: List of column mappings defining comparisons to perform
            
        Returns:
            Tuple containing:
            - examples_by_field: Top-K examples per field comparison (errors prioritized)
            - mismatch_counts: Error count per field mapping
            - aggregates_by_field: Statistics including total_compared, error_count, 
              match_count, and match_rate for each field
              
        Raises:
            ValueError: If key_fields is empty (required for field comparisons)
        """
        if not self.key_fields:
            raise ValueError(
                "key_fields must not be empty for field comparisons. "
                "Ensure your configuration file specifies valid key_fields."
            )

        examples: Dict[str, List[ComparisonResult]] = {}
        mismatch_counts: Dict[str, int] = {}
        aggregates: Dict[str, Dict[str, float]] = {}

        # Optionally sample before heavy operations
        base_df = joined.sample(withReplacement=False, fraction=self.sample_fraction, seed=self.sample_seed) if self.sample_fraction < 1.0 else joined

        for mapping in mappings:
            key = f"{mapping.source_field} vs {mapping.target_field}"
            s_val, t_val, status_col, message_col, metric_col, sev = self._severity_columns(base_df, mapping)

            # Aggregates
            stats_df = base_df.select(status_col.alias('status')).groupBy('status').count()
            stats = {r['status']: r['count'] for r in stats_df.collect()}
            total = float(sum(stats.values()))
            errors = int(stats.get(STATUS_ERROR, 0))
            matches = int(stats.get(STATUS_SUCCESS, 0))
            match_rate = (matches / total) if total > 0 else 1.0
            aggregates[key] = {
                'total_compared': total,
                'error_count': float(errors),
                'match_count': float(matches),
                'match_rate': match_rate
            }
            mismatch_counts[key] = errors

            # Top-k examples
            key_cols = [F.coalesce(base_df[f"s_{k}"], base_df[f"t_{k}"]).alias(k) for k in self.key_fields]
            err_df = base_df.select(*key_cols,
                                    s_val.alias('source_value'),
                                    t_val.alias('target_value'),
                                    status_col.alias('status'),
                                    message_col.alias('message'),
                                    metric_col.alias('metric'),
                                    sev.alias('sev')).where(F.col('status') == STATUS_ERROR)
            topk = err_df.orderBy(F.col('sev')).limit(self.top_k)
            rows = topk.collect()
            # Fallback to successes if no errors
            if not rows:
                succ_df = base_df.select(*key_cols,
                                         s_val.alias('source_value'),
                                         t_val.alias('target_value'),
                                         status_col.alias('status'),
                                         message_col.alias('message'),
                                         metric_col.alias('metric'),
                                         F.lit(0.0).alias('sev')).where(F.col('status') == STATUS_SUCCESS)
                rows = succ_df.limit(self.top_k).collect()

            ex: List[ComparisonResult] = []
            for r in rows:
                ex.append(ComparisonResult(
                    source_field=mapping.source_field,
                    target_field=mapping.target_field,
                    status=r['status'],
                    message=str(r['message']) if r['message'] is not None else '',
                    metric_value=float(r['metric']) if r['metric'] is not None else None,
                    comparison_type=mapping.comparison_type,
                    tolerance=mapping.tolerance,
                    source_value=r['source_value'],
                    target_value=r['target_value'],
                    keys={k: r[k] for k in self.key_fields}
                ))
            examples[key] = ex

        return examples, mismatch_counts, aggregates

    def compare(self, source_df: DataFrame, target_df: DataFrame, mapping: ColumnMapping) -> List[ComparisonResult]:
        """Backward compatible wrapper for single mapping comparison.
        
        This method builds a joined DataFrame for a single mapping and performs the comparison.
        Used primarily by legacy tests and single-field comparison scenarios.
        
        Args:
            source_df: Source DataFrame
            target_df: Target DataFrame  
            mapping: Single column mapping to compare
            
        Returns:
            List of ComparisonResult objects for the mapping
            
        Raises:
            ValueError: If key_fields is empty (required for field comparisons)
        """
        if not self.key_fields:
            raise ValueError(
                "key_fields must not be empty for field comparisons. "
                "Ensure your configuration specifies valid key_fields."
            )
        # Select only needed columns
        s_sel = [F.col(k).alias(f"s_{k}") for k in self.key_fields if k in source_df.columns]
        s_sel.append(F.col(mapping.source_field).alias(f"s_{mapping.source_field}"))
        t_sel = [F.col(k).alias(f"t_{k}") for k in self.key_fields if k in target_df.columns]
        t_sel.append(F.col(mapping.target_field).alias(f"t_{mapping.target_field}"))
        s_pruned = source_df.select(*s_sel)
        t_pruned = target_df.select(*t_sel)
        # Build join condition across all keys (null-safe)
        join_cond = None
        for k in self.key_fields:
            left = s_pruned[f"s_{k}"] if f"s_{k}" in s_pruned.columns else F.lit(None)
            right = t_pruned[f"t_{k}"] if f"t_{k}" in t_pruned.columns else F.lit(None)
            c = left.eqNullSafe(right)
            join_cond = c if join_cond is None else (join_cond & c)
        joined = s_pruned.join(t_pruned, on=join_cond, how='full_outer')
        examples, _, _ = self.compute(joined, [mapping])
        key = f"{mapping.source_field} vs {mapping.target_field}"
        return examples.get(key, [])

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
        
        mappings: List[ColumnMapping] = []
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
                    comparison_type=COMPARISON_TYPE_EXACT
                ))
        
        return mappings
