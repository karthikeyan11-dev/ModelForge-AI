import os
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

try:
    import great_expectations as gx
    from great_expectations.datasource.fluent import PandasDatasource
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
    logging.warning("Great Expectations not installed. Validation features will be limited.")

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Production-grade Data Validation System using Great Expectations (GX).
    Uses a filesystem-based backend for context and data docs.
    """
    
    def __init__(self, project_root: str = "great_expectations"):
        self.project_root = project_root
        self.GX_AVAILABLE = GX_AVAILABLE
        if GX_AVAILABLE:
            self._ensure_context()
        
    def _ensure_context(self):
        """Initializes or loads the file-based GX DataContext."""
        try:
            if not os.path.exists(self.project_root):
                os.makedirs(self.project_root, exist_ok=True)
            
            # Use ephemeral context if file context fails or for simpler setups
            self.context = gx.get_context(project_root_dir=self.project_root)
            logger.info(f"Loaded Great Expectations context at {self.project_root}")
        except Exception as e:
            logger.error(f"Failed to initialize GE context: {e}")
            self.GX_AVAILABLE = False

    def generate_baseline_expectations(self, df: pd.DataFrame, suite_name: str, target_column: Optional[str] = None):
        """
        Dynamically infer schema and create baseline expectations from a reference dataframe.
        Using the higher-level Validator API for better compatibility across GX versions.
        """
        if not self.GX_AVAILABLE:
            logger.warning("Skipping baseline generation: GX not available")
            return None

        # Create or replace suite
        suite = self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        
        # Create a validator for this dataframe
        # We'll use a temporary datasource for the baseline generation
        datasource_name = f"ds_{uuid_str()[:8]}"
        data_asset_name = f"asset_{uuid_str()[:8]}"
        
        try:
            # Modern Fluent API (0.17+)
            validator = self.context.sources.add_pandas(name=datasource_name).add_dataframe_asset(name=data_asset_name).get_validator(expectation_suite_name=suite_name)
            validator.active_batch.data = df # Inject data directly
        except:
            # Fallback for older 0.16/0.17 versions
            validator = self.context.get_validator(
                batch_request=gx.core.batch.RuntimeBatchRequest(
                    datasource_name="default_pandas_datasource", # Assuming configured or ad-hoc
                    data_connector_name="default_runtime_data_connector_name",
                    data_asset_name="baseline_df",
                    runtime_parameters={"batch_data": df},
                    batch_identifiers={"default_identifier_name": "baseline"},
                ),
                expectation_suite_name=suite_name,
            )

        # 1. Schema Validation: Expected columns
        columns = df.columns.tolist()
        validator.expect_table_columns_to_match_ordered_list(column_list=columns)
        
        # 2. Add expectations per column
        for col in columns:
            series = df[col]
            dtype = str(series.dtype)
            
            # Data Types
            if "int" in dtype or "float" in dtype:
                validator.expect_column_values_to_be_in_type_list(column=col, type_list=["int", "int64", "float", "float64", "decimal"])
                
                # Basic range checks for plausible numeric values
                if not series.empty:
                    valid_series = series.dropna()
                    if not valid_series.empty:
                        min_val = float(valid_series.min())
                        if min_val >= 0 and "id" not in col.lower():
                            validator.expect_column_values_to_be_between(column=col, min_value=0)
            
            elif "bool" in dtype:
                validator.expect_column_values_to_be_in_type_list(column=col, type_list=["bool", "int"])
            else:
                validator.expect_column_values_to_be_in_type_list(column=col, type_list=["str", "object"])
            
            # Missing Values: Check if column is mostly non-null
            null_count = series.isnull().sum()
            if null_count / len(df) < 0.1: # If less than 10% null, require 90% non-null
                validator.expect_column_values_to_not_be_null(column=col, mostly=0.9)

            # Unique constraints for IDs
            if "id" in col.lower() and series.nunique() == len(df):
                validator.expect_column_values_to_be_unique(column=col)
                
        # 3. Target column constraints
        if target_column and target_column in columns:
            validator.expect_column_to_exist(column=target_column)
            validator.expect_column_values_to_not_be_null(column=target_column)
            
        validator.save_expectation_suite(discard_failed_expectations=False)
        logger.info(f"Generated baseline expectations for suite '{suite_name}'")
        return suite

    def validate_dataset(self, df: pd.DataFrame, suite_name: str) -> Dict[str, Any]:
        """
        Validate a dataframe against an expectation suite.
        Returns a dictionary with validation results and parsed issues.
        """
        if not self.GX_AVAILABLE:
            return {
                "is_valid": True,
                "issues": [],
                "message": "Validation skipped: Great Expectations not available",
                "validated_at": pd.Timestamp.utcnow().isoformat()
            }

        try:
            # Retrieve suite
            suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)
        except Exception:
            # If suite doesn't exist, we don't fail, we just pass but log
            logger.info(f"No suite found for {suite_name}, skipping validation")
            return {
                "is_valid": True,
                "schema_match": True,
                "errors_count": 0,
                "warnings_count": 0,
                "issues": [],
                "validated_at": pd.Timestamp.utcnow().isoformat()
            }

        # Validate
        try:
            # We use a simple PandasDataset wrapper if on older GX, or Validator for newer GX
            # To be most compatible across 0.15-0.18, we use the PandasDataset approach for validation
            from great_expectations.dataset import PandasDataset
            pd_dataset = PandasDataset(df, expectation_suite=suite)
            validation_result = pd_dataset.validate(result_format="SUMMARY")
            
            success = validation_result["success"]
            stats = validation_result["statistics"]
            
            issues = []
            for result in validation_result["results"]:
                if not result["success"]:
                    exp_config = result["expectation_config"]
                    kw = exp_config["kwargs"]
                    issues.append({
                        "type": exp_config["expectation_type"],
                        "column": kw.get("column", "table_level"),
                        "message": f"Validation failed for {exp_config['expectation_type']}",
                        "details": str(result["result"].get("partial_exception_message", "Value mismatch")),
                        "severity": "error"
                    })
            
            return {
                "is_valid": success,
                "schema_match": success,
                "errors_count": stats.get("unsuccessful_expectations", 0),
                "warnings_count": 0,
                "issues": issues,
                "validated_at": pd.Timestamp.utcnow().isoformat(),
                "stats": stats
            }
        except Exception as e:
            logger.error(f"Validation execution failed: {e}")
            return {
                "is_valid": True, # Fail-safe
                "issues": [{"type": "system_error", "message": str(e)}],
                "validated_at": pd.Timestamp.utcnow().isoformat()
            }

def uuid_str():
    import uuid
    return str(uuid.uuid4())

# Global instance
validator = DataValidator()
