import os
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

import great_expectations as gx
from great_expectations.data_context import FileDataContext
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.dataset import PandasDataset
from great_expectations.checkpoint import SimpleCheckpoint

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Production-grade Data Validation System using Great Expectations (GX).
    Uses a filesystem-based backend for context and data docs.
    """
    
    def __init__(self, project_root: str = "great_expectations"):
        self.project_root = project_root
        self._ensure_context()
        
    def _ensure_context(self):
        """Initializes or loads the file-based GX DataContext."""
        if not os.path.exists(self.project_root):
            os.makedirs(self.project_root, exist_ok=True)
            # Initialize a new file-based context
            self.context = gx.get_context(mode="file", project_root_dir=self.project_root)
            logger.info(f"Initialized new Great Expectations context at {self.project_root}")
        else:
            self.context = gx.get_context(mode="file", project_root_dir=self.project_root)
            logger.info(f"Loaded existing Great Expectations context from {self.project_root}")
    
    def generate_baseline_expectations(self, df: pd.DataFrame, suite_name: str, target_column: Optional[str] = None):
        """
        Dynamically infer schema and create baseline expectations from a reference dataframe.
        """
        # Create or replace suite
        suite = self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        
        # We'll use PandasDataset for easy programmatic expectation generation
        dataset = gx.from_pandas(df)
        
        # 1. Schema Validation: Expected columns
        columns = df.columns.tolist()
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={"column_list": columns}
            )
        )
        
        # 2. Add expectations per column
        for col in columns:
            dtype = str(df[col].dtype)
            
            # Data Types
            if "int" in dtype or "float" in dtype:
                type_list = ["int", "int64", "float", "float64"]
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_type_list",
                        kwargs={"column": col, "type_list": type_list}
                    )
                )
                
                # Basic statistical checks (value ranges based on min/max with some margin, optional)
                min_val = float(df[col].min())
                max_val = float(df[col].max())
                
                # Example: If age > 0 is implied by min_val >= 0
                if min_val >= 0 and "id" not in col.lower():
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_between",
                            kwargs={"column": col, "min_value": 0}
                        )
                    )
                    
            elif "bool" in dtype:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_type_list",
                        kwargs={"column": col, "type_list": ["bool"]}
                    )
                )
            else:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_in_type_list",
                        kwargs={"column": col, "type_list": ["str", "object"]}
                    )
                )
            
            # Missing Values: maximum 20% null allowed
            null_percent = df[col].isnull().sum() / len(df)
            if null_percent < 0.2:
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_proportion_of_unique_values_to_be_between",
                        kwargs={"column": col, "max_value": 1.0, "min_value": 0.0} # Basic bound
                    )
                )
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_not_be_null",
                        kwargs={"column": col, "mostly": 0.8} # at least 80% not null
                    )
                )

            # Unique constraints: If it looks like an ID and is highly unique
            if "id" in col.lower() and df[col].nunique() == len(df):
                suite.add_expectation(
                    ExpectationConfiguration(
                        expectation_type="expect_column_values_to_be_unique",
                        kwargs={"column": col}
                    )
                )
                
        # 3. Optional: Target column existence
        if target_column and target_column in columns:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": target_column}
                )
            )
            # Ensure target has no missing values
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": target_column}
                )
            )
            
        self.context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=suite_name)
        logger.info(f"Generated baseline expectations for suite '{suite_name}'")
        return suite

    def validate_dataset(self, df: pd.DataFrame, suite_name: str) -> Dict[str, Any]:
        """
        Validate a dataframe against an expectation suite.
        Returns a dictionary with validation results and parsed issues.
        """
        # Ensure suite exists
        try:
            suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)
        except Exception:
            raise ValueError(f"Expectation suite '{suite_name}' not found. Generate baseline first.")

        # Convert to GX PandasDataset
        dataset = gx.from_pandas(df)
        
        # Run validation
        validation_result = dataset.validate(expectation_suite=suite, result_format="SUMMARY")
        
        # Extract results
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
                    "message": f"Failed expectation: {exp_config['expectation_type']}",
                    "details": result["result"],
                    "severity": "error"
                })
        
        # Build Great Expectations Data Docs
        try:
            self.context.build_data_docs()
            data_docs_url = self.context.get_docs_sites_urls()[0]["site_url"]
        except Exception as str_exc:
            logger.warning(f"Failed to build data docs: {str_exc}")
            data_docs_url = ""

        return {
            "is_valid": success,
            "schema_match": success, # Simplified status
            "errors_count": stats["unsuccessful_expectations"],
            "warnings_count": 0,
            "issues": issues,
            "validated_at": pd.Timestamp.utcnow().isoformat(),
            "data_docs_url": data_docs_url,
            "stats": stats
        }

validator = DataValidator()
