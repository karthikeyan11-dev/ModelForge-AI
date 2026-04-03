import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class DataCleaning:
    """
    Production-grade rule-based data cleaning.
    Designed for robustness against empty columns, mixed types, and large datasets.
    """
    
    def handle_missing_values(self, df: pd.DataFrame, strategy: str = "mean") -> pd.DataFrame:
        """Handles missing values by filling with mean, median, mode, or dropping."""
        df = df.copy() # Avoid SettingWithCopyWarning
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(exclude=[np.number]).columns
        
        try:
            if strategy == "mean":
                if not numeric_cols.empty:
                    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
            elif strategy == "median":
                if not numeric_cols.empty:
                    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
            elif strategy == "mode":
                # General approach for all columns
                for col in df.columns:
                    modes = df[col].mode()
                    if not modes.empty:
                        df[col] = df[col].fillna(modes.iloc[0])
            elif strategy == "drop":
                df.dropna(inplace=True)
            
            # Final fallback for any remains: fill with "Unknown" or 0
            df[numeric_cols] = df[numeric_cols].fillna(0)
            df[categorical_cols] = df[categorical_cols].fillna("Unknown")
            
        except Exception as e:
            logger.warning(f"Error in handle_missing_values: {e}")
            
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes duplicate rows."""
        return df.drop_duplicates().reset_index(drop=True)

    def fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Attempts to convert columns to appropriate data types."""
        df = df.copy()
        for col in df.columns:
            # Try to convert to numeric if logical
            try:
                # Use errors='ignore' to keep original if it's purely text
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except Exception:
                pass
                
            # Convert to string if it's mixed/object to avoid downstream errors
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('nan', 'Unknown')
                
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies all cleaning steps systematically."""
        if df.empty:
            return df
            
        try:
            # 1. Deduplication
            df = self.remove_duplicates(df)
            
            # 2. Type Inference
            df = self.fix_data_types(df)
            
            # 3. Missing Value Imputation
            df = self.handle_missing_values(df)
            
            # 4. Final sanitization
            df = df.infer_objects(copy=False)
            
            return df
        except Exception as e:
            logger.error(f"Critical error in clean_data pipeline: {e}")
            return df # Return best effort
