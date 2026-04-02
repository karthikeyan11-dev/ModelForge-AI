import os
import pandas as pd
from typing import Union, Optional
from io import BytesIO, StringIO
import logging

logger = logging.getLogger(__name__)

class StorageService:
    @staticmethod
    def read_df(path: str) -> pd.DataFrame:
        """
        Abstration for reading a tabular file into a Pandas DataFrame.
        Supports CSV, XLSX, XLS.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found at: {path}")
            
        file_ext = path.split(".")[-1].lower()
        
        try:
            if file_ext == "csv":
                return pd.read_csv(path)
            elif file_ext in ["xlsx", "xls"]:
                return pd.read_excel(path)
            else:
                raise ValueError(f"Unsupported storage extension: {file_ext}")
        except Exception as e:
            logger.error(f"StorageService Error while reading {path}: {str(e)}")
            raise e

    @staticmethod
    def write_df(path: str, df: pd.DataFrame, overwrite: bool = False) -> str:
        """
        Abstraction for saving a Pandas DataFrame to disk.
        Includes overwrite protection and post-write validation.
        """
        if not overwrite and os.path.exists(path):
            raise FileExistsError(f"Storage Violation: Attempted to overwrite protected artifact at {path}")

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        file_ext = path.split(".")[-1].lower()
        
        try:
            if file_ext == "csv":
                df.to_csv(path, index=False)
            elif file_ext in ["xlsx", "xls"]:
                df.to_excel(path, index=False)
            else:
                df.to_csv(path, index=False)
            
            # Post-Write Validation (Audit Requirement 2A)
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                raise IOError(f"Storage Failure: File at {path} was not written correctly or is empty.")
                
            return path
        except Exception as e:
            logger.error(f"StorageService Error while writing {path}: {str(e)}")
            raise e

    @staticmethod
    def write_raw(path: str, contents: bytes, overwrite: bool = False) -> str:
        """
        Abstraction for saving raw binary contents.
        Includes overwrite protection and post-write validation.
        """
        if not overwrite and os.path.exists(path):
            raise FileExistsError(f"Storage Violation: Attempted to overwrite protected artifact at {path}")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        try:
            with open(path, "wb") as f:
                f.write(contents)
            
            # Post-Write Validation
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                raise IOError(f"Storage Failure: Raw file at {path} was not written correctly or is empty.")
                
            return path
        except Exception as e:
            logger.error(f"StorageService Error while writing raw {path}: {str(e)}")
            raise e

    @staticmethod
    def get_file_size(path: str) -> float:
        """Get file size in bytes."""
        if not os.path.exists(path):
            return 0.0
        return float(os.path.getsize(path))

storage_service = StorageService()
