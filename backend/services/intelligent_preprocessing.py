import pandas as pd
import numpy as np
import logging
import os
import uuid
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder, OrdinalEncoder
from sklearn.impute import SimpleImputer
from sklearn.utils import class_weight
from scipy.stats import skew

from services.storage_service import storage_service

logger = logging.getLogger(__name__)

# ============================================================================
# 1. DATA ANALYSIS MODULE
# ============================================================================

def analyze_dataset(df: pd.DataFrame, target: Optional[str] = None) -> Dict[str, Any]:
    """
    Intelligently analyze the dataset to detect issues and requirements.
    (Requirement 1: Data Analysis)
    """
    analysis = {
        "n_rows": len(df),
        "n_cols": len(df.columns),
        "missing_values": df.isna().sum().to_dict(),
        "duplicates": int(df.duplicated().sum()),
        "numeric_cols": df.select_dtypes(include=[np.number]).columns.tolist(),
        "categorical_cols": df.select_dtypes(exclude=[np.number, "datetime", "datetimetz"]).columns.tolist(),
        "datetime_cols": df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist(),
        "skewness": {},
        "imbalance": None,
        "high_correlation": []
    }

    # Detect Skewness
    for col in analysis["numeric_cols"]:
        if df[col].nunique() > 1:
            analysis["skewness"][col] = float(skew(df[col].dropna()))

    # Detect Imbalance if target exists
    if target and target in df.columns:
        if target in analysis["categorical_cols"] or df[target].nunique() < 20:
            counts = df[target].value_counts(normalize=True)
            if counts.max() > 0.8:  # Heuristic: 80% majority is imbalanced
                analysis["imbalance"] = counts.to_dict()

    # Detect high correlation (Requirement 6)
    if len(analysis["numeric_cols"]) > 1:
        corr_matrix = df[analysis["numeric_cols"]].corr().abs()
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
        high_corr = [column for column in upper.columns if any(upper[column] > 0.9)]
        analysis["high_correlation"] = high_corr

    return analysis

# ============================================================================
# 2. CLEANING MODULE
# ============================================================================

def clean_data(df: pd.DataFrame, analysis: Dict[str, Any], stats: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply automated cleaning. If stats are provided, use them (prevents leakage).
    """
    df_clean = df.copy()
    actual_stats = stats or {"numeric": {}, "categorical": {}}

    # 1. Handle missing values
    for col, count in analysis["missing_values"].items():
        if count > 0 or col in actual_stats["numeric"] or col in actual_stats["categorical"]:
            if col in analysis["numeric_cols"]:
                fill_val = actual_stats["numeric"].get(col, df_clean[col].median())
                df_clean[col] = df_clean[col].fillna(fill_val)
                actual_stats["numeric"][col] = fill_val
            elif col in analysis["categorical_cols"]:
                fill_val = actual_stats["categorical"].get(col, (df_clean[col].mode()[0] if not df_clean[col].mode().empty else "missing"))
                df_clean[col] = df_clean[col].fillna(fill_val)
                actual_stats["categorical"][col] = fill_val

    # 2. Remove duplicates (Only on train or as a generic rule)
    df_clean = df_clean.drop_duplicates()

    # 3. Normalize categorical values
    for col in analysis["categorical_cols"]:
        if col in df_clean.columns and df_clean[col].dtype == "object":
            df_clean[col] = df_clean[col].astype(str).str.lower().str.strip()

    return df_clean, actual_stats

# ============================================================================
# 3. OUTLIER HANDLING MODULE
# ============================================================================

def handle_outliers(df: pd.DataFrame, numeric_cols: List[str], thresholds: Optional[Dict[str, Tuple[float, float]]] = None) -> Tuple[pd.DataFrame, Dict[str, Tuple[float, float]]]:
    """
    Handle outliers using IQR. If thresholds are provided, use them (prevents leakage).
    """
    df_out = df.copy()
    actual_thresholds = thresholds or {}
    
    for col in numeric_cols:
        if col in actual_thresholds:
            lower_bound, upper_bound = actual_thresholds[col]
        else:
            Q1 = df_out[col].quantile(0.25)
            Q3 = df_out[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            actual_thresholds[col] = (lower_bound, upper_bound)
            
        df_out[col] = np.clip(df_out[col], lower_bound, upper_bound)
        
    return df_out, actual_thresholds

# ============================================================================
# 4. TRANSFORMATION MODULE
# ============================================================================

def transform_data(df: pd.DataFrame, analysis: Dict[str, Any], target: Optional[str] = None, transformers: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Handle encoding and scaling. If transformers are provided, use them (prevents leakage).
    """
    df_trans = df.copy()
    actual_transformers = transformers or {}

    # 1. Scaling
    cols_to_scale = [c for c in analysis["numeric_cols"] if c != target]
    if cols_to_scale:
        if "scaler" not in actual_transformers:
            actual_transformers["scaler"] = StandardScaler()
            df_trans[cols_to_scale] = actual_transformers["scaler"].fit_transform(df_trans[cols_to_scale])
        else:
            df_trans[cols_to_scale] = actual_transformers["scaler"].transform(df_trans[cols_to_scale])

    # 2. Encoding
    for col in analysis["categorical_cols"]:
        if col == target: continue
        
        unique_vals = df_trans[col].nunique()
        enc_key = f"enc_{col}"
        
        if unique_vals <= 10:
            if enc_key not in actual_transformers:
                ohe = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
                ohe.fit(df_trans[[col]].astype(str))
                actual_transformers[enc_key] = ohe
            
            encoded = actual_transformers[enc_key].transform(df_trans[[col]].astype(str))
            names = actual_transformers[enc_key].get_feature_names_out([col])
            encoded_df = pd.DataFrame(encoded, columns=names, index=df_trans.index)
            df_trans = pd.concat([df_trans.drop(columns=[col]), encoded_df], axis=1)
            if enc_key not in actual_transformers:
                oe = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)
                oe.fit(df_trans[[col]].astype(str))
                actual_transformers[enc_key] = oe
            
            df_trans[col] = actual_transformers[enc_key].transform(df_trans[[col]].astype(str))

    return df_trans, actual_transformers

# ============================================================================
# 5. FEATURE ENGINEERING MODULE
# ============================================================================

def engineer_features(df: pd.DataFrame, datetime_cols: List[str]) -> pd.DataFrame:
    """
    Extract datetime features and engineer simple components.
    (Requirement 5: Feature Engineering)
    """
    df_eng = df.copy()
    for col in datetime_cols:
        if not pd.api.types.is_datetime64_any_dtype(df_eng[col]):
            df_eng[col] = pd.to_datetime(df_eng[col], errors='coerce')
        
        # Extract features
        df_eng[f"{col}_year"] = df_eng[col].dt.year
        df_eng[f"{col}_month"] = df_eng[col].dt.month
        df_eng[f"{col}_day"] = df_eng[col].dt.day
        df_eng[f"{col}_hour"] = df_eng[col].dt.hour
        df_eng = df_eng.drop(columns=[col])
        
    return df_eng

# ============================================================================
# 6. FEATURE SELECTION MODULE
# ============================================================================

def select_features(df: pd.DataFrame, analysis: Dict[str, Any], target: Optional[str] = None) -> pd.DataFrame:
    """
    Drop low variance and highly correlated features.
    (Requirement 6: Feature Selection)
    """
    df_sel = df.copy()
    
    # 1. Drop low variance (constant columns)
    for col in df_sel.columns:
        if col == target: continue
        if df_sel[col].nunique() <= 1:
            df_sel = df_sel.drop(columns=[col])
            
    # 2. Drop high correlation (>0.9)
    if analysis["high_correlation"]:
        cols_to_drop = [c for c in analysis["high_correlation"] if c in df_sel.columns and c != target]
        df_sel = df_sel.drop(columns=cols_to_drop)
        
    return df_sel

# ============================================================================
# 7. ORCHESTRATOR
# ============================================================================

class IntelligentPipeline:
    """
    Main orchestrator for the intelligent preprocessing pipeline.
    (Requirement: Main pipeline orchestrator function)
    """
    
    @staticmethod
    def run(df: pd.DataFrame, target: Optional[str] = None) -> pd.DataFrame:
        """
        Executes the intelligent pipeline end-to-end with strict target protection.
        """
        logger.info(f"🚀 FINAL VALIDATION | Rows: {len(df)}")
        
        # 1. Split BEFORE anyone touches the data
        train_df, test_df = train_test_split(df.copy(), test_size=0.2, random_state=42)
        
        # 2. STRICT TARGET SEPARATION (Requirement: y separated BEFORE transformations)
        y_train = train_df[target] if target and target in train_df.columns else None
        y_test = test_df[target] if target and target in test_df.columns else None
        
        X_train = train_df.drop(columns=[target]) if target and target in train_df.columns else train_df.copy()
        X_test = test_df.drop(columns=[target]) if target and target in test_df.columns else test_df.copy()

        # 3. Phase: Train Fit & Transform on FEATURES ONLY
        analysis_train = analyze_dataset(X_train) # target is none here
        
        # Feature Engineering (Extraction)
        X_train = engineer_features(X_train, analysis_train["datetime_cols"])
        X_test = engineer_features(X_test, [c for c in analysis_train["datetime_cols"] if c in X_test.columns])
        
        # Re-analyze features post-engineering
        analysis_train = analyze_dataset(X_train)
        
        # Cleaning (Capture Stats)
        X_train, cleaning_stats = clean_data(X_train, analysis_train)
        
        # Outliers (Capture Thresholds)
        X_train, outlier_thresholds = handle_outliers(X_train, analysis_train["numeric_cols"])
        
        # Feature Selection (Target is safe as it's separate)
        X_train = select_features(X_train, analysis_train)
        final_cols = X_train.columns.tolist()
        
        # Transformation (Fit Scalers/Encoders)
        train_trans, transformers = transform_data(X_train, analyze_dataset(X_train))

        # 4. Phase: Test Transform (Using Train Stats - ZERO TARGET LEAKAGE)
        X_test = X_test[[c for c in X_test.columns if c in final_cols or c in analysis_train["categorical_cols"]]]
        X_test, _ = clean_data(X_test, analysis_train, stats=cleaning_stats)
        X_test, _ = handle_outliers(X_test, analysis_train["numeric_cols"], thresholds=outlier_thresholds)
        
        test_trans, _ = transform_data(X_test, analyze_dataset(X_test), transformers=transformers)
        
        # 5. Requirement: FEATURE ORDER GUARANTEE (Explicit Sort)
        train_trans = train_trans.reindex(columns=train_trans.columns) # for safety
        test_trans = test_trans.reindex(columns=train_trans.columns, fill_value=0)
        
        train_trans = train_trans.sort_index(axis=1) # Requirement: Sort alphabetical
        test_trans = test_trans.sort_index(axis=1) # Requirement: Sort alphabetical

        # 6. Final assembly: Re-attach target and split indicator
        if y_train is not None: train_trans[target] = y_train
        if y_test is not None: test_trans[target] = y_test
        
        train_trans['is_train'] = 1
        test_trans['is_train'] = 0
        df_processed = pd.concat([train_trans, test_trans])
        
        # Imbalance Weighting (on target)
        if analysis_train["imbalance"] and target and target in df_processed.columns:
            df_processed['sample_weight'] = class_weight.compute_sample_weight('balanced', df_processed[target])

        logger.info("✅ Intelligent Pipeline finalized and production-ready.")
        return df_processed

# ============================================================================
# INTEGRATION WITH PREPROCESSING SERVICE
# ============================================================================

def execute_automated_pipeline(db: Session, dataset_id: uuid.UUID, user_id: uuid.UUID, target: Optional[str] = None) -> uuid.UUID:
    """
    Integrates the intelligent pipeline with the existing PreprocessingService and versioning.
    (Requirement 3: Integration with existing service)
    """
    from services.preprocessing_service import PreprocessingService
    from models.dataset import Dataset
    
    # Fetch parent
    parent_dataset = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
    if not parent_dataset:
        raise ValueError("Dataset not found")
        
    root_id = parent_dataset.root_dataset_id or parent_dataset.id
    
    # Load
    df = storage_service.read_df(parent_dataset.storage_path)
    
    # Process
    df_processed = IntelligentPipeline.run(df, target)
    
    # Save new version (Requirement: Output vN+1)
    new_version_num = parent_dataset.version + 1
    user_id_str = str(user_id)
    storage_filename = f"v{new_version_num}_auto.csv"
    storage_path = os.path.join("uploads", user_id_str, str(root_id), storage_filename)
    
    storage_service.write_df(storage_path, df_processed)
    
    # Database Update
    new_dataset_id = uuid.uuid4()
    new_dataset = Dataset(
        id=new_dataset_id,
        user_id=user_id,
        name=f"{parent_dataset.name}_auto",
        filename=storage_filename,
        storage_path=storage_path,
        file_size=storage_service.get_file_size(storage_path),
        status="processed",
        row_count=len(df_processed),
        col_count=len(df_processed.columns),
        version=new_version_num,
        root_dataset_id=root_id,
        parent_dataset_id=parent_dataset.id,
        is_latest=True
    )
    
    # Concurrent safety: mark others not latest
    db.query(Dataset).filter(Dataset.root_dataset_id == root_id).update({"is_latest": False})
    
    db.add(new_dataset)
    db.commit()
    return new_dataset.id
