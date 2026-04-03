"""
Prism AI — Intelligent Preprocessing Agent (Production-Grade)
=============================================================
Steps 1–10 of the AutoML Preprocessing Roadmap.

Step  1: Dynamic Decision Engine (Adaptive Thresholds)
Step  2: Target Detection + Problem Type Awareness
Step  3: Feature Importance-Based Selection (Intelligent Pruning)
Step  4: Preprocessing Pipeline Persistence (sklearn Pipeline + joblib)
Step  5: Conditional Format Standardization
Step  6: Data Drift Detection
Step  7: EDA Visualization Layer
Step  8: Enhanced Structured Logging (before/after stats)
Step  9: Robust Error Handling (per-step try/except + fallback)
Step 10: Strict Sequential Execution Discipline
"""

import pandas as pd
import numpy as np
import logging
import math
import joblib
import os
import json
import warnings
from typing import Dict, Any, List, Optional, Tuple
from scipy import stats as scipy_stats
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.feature_selection import mutual_info_classif, mutual_info_regression
try:
    from imblearn.over_sampling import SMOTE
    SMOTE_AVAILABLE = True
except ImportError:
    SMOTE_AVAILABLE = False

warnings.filterwarnings("ignore", category=FutureWarning)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM SKLEARN TRANSFORMERS  (Step 4)
# ─────────────────────────────────────────────────────────────────────────────

class FeatureAligner(BaseEstimator, TransformerMixin):
    """Aligns input DataFrame to the training-time column order.
    Missing columns → filled with 0.  Extra columns → silently dropped."""

    def __init__(self, expected_columns: Optional[List[str]] = None):
        self.expected_columns = expected_columns

    def fit(self, X, y=None):
        if self.expected_columns is None:
            self.expected_columns = list(X.columns)
        return self

    def transform(self, X):
        X = X.copy()
        missing = [c for c in self.expected_columns if c not in X.columns]
        if missing:
            logger.warning(f"FeatureAligner: filling missing columns {missing} with 0")
        for col in missing:
            X[col] = 0
        return X[self.expected_columns]

    def get_feature_names_out(self, input_features=None):
        return np.array(self.expected_columns or [], dtype=object)


class OutlierCapper(BaseEstimator, TransformerMixin):
    """Clips numeric outliers using IQR bounds learned during fit().
    Works with both DataFrames and numpy arrays (inside ColumnTransformer)."""

    def __init__(self, columns: Optional[List[str]] = None):
        self.columns = columns or []
        self.bounds_: Dict[int, Tuple[float, float]] = {}  # index-based
        self.n_features_in_: int = 0

    def fit(self, X, y=None):
        X_arr = np.asarray(X)
        self.n_features_in_ = X_arr.shape[1]
        col_names = list(X.columns) if hasattr(X, 'columns') else [str(i) for i in range(X_arr.shape[1])]
        target_indices = []
        if self.columns:
            for c in self.columns:
                if c in col_names:
                    target_indices.append(col_names.index(c))
        else:
            target_indices = list(range(X_arr.shape[1]))

        for idx in target_indices:
            col_data = X_arr[:, idx].astype(float)
            valid = col_data[~np.isnan(col_data)]
            if len(valid) >= 4:
                q1, q3 = np.percentile(valid, [25, 75])
                iqr = q3 - q1
                if iqr > 0:
                    self.bounds_[idx] = (float(q1 - 1.5 * iqr), float(q3 + 1.5 * iqr))
        return self

    def transform(self, X):
        X_arr = np.array(X, dtype=float, copy=True)
        for idx, (lo, hi) in self.bounds_.items():
            if idx < X_arr.shape[1]:
                X_arr[:, idx] = np.clip(X_arr[:, idx], lo, hi)
        if hasattr(X, 'columns'):
            return pd.DataFrame(X_arr, columns=X.columns, index=X.index)
        return X_arr

    def get_feature_names_out(self, input_features=None):
        if input_features is not None:
            return np.array(input_features, dtype=object)
        return np.array([f"x{i}" for i in range(self.n_features_in_)], dtype=object)


class SkewCorrector(BaseEstimator, TransformerMixin):
    """Applies log1p to columns whose training-time skew exceeded the threshold.
    Works with both DataFrames and numpy arrays (inside ColumnTransformer)."""

    def __init__(self, columns: Optional[List[str]] = None):
        self.columns = columns or []
        self._target_indices: List[int] = []
        self.n_features_in_: int = 0

    def fit(self, X, y=None):
        X_arr = np.asarray(X)
        self.n_features_in_ = X_arr.shape[1]
        col_names = list(X.columns) if hasattr(X, 'columns') else [str(i) for i in range(X_arr.shape[1])]
        self._target_indices = [col_names.index(c) for c in self.columns if c in col_names]
        return self

    def transform(self, X):
        X_arr = np.array(X, dtype=float, copy=True)
        for idx in self._target_indices:
            if idx < X_arr.shape[1]:
                col_data = X_arr[:, idx]
                if np.all(col_data[~np.isnan(col_data)] > 0):
                    X_arr[:, idx] = np.log1p(col_data)
        if hasattr(X, 'columns'):
            return pd.DataFrame(X_arr, columns=X.columns, index=X.index)
        return X_arr

    def get_feature_names_out(self, input_features=None):
        if input_features is not None:
            return np.array(input_features, dtype=object)
        return np.array([f"x{i}" for i in range(self.n_features_in_)], dtype=object)


class FeaturePruner(BaseEstimator, TransformerMixin):
    """Drops a fixed, pre-determined list of features.
    Works with both DataFrames and numpy arrays."""

    def __init__(self, features_to_remove: Optional[List[str]] = None):
        self.features_to_remove = features_to_remove or []
        self._col_names: List[str] = []

    def fit(self, X, y=None):
        if hasattr(X, 'columns'):
            self._col_names = list(X.columns)
        return self

    def transform(self, X):
        if hasattr(X, 'columns'):
            to_drop = [c for c in self.features_to_remove if c in X.columns]
            return X.drop(columns=to_drop)
        # numpy array — use stored column names
        if self._col_names:
            drop_indices = [self._col_names.index(c) for c in self.features_to_remove if c in self._col_names]
            keep_indices = [i for i in range(X.shape[1]) if i not in drop_indices]
            return X[:, keep_indices]
        return X

    def get_feature_names_out(self, input_features=None):
        base = list(input_features) if input_features is not None else self._col_names
        return np.array([f for f in base if f not in self.features_to_remove], dtype=object)


# ─────────────────────────────────────────────────────────────────────────────
# AGENT CORE  (Steps 1–10)
# ─────────────────────────────────────────────────────────────────────────────

class IntelligentPreprocessingAgent:
    """
    Production-grade preprocessing agent for Prism AI.

    Execution order (Step 10):
        Audit → Calibrate → Target Detection → Problem Type →
        Split → Preprocess (Pipeline fit) → Feature Selection →
        Pipeline Save → Output

    Every phase is wrapped in try/except (Step 9) with fallback logic
    and emits structured logs (Step 8).
    """

    # ── constants ──
    DEFAULT_SKEW_FLOOR = 0.5
    DEFAULT_SKEW_CEIL = 2.0
    SMALL_DATASET_THRESHOLD = 100
    SMALL_DATASET_MISSING_LIMIT = 0.4
    MAX_REMOVAL_RATIO = 0.7
    CORRELATION_THRESHOLD = 0.9
    RF_SEEDS = [42, 7, 21]
    RF_N_ESTIMATORS = 50
    RF_MAX_DEPTH = 5
    HEURISTIC_TARGET_NAMES = ["target", "label", "y", "output", "class"]
    ID_UNIQUENESS_THRESHOLD = 0.95

    def __init__(self):
        # thresholds (Step 1)
        self.threshold_missing: float = self.SMALL_DATASET_MISSING_LIMIT
        self.threshold_skew: float = self.DEFAULT_SKEW_FLOOR

        # target/problem (Step 2)
        self.target_column: Optional[str] = None
        self.problem_type: str = "unsupervised"
        self.mode: str = "unsupervised"

        # logging containers (Step 8)
        self.audit_results: Dict[str, Any] = {}
        self.calibration_log: Dict[str, Any] = {}
        self.target_detection_log: Dict[str, Any] = {}
        self.target_analysis_log: Dict[str, Any] = {}
        self.feature_selection_log: Dict[str, Any] = {}
        self.steps_applied: List[Dict[str, Any]] = []
        self.decision_trace: List[Dict[str, Any]] = []
        self.warnings: List[str] = []

        # pipeline (Step 4)
        self.pipeline: Optional[Pipeline] = None
        self._feature_order: List[str] = []
        self._output_columns: List[str] = []

    # ──────────────────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        df: pd.DataFrame,
        metadata_target: Optional[str] = None,
        # legacy alias
        target_col: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Train-time entry point.  Returns (processed_df, summary_dict)."""
        # accept either kwarg name
        metadata_target = metadata_target or target_col

        self.audit_results = {}
        self.calibration_log = {}
        self.target_detection_log = {}
        self.target_analysis_log = {}
        self.feature_selection_log = {}
        self.steps_applied = []
        self.decision_trace = []
        self.warnings = []

        # ── Step 10: strict sequential execution ──

        # Phase 0  – Duplicate removal (row-level, pre-audit)
        df = self._step_safe("duplicate_removal", self._remove_duplicates, df)

        # Phase 1  – Type Conversion & Normalization
        df = self._step_safe("type_conversion", self._convert_types, df)

        # Phase 2  – Audit
        df = self._step_safe("audit", self._run_audit, df)

        # Phase 3  – Calibrate thresholds (Step 1)
        df = self._step_safe("calibration", self._calibrate_thresholds, df)

        # Phase 4  – Target detection (Step 2)
        df = self._step_safe("target_detection",
                             lambda d: self._detect_target(d, metadata_target), df)

        # Phase 5  – Problem type detection (Step 2)
        df = self._step_safe("problem_detection", self._detect_problem, df)

        # Phase 6  – Intelligent Cleaning (Missing ratio check, inconsistencies)
        df = self._step_safe("data_cleaning", self._clean_data, df)

        # Phase 7  – Feature Engineering (Temporal etc.)
        df = self._step_safe("feature_engineering", self._engineer_features, df)

        # Phase 8  – Internal train/test split (zero leakage)
        X, y, X_train, X_test, y_train, y_test = self._split_data(df)

        # Phase 9  – Build & fit sklearn Pipeline (Steps 1+4)
        self._step_safe("pipeline_build",
                        lambda _: self._build_and_fit_pipeline(X_train, y_train), df)

        # Phase 10 – Feature selection (Step 3)
        self._step_safe("feature_selection",
                        lambda _: self._run_feature_selection(X_train, y_train), df)

        # Phase 8  – Final transform on full X
        df_final = self._pipeline_transform(X)
        if self.target_column and self.target_column in df.columns:
            df_final[self.target_column] = df[self.target_column].values

        self._feature_order = [c for c in df_final.columns if c != self.target_column]

        return df_final, self._build_summary(df, df_final)

    # ── Inference entry point ──

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply fitted pipeline to new data (no refitting)."""
        if self.pipeline is None:
            raise RuntimeError("No fitted pipeline.  Call run_pipeline() or load_pipeline() first.")
        X = df.drop(columns=[self.target_column], errors="ignore") if self.target_column else df
        return self._pipeline_transform(X)

    # ── Persistence (Step 4) ──

    def save_pipeline(self, path: str) -> str:
        """Serialize the fitted pipeline + metadata to disk via joblib."""
        if self.pipeline is None:
            raise RuntimeError("Pipeline not fitted yet.")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            "pipeline": self.pipeline,
            "metadata": {
                "target_column": self.target_column,
                "problem_type": self.problem_type,
                "mode": self.mode,
                "feature_order": self._feature_order,
                "output_columns": getattr(self, '_output_columns', []),
            },
        }
        joblib.dump(payload, path)
        self._log_step("pipeline_save", True,
                       f"Pipeline serialized to {os.path.basename(path)}",
                       {"path": path, "steps": [s[0] for s in self.pipeline.steps],
                        "feature_order": self._feature_order})
        logger.info(f"💾 Pipeline saved → {path}")
        return path

    @classmethod
    def load_pipeline(cls, path: str) -> "IntelligentPreprocessingAgent":
        """Reconstruct an agent from a saved artifact (inference-only)."""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Pipeline artifact not found: {path}")
        payload = joblib.load(path)
        agent = cls()
        agent.pipeline = payload["pipeline"]
        meta = payload["metadata"]
        agent.target_column = meta["target_column"]
        agent.problem_type = meta["problem_type"]
        agent.mode = meta["mode"]
        agent._feature_order = meta.get("feature_order", [])
        agent._output_columns = meta.get("output_columns", [])
        logger.info(f"📦 Pipeline loaded ← {path}")
        return agent

    # ── Data Drift Detection (Step 6) ──

    def detect_drift(self, df_new: pd.DataFrame, baseline_stats: Optional[Dict] = None) -> Dict[str, Any]:
        """Compare new data to stored baseline statistics.
        If *baseline_stats* is ``None`` the audit_results from the last
        ``run_pipeline`` call are used as baseline."""
        if baseline_stats is None:
            baseline_stats = self.audit_results.get("numeric_stats", {})
        if not baseline_stats:
            return {"drift_detected": False, "drift_columns": [], "reason": "no baseline"}

        drift_cols = []
        for col, base in baseline_stats.items():
            if col not in df_new.columns:
                continue
            new_col = df_new[col].dropna()
            if len(new_col) < 8:
                continue
            new_mean = float(new_col.mean())
            new_std = float(new_col.std())
            base_mean = base.get("mean", new_mean)
            base_std = base.get("std", new_std)
            # Simple Z-score drift check: if mean shifted > 2 base stds
            if base_std > 0 and abs(new_mean - base_mean) / base_std > 2.0:
                drift_cols.append(col)

        return {
            "drift_detected": len(drift_cols) > 0,
            "drift_columns": drift_cols,
        }

    # ── EDA Visualisation (Step 7) ──

    def generate_eda(self, df: pd.DataFrame, output_dir: str) -> Dict[str, str]:
        """Create lightweight EDA charts and save to *output_dir*.
        Returns dict of chart name → file path."""
        paths: Dict[str, str] = {}
        os.makedirs(output_dir, exist_ok=True)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            # 1. Missing-value heatmap
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.imshow(df.isnull().values.T, aspect="auto", cmap="Reds", interpolation="nearest")
            ax.set_yticks(range(len(df.columns)))
            ax.set_yticklabels(df.columns, fontsize=7)
            ax.set_title("Missing Values Heatmap")
            p = os.path.join(output_dir, "missing_heatmap.png")
            fig.savefig(p, bbox_inches="tight", dpi=100)
            plt.close(fig)
            paths["missing_heatmap"] = p

            # 2. Correlation heatmap (numeric only)
            num_df = df.select_dtypes(include=[np.number])
            if num_df.shape[1] >= 2:
                corr = num_df.corr()
                fig, ax = plt.subplots(figsize=(8, 6))
                cax = ax.matshow(corr, cmap="coolwarm", vmin=-1, vmax=1)
                fig.colorbar(cax)
                ax.set_xticks(range(len(corr.columns)))
                ax.set_xticklabels(corr.columns, rotation=90, fontsize=7)
                ax.set_yticks(range(len(corr.columns)))
                ax.set_yticklabels(corr.columns, fontsize=7)
                ax.set_title("Correlation Heatmap")
                p = os.path.join(output_dir, "correlation_heatmap.png")
                fig.savefig(p, bbox_inches="tight", dpi=100)
                plt.close(fig)
                paths["correlation_heatmap"] = p

            # 3. Distribution plots (top 8 numeric)
            for col in num_df.columns[:8]:
                fig, ax = plt.subplots(figsize=(5, 3))
                ax.hist(num_df[col].dropna(), bins=30, edgecolor="black", alpha=0.7)
                ax.set_title(f"Distribution: {col}")
                p = os.path.join(output_dir, f"dist_{col}.png")
                fig.savefig(p, bbox_inches="tight", dpi=80)
                plt.close(fig)
                paths[f"dist_{col}"] = p

        except ImportError:
            logger.warning("matplotlib not installed — skipping EDA charts")
        except Exception as exc:
            logger.warning(f"EDA generation failed: {exc}")

        return paths

    # ──────────────────────────────────────────────────────────────────────
    #  INTERNAL PHASES  (private)
    # ──────────────────────────────────────────────────────────────────────

    # ── Step 9: safe executor ──

    def _step_safe(self, step_name: str, fn, df: pd.DataFrame) -> pd.DataFrame:
        """Execute *fn(df)*, catch exceptions, log, and continue."""
        try:
            result = fn(df)
            return result if isinstance(result, pd.DataFrame) else df
        except Exception as exc:
            msg = f"Step '{step_name}' failed: {exc}"
            logger.error(msg)
            self.warnings.append(msg)
            self._log_step(step_name, False, msg)
            return df

    # ── Phase 0: Duplicates ──

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates().reset_index(drop=True)
        removed = before - len(df)
        self._log_step("duplicate_removal", removed > 0,
                       f"Removed {removed} duplicate rows" if removed else "No duplicates found",
                       {"before_rows": before, "after_rows": len(df)})
        return df

    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert strings to numeric or datetime where appropriate."""
        converted = []
        for col in df.columns:
            # Skip if it's the target for now (it may have special logic)
            if col == self.target_column: continue

            if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                # Try numeric conversion
                try:
                    df[col] = pd.to_numeric(df[col])
                    converted.append(f"{col}(numeric)")
                    continue
                except: pass

                # Try datetime conversion for likely date columns
                try:
                    lower_col = col.lower()
                    if any(k in lower_col for k in ["date", "time", "timestamp", "year", "month"]):
                        df[col] = pd.to_datetime(df[col])
                        converted.append(f"{col}(datetime)")
                except: pass
        
        self._log_step("type_conversion", len(converted) > 0,
                       f"Converted types for {len(converted)} columns" if converted else "Fixed types maintained",
                       {"converted": converted})
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """In-place normalization, invalid value correction, and forward-fill fallback."""
        applied = []
        
        # Categorical normalization (whitespace/casing)
        obj_cols = df.select_dtypes(include=['object', 'string']).columns
        normalized_count = 0
        for col in obj_cols:
            if col == self.target_column: continue
            df[col] = df[col].astype(str).str.strip().str.lower()
            normalized_count += 1
        if normalized_count > 0:
            applied.append(f"categorical_normalization({normalized_count} columns)")

        # Invalid value handling: Age heuristic
        possible_age_cols = [c for c in df.columns if c.lower() in ["age", "years_old"]]
        for col in possible_age_cols:
            invalid_mask = (df[col] < 0) | (df[col] > 120)
            if invalid_mask.any():
                df.loc[invalid_mask, col] = np.nan
                applied.append(f"age_out_of_range_corrected({invalid_mask.sum()})")

        # Time-series aware missing value fill (fallback if audit shows pattern)
        # We look for a datetime index or a single date column
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) == 1 or isinstance(df.index, pd.DatetimeIndex):
            # Sort by time for fill
            if len(date_cols) == 1:
                df = df.sort_values(by=date_cols[0]).reset_index(drop=True)
            df = df.ffill().bfill()
            applied.append("time_series_aware_interpolation")

        self._log_step("data_cleaning", len(applied) > 0,
                       f"Cleanup applied: {', '.join(applied)}" if applied else "No inconsistent data detected",
                       {"steps": applied})
        return df

    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Automatic feature engineering (temporal decomposition)."""
        derived = []
        date_cols = df.select_dtypes(include=['datetime64']).columns
        for col in date_cols:
            # Standard decomposition
            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_day"] = df[col].dt.day
            df[f"{col}_dayofweek"] = df[col].dt.dayofweek
            
            # Additional logic: is_weekend
            df[f"{col}_is_weekend"] = df[col].dt.dayofweek.isin([5, 6]).astype(int)
            
            derived.extend([f"{col}_year", f"{col}_month", f"{col}_day", f"{col}_dayofweek", f"{col}_is_weekend"])
            # Remove original (to avoid leakage in specific models or duplication)
            df = df.drop(columns=[col])
        
        self._log_step("feature_engineering", len(derived) > 0,
                       f"Engineered {len(derived)} temporal features" if derived else "No feature engineering required",
                       {"new_features": derived})
        return df

    # ── Phase 1: Audit ──

    def _run_audit(self, df: pd.DataFrame) -> pd.DataFrame:
        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_stats: Dict[str, Dict[str, Any]] = {}
        for col in num_cols:
            data = df[col].dropna()
            skew_val = float(scipy_stats.skew(data)) if len(data) > 2 else 0.0
            if math.isnan(skew_val):
                skew_val = 0.0
            numeric_stats[col] = {
                "mean": float(data.mean()) if len(data) else 0.0,
                "std": float(data.std()) if len(data) > 1 else 0.0,
                "min": float(data.min()) if len(data) else 0.0,
                "max": float(data.max()) if len(data) else 0.0,
                "skew": skew_val,
                "outliers_count": int(self._count_outliers(data)),
            }

        self.audit_results = {
            "rows": len(df),
            "cols": len(df.columns),
            "missing_pct": {c: float(df[c].isnull().sum() / len(df)) for c in df.columns},
            "duplicates": 0,  # already removed
            "numeric_stats": numeric_stats,
        }
        self._log_step("audit", True, "Dataset audit complete",
                       {"rows": len(df), "cols": len(df.columns),
                        "numeric_features": len(num_cols),
                        "categorical_features": len(df.columns) - len(num_cols)})
        return df

    # ── Phase 2: Calibrate (Step 1) ──

    def _calibrate_thresholds(self, df: pd.DataFrame) -> pd.DataFrame:
        # Skew threshold
        skews = [abs(s["skew"]) for s in self.audit_results["numeric_stats"].values()
                 if not math.isnan(s["skew"])]
        self.threshold_skew = (
            float(np.clip(np.percentile(skews, 75), self.DEFAULT_SKEW_FLOOR, self.DEFAULT_SKEW_CEIL))
            if skews else self.DEFAULT_SKEW_FLOOR
        )

        # Missing threshold — data-size aware
        n = self.audit_results["rows"]
        if n < self.SMALL_DATASET_THRESHOLD:
            self.threshold_missing = self.SMALL_DATASET_MISSING_LIMIT  # 0.4
        else:
            self.threshold_missing = min(0.5, max(0.1, 1.0 / math.log10(n + 10)))

        self.calibration_log = {
            "num_rows": n,
            "threshold_skew": self.threshold_skew,
            "threshold_missing": round(self.threshold_missing, 4),
        }
        self._log_step("calibration", True,
                       f"Thresholds calibrated: skew={self.threshold_skew:.2f}, "
                       f"missing={self.threshold_missing:.4f}",
                       self.calibration_log)
        return df

    # ── Phase 3: Target detection (Step 2) ──

    def _detect_target(self, df: pd.DataFrame, metadata_target: Optional[str]) -> pd.DataFrame:
        method = "none"
        confidence = "low"
        selected = None

        # Priority 1: explicit metadata
        if metadata_target and metadata_target in df.columns:
            selected = metadata_target
            method = "metadata"
            confidence = "high"

        # Priority 2: heuristic name matching
        if selected is None:
            excluded = self._columns_to_exclude(df)
            candidates = [c for c in df.columns if c not in excluded]
            for h in self.HEURISTIC_TARGET_NAMES:
                for col in candidates:
                    if col.lower() == h:
                        selected = col
                        method = "heuristic"
                        confidence = "medium"
                        break
                if selected:
                    break

        # Priority 3: fallback to last eligible column
        if selected is None:
            excluded = self._columns_to_exclude(df)
            eligible = [c for c in df.columns if c not in excluded]
            if eligible:
                selected = eligible[-1]
                method = "fallback"
                confidence = "low"

        self.target_column = selected
        self.mode = "supervised" if selected else "unsupervised"
        self.target_detection_log = {
            "selected_column": selected,
            "method": method,
            "confidence": confidence,
        }
        self._log_step("target_detection", selected is not None,
                       f"Target='{selected}' via {method} (confidence={confidence})",
                       self.target_detection_log)

        if selected is None or confidence == "low":
            self.warnings.append("No target column detected. Consider specifying a target column for supervised learning.")

        return df

    def _columns_to_exclude(self, df: pd.DataFrame) -> set:
        """Return columns that should NOT be considered as targets."""
        excluded = set()
        for col in df.columns:
            # ID-like: ≥ 95% unique values
            if df[col].nunique() >= self.ID_UNIQUENESS_THRESHOLD * len(df) and len(df) > 1:
                excluded.add(col)
            # Constant columns
            if df[col].nunique() <= 1:
                excluded.add(col)
            # Timestamp / datetime
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                excluded.add(col)
            # Name contains "id" or "index" (case-insensitive)
            if col.lower() in ("id", "index", "row_id", "row_number"):
                excluded.add(col)
        return excluded

    # ── Phase 4: Problem type (Step 2) ──

    def _detect_problem(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.target_column:
            self.problem_type = "unsupervised"
            self.target_analysis_log = {"problem_type": "unsupervised", "reason": "no target"}
        else:
            col = df[self.target_column]
            nunique = col.nunique()
            dtype = col.dtype
            n = len(df)
            is_categorical = dtype == "object" or pd.api.types.is_string_dtype(dtype)
            adaptive_limit = min(20, max(2, int(0.05 * n)))
            is_low_unique = nunique <= adaptive_limit

            if is_categorical or is_low_unique:
                self.problem_type = "classification"
                reason = "categorical dtype" if is_categorical else f"low cardinality ({nunique} ≤ {adaptive_limit})"
            else:
                self.problem_type = "regression"
                reason = f"high cardinality ({nunique} > {adaptive_limit})"

            # Imbalance detection (classification only)
            imbalance_info = {}
            if self.problem_type == "classification":
                vc = col.value_counts(normalize=True)
                minority_ratio = float(vc.min())
                if minority_ratio < 0.15:
                    imbalance_info = {
                        "imbalanced": True,
                        "minority_class": str(vc.idxmin()),
                        "minority_ratio": round(minority_ratio, 4),
                    }
                    self._log_step("imbalance_handling", True,
                                   f"Class imbalance detected (minority={minority_ratio:.2%})",
                                   imbalance_info)
                else:
                    imbalance_info = {"imbalanced": False}

            self.target_analysis_log = {
                "problem_type": self.problem_type,
                "reason": reason,
                "nunique": int(nunique),
                **imbalance_info,
            }

        self._log_step("problem_detection", True,
                       f"Problem type: {self.problem_type}",
                       self.target_analysis_log)
        
        # Phase 5.5 - Imbalance Recommendation
        if self.problem_type == "classification" and self.target_analysis_log.get("imbalanced"):
            strategy = "SMOTE" if SMOTE_AVAILABLE else "class_weight='balanced'"
            self._log_step("imbalance_strategy", True,
                           f"Strategy: Use {strategy} during training",
                           {"strategy": strategy, "smote_ready": SMOTE_AVAILABLE})
        else:
            self._log_step("imbalance_strategy", False, "No significant imbalance detected")
            
        return df

    # ── Phase 5: Split ──

    def _split_data(self, df: pd.DataFrame):
        X = df.drop(columns=[self.target_column]) if self.target_column else df.copy()
        y = df[self.target_column].copy() if self.target_column else None
        
        if self.mode == "supervised" and len(df) > 10:
            # Stratify if classification to prevent leakage/imbalance issues in split
            stratify = y if self.problem_type == "classification" else None
            try:
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.2, random_state=42, stratify=stratify
                )
            except Exception:
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.2, random_state=42
                )
        else:
            X_train, X_test = X.copy(), X.copy()
            y_train = y.copy() if y is not None else None
            y_test = y.copy() if y is not None else None
        
        return X, y, X_train, X_test, y_train, y_test

    # ── Phase 6: Build & fit pipeline (Steps 1 + 4) ──

    def _build_and_fit_pipeline(self, X_train: pd.DataFrame, y_train) -> None:
        missing = self.audit_results.get("missing_pct", {})
        num_stats = self.audit_results.get("numeric_stats", {})

        # columns to drop entirely (missing > threshold)
        drop_cols = [c for c, pct in missing.items()
                     if pct > self.threshold_missing and c != self.target_column]

        all_initial = list(X_train.columns)
        after_drop = [c for c in all_initial if c not in drop_cols]
        num_cols = [c for c in after_drop if c in num_stats]
        cat_cols = [c for c in after_drop if c not in num_stats]

        # ── Scaler Selection (Intelligent) ──
        # Heuristic: If >50% of numeric columns are already scaled or have bounded non-normal distributions, use MinMaxScaler.
        small_range_count = 0
        for col in num_cols:
            stats = num_stats.get(col, {})
            if abs(stats.get("max", 0) - stats.get("min", 0)) <= 20:
                small_range_count += 1
        
        use_min_max = small_range_count > (len(num_cols) / 2) if num_cols else False
        scaler = MinMaxScaler() if use_min_max else StandardScaler()
        scaler_name = "MinMaxScaler" if use_min_max else "StandardScaler"

        # ── Numeric sub-pipeline ──
        num_steps: List[Tuple[str, Any]] = [("imputer", SimpleImputer(strategy="median"))]
        outlier_cols = [c for c in num_cols if num_stats.get(c, {}).get("outliers_count", 0) > 0]
        if outlier_cols:
            num_steps.append(("outlier_capper", OutlierCapper(outlier_cols)))
        
        skew_cols = [c for c in num_cols if abs(num_stats.get(c, {}).get("skew", 0)) > self.threshold_skew]
        if skew_cols:
            num_steps.append(("skew_corrector", SkewCorrector(skew_cols)))
        
        num_steps.append(("scaler", scaler))

        # ── Categorical sub-pipeline ──
        cat_pipe = Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ])

        # ── Column transformer ──
        preprocessor = ColumnTransformer(
            transformers=[
                ("num", Pipeline(num_steps), num_cols),
                ("cat", cat_pipe, cat_cols),
            ],
            remainder="passthrough",
            verbose_feature_names_out=False,
        )

        # ── Outer pipeline ──
        self.pipeline = Pipeline([
            ("aligner", FeatureAligner(all_initial)),
            ("initial_drop", FeaturePruner(drop_cols)),
            ("preprocess", preprocessor),
        ])
        
        # FIT ONLY ON TRAINING (Leakage Prevention)
        self.pipeline.fit(X_train, y_train)

        # Derive output column names manually (sklearn 1.3 compat)
        out_cols = list(num_cols)  # numeric columns pass through with same names
        if cat_cols:
            try:
                ohe = preprocessor.named_transformers_["cat"].named_steps["encoder"]
                out_cols += list(ohe.get_feature_names_out(cat_cols))
            except Exception:
                out_cols += cat_cols
        self._output_columns = out_cols

        self._log_step("pipeline_build", True,
                       f"sklearn Pipeline assembled: {scaler_name} selected based on range analysis",
                       {"dropped_cols": drop_cols, "num_cols": num_cols,
                        "cat_cols": cat_cols, "scaler_used": scaler_name,
                        "leakage_safe": True})

    # ── Phase 7: Feature selection (Step 3) ──

    def _run_feature_selection(self, X_train: pd.DataFrame, y_train) -> None:
        X_clean = self._pipeline_transform(X_train)
        feats = list(X_clean.columns)
        n_initial = len(feats)
        if n_initial < 2:
            self.feature_selection_log = {
                "skipped": True, 
                "reason": "too few features",
                "pruning_skipped_reason": "low_feature_count"
            }
            return

        # 3-A: Correlation clustering (Spearman)
        corr = X_clean[feats].corr(method="spearman").abs()
        clusters: List[List[str]] = []
        visited: set = set()
        for col in feats:
            if col in visited:
                continue
            cluster = [c for c in feats if c not in visited and corr.loc[col, c] > self.CORRELATION_THRESHOLD]
            if not cluster:
                cluster = [col]
            clusters.append(cluster)
            visited.update(cluster)

        # 3-B: Importance scoring (multi-seed RF | MI fallback)
        importance_scores: Dict[str, float] = {}
        if self.mode == "supervised" and y_train is not None and len(X_clean) > 10:
            # Align y_train index with X_clean (pipeline may have dropped rows)
            y_aligned = y_train.loc[X_clean.index] if hasattr(y_train, "loc") else y_train

            try:
                all_importances = []
                for seed in self.RF_SEEDS:
                    rf = (RandomForestClassifier if self.problem_type == "classification"
                          else RandomForestRegressor)(
                        n_estimators=self.RF_N_ESTIMATORS,
                        max_depth=self.RF_MAX_DEPTH,
                        random_state=seed,
                    )
                    rf.fit(X_clean[feats], y_aligned)
                    all_importances.append(rf.feature_importances_)
                avg_imp = np.mean(all_importances, axis=0)
                importance_scores = {f: float(v) for f, v in zip(feats, avg_imp)}
            except Exception:
                # MI fallback
                try:
                    mi_fn = mutual_info_classif if self.problem_type == "classification" else mutual_info_regression
                    mi = mi_fn(X_clean[feats], y_aligned, random_state=42)
                    importance_scores = {f: float(v) for f, v in zip(feats, mi)}
                except Exception:
                    importance_scores = {f: 1.0 / n_initial for f in feats}
        else:
            importance_scores = {f: 1.0 / n_initial for f in feats}

        # 3-C: Cluster-based redundancy removal (keep highest importance in each cluster)
        redundant: List[str] = []
        for cluster in clusters:
            if len(cluster) > 1:
                best = max(cluster, key=lambda c: importance_scores.get(c, 0))
                redundant.extend([c for c in cluster if c != best])

        # 3-D: Low-importance pruning
        imp_threshold = max(float(np.percentile(list(importance_scores.values()), 20)), 0.01)
        low_signal = [f for f, s in importance_scores.items()
                      if s < imp_threshold and f not in redundant]

        # 3-E: Merged candidate list
        all_candidates = sorted(
            list(set(redundant + low_signal)),
            key=lambda f: importance_scores.get(f, 0),
        )

        # 3-F: Safeguards
        min_features = max(2, int(math.sqrt(n_initial)))
        max_to_remove = int(n_initial * self.MAX_REMOVAL_RATIO)

        final_removals: List[str] = []
        for feat in all_candidates:
            remaining = n_initial - len(final_removals) - 1
            if remaining >= min_features and len(final_removals) < max_to_remove:
                final_removals.append(feat)
            else:
                break

        # 3-G: Build per-feature decision trace
        feature_trace: List[Dict[str, Any]] = []
        for f in feats:
            entry: Dict[str, Any] = {"feature": f, "importance": round(importance_scores.get(f, 0), 6)}
            if f in final_removals:
                if f in redundant:
                    entry.update({"status": "removed", "reason": "redundancy (high correlation cluster)"})
                else:
                    entry.update({"status": "removed", "reason": f"low importance ({importance_scores[f]:.4f} < {imp_threshold:.4f})"})
            else:
                entry.update({"status": "kept", "reason": "above threshold"})
            feature_trace.append(entry)

        kept_features = [f for f in feats if f not in final_removals]
        
        # Determine why pruning was skipped (observability fix)
        skipped_reason = "none"
        if not final_removals:
            if n_initial < 2:
                skipped_reason = "low_feature_count"
            elif self.mode == "unsupervised":
                skipped_reason = "unsupervised_mode"
            elif not redundant and not [c for c in clusters if len(c) > 1]:
                skipped_reason = "no_high_correlation"
            elif not low_signal:
                skipped_reason = "importance_below_threshold"
            elif all_candidates:
                skipped_reason = "safeguard_triggered"

        self.feature_selection_log = {
            "initial_features": feats,
            "kept_features": kept_features,
            "removed_features": final_removals,
            "pruning_skipped_reason": skipped_reason,
            "importance_scores": {k: round(v, 6) for k, v in importance_scores.items()},
            "importance_threshold": round(imp_threshold, 6),
            "correlation_clusters": [c for c in clusters if len(c) > 1],
            "decision_trace": feature_trace,
            "min_features_guard": min_features,
            "max_removal_cap": max_to_remove,
        }

        # Append pruner to pipeline
        if final_removals:
            self.pipeline.steps.append(("final_pruner", FeaturePruner(final_removals)))

        self._log_step("feature_selection", len(final_removals) > 0,
                       f"Pruned {len(final_removals)}/{n_initial} features "
                       f"(kept {len(kept_features)})",
                       {"removed": final_removals, "importance_threshold": imp_threshold})

    # ──────────────────────────────────────────────────────────────────────
    #  HELPERS
    # ──────────────────────────────────────────────────────────────────────

    def _count_outliers(self, data: pd.Series) -> int:
        if len(data) < 4:
            return 0
        q1, q3 = data.quantile(0.25), data.quantile(0.75)
        iqr = q3 - q1
        if iqr <= 0:
            return 0
        return int(((data < (q1 - 1.5 * iqr)) | (data > (q3 + 1.5 * iqr))).sum())

    def _pipeline_transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform via pipeline and wrap numpy output into a proper DataFrame.
        Derives column names from the stored _output_columns computed during build."""
        raw = self.pipeline.transform(X)
        if isinstance(raw, pd.DataFrame):
            return raw
        # numpy array — use our pre-computed column names
        cols = getattr(self, '_output_columns', None)
        if cols is None or len(cols) != raw.shape[1]:
            cols = [f"f{i}" for i in range(raw.shape[1])]
        return pd.DataFrame(raw, columns=cols, index=X.index)

    def _log_step(self, step: str, applied: bool, reason: str,
                  details: Optional[Dict[str, Any]] = None) -> None:
        """Step 8: Enhanced structured log entry."""
        entry = {"step": step, "applied": applied, "reason": reason}
        if details:
            entry["details"] = details
        self.steps_applied.append(entry)
        self.decision_trace.append(entry)
        level = logging.INFO if applied else logging.WARNING
        logger.log(level, f"PREPROCESS [{step}] applied={applied} | {reason}")

    def _build_summary(self, df_before: pd.DataFrame, df_after: pd.DataFrame) -> Dict[str, Any]:
        """Step 8: Comprehensive output summary."""
        return {
            "mode": self.mode,
            "target_column": self.target_column,
            "problem_type": self.problem_type,
            "calibration": self.calibration_log,
            "intelligence": {
                "target_detection": self.target_detection_log,
                "target_analysis": self.target_analysis_log,
            },
            "feature_selection": self.feature_selection_log,
            "steps_applied": self.steps_applied,
            "decision_trace": self.decision_trace,
            "warnings": self.warnings,
            "before_shape": list(df_before.shape),
            "final_shape": list(df_after.shape),
            "pipeline_steps": [s[0] for s in self.pipeline.steps] if self.pipeline else [],
        }

    # ── Format Standardization (Step 5) ──
    # NOTE: This is invoked from the Celery task, not inside run_pipeline.

    @staticmethod
    def standardize_format(input_path: str, output_path: str) -> Dict[str, Any]:
        """Convert unsupported formats to CSV.  Passthrough CSV/Parquet."""
        ext = os.path.splitext(input_path)[1].lower()
        result = {"original_format": ext, "converted": False}

        if ext in (".csv", ".parquet"):
            result["action"] = "passthrough"
            if input_path != output_path:
                import shutil
                shutil.copy2(input_path, output_path)
            return result

        df = None
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(input_path)
            result["action"] = "xlsx_to_csv"
        elif ext == ".json":
            df = pd.read_json(input_path)
            result["action"] = "json_to_csv"
        else:
            raise ValueError(f"Unsupported format: {ext}")

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        result["converted"] = True
        result["rows"] = len(df)
        result["cols"] = len(df.columns)
        return result
