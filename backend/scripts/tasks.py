import os
import sys
import pandas as pd
import logging
from typing import Dict, Any, List
import uuid
from .celery_app import celery_app

# Add parent directory for service imports
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from services.ml_pipeline import MLPipeline, AlgorithmType, TrainingConfig
from services.preprocessing import PreprocessingPipeline
from services.mlflow_utils import mlflow_manager
from models.dataset import Dataset
import mlflow
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

logger = logging.getLogger(__name__)

# Reusable Algorithm mapping for tasks
ALGO_MAP = {
    # Handled via string-to-enum conversion at the task level
}

@celery_app.task(bind=True, name="scripts.tasks.train_single_model")
def train_single_model(
    self, 
    dataset_id_str: str, 
    user_id_str: str,
    target_column: str,
    algorithm_name: str,
    hyperparameters: Dict[str, Any] = None,
    cv_folds: int = 5
) -> Dict[str, Any]:
    """
    Background Task: Train a single machine learning model on a versioned dataset artifact.
    """
    from services.storage_service import storage_service
    from services.preprocessing_service import PreprocessingService
    from core.database import SessionLocal
    import uuid
    
    self.update_state(state="PROGRESS", meta={"status": "Starting training", "progress": 5})
    
    db = SessionLocal()
    try:
        dataset_id = uuid.UUID(dataset_id_str)
        user_id = uuid.UUID(user_id_str)
        celery_job_id = self.request.id
        
        # 0. Fetch metadata (Requirement: Get version and metadata for naming)
        ds = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
        if not ds:
            raise ValueError("Dataset not found or access denied")
            
        # 1. Setup MLflow multi-tenant context
        experiment_id = mlflow_manager.set_active_experiment(user_id_str)
        
        # 2. Start Run (Requirement: Improved Run Naming for Traceability)
        run_name = f"{algorithm_name}_{dataset_id_str[:8]}_v{ds.version}"
        with mlflow.start_run(run_name=run_name):
            # 3. Load data via StorageService
            df = storage_service.read_df(ds.storage_path)
            
            # 4. Separate Train/Test using is_train indicator (Requirement: Zero Leakage)
            if 'is_train' in df.columns:
                train_df = df[df['is_train'] == 1].copy()
                test_df = df[df['is_train'] == 0].copy()
            else:
                from sklearn.model_selection import train_test_split
                train_df, test_df = train_test_split(df.copy(), test_size=0.2, random_state=42)
            
            # Separate X and y
            cols_to_drop = [target_column, 'is_train', 'sample_weight']
            X_train = train_df.drop(columns=[c for c in cols_to_drop if c in train_df.columns])
            y_train = train_df[target_column]
            
            X_test = test_df.drop(columns=[c for c in cols_to_drop if c in test_df.columns])
            y_test = test_df[target_column]
            
            sample_weight = train_df.get('sample_weight')

            # 5. Training Context Logging (Requirement: Metadata for Reproducibility)
            mlflow.log_param("train_size", len(X_train))
            mlflow.log_param("test_size", len(X_test))
            mlflow.log_param("num_features", X_train.shape[1])
            
            # 6. Train Model
            self.update_state(state="PROGRESS", meta={"status": f"Training {algorithm_name}...", "progress": 30})
            pipeline = MLPipeline()
            algo_type = AlgorithmType(algorithm_name)
            
            model = pipeline._create_model(algo_type, hyperparameters)
            model.fit(X_train, y_train, sample_weight=sample_weight)
            
            # 7. Evaluate Model
            y_pred = model.predict(X_test)
            is_clf = algorithm_name in [a.value for a in [AlgorithmType.LOGISTIC_REGRESSION, AlgorithmType.RANDOM_FOREST_CLF, AlgorithmType.SVM_CLF, AlgorithmType.XGBOOST_CLF]]
            
            metrics = {}
            if is_clf:
                metrics["accuracy"] = float(accuracy_score(y_test, y_pred))
                metrics["precision"] = float(precision_score(y_test, y_pred, average="weighted", zero_division=0))
                metrics["recall"] = float(recall_score(y_test, y_pred, average="weighted", zero_division=0))
                metrics["f1_score"] = float(f1_score(y_test, y_pred, average="weighted", zero_division=0))
                mlflow.log_param("task_type", "classification")
            else:
                from sklearn.metrics import mean_squared_error, r2_score
                metrics["mse"] = float(mean_squared_error(y_test, y_pred))
                metrics["r2"] = float(r2_score(y_test, y_pred))
                mlflow.log_param("task_type", "regression")

            # 8. Metric Safety (Requirement: Float conversion, No None/NaN)
            import math
            metrics = {k: float(v) for k, v in metrics.items() if v is not None and not (isinstance(v, float) and math.isnan(v))}
            for m_name, m_val in metrics.items():
                mlflow.log_metric(m_name, m_val)

            # 9. Log Standardized Context (Requirement: Parameters & Tags)
            mlflow.log_param("algorithm", algorithm_name)
            mlflow.log_param("dataset_id", dataset_id_str)
            mlflow.log_param("version", ds.version)
            if hyperparameters: mlflow.log_params(hyperparameters)

            mlflow.set_tag("user_id", user_id_str)
            mlflow.set_tag("dataset_id", dataset_id_str)
            mlflow.set_tag("version", str(ds.version))
            mlflow.set_tag("job_id", celery_job_id)

            # 10. Log Model
            mlflow.sklearn.log_model(model, "model")
            
            self.update_state(state="SUCCESS", meta={"status": "Completed", "progress": 100})
            return {
                "algorithm": algorithm_name,
                "metrics": metrics,
                "success": True,
                "dataset_id": dataset_id_str,
                "run_id": mlflow.active_run().info.run_id
            }
            
    except Exception as e:
        logger.error(f"Training Task Error: {str(e)}")
        # 11. Enhanced Error Logging (Requirement: Status failed & Error msg)
        try:
            if mlflow.active_run():
                mlflow.set_tag("status", "failed")
                mlflow.log_param("error_message", str(e)[:250])
        except: pass
        
        self.update_state(state="FAILURE", meta={"status": f"Training failed: {str(e)}", "progress": 0})
        return {"success": False, "error": str(e)}
    finally:
        db.close()

@celery_app.task(bind=True, name="scripts.tasks.compare_multiple_models")
def compare_multiple_models(
    self, 
    dataset_id_str: str, 
    user_id_str: str,
    target_column: str,
    cv_folds: int = 5
) -> Dict[str, Any]:
    """
    Background Task: Generate ranked benchmarking report by training multiple algorithms.
    """
    from services.storage_service import storage_service
    from services.preprocessing_service import PreprocessingService
    from core.database import SessionLocal
    import uuid
    import math

    db = SessionLocal()
    try:
        dataset_id = uuid.UUID(dataset_id_str)
        user_id = uuid.UUID(user_id_str)
        
        # 1. Load Data Lineage
        ds = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
        if not ds:
            raise ValueError("Dataset not found or access denied")
        df = storage_service.read_df(ds.storage_path)
            
        # EDGE CASE: Empty or extremely small dataset
        if len(df) < 5:
            # We need at least enough for a split (5 is a safe heuristic for benchmarking)
            raise ValueError(f"Dataset is too small for benchmarking (rows: {len(df)})")
            
        # 2. Separate Splits
        if 'is_train' in df.columns:
            train_df = df[df['is_train'] == 1].copy()
            test_df = df[df['is_train'] == 0].copy()
        else:
            from sklearn.model_selection import train_test_split
            train_df, test_df = train_test_split(df.copy(), test_size=0.2, random_state=42)
        
        cols_to_drop = [target_column, 'is_train', 'sample_weight']
        X_train = train_df.drop(columns=[c for c in cols_to_drop if c in train_df.columns])
        y_train = train_df[target_column]
        X_test = test_df.drop(columns=[c for c in cols_to_drop if c in test_df.columns])
        y_test = test_df[target_column]
        sample_weight = train_df.get('sample_weight')

        # 3. Detect Task Type
        is_clf = y_train.nunique() < 20 or y_train.dtype == "object"
        
        # EDGE CASE: Single-class classification (Requirement: production robustness)
        if is_clf and y_train.nunique() < 2:
            raise ValueError("Benchmarking failed: classification requires at least 2 distinct classes in the target column.")
        
        # 4. Define Algorithm Suite (Requirement: multi-model benchmarking)
        if is_clf:
            algorithms = [
                AlgorithmType.LOGISTIC_REGRESSION,
                AlgorithmType.RANDOM_FOREST_CLF,
                AlgorithmType.GRADIENT_BOOSTING_CLF
            ]
            primary_metric = "accuracy"
        else:
            algorithms = [
                AlgorithmType.LINEAR_REGRESSION,
                AlgorithmType.RANDOM_FOREST_REG,
                AlgorithmType.GRADIENT_BOOSTING_REG
            ]
            primary_metric = "r2"

        results = []
        pipeline = MLPipeline()
        mlflow_manager.set_active_experiment(user_id_str)

        # 5. Benchmarking Loop (Requirement: One run per model, individual logging)
        for i, algo_type in enumerate(algorithms):
            algo_name = algo_type.value
            progress = int(10 + (i / len(algorithms)) * 80)
            self.update_state(state="PROGRESS", meta={"status": f"Benchmarking {algo_name}", "progress": progress})
            
            try:
                run_name = f"Benchmark_{algo_name}_{dataset_id_str[:8]}_v{ds.version}"
                with mlflow.start_run(run_name=run_name):
                    # Fit (Requirement: Data Immutability via .copy())
                    model = pipeline._create_model(algo_type)
                    model.fit(X_train.copy(), y_train.copy(), sample_weight=sample_weight.copy() if sample_weight is not None else None)
                    
                    # Evaluate
                    y_pred = model.predict(X_test.copy())
                    
                    eval_metrics = {}
                    if is_clf:
                        eval_metrics["accuracy"] = float(accuracy_score(y_test, y_pred))
                        eval_metrics["precision"] = float(precision_score(y_test, y_pred, average="weighted", zero_division=0))
                        eval_metrics["recall"] = float(recall_score(y_test, y_pred, average="weighted", zero_division=0))
                        eval_metrics["f1_score"] = float(f1_score(y_test, y_pred, average="weighted", zero_division=0))
                    else:
                        from sklearn.metrics import mean_squared_error, r2_score
                        eval_metrics["mse"] = float(mean_squared_error(y_test, y_pred))
                        eval_metrics["r2"] = float(r2_score(y_test, y_pred))

                    # Safety check for metrics
                    eval_metrics = {k: float(v) for k, v in eval_metrics.items() if v is not None and not (isinstance(v, float) and math.isnan(v))}
                    
                    # Log artifacts to MLflow
                    mlflow.log_params({
                        "algorithm": algo_name,
                        "dataset_id": dataset_id_str,
                        "version": ds.version,
                        "benchmark": "true"
                    })
                    mlflow.log_metrics(eval_metrics)
                    mlflow.set_tags({
                        "user_id": user_id_str,
                        "dataset_id": dataset_id_str,
                        "benchmarking_session": self.request.id
                    })
                    mlflow.sklearn.log_model(model, "model")
                    
                    # 6. Store Result (Requirement: aggregating results)
                    results.append({
                        "algorithm": algo_name,
                        "metrics": eval_metrics,
                        "success": True,
                        "run_id": mlflow.active_run().info.run_id
                    })
            except Exception as sub_e:
                logger.warning(f"Benchmark failed for {algo_name}: {str(sub_e)}")
                results.append({"algorithm": algo_name, "success": False, "error": str(sub_e)})
                continue

        # 7. Best Model Selection (Requirement: Primary metric based selection)
        successful_results = [r for r in results if r["success"]]
        best_model = None
        if successful_results:
            if primary_metric == "mse":
                best_model = min(successful_results, key=lambda x: x["metrics"].get("mse", float('inf')))
            else:
                best_model = max(successful_results, key=lambda x: x["metrics"].get(primary_metric, -1.0))

        self.update_state(state="SUCCESS", meta={"status": "Benchmarking completed", "progress": 100})
        
        # 8. Return Response
        return {
            "success": True,
            "results": results,
            "best_model": best_model,
            "primary_metric": primary_metric,
            "dataset_id": dataset_id_str
        }
    except Exception as e:
        logger.error(f"Comparison Task Error: {str(e)}")
        self.update_state(state="FAILURE", meta={"status": f"Comparison failed: {str(e)}", "progress": 0})
        return {"success": False, "error": str(e)}
    finally:
        db.close()

@celery_app.task(bind=True, name="scripts.tasks.preprocess_dataset_task")
def preprocess_dataset_task(
    self, 
    dataset_id_str: str, 
    user_id_str: str,
    options: Dict[str, bool]
) -> Dict[str, Any]:
    """
    Background Task: Preprocess a dataset with the specified options.
    """
    from services.preprocessing_service import PreprocessingService
    from core.database import SessionLocal
    import uuid
    from datetime import datetime
    
    start_time = datetime.now()
    job_id = self.request.id
    logger.info(f"🚀 TASK START | Job: {job_id} | Dataset: {dataset_id_str} | User: {user_id_str} | Time: {start_time}")

    self.update_state(state="PROGRESS", meta={"status": "Starting preprocessing...", "progress": 10})
    
    db = SessionLocal()
    dataset_id = uuid.UUID(dataset_id_str)
    user_id = uuid.UUID(user_id_str)
    
    try:
        # Idempotency Check (Audit Step 3A)
        # Check if this task already created a child version to prevent duplicates on retry
        existing_child = db.query(Dataset).filter(
            Dataset.parent_dataset_id == dataset_id,
            Dataset.status == "processed",
            Dataset.user_id == user_id
        ).first()
        
        if existing_child:
            logger.info(f"♻️ IDEMPOTENCY HIT | Job: {job_id} | Existing Dataset: {existing_child.id}")
            return {
                "success": True, 
                "dataset_id": str(existing_child.id),
                "original_id": dataset_id_str,
                "message": "Retrieved existing processed version."
            }

        self.update_state(state="PROGRESS", meta={"status": "Processing data lineage...", "progress": 40})
        
        # Logic is now transaction-safe within the service
        if options.get("automated"):
            new_ds_id = PreprocessingService.automated_preprocessing(
                db, dataset_id, user_id, target=options.get("target_column")
            )
        else:
            new_ds_id = PreprocessingService.preprocess_dataset(db, dataset_id, user_id, options)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"✅ TASK SUCCESS | Job: {job_id} | New Dataset: {new_ds_id} | Duration: {duration}s")

        return {
            "success": True, 
            "dataset_id": str(new_ds_id),
            "original_id": dataset_id_str,
            "message": "Dataset version created successfully."
        }
    except Exception as e:
        logger.error(f"❌ TASK FAILURE | Job: {job_id} | Error: {str(e)}")
        # Failure Handling (Audit Step 3B)
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
        if dataset:
            dataset.status = "error"
            db.commit()
            
        self.update_state(
            state="FAILURE", 
            meta={"status": f"Preprocessing failed: {str(e)}", "progress": 0}
        )
        return {"success": False, "error": str(e)}
    finally:
        db.close()
