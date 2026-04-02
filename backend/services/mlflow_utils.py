import mlflow
import mlflow.sklearn
import mlflow.xgboost
import os
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class MLflowManager:
    """
    Utility class to manage MLflow tracking, logging, and model retrieval.
    Encapsulates all MLflow-specific logic for the AutoML pipeline.
    """
    
    def __init__(self, tracking_uri: str = "http://mlflow:5000"):
        """
        Initialize MLflow manager.
        
        Args:
            tracking_uri: The URI for the MLflow tracking server.
        """
        self.tracking_uri = tracking_uri
        mlflow.set_tracking_uri(self.tracking_uri)
        
    def get_or_create_experiment(self, experiment_name: str) -> str:
        """
        Retrieve or create an MLflow experiment by name.
        
        Args:
            experiment_name: Name of the experiment (e.g., 'Project_A_Classification')
            
        Returns:
            experiment_id: The ID of the experiment
        """
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment:
            return experiment.experiment_id
        else:
            return mlflow.create_experiment(experiment_name)

    def set_active_experiment(self, user_id: str) -> str:
        """
        Setup tracking for a specific user and set it as active experiment.
        Ensures the experiment exists and is correctly selected in the global context.
        """
        experiment_name = f"automl_user_{user_id}"
        experiment_id = self.get_or_create_experiment(experiment_name)
        mlflow.set_experiment(experiment_name)
        return experiment_id

    def log_training_run(
        self, 
        model: Any, 
        algorithm_name: str, 
        params: Dict[str, Any], 
        metrics: Dict[str, float], 
        preprocessing_pipeline: Any = None,
        tags: Dict[str, str] = None
    ):
        """
        Log a complete training run to MLflow.
        
        Args:
            model: The trained model object (sklearn or xgboost)
            algorithm_name: Name of the algorithm used
            params: Dictionary of hyperparameters
            metrics: Dictionary of evaluation metrics
            preprocessing_pipeline: Optional preprocessing pipeline object
            tags: Optional tags for the run
        """
        # Log basic info
        mlflow.log_param("algorithm", algorithm_name)
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        
        if tags:
            mlflow.set_tags(tags)
            
        # Log model artifact based on type
        try:
            if "XGB" in str(type(model)):
                mlflow.xgboost.log_model(model, artifact_path="model")
            else:
                mlflow.sklearn.log_model(model, artifact_path="model")
                
            # Log preprocessing pipeline if provided
            if preprocessing_pipeline:
                mlflow.sklearn.log_model(preprocessing_pipeline, artifact_path="preprocessing")
                
        except Exception as e:
            logger.error(f"Failed to log model to MLflow: {str(e)}")

    def get_best_run(self, experiment_id: str, metric_name: str = "cv_mean") -> Optional[Dict[str, Any]]:
        """
        Retrieve the best run for an experiment based on a metric.
        
        Args:
            experiment_id: The ID of the experiment
            metric_name: The metric to maximize (e.g., 'cv_mean')
            
        Returns:
            run_details: Dictionary containing run_id and metadata
        """
        try:
            runs = mlflow.search_runs(
                experiment_ids=[experiment_id],
                order_by=[f"metrics.{metric_name} DESC"],
                max_results=1
            )
            
            if len(runs) > 0:
                best_run = runs.iloc[0]
                return best_run.to_dict()
            return None
        except Exception as e:
            logger.error(f"Error searching for best run: {str(e)}")
            return None

    def list_experiments(self) -> List[Dict[str, Any]]:
        """List all experiments in the tracking server."""
        experiments = mlflow.search_experiments()
        return [
            {
                "id": e.experiment_id,
                "name": e.name,
                "artifact_location": e.artifact_location,
                "lifecycle_stage": e.lifecycle_stage
            } 
            for e in experiments
        ]

    def list_runs(self, experiment_id: str) -> List[Dict[str, Any]]:
        """List runs for a specific experiment."""
        runs = mlflow.search_runs(experiment_ids=[experiment_id])
        return runs.to_dict(orient="records")

    def get_leaderboard(self, user_id: str, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Fetch and rank models for a specific user and dataset from MLflow.
        """
        experiment_name = f"automl_user_{user_id}"
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if not experiment:
            return []
            
        try:
            # Search for runs with strict dataset_id filtering for audit compliance
            runs = mlflow.search_runs(
                experiment_ids=[experiment.experiment_id],
                filter_string=f"tags.dataset_id = '{dataset_id}'",
                run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY
            )
            
            if runs.empty:
                return []
                
            leaderboard = []
            for _, run in runs.iterrows():
                # Detect task type (Requirement: Defensive extraction)
                task_type = run.get("params.task_type", "classification")
                primary_metric = "accuracy" if task_type == "classification" else "r2"
                
                # NULL / NaN Safety (Requirement: Explicit check and continue)
                score = run.get(f"metrics.{primary_metric}")
                
                if score is None or (isinstance(score, float) and math.isnan(score)):
                    # Final attempt fallback
                    score = run.get("metrics.cv_mean")
                
                if score is None or (isinstance(score, float) and math.isnan(score)):
                    continue
                
                # Safe Type Conversion (Requirement: float cast)
                score_val = round(float(score), 4)
                    
                leaderboard.append({
                    "run_id": run["run_id"],
                    "model": run.get("params.algorithm", "unknown"),
                    "score": score_val,
                    "metric": primary_metric,
                    "version": run.get("params.version", "1"),
                    "created_at": run.get("start_time"),
                    "status": run.get("status", "FINISHED")
                })
                
            # Sort: Higher is better for accuracy and R2
            leaderboard.sort(key=lambda x: x["score"], reverse=True)
            
            # Limit Results (Requirement: Top 10)
            leaderboard = leaderboard[:10]
            
            # Add Rank Field (Requirement: 1-indexed rank)
            for i, entry in enumerate(leaderboard):
                entry["rank"] = i + 1
                
            return leaderboard
            
        except Exception as e:
            logger.error(f"Failed to fetch leaderboard from MLflow: {str(e)}")
            return []

# Global instance for easy access
# We use the internal docker network 'mlflow' for communication
mlflow_manager = MLflowManager(tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
