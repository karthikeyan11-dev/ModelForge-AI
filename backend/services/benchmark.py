import pandas as pd
import uuid
import time
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

from services.ml_pipeline import MLPipeline, AlgorithmType, TrainingConfig, get_classification_algorithms, get_regression_algorithms

BENCHMARK_STORAGE = "data/benchmarks.json"

class BenchmarkService:
    """
    Production-grade Benchmark system for ML models.
    Executes concurrent parallel training evaluation across families of architectures.
    """
    def __init__(self):
        self.jobs = {}
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._load_history()
        
    def _load_history(self):
        import os
        if os.path.exists(BENCHMARK_STORAGE):
            try:
                with open(BENCHMARK_STORAGE, 'r') as f:
                    self.history = json.load(f)
            except Exception:
                self.history = {}
        else:
            self.history = {}
            os.makedirs("data", exist_ok=True)
            
    def _save_history(self):
        with open(BENCHMARK_STORAGE, 'w') as f:
            json.dump(self.history, f, indent=2)

    def detect_problem_type(self, y: pd.Series) -> str:
        if y.nunique() < 20 or y.dtype == 'object' or y.dtype.name == 'category':
            return 'classification'
        return 'regression'

    def start_benchmark(self, dataset_name: str, X: pd.DataFrame, y: pd.Series, target_column: str) -> str:
        job_id = f"bench_{uuid.uuid4().hex[:8]}"
        problem_type = self.detect_problem_type(y)
        
        self.jobs[job_id] = {
            "status": "running",
            "dataset_name": dataset_name,
            "problem_type": problem_type,
            "target_column": target_column,
            "progress": 0,
            "results": [],
            "error": None,
            "start_time": time.time(),
            "algorithms_expected": 0
        }
        
        # Fire and forget thread to manage parallel execution cleanly natively
        self.executor.submit(self._run_parallel_benchmark, job_id, dataset_name, X, y, problem_type)
        return job_id

    def _run_parallel_benchmark(self, job_id: str, dataset_name: str, X: pd.DataFrame, y: pd.Series, problem_type: str):
        try:
            if problem_type == 'classification':
                algorithms = get_classification_algorithms()
                scoring = "accuracy"
                primary_metric = "Accuracy"
                is_reverse = True # Higher accuracy is better
            else:
                algorithms = get_regression_algorithms()
                scoring = "neg_mean_squared_error" 
                primary_metric = "RMSE"
                is_reverse = False # Lower RMSE is better

            pipeline = MLPipeline()
            self.jobs[job_id]["algorithms_expected"] = len(algorithms)
            
            futures = []
            
            # Process inside a bounded thread pool mapped uniquely ensuring non-blocking operations natively per algorithm
            with ThreadPoolExecutor(max_workers=min(4, len(algorithms))) as inner_exec:
                for algo in algorithms:
                    # Note: We enforce n_jobs=1 internally to prevent thread starvation since outer pool manages parallelism
                    config = TrainingConfig(algorithm=algo, cv_folds=3, scoring=scoring, n_jobs=1) 
                    futures.append(inner_exec.submit(pipeline.train, X.copy(), y.copy(), config))
                
                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    if result.success:
                        cv_val = abs(result.cv_mean)
                        self.jobs[job_id]["results"].append({
                            "algorithm": result.algorithm,
                            "cv_mean": round(cv_val, 4),
                            "cv_std": round(result.cv_std, 4),
                            "training_time": round(result.training_time_seconds, 3)
                        })
                    completed += 1
                    self.jobs[job_id]["progress"] = round((completed / len(algorithms)) * 100, 2)
            
            # Leaderboard Sorting
            self.jobs[job_id]["results"].sort(key=lambda x: x["cv_mean"], reverse=is_reverse)
            
            # Rank Assignment
            for i, r in enumerate(self.jobs[job_id]["results"]):
                r["rank"] = i + 1
                r["primary_metric"] = primary_metric
                
            self.jobs[job_id]["status"] = "completed"
            self.jobs[job_id]["execution_time"] = time.time() - self.jobs[job_id]["start_time"]
            
            # Result Persistence Storage
            self.history[job_id] = {
                "dataset_name": dataset_name,
                "timestamp": datetime.now().isoformat(),
                "problem_type": problem_type,
                "leaderboard": self.jobs[job_id]["results"],
                "execution_time": self.jobs[job_id]["execution_time"]
            }
            self._save_history()
            
        except Exception as e:
            logger.error(f"Benchmark failed: {str(e)}")
            self.jobs[job_id]["status"] = "failed"
            self.jobs[job_id]["error"] = str(e)

    def get_status(self, job_id: str) -> dict:
        job = self.jobs.get(job_id)
        if not job:
            return {"status": "not_found"}
        return {"status": job["status"], "progress": job["progress"], "error": job["error"]}
        
    def get_results(self, job_id: str) -> dict:
        job = self.jobs.get(job_id)
        if not job and job_id in self.history:
            job = self.history[job_id]
            job["status"] = "completed"
            job["results"] = job["leaderboard"]
            
        if not job or job.get("status") != "completed":
            return {"status": "unavailable", "message": "Job is still processing or does not exist."}
            
        leaderboard = job["results"]
        # Unified Chart Extract compatible with Chart.js natively
        chart_data = {
            "labels": [r["algorithm"] for r in leaderboard],
            "metrics": [r["cv_mean"] for r in leaderboard],
            "times": [r["training_time"] for r in leaderboard],
            "metric_name": leaderboard[0]["primary_metric"] if leaderboard else "Score"
        }
        
        return {
            "status": "completed",
            "dataset_name": job.get("dataset_name"),
            "problem_type": job.get("problem_type"),
            "leaderboard": leaderboard,
            "chart_data": chart_data,
            "execution_time_seconds": round(job.get("execution_time", 0.0), 2),
            "timestamp": job.get("timestamp")
        }

benchmark_service = BenchmarkService()
