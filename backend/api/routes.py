"""
FastAPI Routes - Production-Grade AutoML API

Implements all API endpoints for:
- Data ingestion and cleaning
- ML training and evaluation
- Model management and export
- Hyperparameter tuning
- Retraining workflows
"""

import os
import sys
import logging
import requests
import re
from datetime import datetime
from typing import Optional, List, Dict, Any
from io import StringIO, BytesIO

import pandas as pd
from sqlalchemy.orm import Session
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, BackgroundTasks, Form, Query
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
import uuid

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from .auth import get_current_user
from .database import get_db, SessionLocal
from models.user import User
from services.storage_service import storage_service
from scripts.tasks import train_single_model, compare_multiple_models
from scripts.celery_app import celery_app
from services.mlflow_utils import mlflow_manager
from .schemas import (
    # Enums
    DataSourceType, ProblemType, AlgorithmType, TuningMethod,
    MissingValueStrategy, ScalingStrategy, EncodingStrategy,
    
    # Requests
    DatabaseQueryRequest, APIDataRequest, CleaningConfig,
    SelectTargetRequest, PreprocessingConfig, TrainModelRequest,
    TrainMultipleRequest, TuneModelRequest, SaveModelRequest,
    ExportModelRequest, RollbackRequest, RetrainModelRequest,
    PredictRequest,
    
    # Responses
    CleaningResponse, DataProfileResponse, ProblemDetectionResponse,
    TargetSuggestion, PreprocessingResponse, TrainModelResponse,
    TrainingResult, CompareModelsResponse, LeaderboardEntry,
    TuningResponse, EvaluationResponse, ClassificationMetricsResponse,
    RegressionMetricsResponse, ModelVersionInfo, ExportModelResponse,
    RetrainResponse, PredictResponse, HealthResponse, SessionState,
    JobStatus, JobSubmitResponse, JobStatusResponse,
    ValidationRequest, ValidationResponse,
    
    # Dataset & Ingestion (New)
    DatasetResponse, PreprocessingRequest, APIFetchRequest, APIIngestRequest,
    UploadResponse, PreprocessResponse, PreprocessStatusResponse
)

logger = logging.getLogger(__name__)

# ============================================================================
# Router Setup
# ============================================================================

router = APIRouter(
    prefix="/api/v2", 
    tags=["AutoML"],
    dependencies=[Depends(get_current_user)]
)
mlops_router = APIRouter(
    prefix="/api/v2/mlops", 
    tags=["MLOps"],
    dependencies=[Depends(get_current_user)]
)
# Public router for health checks and status
health_router = APIRouter(
    prefix="/api/v2",
    tags=["Health"]
)


# ============================================================================
# Session State Management
# ============================================================================

class SessionStore:
    """
    In-memory session store for development.
    In production, use Redis or database-backed sessions.
    """
    
    def __init__(self):
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._default_session = "default"
    
    def get(self, user_id: str) -> Dict[str, Any]:
        if user_id not in self._sessions:
            self._sessions[user_id] = {
                "data": None,
                "target_column": None,
                "problem_type": None,
                "preprocessing_pipeline": None,
                "feature_engineer": None,
                "X_train": None,
                "X_test": None,
                "y_train": None,
                "y_test": None,
                "ml_pipeline": None,
                "trained_models": [],
                "best_model": None,
            }
        return self._sessions[user_id]
    
    def clear(self, user_id: str):
        if user_id in self._sessions:
            del self._sessions[user_id]


# Global session store
session_store = SessionStore()


def get_session(current_user: User = Depends(get_current_user)) -> Dict[str, Any]:
    """Dependency to get current session isolated by user_id."""
    return session_store.get(str(current_user.id))


# ============================================================================
# Data Upload & Cleaning Endpoints
# ============================================================================

@router.post("/upload-data", response_model=UploadResponse)
async def upload_data(
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Step 1: Production-Grade Stable Ingestion
    - Non-blocking: ONLY handles storage and DB entry
    - Memory-Efficient: Streams file directly to disk
    - Resilient: Comprehensive error handling returning JSON
    """
    import shutil
    import uuid
    from models.dataset import Dataset

    request_id = str(uuid.uuid4())[:8]
    logger.info(f"[{request_id}] Raw Ingestion Event: {file.filename} (User: {current_user.id})")
    
    try:
        # 1. Validation Logic
        file_ext = file.filename.split(".")[-1].lower()
        if file_ext not in ["csv", "xlsx", "xls", "json"]:
            logger.warning(f"[{request_id}] Validation Blocked: Unsupported extension '{file_ext}'")
            raise HTTPException(status_code=400, detail="Unsupported file format.")
        
        # 2. Storage Setup
        dataset_uuid = uuid.uuid4()
        user_id_str = str(current_user.id)
        user_storage_dir = os.path.join("uploads", user_id_str, str(dataset_uuid))
        storage_filename = f"raw_{file.filename}"
        storage_path = os.path.join(user_storage_dir, storage_filename)
        
        os.makedirs(user_storage_dir, exist_ok=True)
        
        # 3. Stream File (Rule #1 Fix: Zero-Load Memory Streaming)
        logger.info(f"[{request_id}] Write Cycle Started: {storage_path}")
        start_time = datetime.utcnow()
        
        # Explicit file pointer streaming to prevent Event Loop Blocking or OOM
        with open(storage_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        write_time = (datetime.utcnow() - start_time).total_seconds()
        file_size = os.path.getsize(storage_path)
        logger.info(f"[{request_id}] Write Cycle Completed ({file_size} bytes in {write_time:.2f}s)")
        
        # 4. Database Transaction
        display_name = dataset_name or file.filename
        new_dataset = Dataset(
            id=dataset_uuid,
            user_id=current_user.id,
            name=display_name,
            filename=file.filename,
            storage_path=storage_path,
            file_size=float(file_size),
            status="uploaded",
            version=1,
            is_latest=True
        )
        db.add(new_dataset)
        db.commit()
        logger.info(f"[{request_id}] Transaction Committed: {dataset_uuid}")
        
        return UploadResponse(
            dataset_id=dataset_uuid,
            filename=file.filename,
            status="uploaded"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] CRITICAL INGESTION CRASH: {str(e)}", exc_info=True)
        # 5. Cleanup: If file was written but DB failed, remove the file to prevent orphans
        try:
            if 'storage_path' in locals() and os.path.exists(storage_path):
                os.remove(storage_path)
                logger.info(f"[{request_id}] Cleanup: Removed orphaned file {storage_path}")
                # Recursively remove dir if empty
                parent_dir = os.path.dirname(storage_path)
                if not os.listdir(parent_dir):
                    os.rmdir(parent_dir)
        except Exception as cleanup_err:
            logger.error(f"[{request_id}] Cleanup failed: {str(cleanup_err)}")

        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Ingestion Failure: Internal system error during data persistence. Technical detail: {str(e)}",
                "request_id": request_id
            }
        )
    finally:
        # Mandatory: Close the spool file to release handles
        await file.close()


@router.get("/datasets", response_model=List[DatasetResponse])
async def get_datasets(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Step 2: Dataset Listing (Preprocess Page)
    Fetch all datasets uploaded by the current user.
    """
    from models.dataset import Dataset
    datasets = db.query(Dataset).filter(Dataset.user_id == current_user.id).order_by(Dataset.created_at.desc()).all()
    return datasets


@router.post("/preprocess/{dataset_id}", response_model=PreprocessResponse)
async def trigger_preprocessing(
    dataset_id: uuid.UUID,
    config: PreprocessingRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Step 3: Preprocessing Trigger
    Triggers a Celery background task for intelligent preprocessing.
    """
    from models.dataset import Dataset
    from scripts.tasks import preprocess_dataset_task
    
    # 1. Verify dataset exists and belongs to user
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == current_user.id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # 2. Trigger Celery task
    task = preprocess_dataset_task.delay(
        str(dataset_id), 
        str(current_user.id),
        config.options.dict()
    )
    
    # 3. Update dataset status
    dataset.status = "processing_started"
    db.commit()
    
    return PreprocessResponse(
        task_id=task.id,
        status="processing_started"
    )


@router.get("/preprocess-status/{task_id}", response_model=PreprocessStatusResponse)
async def get_preprocess_status(
    task_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Step 6: Status Tracking
    Poll Celery task status for preprocessing results.
    """
    from celery.result import AsyncResult
    from scripts.celery_app import celery_app
    
    res = AsyncResult(task_id, app=celery_app)
    
    status_map = {
        "PENDING": "pending",
        "STARTED": "running",
        "PROGRESS": "running",
        "SUCCESS": "completed",
        "FAILURE": "failed",
        "RETRY": "running"
    }
    
    status = status_map.get(res.status, "pending")
    result = None
    error = None
    
    if res.status == "SUCCESS":
        result = res.result
    elif res.status == "FAILURE":
        error = str(res.result)
    
    return PreprocessStatusResponse(
        task_id=task_id,
        status=status,
        result=result,
        error=error
    )


@router.post("/validate-dataset", response_model=ValidationResponse)
async def validate_dataset(
    request: ValidationRequest,
    session: Dict = Depends(get_session)
):
    """
    Validate the current session data against a Great Expectations suite.
    """
    if session["data"] is None:
        raise HTTPException(status_code=400, detail="No data in session. Upload data first.")
    
    from services.validation import validator
    df = session["data"]
    
    if request.generate_baseline:
        try:
            validator.generate_baseline_expectations(df, request.suite_name, request.target_column)
            message = "Generated baseline effectively."
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to generate expectations: {e}")
            
    try:
        val_results = validator.validate_dataset(df, request.suite_name)
        return ValidationResponse(**val_results)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")



@router.post("/clean-data", response_model=CleaningResponse)
async def clean_data(
    config: CleaningConfig,
    session: Dict = Depends(get_session)
):
    """
    Clean data currently in session using source-aware AI routing.
    """
    if session["data"] is None:
        raise HTTPException(status_code=400, detail="No data in session. Upload data first.")
    
    try:
        from services.ai_router import AIRouter, DataSourceType as DST, CleaningRequest
        from scripts.data_cleaning import DataCleaning
        
        df = session["data"]
        raw_shape = list(df.shape)
        
        # Map source type
        source_map = {
            DataSourceType.DATABASE: DST.DATABASE,
            DataSourceType.UPLOAD: DST.UPLOAD,
            DataSourceType.API: DST.API
        }
        
        # Rule-based cleaning
        cleaner = DataCleaning()
        df = cleaner.clean_data(df)
        
        # AI-powered cleaning
        ai_router = AIRouter()
        request = CleaningRequest(
            data=df,
            source_type=source_map[config.source_type],
            batch_size=config.batch_size,
            max_retries=config.max_retries
        )
        result = ai_router.clean(request)
        
        # Update session
        session["data"] = result.cleaned_data
        
        return CleaningResponse(
            success=True,
            cleaned_data=result.cleaned_data.head(100).to_dict(orient="records"),
            raw_shape=raw_shape,
            cleaned_shape=list(result.cleaned_data.shape),
            model_used=result.model_used,
            processing_time_ms=result.processing_time_ms,
            message="Data cleaned successfully",
            errors=result.errors
        )
        
    except Exception as e:
        logger.error(f"Cleaning error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clean-database", response_model=CleaningResponse)
async def clean_database(
    query: DatabaseQueryRequest,
    session: Dict = Depends(get_session)
):
    """
    Execute database query and clean results.
    """
    try:
        from sqlalchemy import create_engine
        from services.ai_router import AIRouter, DataSourceType as DST, CleaningRequest
        from scripts.data_cleaning import DataCleaning
        
        # Execute query
        engine = create_engine(query.db_url)
        df = pd.read_sql(query.query, engine)
        raw_shape = list(df.shape)
        
        # Rule-based cleaning
        cleaner = DataCleaning()
        df = cleaner.clean_data(df)
        
        # AI cleaning with database-specific model
        ai_router = AIRouter()
        request = CleaningRequest(
            data=df,
            source_type=DST.DATABASE,
            batch_size=20
        )
        result = ai_router.clean(request)
        
        # Store in session
        session["data"] = result.cleaned_data
        session["source_type"] = "database"
        
        return CleaningResponse(
            success=True,
            cleaned_data=result.cleaned_data.head(100).to_dict(orient="records"),
            raw_shape=raw_shape,
            cleaned_shape=list(result.cleaned_data.shape),
            model_used=result.model_used,
            processing_time_ms=result.processing_time_ms,
            message="Database data cleaned successfully"
        )
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clean-api", response_model=CleaningResponse)
async def clean_api_data(
    request: APIDataRequest,
    session: Dict = Depends(get_session)
):
    """
    Fetch and clean data from external API.
    """
    try:
        import aiohttp
        from services.ai_router import AIRouter, DataSourceType as DST, CleaningRequest
        from scripts.data_cleaning import DataCleaning
        
        # Fetch data
        async with aiohttp.ClientSession() as client:
            async with client.get(request.api_url, headers=request.headers) as resp:
                if resp.status != 200:
                    raise HTTPException(status_code=400, detail="Failed to fetch API data")
                data = await resp.json()
        
        df = pd.DataFrame(data)
        raw_shape = list(df.shape)
        
        # Rule-based cleaning
        cleaner = DataCleaning()
        df = cleaner.clean_data(df)
        
        # AI cleaning with API-specific model
        ai_router = AIRouter()
        clean_request = CleaningRequest(
            data=df,
            source_type=DST.API,
            batch_size=20
        )
        result = ai_router.clean(clean_request)
        
        # Store in session
        session["data"] = result.cleaned_data
        session["source_type"] = "api"
        
        return CleaningResponse(
            success=True,
            cleaned_data=result.cleaned_data.head(100).to_dict(orient="records"),
            raw_shape=raw_shape,
            cleaned_shape=list(result.cleaned_data.shape),
            model_used=result.model_used,
            processing_time_ms=result.processing_time_ms,
            message="API data cleaned successfully"
        )
        
    except Exception as e:
        logger.error(f"API data error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Target Selection & Problem Detection
# ============================================================================

@router.get("/suggest-targets", response_model=List[TargetSuggestion])
async def suggest_targets(session: Dict = Depends(get_session)):
    """
    Suggest potential target columns based on data analysis.
    """
    if session["data"] is None:
        raise HTTPException(status_code=400, detail="No data in session")
    
    from services.problem_detection import ProblemDetector
    
    detector = ProblemDetector()
    suggestions = detector.suggest_target(session["data"])
    
    return [TargetSuggestion(**s) for s in suggestions]


@router.post("/select-target", response_model=ProblemDetectionResponse)
async def select_target(
    request: SelectTargetRequest,
    session: Dict = Depends(get_session)
):
    """
    Select target column and detect problem type.
    """
    if session["data"] is None:
        raise HTTPException(status_code=400, detail="No data in session")
    
    df = session["data"]
    
    if request.target_column not in df.columns:
        raise HTTPException(
            status_code=400, 
            detail=f"Column '{request.target_column}' not found"
        )
    
    from services.problem_detection import ProblemDetector
    from services.problem_detection import ProblemType as PT
    
    detector = ProblemDetector()
    
    # Map force type
    force_type = None
    if request.force_problem_type:
        force_map = {
            ProblemType.BINARY_CLASSIFICATION: PT.BINARY_CLASSIFICATION,
            ProblemType.MULTICLASS_CLASSIFICATION: PT.MULTICLASS_CLASSIFICATION,
            ProblemType.REGRESSION: PT.REGRESSION
        }
        force_type = force_map.get(request.force_problem_type)
    
    result = detector.detect(df, request.target_column, force_type)
    
    # Store in session
    session["target_column"] = request.target_column
    session["problem_type"] = result.problem_type.value
    
    return ProblemDetectionResponse(
        problem_type=ProblemType(result.problem_type.value),
        target_column=request.target_column,
        unique_values=result.target_analysis.unique_values,
        is_trainable=result.is_trainable,
        recommended_algorithms=[
            {
                "name": algo.name,
                "algorithm_id": algo.algorithm_id,
                "priority": algo.priority,
                "reason": algo.reason
            }
            for algo in result.recommended_algorithms
        ],
        data_issues=[issue.value for issue in result.data_issues],
        warnings=result.warnings,
        dataset_stats=result.dataset_stats
    )


# ============================================================================
# Preprocessing
# ============================================================================

@router.post("/preprocess", response_model=PreprocessingResponse)
async def preprocess_data(
    config: PreprocessingConfig,
    session: Dict = Depends(get_session)
):
    """
    Apply preprocessing pipeline to data.
    """
    if session["data"] is None:
        raise HTTPException(status_code=400, detail="No data in session")
    if session["target_column"] is None:
        raise HTTPException(status_code=400, detail="Target not selected")
    
    from services.preprocessing import (
        PreprocessingPipeline, 
        PreprocessingConfig as PPConfig,
        MissingValueStrategy as MVS,
        ScalingStrategy as SS,
        EncodingStrategy as ES
    )
    from services.feature_engineering import FeatureEngineer, SplitConfig
    
    df = session["data"]
    target = session["target_column"]
    
    # Map strategies
    mvs_map = {
        MissingValueStrategy.MEAN: MVS.MEAN,
        MissingValueStrategy.MEDIAN: MVS.MEDIAN,
        MissingValueStrategy.MODE: MVS.MODE,
        MissingValueStrategy.DROP: MVS.DROP
    }
    
    ss_map = {
        ScalingStrategy.STANDARD: SS.STANDARD,
        ScalingStrategy.MINMAX: SS.MINMAX,
        ScalingStrategy.ROBUST: SS.ROBUST,
        ScalingStrategy.NONE: SS.NONE
    }
    
    es_map = {
        EncodingStrategy.LABEL: ES.LABEL,
        EncodingStrategy.ONEHOT: ES.ONEHOT,
        EncodingStrategy.ORDINAL: ES.ORDINAL,
        EncodingStrategy.NONE: ES.NONE
    }
    
    # Create preprocessing config
    pp_config = PPConfig(
        missing_strategy=mvs_map[config.missing_strategy],
        scaling_strategy=ss_map[config.scaling_strategy],
        encoding_strategy=es_map[config.encoding_strategy],
        drop_constant_columns=config.drop_constant_columns,
        outlier_handling=config.handle_outliers,
        outlier_threshold=config.outlier_threshold
    )
    
    # Apply preprocessing
    pipeline = PreprocessingPipeline(pp_config)
    original_shape = list(df.shape)
    
    df_processed = pipeline.fit_transform(df, target)
    
    # Split data
    X = df_processed.drop(columns=[target])
    y = df_processed[target]
    
    is_classification = session["problem_type"] in ["binary_classification", "multiclass_classification"]
    
    split_config = SplitConfig(
        test_size=0.2,
        stratify=is_classification
    )
    
    X_train, X_test, y_train, y_test = FeatureEngineer.split_data(X, y, split_config)
    
    # Store in session
    session["preprocessing_pipeline"] = pipeline
    session["X_train"] = X_train
    session["X_test"] = X_test
    session["y_train"] = y_train
    session["y_test"] = y_test
    
    return PreprocessingResponse(
        success=True,
        original_shape=original_shape,
        processed_shape=list(df_processed.shape),
        numeric_columns=pipeline._numeric_columns,
        categorical_columns=pipeline._categorical_columns,
        dropped_columns=pipeline._dropped_columns,
        feature_names=X.columns.tolist(),
        message="Preprocessing completed successfully"
    )


# ============================================================================
# Training
# ============================================================================

@router.post("/train-model", response_model=JobSubmitResponse)
async def train_model(
    request: TrainModelRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Launch a background task to train a single model from a versioned dataset.
    """
    try:
        # Launch Celery task with dataset_id (Rule #2, #3, #4)
        task = train_single_model.delay(
            dataset_id_str=str(request.dataset_id),
            user_id_str=str(current_user.id),
            target_column="target", # Default from preprocessing logic or we can add to schema
            algorithm_name=request.algorithm.value,
            hyperparameters=request.hyperparameters or {},
            cv_folds=request.cv_folds
        )
        
        return JobSubmitResponse(
            job_id=task.id,
            status=JobStatus.PENDING,
            message=f"Training {request.algorithm.value} job submitted successfully"
        )
        
    except Exception as e:
        logger.error(f"Task submission error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compare-models", response_model=JobSubmitResponse)
async def compare_models(
    request: TrainMultipleRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Launch a background task to compare multiple models on a versioned dataset.
    """
    try:
        # Launch Comparison task via dataset_id
        algorithms = [a.value for a in request.algorithms]
        task = compare_multiple_models.delay(
            dataset_id_str=str(request.dataset_id),
            user_id_str=str(current_user.id),
            target_column="target", 
            algorithms=algorithms,
            cv_folds=request.cv_folds
        )
        
        return JobSubmitResponse(
            job_id=task.id,
            status=JobStatus.PENDING,
            message="Comparison job submitted successfully"
        )
        
    except Exception as e:
        logger.error(f"Task submission error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Check the status of a background training job using Celery AsyncResult.
    """
    from celery.result import AsyncResult
    
    result = AsyncResult(job_id, app=celery_app)
    
    # Map Celery internal states to our JobStatus enum
    state_map = {
        "PENDING": JobStatus.PENDING,
        "STARTED": JobStatus.RUNNING,
        "PROGRESS": JobStatus.RUNNING,
        "SUCCESS": JobStatus.COMPLETED,
        "FAILURE": JobStatus.FAILED,
        "RETRY": JobStatus.RUNNING,
        "REVOKED": JobStatus.CANCELLED
    }
    
    status = state_map.get(result.state, JobStatus.PENDING)
    
    # Extract metadata if available (progress, etc.)
    progress = 0.0
    current_step = "Unknown"
    job_result = None
    error = None
    
    if result.state == "PROGRESS":
        progress = result.info.get("progress", 0) / 100.0
        current_step = result.info.get("status", "Processing...")
    elif result.state == "SUCCESS":
        progress = 1.0
        current_step = "Completed"
        job_result = result.result
    elif result.state == "FAILURE":
        progress = 0.0
        current_step = "Failed"
        error = str(result.result)
        
    return JobStatusResponse(
        job_id=job_id,
        status=status,
        algorithm="Unknown", 
        progress=progress,
        current_step=current_step,
        result=job_result,
        error=error
    )


# ============================================================================
# MLflow Experiment Tracking Endpoints
# ============================================================================

@router.get("/experiments", tags=["MLflow"])
async def list_experiments():
    """List all MLflow experiments."""
    return mlflow_manager.list_experiments()


@router.get("/runs/{experiment_id}", tags=["MLflow"])
async def list_runs(experiment_id: str):
    """List all runs for a given experiment."""
    return mlflow_manager.list_runs(experiment_id)


@router.get("/best-model/{experiment_id}", tags=["MLflow"])
async def get_best_model(experiment_id: str):
    """Fetch details of the best performing model in an experiment."""
    best_run = mlflow_manager.get_best_run(experiment_id)
    if not best_run:
        raise HTTPException(status_code=404, detail="No runs found for this experiment")
    return best_run


# ============================================================================
# Hyperparameter Tuning
# ============================================================================

@router.post("/tune-model", response_model=TuningResponse)
async def tune_model(
    request: TuneModelRequest,
    session: Dict = Depends(get_session)
):
    """
    Tune hyperparameters for an algorithm.
    """
    if session["X_train"] is None:
        raise HTTPException(status_code=400, detail="Data not preprocessed")
    
    from services.hyperparameter_tuning import (
        HyperparameterTuner, 
        TuningConfig,
        TuningMethod as TM
    )
    from services.ml_pipeline import AlgorithmType as AT
    
    algo_map = {
        AlgorithmType.RIDGE: AT.RIDGE,
        AlgorithmType.LASSO: AT.LASSO,
        AlgorithmType.RANDOM_FOREST_REG: AT.RANDOM_FOREST_REG,
        AlgorithmType.XGBOOST_REG: AT.XGBOOST_REG,
        AlgorithmType.LOGISTIC_REGRESSION: AT.LOGISTIC_REGRESSION,
        AlgorithmType.RANDOM_FOREST_CLF: AT.RANDOM_FOREST_CLF,
        AlgorithmType.SVM_CLF: AT.SVM_CLF,
        AlgorithmType.XGBOOST_CLF: AT.XGBOOST_CLF,
    }
    
    method_map = {
        TuningMethod.GRID_SEARCH: TM.GRID_SEARCH,
        TuningMethod.RANDOM_SEARCH: TM.RANDOM_SEARCH,
        TuningMethod.OPTUNA: TM.OPTUNA
    }
    
    try:
        tuner = HyperparameterTuner()
        
        config = TuningConfig(
            method=method_map[request.method],
            n_trials=request.n_trials,
            cv_folds=request.cv_folds,
            timeout=request.timeout_seconds
        )
        
        result = tuner.tune(
            session["X_train"],
            session["y_train"],
            algo_map[request.algorithm],
            config,
            request.custom_search_space
        )
        
        return TuningResponse(
            success=True,
            algorithm=result.algorithm,
            method=result.method,
            best_params=result.best_params,
            best_score=result.best_score,
            n_trials_completed=result.n_trials_completed,
            total_time_seconds=result.total_time_seconds,
            message=f"Tuning completed with best score {result.best_score:.4f}"
        )
        
    except Exception as e:
        logger.error(f"Tuning error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# MLOps Routes - Model Management
# ============================================================================

@mlops_router.post("/save-model", response_model=ModelVersionInfo)
async def save_model(
    request: SaveModelRequest,
    current_user: User = Depends(get_current_user),
    session: Dict = Depends(get_session)
):
    """
    Save trained model with versioning.
    """
    if session["ml_pipeline"] is None or session["ml_pipeline"]._best_model is None:
        raise HTTPException(status_code=400, detail="No trained model to save")
    
    from services.model_manager import ModelManager
    
    manager = ModelManager()
    best = session["ml_pipeline"]._best_model
    
    version = manager.save_model(
        user_id=str(current_user.id),
        project_id=request.project_id,
        model=best.model,
        metrics={"cv_mean": best.cv_mean, "cv_std": best.cv_std},
        hyperparameters=best.hyperparameters,
        feature_names=session["X_train"].columns.tolist(),
        target_name=session["target_column"],
        training_data=session["data"],
        preprocessing_pipeline=session["preprocessing_pipeline"],
        notes=request.notes
    )
    
    return ModelVersionInfo(
        version_id=version.version_id,
        algorithm=version.algorithm,
        created_at=version.created_at,
        status=version.status.value,
        metrics=version.metrics,
        notes=version.notes
    )


@mlops_router.get("/versions/{project_id}", response_model=List[ModelVersionInfo])
async def list_versions(
    project_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    List all versions for a project.
    """
    from services.model_manager import ModelManager
    
    manager = ModelManager()
    versions = manager.list_versions(str(current_user.id), project_id)
    
    return [
        ModelVersionInfo(
            version_id=v.version_id,
            algorithm=v.algorithm,
            created_at=v.created_at,
            status=v.status.value,
            metrics=v.metrics,
            notes=v.notes
        )
        for v in versions
    ]


@mlops_router.post("/export-model", response_model=ExportModelResponse)
async def export_model(
    request: ExportModelRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Export model package for deployment.
    """
    from services.model_manager import ModelManager
    
    manager = ModelManager()
    
    try:
        export_path = manager.export_model(
            user_id=str(current_user.id),
            project_id=request.project_id,
            version_id=request.version_id
        )
        
        files = [f.name for f in export_path.iterdir()]
        
        return ExportModelResponse(
            success=True,
            export_path=str(export_path),
            files=files,
            message="Model exported successfully"
        )
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@mlops_router.post("/rollback", response_model=Dict[str, Any])
async def rollback_version(
    request: RollbackRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Rollback to a previous model version.
    """
    from services.model_manager import ModelManager
    
    manager = ModelManager()
    
    try:
        success = manager.rollback(
            user_id=str(current_user.id),
            project_id=request.project_id,
            target_version_id=request.version_id
        )
        
        return {
            "success": success,
            "message": f"Rolled back to version {request.version_id}"
        }
        
    except Exception as e:
        logger.error(f"Rollback error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@mlops_router.post("/retrain", response_model=RetrainResponse)
async def retrain_model(
    request: RetrainModelRequest,
    current_user: User = Depends(get_current_user),
    session: Dict = Depends(get_session)
):
    """
    Retrain model with new data or tuned hyperparameters.
    """
    if session["X_train"] is None:
        raise HTTPException(status_code=400, detail="No training data")
    
    from services.model_manager import ModelManager
    from services.ml_pipeline import MLPipeline, AlgorithmType as AT, TrainingConfig
    from services.hyperparameter_tuning import HyperparameterTuner, TuningConfig
    
    manager = ModelManager()
    
    try:
        # Load previous version
        model, metadata, preprocessing = manager.load_model(
            str(current_user.id), request.project_id, request.version_id
        )
        
        previous_version_id = request.version_id or metadata.get("version_id", "unknown")
        algorithm_name = metadata["algorithm"]
        
        # Map algorithm
        algo_map = {
            "RandomForestClassifier": AT.RANDOM_FOREST_CLF,
            "RandomForestRegressor": AT.RANDOM_FOREST_REG,
            "LogisticRegression": AT.LOGISTIC_REGRESSION,
            "XGBClassifier": AT.XGBOOST_CLF,
            "XGBRegressor": AT.XGBOOST_REG,
            "Ridge": AT.RIDGE,
            "Lasso": AT.LASSO,
            "LinearRegression": AT.LINEAR_REGRESSION,
            "SVC": AT.SVM_CLF,
        }
        
        algorithm = algo_map.get(algorithm_name)
        if not algorithm:
            raise ValueError(f"Unknown algorithm: {algorithm_name}")
        
        # Optionally tune hyperparameters
        best_params = metadata.get("hyperparameters", {})
        
        if request.tune_hyperparameters and request.tuning_config:
            tuner = HyperparameterTuner()
            tuning_result = tuner.tune(
                session["X_train"],
                session["y_train"],
                algorithm
            )
            best_params = tuning_result.best_params
        
        # Train with new data
        pipeline = MLPipeline()
        config = TrainingConfig(
            algorithm=algorithm,
            hyperparameters=best_params
        )
        
        result = pipeline.train(
            session["X_train"],
            session["y_train"],
            config
        )
        
        # Save new version
        new_version = manager.save_model(
            user_id=str(current_user.id),
            project_id=request.project_id,
            model=result.model,
            metrics={"cv_mean": result.cv_mean, "cv_std": result.cv_std},
            hyperparameters=best_params,
            feature_names=session["X_train"].columns.tolist(),
            target_name=session["target_column"],
            preprocessing_pipeline=session["preprocessing_pipeline"],
            notes=request.notes,
            parent_version=previous_version_id
        )
        
        return RetrainResponse(
            success=True,
            new_version_id=new_version.version_id,
            previous_version_id=previous_version_id,
            metrics_comparison={
                "previous": metadata.get("metrics", {}),
                "new": {"cv_mean": result.cv_mean, "cv_std": result.cv_std}
            },
            message="Model retrained successfully"
        )
        
    except Exception as e:
        logger.error(f"Retrain error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@mlops_router.post("/predict", response_model=PredictResponse)
async def predict(
    request: PredictRequest,
    current_user: User = Depends(get_current_user)
):
    """
    Make predictions using saved model.
    """
    from services.model_manager import ModelManager
    
    manager = ModelManager()
    
    try:
        inference_pipeline = manager.create_inference_pipeline(
            str(current_user.id), request.project_id, request.version_id
        )
        
        df = pd.DataFrame(request.data)
        predictions = inference_pipeline.predict(df)
        
        probabilities = None
        try:
            probabilities = inference_pipeline.predict_proba(df).tolist()
        except:
            pass
        
        return PredictResponse(
            success=True,
            predictions=predictions.tolist(),
            probabilities=probabilities,
            message="Predictions generated successfully"
        )
        
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))




@router.get("/session-state", response_model=SessionState)
async def get_session_state(session: Dict = Depends(get_session)):
    """
    Get current session state.
    """
    return SessionState(
        has_data=session["data"] is not None,
        data_shape=list(session["data"].shape) if session["data"] is not None else None,
        data_source=session.get("source_type"),
        target_selected=session["target_column"] is not None,
        target_column=session["target_column"],
        problem_type=session["problem_type"],
        preprocessing_done=session["preprocessing_pipeline"] is not None,
        models_trained=len(session["trained_models"]),
        best_model=session["best_model"]
    )


@router.post("/reset-session")
async def reset_session(session_id: str = None):
    """
    Reset current session.
    """
    session_store.clear(session_id)
    return {"success": True, "message": "Session reset"}


# ============================================================================
# Explainability (SHAP & LLM Narratives)
# ============================================================================

from fastapi import Body

@mlops_router.get("/explain/global")
async def explain_global(session: Dict = Depends(get_session)):
    """Computes global feature importance across dataset."""
    if not session.get("best_model") or session.get("X_train") is None:
        raise HTTPException(status_code=400, detail="No best model or training data available.")
        
    from services.explainability import ExplainabilityService
    best_model = session["ml_pipeline"]._best_model
    try:
        service = ExplainabilityService(best_model.model)
        result = service.get_global_explanation(session["X_train"])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@mlops_router.post("/explain/local")
async def explain_local(data_point: Dict[str, Any] = Body(...), session: Dict = Depends(get_session)):
    """Computes SHAP values per feature for a single input."""
    if not session.get("best_model"):
        raise HTTPException(status_code=400, detail="No best model available.")
        
    import pandas as pd
    from services.explainability import ExplainabilityService
    best_model = session["ml_pipeline"]._best_model
    
    try:
        input_df = pd.DataFrame([data_point])
        service = ExplainabilityService(best_model.model)
        result = service.explain_prediction(input_df)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@mlops_router.post("/explain/narrative")
async def explain_narrative(local_explanation: Dict[str, Any] = Body(...)):
    """Translates SHAP outputs into human-readable explanations using LLM."""
    from services.explainability import generate_narrative_explanation
    narrative = generate_narrative_explanation(local_explanation)
    return {"narrative": narrative}

@mlops_router.get("/explain/model-comparison")
async def explain_model_comparison(session: Dict = Depends(get_session)):
    """Compares top models from AutoML using LLM narrative."""
    if len(session.get("trained_models", [])) < 2:
        raise HTTPException(status_code=400, detail="Need at least 2 trained models to compare.")
        
    from services.explainability import compare_models_narrative
    models = sorted(session["trained_models"], key=lambda m: m.cv_mean, reverse=True)[:2]
    m1, m2 = models[0], models[1]
    
    m1_meta = {"name": m1.algorithm, "metrics": {"cv_mean": m1.cv_mean, "cv_std": m1.cv_std}}
    m2_meta = {"name": m2.algorithm, "metrics": {"cv_mean": m2.cv_mean, "cv_std": m2.cv_std}}
    
    narrative = compare_models_narrative(m1_meta, m2_meta)
    return {"comparison_narrative": narrative, "model1": m1_meta, "model2": m2_meta}


# ============================================================================
# Benchmarks
# ============================================================================

from pydantic import BaseModel

class BenchmarkRequest(BaseModel):
    target_column: str
    dataset_name: str = "custom_dataset"

@mlops_router.post("/benchmark/run")
async def benchmark_run(request: BenchmarkRequest, session: Dict = Depends(get_session)):
    """Run parallel AutoML model benchmarking securely."""
    if session.get("data") is None:
        raise HTTPException(status_code=400, detail="No dataset uploaded in session.")
        
    from services.benchmark import benchmark_service
    df = session["data"]
    
    if request.target_column not in df.columns:
        raise HTTPException(status_code=400, detail="Target column not found.")
        
    y = df[request.target_column]
    X = df.drop(columns=[request.target_column])
    
    # Optional encoding drop logic
    X = X.select_dtypes(exclude=['object', 'category'])
    
    job_id = benchmark_service.start_benchmark(request.dataset_name, X, y, request.target_column)
    return {"job_id": job_id, "message": "Benchmark job started correctly."}

@mlops_router.get("/benchmark/status/{job_id}")
async def benchmark_status(job_id: str):
    """Get the current progress of a benchmark job actively formatting models."""
    from services.benchmark import benchmark_service
    return benchmark_service.get_status(job_id)

@mlops_router.get("/benchmark/results/{job_id}")
async def benchmark_results(job_id: str):
    """Fetches full benchmark reports and formatted ChartJS data natively."""
    from services.benchmark import benchmark_service
    res = benchmark_service.get_results(job_id)
    if res.get("status") != "completed":
        raise HTTPException(status_code=404, detail="Benchmark not completed or not found.")
    return res


# ============================================================================
# Preprocessing Module Endpoints (New)
# ============================================================================

@router.get("/datasets", response_model=List[DatasetResponse])
async def get_datasets(
    latest_only: bool = Query(True, description="Only return the latest versions of each dataset group"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Fetch all datasets for the authenticated user."""
    from services.preprocessing_service import PreprocessingService
    if latest_only:
        return PreprocessingService.get_latest_datasets(db, current_user.id)
    return PreprocessingService.get_user_datasets(db, current_user.id)


@router.get("/datasets/{dataset_id}/versions", response_model=List[DatasetResponse])
async def get_dataset_versions(
    dataset_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Fetch all versions of a specific dataset lineage."""
    from services.preprocessing_service import PreprocessingService
    return PreprocessingService.get_dataset_versions(db, dataset_id, current_user.id)




@router.get("/preprocess/status/{job_id}", response_model=JobStatusResponse)
async def get_preprocessing_status(
    job_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Get the status of a preprocessing job.
    Uses unified JobStatusResponse for consistency.
    """
    from celery.result import AsyncResult
    from .celery_app import celery_app
    
    res = AsyncResult(job_id, app=celery_app)
    
    status_map = {
        "PENDING": JobStatus.PENDING,
        "STARTED": JobStatus.RUNNING,
        "PROGRESS": JobStatus.RUNNING,
        "SUCCESS": JobStatus.SUCCESS,
        "FAILURE": JobStatus.FAILURE
    }
    
    # Extract metadata if available
    meta = res.info if isinstance(res.info, dict) else {}
    
    return JobStatusResponse(
        job_id=job_id,
        status=status_map.get(res.state, JobStatus.PENDING),
        algorithm="Data Preprocessing",
        progress=meta.get("progress", 0.0) / 100.0 if res.state == "PROGRESS" else (1.0 if res.state == "SUCCESS" else 0.0),
        current_step=meta.get("status", res.state),
        result=res.result if res.state == "SUCCESS" else None,
        error=str(res.result) if res.state == "FAILURE" else None
    )

# ============================================================================
# API Ingestion Endpoints (New)
# ============================================================================

@router.post("/ingest/api/preview")
async def api_preview(
    request: APIFetchRequest,
    current_user: User = Depends(get_current_user)
):
    """Fetch external API data and return a row preview."""
    try:
        response = requests.get(request.api_url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Ensure it's tabular-convertible
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list):
                    data = val
                    break
        
        if not isinstance(data, list):
            raise HTTPException(status_code=400, detail="API response is not a valid list of objects.")
            
        df = pd.DataFrame(data)
        return {
            "preview": df.head(10).to_dict(orient="records"),
            "row_count": len(df),
            "col_count": len(df.columns)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"API Fetch Error: {str(e)}")


@router.post("/ingest/api")
async def api_ingest(
    request: APIIngestRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Fetch API, convert to CSV, and save via StorageService (Mandatory).
    """
    if not re.match(r'^[\w\s-]+$', request.dataset_name):
        raise HTTPException(status_code=400, detail="Invalid character in dataset name.")

    try:
        response = requests.get(request.api_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Tabular normalize
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list):
                    data = val
                    break
        
        df = pd.DataFrame(data)
        if df.empty:
            raise HTTPException(status_code=400, detail="API returned an empty dataset.")
        
        # Mandatory Path: uploads/{user_id}/{dataset_id}/v1.csv
        dataset_uuid = uuid.uuid4()
        user_id_str = str(current_user.id)
        storage_filename = "v1.csv"
        user_storage_dir = os.path.join("uploads", user_id_str, str(dataset_uuid))
        storage_path = os.path.join(user_storage_dir, storage_filename)
        
        # Save via StorageService
        storage_service.write_df(storage_path, df)
        
        from models.dataset import Dataset
        new_dataset = Dataset(
            id=dataset_uuid,
            user_id=current_user.id,
            name=request.dataset_name,
            filename=f"v1_{request.dataset_name}.csv",
            storage_path=storage_path,
            file_size=storage_service.get_file_size(storage_path),
            status="raw",
            row_count=len(df),
            col_count=len(df.columns),
            version=1,
            is_latest=True
        )
        db.add(new_dataset)
        db.commit()
        
        return {"success": True, "dataset_id": str(dataset_uuid)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"API Ingestion Failed: {str(e)}")


@router.get("/leaderboard/{dataset_id}", response_model=Dict[str, Any])
async def get_leaderboard(
    dataset_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Fetch and rank all trained models for a specific dataset from MLflow.
    
    - Filters by dataset_id (UUID string)
    - Automatically discovers task type (classification/regression)
    - Ranks by accuracy (CLF) or R2 (REG)
    - Multi-tenant isolated (User ID verified)
    """
    try:
        user_id_str = str(current_user.id)
        # Requirement: Search MLflow and return ranked results
        leaderboard = mlflow_manager.get_leaderboard(user_id_str, dataset_id)
        
        return {
            "leaderboard": leaderboard,
            "dataset_id": dataset_id,
            "count": len(leaderboard)
        }
    except Exception as e:
        logger.error(f"Leaderboard API Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch model leaderboard from MLflow server.")


@health_router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Step 4: Health Audit
    Used by Docker orchestration to verify system availability.
    """
    return HealthResponse(
        status="ok",
        version="2.0.0-stable",
        services={
            "api": "online",
            "storage": "active",
            "ml_engine": "ready"
        },
        timestamp=datetime.utcnow().isoformat()
    )
