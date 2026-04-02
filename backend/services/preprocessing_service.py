import os
import pandas as pd
import logging
import uuid
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from models.dataset import Dataset
from services.preprocessing import PreprocessingPipeline, PreprocessingConfig, MissingValueStrategy, ScalingStrategy, EncodingStrategy

logger = logging.getLogger(__name__)

from services.storage_service import storage_service
from services.intelligent_preprocessing import IntelligentPipeline, analyze_dataset

class PreprocessingService:
    @staticmethod
    def get_user_datasets(db: Session, user_id: uuid.UUID):
        """Fetch all datasets belonging to a specific user (including historical versions)."""
        return db.query(Dataset).filter(Dataset.user_id == user_id).order_by(Dataset.created_at.desc()).all()

    @staticmethod
    def get_dataset_by_id(db: Session, dataset_id: uuid.UUID, user_id: uuid.UUID):
        """Fetch a single dataset version with user isolation."""
        return db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()

    @staticmethod
    def preprocess_dataset(
        db: Session, 
        dataset_id: uuid.UUID, 
        user_id: uuid.UUID,
        options: Dict[str, bool]
    ) -> uuid.UUID:
        """
        Perform preprocessing on a dataset version and create a NEW version artifact.
        
        Production Rules:
        1. Always maintain root_dataset_id (Audit 1A)
        2. Version = parent.version + 1 (Audit 1B)
        3. Transaction-safe is_latest update (Audit 1C)
        """
        # Fetch parent with isolation
        parent_dataset = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
        if not parent_dataset:
            raise ValueError("Dataset not found or access denied")

        # Define lineage root (v1 is its own root)
        root_id = parent_dataset.root_dataset_id if parent_dataset.root_dataset_id else parent_dataset.id

        # 1. Load data via StorageService
        df = storage_service.read_df(parent_dataset.storage_path)
        
        # 2. Pipeline Application
        config = PreprocessingConfig(
            missing_strategy=MissingValueStrategy.MEDIAN if options.get("handle_missing") else MissingValueStrategy.DROP,
            scaling_strategy=ScalingStrategy.STANDARD if options.get("scale_numerical") else ScalingStrategy.NONE,
            encoding_strategy=EncodingStrategy.ONEHOT if options.get("encode_categorical") else EncodingStrategy.NONE,
            outlier_handling=options.get("remove_outliers", False),
            drop_constant_columns=True
        )
        pipeline = PreprocessingPipeline(config)
        if options.get("remove_duplicates"):
            df = df.drop_duplicates()
        df_processed = pipeline.fit_transform(df)
        
        # 3. Save processed file as a NEW version
        new_version_num = parent_dataset.version + 1
        storage_filename = f"v{new_version_num}.csv"
        user_id_str = str(user_id)
        
        # Mandatory Path: uploads/{user_id}/{root_id}/v{version}.csv
        storage_path = os.path.join("uploads", user_id_str, str(root_id), storage_filename)
        
        # Write via StorageService (Includes Overwrite Protection Audit 2B)
        storage_service.write_df(storage_path, df_processed)
        
        # 4. Atomic Database Update (Audit 1C)
        try:
            # Mark all in this lineage as NOT latest
            db.query(Dataset).filter(
                Dataset.user_id == user_id, 
                Dataset.root_dataset_id == root_id
            ).update({"is_latest": False})
            
            # Special case for v1 (it might not have root_dataset_id set yet in legacy)
            db.query(Dataset).filter(Dataset.id == root_id).update({"is_latest": False, "root_dataset_id": root_id})

            new_dataset_id = uuid.uuid4()
            new_dataset = Dataset(
                id=new_dataset_id,
                user_id=user_id,
                name=parent_dataset.name,
                filename=f"v{new_version_num}_{parent_dataset.name}.csv",
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

            db.add(new_dataset)
            db.commit()
            db.refresh(new_dataset)
            return new_dataset.id
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to commit new dataset version: {str(e)}")
            raise e

    @staticmethod
    def automated_preprocessing(
        db: Session, 
        dataset_id: uuid.UUID, 
        user_id: uuid.UUID,
        target: Optional[str] = None
    ) -> uuid.UUID:
        """
        Production-grade Automated Preprocessing Orchestrator.
        Requirement: Analyze -> Detect -> Clean -> Transform -> Version
        """
        # 1. Fetch parent and lineage
        parent_dataset = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
        if not parent_dataset:
            raise ValueError("Dataset not found")
        
        root_id = parent_dataset.root_dataset_id or parent_dataset.id
        
        # 2. Load via StorageService
        df = storage_service.read_df(parent_dataset.storage_path)
        
        # 3. Intelligent Pipeline Run (Modular internal logic)
        df_processed = IntelligentPipeline.run(df, target)
        
        # 4. Save NEW version
        new_version_num = parent_dataset.version + 1
        storage_filename = f"v{new_version_num}_auto.csv"
        user_id_str = str(user_id)
        storage_path = os.path.join("uploads", user_id_str, str(root_id), storage_filename)
        
        storage_service.write_df(storage_path, df_processed)
        
        # 5. Database Update (Atomic)
        try:
            db.query(Dataset).filter(Dataset.root_dataset_id == root_id).update({"is_latest": False})
            
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
            
            db.add(new_dataset)
            db.commit()
            return new_dataset.id
        except Exception as e:
            db.rollback()
            raise e

    @staticmethod
    def get_latest_datasets(db: Session, user_id: uuid.UUID):
        """Fetch all unique datasets (latest versions only) for a user."""
        return db.query(Dataset).filter(
            Dataset.user_id == user_id, 
            Dataset.is_latest == True
        ).order_by(Dataset.created_at.desc()).all()

    @staticmethod
    def get_dataset_versions(db: Session, dataset_id: uuid.UUID, user_id: uuid.UUID):
        """Fetch all versions of a specific dataset lineage."""
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id, Dataset.user_id == user_id).first()
        if not dataset:
            return []
        
        return db.query(Dataset).filter(
            Dataset.user_id == user_id, 
            Dataset.name == dataset.name
        ).order_by(Dataset.version.desc()).all()
