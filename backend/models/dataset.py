from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey, Float, Integer
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid
from core.database import Base

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    name = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    storage_path = Column(String, nullable=False)
    file_size = Column(Float, nullable=True) # in bytes
    status = Column(String, default="raw") # raw, processing, processed, error
    row_count = Column(Integer, nullable=True)
    col_count = Column(Integer, nullable=True)
    
    # Versioning System (Audit-Ready Lineage)
    version = Column(Integer, default=1, nullable=False)
    # The original "v1" dataset ID for this artifact lineage
    root_dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)
    # The direct predecessor
    parent_dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id"), nullable=True)
    is_latest = Column(Boolean, default=True, nullable=False)
    
    # ML Metadata (Target Awareness)
    target_column = Column(String, nullable=True)
    problem_type = Column(String, nullable=True) # classification, regression, unsupervised
    pipeline_path = Column(String, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship to user is optional but good for joins
    # user = relationship("User", back_populates="datasets")

    def __repr__(self):
        return f"<Dataset(name={self.name}, status={self.status}, user_id={self.user_id})>"
