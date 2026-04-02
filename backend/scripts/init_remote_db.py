import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api.database import engine, Base
from models.user import User  # This registers the User model with Base.metadata

print("🔍 Starting manual table verification for Supabase...")
try:
    # Explicitly check what tables are in metadata
    print(f"📦 Registered models: {list(Base.metadata.tables.keys())}")
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    print("✅ Tables initialized successfully onto Supabase schema!")
    
except Exception as e:
    print(f"❌ Initialization FAILED: {str(e)}")
    sys.exit(1)
