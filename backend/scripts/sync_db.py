import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import DATABASE_URL, engine

def sync_schema():
    """
    Manually ensures all required columns exist in Supabase datasets table.
    SQLAlchemy create_all doesn't add columns to existing tables.
    """
    print(f"🔄 Auditing Schema for {DATABASE_URL.split('@')[-1]}...")
    
    with engine.connect() as conn:
        # Check datasets table
        try:
            # We add columns IF NOT EXISTS. (Postgres 9.6+)
            # Note: Postgres doesn't have native "ADD COLUMN IF NOT EXISTS" in one line easily for all types, 
            # but we can use a PL/pgSQL block or just catch errors.
            
            alter_queries = [
                # New Columns
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS name VARCHAR;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS filename VARCHAR;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS storage_path VARCHAR;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS file_size FLOAT;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'raw';",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS row_count INTEGER;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS col_count INTEGER;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS root_dataset_id UUID;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS parent_dataset_id UUID;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS is_latest BOOLEAN DEFAULT TRUE;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW();",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS target_column VARCHAR;",
                "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS problem_type VARCHAR;",
                
                # Cleanup legacy NOT NULL constraints from conflicting old schema
                "ALTER TABLE datasets ALTER COLUMN dataset_name DROP NOT NULL;",
                "ALTER TABLE datasets ALTER COLUMN file_path DROP NOT NULL;",
                "ALTER TABLE datasets ALTER COLUMN source DROP NOT NULL;"
            ]
            
            print("📦 Checking 'datasets' table columns...")
            for q in alter_queries:
                try:
                    conn.execute(text(q))
                    conn.commit()
                except Exception as e:
                    # Column might already exist, Postgres might be sensitive depending on version
                    print(f"   ℹ️ {q.split(' ')[5]} check: {str(e).splitlines()[0]}")
            
            print("✅ Schema Audit Complete!")
            
        except Exception as e:
            print(f"❌ Failed to audit schema: {e}")

if __name__ == "__main__":
    sync_schema()
