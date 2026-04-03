import sys
import os
from sqlalchemy import create_engine, text

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import engine

def migrate_db():
    print("🔄 Migrating database schema...")
    with engine.connect() as conn:
        try:
            # 1. Add missing pipeline_path column (from previous update)
            query_pipeline = "ALTER TABLE datasets ADD COLUMN IF NOT EXISTS pipeline_path VARCHAR;"
            conn.execute(text(query_pipeline))
            print("✅ Checked 'pipeline_path' in 'datasets' table")
            
            # 2. Add username column to users table
            query_username = "ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR;"
            conn.execute(text(query_username))
            print("✅ Checked 'username' in 'users' table")
            
            # 3. Update existing users with username = "Karthikeyan"
            # Target emails: kishoreguyz7@gmail.com, karthiblizz@gmail.com
            update_query = """
                UPDATE users SET username = 'Karthikeyan' 
                WHERE email IN ('kishoreguyz7@gmail.com', 'karthiblizz@gmail.com')
            """
            conn.execute(text(update_query))
            conn.commit()
            print("✅ Initialized username for core developers")
            
            print("✅ Database migration complete!")
        except Exception as e:
            print(f"❌ Migration failed: {e}")

if __name__ == "__main__":
    migrate_db()
