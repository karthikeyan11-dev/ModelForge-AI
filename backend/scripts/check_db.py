import sys
import os
from sqlalchemy import create_engine, inspect

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import engine

def check_columns():
    inspector = inspect(engine)
    columns = inspector.get_columns('datasets')
    print("Columns in 'datasets' table:")
    for column in columns:
        print(f" - {column['name']} ({column['type']})")

if __name__ == "__main__":
    check_columns()
