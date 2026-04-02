from core.database import SessionLocal, engine, Base

# Dependency to get DB session (FastAPI Injection)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
