from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from api.database import get_db
from core.security import verify_token
from typing import Any

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v2/auth/login")

def get_current_user(
    db: Session = Depends(get_db),
    token: str = Depends(oauth2_scheme)
) -> Any:
    """Dependency injection to fetch the currently authenticated user from DB."""
    from models.user import User  # LAZY IMPORT to break circular dependency
    
    user_id = verify_token(token)
    
    # Check if user exists and is active
    user = db.query(User).get(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not find user associated with this token",
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is disabled",
        )
    return user
