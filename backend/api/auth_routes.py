from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from typing import Dict

from api.database import get_db
from services.auth_service import AuthService
from api.schemas import UserRegister, UserLogin, Token

router = APIRouter(prefix="/api/v2/auth", tags=["Authentication"])

@router.post("/register", status_code=status.HTTP_201_CREATED)
def register(user_in: UserRegister, db: Session = Depends(get_db)):
    """Register a new user to enable multi-tenant access."""
    AuthService.register_user(db, email=user_in.email, password=user_in.password)
    return {"message": "User created successfully"}

@router.post("/login", response_model=Token)
def login(user_in: UserLogin, db: Session = Depends(get_db)):
    """Authenticate and obtain JWT bearer token for API access."""
    token = AuthService.login_user(db, email=user_in.email, password=user_in.password)
    return {"access_token": token, "token_type": "bearer"}
