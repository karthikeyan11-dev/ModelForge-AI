from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from typing import Dict

from api.database import get_db
from services.auth_service import AuthService
from api.schemas import UserRegister, UserLogin, Token, UserResponse
from models.user import User
from api.auth import get_current_user

router = APIRouter(prefix="/api/v2/auth", tags=["Authentication"])

@router.post("/register", status_code=status.HTTP_201_CREATED)
def register(user_in: UserRegister, db: Session = Depends(get_db)):
    """Register a new user to enable multi-tenant access."""
    AuthService.register_user(
        db, 
        email=user_in.email, 
        password=user_in.password, 
        username=user_in.username
    )
    return {"message": "User created successfully"}

@router.post("/login", response_model=Token)
def login(user_in: UserLogin, db: Session = Depends(get_db)):
    """Authenticate and obtain JWT bearer token for API access."""
    token, username = AuthService.login_user(db, email=user_in.email, password=user_in.password)
    return {"access_token": token, "token_type": "bearer", "username": username}

@router.get("/profile", response_model=UserResponse)
def get_profile(user: User = Depends(get_current_user)):
    """Retrieve the currently authenticated user's profile information."""
    return user
