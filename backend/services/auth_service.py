import logging
from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from models.user import User
from core.security import hash_password, verify_password, create_access_token
from pydantic import EmailStr

logger = logging.getLogger(__name__)

class AuthService:
    """Service for handling authentication and user registration."""
    
    @staticmethod
    def register_user(db: Session, email: EmailStr, password: str, username: str) -> User:
        """Register a new user in the system."""
        try:
            logger.info(f"Attempting to register user: {email} ({username})")
            
            # Check for existing user
            db_user = db.query(User).filter(User.email == email).first()
            if db_user:
                logger.warning(f"Registration failed: Email {email} already exists")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="User with this email already registered"
                )
            
            # Create new user
            new_user = User(
                email=email,
                username=username,
                hashed_password=hash_password(password)
            )
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            
            logger.info(f"User registered successfully: {email}")
            return new_user
            
        except HTTPException:
            # Re-raise explicit HTTP exceptions
            raise
        except Exception as e:
            from sqlalchemy.exc import SQLAlchemyError
            if isinstance(e, SQLAlchemyError):
                logger.error(f"Database error during registration for {email}: {str(e)}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="A database error occurred. The 'users' table may be missing or the connection failed."
                )
            
            logger.error(f"Critical error during registration for {email}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="A critical error occurred while creating your account."
            )

    @staticmethod
    def login_user(db: Session, email: EmailStr, password: str) -> tuple[str, str]:
        """Authenticate user and return JWT access token and username."""
        user = db.query(User).filter(User.email == email).first()
        
        # 401: Invalid credentials
        if not user or not verify_password(password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Check if active
        if not user.is_active:
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User account is inactive"
            )

        # Generate access token with user.id as subject
        token = create_access_token(subject=str(user.id))
        return token, user.username
