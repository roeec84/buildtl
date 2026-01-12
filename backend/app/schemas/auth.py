"""
Pydantic schemas for authentication requests and responses.
"""
from pydantic import BaseModel, EmailStr, Field


class UserLogin(BaseModel):
    """Request schema for user login"""
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)


class UserCreate(BaseModel):
    """Request schema for user registration"""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=6)
    organization: str | None = None


class UserResponse(BaseModel):
    """Response schema for user data (without password)"""
    id: int
    username: str
    email: str
    organization: str | None
    is_org_admin: bool
    is_active: bool

    model_config = {"from_attributes": True}  # Allows ORM model conversion


class Token(BaseModel):
    """Response schema for JWT token"""
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Schema for decoded token data"""
    user_id: int | None = None
