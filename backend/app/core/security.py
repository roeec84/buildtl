"""
Security utilities for password hashing and JWT token management.
"""
from datetime import datetime, timedelta
from typing import Any, Optional
from jose import jwt, JWTError
from passlib.context import CryptContext
from app.core.config import settings


# Password hashing context using bcrypt
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain password against a hashed password.

    Args:
        plain_password: The plain text password from user input
        hashed_password: The hashed password from database

    Returns:
        True if password matches, False otherwise
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    Hash a password using bcrypt.

    Args:
        password: Plain text password

    Returns:
        Hashed password string
    """
    return pwd_context.hash(password)


def create_access_token(data: dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token.

    Args:
        data: Dictionary containing claims to encode in the token (e.g., {"sub": user_id})
        expires_delta: Optional custom expiration time

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    # Set expiration time
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})

    print(f"[CREATE_TOKEN] Using SECRET_KEY (first 10 chars): {settings.SECRET_KEY[:10]}...")
    print(f"[CREATE_TOKEN] Using ALGORITHM: {settings.ALGORITHM}")
    print(f"[CREATE_TOKEN] Payload to encode: {to_encode}")

    # Encode the token
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict[str, Any]]:
    """
    Decode and validate a JWT token.

    Args:
        token: JWT token string

    Returns:
        Dictionary containing token payload if valid, None otherwise
    """
    try:
        print(f"[DECODE] Using SECRET_KEY (first 10 chars): {settings.SECRET_KEY[:10]}...")
        print(f"[DECODE] Using ALGORITHM: {settings.ALGORITHM}")
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        print(f"[DECODE] Successfully decoded: {payload}")
        return payload
    except JWTError as e:
        print(f"[DECODE] JWT decode error: {e}")
        return None


# --- Encryption Helpers ---
from cryptography.fernet import Fernet
import base64
import hashlib

def _get_fernet() -> Fernet:
    """
    Get Fernet instance with key derived from SECRET_KEY.
    Ensures a valid 32-byte url-safe base64-encoded key.
    """
    # Derive a 32-byte key from the secret key using SHA256
    key = hashlib.sha256(settings.SECRET_KEY.encode()).digest()
    # Base64 encode it to make it URL-safe for Fernet
    fern_key = base64.urlsafe_b64encode(key)
    return Fernet(fern_key)


def encrypt_value(value: str) -> str:
    """
    Encrypt a string value.
    
    Args:
        value: The string to encrypt
        
    Returns:
        Encrypted string (base64 encoded)
    """
    if not value:
        return value
        
    f = _get_fernet()
    return f.encrypt(value.encode()).decode()


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt an encrypted string value.
    
    Args:
        encrypted_value: The encrypted string
        
    Returns:
        Decrypted string
    """
    if not encrypted_value:
        return encrypted_value
        
    try:
        f = _get_fernet()
        return f.decrypt(encrypted_value.encode()).decode()
    except Exception as e:
        print(f"Decryption error: {e}")
        # In case it wasn't encrypted or key changed, return original? 
        # Or better to raise/return empty to be safe?
        # For this use case, if it fails, it's likely bad data.
        return encrypted_value # Fallback for now to support non-encrypted legacy data

