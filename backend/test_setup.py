"""
Quick test script to verify backend setup.
Run this to check if all imports work correctly.
"""
import asyncio
from app.core.config import settings
from app.core.security import get_password_hash, verify_password
from app.db.database import init_db


async def test_config():
    """Test configuration"""
    print("🔧 Testing Configuration...")
    print(f"   App Name: {settings.APP_NAME}")
    print(f"   Environment: {settings.ENVIRONMENT}")
    print(f"   Debug: {settings.DEBUG}")
    print(f"   Database URL: {settings.DATABASE_URL}")
    print("   ✅ Configuration OK\n")


def test_security():
    """Test password hashing"""
    print("🔐 Testing Security...")
    password = "test_password_123"
    hashed = get_password_hash(password)
    print(f"   Original: {password}")
    print(f"   Hashed: {hashed[:50]}...")

    is_valid = verify_password(password, hashed)
    print(f"   Verification: {'✅ PASS' if is_valid else '❌ FAIL'}")

    is_invalid = verify_password("wrong_password", hashed)
    print(f"   Wrong password rejected: {'✅ PASS' if not is_invalid else '❌ FAIL'}\n")


async def test_database():
    """Test database initialization"""
    print("🗄️  Testing Database...")
    try:
        await init_db()
        print("   ✅ Database initialized successfully\n")
    except Exception as e:
        print(f"   ❌ Database error: {e}\n")


async def main():
    """Run all tests"""
    print("\n" + "="*50)
    print("🧪 Backend Setup Test")
    print("="*50 + "\n")

    await test_config()
    test_security()
    await test_database()

    print("="*50)
    print("✅ All tests completed!")
    print("="*50 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
