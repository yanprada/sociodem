"""Database connection setup for FastAPI with SQLAlchemy and asyncpg"""

import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Database connection URL
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+asyncpg://user:password@localhost/geodata"
)

# Create async engine
engine = create_async_engine(DATABASE_URL, echo=True)

# Create a session factory
async_session = sessionmaker(  # type: ignore
    bind=engine,  # type: ignore
    class_=AsyncSession,
    expire_on_commit=False,
)


# Dependency for routes
async def get_db():
    """Dependency to get a database session."""
    async with async_session() as session:  # type: ignore
        yield session
