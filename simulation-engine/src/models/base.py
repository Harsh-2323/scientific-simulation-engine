"""
SQLAlchemy declarative base for all ORM models.

All models in the application inherit from this Base class,
which provides the metadata registry for table definitions
and Alembic migration generation.
"""

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models in the simulation engine."""

    pass
