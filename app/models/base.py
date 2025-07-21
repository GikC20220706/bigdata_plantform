"""
TODO 大数据平台的基础数据库模型和通用组件。 此模块提供了基础模型类和公共数据库组件，这些组件在应用程序中的所有模型中共享。
"""

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import Column, DateTime, Integer, Text
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.sql import func


@as_declarative()
class Base:
    """Base class for all database models."""

    id: Any
    __name__: str

    # Generate table name automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="Record creation timestamp"
    )

    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="Record last update timestamp"
    )


class BaseModel(Base, TimestampMixin):
    """
    Base model with common fields for all entities.

    Provides:
    - Auto-generated primary key
    - Created and updated timestamps
    - Common utility methods
    """
    __abstract__ = True

    id = Column(
        Integer,
        primary_key=True,
        index=True,
        autoincrement=True,
        comment="Primary key"
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary."""
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        }

    def update_from_dict(self, data: Dict[str, Any]) -> None:
        """Update model instance from dictionary."""
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)

    @classmethod
    def get_table_name(cls) -> str:
        """Get the table name for this model."""
        return cls.__tablename__

    def __repr__(self) -> str:
        """String representation of the model."""
        return f"<{self.__class__.__name__}(id={self.id})>"