"""add title column to prompt table if missing

Revision ID: fix_prompt_title
Revises: 018012973d35
Create Date: 2026-02-02

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = "fix_prompt_title"
down_revision: Union[str, None] = "018012973d35"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add title column to prompt table if it doesn't exist."""
    conn = op.get_bind()
    inspector = inspect(conn)
    
    # Check if prompt table exists
    tables = inspector.get_table_names()
    if "prompt" not in tables:
        return
    
    # Check if title column already exists
    columns = [col["name"] for col in inspector.get_columns("prompt")]
    if "title" not in columns:
        op.add_column("prompt", sa.Column("title", sa.Text(), nullable=True))
    
    # Also add access_control if missing (required by model)
    if "access_control" not in columns:
        op.add_column("prompt", sa.Column("access_control", sa.JSON(), nullable=True))


def downgrade() -> None:
    """Remove title and access_control columns from prompt table."""
    conn = op.get_bind()
    inspector = inspect(conn)
    
    tables = inspector.get_table_names()
    if "prompt" not in tables:
        return
    
    columns = [col["name"] for col in inspector.get_columns("prompt")]
    
    if "title" in columns:
        op.drop_column("prompt", "title")
    if "access_control" in columns:
        op.drop_column("prompt", "access_control")
