"""
Migration v001: Add doc_info column to sticker_set_details
"""
import asyncpg
import logging

logger = logging.getLogger(__name__)

VERSION = 1
DESCRIPTION = "Add doc_info JSONB column to sticker_set_details"

async def migrate(conn: asyncpg.Connection):
    """Applies migration changes."""
    logger.info(f"Running migration v{VERSION}: {DESCRIPTION}")
    await conn.execute("""
        ALTER TABLE sticker_set_details 
        ADD COLUMN IF NOT EXISTS doc_info JSONB NOT NULL DEFAULT '[]'::jsonb;
    """)