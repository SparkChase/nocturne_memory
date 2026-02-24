import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

ROOT_NODE_UUID = "00000000-0000-0000-0000-000000000000"


async def up(engine: AsyncEngine):
    """
    Version: v1.1.0
    Deprecate orphan memories whose nodes are unreachable.

    A node is "unreachable" if no path entry's edge points to it
    (edge.child_uuid).  Active memories on such nodes are marked
    deprecated with migrated_to=NULL (the orphan-deprecation pattern).

    Must run after 005 (cascade path backfill) so that any node made
    reachable by sub-path expansion is excluded.
    """
    async with engine.begin() as conn:
        result = await conn.execute(text("""
            UPDATE memories
            SET deprecated = 1, migrated_to = NULL
            WHERE deprecated = 0
              AND node_uuid IS NOT NULL
              AND node_uuid != :root_uuid
              AND node_uuid NOT IN (
                  SELECT DISTINCT e.child_uuid
                  FROM paths p
                  JOIN edges e ON p.edge_id = e.id
                  WHERE e.child_uuid IS NOT NULL
              )
        """), {"root_uuid": ROOT_NODE_UUID})

        affected = result.rowcount
        if affected:
            logger.info(
                "Migration 006: deprecated %d orphan memories on unreachable nodes",
                affected,
            )
