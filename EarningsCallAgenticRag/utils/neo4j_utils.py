"""Shared Neo4j utilities for lazy driver initialization.

This module consolidates Neo4j driver creation to avoid code duplication
across agent files.
"""

from __future__ import annotations

from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j import Driver


def get_neo4j_driver(creds: Dict[str, Any]) -> "Driver":
    """Create a Neo4j driver instance from credentials.

    Args:
        creds: Dictionary containing neo4j_uri, neo4j_username, neo4j_password

    Returns:
        Neo4j Driver instance

    Note:
        This function performs a lazy import of neo4j to avoid loading
        the driver unless actually needed.
    """
    from neo4j import GraphDatabase
    return GraphDatabase.driver(
        creds["neo4j_uri"],
        auth=(creds["neo4j_username"], creds["neo4j_password"])
    )
