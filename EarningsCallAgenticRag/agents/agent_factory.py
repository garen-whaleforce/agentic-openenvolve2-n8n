"""
Agent Factory
=============
Factory functions for creating helper agents based on configuration.

根據 USE_PG_DB_AGENTS 設定自動選擇使用哪種 Agent 實作：
- True: 使用 PG DB Agents (PostgreSQL only, 無 Neo4j)
- False: 使用原始 Agents (PostgreSQL 優先, Neo4j fallback)

Usage:
    from agents.agent_factory import (
        get_historical_performance_agent,
        get_historical_earnings_agent,
        get_comparative_agent,
    )

    agent = get_historical_performance_agent(credentials_file="credentials.json")
    result = agent.run(facts, row, quarter)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from utils.config import USE_PG_DB_AGENTS

logger = logging.getLogger(__name__)


def get_historical_performance_agent(
    credentials_file: str = "credentials.json",
    model: Optional[str] = None,
    temperature: float = 0.7,
    **kwargs
) -> Any:
    """Get HistoricalPerformanceAgent instance.

    Args:
        credentials_file: Path to credentials JSON file
        model: LLM model name (uses default from config if not specified)
        temperature: LLM temperature setting
        **kwargs: Additional arguments passed to agent constructor

    Returns:
        HistoricalPerformanceAgent instance (PG or Neo4j variant)
    """
    if USE_PG_DB_AGENTS:
        from agents.pg_db_agents import PgHistoricalPerformanceAgent
        logger.debug("Using PgHistoricalPerformanceAgent (PostgreSQL only)")
        agent_kwargs = {"credentials_file": credentials_file, "temperature": temperature}
        if model:
            agent_kwargs["model"] = model
        return PgHistoricalPerformanceAgent(**agent_kwargs)
    else:
        from agents.historicalPerformanceAgent import HistoricalPerformanceAgent
        logger.debug("Using HistoricalPerformanceAgent (PostgreSQL + Neo4j fallback)")
        agent_kwargs = {"credentials_file": credentials_file, "temperature": temperature}
        if model:
            agent_kwargs["model"] = model
        return HistoricalPerformanceAgent(**agent_kwargs)


def get_historical_earnings_agent(
    credentials_file: str = "credentials.json",
    model: Optional[str] = None,
    temperature: float = 0.7,
    **kwargs
) -> Any:
    """Get HistoricalEarningsAgent instance.

    Args:
        credentials_file: Path to credentials JSON file
        model: LLM model name (uses default from config if not specified)
        temperature: LLM temperature setting
        **kwargs: Additional arguments passed to agent constructor

    Returns:
        HistoricalEarningsAgent instance (PG or Neo4j variant)
    """
    if USE_PG_DB_AGENTS:
        from agents.pg_db_agents import PgHistoricalEarningsAgent
        logger.debug("Using PgHistoricalEarningsAgent (PostgreSQL only)")
        agent_kwargs = {"credentials_file": credentials_file, "temperature": temperature}
        if model:
            agent_kwargs["model"] = model
        return PgHistoricalEarningsAgent(**agent_kwargs)
    else:
        from agents.historicalEarningsAgent import HistoricalEarningsAgent
        logger.debug("Using HistoricalEarningsAgent (PostgreSQL + Neo4j fallback)")
        agent_kwargs = {"credentials_file": credentials_file, "temperature": temperature}
        if model:
            agent_kwargs["model"] = model
        return HistoricalEarningsAgent(**agent_kwargs)


def get_comparative_agent(
    credentials_file: str = "credentials.json",
    model: Optional[str] = None,
    temperature: float = 0.7,
    sector_map: Optional[Dict[str, str]] = None,
    **kwargs
) -> Any:
    """Get ComparativeAgent instance.

    Args:
        credentials_file: Path to credentials JSON file
        model: LLM model name (uses default from config if not specified)
        temperature: LLM temperature setting
        sector_map: Optional sector mapping dictionary
        **kwargs: Additional arguments passed to agent constructor

    Returns:
        ComparativeAgent instance (PG or Neo4j variant)
    """
    if USE_PG_DB_AGENTS:
        from agents.pg_db_agents import PgComparativeAgent
        logger.debug("Using PgComparativeAgent (PostgreSQL only)")
        agent_kwargs = {
            "credentials_file": credentials_file,
            "temperature": temperature,
        }
        if model:
            agent_kwargs["model"] = model
        if sector_map:
            agent_kwargs["sector_map"] = sector_map
        return PgComparativeAgent(**agent_kwargs)
    else:
        from agents.comparativeAgent import ComparativeAgent
        logger.debug("Using ComparativeAgent (PostgreSQL + Neo4j fallback)")
        agent_kwargs = {
            "credentials_file": credentials_file,
            "temperature": temperature,
        }
        if model:
            agent_kwargs["model"] = model
        if sector_map:
            agent_kwargs["sector_map"] = sector_map
        return ComparativeAgent(**agent_kwargs)


def get_all_helper_agents(
    credentials_file: str = "credentials.json",
    model: Optional[str] = None,
    temperature: float = 0.7,
    sector_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Get all three helper agents as a dictionary.

    Args:
        credentials_file: Path to credentials JSON file
        model: LLM model name
        temperature: LLM temperature setting
        sector_map: Optional sector mapping dictionary

    Returns:
        Dict with keys: 'performance', 'earnings', 'comparative'
    """
    return {
        "performance": get_historical_performance_agent(
            credentials_file=credentials_file,
            model=model,
            temperature=temperature,
        ),
        "earnings": get_historical_earnings_agent(
            credentials_file=credentials_file,
            model=model,
            temperature=temperature,
        ),
        "comparative": get_comparative_agent(
            credentials_file=credentials_file,
            model=model,
            temperature=temperature,
            sector_map=sector_map,
        ),
    }


# =============================================================================
# Agent Type Information
# =============================================================================

def get_agent_type() -> str:
    """Get the current agent type being used.

    Returns:
        'pg_db' or 'neo4j_fallback'
    """
    return "pg_db" if USE_PG_DB_AGENTS else "neo4j_fallback"


def is_using_pg_db_agents() -> bool:
    """Check if using PG DB agents."""
    return USE_PG_DB_AGENTS
