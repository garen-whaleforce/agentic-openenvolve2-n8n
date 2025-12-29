"""Centralized configuration for EarningsCallAgenticRag.

All configurable constants should be defined here to ensure consistency
across all agents and modules.
"""

from __future__ import annotations

import os

# =============================================================================
# Model Configuration
# =============================================================================
MAIN_MODEL = os.getenv("MAIN_MODEL", "gpt-5-mini")
HELPER_MODEL = os.getenv("HELPER_MODEL", "gpt-5-mini")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-ada")

# Default temperature for LLM calls
DEFAULT_TEMPERATURE = float(os.getenv("DEFAULT_TEMPERATURE", "0.7"))

# =============================================================================
# Max Tokens Configuration (output limits)
# =============================================================================
# NOTE: GPT-5 reasoning models use part of max_tokens for internal reasoning.
# We need higher limits to accommodate reasoning_tokens + output_tokens.
# Extraction: ~40 facts × ~50 tokens each = ~2000 output tokens, plus ~5000 reasoning
MAX_TOKENS_EXTRACTION = int(os.getenv("MAX_TOKENS_EXTRACTION", "8000"))
# Summary: Direction + explanation + LongEligible JSON ~800 output tokens, plus ~2500 reasoning
MAX_TOKENS_SUMMARY = int(os.getenv("MAX_TOKENS_SUMMARY", "4000"))
# Helper agents: shorter analysis notes ~400 output tokens, plus ~1000 reasoning
MAX_TOKENS_HELPER = int(os.getenv("MAX_TOKENS_HELPER", "2000"))

# =============================================================================
# Return Horizon Configuration (T+N days)
# =============================================================================
RETURN_HORIZON_DAYS = int(os.getenv("RETURN_HORIZON_DAYS", "30"))
RETURN_COLUMN_FALLBACK = os.getenv("RETURN_COLUMN_FALLBACK", "future_3bday_cum_return")

# =============================================================================
# Facts Processing Limits (optimized for token efficiency without accuracy loss)
# =============================================================================
# Main Agent
MAX_FACTS_PER_HELPER = int(os.getenv("MAX_FACTS_PER_HELPER", "30"))  # Reduced from 80
MAX_PEERS = int(os.getenv("MAX_PEERS", "5"))  # Reduced from 10

# Comparative Agent (peer comparison)
MAX_FACTS_FOR_PEERS = int(os.getenv("MAX_FACTS_FOR_PEERS", "25"))  # Reduced from 60
MAX_PEER_FACTS = int(os.getenv("MAX_PEER_FACTS", "40"))  # Reduced from 120

# Historical Performance Agent (financial statements)
MAX_FACTS_FOR_FINANCIALS = int(os.getenv("MAX_FACTS_FOR_FINANCIALS", "20"))  # Reduced from 40
MAX_FINANCIAL_FACTS = int(os.getenv("MAX_FINANCIAL_FACTS", "30"))  # Reduced from 80

# Historical Earnings Agent (past calls)
MAX_FACTS_FOR_PAST = int(os.getenv("MAX_FACTS_FOR_PAST", "20"))  # Reduced from 40
MAX_HISTORICAL_FACTS = int(os.getenv("MAX_HISTORICAL_FACTS", "30"))  # Reduced from 80

# =============================================================================
# Vector Search Configuration
# =============================================================================
MIN_SIMILARITY_SCORE = float(os.getenv("MIN_SIMILARITY_SCORE", "0.3"))
DEFAULT_TOP_K = int(os.getenv("DEFAULT_TOP_K", "10"))

# =============================================================================
# Orchestrator Configuration
# =============================================================================
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "300"))

# =============================================================================
# Logging Directories
# =============================================================================
TOKEN_LOG_DIR = os.getenv("TOKEN_LOG_DIR", "token_logs")
TIMING_LOG_DIR = os.getenv("TIMING_LOG_DIR", "timing_logs")
NEO4J_LOG_DIR = os.getenv("NEO4J_LOG_DIR", "neo4j_logs")

# =============================================================================
# PG DB Agent Configuration
# =============================================================================
# Historical Performance Agent
PG_AGENT_HISTORICAL_LIMIT = int(os.getenv("PG_AGENT_HISTORICAL_LIMIT", "4"))
PG_AGENT_PERFORMANCE_FACTS_LIMIT = int(os.getenv("PG_AGENT_PERFORMANCE_FACTS_LIMIT", "20"))

# Historical Earnings Agent
PG_AGENT_TRANSCRIPT_EXCERPT_LENGTH = int(os.getenv("PG_AGENT_TRANSCRIPT_EXCERPT_LENGTH", "2000"))
PG_AGENT_EARNINGS_FACTS_LIMIT = int(os.getenv("PG_AGENT_EARNINGS_FACTS_LIMIT", "15"))
PG_AGENT_HISTORICAL_EXCERPTS_LIMIT = int(os.getenv("PG_AGENT_HISTORICAL_EXCERPTS_LIMIT", "3"))

# Comparative Agent
PG_AGENT_COMPARATIVE_FACTS_LIMIT = int(os.getenv("PG_AGENT_COMPARATIVE_FACTS_LIMIT", "15"))

# =============================================================================
# PostgreSQL DB Scoring Weights (for peer comparison)
# =============================================================================
PEER_SCORE_WEIGHTS = {
    "revenue": float(os.getenv("PEER_SCORE_REVENUE", "0.9")),
    "net_income": float(os.getenv("PEER_SCORE_NET_INCOME", "0.85")),
    "eps": float(os.getenv("PEER_SCORE_EPS", "0.85")),
    "revenue_growth": float(os.getenv("PEER_SCORE_REVENUE_GROWTH", "0.8")),
    "earnings_day_return": float(os.getenv("PEER_SCORE_EARNINGS_RETURN", "0.75")),
}

# =============================================================================
# PostgreSQL Connection Pool
# =============================================================================
PG_POOL_MINCONN = int(os.getenv("PG_POOL_MINCONN", "1"))
PG_POOL_MAXCONN = int(os.getenv("PG_POOL_MAXCONN", "10"))

# =============================================================================
# Feature Flags
# =============================================================================
USE_PG_DB_AGENTS = os.getenv("USE_PG_DB_AGENTS", "false").lower() == "true"
INGEST_HISTORY_QUARTERS = int(os.getenv("INGEST_HISTORY_QUARTERS", "4"))
