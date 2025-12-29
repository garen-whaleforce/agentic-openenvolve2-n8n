"""
PostgreSQL DB-based Helper Agents
=================================
These agents use PostgreSQL directly instead of Neo4j vector search.
Faster and more reliable for batch processing.

Refactored with BasePgAgent base class for code reuse.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add parent directory to path for pg_client import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pg_client import (
    get_historical_financials,
    get_historical_transcripts,
    get_peer_financials,
    get_company_profile,
    get_earnings_surprise,
    parse_quarter,
)

from agents.prompts.prompts import (
    get_financials_system_message,
    get_historical_earnings_system_message,
    get_comparative_system_message,
)
from utils.llm import build_chat_client
from utils.token_tracker import TokenTracker
from utils.config import (
    HELPER_MODEL as DEFAULT_HELPER_MODEL,
    DEFAULT_TEMPERATURE,
    PG_AGENT_HISTORICAL_LIMIT,
    PG_AGENT_PERFORMANCE_FACTS_LIMIT,
    PG_AGENT_TRANSCRIPT_EXCERPT_LENGTH,
    PG_AGENT_EARNINGS_FACTS_LIMIT,
    PG_AGENT_HISTORICAL_EXCERPTS_LIMIT,
    PG_AGENT_COMPARATIVE_FACTS_LIMIT,
    MAX_TOKENS_HELPER,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions
# =============================================================================

# Quarter month mapping for date conversion
_QUARTER_MONTHS = {"1": "03", "2": "06", "3": "09", "4": "12"}


def parse_quarter_to_date(quarter: str, default_date: str = "2025-01-01") -> str:
    """Parse quarter string to approximate date.

    Args:
        quarter: Quarter string in format "YYYY-QN" (e.g., "2024-Q1")
        default_date: Fallback date if parsing fails

    Returns:
        Date string in format "YYYY-MM-DD"
    """
    parsed = parse_quarter(quarter)
    if not parsed:
        return default_date
    year, q = parsed
    return f"{year}-{_QUARTER_MONTHS.get(str(q), '12')}-01"


def parse_quarter_to_year_q(quarter: str) -> tuple[int, int] | None:
    """Parse quarter string to year and quarter number.

    Wrapper around pg_client.parse_quarter for backward compatibility.
    """
    return parse_quarter(quarter)


def format_facts_text(facts: List[Dict[str, str]], limit: int) -> str:
    """Format facts list into text for prompts.

    Args:
        facts: List of fact dictionaries
        limit: Maximum number of facts to include

    Returns:
        Formatted text string
    """
    return "\n".join([
        f"- {f.get('metric', '?')}: {f.get('value', '?')} ({f.get('context', '')})"
        for f in facts[:limit]
    ])


# =============================================================================
# Base Agent Class
# =============================================================================

class BasePgAgent(ABC):
    """Base class for all PostgreSQL-based helper agents.

    Provides common functionality:
    - LLM client initialization
    - Token tracking
    - Common LLM call pattern
    """

    def __init__(
        self,
        credentials_file: str = "credentials.json",
        model: str = DEFAULT_HELPER_MODEL,
        temperature: float = DEFAULT_TEMPERATURE,
    ) -> None:
        """Initialize the agent with LLM client.

        Args:
            credentials_file: Path to credentials JSON file
            model: LLM model name
            temperature: LLM temperature setting
        """
        creds = json.loads(Path(credentials_file).read_text())
        self.client, self.model = build_chat_client(creds, model)
        self.temperature = temperature
        self.token_tracker = TokenTracker()

    def _call_llm(self, system_message: str, user_message: str, max_tokens: int | None = None) -> str:
        """Make an LLM call with token tracking.

        Args:
            system_message: System prompt
            user_message: User prompt
            max_tokens: Maximum tokens for response (None = use default MAX_TOKENS_HELPER)

        Returns:
            LLM response content
        """
        kwargs = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ],
            "max_tokens": max_tokens if max_tokens is not None else MAX_TOKENS_HELPER,
        }
        # GPT-5 models only support temperature=1; others use configured temperature
        if "gpt-5" not in self.model.lower():
            kwargs["temperature"] = self.temperature

        resp = self.client.chat.completions.create(**kwargs)

        if hasattr(resp, 'usage') and resp.usage:
            self.token_tracker.add_usage(
                input_tokens=resp.usage.prompt_tokens,
                output_tokens=resp.usage.completion_tokens,
                model=self.model
            )

        return resp.choices[0].message.content.strip()

    def _reset_token_tracker(self) -> None:
        """Reset token tracker for new run."""
        self.token_tracker = TokenTracker()

    @abstractmethod
    def run(self, *args, **kwargs) -> Optional[str]:
        """Run the agent analysis. Must be implemented by subclasses."""
        pass

    def close(self) -> None:
        """Clean up resources. No-op for compatibility."""
        pass


# =============================================================================
# Historical Performance Agent
# =============================================================================

class PgHistoricalPerformanceAgent(BasePgAgent):
    """
    Compare current-quarter facts with prior financial statements.
    Uses PostgreSQL instead of Neo4j.
    """

    def _format_financials(self, financials: Dict) -> str:
        """Format financial statements for the prompt."""
        lines = []

        for stmt in financials.get("income", []):
            date = stmt.get("date", "?")
            period = stmt.get("period", "?")
            lines.append(f"Income ({date}, {period}):")
            lines.append(f"  Revenue: {stmt.get('revenue', 'N/A'):,}" if stmt.get('revenue') else "  Revenue: N/A")
            lines.append(f"  Net Income: {stmt.get('net_income', 'N/A'):,}" if stmt.get('net_income') else "  Net Income: N/A")
            lines.append(f"  EPS: {stmt.get('eps', 'N/A')}")
            lines.append(f"  Gross Profit: {stmt.get('gross_profit', 'N/A'):,}" if stmt.get('gross_profit') else "  Gross Profit: N/A")

        for stmt in financials.get("balance", []):
            date = stmt.get("date", "?")
            lines.append(f"Balance Sheet ({date}):")
            lines.append(f"  Total Assets: {stmt.get('total_assets', 'N/A'):,}" if stmt.get('total_assets') else "  Total Assets: N/A")
            lines.append(f"  Total Debt: {stmt.get('total_debt', 'N/A'):,}" if stmt.get('total_debt') else "  Total Debt: N/A")
            lines.append(f"  Cash: {stmt.get('cash_and_cash_equivalents', 'N/A'):,}" if stmt.get('cash_and_cash_equivalents') else "  Cash: N/A")

        for stmt in financials.get("cashFlow", []):
            date = stmt.get("date", "?")
            lines.append(f"Cash Flow ({date}):")
            lines.append(f"  Operating CF: {stmt.get('operating_cash_flow', 'N/A'):,}" if stmt.get('operating_cash_flow') else "  Operating CF: N/A")
            lines.append(f"  Free CF: {stmt.get('free_cash_flow', 'N/A'):,}" if stmt.get('free_cash_flow') else "  Free CF: N/A")

        return "\n".join(lines) if lines else "No historical financial data available."

    def run(
        self,
        facts: List[Dict[str, str]],
        row: Dict,
        quarter: str,
        ticker: Optional[str] = None,
        top_n: int = PG_AGENT_HISTORICAL_LIMIT,
    ) -> Optional[str]:
        """Compare facts with historical financials from PostgreSQL DB.

        Args:
            facts: Current quarter facts
            row: DataFrame row with earnings data
            quarter: Quarter string (e.g., "2024-Q1")
            ticker: Stock ticker (optional, extracted from row if not provided)
            top_n: Number of historical periods to fetch

        Returns:
            Analysis text or None if insufficient data
        """
        self._reset_token_tracker()

        ticker = ticker or row.get("ticker", "")
        if not ticker:
            return None

        before_date = parse_quarter_to_date(quarter)

        # Get historical financials from PostgreSQL DB
        historical = get_historical_financials(ticker, before_date, limit=top_n)

        if not historical.get("income") and not historical.get("balance") and not historical.get("cashFlow"):
            return None

        # Format for prompt
        historical_text = self._format_financials(historical)
        facts_text = format_facts_text(facts, PG_AGENT_PERFORMANCE_FACTS_LIMIT)

        prompt = f"""Analyze these current quarter facts against historical financial performance:

**Current Quarter Facts ({ticker}, {quarter}):**
{facts_text}

**Historical Financial Statements:**
{historical_text}

Compare the current facts with historical trends. Identify:
1. Significant changes from prior quarters
2. Trends in key metrics (revenue, profit, cash flow)
3. Any concerning or positive patterns

Provide a concise analysis (2-3 paragraphs)."""

        return self._call_llm(get_financials_system_message(), prompt)


# =============================================================================
# Historical Earnings Agent
# =============================================================================

class PgHistoricalEarningsAgent(BasePgAgent):
    """
    Compare current facts with the firm's own historical earnings calls.
    Uses PostgreSQL instead of Neo4j.
    """

    def run(
        self,
        facts: List[Dict[str, str]],
        ticker: str,
        quarter: str,
        top_k: int = PG_AGENT_HISTORICAL_LIMIT,
    ) -> Optional[str]:
        """Compare facts with historical earnings calls from PostgreSQL DB.

        Args:
            facts: Current quarter facts
            ticker: Stock ticker
            quarter: Quarter string (e.g., "2024-Q1")
            top_k: Number of historical transcripts to fetch

        Returns:
            Analysis text or None if insufficient data
        """
        self._reset_token_tracker()

        if not ticker:
            return None

        parsed = parse_quarter_to_year_q(quarter)
        if not parsed:
            return None
        year, q = parsed

        # Get historical transcripts from PostgreSQL DB
        historical = get_historical_transcripts(ticker, year, q, limit=top_k)

        if not historical:
            return None

        # Extract key excerpts from historical transcripts
        historical_excerpts = []
        for h in historical:
            content = h.get("content", "")[:PG_AGENT_TRANSCRIPT_EXCERPT_LENGTH]
            historical_excerpts.append(
                f"**{h['year']}-Q{h['quarter']}** ({h.get('date', 'N/A')}):\n{content}..."
            )

        facts_text = format_facts_text(facts, PG_AGENT_EARNINGS_FACTS_LIMIT)

        prompt = f"""Compare these current quarter facts with the company's historical earnings calls:

**Current Quarter Facts ({ticker}, {quarter}):**
{facts_text}

**Historical Earnings Call Excerpts:**
{chr(10).join(historical_excerpts[:PG_AGENT_HISTORICAL_EXCERPTS_LIMIT])}

Analyze:
1. How does management's tone/message compare to previous quarters?
2. Are there recurring themes or concerns?
3. Any notable changes in guidance or outlook?

Provide a concise analysis (2-3 paragraphs)."""

        return self._call_llm(get_historical_earnings_system_message(), prompt)


# =============================================================================
# Comparative Agent
# =============================================================================

class PgComparativeAgent(BasePgAgent):
    """
    Compare facts against peer companies in the same sector.
    Uses PostgreSQL instead of Neo4j.
    """

    def __init__(
        self,
        credentials_file: str = "credentials.json",
        model: str = DEFAULT_HELPER_MODEL,
        sector_map: dict = None,
        temperature: float = DEFAULT_TEMPERATURE,
        **kwargs,
    ) -> None:
        """Initialize with optional sector map.

        Args:
            credentials_file: Path to credentials JSON file
            model: LLM model name
            sector_map: Optional sector mapping dictionary
            temperature: LLM temperature setting
            **kwargs: Additional arguments (ignored for compatibility)
        """
        super().__init__(credentials_file, model, temperature)
        self.sector_map = sector_map or {}

    def run(
        self,
        facts: List[Dict[str, str]],
        ticker: str,
        quarter: str,
        peers: list[str] | None = None,
        sector: str | None = None,
        top_k: int = 5,
    ) -> Optional[str]:
        """Compare facts with peer companies from PostgreSQL DB.

        Args:
            facts: Current quarter facts
            ticker: Stock ticker
            quarter: Quarter string (e.g., "2024-Q1")
            peers: Optional list of peer tickers (not used currently)
            sector: Optional sector name
            top_k: Number of peers to fetch

        Returns:
            Analysis text or None if insufficient data
        """
        self._reset_token_tracker()

        if not ticker:
            return None

        # Get sector if not provided
        if not sector:
            profile = get_company_profile(ticker)
            sector = profile.get("sector") if profile else None

        if not sector:
            return None

        as_of_date = parse_quarter_to_date(quarter)

        # Get peer financials from PostgreSQL DB
        peer_data = get_peer_financials(sector, ticker, as_of_date, limit=top_k)

        if not peer_data:
            return None

        # Format peer data
        peer_lines = []
        for p in peer_data:
            fin = p.get("financials") or {}
            line = f"**{p['symbol']}** ({p.get('name', 'N/A')}):\n"
            line += f"  Revenue: {fin.get('revenue', 'N/A'):,}" if fin.get('revenue') else "  Revenue: N/A"
            if fin.get('net_income'):
                line += f", Net Income: {fin['net_income']:,}"
            if fin.get('eps'):
                line += f", EPS: {fin['eps']}"
            peer_lines.append(line)

        facts_text = format_facts_text(facts, PG_AGENT_COMPARATIVE_FACTS_LIMIT)

        prompt = f"""Compare {ticker}'s performance with sector peers ({sector}):

**{ticker} Current Quarter Facts ({quarter}):**
{facts_text}

**Peer Companies ({sector} sector):**
{chr(10).join(peer_lines)}

Analyze:
1. How does {ticker} compare to peers on key metrics?
2. Is {ticker} outperforming or underperforming the sector?
3. Any notable competitive advantages or disadvantages?

Provide a concise analysis (2-3 paragraphs)."""

        return self._call_llm(get_comparative_system_message(), prompt)
