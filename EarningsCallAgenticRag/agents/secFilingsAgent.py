"""SEC Filings Agent - Analyzes 10-K/10-Q filings for context enrichment.

This agent integrates with the SEC Filings Service to retrieve and analyze
annual (10-K) and quarterly (10-Q) reports, providing additional context
for earnings call analysis.
"""

from __future__ import annotations

import os
import json
import logging
import asyncio
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Import the SEC Filings client
import sys
sys.path.insert(0, str(__file__).rsplit("/", 3)[0])
from services.sec_filings_client import SECFilingsClient, get_sec_filings_client


@dataclass
class SECFilingsSummary:
    """Summary of SEC filings analysis."""
    ticker: str
    cik: str
    filings_found: int
    latest_10k: Optional[Dict[str, Any]] = None
    latest_10q: Optional[Dict[str, Any]] = None
    risk_factors: List[str] = field(default_factory=list)
    business_summary: str = ""
    key_metrics_mentioned: List[str] = field(default_factory=list)
    error: Optional[str] = None


class SECFilingsAgent:
    """Agent for analyzing SEC filings (10-K, 10-Q, 13F)."""

    def __init__(
        self,
        client: Optional[SECFilingsClient] = None,
        llm_client: Optional[Any] = None,
        model: str = "gpt-4o-mini",
    ):
        self.client = client or get_sec_filings_client()
        self.llm_client = llm_client
        self.model = model

    async def get_company_cik(self, ticker: str) -> Optional[str]:
        """Look up CIK for a ticker symbol."""
        try:
            result = await self.client.search(ticker)
            return result.get("cik")
        except Exception as e:
            logger.warning(f"Failed to get CIK for {ticker}: {e}")
            return None

    async def get_recent_filings(
        self,
        ticker: str,
        years_back: int = 2,
    ) -> Dict[str, Any]:
        """
        Get recent 10-K and 10-Q filings for a ticker.

        Args:
            ticker: Stock ticker symbol
            years_back: Number of years of filings to retrieve

        Returns:
            Dictionary with 10-K and 10-Q filing metadata
        """
        from datetime import datetime, timedelta

        cik = await self.get_company_cik(ticker)
        if not cik:
            return {"error": f"Could not find CIK for {ticker}"}

        end_date = datetime.now()
        start_date = end_date - timedelta(days=365 * years_back)

        try:
            # Get 10-K filings
            filings_10k = await self.client.get_filings(
                cik=[cik],
                form="10-K",
                from_date=start_date.strftime("%Y-%m-%d"),
                to_date=end_date.strftime("%Y-%m-%d"),
            )

            # Get 10-Q filings
            filings_10q = await self.client.get_filings(
                cik=[cik],
                form="10-Q",
                from_date=start_date.strftime("%Y-%m-%d"),
                to_date=end_date.strftime("%Y-%m-%d"),
            )

            return {
                "ticker": ticker,
                "cik": cik,
                "10k_filings": filings_10k.get("filings", []),
                "10q_filings": filings_10q.get("filings", []),
            }
        except Exception as e:
            logger.error(f"Failed to get filings for {ticker}: {e}")
            return {"error": str(e)}

    async def get_filing_content(
        self,
        ticker: str,
        form_type: str = "10-K",
        items: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get specific items from a filing.

        Args:
            ticker: Stock ticker symbol
            form_type: Filing type (10-K or 10-Q)
            items: Item codes to retrieve (e.g., ["1A", "7", "7A"])

        Returns:
            Filing content for specified items
        """
        if items is None:
            # Default items: Risk Factors (1A), MD&A (7), Quantitative Disclosures (7A)
            items = ["1A", "7", "7A"]

        cik = await self.get_company_cik(ticker)
        if not cik:
            return {"error": f"Could not find CIK for {ticker}"}

        try:
            filings = await self.client.get_filings(
                cik=[cik],
                form=form_type,
                items=items,
                formats=["text"],
                include_items=True,
            )
            return filings
        except Exception as e:
            logger.error(f"Failed to get filing content for {ticker}: {e}")
            return {"error": str(e)}

    async def get_institutional_holders(
        self,
        ticker: str,
        year: int,
        quarter: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get 13F institutional holder data.

        Args:
            ticker: Stock ticker symbol (of the holder, not the held stock)
            year: Year
            quarter: Quarter (1-4) or None for all quarters

        Returns:
            Institutional holdings data
        """
        cik = await self.get_company_cik(ticker)
        if not cik:
            return {"error": f"Could not find CIK for {ticker}"}

        try:
            quarters = [quarter] if quarter else None
            holders = await self.client.get_holder_analytics(
                cik=[cik],
                year=[year],
                quarter=quarters,
            )
            return holders
        except Exception as e:
            logger.error(f"Failed to get holder data for {ticker}: {e}")
            return {"error": str(e)}

    async def analyze_filings_for_earnings(
        self,
        ticker: str,
        fiscal_year: int,
        fiscal_quarter: int,
    ) -> SECFilingsSummary:
        """
        Analyze SEC filings relevant to an earnings call.

        This retrieves the most recent 10-K and 10-Q filings prior to the
        earnings date and extracts key information.

        Args:
            ticker: Stock ticker symbol
            fiscal_year: Fiscal year of the earnings call
            fiscal_quarter: Fiscal quarter of the earnings call

        Returns:
            SECFilingsSummary with relevant filing information
        """
        cik = await self.get_company_cik(ticker)
        if not cik:
            return SECFilingsSummary(
                ticker=ticker,
                cik="",
                filings_found=0,
                error=f"Could not find CIK for {ticker}",
            )

        try:
            filings = await self.get_recent_filings(ticker, years_back=2)

            if filings.get("error"):
                return SECFilingsSummary(
                    ticker=ticker,
                    cik=cik,
                    filings_found=0,
                    error=filings["error"],
                )

            filings_10k = filings.get("10k_filings", [])
            filings_10q = filings.get("10q_filings", [])

            total_filings = len(filings_10k) + len(filings_10q)

            # Get the latest 10-K
            latest_10k = filings_10k[0] if filings_10k else None

            # Get the latest 10-Q
            latest_10q = filings_10q[0] if filings_10q else None

            return SECFilingsSummary(
                ticker=ticker,
                cik=cik,
                filings_found=total_filings,
                latest_10k=latest_10k,
                latest_10q=latest_10q,
            )

        except Exception as e:
            logger.error(f"Error analyzing filings for {ticker}: {e}")
            return SECFilingsSummary(
                ticker=ticker,
                cik=cik,
                filings_found=0,
                error=str(e),
            )

    def format_for_prompt(self, summary: SECFilingsSummary) -> str:
        """
        Format SEC filings summary for inclusion in an LLM prompt.

        Args:
            summary: SECFilingsSummary object

        Returns:
            Formatted string for prompt inclusion
        """
        if summary.error:
            return f"SEC Filings: Unable to retrieve ({summary.error})"

        parts = [f"SEC Filings for {summary.ticker} (CIK: {summary.cik}):"]
        parts.append(f"- Total filings found: {summary.filings_found}")

        if summary.latest_10k:
            parts.append(f"- Latest 10-K: Filed {summary.latest_10k.get('filed_date', 'N/A')}")

        if summary.latest_10q:
            parts.append(f"- Latest 10-Q: Filed {summary.latest_10q.get('filed_date', 'N/A')}")

        if summary.risk_factors:
            parts.append("- Key Risk Factors:")
            for rf in summary.risk_factors[:3]:  # Limit to top 3
                parts.append(f"  * {rf[:200]}...")

        if summary.business_summary:
            parts.append(f"- Business Summary: {summary.business_summary[:500]}...")

        return "\n".join(parts)


# Factory function for integration with the agent pipeline
async def get_sec_context(
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
) -> Dict[str, Any]:
    """
    Get SEC filings context for an earnings analysis.

    This is the main entry point for integrating SEC filings
    into the agentic RAG pipeline.

    Args:
        ticker: Stock ticker symbol
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter

    Returns:
        Dictionary with SEC filings context
    """
    agent = SECFilingsAgent()
    summary = await agent.analyze_filings_for_earnings(
        ticker=ticker,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
    )

    return {
        "ticker": summary.ticker,
        "cik": summary.cik,
        "filings_found": summary.filings_found,
        "latest_10k": summary.latest_10k,
        "latest_10q": summary.latest_10q,
        "risk_factors": summary.risk_factors,
        "business_summary": summary.business_summary,
        "formatted_context": agent.format_for_prompt(summary),
        "error": summary.error,
    }


# Synchronous wrapper for non-async contexts
def get_sec_context_sync(
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
) -> Dict[str, Any]:
    """Synchronous wrapper for get_sec_context."""
    return asyncio.run(get_sec_context(ticker, fiscal_year, fiscal_quarter))
