"""
SEC Filings Service Client

Provides access to SEC filings including 10-K, 10-Q, 13F, and holder analytics.
Service endpoint: http://172.23.22.100:8001
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional
from datetime import date

import httpx

logger = logging.getLogger(__name__)

DEFAULT_SEC_FILINGS_URL = "http://172.23.22.100:8001"


class SECFilingsClient:
    """Client for SEC Filings Service API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url or os.getenv("SEC_FILINGS_API_URL", DEFAULT_SEC_FILINGS_URL)
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def health_check(self) -> Dict[str, Any]:
        """Check service health."""
        client = await self._get_client()
        resp = await client.get("/health")
        resp.raise_for_status()
        return resp.json()

    async def search(self, query: str) -> Dict[str, Any]:
        """
        Search for company by CIK or ticker symbol.

        Args:
            query: CIK number or ticker symbol (e.g., "AAPL" or "320193")

        Returns:
            Company information including CIK and name
        """
        client = await self._get_client()
        resp = await client.get("/search", params={"query": query})
        resp.raise_for_status()
        return resp.json()

    async def get_filings(
        self,
        cik: List[str],
        form: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        items: Optional[List[str]] = None,
        formats: Optional[List[str]] = None,
        include_items: bool = True,
    ) -> Dict[str, Any]:
        """
        Get SEC filings for given CIKs.

        Args:
            cik: List of CIK numbers
            form: Form type filter (e.g., "10-K", "10-Q", "8-K")
            from_date: Filed date lower bound (YYYY-MM-DD)
            to_date: Filed date upper bound (YYYY-MM-DD)
            items: Item codes to include (e.g., ["1A", "7", "7A"])
            formats: Output formats (e.g., ["text", "html", "pdf"])
            include_items: Whether to include filing_items rows

        Returns:
            Filing metadata and download links
        """
        client = await self._get_client()
        params: Dict[str, Any] = {"cik": cik, "include_items": include_items}
        if form:
            params["form"] = form
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date
        if items:
            params["items"] = items
        if formats:
            params["formats"] = formats

        resp = await client.get("/filings", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_10k_filings(
        self,
        ticker: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method to get 10-K annual reports for a ticker.

        Args:
            ticker: Stock ticker symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            10-K filing metadata
        """
        search_result = await self.search(ticker)
        if not search_result.get("cik"):
            raise ValueError(f"Could not find CIK for ticker: {ticker}")

        cik = search_result["cik"]
        return await self.get_filings(
            cik=[cik],
            form="10-K",
            from_date=from_date,
            to_date=to_date,
        )

    async def get_10q_filings(
        self,
        ticker: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Convenience method to get 10-Q quarterly reports for a ticker.

        Args:
            ticker: Stock ticker symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            10-Q filing metadata
        """
        search_result = await self.search(ticker)
        if not search_result.get("cik"):
            raise ValueError(f"Could not find CIK for ticker: {ticker}")

        cik = search_result["cik"]
        return await self.get_filings(
            cik=[cik],
            form="10-Q",
            from_date=from_date,
            to_date=to_date,
        )

    async def get_holder_analytics(
        self,
        cik: List[str],
        year: List[int],
        quarter: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Get 13F holder analytics data.

        Args:
            cik: List of holder CIKs (institutional investors)
            year: List of years
            quarter: List of quarters (1-4), or None for all quarters

        Returns:
            Holder position data from 13F filings
        """
        client = await self._get_client()
        params: Dict[str, Any] = {"cik": cik, "year": year}
        if quarter:
            params["quarter"] = quarter

        resp = await client.get("/holder", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_filing_keys(
        self,
        cik: List[str],
        form: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        items: Optional[List[str]] = None,
        formats: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get S3 keys for filing documents.

        Args:
            cik: List of CIK numbers
            form: Form type filter
            from_date: Filed date lower bound
            to_date: Filed date upper bound
            items: Item codes to include
            formats: Output formats

        Returns:
            S3 keys for downloading filing documents
        """
        client = await self._get_client()
        params: Dict[str, Any] = {"cik": cik}
        if form:
            params["form"] = form
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date
        if items:
            params["items"] = items
        if formats:
            params["formats"] = formats

        resp = await client.get("/filing-keys", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of available filings."""
        client = await self._get_client()
        resp = await client.get("/summary")
        resp.raise_for_status()
        return resp.json()


# Singleton instance for convenience
_default_client: Optional[SECFilingsClient] = None


def get_sec_filings_client() -> SECFilingsClient:
    """Get the default SEC Filings client instance."""
    global _default_client
    if _default_client is None:
        _default_client = SECFilingsClient()
    return _default_client
