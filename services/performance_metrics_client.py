"""
Performance Metrics Service Client

Provides access to Sharpe Ratio and excess return calculations.
Service endpoint: http://172.23.22.100:8100
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

DEFAULT_PERFORMANCE_METRICS_URL = "http://172.23.22.100:8100"


class PerformanceMetricsClient:
    """Client for Performance Metrics Service API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.base_url = base_url or os.getenv(
            "PERFORMANCE_METRICS_API_URL", DEFAULT_PERFORMANCE_METRICS_URL
        )
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
        resp = await client.get("/api/health")
        resp.raise_for_status()
        return resp.json()

    async def get_metrics(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Calculate performance metrics for a ticker.

        Args:
            ticker: Stock ticker symbol (1-10 characters)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Performance metrics including:
            - ticker: Stock symbol
            - benchmark: Benchmark used (VOO)
            - start_date: Analysis start date
            - end_date: Analysis end date
            - trading_days: Number of trading days
            - ticker_total_return_pct: Total return percentage
            - benchmark_total_return_pct: Benchmark return percentage
            - excess_return_pct: Excess return over benchmark
            - annualized_excess_return_pct: Annualized excess return
            - sharpe_ratio: Annualized Sharpe Ratio
            - annualized_volatility_pct: Annualized volatility
        """
        client = await self._get_client()
        params = {
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
        }
        resp = await client.get("/api/metrics", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_post_earnings_metrics(
        self,
        ticker: str,
        earnings_date: str,
        holding_days: int = 30,
    ) -> Dict[str, Any]:
        """
        Calculate performance metrics for post-earnings period.

        Args:
            ticker: Stock ticker symbol
            earnings_date: Earnings announcement date (YYYY-MM-DD)
            holding_days: Number of trading days after earnings

        Returns:
            Performance metrics for the post-earnings period
        """
        from datetime import datetime, timedelta

        earnings_dt = datetime.strptime(earnings_date, "%Y-%m-%d")
        # Approximate end date (add buffer for weekends/holidays)
        end_dt = earnings_dt + timedelta(days=int(holding_days * 1.5))

        try:
            metrics = await self.get_metrics(
                ticker=ticker,
                start_date=earnings_date,
                end_date=end_dt.strftime("%Y-%m-%d"),
            )
            metrics["earnings_date"] = earnings_date
            metrics["requested_holding_days"] = holding_days
            return metrics
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return {
                    "ticker": ticker,
                    "earnings_date": earnings_date,
                    "error": "No data available for the specified period",
                }
            raise

    async def compare_to_benchmark(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Get a summary comparing ticker performance to VOO benchmark.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Comparison summary with outperformance indicator
        """
        metrics = await self.get_metrics(ticker, start_date, end_date)

        excess = metrics.get("excess_return_pct", 0)
        outperformed = excess > 0

        return {
            "ticker": ticker,
            "benchmark": metrics.get("benchmark", "VOO"),
            "period": f"{start_date} to {end_date}",
            "trading_days": metrics.get("trading_days"),
            "ticker_return_pct": metrics.get("ticker_total_return_pct"),
            "benchmark_return_pct": metrics.get("benchmark_total_return_pct"),
            "excess_return_pct": excess,
            "outperformed_benchmark": outperformed,
            "sharpe_ratio": metrics.get("sharpe_ratio"),
            "volatility_pct": metrics.get("annualized_volatility_pct"),
        }

    async def batch_get_metrics(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """
        Get metrics for multiple tickers.

        Note: This service doesn't support batch queries natively,
        so we make sequential requests.

        Args:
            tickers: List of stock ticker symbols
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Dictionary mapping ticker to metrics (or error)
        """
        results = {}
        for ticker in tickers:
            try:
                metrics = await self.get_metrics(ticker, start_date, end_date)
                results[ticker] = {"success": True, "data": metrics}
            except httpx.HTTPStatusError as e:
                results[ticker] = {
                    "success": False,
                    "error": str(e),
                    "status_code": e.response.status_code,
                }
            except Exception as e:
                results[ticker] = {"success": False, "error": str(e)}

        return {
            "start_date": start_date,
            "end_date": end_date,
            "results": results,
            "success_count": sum(1 for r in results.values() if r.get("success")),
            "failure_count": sum(1 for r in results.values() if not r.get("success")),
        }


# Singleton instance for convenience
_default_client: Optional[PerformanceMetricsClient] = None


def get_performance_metrics_client() -> PerformanceMetricsClient:
    """Get the default Performance Metrics client instance."""
    global _default_client
    if _default_client is None:
        _default_client = PerformanceMetricsClient()
    return _default_client
