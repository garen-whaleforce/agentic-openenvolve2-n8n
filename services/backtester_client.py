"""
Backtester API Service Client

Provides access to backtesting functionality and OHLCV data.
Service endpoint: https://backtest.api.whaleforce.dev
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Optional
from datetime import date, datetime

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BACKTESTER_URL = "https://backtest.api.whaleforce.dev"


class BacktesterClient:
    """Client for Backtester API Service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 60.0,
    ):
        self.base_url = base_url or os.getenv("BACKTESTER_API_URL", DEFAULT_BACKTESTER_URL)
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

    async def get_ohlcv(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        interval: str = "1d",
    ) -> Dict[str, Any]:
        """
        Get OHLCV (Open, High, Low, Close, Volume) data for a ticker.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            interval: Data interval (e.g., "1d", "1h")

        Returns:
            OHLCV data with columns: date, open, high, low, close, volume
        """
        client = await self._get_client()
        params = {
            "ticker": ticker,
            "start": start_date,
            "end": end_date,
            "interval": interval,
        }
        resp = await client.get("/data-management/ohlcv", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_available_tickers(self) -> List[str]:
        """Get list of available tickers with OHLCV data."""
        client = await self._get_client()
        resp = await client.get("/data-management/ohlcv/tickers")
        resp.raise_for_status()
        return resp.json()

    async def get_available_intervals(self) -> List[str]:
        """Get list of available data intervals."""
        client = await self._get_client()
        resp = await client.get("/data-management/ohlcv/intervals")
        resp.raise_for_status()
        return resp.json()

    async def run_backtest(
        self,
        strategy: str,
        start_date: str,
        end_date: str,
        initial_capital: float = 100000.0,
        tickers: Optional[List[str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Run a backtest with specified strategy.

        Args:
            strategy: Strategy name or identifier
            start_date: Backtest start date (YYYY-MM-DD)
            end_date: Backtest end date (YYYY-MM-DD)
            initial_capital: Starting capital
            tickers: List of tickers to trade
            params: Additional strategy parameters

        Returns:
            Backtest ID and initial status
        """
        client = await self._get_client()
        payload = {
            "strategy": strategy,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
        }
        if tickers:
            payload["tickers"] = tickers
        if params:
            payload["params"] = params

        resp = await client.post("/backtest/run", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def get_backtest_result(self, backtest_id: str) -> Dict[str, Any]:
        """
        Get backtest result by ID.

        Args:
            backtest_id: Backtest identifier

        Returns:
            Backtest results including performance metrics
        """
        client = await self._get_client()
        resp = await client.get(f"/backtest/result/{backtest_id}")
        resp.raise_for_status()
        return resp.json()

    async def list_backtests(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List recent backtests.

        Args:
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            List of backtest summaries
        """
        client = await self._get_client()
        params = {"limit": limit, "offset": offset}
        resp = await client.get("/backtest/list", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_backtest_positions(self, backtest_id: str) -> Dict[str, Any]:
        """Get positions from a backtest."""
        client = await self._get_client()
        resp = await client.get(f"/backtest/positions/{backtest_id}")
        resp.raise_for_status()
        return resp.json()

    async def get_backtest_trades(self, backtest_id: str) -> Dict[str, Any]:
        """Get trade history from a backtest."""
        client = await self._get_client()
        resp = await client.get(f"/backtest/trades/{backtest_id}")
        resp.raise_for_status()
        return resp.json()

    async def get_daily_snapshots(self, backtest_id: str) -> Dict[str, Any]:
        """Get daily portfolio snapshots from a backtest."""
        client = await self._get_client()
        resp = await client.get(f"/backtest/daily-snapshots/{backtest_id}")
        resp.raise_for_status()
        return resp.json()

    async def get_benchmarks(self, backtest_id: str) -> Dict[str, Any]:
        """Get benchmark comparisons for a backtest."""
        client = await self._get_client()
        resp = await client.get(f"/backtest/benchmarks/{backtest_id}")
        resp.raise_for_status()
        return resp.json()

    async def get_data_freshness(self) -> Dict[str, Any]:
        """Get information about data freshness."""
        client = await self._get_client()
        resp = await client.get("/backtest/data/freshness")
        resp.raise_for_status()
        return resp.json()

    async def get_risk_free_rate(self) -> Dict[str, Any]:
        """Get current risk-free rate setting."""
        client = await self._get_client()
        resp = await client.get("/backtest/settings/risk_free_rate")
        resp.raise_for_status()
        return resp.json()

    # Convenience methods for earnings analysis integration

    async def get_price_around_earnings(
        self,
        ticker: str,
        earnings_date: str,
        days_before: int = 5,
        days_after: int = 30,
    ) -> Dict[str, Any]:
        """
        Get price data around an earnings announcement date.

        Args:
            ticker: Stock ticker symbol
            earnings_date: Earnings announcement date (YYYY-MM-DD)
            days_before: Trading days before earnings to include
            days_after: Trading days after earnings to include

        Returns:
            OHLCV data with calculated returns
        """
        from datetime import datetime, timedelta

        earnings_dt = datetime.strptime(earnings_date, "%Y-%m-%d")
        start_dt = earnings_dt - timedelta(days=days_before * 2)  # Buffer for weekends
        end_dt = earnings_dt + timedelta(days=days_after * 2)

        data = await self.get_ohlcv(
            ticker=ticker,
            start_date=start_dt.strftime("%Y-%m-%d"),
            end_date=end_dt.strftime("%Y-%m-%d"),
        )

        # Add earnings_date reference to response
        data["earnings_date"] = earnings_date
        return data

    async def calculate_post_earnings_return(
        self,
        ticker: str,
        earnings_date: str,
        holding_days: int = 30,
    ) -> Dict[str, Any]:
        """
        Calculate return after earnings announcement.

        Args:
            ticker: Stock ticker symbol
            earnings_date: Earnings announcement date (YYYY-MM-DD)
            holding_days: Number of trading days to hold

        Returns:
            Post-earnings return calculation
        """
        data = await self.get_price_around_earnings(
            ticker=ticker,
            earnings_date=earnings_date,
            days_before=1,
            days_after=holding_days + 5,
        )

        if not data.get("data") or len(data["data"]) < 2:
            return {
                "ticker": ticker,
                "earnings_date": earnings_date,
                "error": "Insufficient price data",
            }

        prices = data["data"]
        earnings_dt = datetime.strptime(earnings_date, "%Y-%m-%d")

        # Find the first trading day after earnings
        entry_price = None
        entry_date = None
        for row in prices:
            row_date = datetime.strptime(row["date"], "%Y-%m-%d")
            if row_date >= earnings_dt:
                entry_price = row["close"]
                entry_date = row["date"]
                break

        if entry_price is None:
            return {
                "ticker": ticker,
                "earnings_date": earnings_date,
                "error": "Could not find entry price",
            }

        # Find exit price (holding_days later)
        exit_price = None
        exit_date = None
        trading_days_count = 0
        for row in prices:
            row_date = datetime.strptime(row["date"], "%Y-%m-%d")
            if row_date > datetime.strptime(entry_date, "%Y-%m-%d"):
                trading_days_count += 1
                if trading_days_count >= holding_days:
                    exit_price = row["close"]
                    exit_date = row["date"]
                    break

        if exit_price is None:
            # Use last available price
            exit_price = prices[-1]["close"]
            exit_date = prices[-1]["date"]

        return_pct = ((exit_price - entry_price) / entry_price) * 100

        return {
            "ticker": ticker,
            "earnings_date": earnings_date,
            "entry_date": entry_date,
            "entry_price": entry_price,
            "exit_date": exit_date,
            "exit_price": exit_price,
            "holding_days": trading_days_count,
            "return_pct": round(return_pct, 4),
        }


# Singleton instance for convenience
_default_client: Optional[BacktesterClient] = None


def get_backtester_client() -> BacktesterClient:
    """Get the default Backtester client instance."""
    global _default_client
    if _default_client is None:
        _default_client = BacktesterClient()
    return _default_client
