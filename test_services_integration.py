#!/usr/bin/env python3
"""
Test script for Whaleforce Services Integration.

This script tests the integration with:
- SEC Filings Service
- Backtester API
- Performance Metrics Service
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from services.sec_filings_client import SECFilingsClient
from services.backtester_client import BacktesterClient
from services.performance_metrics_client import PerformanceMetricsClient


async def test_sec_filings_service():
    """Test SEC Filings Service integration."""
    print("\n" + "=" * 60)
    print("Testing SEC Filings Service")
    print("=" * 60)

    client = SECFilingsClient()

    try:
        # Test health check
        print("\n1. Health Check:")
        health = await client.health_check()
        print(f"   Status: OK - {health}")

        # Test company search
        print("\n2. Company Search (AAPL):")
        search_result = await client.search("AAPL")
        print(f"   Result: {search_result}")

        # Test get filings
        if search_result.get("cik"):
            print("\n3. Get 10-K Filings:")
            filings = await client.get_10k_filings("AAPL", from_date="2023-01-01")
            filing_count = len(filings.get("filings", []))
            print(f"   Found {filing_count} 10-K filings")

        print("\n   SEC Filings Service: PASSED")
        return True

    except Exception as e:
        print(f"\n   SEC Filings Service: FAILED - {e}")
        return False

    finally:
        await client.close()


async def test_backtester_service():
    """Test Backtester API integration."""
    print("\n" + "=" * 60)
    print("Testing Backtester API Service")
    print("=" * 60)

    client = BacktesterClient()

    try:
        # Test health check
        print("\n1. Health Check:")
        health = await client.health_check()
        print(f"   Status: OK - {health}")

        # Test get OHLCV data
        print("\n2. Get OHLCV Data (AAPL, last 30 days):")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        ohlcv = await client.get_ohlcv(
            ticker="AAPL",
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
        )
        data_points = len(ohlcv.get("data", []))
        print(f"   Retrieved {data_points} data points")

        # Test post-earnings return calculation
        print("\n3. Calculate Post-Earnings Return (AAPL, 2024-10-31):")
        result = await client.calculate_post_earnings_return(
            ticker="AAPL",
            earnings_date="2024-10-31",
            holding_days=30,
        )
        if result.get("error"):
            print(f"   Error: {result['error']}")
        else:
            print(f"   Return: {result.get('return_pct', 'N/A')}%")

        print("\n   Backtester API Service: PASSED")
        return True

    except Exception as e:
        print(f"\n   Backtester API Service: FAILED - {e}")
        return False

    finally:
        await client.close()


async def test_performance_metrics_service():
    """Test Performance Metrics Service integration."""
    print("\n" + "=" * 60)
    print("Testing Performance Metrics Service")
    print("=" * 60)

    client = PerformanceMetricsClient()

    try:
        # Test health check
        print("\n1. Health Check:")
        health = await client.health_check()
        print(f"   Status: OK - {health}")

        # Test get metrics
        print("\n2. Get Performance Metrics (AAPL, 2024-01-01 to 2024-06-30):")
        metrics = await client.get_metrics(
            ticker="AAPL",
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        print(f"   Sharpe Ratio: {metrics.get('sharpe_ratio', 'N/A')}")
        print(f"   Excess Return: {metrics.get('excess_return_pct', 'N/A')}%")
        print(f"   Trading Days: {metrics.get('trading_days', 'N/A')}")

        # Test benchmark comparison
        print("\n3. Benchmark Comparison:")
        comparison = await client.compare_to_benchmark(
            ticker="AAPL",
            start_date="2024-01-01",
            end_date="2024-06-30",
        )
        outperformed = comparison.get("outperformed_benchmark", False)
        print(f"   Outperformed VOO: {outperformed}")

        print("\n   Performance Metrics Service: PASSED")
        return True

    except Exception as e:
        print(f"\n   Performance Metrics Service: FAILED - {e}")
        return False

    finally:
        await client.close()


async def test_sec_filings_agent():
    """Test SEC Filings Agent integration."""
    print("\n" + "=" * 60)
    print("Testing SEC Filings Agent")
    print("=" * 60)

    try:
        from EarningsCallAgenticRag.agents.secFilingsAgent import get_sec_context

        print("\n1. Get SEC Context for AAPL 2024 Q3:")
        context = await get_sec_context("AAPL", 2024, 3)

        print(f"   CIK: {context.get('cik', 'N/A')}")
        print(f"   Filings Found: {context.get('filings_found', 0)}")
        if context.get("latest_10k"):
            print(f"   Latest 10-K: Available")
        if context.get("latest_10q"):
            print(f"   Latest 10-Q: Available")
        if context.get("error"):
            print(f"   Error: {context['error']}")

        print("\n   SEC Filings Agent: PASSED")
        return True

    except Exception as e:
        print(f"\n   SEC Filings Agent: FAILED - {e}")
        return False


async def main():
    """Run all integration tests."""
    print("\n" + "=" * 60)
    print("Whaleforce Services Integration Tests")
    print("=" * 60)
    print(f"Started at: {datetime.now().isoformat()}")

    results = {
        "SEC Filings Service": await test_sec_filings_service(),
        "Backtester API": await test_backtester_service(),
        "Performance Metrics": await test_performance_metrics_service(),
        "SEC Filings Agent": await test_sec_filings_agent(),
    }

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "PASSED" if result else "FAILED"
        print(f"   {name}: {status}")

    print(f"\n   Total: {passed}/{total} tests passed")

    if passed == total:
        print("\n   All tests PASSED!")
        return 0
    else:
        print("\n   Some tests FAILED!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
