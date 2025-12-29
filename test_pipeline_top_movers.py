#!/usr/bin/env python3
"""
Test Pipeline for 2024 Top Movers
=================================
Tests the analysis pipeline on:
- Top 100 gainers (largest positive T+30 returns)
- Top 100 losers (largest negative T+30 returns)

Usage:
    python test_pipeline_top_movers.py [--test]  # --test runs only 3 samples first
    python test_pipeline_top_movers.py [--concurrency N]  # Run with N concurrent workers (default: 5)
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("neo4j").setLevel(logging.WARNING)


def get_top_movers_2024(top_n: int = 100) -> tuple[List[Dict], List[Dict]]:
    """Get top N gainers and top N losers from 2024."""
    from pg_client import get_cursor

    with get_cursor() as cur:
        if cur is None:
            raise RuntimeError("Database connection failed")

        # Top gainers (largest positive returns)
        cur.execute("""
            SELECT
                et.symbol,
                et.year,
                et.quarter,
                et.transcript_date_str,
                c.name as company_name,
                c.sector,
                pa.pct_change_t_plus_30 as actual_return_30d
            FROM earnings_transcripts et
            JOIN companies c ON et.symbol = c.symbol
            JOIN transcript_content tc ON et.id = tc.transcript_id
            JOIN price_analysis pa ON et.id = pa.transcript_id
            WHERE et.year = 2024
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
            ORDER BY pa.pct_change_t_plus_30 DESC
            LIMIT %s
        """, (top_n,))

        gainers = []
        for row in cur.fetchall():
            gainers.append({
                "symbol": row["symbol"],
                "year": row["year"],
                "quarter": row["quarter"],
                "transcript_date": row["transcript_date_str"],
                "company_name": row["company_name"],
                "sector": row["sector"],
                "actual_return_30d": float(row["actual_return_30d"]),
                "category": "GAINER"
            })

        # Top losers (largest negative returns)
        cur.execute("""
            SELECT
                et.symbol,
                et.year,
                et.quarter,
                et.transcript_date_str,
                c.name as company_name,
                c.sector,
                pa.pct_change_t_plus_30 as actual_return_30d
            FROM earnings_transcripts et
            JOIN companies c ON et.symbol = c.symbol
            JOIN transcript_content tc ON et.id = tc.transcript_id
            JOIN price_analysis pa ON et.id = pa.transcript_id
            WHERE et.year = 2024
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
            ORDER BY pa.pct_change_t_plus_30 ASC
            LIMIT %s
        """, (top_n,))

        losers = []
        for row in cur.fetchall():
            losers.append({
                "symbol": row["symbol"],
                "year": row["year"],
                "quarter": row["quarter"],
                "transcript_date": row["transcript_date_str"],
                "company_name": row["company_name"],
                "sector": row["sector"],
                "actual_return_30d": float(row["actual_return_30d"]),
                "category": "LOSER"
            })

        return gainers, losers


async def analyze_single(
    symbol: str,
    year: int,
    quarter: int,
    main_model: str = None,
    helper_model: str = None,
) -> Dict[str, Any]:
    """Run analysis for a single earnings call."""
    from analysis_engine import analyze_earnings_async

    start_time = time.time()
    result = {
        "symbol": symbol,
        "year": year,
        "quarter": quarter,
        "success": False,
        "error": None,
        "time_seconds": 0,
        "prediction": None,
        "confidence": None,
        "actual_return": None,
        "correct": None,
        "summary": None,
        "reasons": None,
        "token_usage": None,
        "agent_notes": None,
    }

    try:
        payload = await analyze_earnings_async(
            symbol=symbol,
            year=year,
            quarter=quarter,
            main_model=main_model,
            helper_model=helper_model,
            skip_cache=True,
        )

        result["success"] = True
        result["time_seconds"] = time.time() - start_time

        agentic = payload.get("agentic_result", {})
        result["prediction"] = agentic.get("prediction")
        result["confidence"] = agentic.get("confidence")
        result["summary"] = agentic.get("summary")
        result["reasons"] = agentic.get("reasons")

        raw = agentic.get("raw", {})
        result["token_usage"] = raw.get("token_usage")
        result["agent_notes"] = raw.get("notes")

        backtest = payload.get("backtest", {})
        if backtest and backtest.get("change_pct") is not None:
            result["actual_return"] = backtest["change_pct"]
        elif payload.get("post_earnings_return") is not None:
            result["actual_return"] = payload["post_earnings_return"]

    except Exception as e:
        result["error"] = str(e)
        result["time_seconds"] = time.time() - start_time
        logger.error(f"Analysis failed for {symbol} {year}-Q{quarter}: {e}")

    return result


def calculate_correctness(prediction: str, actual_return: float) -> Optional[bool]:
    """Determine if prediction was correct."""
    if prediction is None or actual_return is None:
        return None
    pred = prediction.upper()
    if pred == "UP":
        return actual_return > 0
    elif pred == "DOWN":
        return actual_return < 0
    elif pred == "NEUTRAL":
        return abs(actual_return) < 1.0
    return None


def calculate_token_cost(token_usage: Dict) -> float:
    """Calculate estimated cost from token usage."""
    if not token_usage:
        return 0.0
    input_price = 0.15 / 1_000_000
    output_price = 0.60 / 1_000_000
    total_cost = 0.0
    if isinstance(token_usage, dict):
        if "total_tokens" in token_usage:
            prompt = token_usage.get("prompt_tokens", 0) or 0
            completion = token_usage.get("completion_tokens", 0) or 0
            total_cost = prompt * input_price + completion * output_price
        elif any(isinstance(v, dict) for v in token_usage.values()):
            for agent, usage in token_usage.items():
                if isinstance(usage, dict):
                    prompt = usage.get("prompt_tokens", 0) or 0
                    completion = usage.get("completion_tokens", 0) or 0
                    total_cost += prompt * input_price + completion * output_price
    return total_cost


def print_result_line(idx: int, total: int, result: Dict, sample: Dict):
    """Print a single result line."""
    status = "✓" if result["success"] else "✗"
    symbol = result["symbol"]
    quarter = f"{result['year']}-Q{result['quarter']}"
    category = sample.get("category", "?")

    if result["success"]:
        pred = result.get("prediction", "N/A")
        conf = result.get("confidence", 0) or 0
        actual = sample.get("actual_return_30d", 0)
        correct = calculate_correctness(pred, actual)
        correct_str = "✓" if correct else ("✗" if correct is False else "?")
        time_s = result["time_seconds"]
        print(f"  [{idx}/{total}] {status} {symbol} {quarter} ({category}) | Pred: {pred} ({conf:.2f}) | Actual: {actual:+.2f}% | Hit: {correct_str} | {time_s:.1f}s")
    else:
        error = result.get("error", "Unknown")[:60]
        print(f"  [{idx}/{total}] {status} {symbol} {quarter} ({category}) | ERROR: {error}")


def print_interim_summary(results: List[Dict], samples: List[Dict]):
    """Print interim summary statistics."""
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    # Build lookup for actual returns
    sample_lookup = {(s["symbol"], s["year"], s["quarter"]): s for s in samples}

    correct_count = 0
    total_with_prediction = 0
    gainers_correct = 0
    gainers_total = 0
    losers_correct = 0
    losers_total = 0

    for r in successful:
        key = (r["symbol"], r["year"], r["quarter"])
        sample = sample_lookup.get(key, {})
        actual = sample.get("actual_return_30d")
        category = sample.get("category")

        if r["prediction"] and actual is not None:
            total_with_prediction += 1
            is_correct = calculate_correctness(r["prediction"], actual)
            if is_correct:
                correct_count += 1

            if category == "GAINER":
                gainers_total += 1
                if is_correct:
                    gainers_correct += 1
            elif category == "LOSER":
                losers_total += 1
                if is_correct:
                    losers_correct += 1

    times = [r["time_seconds"] for r in successful]
    total_time = sum(times) if times else 0
    avg_time = total_time / len(times) if times else 0

    print(f"\n--- Interim Summary ({len(results)} completed) ---")
    print(f"Success: {len(successful)} | Failed: {len(failed)}")
    if total_with_prediction > 0:
        hit_rate = correct_count / total_with_prediction * 100
        print(f"Overall Hit Rate: {correct_count}/{total_with_prediction} = {hit_rate:.1f}%")
        if gainers_total > 0:
            print(f"  Gainers: {gainers_correct}/{gainers_total} = {gainers_correct/gainers_total*100:.1f}%")
        if losers_total > 0:
            print(f"  Losers: {losers_correct}/{losers_total} = {losers_correct/losers_total*100:.1f}%")
    print(f"Avg time: {avg_time:.1f}s | Total time: {total_time:.1f}s")


def save_results_csv(results: List[Dict], samples: List[Dict], filename: str) -> str:
    """Save detailed results to CSV."""
    output_path = project_root / filename
    sample_lookup = {(s["symbol"], s["year"], s["quarter"]): s for s in samples}

    rows = []
    for r in results:
        key = (r["symbol"], r["year"], r["quarter"])
        sample = sample_lookup.get(key, {})
        actual = sample.get("actual_return_30d")
        correct = calculate_correctness(r.get("prediction"), actual) if r["success"] else None

        row = {
            "symbol": r["symbol"],
            "year": r["year"],
            "quarter": r["quarter"],
            "category": sample.get("category", ""),
            "company_name": sample.get("company_name", ""),
            "sector": sample.get("sector", ""),
            "success": r["success"],
            "error": r.get("error", ""),
            "time_seconds": round(r["time_seconds"], 2),
            "prediction": r.get("prediction", ""),
            "confidence": round(r["confidence"], 3) if r.get("confidence") else "",
            "actual_return_30d_pct": round(actual, 2) if actual is not None else "",
            "correct": correct if correct is not None else "",
            "summary": str(r.get("summary") or "")[:500],
            "reasons": json.dumps(r.get("reasons") or []),
            "token_cost_usd": round(float((r.get("token_usage") or {}).get("cost_usd", 0.0) or 0.0), 6),
            "agent_notes": str(r.get("agent_notes") or "")[:1000],
        }
        rows.append(row)

    if rows:
        fieldnames = list(rows[0].keys())
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return str(output_path)


async def run_test_batch(samples: List[Dict], main_model: str, helper_model: str, all_samples: List[Dict], concurrency: int = 1) -> List[Dict]:
    """Run analysis on a batch of samples with optional concurrency."""
    results = []
    total = len(samples)
    completed = 0
    lock = asyncio.Lock()

    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def process_sample(idx: int, sample: Dict) -> Dict:
        nonlocal completed
        async with semaphore:
            result = await analyze_single(
                symbol=sample["symbol"],
                year=sample["year"],
                quarter=sample["quarter"],
                main_model=main_model,
                helper_model=helper_model,
            )

            # Add actual return from sample
            if result["actual_return"] is None:
                result["actual_return"] = sample.get("actual_return_30d")

            async with lock:
                completed += 1
                print_result_line(completed, total, result, sample)

                # Print interim summary every 20 results
                if completed % 20 == 0:
                    # Collect all completed results so far
                    completed_results = [r for r in results if r is not None]
                    completed_results.append(result)
                    print_interim_summary(completed_results, all_samples)

            return result

    if concurrency == 1:
        # Sequential execution (original behavior)
        for i, sample in enumerate(samples, 1):
            result = await analyze_single(
                symbol=sample["symbol"],
                year=sample["year"],
                quarter=sample["quarter"],
                main_model=main_model,
                helper_model=helper_model,
            )

            if result["actual_return"] is None:
                result["actual_return"] = sample.get("actual_return_30d")

            results.append(result)
            print_result_line(i, total, result, sample)

            if i % 10 == 0:
                print_interim_summary(results, all_samples)
    else:
        # Concurrent execution
        print(f"\n🚀 Running with {concurrency} concurrent workers...")
        tasks = [process_sample(i, sample) for i, sample in enumerate(samples, 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions that occurred
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                sample = samples[i]
                results[i] = {
                    "symbol": sample["symbol"],
                    "year": sample["year"],
                    "quarter": sample["quarter"],
                    "success": False,
                    "error": str(result),
                    "time_seconds": 0,
                    "prediction": None,
                    "confidence": None,
                    "actual_return": sample.get("actual_return_30d"),
                    "correct": None,
                    "summary": None,
                    "reasons": None,
                    "token_usage": None,
                    "agent_notes": None,
                }

    return results


async def main():
    """Main test runner."""
    # Parse arguments
    parser = argparse.ArgumentParser(description="Test pipeline for 2024 top movers")
    parser.add_argument("--test", action="store_true", help="Run only 3 test samples")
    parser.add_argument("--limit", "-n", type=int, default=0, help="Limit to N samples (0=all)")
    parser.add_argument("--concurrency", "-c", type=int, default=5, help="Number of concurrent workers (default: 5)")
    args = parser.parse_args()

    test_mode = args.test
    limit = args.limit
    concurrency = args.concurrency

    print("=" * 80)
    print("2024 Top Movers Analysis Pipeline Test")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'TEST (3 samples)' if test_mode else 'FULL (200 samples)'}")
    print(f"Concurrency: {concurrency} workers")

    main_model = os.getenv("MAIN_MODEL", "cli-gpt-5.2")
    helper_model = os.getenv("HELPER_MODEL", "cli-gpt-5.2")
    print(f"Models: main={main_model}, helper={helper_model}")

    # Get top movers
    print("\nFetching top movers from 2024...")
    try:
        gainers, losers = get_top_movers_2024(100)
    except Exception as e:
        logger.error(f"Failed to get samples: {e}")
        return

    print(f"Found {len(gainers)} top gainers and {len(losers)} top losers")

    if test_mode:
        # Test with just 3 samples (2 gainers, 1 loser)
        test_samples = gainers[:2] + losers[:1]
        print("\n--- TEST MODE: Running 3 samples ---")
        print("Top 2 Gainers:")
        for s in gainers[:2]:
            print(f"  {s['symbol']} {s['year']}-Q{s['quarter']}: {s['actual_return_30d']:+.2f}%")
        print("Top 1 Loser:")
        for s in losers[:1]:
            print(f"  {s['symbol']} {s['year']}-Q{s['quarter']}: {s['actual_return_30d']:+.2f}%")

        print("\n" + "-" * 80)
        print("Running test analyses...")
        print("-" * 80)

        results = await run_test_batch(test_samples, main_model, helper_model, test_samples, concurrency=1)

        # Check for errors
        errors = [r for r in results if not r["success"]]
        empty_predictions = [r for r in results if r["success"] and not r["prediction"]]

        print("\n" + "=" * 80)
        print("TEST RESULTS")
        print("=" * 80)
        print(f"Successful: {len([r for r in results if r['success']])}/{len(results)}")
        print(f"Errors: {len(errors)}")
        print(f"Empty predictions: {len(empty_predictions)}")

        if errors:
            print("\nErrors found (intermittent LiteLLM errors are expected):")
            for r in errors:
                print(f"  - {r['symbol']}: {r['error'][:100]}")

        if empty_predictions:
            print("\nEmpty predictions found:")
            for r in empty_predictions:
                print(f"  - {r['symbol']}")

        successful_count = len([r for r in results if r["success"]])
        if successful_count >= 2:
            print(f"\n✅ Test passed ({successful_count}/3 successful)! Run without --test to process all 200 samples")
        else:
            print("\n❌ Test failed - too many errors")
            return

        # Save test results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = save_results_csv(results, test_samples, f"test_results_movers_TEST_{timestamp}.csv")
        print(f"\n📁 Test results saved to: {csv_path}")

    else:
        # Full run with all 200 samples
        all_samples = gainers + losers
        if limit > 0:
            # Take limit/2 from gainers and limit/2 from losers for balanced sampling
            half = limit // 2
            all_samples = gainers[:half] + losers[:half]
        print(f"\nTotal samples to process: {len(all_samples)}")

        print("\nTop 5 Gainers:")
        for s in gainers[:5]:
            print(f"  {s['symbol']} {s['year']}-Q{s['quarter']} ({s['company_name'][:30]}): {s['actual_return_30d']:+.2f}%")

        print("\nTop 5 Losers:")
        for s in losers[:5]:
            print(f"  {s['symbol']} {s['year']}-Q{s['quarter']} ({s['company_name'][:30]}): {s['actual_return_30d']:+.2f}%")

        print("\n" + "-" * 80)
        print("Running analyses...")
        print("-" * 80)

        results = await run_test_batch(all_samples, main_model, helper_model, all_samples, concurrency=concurrency)

        # Final summary
        print("\n" + "=" * 80)
        print("FINAL SUMMARY")
        print("=" * 80)
        print_interim_summary(results, all_samples)

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = save_results_csv(results, all_samples, f"test_results_movers_FULL_{timestamp}.csv")
        print(f"\n📁 Full results saved to: {csv_path}")

    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
