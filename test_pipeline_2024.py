#!/usr/bin/env python3
"""
Test Pipeline for 2024 Earnings Analysis
=========================================
Randomly selects 10 earnings calls from 2024, runs the full analysis pipeline,
and outputs detailed results including:
- Prediction accuracy (hit rate)
- Time spent per analysis
- Token costs
- Detailed agent outputs

Usage:
    python test_pipeline_2024.py
"""

import asyncio
import csv
import json
import logging
import os
import random
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


def get_random_2024_samples(n: int = 10) -> List[Dict[str, Any]]:
    """Get n random earnings calls from 2024 that have transcripts and price analysis."""
    from pg_client import get_cursor

    with get_cursor() as cur:
        if cur is None:
            raise RuntimeError("Database connection failed")

        # Query 2024 earnings with transcripts and price analysis
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
            LEFT JOIN price_analysis pa ON et.id = pa.transcript_id
            WHERE et.year = 2024
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
            ORDER BY RANDOM()
            LIMIT %s
        """, (n,))

        results = []
        for row in cur.fetchall():
            results.append({
                "symbol": row["symbol"],
                "year": row["year"],
                "quarter": row["quarter"],
                "transcript_date": row["transcript_date_str"],
                "company_name": row["company_name"],
                "sector": row["sector"],
                "actual_return_30d": float(row["actual_return_30d"]) if row["actual_return_30d"] else None,
            })

        return results


async def analyze_single(
    symbol: str,
    year: int,
    quarter: int,
    main_model: str = None,
    helper_model: str = None,
) -> Dict[str, Any]:
    """Run analysis for a single earnings call and capture detailed metrics."""
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
            skip_cache=True,  # Force fresh analysis
        )

        result["success"] = True
        result["time_seconds"] = time.time() - start_time

        # Extract prediction info
        agentic = payload.get("agentic_result", {})
        result["prediction"] = agentic.get("prediction")
        result["confidence"] = agentic.get("confidence")
        result["summary"] = agentic.get("summary")
        result["reasons"] = agentic.get("reasons")

        # Extract token usage from raw
        raw = agentic.get("raw", {})
        result["token_usage"] = raw.get("token_usage")
        result["agent_notes"] = raw.get("notes")

        # Get actual return from backtest
        backtest = payload.get("backtest", {})
        if backtest and backtest.get("change_pct") is not None:
            result["actual_return"] = backtest["change_pct"]
        elif payload.get("post_earnings_return") is not None:
            result["actual_return"] = payload["post_earnings_return"]

        # Determine correctness
        if result["actual_return"] is not None and result["prediction"]:
            pred = result["prediction"].upper()
            ret = result["actual_return"]
            if pred == "UP":
                result["correct"] = ret > 0
            elif pred == "DOWN":
                result["correct"] = ret < 0
            elif pred == "NEUTRAL":
                result["correct"] = abs(ret) < 1.0  # Within 1%

    except Exception as e:
        result["error"] = str(e)
        result["time_seconds"] = time.time() - start_time
        logger.error(f"Analysis failed for {symbol} {year}-Q{quarter}: {e}")

    return result


def calculate_token_cost(token_usage: Dict) -> float:
    """Calculate estimated cost from token usage."""
    if not token_usage:
        return 0.0

    # Pricing for cli-gpt-5.2 via LiteLLM (estimate based on GPT-4o-mini)
    # Input: $0.15 / 1M tokens, Output: $0.60 / 1M tokens
    input_price = 0.15 / 1_000_000
    output_price = 0.60 / 1_000_000

    total_cost = 0.0

    # Handle different token_usage structures
    if isinstance(token_usage, dict):
        # Check for aggregated format
        if "total_tokens" in token_usage:
            prompt = token_usage.get("prompt_tokens", 0) or 0
            completion = token_usage.get("completion_tokens", 0) or 0
            total_cost = prompt * input_price + completion * output_price
        # Check for per-agent breakdown
        elif any(isinstance(v, dict) for v in token_usage.values()):
            for agent, usage in token_usage.items():
                if isinstance(usage, dict):
                    prompt = usage.get("prompt_tokens", 0) or 0
                    completion = usage.get("completion_tokens", 0) or 0
                    total_cost += prompt * input_price + completion * output_price

    return total_cost


def print_summary(results: List[Dict[str, Any]]) -> None:
    """Print summary statistics."""
    print("\n" + "=" * 80)
    print("PIPELINE TEST SUMMARY")
    print("=" * 80)

    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    print(f"\nTotal tests: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed analyses:")
        for r in failed:
            print(f"  - {r['symbol']} {r['year']}-Q{r['quarter']}: {r['error'][:100]}")

    if successful:
        # Accuracy
        with_prediction = [r for r in successful if r["correct"] is not None]
        correct = [r for r in with_prediction if r["correct"]]

        print(f"\n--- Prediction Accuracy ---")
        print(f"Predictions with known outcome: {len(with_prediction)}")
        if with_prediction:
            accuracy = len(correct) / len(with_prediction) * 100
            print(f"Correct predictions: {len(correct)}")
            print(f"Hit rate: {accuracy:.1f}%")

        # Timing
        times = [r["time_seconds"] for r in successful]
        print(f"\n--- Timing ---")
        print(f"Total time: {sum(times):.1f}s")
        print(f"Average time per analysis: {sum(times)/len(times):.1f}s")
        print(f"Min time: {min(times):.1f}s")
        print(f"Max time: {max(times):.1f}s")

        # Cost
        costs = [calculate_token_cost(r["token_usage"]) for r in successful]
        total_cost = sum(costs)
        print(f"\n--- Token Cost (estimated) ---")
        print(f"Total cost: ${total_cost:.4f}")
        print(f"Average cost per analysis: ${total_cost/len(successful):.4f}")

        # Token counts
        total_tokens = 0
        for r in successful:
            if r["token_usage"]:
                if "total_tokens" in r["token_usage"]:
                    total_tokens += r["token_usage"]["total_tokens"] or 0
                elif isinstance(r["token_usage"], dict):
                    for usage in r["token_usage"].values():
                        if isinstance(usage, dict):
                            total_tokens += usage.get("total_tokens", 0) or 0

        if total_tokens:
            print(f"Total tokens used: {total_tokens:,}")


def save_results_csv(results: List[Dict[str, Any]], filename: str) -> str:
    """Save detailed results to CSV."""
    output_path = project_root / filename

    # Flatten results for CSV
    rows = []
    for r in results:
        row = {
            "symbol": r["symbol"],
            "year": r["year"],
            "quarter": r["quarter"],
            "success": r["success"],
            "error": r.get("error", ""),
            "time_seconds": round(r["time_seconds"], 2),
            "prediction": r.get("prediction", ""),
            "confidence": round(r["confidence"], 3) if r.get("confidence") else "",
            "actual_return_pct": round(r["actual_return"], 2) if r.get("actual_return") is not None else "",
            "correct": r.get("correct", ""),
            "summary": (r.get("summary") or "")[:500],  # Truncate for CSV
            "reasons": json.dumps(r.get("reasons") or []),
            "token_cost_usd": round(calculate_token_cost(r.get("token_usage")), 4),
            "token_usage": json.dumps(r.get("token_usage") or {}),
            "agent_notes": str(r.get("agent_notes") or "")[:1000],  # Truncate for CSV
        }
        rows.append(row)

    fieldnames = list(rows[0].keys()) if rows else []

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return str(output_path)


async def main():
    """Main test runner."""
    print("=" * 80)
    print("2024 Earnings Analysis Pipeline Test")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Get model config
    main_model = os.getenv("MAIN_MODEL", "cli-gpt-5.2")
    helper_model = os.getenv("HELPER_MODEL", "cli-gpt-5.2")
    print(f"\nModels: main={main_model}, helper={helper_model}")

    # Get random samples
    print("\nFetching random 2024 earnings calls...")
    try:
        samples = get_random_2024_samples(10)
    except Exception as e:
        logger.error(f"Failed to get samples: {e}")
        return

    if not samples:
        print("No samples found in database!")
        return

    print(f"Found {len(samples)} samples:")
    for s in samples:
        print(f"  - {s['symbol']} {s['year']}-Q{s['quarter']} ({s['company_name']}) | Sector: {s['sector']} | Actual 30d return: {s['actual_return_30d']:.2f}%")

    # Run analyses
    print("\n" + "-" * 80)
    print("Running analyses...")
    print("-" * 80)

    results = []
    for i, sample in enumerate(samples, 1):
        print(f"\n[{i}/{len(samples)}] Analyzing {sample['symbol']} {sample['year']}-Q{sample['quarter']}...")
        result = await analyze_single(
            symbol=sample["symbol"],
            year=sample["year"],
            quarter=sample["quarter"],
            main_model=main_model,
            helper_model=helper_model,
        )

        # Add sample metadata
        result["company_name"] = sample["company_name"]
        result["sector"] = sample["sector"]

        # Use actual return from sample if not in result
        if result["actual_return"] is None and sample["actual_return_30d"] is not None:
            result["actual_return"] = sample["actual_return_30d"]
            # Re-calculate correctness
            if result["prediction"]:
                pred = result["prediction"].upper()
                ret = result["actual_return"]
                if pred == "UP":
                    result["correct"] = ret > 0
                elif pred == "DOWN":
                    result["correct"] = ret < 0
                elif pred == "NEUTRAL":
                    result["correct"] = abs(ret) < 1.0

        results.append(result)

        # Print interim result
        status = "✓" if result["success"] else "✗"
        if result["success"]:
            pred = result.get("prediction", "N/A")
            conf = result.get("confidence", 0)
            actual = result.get("actual_return")
            correct = result.get("correct")
            correct_str = "✓" if correct else ("✗" if correct is False else "?")
            print(f"  {status} Prediction: {pred} (conf: {conf:.2f}) | Actual: {actual:.2f}% | Correct: {correct_str} | Time: {result['time_seconds']:.1f}s")
        else:
            print(f"  {status} Error: {result.get('error', 'Unknown error')[:80]}")

    # Print summary
    print_summary(results)

    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"test_results_2024_{timestamp}.csv"
    csv_path = save_results_csv(results, csv_filename)
    print(f"\n📁 Detailed results saved to: {csv_path}")

    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
