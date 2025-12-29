#!/usr/bin/env python3
"""
Model Comparison Test: gpt-4o-mini vs gpt-5-mini
================================================
Compare price (token cost), judgment accuracy (hit rate), and processing time.

Runs 10 samples with each model (20 total - 5 gainers + 5 losers per model).
"""

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
        "model": main_model,
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
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cost_usd": 0.0,
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

        # Extract token counts
        token_usage = raw.get("token_usage", {})
        if token_usage:
            result["input_tokens"] = token_usage.get("input_tokens", 0) or token_usage.get("prompt_tokens", 0) or 0
            result["output_tokens"] = token_usage.get("output_tokens", 0) or token_usage.get("completion_tokens", 0) or 0
            result["total_tokens"] = token_usage.get("total_tokens", 0) or (result["input_tokens"] + result["output_tokens"])
            result["cost_usd"] = token_usage.get("cost_usd", 0) or 0

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


def calculate_token_cost(token_usage: Dict, model: str) -> float:
    """Calculate estimated cost from token usage based on model."""
    if not token_usage:
        return 0.0

    # Pricing per 1M tokens
    PRICING = {
        "gpt-4o-mini": {"input": 0.15, "output": 0.60},
        "gpt-5-mini": {"input": 0.15, "output": 0.60},  # Approximate
    }

    pricing = PRICING.get(model, {"input": 0.15, "output": 0.60})
    input_price = pricing["input"] / 1_000_000
    output_price = pricing["output"] / 1_000_000

    total_cost = 0.0
    if isinstance(token_usage, dict):
        prompt = token_usage.get("prompt_tokens", 0) or token_usage.get("input_tokens", 0) or 0
        completion = token_usage.get("completion_tokens", 0) or token_usage.get("output_tokens", 0) or 0
        total_cost = prompt * input_price + completion * output_price
    return total_cost


def print_result_line(idx: int, total: int, result: Dict, sample: Dict):
    """Print a single result line."""
    status = "✓" if result["success"] else "✗"
    symbol = result["symbol"]
    quarter = f"{result['year']}-Q{result['quarter']}"
    category = sample.get("category", "?")
    model = result.get("model", "?")

    if result["success"]:
        pred = result.get("prediction", "N/A")
        conf = result.get("confidence", 0) or 0
        actual = sample.get("actual_return_30d", 0)
        correct = calculate_correctness(pred, actual)
        correct_str = "✓" if correct else ("✗" if correct is False else "?")
        time_s = result["time_seconds"]
        tokens = result.get("total_tokens", 0)
        cost = result.get("cost_usd", 0)
        print(f"  [{idx}/{total}] {status} {symbol} {quarter} ({category}) [{model}] | Pred: {pred} ({conf:.2f}) | Actual: {actual:+.2f}% | Hit: {correct_str} | {time_s:.1f}s | {tokens} tokens | ${cost:.4f}")
    else:
        error = result.get("error", "Unknown")[:60]
        print(f"  [{idx}/{total}] {status} {symbol} {quarter} ({category}) [{model}] | ERROR: {error}")


def print_model_summary(model: str, results: List[Dict], samples: List[Dict]):
    """Print summary for a specific model."""
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    sample_lookup = {(s["symbol"], s["year"], s["quarter"]): s for s in samples}

    correct_count = 0
    total_with_prediction = 0
    gainers_correct = 0
    gainers_total = 0
    losers_correct = 0
    losers_total = 0
    total_tokens = 0
    total_cost = 0.0

    for r in successful:
        key = (r["symbol"], r["year"], r["quarter"])
        sample = sample_lookup.get(key, {})
        actual = sample.get("actual_return_30d")
        category = sample.get("category")

        total_tokens += r.get("total_tokens", 0)
        total_cost += r.get("cost_usd", 0) or calculate_token_cost(r.get("token_usage", {}), model)

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

    print(f"\n{'='*60}")
    print(f"Model: {model}")
    print(f"{'='*60}")
    print(f"Total samples: {len(results)} | Successful: {len(successful)} | Failed: {len(failed)}")

    if total_with_prediction > 0:
        hit_rate = correct_count / total_with_prediction * 100
        print(f"\nAccuracy:")
        print(f"  Overall Hit Rate: {correct_count}/{total_with_prediction} = {hit_rate:.1f}%")
        if gainers_total > 0:
            print(f"  Gainers: {gainers_correct}/{gainers_total} = {gainers_correct/gainers_total*100:.1f}%")
        if losers_total > 0:
            print(f"  Losers: {losers_correct}/{losers_total} = {losers_correct/losers_total*100:.1f}%")

    print(f"\nTime:")
    print(f"  Total time: {total_time:.1f}s")
    print(f"  Avg time per sample: {avg_time:.1f}s")

    print(f"\nCost:")
    print(f"  Total tokens: {total_tokens:,}")
    print(f"  Total cost: ${total_cost:.4f}")
    print(f"  Avg cost per sample: ${total_cost/len(successful):.4f}" if successful else "  Avg cost: N/A")

    return {
        "model": model,
        "total_samples": len(results),
        "successful": len(successful),
        "failed": len(failed),
        "hit_rate": correct_count / total_with_prediction * 100 if total_with_prediction > 0 else 0,
        "gainers_hit_rate": gainers_correct / gainers_total * 100 if gainers_total > 0 else 0,
        "losers_hit_rate": losers_correct / losers_total * 100 if losers_total > 0 else 0,
        "total_time": total_time,
        "avg_time": avg_time,
        "total_tokens": total_tokens,
        "total_cost": total_cost,
        "avg_cost": total_cost / len(successful) if successful else 0,
    }


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
            "model": r.get("model", ""),
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
            "input_tokens": r.get("input_tokens", 0),
            "output_tokens": r.get("output_tokens", 0),
            "total_tokens": r.get("total_tokens", 0),
            "cost_usd": round(r.get("cost_usd", 0), 6),
            "summary": str(r.get("summary") or "")[:500],
        }
        rows.append(row)

    if rows:
        fieldnames = list(rows[0].keys())
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return str(output_path)


async def run_model_test(
    model: str,
    samples: List[Dict],
    all_samples: List[Dict],
) -> List[Dict]:
    """Run analysis on samples with a specific model."""
    results = []
    total = len(samples)

    print(f"\n{'='*60}")
    print(f"Testing model: {model}")
    print(f"{'='*60}")

    for i, sample in enumerate(samples, 1):
        result = await analyze_single(
            symbol=sample["symbol"],
            year=sample["year"],
            quarter=sample["quarter"],
            main_model=model,
            helper_model=model,
        )

        if result["actual_return"] is None:
            result["actual_return"] = sample.get("actual_return_30d")

        results.append(result)
        print_result_line(i, total, result, sample)

    return results


async def main():
    """Main comparison runner."""
    print("=" * 80)
    print("Model Comparison: gpt-4o-mini vs gpt-5-mini")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Testing: 10 samples per model (5 gainers + 5 losers)")

    # Get top movers
    print("\nFetching top movers from 2024...")
    try:
        gainers, losers = get_top_movers_2024(100)
    except Exception as e:
        logger.error(f"Failed to get samples: {e}")
        return

    print(f"Found {len(gainers)} top gainers and {len(losers)} top losers")

    # Select 5 gainers + 5 losers for testing
    test_gainers = gainers[:5]
    test_losers = losers[:5]
    test_samples = test_gainers + test_losers

    print("\nTest samples:")
    print("Gainers:")
    for s in test_gainers:
        print(f"  {s['symbol']} {s['year']}-Q{s['quarter']}: {s['actual_return_30d']:+.2f}%")
    print("Losers:")
    for s in test_losers:
        print(f"  {s['symbol']} {s['year']}-Q{s['quarter']}: {s['actual_return_30d']:+.2f}%")

    models_to_test = ["gpt-4o-mini", "gpt-5-mini"]
    all_results = {}
    summaries = []

    for model in models_to_test:
        results = await run_model_test(model, test_samples, test_samples)
        all_results[model] = results
        summary = print_model_summary(model, results, test_samples)
        summaries.append(summary)

    # Print comparison table
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY")
    print("=" * 80)
    print(f"{'Metric':<25} {'gpt-4o-mini':<20} {'gpt-5-mini':<20}")
    print("-" * 65)

    s1, s2 = summaries[0], summaries[1]
    print(f"{'Overall Hit Rate':<25} {s1['hit_rate']:<20.1f}% {s2['hit_rate']:<18.1f}%")
    print(f"{'Gainers Hit Rate':<25} {s1['gainers_hit_rate']:<20.1f}% {s2['gainers_hit_rate']:<18.1f}%")
    print(f"{'Losers Hit Rate':<25} {s1['losers_hit_rate']:<20.1f}% {s2['losers_hit_rate']:<18.1f}%")
    print(f"{'Avg Time (s)':<25} {s1['avg_time']:<20.1f} {s2['avg_time']:<20.1f}")
    print(f"{'Total Time (s)':<25} {s1['total_time']:<20.1f} {s2['total_time']:<20.1f}")
    print(f"{'Total Tokens':<25} {s1['total_tokens']:<20,} {s2['total_tokens']:<20,}")
    print(f"{'Total Cost ($)':<25} {s1['total_cost']:<20.4f} {s2['total_cost']:<20.4f}")
    print(f"{'Avg Cost/Sample ($)':<25} {s1['avg_cost']:<20.4f} {s2['avg_cost']:<20.4f}")

    # Save combined results
    combined_results = all_results["gpt-4o-mini"] + all_results["gpt-5-mini"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = save_results_csv(combined_results, test_samples * 2, f"model_comparison_{timestamp}.csv")
    print(f"\n📁 Results saved to: {csv_path}")

    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
