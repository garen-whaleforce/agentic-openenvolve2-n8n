#!/usr/bin/env python3
"""
Long-only strategy test script.
Runs a subset of samples to validate the new LongEligible JSON output.
"""

import asyncio
import os
import sys
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from analysis_engine import analyze_earnings_async
from pg_client import get_cursor


def get_samples(year_start: int = 2019, year_end: int = 2025, limit_per_category: int = 50) -> List[Dict]:
    """Get balanced samples (GAINER/LOSER) across years."""
    with get_cursor() as cur:
        if cur is None:
            raise RuntimeError("Database connection failed")

        samples = []

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
            WHERE et.year BETWEEN %s AND %s
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
                AND pa.pct_change_t_plus_30 > 10
            ORDER BY RANDOM()
            LIMIT %s
        """, (year_start, year_end, limit_per_category))

        for row in cur.fetchall():
            samples.append({
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
            WHERE et.year BETWEEN %s AND %s
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
                AND pa.pct_change_t_plus_30 < -10
            ORDER BY RANDOM()
            LIMIT %s
        """, (year_start, year_end, limit_per_category))

        for row in cur.fetchall():
            samples.append({
                "symbol": row["symbol"],
                "year": row["year"],
                "quarter": row["quarter"],
                "transcript_date": row["transcript_date_str"],
                "company_name": row["company_name"],
                "sector": row["sector"],
                "actual_return_30d": float(row["actual_return_30d"]),
                "category": "LOSER"
            })

        return samples


async def run_single_test(symbol: str, year: int, quarter: int, category: str, company_name: str, sector: str):
    """Run a single test and return results."""
    start = time.time()
    try:
        result = await analyze_earnings_async(
            symbol=symbol,
            year=year,
            quarter=quarter,
            skip_cache=True,
        )
        elapsed = time.time() - start

        agentic_result = result.get("agentic_result", {})
        prediction = agentic_result.get("prediction", "UNKNOWN")
        confidence = agentic_result.get("confidence")
        summary = agentic_result.get("summary", "")
        trade_long = agentic_result.get("trade_long", False)
        long_eligible_json = agentic_result.get("long_eligible_json", {})

        actual_return = result.get("post_earnings_return")

        # Determine correctness
        correct = None
        if actual_return is not None and prediction in ["UP", "DOWN"]:
            if prediction == "UP":
                correct = actual_return > 0
            else:
                correct = actual_return < 0

        # Extract direction score from long_eligible_json
        direction_score = None
        if long_eligible_json:
            direction_score = long_eligible_json.get("DirectionScore")

        return {
            "symbol": symbol,
            "year": year,
            "quarter": quarter,
            "category": category,
            "company_name": company_name,
            "sector": sector,
            "success": True,
            "error": None,
            "time_seconds": elapsed,
            "prediction": prediction,
            "confidence": confidence,
            "direction_score": direction_score,
            "actual_return_30d_pct": actual_return,
            "correct": correct,
            "trade_long": trade_long,
            "long_eligible_json": long_eligible_json,
            "summary": summary[:500] if summary else "",
        }

    except Exception as e:
        elapsed = time.time() - start
        return {
            "symbol": symbol,
            "year": year,
            "quarter": quarter,
            "category": category,
            "company_name": company_name,
            "sector": sector,
            "success": False,
            "error": str(e),
            "time_seconds": elapsed,
            "prediction": None,
            "confidence": None,
            "direction_score": None,
            "actual_return_30d_pct": None,
            "correct": None,
            "trade_long": False,
            "long_eligible_json": None,
            "summary": None,
        }


async def main():
    """Main test function."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=20, help="Number of samples to test")
    parser.add_argument("--test", action="store_true", help="Run only 3 samples for quick test")
    args = parser.parse_args()

    NUM_SAMPLES = 3 if args.test else args.samples

    print("=" * 70)
    print(f"Long-only Strategy Validation Test")
    print(f"Samples: {NUM_SAMPLES}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Get balanced samples
    all_samples = get_samples(year_start=2019, year_end=2025, limit_per_category=NUM_SAMPLES)
    random.shuffle(all_samples)
    samples = all_samples[:NUM_SAMPLES]

    print(f"\nLoaded {len(samples)} samples")
    if samples:
        df_samples = pd.DataFrame(samples)
        print(f"Year distribution: {df_samples['year'].value_counts().to_dict()}")
        print(f"Category distribution: {df_samples['category'].value_counts().to_dict()}")

    # Run tests
    results = []
    for i, sample in enumerate(samples, 1):
        print(f"\n[{i}/{len(samples)}] Testing {sample['symbol']} {sample['year']}Q{sample['quarter']} ({sample['category']})...")

        result = await run_single_test(
            symbol=sample["symbol"],
            year=sample["year"],
            quarter=sample["quarter"],
            category=sample["category"],
            company_name=sample.get("company_name", ""),
            sector=sample.get("sector", ""),
        )
        results.append(result)

        # Print result
        if result["success"]:
            print(f"  Prediction: {result['prediction']} (Direction: {result['direction_score']})")
            print(f"  Trade Long: {result['trade_long']}")
            ret_str = f"{result['actual_return_30d_pct']:.2f}%" if result['actual_return_30d_pct'] else "N/A"
            print(f"  Actual Return: {ret_str}")
            print(f"  Correct: {result['correct']}")
            if result['long_eligible_json']:
                lj = result['long_eligible_json']
                print(f"  Hard Positives: {lj.get('HardPositivesCount', 'N/A')}, Hard Vetoes: {lj.get('HardVetoCount', 'N/A')}")
                print(f"  PricedInRisk: {lj.get('PricedInRisk', 'N/A')}, LongEligible: {lj.get('LongEligible', 'N/A')}")
        else:
            print(f"  ERROR: {result['error']}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    df = pd.DataFrame(results)
    successful = df[df["success"] == True]

    if len(successful) > 0:
        # Overall accuracy
        with_prediction = successful[successful["correct"].notna()]
        if len(with_prediction) > 0:
            overall_accuracy = with_prediction["correct"].mean() * 100
            print(f"\nOverall Accuracy: {overall_accuracy:.1f}% ({int(with_prediction['correct'].sum())}/{len(with_prediction)})")

        # Long-only accuracy
        trade_long_df = successful[successful["trade_long"] == True]
        if len(trade_long_df) > 0:
            trade_long_correct = trade_long_df[trade_long_df["correct"].notna()]
            if len(trade_long_correct) > 0:
                long_accuracy = trade_long_correct["correct"].mean() * 100
                print(f"\nLong-only Strategy:")
                print(f"  Coverage: {len(trade_long_df)}/{len(successful)} ({len(trade_long_df)/len(successful)*100:.1f}%)")
                print(f"  Win Rate: {long_accuracy:.1f}% ({int(trade_long_correct['correct'].sum())}/{len(trade_long_correct)})")

                # Average return for trade_long
                avg_return = trade_long_df["actual_return_30d_pct"].mean()
                print(f"  Avg Return: {avg_return:.2f}%")
        else:
            print(f"\nNo samples qualified for Long-only (trade_long=True)")

        # Direction score distribution
        print(f"\nDirection Score Distribution:")
        direction_scores = successful["direction_score"].dropna()
        if len(direction_scores) > 0:
            for score in sorted(direction_scores.unique()):
                count = (direction_scores == score).sum()
                print(f"  Score {int(score)}: {count} samples")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"/Users/garen.lee/Coding/agentic-openenvolve2/long_only_test_{timestamp}.csv"

    # Flatten long_eligible_json for CSV
    for r in results:
        if r.get("long_eligible_json"):
            for k, v in r["long_eligible_json"].items():
                r[f"le_{k}"] = v
        if "long_eligible_json" in r:
            del r["long_eligible_json"]

    df = pd.DataFrame(results)
    df.to_csv(csv_path, index=False)
    print(f"\nResults saved to: {csv_path}")


if __name__ == "__main__":
    asyncio.run(main())
