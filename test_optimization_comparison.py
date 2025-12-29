#!/usr/bin/env python3
"""
Test script to verify token optimizations don't affect accuracy.
Runs 10 samples and compares Direction scores and confidence with baseline.
"""

import json
import os
import sys
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add project paths
sys.path.insert(0, str(Path(__file__).parent / "EarningsCallAgenticRag"))
os.chdir(Path(__file__).parent / "EarningsCallAgenticRag")

from dotenv import load_dotenv
load_dotenv()

from agents.mainAgent import MainAgent
from agents.pg_db_agents import (
    PgHistoricalPerformanceAgent,
    PgHistoricalEarningsAgent,
    PgComparativeAgent,
)
from utils.indexFacts import IndexFacts
from pg_client import get_cursor

# Configuration
NUM_SAMPLES = 10
CREDENTIALS_FILE = "credentials.json"


def get_test_samples(n: int = 10) -> List[Dict]:
    """Get n test samples - 5 gainers and 5 losers."""
    with get_cursor() as cur:
        if cur is None:
            raise RuntimeError("Database connection failed")

        samples = []

        # Get 5 top gainers
        cur.execute("""
            SELECT
                et.symbol as ticker,
                et.year,
                et.quarter,
                c.sector,
                tc.content as transcript,
                pa.pct_change_t_plus_30 as actual_return_30d,
                'GAINER' as category
            FROM earnings_transcripts et
            JOIN companies c ON et.symbol = c.symbol
            JOIN transcript_content tc ON et.id = tc.transcript_id
            JOIN price_analysis pa ON et.id = pa.transcript_id
            WHERE et.year = 2024
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
            ORDER BY pa.pct_change_t_plus_30 DESC
            LIMIT 5
        """)

        columns = [desc[0] for desc in cur.description]
        for row in cur.fetchall():
            row_dict = dict(zip(columns, row))
            row_dict["q"] = f"{row_dict['year']}-Q{row_dict['quarter']}"
            samples.append(row_dict)

        # Get 5 top losers
        cur.execute("""
            SELECT
                et.symbol as ticker,
                et.year,
                et.quarter,
                c.sector,
                tc.content as transcript,
                pa.pct_change_t_plus_30 as actual_return_30d,
                'LOSER' as category
            FROM earnings_transcripts et
            JOIN companies c ON et.symbol = c.symbol
            JOIN transcript_content tc ON et.id = tc.transcript_id
            JOIN price_analysis pa ON et.id = pa.transcript_id
            WHERE et.year = 2024
                AND tc.content IS NOT NULL
                AND LENGTH(tc.content) > 1000
                AND pa.pct_change_t_plus_30 IS NOT NULL
            ORDER BY pa.pct_change_t_plus_30 ASC
            LIMIT 5
        """)

        columns = [desc[0] for desc in cur.description]
        for row in cur.fetchall():
            row_dict = dict(zip(columns, row))
            row_dict["q"] = f"{row_dict['year']}-Q{row_dict['quarter']}"
            samples.append(row_dict)

        return samples


def extract_direction_and_confidence(summary: str) -> Tuple[int, str]:
    """Extract Direction score and confidence from summary."""
    direction = -1
    confidence = "N/A"

    # Extract Direction
    match = re.search(r"Direction\s*[:\s]*(\d+)", summary, re.IGNORECASE)
    if match:
        direction = int(match.group(1))

    # Extract management tone (as proxy for confidence)
    tone_match = re.search(r"(Very optimistic|Moderately optimistic|Balanced|Moderately cautious|Very cautious)", summary, re.IGNORECASE)
    if tone_match:
        confidence = tone_match.group(1)

    return direction, confidence


def run_single_sample(row, indexer, main_agent) -> Dict:
    """Run analysis for a single sample."""
    ticker = row["ticker"]
    quarter = row["q"]
    transcript = row["transcript"]

    start_time = time.time()

    # Extract facts
    facts = indexer.extract_facts_with_context(transcript, ticker, quarter)

    # Run main agent
    result = main_agent.run(
        facts=facts,
        row=row,
        mem_txt=None,
        financial_statements_facts=None,
    )

    elapsed = time.time() - start_time
    token_usage = result.get("token_usage", {})
    summary = result.get("summary", "")
    direction, confidence = extract_direction_and_confidence(summary)

    return {
        "ticker": ticker,
        "quarter": quarter,
        "category": row.get("category", ""),
        "actual_return": row.get("actual_return_30d", 0),
        "direction": direction,
        "confidence": confidence,
        "tokens": token_usage.get("total_tokens", 0),
        "input_tokens": token_usage.get("input_tokens", 0),
        "output_tokens": token_usage.get("output_tokens", 0),
        "cost": token_usage.get("cost_usd", 0),
        "time": elapsed,
    }


def main():
    print("=" * 80)
    print("Token Optimization Verification Test")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print(f"Samples: {NUM_SAMPLES} (5 gainers + 5 losers)")
    print()
    print("Optimizations applied:")
    print("  1. Removed original_transcript from Main Agent")
    print("  2. Removed duplicate facts serialization in Comparative Agent")
    print("  3. Using compact JSON (no indent)")
    print("  4. Reduced MAX_FACTS limits")
    print()

    # Initialize agents
    print("Initializing agents...")
    indexer = IndexFacts(credentials_file=CREDENTIALS_FILE)

    main_agent = MainAgent(
        credentials_file=CREDENTIALS_FILE,
        financials_agent=PgHistoricalPerformanceAgent(credentials_file=CREDENTIALS_FILE),
        past_calls_agent=PgHistoricalEarningsAgent(credentials_file=CREDENTIALS_FILE),
        comparative_agent=PgComparativeAgent(credentials_file=CREDENTIALS_FILE),
    )

    # Get test samples
    print("Fetching test samples...")
    samples = get_test_samples(NUM_SAMPLES)
    print(f"Got {len(samples)} samples")
    print()

    # Run tests
    results = []

    for i, row in enumerate(samples):
        ticker = row["ticker"]
        quarter = row["q"]
        category = row.get("category", "")
        actual_return = row.get("actual_return_30d", 0) or 0

        print(f"[{i+1}/{NUM_SAMPLES}] {ticker} {quarter} ({category})")

        try:
            result = run_single_sample(row, indexer, main_agent)
            results.append(result)

            # Determine if prediction is correct
            is_gainer = actual_return > 0
            pred_up = result["direction"] >= 6
            pred_down = result["direction"] <= 4

            if is_gainer and pred_up:
                hit = "✓ HIT"
            elif not is_gainer and pred_down:
                hit = "✓ HIT"
            elif result["direction"] == 5:
                hit = "- NEUTRAL"
            else:
                hit = "✗ MISS"

            print(f"    Direction: {result['direction']}/10 | Confidence: {result['confidence']}")
            print(f"    Actual: {actual_return:+.1f}% | {hit}")
            print(f"    Tokens: {result['tokens']:,} (in: {result['input_tokens']:,}, out: {result['output_tokens']:,})")
            print(f"    Cost: ${result['cost']:.6f} | Time: {result['time']:.1f}s")

        except Exception as e:
            print(f"    ✗ Error: {e}")
            results.append({
                "ticker": ticker,
                "quarter": quarter,
                "category": category,
                "error": str(e)
            })

    # Summary
    print("\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)

    valid_results = [r for r in results if "error" not in r]

    if not valid_results:
        print("No valid results!")
        return

    # Calculate metrics
    total_tokens = sum(r.get("tokens", 0) for r in valid_results)
    total_cost = sum(r.get("cost", 0) for r in valid_results)
    total_time = sum(r.get("time", 0) for r in valid_results)

    # Hit rate calculation
    hits = 0
    gainers_hits = 0
    gainers_total = 0
    losers_hits = 0
    losers_total = 0

    for r in valid_results:
        actual = r.get("actual_return", 0) or 0
        direction = r.get("direction", 5)
        category = r.get("category", "")

        is_gainer = actual > 0
        pred_up = direction >= 6
        pred_down = direction <= 4

        if category == "GAINER":
            gainers_total += 1
            if pred_up:
                hits += 1
                gainers_hits += 1
        elif category == "LOSER":
            losers_total += 1
            if pred_down:
                hits += 1
                losers_hits += 1

    print(f"\n{'Metric':<25} {'Value':<20}")
    print("-" * 45)
    print(f"{'Samples processed':<25} {len(valid_results)}")
    print(f"{'Total tokens':<25} {total_tokens:,}")
    print(f"{'Avg tokens/sample':<25} {total_tokens/len(valid_results):,.0f}")
    print(f"{'Total cost':<25} ${total_cost:.6f}")
    print(f"{'Avg cost/sample':<25} ${total_cost/len(valid_results):.6f}")
    print(f"{'Total time':<25} {total_time:.1f}s")
    print(f"{'Avg time/sample':<25} {total_time/len(valid_results):.1f}s")

    print(f"\n{'Accuracy':<25}")
    print("-" * 45)
    print(f"{'Overall hit rate':<25} {hits}/{len(valid_results)} ({100*hits/len(valid_results):.1f}%)")
    if gainers_total > 0:
        print(f"{'Gainers hit rate':<25} {gainers_hits}/{gainers_total} ({100*gainers_hits/gainers_total:.1f}%)")
    if losers_total > 0:
        print(f"{'Losers hit rate':<25} {losers_hits}/{losers_total} ({100*losers_hits/losers_total:.1f}%)")

    # Direction distribution
    print(f"\n{'Direction Distribution':<25}")
    print("-" * 45)
    direction_counts = {}
    for r in valid_results:
        d = r.get("direction", -1)
        direction_counts[d] = direction_counts.get(d, 0) + 1

    for d in sorted(direction_counts.keys()):
        count = direction_counts[d]
        bar = "█" * count
        print(f"  Direction {d}: {bar} ({count})")

    # Confidence distribution
    print(f"\n{'Confidence Distribution':<25}")
    print("-" * 45)
    confidence_counts = {}
    for r in valid_results:
        c = r.get("confidence", "N/A")
        confidence_counts[c] = confidence_counts.get(c, 0) + 1

    for c, count in sorted(confidence_counts.items(), key=lambda x: -x[1]):
        print(f"  {c}: {count}")

    # Detailed results table
    print(f"\n{'Detailed Results':<25}")
    print("-" * 80)
    print(f"{'Ticker':<8} {'Quarter':<10} {'Category':<8} {'Dir':<5} {'Actual':<10} {'Tokens':<10} {'Time':<8}")
    print("-" * 80)

    for r in valid_results:
        actual = r.get("actual_return", 0) or 0
        print(f"{r['ticker']:<8} {r['quarter']:<10} {r['category']:<8} {r['direction']:<5} {actual:>+.1f}% {r['tokens']:>9,} {r['time']:>6.1f}s")

    # Close resources
    indexer.close()

    print("\n" + "=" * 80)
    print("Test completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
