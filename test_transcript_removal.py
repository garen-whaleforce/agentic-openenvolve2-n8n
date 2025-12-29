#!/usr/bin/env python3
"""
Test script to compare results with and without transcript in Main Agent.
Runs 5 samples with both configurations to compare:
1. Prediction accuracy (Direction score)
2. Token usage / cost
3. Time per sample
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

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
NUM_SAMPLES = 5
CREDENTIALS_FILE = "credentials.json"


def get_test_samples(n: int = 5) -> List[Dict]:
    """Get n test samples from top movers."""
    with get_cursor() as cur:
        if cur is None:
            raise RuntimeError("Database connection failed")

        # Top gainers (largest positive returns)
        cur.execute("""
            SELECT
                et.symbol as ticker,
                et.year,
                et.quarter,
                et.transcript_date_str,
                c.name as company_name,
                c.sector,
                tc.content as transcript,
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
        """, (n,))

        columns = [desc[0] for desc in cur.description]
        rows = []
        for row in cur.fetchall():
            row_dict = dict(zip(columns, row))
            # Add 'q' field for compatibility
            row_dict["q"] = f"{row_dict['year']}-Q{row_dict['quarter']}"
            rows.append(row_dict)

        return rows


def run_single_sample_with_transcript(row, indexer, main_agent):
    """Run analysis WITH full transcript (current behavior)."""
    ticker = row["ticker"]
    quarter = row["q"]
    transcript = row["transcript"]

    start_time = time.time()

    # Extract facts
    facts = indexer.extract_facts_with_context(transcript, ticker, quarter)

    # Run main agent (with transcript)
    result = main_agent.run(
        facts=facts,
        row=row,
        mem_txt=None,
        original_transcript=transcript,  # WITH transcript
        financial_statements_facts=None,
    )

    elapsed = time.time() - start_time
    token_usage = result.get("token_usage", {})

    return {
        "ticker": ticker,
        "quarter": quarter,
        "summary": result.get("summary", ""),
        "tokens": token_usage.get("total_tokens", 0),
        "cost": token_usage.get("cost_usd", 0),
        "time": elapsed,
    }


def run_single_sample_without_transcript(row, indexer, main_agent):
    """Run analysis WITHOUT transcript (proposed optimization)."""
    ticker = row["ticker"]
    quarter = row["q"]
    transcript = row["transcript"]

    start_time = time.time()

    # Extract facts (still need transcript for extraction)
    facts = indexer.extract_facts_with_context(transcript, ticker, quarter)

    # Run main agent (WITHOUT transcript)
    result = main_agent.run(
        facts=facts,
        row=row,
        mem_txt=None,
        original_transcript=None,  # WITHOUT transcript
        financial_statements_facts=None,
    )

    elapsed = time.time() - start_time
    token_usage = result.get("token_usage", {})

    return {
        "ticker": ticker,
        "quarter": quarter,
        "summary": result.get("summary", ""),
        "tokens": token_usage.get("total_tokens", 0),
        "cost": token_usage.get("cost_usd", 0),
        "time": elapsed,
    }


def extract_direction(summary: str) -> int:
    """Extract Direction score from summary."""
    import re
    match = re.search(r"Direction\s*[:\s]*(\d+)", summary, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return -1


def main():
    print("=" * 70)
    print("Transcript Removal Test - Comparing WITH vs WITHOUT transcript")
    print("=" * 70)
    print(f"Start time: {datetime.now()}")
    print(f"Samples: {NUM_SAMPLES}")
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

    # Results storage
    results_with = []
    results_without = []

    # Run tests
    for i, row in enumerate(samples):
        ticker = row["ticker"]
        quarter = row["q"]
        actual_return = row.get("actual_return_30d", 0) or 0

        print(f"\n[{i+1}/{NUM_SAMPLES}] Testing {ticker} {quarter}")
        print("-" * 50)

        # Test WITH transcript
        print("  Running WITH transcript...")
        try:
            result_with = run_single_sample_with_transcript(row, indexer, main_agent)
            result_with["actual_return"] = actual_return
            result_with["direction"] = extract_direction(result_with["summary"])
            results_with.append(result_with)
            print(f"    ✓ Tokens: {result_with['tokens']:,}, Cost: ${result_with['cost']:.4f}, Time: {result_with['time']:.1f}s")
            print(f"    Direction: {result_with['direction']}")
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results_with.append({"ticker": ticker, "quarter": quarter, "error": str(e)})

        # Test WITHOUT transcript
        print("  Running WITHOUT transcript...")
        try:
            result_without = run_single_sample_without_transcript(row, indexer, main_agent)
            result_without["actual_return"] = actual_return
            result_without["direction"] = extract_direction(result_without["summary"])
            results_without.append(result_without)
            print(f"    ✓ Tokens: {result_without['tokens']:,}, Cost: ${result_without['cost']:.4f}, Time: {result_without['time']:.1f}s")
            print(f"    Direction: {result_without['direction']}")
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results_without.append({"ticker": ticker, "quarter": quarter, "error": str(e)})

    # Summary
    print("\n" + "=" * 70)
    print("COMPARISON SUMMARY")
    print("=" * 70)

    # Calculate totals
    total_tokens_with = sum(r.get("tokens", 0) for r in results_with if "error" not in r)
    total_tokens_without = sum(r.get("tokens", 0) for r in results_without if "error" not in r)
    total_cost_with = sum(r.get("cost", 0) for r in results_with if "error" not in r)
    total_cost_without = sum(r.get("cost", 0) for r in results_without if "error" not in r)
    total_time_with = sum(r.get("time", 0) for r in results_with if "error" not in r)
    total_time_without = sum(r.get("time", 0) for r in results_without if "error" not in r)

    valid_with = len([r for r in results_with if "error" not in r])
    valid_without = len([r for r in results_without if "error" not in r])

    if valid_with > 0 and valid_without > 0 and total_tokens_with > 0:
        print(f"\n{'Metric':<25} {'WITH Transcript':<20} {'WITHOUT Transcript':<20} {'Savings':<15}")
        print("-" * 80)
        print(f"{'Total Tokens':<25} {total_tokens_with:>15,} {total_tokens_without:>15,} {(1-total_tokens_without/total_tokens_with)*100:>10.1f}%")
        if total_cost_with > 0:
            print(f"{'Total Cost':<25} ${total_cost_with:>14.4f} ${total_cost_without:>14.4f} {(1-total_cost_without/total_cost_with)*100:>10.1f}%")
        if total_time_with > 0:
            print(f"{'Total Time':<25} {total_time_with:>14.1f}s {total_time_without:>14.1f}s {(1-total_time_without/total_time_with)*100:>10.1f}%")
        print(f"{'Avg Tokens/Sample':<25} {total_tokens_with/valid_with:>15,.0f} {total_tokens_without/valid_without:>15,.0f}")
        if total_cost_with > 0:
            print(f"{'Avg Cost/Sample':<25} ${total_cost_with/valid_with:>14.4f} ${total_cost_without/valid_without:>14.4f}")
        print(f"{'Avg Time/Sample':<25} {total_time_with/valid_with:>14.1f}s {total_time_without/valid_without:>14.1f}s")

    # Direction comparison
    print("\n" + "-" * 80)
    print("DIRECTION COMPARISON (Does removing transcript change predictions?)")
    print("-" * 80)
    print(f"{'Ticker':<10} {'Quarter':<10} {'WITH':<10} {'WITHOUT':<10} {'Same?':<10} {'Actual Return':<15}")
    print("-" * 80)

    same_count = 0
    for rw, rwo in zip(results_with, results_without):
        if "error" in rw or "error" in rwo:
            continue
        is_same = rw["direction"] == rwo["direction"]
        if is_same:
            same_count += 1
        actual = rw.get("actual_return", 0) or 0
        try:
            actual_str = f"{float(actual):>+.2f}%"
        except:
            actual_str = str(actual)
        print(f"{rw['ticker']:<10} {rw['quarter']:<10} {rw['direction']:<10} {rwo['direction']:<10} {'✓' if is_same else '✗':<10} {actual_str:<15}")

    if min(valid_with, valid_without) > 0:
        print("-" * 80)
        print(f"Same predictions: {same_count}/{min(valid_with, valid_without)} ({same_count/min(valid_with, valid_without)*100:.0f}%)")

    # Close resources
    indexer.close()

    print("\n" + "=" * 70)
    print("Test completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
