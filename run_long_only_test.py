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

# Import LLM provider utilities
try:
    from EarningsCallAgenticRag.utils.llm import (
        switch_to_azure_fallback,
        get_current_provider,
        build_litellm_client,
    )
    HAS_PROVIDER_UTILS = True
except ImportError:
    HAS_PROVIDER_UTILS = False


def check_and_switch_provider():
    """Check if LiteLLM is available, switch to Azure if not."""
    if not HAS_PROVIDER_UTILS:
        print("⚠️  Provider utils not available, using default provider")
        return

    print("🔍 Checking LiteLLM availability...")
    try:
        client, model = build_litellm_client("gpt-4o-mini")
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "hi"}],
            max_tokens=5,
        )
        print(f"✅ LiteLLM is available (provider: {get_current_provider()})")
    except Exception as e:
        error_str = str(e).lower()
        if "503" in error_str or "502" in error_str or "401" in error_str or "connection" in error_str:
            print(f"⚠️  LiteLLM unavailable: {str(e)[:100]}")
            print("🔄 Switching to Azure OpenAI fallback...")
            switch_to_azure_fallback()
            print(f"✅ Now using: {get_current_provider()}")
        else:
            print(f"⚠️  LiteLLM error (not switching): {str(e)[:100]}")


def get_samples(year_start: int = 2019, year_end: int = 2024, limit_per_category: int = 300) -> List[Dict]:
    """Get balanced samples (GAINER/LOSER) across years with even distribution."""
    with get_cursor() as cur:
        if cur is None:
            raise RuntimeError("Database connection failed")

        samples = []
        years = list(range(year_start, year_end + 1))
        samples_per_year_per_cat = limit_per_category // len(years) + 1

        # Top gainers (largest positive returns) - distributed across years
        for year in years:
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
                WHERE et.year = %s
                    AND tc.content IS NOT NULL
                    AND LENGTH(tc.content) > 1000
                    AND pa.pct_change_t_plus_30 IS NOT NULL
                    AND pa.pct_change_t_plus_30 > 10
                ORDER BY RANDOM()
                LIMIT %s
            """, (year, samples_per_year_per_cat))

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

        # Top losers (largest negative returns) - distributed across years
        for year in years:
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
                WHERE et.year = %s
                    AND tc.content IS NOT NULL
                    AND LENGTH(tc.content) > 1000
                    AND pa.pct_change_t_plus_30 IS NOT NULL
                    AND pa.pct_change_t_plus_30 < -10
                ORDER BY RANDOM()
                LIMIT %s
            """, (year, samples_per_year_per_cat))

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


async def run_single_test(symbol: str, year: int, quarter: int, category: str, company_name: str, sector: str, actual_return_30d: float = None):
    """Run a single test and return results.

    Args:
        actual_return_30d: Pre-fetched T+30 return from PG database (same source as sampling)
    """
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

        # Use pre-fetched actual_return_30d from sample (same source as sampling)
        # This ensures consistent return source for evaluation
        actual_return = actual_return_30d

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

        # Extract token usage from result
        token_usage = result.get("agentic_result", {}).get("raw", {}).get("token_usage", {})
        input_tokens = token_usage.get("input_tokens", 0) if token_usage else 0
        output_tokens = token_usage.get("output_tokens", 0) if token_usage else 0
        cost_usd = token_usage.get("cost_usd", 0.0) if token_usage else 0.0

        # Extract new fields for offline grid search
        risk_code = agentic_result.get("risk_code", "unknown")
        long_block_reason = agentic_result.get("long_block_reason", "")
        trade_long_tier = agentic_result.get("trade_long_tier", "")  # D7_CORE or D6_STRICT
        market_anchors = agentic_result.get("market_anchors", {}) or {}
        computed_positives = agentic_result.get("computed_positives", 0)
        computed_vetoes = agentic_result.get("computed_vetoes", 0)

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
            "trade_long_tier": trade_long_tier,  # D7_CORE or D6_STRICT
            "long_eligible_json": long_eligible_json,
            "summary": summary[:500] if summary else "",
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_usd": cost_usd,
            # New fields for offline grid search
            "risk_code": risk_code,
            "long_block_reason": long_block_reason,
            "eps_surprise": market_anchors.get("eps_surprise"),
            "earnings_day_return": market_anchors.get("earnings_day_return"),
            "pre_earnings_5d_return": market_anchors.get("pre_earnings_5d_return"),
            "computed_positives": computed_positives,
            "computed_vetoes": computed_vetoes,
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
            "actual_return_30d_pct": actual_return_30d,  # Still track the expected return
            "correct": None,
            "trade_long": False,
            "trade_long_tier": "",
            "long_eligible_json": None,
            "summary": None,
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
            # New fields for offline grid search
            "risk_code": "unknown",
            "long_block_reason": "EXCEPTION",
            "eps_surprise": None,
            "earnings_day_return": None,
            "pre_earnings_5d_return": None,
            "computed_positives": 0,
            "computed_vetoes": 0,
        }


class ProgressTracker:
    """Track and report progress with periodic stats."""

    def __init__(self, total: int, report_interval: int = 25):
        self.total = total
        self.report_interval = report_interval
        self.completed = 0
        self.results: List[Dict] = []
        self.start_time = time.time()
        self.lock = asyncio.Lock()

    async def add_result(self, result: Dict) -> int:
        """Add a result and return current count."""
        async with self.lock:
            self.results.append(result)
            self.completed += 1
            current = self.completed

            # Print periodic progress report
            if current % self.report_interval == 0 or current == self.total:
                self._print_progress_report()

            return current

    def _print_progress_report(self):
        """Print current progress statistics."""
        elapsed = time.time() - self.start_time

        # Calculate stats from results so far
        successful = [r for r in self.results if r.get("success")]
        failed = len(self.results) - len(successful)

        # Overall accuracy
        with_prediction = [r for r in successful if r.get("correct") is not None]
        overall_correct = sum(1 for r in with_prediction if r.get("correct"))
        overall_acc = (overall_correct / len(with_prediction) * 100) if with_prediction else 0

        # Long-only stats
        long_trades = [r for r in successful if r.get("trade_long")]
        long_correct = [r for r in long_trades if r.get("correct") is not None]
        long_wins = sum(1 for r in long_correct if r.get("correct"))
        long_win_rate = (long_wins / len(long_correct) * 100) if long_correct else 0
        long_avg_ret = (sum(r.get("actual_return_30d_pct", 0) or 0 for r in long_trades) / len(long_trades)) if long_trades else 0

        # Cost stats
        total_cost = sum(r.get("cost_usd", 0) or 0 for r in self.results)
        avg_time = elapsed / self.completed if self.completed else 0
        eta_seconds = avg_time * (self.total - self.completed)
        eta_min = eta_seconds / 60

        print("\n" + "=" * 70)
        print(f"📊 PROGRESS REPORT [{self.completed}/{self.total}] ({self.completed/self.total*100:.1f}%)")
        print("=" * 70)
        print(f"⏱️  Elapsed: {elapsed/60:.1f} min | ETA: {eta_min:.1f} min | Avg: {avg_time:.1f}s/sample")
        print(f"✅ Success: {len(successful)} | ❌ Failed: {failed}")
        print(f"🎯 Overall Accuracy: {overall_acc:.1f}% ({overall_correct}/{len(with_prediction)})")
        print(f"📈 Long-only: {len(long_trades)} trades | Win Rate: {long_win_rate:.1f}% ({long_wins}/{len(long_correct)}) | Avg Ret: {long_avg_ret:+.1f}%")
        print(f"💰 Total Cost: ${total_cost:.4f} | Avg: ${total_cost/self.completed:.4f}/sample")
        print("=" * 70 + "\n")


async def run_single_test_with_semaphore(
    semaphore: asyncio.Semaphore,
    idx: int,
    total: int,
    sample: Dict,
    tracker: ProgressTracker,
) -> Dict:
    """Run a single test with semaphore for concurrency control."""
    async with semaphore:
        symbol = sample["symbol"]
        year = sample["year"]
        quarter = sample["quarter"]
        category = sample["category"]

        print(f"[START {idx}/{total}] {symbol} {year}Q{quarter} ({category})")

        result = await run_single_test(
            symbol=symbol,
            year=year,
            quarter=quarter,
            category=category,
            company_name=sample.get("company_name", ""),
            sector=sample.get("sector", ""),
            actual_return_30d=sample.get("actual_return_30d"),
        )

        # Add to tracker
        done = await tracker.add_result(result)

        # Print result
        if result["success"]:
            pred = result['prediction']
            direction = result['direction_score']
            trade_long = result['trade_long']
            ret_pct = result['actual_return_30d_pct']
            ret_str = f"{ret_pct:.1f}%" if ret_pct else "N/A"
            correct = result['correct']
            print(f"[DONE {done}/{total}] {symbol}: {pred}(D:{direction}) TL:{trade_long} Ret:{ret_str} Correct:{correct}")
        else:
            err_msg = result['error'][:50] if result.get('error') else 'Unknown'
            print(f"[DONE {done}/{total}] {symbol}: ERROR - {err_msg}")

        return result


async def main():
    """Main test function."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=600, help="Number of samples to test (default: 600)")
    parser.add_argument("--test", action="store_true", help="Run only 10 samples for quick test")
    parser.add_argument("--year-start", type=int, default=2019, help="Start year (default: 2019)")
    parser.add_argument("--year-end", type=int, default=2024, help="End year (default: 2024)")
    parser.add_argument("--parallel", type=int, default=8, help="Number of parallel workers (default: 8)")
    parser.add_argument("--report-interval", type=int, default=25, help="Progress report interval (default: 25)")
    args = parser.parse_args()

    NUM_SAMPLES = 10 if args.test else args.samples
    YEAR_START = args.year_start
    YEAR_END = args.year_end
    PARALLEL = args.parallel
    REPORT_INTERVAL = args.report_interval

    total_start_time = time.time()

    print("=" * 70)
    print(f"Long-only Strategy Validation Test (PARALLEL MODE)")
    print(f"Samples: {NUM_SAMPLES}, Years: {YEAR_START}-{YEAR_END}, Parallel Workers: {PARALLEL}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Check LiteLLM availability and switch to Azure if needed
    check_and_switch_provider()

    # Get samples distributed across years
    all_samples = get_samples(year_start=YEAR_START, year_end=YEAR_END, limit_per_category=NUM_SAMPLES // 2)
    random.shuffle(all_samples)
    samples = all_samples[:NUM_SAMPLES]

    print(f"\nLoaded {len(samples)} samples")
    if samples:
        df_samples = pd.DataFrame(samples)
        print(f"Year distribution: {df_samples['year'].value_counts().sort_index().to_dict()}")
        print(f"Category distribution: {df_samples['category'].value_counts().to_dict()}")
        print(f"Sector distribution (top 5): {df_samples['sector'].value_counts().head().to_dict()}")

    # Run tests in parallel with semaphore
    semaphore = asyncio.Semaphore(PARALLEL)
    tracker = ProgressTracker(total=len(samples), report_interval=REPORT_INTERVAL)

    print(f"\nRunning {len(samples)} tests with {PARALLEL} parallel workers...")
    print(f"Progress reports every {REPORT_INTERVAL} samples\n")

    tasks = [
        run_single_test_with_semaphore(
            semaphore=semaphore,
            idx=i,
            total=len(samples),
            sample=sample,
            tracker=tracker,
        )
        for i, sample in enumerate(samples, 1)
    ]

    results = await asyncio.gather(*tasks)

    # Final Summary
    print("\n" + "=" * 70)
    print("🏁 FINAL SUMMARY")
    print("=" * 70)

    df = pd.DataFrame(results)
    successful = df[df["success"] == True]
    failed = df[df["success"] == False]

    print(f"\n📊 Execution Stats:")
    print(f"  Total Samples: {len(df)}")
    print(f"  Successful: {len(successful)} ({len(successful)/len(df)*100:.1f}%)")
    print(f"  Failed: {len(failed)} ({len(failed)/len(df)*100:.1f}%)")

    if len(successful) > 0:
        # Overall accuracy
        with_prediction = successful[successful["correct"].notna()]
        if len(with_prediction) > 0:
            overall_accuracy = with_prediction["correct"].mean() * 100
            print(f"\n🎯 Overall Accuracy: {overall_accuracy:.1f}% ({int(with_prediction['correct'].sum())}/{len(with_prediction)})")

        # Long-only accuracy
        trade_long_df = successful[successful["trade_long"] == True]
        if len(trade_long_df) > 0:
            trade_long_correct = trade_long_df[trade_long_df["correct"].notna()]
            if len(trade_long_correct) > 0:
                long_accuracy = trade_long_correct["correct"].mean() * 100
                long_wins = int(trade_long_correct['correct'].sum())
                long_losses = len(trade_long_correct) - long_wins
                print(f"\n📈 Long-only Strategy:")
                print(f"  Coverage: {len(trade_long_df)}/{len(successful)} ({len(trade_long_df)/len(successful)*100:.1f}%)")
                print(f"  Win Rate: {long_accuracy:.1f}% ({long_wins}W / {long_losses}L)")

                # Average return for trade_long
                avg_return = trade_long_df["actual_return_30d_pct"].mean()
                print(f"  Avg Return: {avg_return:+.2f}%")

                # Win/Loss breakdown
                winners = trade_long_df[trade_long_df["correct"] == True]
                losers = trade_long_df[trade_long_df["correct"] == False]
                if len(winners) > 0:
                    print(f"  Winners Avg Return: +{winners['actual_return_30d_pct'].mean():.2f}%")
                if len(losers) > 0:
                    print(f"  Losers Avg Return: {losers['actual_return_30d_pct'].mean():.2f}%")
        else:
            print(f"\n📈 No samples qualified for Long-only (trade_long=True)")

        # Year breakdown
        print(f"\n📅 Performance by Year:")
        for year in sorted(successful["year"].unique()):
            year_df = successful[successful["year"] == year]
            year_correct = year_df[year_df["correct"].notna()]
            year_acc = year_correct["correct"].mean() * 100 if len(year_correct) > 0 else 0
            year_long = year_df[year_df["trade_long"] == True]
            year_long_correct = year_long[year_long["correct"].notna()]
            year_long_win = year_long_correct["correct"].mean() * 100 if len(year_long_correct) > 0 else 0
            print(f"  {year}: {len(year_df)} samples | Acc: {year_acc:.1f}% | Long: {len(year_long)} ({year_long_win:.0f}% win)")

        # Sector breakdown for long trades
        if len(trade_long_df) > 0:
            print(f"\n🏭 Long-only by Sector:")
            for sector in trade_long_df["sector"].value_counts().head(10).index:
                sector_df = trade_long_df[trade_long_df["sector"] == sector]
                sector_correct = sector_df[sector_df["correct"].notna()]
                if len(sector_correct) > 0:
                    sector_win_rate = sector_correct["correct"].mean() * 100
                    sector_avg_ret = sector_df["actual_return_30d_pct"].mean()
                    print(f"  {sector}: {len(sector_df)} trades | {sector_win_rate:.0f}% win | {sector_avg_ret:+.1f}% avg ret")

        # Direction score distribution
        print(f"\n📊 Direction Score Distribution:")
        direction_scores = successful["direction_score"].dropna()
        if len(direction_scores) > 0:
            for score in sorted(direction_scores.unique()):
                count = (direction_scores == score).sum()
                print(f"  Score {int(score)}: {count} samples")

    # Token usage and cost summary
    total_time = time.time() - total_start_time
    total_input_tokens = sum(r.get("input_tokens", 0) for r in results)
    total_output_tokens = sum(r.get("output_tokens", 0) for r in results)
    total_tokens = total_input_tokens + total_output_tokens
    total_cost = sum(r.get("cost_usd", 0.0) for r in results)
    avg_time_per_sample = total_time / len(results) if results else 0

    print("\n" + "-" * 40)
    print("💰 EXECUTION STATS")
    print("-" * 40)
    print(f"Total Time: {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"Avg Time/Sample: {avg_time_per_sample:.1f}s")
    print(f"Total Tokens: {total_tokens:,} (input: {total_input_tokens:,}, output: {total_output_tokens:,})")
    print(f"Total Cost: ${total_cost:.4f}")
    if len(results) > 0:
        print(f"Avg Cost/Sample: ${total_cost/len(results):.4f}")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"/Users/garen.lee/Coding/agentic-openenvolve2/long_only_test_{YEAR_START}_{YEAR_END}_{timestamp}.csv"

    # Flatten long_eligible_json for CSV
    for r in results:
        if r.get("long_eligible_json"):
            for k, v in r["long_eligible_json"].items():
                r[f"le_{k}"] = v
        if "long_eligible_json" in r:
            del r["long_eligible_json"]

    df = pd.DataFrame(results)
    df.to_csv(csv_path, index=False)
    print(f"\n📁 Results saved to: {csv_path}")

    # Print losers if any for analysis
    if len(successful) > 0 and 'trade_long_df' in dir() and len(trade_long_df) > 0:
        losers = trade_long_df[trade_long_df["correct"] == False]
        if len(losers) > 0:
            print(f"\n⚠️  Long-only LOSSES ({len(losers)} trades):")
            for _, row in losers.iterrows():
                print(f"  {row['symbol']} {row['year']}Q{int(row['quarter'])}: {row['actual_return_30d_pct']:.1f}% | {row['sector']} | D:{row['direction_score']}")


if __name__ == "__main__":
    asyncio.run(main())
