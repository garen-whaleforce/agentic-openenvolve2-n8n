#!/usr/bin/env python3
"""
Generate a reproducible test manifest for long-only strategy validation.

Features:
- Quarter-stratified sampling (每季固定抽樣)
- Sector floor guarantee (每季每sector至少抽N筆)
- Fixed seed for reproducibility
- Outputs manifest.csv for consistent A/B testing

Usage:
    python generate_manifest.py --per-quarter 100 --min-per-sector 5 --seed 42 \
        --start 2019Q1 --end 2025Q2 --output manifest_2019Q1_2025Q2.csv
"""

import argparse
import random
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from pg_client import get_cursor


def parse_quarter(q_str: str) -> Tuple[int, int]:
    """Parse '2019Q1' -> (2019, 1)"""
    year = int(q_str[:4])
    quarter = int(q_str[5])
    return year, quarter


def quarter_to_str(year: int, quarter: int) -> str:
    """Convert (2019, 1) -> '2019Q1'"""
    return f"{year}Q{quarter}"


def get_all_quarters(start: str, end: str) -> List[Tuple[int, int]]:
    """Generate list of (year, quarter) tuples between start and end inclusive."""
    start_year, start_q = parse_quarter(start)
    end_year, end_q = parse_quarter(end)

    quarters = []
    year, q = start_year, start_q
    while (year, q) <= (end_year, end_q):
        quarters.append((year, q))
        q += 1
        if q > 4:
            q = 1
            year += 1
    return quarters


def fetch_candidates_for_quarter(year: int, quarter: int, min_transcript_length: int = 1000) -> pd.DataFrame:
    """
    Fetch all valid candidates for a given quarter from database.

    Criteria:
    - Transcript exists and length > min_transcript_length
    - T+30 return exists (label available)
    - Sector exists (or marked as Unknown)
    """
    query = """
    SELECT
        c.symbol,
        c.name as company_name,
        c.sector,
        c.sub_sector as industry,
        et.year,
        et.quarter,
        et.id as transcript_id,
        et.transcript_date,
        et.content_length as transcript_length,
        pa.pct_change_t_plus_30 as return_30d
    FROM companies c
    JOIN earnings_transcripts et ON c.symbol = et.symbol
    LEFT JOIN price_analysis pa ON et.id = pa.transcript_id
    WHERE et.year = %s
      AND et.quarter = %s
      AND et.content_length IS NOT NULL
      AND et.content_length > %s
      AND pa.pct_change_t_plus_30 IS NOT NULL
    ORDER BY c.symbol
    """

    with get_cursor() as cur:
        cur.execute(query, (year, quarter, min_transcript_length))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=columns)
    # Fill missing sectors
    df['sector'] = df['sector'].fillna('Unknown')
    return df


def stratified_sample_quarter(
    df: pd.DataFrame,
    per_quarter: int,
    min_per_sector: int,
    rng: random.Random
) -> pd.DataFrame:
    """
    Sample from a quarter's candidates with sector floor guarantee.

    1. First, ensure each sector gets at least min_per_sector samples (or all if fewer available)
    2. Then fill remaining quota proportionally by sector
    """
    if len(df) == 0:
        return df

    if len(df) <= per_quarter:
        return df  # Take all if not enough

    sectors = df['sector'].unique()
    selected_indices = set()

    # Step 1: Sector floor - ensure minimum per sector
    for sector in sectors:
        sector_df = df[df['sector'] == sector]
        n_available = len(sector_df)
        n_to_take = min(min_per_sector, n_available)

        if n_to_take > 0:
            sampled = rng.sample(list(sector_df.index), n_to_take)
            selected_indices.update(sampled)

    # Step 2: Fill remaining quota proportionally
    remaining_quota = per_quarter - len(selected_indices)

    if remaining_quota > 0:
        # Get candidates not yet selected
        remaining_df = df[~df.index.isin(selected_indices)]

        if len(remaining_df) > 0:
            if len(remaining_df) <= remaining_quota:
                selected_indices.update(remaining_df.index)
            else:
                # Proportional sampling by sector
                sector_counts = remaining_df['sector'].value_counts()
                total_remaining = len(remaining_df)

                for sector in sector_counts.index:
                    sector_remaining = remaining_df[remaining_df['sector'] == sector]
                    # Proportional allocation
                    sector_quota = int(remaining_quota * len(sector_remaining) / total_remaining)

                    if sector_quota > 0 and len(sector_remaining) > 0:
                        n_to_take = min(sector_quota, len(sector_remaining))
                        sampled = rng.sample(list(sector_remaining.index), n_to_take)
                        selected_indices.update(sampled)

                # If still under quota, randomly fill
                still_remaining = per_quarter - len(selected_indices)
                if still_remaining > 0:
                    available = remaining_df[~remaining_df.index.isin(selected_indices)]
                    if len(available) > 0:
                        n_to_take = min(still_remaining, len(available))
                        sampled = rng.sample(list(available.index), n_to_take)
                        selected_indices.update(sampled)

    return df.loc[list(selected_indices)]


def generate_manifest(
    start: str,
    end: str,
    per_quarter: int,
    min_per_sector: int,
    seed: int,
    min_transcript_length: int = 1000
) -> pd.DataFrame:
    """Generate the complete manifest."""

    rng = random.Random(seed)
    quarters = get_all_quarters(start, end)

    all_samples = []
    stats = []

    print(f"\n{'='*70}")
    print(f"Generating Manifest: {start} to {end}")
    print(f"Per-quarter target: {per_quarter}, Min per sector: {min_per_sector}, Seed: {seed}")
    print(f"{'='*70}\n")

    for year, quarter in quarters:
        q_str = quarter_to_str(year, quarter)

        # Fetch candidates
        candidates = fetch_candidates_for_quarter(year, quarter, min_transcript_length)
        n_candidates = len(candidates)

        if n_candidates == 0:
            print(f"  {q_str}: No candidates found")
            stats.append({'quarter': q_str, 'candidates': 0, 'sampled': 0})
            continue

        # Sample
        sampled = stratified_sample_quarter(candidates, per_quarter, min_per_sector, rng)
        n_sampled = len(sampled)

        # Add quarter info
        sampled = sampled.copy()
        sampled['quarter_str'] = q_str

        all_samples.append(sampled)

        # Stats
        sectors_in_sample = sampled['sector'].nunique()
        print(f"  {q_str}: {n_candidates} candidates -> {n_sampled} sampled ({sectors_in_sample} sectors)")
        stats.append({'quarter': q_str, 'candidates': n_candidates, 'sampled': n_sampled})

    if not all_samples:
        print("\nNo samples generated!")
        return pd.DataFrame()

    # Combine all
    manifest = pd.concat(all_samples, ignore_index=True)

    # Convert Decimal to float for return_30d
    manifest['return_30d'] = manifest['return_30d'].astype(float)

    # Select and order columns
    manifest = manifest[[
        'symbol', 'year', 'quarter', 'quarter_str', 'transcript_id', 'transcript_date',
        'company_name', 'sector', 'industry', 'transcript_length', 'return_30d'
    ]]

    # Summary stats
    print(f"\n{'='*70}")
    print("MANIFEST SUMMARY")
    print(f"{'='*70}")
    print(f"Total samples: {len(manifest)}")
    print(f"Quarters covered: {manifest['quarter_str'].nunique()}")
    print(f"\nSamples per year:")
    for year in sorted(manifest['year'].unique()):
        n = len(manifest[manifest['year'] == year])
        print(f"  {year}: {n}")

    print(f"\nSamples per sector:")
    for sector in manifest['sector'].value_counts().head(15).index:
        n = len(manifest[manifest['sector'] == sector])
        pct = n / len(manifest) * 100
        print(f"  {sector}: {n} ({pct:.1f}%)")

    print(f"\nReturn distribution:")
    print(f"  Mean: {manifest['return_30d'].mean():.2f}%")
    print(f"  Median: {manifest['return_30d'].median():.2f}%")
    print(f"  Std: {manifest['return_30d'].std():.2f}%")
    print(f"  Gainers (>10%): {len(manifest[manifest['return_30d'] > 10])} ({len(manifest[manifest['return_30d'] > 10])/len(manifest)*100:.1f}%)")
    print(f"  Losers (<-10%): {len(manifest[manifest['return_30d'] < -10])} ({len(manifest[manifest['return_30d'] < -10])/len(manifest)*100:.1f}%)")

    return manifest


def main():
    parser = argparse.ArgumentParser(description="Generate test manifest for long-only strategy")
    parser.add_argument('--start', type=str, default='2019Q1', help='Start quarter (e.g., 2019Q1)')
    parser.add_argument('--end', type=str, default='2025Q2', help='End quarter (e.g., 2025Q2)')
    parser.add_argument('--per-quarter', type=int, default=100, help='Target samples per quarter')
    parser.add_argument('--min-per-sector', type=int, default=5, help='Minimum samples per sector per quarter')
    parser.add_argument('--seed', type=int, default=42, help='Random seed for reproducibility')
    parser.add_argument('--min-transcript-length', type=int, default=1000, help='Minimum transcript length')
    parser.add_argument('--output', type=str, default=None, help='Output CSV path')

    args = parser.parse_args()

    # Generate manifest
    manifest = generate_manifest(
        start=args.start,
        end=args.end,
        per_quarter=args.per_quarter,
        min_per_sector=args.min_per_sector,
        seed=args.seed,
        min_transcript_length=args.min_transcript_length
    )

    if len(manifest) == 0:
        print("No manifest generated!")
        return

    # Output path
    if args.output:
        output_path = args.output
    else:
        output_path = f"manifest_{args.start}_{args.end}_q{args.per_quarter}_s{args.seed}.csv"

    # Save
    manifest.to_csv(output_path, index=False)
    print(f"\n✅ Manifest saved to: {output_path}")
    print(f"   Total samples: {len(manifest)}")


if __name__ == "__main__":
    main()
