#!/usr/bin/env python3
"""
Offline Grid Search for Long-only Strategy Thresholds.

Uses Tune set (2019-2023) CSV to find optimal thresholds.
Target: maximize Sharpe-like metric (win_rate * sqrt(n_trades) / return_volatility)

Usage:
    python grid_search_thresholds.py --input long_only_test_2019_2023_*.csv
"""

import argparse
import itertools
from pathlib import Path
from typing import Dict, List, Tuple, Any

import pandas as pd
import numpy as np


def load_tune_data(csv_path: str) -> pd.DataFrame:
    """Load and preprocess tune set data."""
    df = pd.read_csv(csv_path)

    # Convert boolean-like strings
    bool_cols = ['le_GuidanceRaised', 'le_DemandAcceleration', 'le_MarginExpansion',
                 'le_FCFImprovement', 'le_VisibilityImproving', 'le_GuidanceCut',
                 'le_DemandSoftness', 'le_MarginWeakness', 'le_CashBurn',
                 'le_VisibilityWorsening']

    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map({'YES': True, 'NO': False, True: True, False: False})

    # Convert le_LongEligible
    if 'le_LongEligible' in df.columns:
        df['le_LongEligible'] = df['le_LongEligible'].map({'YES': True, 'NO': False, True: True, False: False})

    # Fill NaN for numeric columns
    numeric_cols = ['le_DirectionScore', 'le_HardPositivesCount', 'le_HardVetoCount',
                    'eps_surprise', 'earnings_day_return', 'actual_return_30d_pct']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate positives count from individual flags
    pos_flags = ['le_GuidanceRaised', 'le_DemandAcceleration', 'le_MarginExpansion',
                 'le_FCFImprovement', 'le_VisibilityImproving']
    df['calc_positives'] = df[pos_flags].sum(axis=1)

    # Calculate vetoes count from individual flags
    veto_flags = ['le_GuidanceCut', 'le_DemandSoftness', 'le_MarginWeakness',
                  'le_CashBurn', 'le_VisibilityWorsening']
    df['calc_vetoes'] = df[veto_flags].sum(axis=1)

    # Risk level
    df['is_high_risk'] = df['le_PricedInRisk'].str.lower() == 'high'
    df['is_low_risk'] = df['le_PricedInRisk'].str.lower() == 'low'

    print(f"Loaded {len(df)} samples from {csv_path}")
    print(f"Direction score distribution:")
    print(df['le_DirectionScore'].value_counts().sort_index())

    return df


def simulate_strategy(
    df: pd.DataFrame,
    # D7+ CORE tier params
    d7_enabled: bool = True,
    d7_min_direction: int = 7,
    d7_max_vetoes: int = 0,
    d7_require_not_high_risk: bool = True,
    d7_min_eps_surprise: float = 0.0,  # Must be > 0 (positive)
    d7_min_day_return: float = 1.0,
    d7_min_positives: int = 0,
    # D6 STRICT tier params
    d6_enabled: bool = True,
    d6_direction: int = 6,
    d6_max_vetoes: int = 0,
    d6_require_low_risk: bool = True,
    d6_min_eps_surprise: float = 0.02,
    d6_min_day_return: float = 0.5,
    d6_min_positives: int = 2,
    d6_exclude_sectors: List[str] = None,
) -> Dict[str, Any]:
    """
    Simulate the two-tier long-only strategy on the dataset.
    Returns performance metrics.
    """
    if d6_exclude_sectors is None:
        d6_exclude_sectors = []

    trades = []

    for idx, row in df.iterrows():
        direction = row.get('le_DirectionScore', 0) or 0
        vetoes = row.get('calc_vetoes', 0) or 0
        positives = row.get('calc_positives', 0) or 0
        eps_surprise = row.get('eps_surprise', 0) or 0
        day_return = row.get('earnings_day_return', 0) or 0
        sector = row.get('sector', '')
        is_high_risk = row.get('is_high_risk', False)
        is_low_risk = row.get('is_low_risk', False)
        actual_return = row.get('actual_return_30d_pct', 0) or 0

        tier = None

        # Check D7+ CORE tier
        if d7_enabled and direction >= d7_min_direction:
            if vetoes <= d7_max_vetoes:
                if not d7_require_not_high_risk or not is_high_risk:
                    if eps_surprise > d7_min_eps_surprise:  # Must be positive
                        if day_return >= d7_min_day_return:
                            if positives >= d7_min_positives:
                                tier = 'D7_CORE'

        # Check D6 STRICT tier (only if not already matched)
        if tier is None and d6_enabled and direction == d6_direction:
            if vetoes <= d6_max_vetoes:
                if not d6_require_low_risk or is_low_risk:
                    if eps_surprise >= d6_min_eps_surprise:
                        if day_return >= d6_min_day_return:
                            if positives >= d6_min_positives:
                                if sector not in d6_exclude_sectors:
                                    tier = 'D6_STRICT'

        if tier:
            trades.append({
                'symbol': row.get('symbol'),
                'year': row.get('year'),
                'quarter': row.get('quarter'),
                'sector': sector,
                'direction': direction,
                'tier': tier,
                'actual_return': actual_return,
                'win': actual_return > 10.0
            })

    # Calculate metrics
    n_trades = len(trades)
    if n_trades == 0:
        return {
            'n_trades': 0,
            'coverage': 0,
            'win_rate': 0,
            'avg_return': 0,
            'std_return': 0,
            'sharpe_like': 0,
            'd7_trades': 0,
            'd6_trades': 0,
        }

    trades_df = pd.DataFrame(trades)
    wins = trades_df['win'].sum()
    win_rate = wins / n_trades
    avg_return = trades_df['actual_return'].mean()
    std_return = trades_df['actual_return'].std() if n_trades > 1 else 0

    # Sharpe-like metric: win_rate * sqrt(n_trades) * avg_return / std_return
    # We want high win rate, many trades, high returns, low volatility
    if std_return > 0:
        sharpe_like = (win_rate * np.sqrt(n_trades) * avg_return) / std_return
    else:
        sharpe_like = win_rate * np.sqrt(n_trades) * avg_return

    d7_trades = len(trades_df[trades_df['tier'] == 'D7_CORE'])
    d6_trades = len(trades_df[trades_df['tier'] == 'D6_STRICT'])

    return {
        'n_trades': n_trades,
        'coverage': n_trades / len(df) * 100,
        'win_rate': win_rate * 100,
        'wins': wins,
        'losses': n_trades - wins,
        'avg_return': avg_return,
        'std_return': std_return,
        'sharpe_like': sharpe_like,
        'd7_trades': d7_trades,
        'd6_trades': d6_trades,
        'trades_df': trades_df,
    }


def grid_search(df: pd.DataFrame) -> List[Dict]:
    """
    Run grid search over parameter combinations.
    """
    # Parameter grid
    d7_min_directions = [7, 8]
    d7_min_day_returns = [0.0, 0.5, 1.0, 1.5]
    d7_min_positives_list = [0, 1, 2]
    d7_require_not_high_risk_list = [True, False]

    d6_min_eps_surprises = [0.0, 0.01, 0.02, 0.03]
    d6_min_day_returns = [0.0, 0.25, 0.5, 1.0]
    d6_min_positives_list = [1, 2, 3]
    d6_require_low_risk_list = [True, False]
    d6_exclude_tech_list = [True, False]

    results = []
    total_combos = (
        len(d7_min_directions) * len(d7_min_day_returns) * len(d7_min_positives_list) *
        len(d7_require_not_high_risk_list) * len(d6_min_eps_surprises) *
        len(d6_min_day_returns) * len(d6_min_positives_list) *
        len(d6_require_low_risk_list) * len(d6_exclude_tech_list)
    )

    print(f"\nRunning grid search over {total_combos} combinations...")

    combo_idx = 0
    for d7_min_dir in d7_min_directions:
        for d7_day_ret in d7_min_day_returns:
            for d7_pos in d7_min_positives_list:
                for d7_risk in d7_require_not_high_risk_list:
                    for d6_eps in d6_min_eps_surprises:
                        for d6_day_ret in d6_min_day_returns:
                            for d6_pos in d6_min_positives_list:
                                for d6_risk in d6_require_low_risk_list:
                                    for d6_no_tech in d6_exclude_tech_list:
                                        combo_idx += 1

                                        exclude_sectors = ['Technology'] if d6_no_tech else []

                                        metrics = simulate_strategy(
                                            df,
                                            d7_enabled=True,
                                            d7_min_direction=d7_min_dir,
                                            d7_max_vetoes=0,
                                            d7_require_not_high_risk=d7_risk,
                                            d7_min_eps_surprise=0.0,
                                            d7_min_day_return=d7_day_ret,
                                            d7_min_positives=d7_pos,
                                            d6_enabled=True,
                                            d6_direction=6,
                                            d6_max_vetoes=0,
                                            d6_require_low_risk=d6_risk,
                                            d6_min_eps_surprise=d6_eps,
                                            d6_min_day_return=d6_day_ret,
                                            d6_min_positives=d6_pos,
                                            d6_exclude_sectors=exclude_sectors,
                                        )

                                        results.append({
                                            'd7_min_dir': d7_min_dir,
                                            'd7_day_ret': d7_day_ret,
                                            'd7_pos': d7_pos,
                                            'd7_no_high_risk': d7_risk,
                                            'd6_eps': d6_eps,
                                            'd6_day_ret': d6_day_ret,
                                            'd6_pos': d6_pos,
                                            'd6_low_risk': d6_risk,
                                            'd6_no_tech': d6_no_tech,
                                            **{k: v for k, v in metrics.items() if k != 'trades_df'}
                                        })

    print(f"Grid search complete. {len(results)} configurations tested.")
    return results


def analyze_results(results: List[Dict], min_trades: int = 50, min_win_rate: float = 75.0):
    """Analyze and rank grid search results."""

    df = pd.DataFrame(results)

    # Filter by minimum requirements
    filtered = df[(df['n_trades'] >= min_trades) & (df['win_rate'] >= min_win_rate)]

    print(f"\n{'='*80}")
    print(f"GRID SEARCH RESULTS (filtered: trades>={min_trades}, win_rate>={min_win_rate}%)")
    print(f"{'='*80}")
    print(f"Total configurations tested: {len(df)}")
    print(f"Configurations meeting criteria: {len(filtered)}")

    if len(filtered) == 0:
        print("\nNo configurations meet the criteria. Relaxing filters...")
        filtered = df[df['n_trades'] >= 20].nlargest(20, 'win_rate')

    # Sort by composite score: prioritize win_rate, then sharpe_like
    filtered = filtered.copy()
    filtered['score'] = filtered['win_rate'] * 0.7 + filtered['sharpe_like'] * 0.3
    top = filtered.nlargest(20, 'score')

    print(f"\n{'='*80}")
    print("TOP 20 CONFIGURATIONS (by composite score)")
    print(f"{'='*80}")

    for i, row in top.iterrows():
        print(f"\n[Rank {top.index.get_loc(i)+1}] Score: {row['score']:.2f}")
        print(f"  D7: dir>={row['d7_min_dir']}, day>={row['d7_day_ret']}%, pos>={row['d7_pos']}, no_high_risk={row['d7_no_high_risk']}")
        print(f"  D6: eps>={row['d6_eps']*100:.0f}%, day>={row['d6_day_ret']}%, pos>={row['d6_pos']}, low_risk={row['d6_low_risk']}, no_tech={row['d6_no_tech']}")
        print(f"  Results: {row['n_trades']} trades ({row['coverage']:.1f}% coverage)")
        print(f"           Win Rate: {row['win_rate']:.1f}% ({int(row['wins'])}W/{int(row['losses'])}L)")
        print(f"           Avg Return: {row['avg_return']:+.2f}%, Std: {row['std_return']:.2f}%")
        print(f"           D7: {int(row['d7_trades'])}, D6: {int(row['d6_trades'])}")

    # Best by different criteria
    print(f"\n{'='*80}")
    print("BEST BY SPECIFIC CRITERIA")
    print(f"{'='*80}")

    if len(df[df['n_trades'] >= 50]) > 0:
        best_winrate = df[df['n_trades'] >= 50].nlargest(1, 'win_rate').iloc[0]
        print(f"\nBest Win Rate (trades>=50): {best_winrate['win_rate']:.1f}%")
        print(f"  D7: dir>={best_winrate['d7_min_dir']}, day>={best_winrate['d7_day_ret']}%, pos>={best_winrate['d7_pos']}")
        print(f"  D6: eps>={best_winrate['d6_eps']*100:.0f}%, day>={best_winrate['d6_day_ret']}%, pos>={best_winrate['d6_pos']}")
        print(f"  Trades: {best_winrate['n_trades']}, Avg Ret: {best_winrate['avg_return']:+.2f}%")

    if len(df[df['win_rate'] >= 80]) > 0:
        best_trades = df[df['win_rate'] >= 80].nlargest(1, 'n_trades').iloc[0]
        print(f"\nMost Trades (win_rate>=80%): {best_trades['n_trades']} trades")
        print(f"  D7: dir>={best_trades['d7_min_dir']}, day>={best_trades['d7_day_ret']}%, pos>={best_trades['d7_pos']}")
        print(f"  D6: eps>={best_trades['d6_eps']*100:.0f}%, day>={best_trades['d6_day_ret']}%, pos>={best_trades['d6_pos']}")
        print(f"  Win Rate: {best_trades['win_rate']:.1f}%, Avg Ret: {best_trades['avg_return']:+.2f}%")

    best_sharpe = df.nlargest(1, 'sharpe_like').iloc[0]
    print(f"\nBest Sharpe-like: {best_sharpe['sharpe_like']:.2f}")
    print(f"  D7: dir>={best_sharpe['d7_min_dir']}, day>={best_sharpe['d7_day_ret']}%, pos>={best_sharpe['d7_pos']}")
    print(f"  D6: eps>={best_sharpe['d6_eps']*100:.0f}%, day>={best_sharpe['d6_day_ret']}%, pos>={best_sharpe['d6_pos']}")
    print(f"  Trades: {best_sharpe['n_trades']}, Win Rate: {best_sharpe['win_rate']:.1f}%")

    return df, top


def main():
    parser = argparse.ArgumentParser(description="Grid search for long-only thresholds")
    parser.add_argument('--input', type=str, required=True, help='Input CSV from tune set')
    parser.add_argument('--output', type=str, default='grid_search_results.csv', help='Output CSV')
    parser.add_argument('--min-trades', type=int, default=50, help='Minimum trades for ranking')
    parser.add_argument('--min-win-rate', type=float, default=75.0, help='Minimum win rate for ranking')

    args = parser.parse_args()

    # Load data
    df = load_tune_data(args.input)

    # Run grid search
    results = grid_search(df)

    # Analyze results
    results_df, top = analyze_results(results, args.min_trades, args.min_win_rate)

    # Save results
    results_df.to_csv(args.output, index=False)
    print(f"\nFull results saved to: {args.output}")


if __name__ == "__main__":
    main()
