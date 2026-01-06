# Long-only Strategy v1.1 Backtest Summary (2017-2025)

**Version**: v1.1-live-safe (Corrected reaction_date)
**Last Updated**: 2026-01-01

---

## Data Pipeline

### 1. Data Sources
| Source | Years | Description |
|--------|-------|-------------|
| `long_only_test_2017_2018_*.csv` | 2017-2018 | Historical LLM analysis results |
| `long_only_test_2019_2024_combined.csv` | 2019-2024 | Historical LLM analysis results |
| `long_only_test_2025_*.csv` | 2025 | Latest LLM analysis results |

### 2. Date Correction
- **Issue**: Original reaction_date used fiscal quarter end dates
- **Fix**: Retrieved actual `t_day` from PostgreSQL `earnings_transcripts` table
- **Impact**: 2,449 dates corrected, average 83-day adjustment

### 3. Price Data
- **Source**: Whaleforce Backtest API (`https://backtest.api.whaleforce.dev`)
- **Format**: Per-symbol OHLCV CSV
- **Location**: `backtest_tools/prices/`

---

## Backtest Configuration (v1.1-live-safe)

### Entry/Exit Rules
| Parameter | Value | Description |
|-----------|-------|-------------|
| Decision Time | Earnings reaction day close | Make decision after market closes |
| Entry | T+1 Open | Next trading session's open |
| Exit | T+31 Close | 30 trading sessions after entry |

### Position Sizing
| Parameter | Value |
|-----------|-------|
| CAP per Quarter | 12 (First-N) |
| Max Concurrent Positions | 12 |
| Allocation | 1/12 Equal Weight |

### Friction Costs (Zero-cost backtest shown, see sensitivity analysis)
| Parameter | Value |
|-----------|-------|
| Commission | 0 bps |
| Slippage | 0 bps |

### Trading Calendar
- XNYS (NYSE)

---

## Backtest Results (SSOT - Fixed Dates)

### Performance Metrics
| Metric | Value | 95% CI |
|--------|-------|--------|
| **Total Return** | 184.27% | - |
| **CAGR (ARR)** | 14.16% | [7.15%, 21.30%] |
| **Annual Vol** | 7.06% | - |
| **Sharpe Ratio** | 1.91 | [0.91, 2.92] |
| **Sortino Ratio** | 3.09 | - |
| **Max Drawdown** | -16.95% | [-25.16%, -4.46%] |
| **Calmar Ratio** | 0.84 | - |

### Trade Statistics
| Metric | Value | 95% CI |
|--------|-------|--------|
| Total Trades | 180 | - |
| Win Rate | 86.11% | [81.1%, 91.1%] |
| Profit Factor | 4.06 | [2.65, 7.08] |
| Avg Win | +11.29% | - |
| Avg Loss | -17.24% | - |
| Avg Exposure | 23.03% | - |

### Yearly Breakdown
| Year | Trades | Win Rate | Notes |
|------|--------|----------|-------|
| 2017 | 37 | 86.5% | Initial deployment year |
| 2018 | 30 | 66.7% | Market correction |
| 2019 | 3 | 66.7% | Limited signals |
| 2020 | 12 | 91.7% | COVID recovery rally |
| 2021 | 28 | 85.7% | Bull market |
| 2022 | 25 | 68.0% | Bear market test |
| 2023 | 25 | 80.0% | AI-driven rally |
| 2024 | 15 | 80.0% | Mixed market |
| 2025 | 4 | 75.0% | YTD |

---

## Friction Sensitivity

| Slippage | Commission | Total | CAGR | Sharpe | MaxDD |
|----------|------------|-------|------|--------|-------|
| 0 bps | 0 bps | 0 | 14.18% | 1.91 | -16.95% |
| 10 bps | 2 bps | 12 | 13.64% | 1.85 | -17.00% |
| 25 bps | 2 bps | 27 | 12.98% | 1.77 | -17.32% |

---

## Two-Tier Selection Rules

### Tier 1: D7 CORE (Direction Score >= 7)
- `LONG_D7_MIN_POSITIVES=0`
- `LONG_D7_MIN_DAY_RET=1.5`
- `LONG_D7_REQUIRE_EPS_POS=1`

### Tier 2: D6 STRICT (Direction Score = 6)
- `LONG_D6_MIN_EPS_SURPRISE=0.0`
- `LONG_D6_MIN_POSITIVES=1`
- `LONG_D6_MIN_DAY_RET=1.0`

---

## Output Files

| File | Description |
|------|-------------|
| `signals_fixed_dates.csv` | Official signals with corrected dates (3,388 samples) |
| `long_only_signals_2017_2025_final.csv` | trade_long=True signals (266 entries) |
| `backtest_trades_2017_2025_final.csv` | Executed trades (180 entries) |
| `out_backtest_fixed/nav.csv` | Daily NAV series |
| `out_backtest_fixed/trades.csv` | Trade details |
| `out_backtest_fixed/metrics.json` | Performance metrics JSON |

---

## Deprecated Files

| File | Issue |
|------|-------|
| `out_backtest_complete/` | Uses old reaction_date (fiscal/calendar misalignment) |
| `signals_backtest_ready.csv` | Pre-correction dates |

---

*See SINGLE_SOURCE_OF_TRUTH.md for official reference*
*See v1.1_Final_Validation_Report_CORRECTED.md for detailed validation*

Generated: 2026-01-01
