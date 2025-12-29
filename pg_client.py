"""
Unified PostgreSQL Client
==========================
Consolidated client for PostgreSQL (pead_reversal database).
Combines functionality from pg_db_client.py and fmp_db.py.

Features:
- Connection pooling for better performance
- Context manager for safe cursor handling
- All data access functions in one place

Tables:
- companies: symbol, name, sector, sub_sector, gics, cik
- earnings_transcripts: metadata (symbol, year, quarter, transcript_date)
- transcript_content: full transcript text
- income_statements, balance_sheets, cash_flow_statements: financial data
- historical_prices: daily OHLCV data
- price_analysis: pre-computed price analysis around earnings
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# =============================================================================
# Connection Pool Management
# =============================================================================

PG_DSN = os.getenv("POSTGRES_DSN")

# Pool configuration - try to import from config, fallback to defaults
try:
    from EarningsCallAgenticRag.utils.config import PG_POOL_MINCONN, PG_POOL_MAXCONN
except ImportError:
    PG_POOL_MINCONN = int(os.getenv("PG_POOL_MINCONN", "1"))
    PG_POOL_MAXCONN = int(os.getenv("PG_POOL_MAXCONN", "10"))

# Thread-safe connection pool
_pool: Optional[pool.ThreadedConnectionPool] = None


def _get_pool() -> pool.ThreadedConnectionPool:
    """Get or create the connection pool."""
    global _pool
    if _pool is None:
        if not PG_DSN:
            raise RuntimeError("POSTGRES_DSN not set in environment")
        try:
            _pool = pool.ThreadedConnectionPool(
                minconn=PG_POOL_MINCONN,
                maxconn=PG_POOL_MAXCONN,
                dsn=PG_DSN,
            )
            logger.info(f"PostgreSQL connection pool initialized (min={PG_POOL_MINCONN}, max={PG_POOL_MAXCONN})")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    return _pool


@contextmanager
def get_cursor():
    """Context manager for database cursor with automatic connection handling."""
    if not PG_DSN:
        logger.debug("POSTGRES_DSN not configured, skipping database operation")
        yield None
        return

    conn = None
    try:
        p = _get_pool()
        conn = p.getconn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=RealDictCursor)
        yield cur
        cur.close()
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error: {e}")
        yield None
    except psycopg2.DatabaseError as e:
        logger.error(f"Database error: {e}")
        yield None
    except Exception as e:
        logger.exception(f"Unexpected database error: {e}")
        yield None
    finally:
        if conn:
            try:
                _get_pool().putconn(conn)
            except Exception as e:
                logger.warning(f"Failed to return connection to pool: {e}")


def check_connection() -> bool:
    """Test if database connection is working."""
    with get_cursor() as cur:
        if cur is None:
            return False
        try:
            cur.execute("SELECT 1")
            return True
        except Exception:
            return False


def is_available() -> bool:
    """Check if PostgreSQL DB connection is available."""
    return check_connection()


def close_pool():
    """Close all connections in the pool."""
    global _pool
    if _pool:
        try:
            _pool.closeall()
            logger.info("PostgreSQL connection pool closed")
        except Exception as e:
            logger.warning(f"Error closing pool: {e}")
        _pool = None


# =============================================================================
# Utility Functions
# =============================================================================

def _serialize_row(row: Dict) -> Dict:
    """Convert database row to JSON-serializable dict."""
    if row is None:
        return {}
    result = {}
    for key, value in row.items():
        if isinstance(value, Decimal):
            result[key] = float(value)
        elif isinstance(value, (date, datetime)):
            result[key] = value.isoformat() if hasattr(value, 'isoformat') else str(value)
        else:
            result[key] = value
    return result


def parse_quarter(quarter: str) -> Optional[tuple[int, int]]:
    """Parse quarter string to (year, quarter_number) tuple.

    Args:
        quarter: Quarter string in format "YYYY-QN" (e.g., "2024-Q1")

    Returns:
        Tuple of (year, quarter_number) or None if parsing fails

    Examples:
        >>> parse_quarter("2024-Q1")
        (2024, 1)
        >>> parse_quarter("2023-Q4")
        (2023, 4)
        >>> parse_quarter("invalid")
        None
    """
    if not quarter or not isinstance(quarter, str):
        return None
    try:
        parts = quarter.split('-Q')
        if len(parts) != 2:
            return None
        return int(parts[0]), int(parts[1])
    except (ValueError, AttributeError):
        return None


# =============================================================================
# Company Functions
# =============================================================================

def get_company_profile(symbol: str) -> Optional[Dict]:
    """Get company profile including GICS classification."""
    if not symbol:
        return None

    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            cur.execute("""
                SELECT symbol, name, sector, sub_sector, gics, cik
                FROM companies
                WHERE UPPER(symbol) = %s
                LIMIT 1
            """, (symbol.upper(),))
            row = cur.fetchone()
            if row:
                return {
                    "symbol": row["symbol"],
                    "company": row["name"],
                    "name": row["name"],
                    "sector": row["sector"],
                    "sub_sector": row.get("sub_sector"),
                    "gics": row.get("gics"),
                    "cik": row.get("cik"),
                }
        except Exception as e:
            logger.debug(f"get_company_profile error: {e}")
    return None


def get_company_info(symbol: str) -> Optional[Dict[str, Any]]:
    """Alias for get_company_profile for backward compatibility."""
    return get_company_profile(symbol)


def get_peers_by_sector(sector: str, exclude_symbol: str = None, limit: int = 10) -> List[str]:
    """Get peer company symbols in the same sector."""
    if not sector:
        return []

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            if exclude_symbol:
                cur.execute("""
                    SELECT symbol FROM companies
                    WHERE sector = %s AND UPPER(symbol) != %s
                    LIMIT %s
                """, (sector, exclude_symbol.upper(), limit))
            else:
                cur.execute("""
                    SELECT symbol FROM companies
                    WHERE sector = %s
                    LIMIT %s
                """, (sector, limit))
            return [row["symbol"] for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_peers_by_sector error: {e}")
    return []


def get_companies_by_sector(sector: str) -> List[str]:
    """Get all company symbols in a given sector."""
    return get_peers_by_sector(sector, limit=1000)


def get_all_companies() -> List[Dict[str, Any]]:
    """Get all companies with their GICS info."""
    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute("""
                SELECT symbol, name, sector, sub_sector, gics
                FROM companies
                ORDER BY symbol
            """)
            return [_serialize_row(dict(row)) for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_all_companies error: {e}")
    return []


def get_all_sectors() -> List[str]:
    """Get list of all unique sectors."""
    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute("""
                SELECT DISTINCT sector FROM companies
                WHERE sector IS NOT NULL
                ORDER BY sector
            """)
            return [row["sector"] for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_all_sectors error: {e}")
    return []


def get_companies_count() -> int:
    """Get total number of companies."""
    with get_cursor() as cur:
        if cur is None:
            return 0
        try:
            cur.execute("SELECT COUNT(*) as cnt FROM companies")
            row = cur.fetchone()
            return row["cnt"] if row else 0
        except Exception:
            return 0


# =============================================================================
# Transcript Functions
# =============================================================================

def get_transcript(symbol: str, year: int, quarter: int) -> Optional[Dict]:
    """Get earnings call transcript content."""
    if not symbol or not year or not quarter:
        return None

    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            cur.execute("""
                SELECT
                    tc.symbol, tc.year, tc.quarter, tc.content,
                    et.transcript_date
                FROM transcript_content tc
                JOIN earnings_transcripts et ON tc.transcript_id = et.id
                WHERE UPPER(tc.symbol) = %s AND tc.year = %s AND tc.quarter = %s
            """, (symbol.upper(), year, quarter))
            row = cur.fetchone()
            if row and row.get("content"):
                return {
                    "symbol": row["symbol"],
                    "year": row["year"],
                    "quarter": row["quarter"],
                    "date": str(row["transcript_date"]) if row.get("transcript_date") else None,
                    "content": row["content"],
                }
        except Exception as e:
            logger.debug(f"get_transcript error: {e}")
    return None


def get_transcript_content(symbol: str, year: int, quarter: int) -> Optional[str]:
    """Get just the transcript content string."""
    result = get_transcript(symbol, year, quarter)
    return result.get("content") if result else None


def get_transcript_metadata(symbol: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
    """Get earnings call metadata."""
    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            cur.execute("""
                SELECT
                    symbol, year, quarter,
                    transcript_date, transcript_date_str,
                    t_day, market_timing, detection_method
                FROM earnings_transcripts
                WHERE UPPER(symbol) = %s AND year = %s AND quarter = %s
                LIMIT 1
            """, (symbol.upper(), year, quarter))
            row = cur.fetchone()
            return _serialize_row(dict(row)) if row else None
        except Exception as e:
            logger.debug(f"get_transcript_metadata error: {e}")
    return None


def get_transcript_dates(symbol: str) -> List[Dict]:
    """Get all available transcript dates for a symbol."""
    if not symbol:
        return []

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute("""
                SELECT year, quarter, transcript_date, market_timing
                FROM earnings_transcripts
                WHERE UPPER(symbol) = %s
                ORDER BY year DESC, quarter DESC
            """, (symbol.upper(),))
            results = []
            for row in cur.fetchall():
                results.append({
                    "year": row["year"],
                    "quarter": row["quarter"],
                    "date": str(row["transcript_date"]) if row.get("transcript_date") else None,
                    "market_timing": row.get("market_timing"),
                })
            return results
        except Exception as e:
            logger.debug(f"get_transcript_dates error: {e}")
    return []


def get_all_transcript_dates(symbol: str) -> List[Dict[str, Any]]:
    """Get all earnings call dates for a symbol."""
    return get_transcript_dates(symbol)


# =============================================================================
# Financial Statements Functions
# =============================================================================

def _get_financial_statements(table_name: str, symbol: str, limit: int = 4) -> List[Dict[str, Any]]:
    """Generic function to get financial statements from any table.

    Args:
        table_name: Name of the table (income_statements, balance_sheets, cash_flow_statements)
        symbol: Stock ticker symbol
        limit: Maximum number of records to return

    Returns:
        List of financial statement records
    """
    if not symbol:
        return []

    # Validate table name to prevent SQL injection
    valid_tables = {"income_statements", "balance_sheets", "cash_flow_statements"}
    if table_name not in valid_tables:
        logger.warning(f"Invalid table name: {table_name}")
        return []

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute(f"""
                SELECT * FROM {table_name}
                WHERE UPPER(symbol) = %s
                ORDER BY date DESC
                LIMIT %s
            """, (symbol.upper(), limit))
            return [_serialize_row(dict(row)) for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"_get_financial_statements({table_name}) error: {e}")
    return []


def get_income_statements(symbol: str, limit: int = 4) -> List[Dict[str, Any]]:
    """Get quarterly income statements."""
    return _get_financial_statements("income_statements", symbol, limit)


def get_balance_sheets(symbol: str, limit: int = 4) -> List[Dict[str, Any]]:
    """Get quarterly balance sheets."""
    return _get_financial_statements("balance_sheets", symbol, limit)


def get_cash_flow_statements(symbol: str, limit: int = 4) -> List[Dict[str, Any]]:
    """Get quarterly cash flow statements."""
    return _get_financial_statements("cash_flow_statements", symbol, limit)


def get_quarterly_financials(symbol: str, limit: int = 4) -> Optional[Dict]:
    """Get all three financial statements."""
    if not symbol:
        return None

    income = get_income_statements(symbol, limit)
    balance = get_balance_sheets(symbol, limit)
    cash_flow = get_cash_flow_statements(symbol, limit)

    if income or balance or cash_flow:
        return {
            "income": income,
            "balance": balance,
            "cashFlow": cash_flow,
        }
    return None


# =============================================================================
# Historical Financials (for Helper Agents)
# =============================================================================

def get_historical_financials(symbol: str, before_date: str, limit: int = 4) -> Dict:
    """Get historical financial statements before a given date."""
    if not symbol:
        return {"income": [], "balance": [], "cashFlow": []}

    with get_cursor() as cur:
        if cur is None:
            return {"income": [], "balance": [], "cashFlow": []}
        try:
            # Income statements
            cur.execute("""
                SELECT date, fiscal_year, period, revenue, gross_profit, operating_income,
                       net_income, ebitda, eps, eps_diluted
                FROM income_statements
                WHERE UPPER(symbol) = %s AND date < %s
                ORDER BY date DESC
                LIMIT %s
            """, (symbol.upper(), before_date, limit))
            income = [_serialize_row(dict(row)) for row in cur.fetchall()]

            # Balance sheets
            cur.execute("""
                SELECT date, fiscal_year, period, total_assets, total_liabilities,
                       total_stockholders_equity, cash_and_cash_equivalents, total_debt
                FROM balance_sheets
                WHERE UPPER(symbol) = %s AND date < %s
                ORDER BY date DESC
                LIMIT %s
            """, (symbol.upper(), before_date, limit))
            balance = [_serialize_row(dict(row)) for row in cur.fetchall()]

            # Cash flow statements
            cur.execute("""
                SELECT date, fiscal_year, period, operating_cash_flow, free_cash_flow,
                       capital_expenditure, net_income
                FROM cash_flow_statements
                WHERE UPPER(symbol) = %s AND date < %s
                ORDER BY date DESC
                LIMIT %s
            """, (symbol.upper(), before_date, limit))
            cash_flow = [_serialize_row(dict(row)) for row in cur.fetchall()]

            return {"income": income, "balance": balance, "cashFlow": cash_flow}
        except Exception as e:
            logger.debug(f"get_historical_financials error: {e}")
    return {"income": [], "balance": [], "cashFlow": []}


def get_historical_transcripts(symbol: str, before_year: int, before_quarter: int, limit: int = 4) -> List[Dict]:
    """Get historical earnings transcripts before a given quarter."""
    if not symbol:
        return []

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute("""
                SELECT tc.year, tc.quarter, et.transcript_date, tc.content
                FROM transcript_content tc
                JOIN earnings_transcripts et ON tc.transcript_id = et.id
                WHERE UPPER(tc.symbol) = %s
                  AND (tc.year < %s OR (tc.year = %s AND tc.quarter < %s))
                ORDER BY tc.year DESC, tc.quarter DESC
                LIMIT %s
            """, (symbol.upper(), before_year, before_year, before_quarter, limit))
            results = []
            for row in cur.fetchall():
                results.append({
                    "year": row["year"],
                    "quarter": row["quarter"],
                    "date": str(row["transcript_date"]) if row.get("transcript_date") else None,
                    "content": row["content"],
                })
            return results
        except Exception as e:
            logger.debug(f"get_historical_transcripts error: {e}")
    return []


# =============================================================================
# Historical Prices Functions
# =============================================================================

def get_historical_prices(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Get historical daily prices."""
    if not symbol:
        return []

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            query = """
                SELECT date, open, high, low, close, adj_close, volume
                FROM historical_prices
                WHERE UPPER(symbol) = %s
            """
            params: List[Any] = [symbol.upper()]

            if start_date:
                query += " AND date >= %s"
                params.append(start_date)
            if end_date:
                query += " AND date <= %s"
                params.append(end_date)

            query += " ORDER BY date DESC"
            cur.execute(query, params)
            return [_serialize_row(dict(row)) for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_historical_prices error: {e}")
    return []


def get_price_on_date(symbol: str, target_date: date) -> Optional[Dict[str, Any]]:
    """Get price data for a specific date."""
    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            cur.execute("""
                SELECT date, open, high, low, close, adj_close, volume
                FROM historical_prices
                WHERE UPPER(symbol) = %s AND date = %s
                LIMIT 1
            """, (symbol.upper(), target_date))
            row = cur.fetchone()
            return _serialize_row(dict(row)) if row else None
        except Exception as e:
            logger.debug(f"get_price_on_date error: {e}")
    return None


# =============================================================================
# Price Analysis Functions
# =============================================================================

def get_price_analysis(symbol: str, year: int, quarter: int) -> Optional[Dict]:
    """Get pre-computed price analysis for an earnings transcript."""
    if not symbol:
        return None

    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            cur.execute("""
                SELECT pa.*
                FROM price_analysis pa
                JOIN earnings_transcripts et ON pa.transcript_id = et.id
                WHERE UPPER(et.symbol) = %s AND et.year = %s AND et.quarter = %s
            """, (symbol.upper(), year, quarter))
            row = cur.fetchone()
            if row:
                return _serialize_row(dict(row))
        except Exception as e:
            logger.debug(f"get_price_analysis error: {e}")
    return None


def get_post_earnings_return(symbol: str, year: int, quarter: int, days: int = 1) -> Optional[float]:
    """Get post-earnings return for a specific period.

    Args:
        symbol: Stock ticker
        year: Fiscal year
        quarter: Fiscal quarter
        days: Number of days after earnings (1, 20, 30, 40, or 50)

    Returns:
        Percentage return as float (e.g., 5.5 for +5.5%), or None
    """
    analysis = get_price_analysis(symbol, year, quarter)
    if not analysis:
        return None

    if days == 1:
        return analysis.get("pct_change_t")

    field = f"pct_change_t_plus_{days}"
    return analysis.get(field)


# =============================================================================
# Peer Comparison Functions
# =============================================================================

def get_peer_financials(sector: str, exclude_symbol: str, as_of_date: str, limit: int = 5) -> List[Dict]:
    """Get financial data for peer companies in the same sector.

    Optimized to use a single JOIN query instead of N+1 queries.
    """
    if not sector:
        return []

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            # Single query with DISTINCT ON to get latest financials per peer
            cur.execute("""
                SELECT DISTINCT ON (c.symbol)
                    c.symbol, c.name,
                    inc.date, inc.revenue, inc.net_income, inc.eps,
                    inc.gross_profit, inc.operating_income
                FROM companies c
                LEFT JOIN income_statements inc ON c.symbol = inc.symbol
                    AND inc.date <= %s
                WHERE c.sector = %s AND UPPER(c.symbol) != %s
                ORDER BY c.symbol, inc.date DESC
                LIMIT %s
            """, (as_of_date, sector, exclude_symbol.upper(), limit))

            results = []
            for row in cur.fetchall():
                row_dict = _serialize_row(dict(row))
                results.append({
                    "symbol": row_dict["symbol"],
                    "name": row_dict["name"],
                    "financials": {
                        "date": row_dict.get("date"),
                        "revenue": row_dict.get("revenue"),
                        "net_income": row_dict.get("net_income"),
                        "eps": row_dict.get("eps"),
                        "gross_profit": row_dict.get("gross_profit"),
                        "operating_income": row_dict.get("operating_income"),
                    } if row_dict.get("date") else None,
                })
            return results
        except Exception as e:
            logger.debug(f"get_peer_financials error: {e}")
    return []


def get_peer_transcripts(symbol: str, quarter: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Get peer company transcripts from same sector."""
    if not symbol:
        return []

    parsed = parse_quarter(quarter)
    if not parsed:
        return []
    year, q = parsed

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            # Get company sector
            cur.execute("""
                SELECT sector FROM companies WHERE UPPER(symbol) = %s LIMIT 1
            """, (symbol.upper(),))
            row = cur.fetchone()
            if not row or not row.get("sector"):
                return []
            sector = row["sector"]

            # Get peer transcripts
            cur.execute("""
                SELECT c.symbol, c.name, c.sector, tc.content
                FROM companies c
                JOIN earnings_transcripts et ON c.symbol = et.symbol
                JOIN transcript_content tc ON et.symbol = tc.symbol
                    AND et.year = tc.year AND et.quarter = tc.quarter
                WHERE c.sector = %s
                    AND UPPER(c.symbol) != %s
                    AND et.year = %s AND et.quarter = %s
                ORDER BY c.symbol
                LIMIT %s
            """, (sector, symbol.upper(), year, q, limit))
            return [_serialize_row(dict(row)) for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_peer_transcripts error: {e}")
    return []


def get_peer_facts_summary(symbol: str, quarter: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Get summarized peer data combining transcripts and key financials."""
    if not symbol:
        return []

    parsed = parse_quarter(quarter)
    if not parsed:
        return []
    year, q = parsed

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            # Get company sector
            cur.execute("""
                SELECT sector FROM companies WHERE UPPER(symbol) = %s LIMIT 1
            """, (symbol.upper(),))
            row = cur.fetchone()
            if not row or not row.get("sector"):
                return []
            sector = row["sector"]

            # Get peer data with key financial metrics using DISTINCT ON for efficiency
            cur.execute("""
                SELECT DISTINCT ON (c.symbol)
                    c.symbol, c.name, c.sector,
                    inc.revenue, inc.net_income, inc.eps,
                    inc.revenue_growth, inc.operating_income,
                    pa.pct_change_t as earnings_day_return,
                    pa.pct_change_t_plus_20 as return_20d
                FROM companies c
                LEFT JOIN income_statements inc ON c.symbol = inc.symbol
                LEFT JOIN earnings_transcripts et ON c.symbol = et.symbol
                    AND et.year = %s AND et.quarter = %s
                LEFT JOIN price_analysis pa ON et.id = pa.transcript_id
                WHERE c.sector = %s AND UPPER(c.symbol) != %s
                ORDER BY c.symbol, inc.date DESC
                LIMIT %s
            """, (year, q, sector, symbol.upper(), limit))

            return [_serialize_row(dict(row)) for row in cur.fetchall()]
        except Exception as e:
            logger.debug(f"get_peer_facts_summary error: {e}")
    return []


# =============================================================================
# Earnings Surprise Functions
# =============================================================================

def get_earnings_surprise(symbol: str, year: int, quarter: int) -> Optional[Dict]:
    """Get EPS surprise data for a specific earnings call."""
    if not symbol:
        return None

    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            # Get transcript date first
            cur.execute("""
                SELECT transcript_date FROM earnings_transcripts
                WHERE UPPER(symbol) = %s AND year = %s AND quarter = %s
            """, (symbol.upper(), year, quarter))
            row = cur.fetchone()
            if not row:
                return None

            transcript_date = row["transcript_date"]

            # Get earnings surprise closest to transcript date
            cur.execute("""
                SELECT date, eps_actual, eps_estimated, eps_surprise
                FROM earnings_surprises
                WHERE UPPER(symbol) = %s
                  AND date BETWEEN %s::date - interval '7 days' AND %s::date + interval '7 days'
                ORDER BY ABS(date - %s::date)
                LIMIT 1
            """, (symbol.upper(), transcript_date, transcript_date, transcript_date))
            surprise_row = cur.fetchone()
            if surprise_row:
                return {
                    "date": str(surprise_row["date"]),
                    "eps_actual": float(surprise_row["eps_actual"]) if surprise_row.get("eps_actual") else None,
                    "eps_estimated": float(surprise_row["eps_estimated"]) if surprise_row.get("eps_estimated") else None,
                    "eps_surprise": float(surprise_row["eps_surprise"]) if surprise_row.get("eps_surprise") else None,
                }
        except Exception as e:
            logger.debug(f"get_earnings_surprise error: {e}")
    return None


def get_market_timing(symbol: str, year: int, quarter: int) -> Optional[str]:
    """Get BMO/AMC market timing for an earnings call."""
    if not symbol:
        return None

    with get_cursor() as cur:
        if cur is None:
            return None
        try:
            cur.execute("""
                SELECT market_timing FROM earnings_transcripts
                WHERE UPPER(symbol) = %s AND year = %s AND quarter = %s
            """, (symbol.upper(), year, quarter))
            row = cur.fetchone()
            if row and row.get("market_timing"):
                return row["market_timing"]
        except Exception as e:
            logger.debug(f"get_market_timing error: {e}")
    return None


# =============================================================================
# Historical Facts Functions (for Helper Agents)
# =============================================================================

def get_historical_financials_facts(
    symbol: str,
    current_quarter: str,
    num_quarters: int = 8
) -> List[Dict[str, Any]]:
    """Get historical financial data formatted as facts for LLM comparison."""
    parsed = parse_quarter(current_quarter)
    if not parsed:
        return []
    current_year, current_q = parsed

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute("""
                SELECT
                    inc.date, inc.period,
                    inc.revenue, inc.net_income, inc.eps, inc.ebitda,
                    inc.revenue_growth, inc.gross_profit, inc.operating_income
                FROM income_statements inc
                WHERE UPPER(inc.symbol) = %s
                ORDER BY inc.date DESC
                LIMIT %s
            """, (symbol.upper(), num_quarters + 2))

            facts = []
            for row in cur.fetchall():
                row_dict = _serialize_row(dict(row))
                # Infer quarter from date
                if row_dict.get("date"):
                    d = row_dict["date"]
                    try:
                        year = int(str(d)[:4])
                        month = int(str(d)[5:7])
                        q = (month - 1) // 3 + 1
                        quarter = f"{year}-Q{q}"

                        # Skip current quarter
                        if year == current_year and q == current_q:
                            continue

                        # Add facts
                        if row_dict.get("revenue"):
                            facts.append({
                                "metric": "Revenue",
                                "value": f"${row_dict['revenue']:,.0f}",
                                "reason": "Historical quarterly revenue",
                                "quarter": quarter,
                                "ticker": symbol.upper(),
                                "type": "Result",
                            })
                        if row_dict.get("net_income"):
                            facts.append({
                                "metric": "Net Income",
                                "value": f"${row_dict['net_income']:,.0f}",
                                "reason": "Historical quarterly net income",
                                "quarter": quarter,
                                "ticker": symbol.upper(),
                                "type": "Result",
                            })
                        if row_dict.get("eps"):
                            facts.append({
                                "metric": "EPS",
                                "value": f"${row_dict['eps']:.2f}",
                                "reason": "Historical quarterly EPS",
                                "quarter": quarter,
                                "ticker": symbol.upper(),
                                "type": "Result",
                            })
                    except (ValueError, IndexError):
                        continue

            return facts[:num_quarters * 6]  # ~6 facts per quarter
        except Exception as e:
            logger.debug(f"get_historical_financials_facts error: {e}")
    return []


def get_historical_earnings_facts(
    symbol: str,
    current_quarter: str,
    num_quarters: int = 8
) -> List[Dict[str, Any]]:
    """Get historical earnings data formatted as facts for LLM comparison."""
    parsed = parse_quarter(current_quarter)
    if not parsed:
        return []
    current_year, current_q = parsed

    with get_cursor() as cur:
        if cur is None:
            return []
        try:
            cur.execute("""
                SELECT
                    et.year, et.quarter, et.transcript_date_str,
                    et.market_timing,
                    pa.pct_change_t as earnings_day_return,
                    pa.pct_change_t_plus_20 as return_20d,
                    pa.pct_change_t_plus_30 as return_30d,
                    pa.trend_category
                FROM earnings_transcripts et
                LEFT JOIN price_analysis pa ON et.id = pa.transcript_id
                WHERE UPPER(et.symbol) = %s
                    AND NOT (et.year = %s AND et.quarter = %s)
                ORDER BY et.year DESC, et.quarter DESC
                LIMIT %s
            """, (symbol.upper(), current_year, current_q, num_quarters))

            facts = []
            for row in cur.fetchall():
                row_dict = _serialize_row(dict(row))
                quarter = f"{row_dict['year']}-Q{row_dict['quarter']}"

                if row_dict.get("earnings_day_return") is not None:
                    facts.append({
                        "metric": "Earnings Day Return",
                        "value": f"{row_dict['earnings_day_return']:.2f}%",
                        "reason": "Post-earnings price movement on announcement day",
                        "quarter": quarter,
                        "ticker": symbol.upper(),
                        "type": "Result",
                    })
                if row_dict.get("return_30d") is not None:
                    facts.append({
                        "metric": "30-Day Post-Earnings Return",
                        "value": f"{row_dict['return_30d']:.2f}%",
                        "reason": "Price movement 30 days after earnings",
                        "quarter": quarter,
                        "ticker": symbol.upper(),
                        "type": "Result",
                    })
                if row_dict.get("trend_category"):
                    facts.append({
                        "metric": "Post-Earnings Trend",
                        "value": row_dict["trend_category"],
                        "reason": "Classified trend pattern after earnings",
                        "quarter": quarter,
                        "ticker": symbol.upper(),
                        "type": "Result",
                    })

            return facts
        except Exception as e:
            logger.debug(f"get_historical_earnings_facts error: {e}")
    return []


# =============================================================================
# Sector Context (for analysis guidance)
# =============================================================================

def get_sector_context(sector: str) -> Optional[str]:
    """Get sector-specific analysis guidance."""
    sector_guidance = {
        "Technology": (
            "Tech sector key factors: Cloud revenue growth, subscription metrics (ARR/MRR), "
            "R&D spending, AI/ML adoption, customer acquisition costs, net revenue retention, "
            "and guidance on enterprise vs. consumer segments."
        ),
        "Healthcare": (
            "Healthcare sector key factors: Pipeline updates, FDA approvals/setbacks, "
            "clinical trial results, patent expiration impacts, drug pricing pressures, "
            "and M&A activity in the space."
        ),
        "Financial Services": (
            "Financial sector key factors: Net interest margin, loan growth, credit quality, "
            "provisions for loan losses, fee income trends, capital ratios, and regulatory impacts."
        ),
        "Consumer Cyclical": (
            "Consumer sector key factors: Same-store sales, e-commerce penetration, "
            "inventory levels, consumer confidence correlation, seasonal patterns, "
            "and supply chain resilience."
        ),
        "Consumer Defensive": (
            "Consumer staples key factors: Volume vs. price growth, private label competition, "
            "input cost inflation, brand loyalty metrics, and distribution expansion."
        ),
        "Industrials": (
            "Industrial sector key factors: Order backlog, capacity utilization, "
            "infrastructure spending correlation, supply chain constraints, "
            "and international exposure to trade policies."
        ),
        "Energy": (
            "Energy sector key factors: Production volumes, price realizations, "
            "breakeven costs, CAPEX discipline, reserve replacement, and ESG transition plans."
        ),
        "Communication Services": (
            "Telecom/Media key factors: Subscriber growth, ARPU trends, churn rates, "
            "content spending efficiency, advertising revenue, and spectrum investments."
        ),
        "Real Estate": (
            "Real estate key factors: Occupancy rates, rent growth, FFO/AFFO, "
            "cap rates, interest rate sensitivity, and development pipeline."
        ),
        "Basic Materials": (
            "Materials sector key factors: Commodity price exposure, production costs, "
            "inventory cycles, China demand, and environmental compliance costs."
        ),
        "Utilities": (
            "Utilities key factors: Rate case outcomes, regulatory environment, "
            "renewable energy transition, infrastructure investments, and weather impacts."
        ),
    }

    if not sector:
        return None

    # Try exact match first
    if sector in sector_guidance:
        return sector_guidance[sector]

    # Try partial match
    sector_lower = sector.lower()
    for key, value in sector_guidance.items():
        if key.lower() in sector_lower or sector_lower in key.lower():
            return value

    return None


# =============================================================================
# Pre-earnings Momentum
# =============================================================================

def get_pre_earnings_momentum(symbol: str, earnings_date: str, days: int = 5) -> Optional[Dict]:
    """Calculate pre-earnings price momentum."""
    from datetime import timedelta

    if not symbol or not earnings_date:
        return None

    try:
        end_dt = datetime.strptime(earnings_date, "%Y-%m-%d")
    except ValueError:
        return None

    start_dt = end_dt - timedelta(days=days + 10)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    prices = get_historical_prices(symbol, start_str, end_str)
    if not prices or len(prices) < 2:
        return None

    # Prices are in DESC order, reverse for chronological
    prices = list(reversed(prices))

    # Get the last trading day before or on earnings date
    end_row = None
    for p in reversed(prices):
        if p.get("date") <= earnings_date:
            end_row = p
            break

    if not end_row:
        return None

    end_idx = prices.index(end_row)
    start_idx = max(0, end_idx - days)

    if start_idx >= end_idx:
        return None

    start_row = prices[start_idx]

    try:
        start_price = float(start_row.get("close"))
        end_price = float(end_row.get("close"))
    except (TypeError, ValueError):
        return None

    if not start_price or not end_price:
        return None

    return_pct = ((end_price - start_price) / start_price) * 100

    return {
        "start_date": start_row.get("date"),
        "end_date": end_row.get("date"),
        "start_price": round(start_price, 2),
        "end_price": round(end_price, 2),
        "return_pct": round(return_pct, 2),
        "days": end_idx - start_idx,
    }


# =============================================================================
# Statistics
# =============================================================================

def get_stats() -> Dict[str, int]:
    """Get row counts for all tables."""
    with get_cursor() as cur:
        if cur is None:
            return {}
        try:
            stats = {}
            tables = [
                "companies", "earnings_transcripts", "transcript_content",
                "historical_prices", "income_statements", "balance_sheets",
                "cash_flow_statements", "price_analysis"
            ]
            for table in tables:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                row = cur.fetchone()
                stats[table] = row["cnt"] if row else 0
            return stats
        except Exception as e:
            logger.debug(f"get_stats error: {e}")
    return {}
