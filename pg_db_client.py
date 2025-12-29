"""
Backward compatibility layer for pg_db_client.
All functionality has been moved to pg_client.py.
"""

from pg_client import (
    # Connection management
    is_available,
    check_connection,
    get_cursor,
    close_pool,

    # Company functions
    get_company_profile,
    get_company_info,
    get_peers_by_sector,
    get_companies_by_sector,
    get_all_companies,
    get_all_sectors,
    get_companies_count,

    # Transcript functions
    get_transcript,
    get_transcript_content,
    get_transcript_metadata,
    get_transcript_dates,
    get_all_transcript_dates,

    # Financial statements
    get_income_statements,
    get_balance_sheets,
    get_cash_flow_statements,
    get_quarterly_financials,
    get_historical_financials,
    get_historical_transcripts,

    # Historical prices
    get_historical_prices,
    get_price_on_date,

    # Price analysis
    get_price_analysis,
    get_post_earnings_return,

    # Peer comparison
    get_peer_financials,
    get_peer_transcripts,
    get_peer_facts_summary,

    # Earnings surprise
    get_earnings_surprise,
    get_market_timing,

    # Historical facts for agents
    get_historical_financials_facts,
    get_historical_earnings_facts,

    # Sector context
    get_sector_context,
    get_pre_earnings_momentum,

    # Statistics
    get_stats,

    # Utility functions
    parse_quarter,
)

# For backward compatibility
def close_connection():
    """Deprecated: Use close_pool() instead."""
    close_pool()
