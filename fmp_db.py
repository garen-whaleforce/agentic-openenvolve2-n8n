"""
Backward compatibility layer for fmp_db.
All functionality has been moved to pg_client.py.
"""

from pg_client import (
    # Connection management
    is_available as check_connection,
    close_pool as close_connection,
    get_cursor,

    # Company functions
    get_company_profile,
    get_peers_by_sector,
    get_all_sectors,
    get_companies_count,

    # Transcript functions
    get_transcript,
    get_transcript_dates,

    # Financial statements
    get_quarterly_financials,
    get_historical_financials,
    get_historical_transcripts,
    get_historical_prices,

    # Price analysis
    get_price_analysis,

    # Peer comparison
    get_peer_financials,

    # Earnings surprise
    get_earnings_surprise,
    get_market_timing,

    # Sector context
    get_sector_context,
    get_pre_earnings_momentum,
)

# Re-export check_connection for modules that expect it
__all__ = [
    'check_connection',
    'close_connection',
    'get_cursor',
    'get_company_profile',
    'get_peers_by_sector',
    'get_all_sectors',
    'get_companies_count',
    'get_transcript',
    'get_transcript_dates',
    'get_quarterly_financials',
    'get_historical_financials',
    'get_historical_transcripts',
    'get_historical_prices',
    'get_price_analysis',
    'get_peer_financials',
    'get_earnings_surprise',
    'get_market_timing',
    'get_sector_context',
    'get_pre_earnings_momentum',
]
