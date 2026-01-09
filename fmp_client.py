import asyncio
import os
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging

import httpx
from dotenv import load_dotenv
from storage import get_fmp_cache, set_fmp_cache

load_dotenv()

# Import return horizon from centralized config
RETURN_HORIZON_DAYS = int(os.getenv("RETURN_HORIZON_DAYS", "30"))

# PostgreSQL Database - primary data source (falls back to API if unavailable)
try:
    import pg_client
    FMP_DB_ENABLED = pg_client.check_connection()
    if FMP_DB_ENABLED:
        logging.getLogger(__name__).info("PostgreSQL DB enabled as primary data source")
except ImportError:
    FMP_DB_ENABLED = False
except Exception:
    FMP_DB_ENABLED = False

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE_URL = os.getenv("FMP_BASE_URL", "https://financialmodelingprep.com/stable").rstrip("/")
FMP_TIMEOUT = float(os.getenv("FMP_TIMEOUT_SECONDS", "8.0"))
_CLIENT: Optional[httpx.Client] = None
_ASYNC_CLIENT: Optional[httpx.AsyncClient] = None
logger = logging.getLogger(__name__)


def _truncate_transcript_text(text: str) -> str:
    """
    Return transcript text without truncation by default.

    If the environment variable MAX_TRANSCRIPT_CHARS is explicitly set to a positive integer,
    truncate the text to at most that many characters.

    This helper is kept for backwards compatibility but now behaves as a no-op unless an
    explicit limit is configured.
    """
    if not text:
        return ""

    max_chars_raw = os.getenv("MAX_TRANSCRIPT_CHARS", "")
    try:
        max_chars_str = max_chars_raw.strip()
    except Exception:
        return text

    if not max_chars_str:
        return text

    try:
        max_chars = int(max_chars_str)
    except Exception:
        return text

    if max_chars <= 0:
        return text

    return text[:max_chars]


def _require_api_key() -> str:
    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY is not set. Please configure it in your environment or .env file.")
    return FMP_API_KEY


def _get_client() -> httpx.Client:
    """
    Lazy singleton httpx client so connections can be pooled across requests.
    """
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.Client(base_url=FMP_BASE_URL, timeout=FMP_TIMEOUT)
    return _CLIENT


def _get_async_client() -> httpx.AsyncClient:
    """
    Lazy singleton async client for connection pooling in async workflows.
    """
    global _ASYNC_CLIENT
    if _ASYNC_CLIENT is None:
        _ASYNC_CLIENT = httpx.AsyncClient(base_url=FMP_BASE_URL, timeout=FMP_TIMEOUT)
    return _ASYNC_CLIENT


def close_fmp_client() -> None:
    global _CLIENT
    if _CLIENT is not None:
        try:
            _CLIENT.close()
        finally:
            _CLIENT = None


async def close_fmp_async_client() -> None:
    global _ASYNC_CLIENT
    if _ASYNC_CLIENT is not None:
        try:
            await _ASYNC_CLIENT.aclose()
        finally:
            _ASYNC_CLIENT = None


def _get(client: httpx.Client, path: str, params: dict) -> httpx.Response:
    """
    Ensure we keep the /stable prefix; paths starting with "/" would drop it.
    """
    clean_path = path.lstrip("/")
    retry_status = {429, 500, 502, 503}
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = client.get(clean_path, params=params)
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if exc.response.status_code in retry_status and attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            raise
        except httpx.RequestError as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            raise
    if last_exc:
        logger.error("HTTP request failed after retries: %s %s", path, last_exc)
        raise last_exc
    raise RuntimeError("Unexpected HTTP error without exception")


async def _aget(client: httpx.AsyncClient, path: str, params: dict) -> httpx.Response:
    """
    Async GET with retries/backoff.
    """
    clean_path = path.lstrip("/")
    retry_status = {429, 500, 502, 503}
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = await client.get(clean_path, params=params)
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if exc.response.status_code in retry_status and attempt < 2:
                await asyncio.sleep(0.5 * (2**attempt))
                continue
            raise
        except httpx.RequestError as exc:
            last_exc = exc
            if attempt < 2:
                await asyncio.sleep(0.5 * (2**attempt))
                continue
            raise
    if last_exc:
        logger.error("HTTP request failed after retries: %s %s", path, last_exc)
        raise last_exc
    raise RuntimeError("Unexpected HTTP error without exception")


async def _get_company_profile_async(symbol: str) -> Dict:
    """
    Async version of company profile fetch.
    """
    if not symbol:
        return {}

    # Try AWS FMP DB first
    if FMP_DB_ENABLED:
        try:
            aws_profile = await asyncio.to_thread(pg_client.get_company_profile, symbol)
            if aws_profile and aws_profile.get("company") and aws_profile.get("sector"):
                return aws_profile
        except Exception as e:
            logger.debug("AWS FMP DB async profile lookup failed: %s", e)

    _require_api_key()
    cache_ttl = int(os.getenv("PROFILE_CACHE_MIN", "1440"))
    cache_key = f"fmp:profile:{symbol.upper()}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached:
        return cached

    client = _get_async_client()
    resp = await _aget(client, "profile", params={"symbol": symbol, "apikey": FMP_API_KEY})
    data = resp.json() or []
    if not data:
        raise ValueError(f"Company profile not found for {symbol}")

    first = data[0]
    out = {
        "company": first.get("companyName") or first.get("name"),
        "exchange": first.get("exchangeShortName") or first.get("exchange"),
        "sector": first.get("sector"),
        "industry": first.get("industry"),
        "country": first.get("country"),
    }
    set_fmp_cache(cache_key, out)
    return out


def get_company_profile(symbol: str) -> Dict:
    """
    Fetch basic company profile (name, exchange, sector) for enrichment.
    Checks AWS FMP DB first, then falls back to FMP API.
    """
    if not symbol:
        return {}

    # Try AWS FMP DB first
    if FMP_DB_ENABLED:
        try:
            aws_profile = pg_client.get_company_profile(symbol)
            if aws_profile and aws_profile.get("company") and aws_profile.get("sector"):
                return aws_profile
        except Exception as e:
            logger.debug("AWS FMP DB profile lookup failed: %s", e)

    _require_api_key()
    cache_ttl = int(os.getenv("PROFILE_CACHE_MIN", "1440"))
    cache_key = f"fmp:profile:{symbol.upper()}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached:
        # If cached record is missing key fields, refetch to enrich.
        if cached.get("company") and cached.get("sector") and cached.get("exchange") and cached.get("country") is not None:
            return cached

    client = _get_client()
    # FMP stable profile expects symbol as query param, not path segment
    resp = _get(client, "profile", params={"symbol": symbol, "apikey": FMP_API_KEY})
    resp.raise_for_status()
    data = resp.json() or []
    if not data:
        raise ValueError(f"Company profile not found for {symbol}")

    first = data[0]
    out = {
        "company": first.get("companyName") or first.get("name"),
        "exchange": first.get("exchangeShortName") or first.get("exchange"),
        "sector": first.get("sector"),
        "industry": first.get("industry"),
        "country": first.get("country"),
    }
    raw_cap = first.get("mktCap") or first.get("marketCap")
    try:
        out["market_cap"] = float(raw_cap) if raw_cap is not None else None
    except Exception:
        out["market_cap"] = None
    set_fmp_cache(cache_key, out)
    return out


def get_peers_by_sector(sector: str, exclude_symbol: str = None, limit: int = 10) -> List[str]:
    """
    Get peer company symbols in the same sector from AWS FMP DB.

    Args:
        sector: GICS sector name (e.g., 'Technology')
        exclude_symbol: Symbol to exclude from results
        limit: Maximum number of peers to return

    Returns: List of ticker symbols.
    """
    if not sector:
        return []

    if FMP_DB_ENABLED:
        try:
            return pg_client.get_peers_by_sector(sector, exclude_symbol, limit)
        except Exception as e:
            logger.debug("AWS FMP DB peers lookup failed: %s", e)
    return []


def get_market_cap(symbol: str) -> Optional[float]:
    """
    Fetch latest market capitalization for the given symbol.
    """
    if not symbol:
        return None

    _require_api_key()
    cache_ttl = int(os.getenv("MARKET_CAP_CACHE_MIN", "1440"))
    profile_cache = get_fmp_cache(f"fmp:profile:{symbol.upper()}", max_age_minutes=cache_ttl)
    if profile_cache:
        try:
            cached_cap = profile_cache.get("market_cap")
            if cached_cap is not None:
                return float(cached_cap)
        except Exception:
            pass
    cache_key = f"fmp:market-cap:{symbol.upper()}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached is not None:
        try:
            cached_val = cached.get("market_cap")
            if cached_val is not None:
                return float(cached_val)
        except Exception:
            pass

    client = _get_client()
    resp = _get(client, "market-capitalization", params={"symbol": symbol, "apikey": FMP_API_KEY})
    data = resp.json() or []
    if not isinstance(data, list):
        raise ValueError("Unexpected market cap response format")
    if not data:
        return None
    latest = sorted(data, key=lambda x: x.get("date", ""), reverse=True)[0]
    raw_cap = latest.get("marketCap")
    if raw_cap is None:
        return None
    try:
        market_cap = float(raw_cap)
    except Exception as exc:
        raise ValueError(f"Invalid market cap value for {symbol}") from exc

    set_fmp_cache(cache_key, {"market_cap": market_cap})
    return market_cap


def get_earnings_calendar_for_date(
    target_date: Optional[str] = None, min_market_cap: float = 1_000_000_000, skip_cache: bool = False
) -> List[Dict]:
    """
    Fetch earnings calendar for a specific UTC date and filter by market cap.
    Uses FMP API as the primary source.
    """
    _require_api_key()

    if target_date is None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    else:
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("target_date must be in YYYY-MM-DD format") from exc
        date_str = target_date

    cache_ttl = int(os.getenv("EARNINGS_CALENDAR_CACHE_MIN", "30"))
    cache_key = f"fmp:earnings-calendar:{date_str}:{int(min_market_cap)}:US"
    cached = None if skip_cache else get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached is not None and "data" in cached:
        cached_data = cached.get("data") or []
        if cached_data and all(item.get("company") and item.get("sector") and item.get("exchange") for item in cached_data):
            return cached_data

    client = _get_client()
    resp = _get(client, "earnings-calendar", params={"from": date_str, "to": date_str, "apikey": FMP_API_KEY})
    data = resp.json() or []
    if not isinstance(data, list):
        raise ValueError("Unexpected earnings calendar response format")

    def _safe_float(val: object) -> Optional[float]:
        try:
            return float(val)
        except Exception:
            return None

    results: List[Dict] = []
    allowed_exchange_prefixes = ("NASDAQ", "NYSE", "AMEX", "BATS", "ARCA")
    allowed_countries = {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}
    for item in data:
        symbol = item.get("symbol")
        if not symbol:
            continue
        profile = get_company_profile(symbol)
        country = (profile.get("country") or "").upper()
        exchange = (profile.get("exchange") or "").upper()
        if exchange:
            if not any(exchange.startswith(pref) for pref in allowed_exchange_prefixes):
                continue
        else:
            if not country or country not in allowed_countries:
                continue

        market_cap = profile.get("market_cap")
        if market_cap is None:
            market_cap = get_market_cap(symbol)
        if market_cap is None or market_cap < min_market_cap:
            continue

        eps_est = _safe_float(item.get("epsEstimated"))
        eps_act = _safe_float(item.get("epsActual"))
        company = item.get("company") or item.get("companyName") or profile.get("company") or ""
        sector = item.get("sector") or profile.get("sector") or ""
        results.append(
            {
                "symbol": symbol,
                "company": company,
                "sector": sector,
                "exchange": profile.get("exchange"),
                "date": item.get("date"),
                "eps_estimated": eps_est,
                "eps_actual": eps_act,
                "market_cap": market_cap,
                "raw": item,
            }
        )

    set_fmp_cache(cache_key, {"data": results})
    return results


def get_earnings_calendar_for_range(
    start_date: str,
    end_date: str,
    min_market_cap: float = 1_000_000_000,
    skip_cache: bool = False,
) -> List[Dict]:
    """
    Fetch earnings calendars between start_date and end_date (inclusive) and deduplicate by symbol/date.
    Uses FMP API as the primary source.
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("start_date must be in YYYY-MM-DD format") from exc
    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("end_date must be in YYYY-MM-DD format") from exc

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    # Collect results across each day (since per-day caching already enriches with market cap)
    all_results: List[Dict] = []
    seen_keys: set[tuple[str, str]] = set()
    current = start_dt
    while current <= end_dt:
        day_results = get_earnings_calendar_for_date(current.isoformat(), min_market_cap, skip_cache)
        for item in day_results:
            key = (item.get("symbol"), item.get("date"))
            if key not in seen_keys:
                seen_keys.add(key)
                all_results.append(item)
        current += timedelta(days=1)

    return all_results


def _historical_prices(symbol: str, start: datetime, end: datetime) -> List[dict]:
    """
    Fetch daily historical prices between start and end (inclusive).
    Checks AWS FMP DB first, then falls back to FMP API.
    """
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    # Try AWS FMP DB first
    if FMP_DB_ENABLED:
        try:
            aws_prices = pg_client.get_historical_prices(symbol, start_str, end_str)
            if aws_prices:
                logger.debug("Historical prices for %s found in AWS FMP DB (%d days)", symbol, len(aws_prices))
                return aws_prices
        except Exception as e:
            logger.debug("AWS FMP DB historical prices lookup failed: %s", e)

    _require_api_key()
    use_server_window = True
    params = {
        "symbol": symbol,
        "from": start_str,
        "to": end_str,
        "apikey": FMP_API_KEY,
    }
    client = _get_client()
    try:
        resp = _get(
            client,
            "historical-price-eod/full",
            params=params,
        )
        resp.raise_for_status()
        data = resp.json() or {}
    except httpx.HTTPStatusError:
        # If data not yet available or 404, return empty
        data = {}

    # fallback: if empty, try without date filters to get recent window then filter locally
    if (isinstance(data, dict) and not data.get("historical")) or (isinstance(data, list) and not data):
        try:
            use_server_window = False
            resp = _get(client, "historical-price-eod/full", params={"symbol": symbol, "apikey": FMP_API_KEY})
            resp.raise_for_status()
            data = resp.json() or {}
        except Exception:
            data = {}

    if isinstance(data, dict):
        hist = data.get("historical") or []
    elif isinstance(data, list):
        hist = data
    else:
        hist = []

    # If fetched without date filters, slice to requested window
    if hist and not use_server_window:
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        hist = [h for h in hist if start_str <= h.get("date", "") <= end_str]

    # FMP returns descending by date; ensure sorted ascending
    hist_sorted = sorted(hist, key=lambda x: x.get("date", ""))
    return hist_sorted


def compute_post_return(symbol: str, call_date: str, days: int = 3) -> Dict[str, Optional[float]]:
    """
    Compute post-earnings return using daily close prices:
      - start price = first trading day after call_date
      - end price   = trading day N after start
      - return = (end - start) / start

    Returns dict with start_date, end_date, start_price, end_price, return.
    """
    try:
        call_dt = datetime.fromisoformat(call_date)
    except Exception:
        try:
            call_dt = datetime.strptime(call_date, "%Y-%m-%d")
        except Exception:
            return {"return": None}

    # if call date is in the future, skip price calc
    if call_dt > datetime.utcnow():
        return {"return": None}

    start = call_dt + timedelta(days=0)
    # broaden buffer to catch weekends/holidays; for T+30 we need ~45 calendar days
    end = call_dt + timedelta(days=days + 20)
    prices = _historical_prices(symbol, start, end)
    if not prices:
        return {"return": None}

    # find first trading day after call_date
    start_row = next((p for p in prices if p.get("date") > call_dt.strftime("%Y-%m-%d")), None)
    if not start_row:
        return {"return": None}
    start_idx = prices.index(start_row)
    end_idx = start_idx + days
    if end_idx >= len(prices):
        # not enough trading days after the call
        return {"return": None}
    end_row = prices[end_idx]
    try:
        start_price = float(start_row.get("close"))
        end_price = float(end_row.get("close"))
    except Exception:
        return {"return": None}
    ret = (end_price - start_price) / start_price if start_price else None
    return {
        "start_date": start_row.get("date"),
        "end_date": end_row.get("date"),
        "start_price": start_price,
        "end_price": end_price,
        "return": ret,
    }


def search_symbols(query: str) -> List[Dict]:
    """
    Call FMP Stock Symbol Search API:
    GET /stable/search-symbol?query={query}&apikey=FMP_API_KEY
    """
    if not query or not query.strip():
        return []

    _require_api_key()
    cleaned = query.strip()
    client = _get_client()
    resp = _get(client, "search-symbol", params={"query": cleaned, "apikey": FMP_API_KEY})
    resp.raise_for_status()
    data = resp.json() or []

    results: List[Dict] = []
    for item in data:
        results.append(
            {
                "symbol": item.get("symbol") or item.get("ticker"),
                "name": item.get("name"),
                "exchange": item.get("exchangeShortName") or item.get("exchange"),
                "currency": item.get("currency") or item.get("currencyCode"),
            }
        )
    return [r for r in results if r.get("symbol")]


def get_transcript_dates(symbol: str) -> List[Dict]:
    """
    Call FMP Transcripts Dates By Symbol API:
    GET /stable/earning-call-transcript-dates?symbol={symbol}&apikey=FMP_API_KEY
    """
    def _calendar_from_date(date_str: str) -> Dict[str, Optional[int]]:
        try:
            dt = datetime.fromisoformat(date_str[:10])
            quarter = (dt.month - 1) // 3 + 1
            return {"calendar_year": dt.year, "calendar_quarter": quarter}
        except Exception:
            return {"calendar_year": None, "calendar_quarter": None}

    if not symbol:
        return []

    _require_api_key()
    client = _get_client()
    resp = _get(client, "earning-call-transcript-dates", params={"symbol": symbol, "apikey": FMP_API_KEY})
    resp.raise_for_status()
    data = resp.json() or []

    normalized: List[Dict] = []
    for item in data:
        year = item.get("fiscalYear") or item.get("year")
        quarter = item.get("fiscalQuarter") or item.get("quarter")
        date = item.get("date") or item.get("reportDate")
        cal = _calendar_from_date(date) if date else {"calendar_year": None, "calendar_quarter": None}
        normalized.append(
            {
                "year": year,
                "quarter": quarter,
                "date": date,
                "calendar_year": cal["calendar_year"],
                "calendar_quarter": cal["calendar_quarter"],
            }
        )
    return normalized


def get_fiscal_quarter_by_date(symbol: str, target_date: str, tolerance_days: int = 30) -> Optional[Dict[str, int]]:
    """
    Look up the fiscal year and quarter for a given earnings date.
    Supports fuzzy matching - finds the closest date within tolerance.

    Args:
        symbol: Ticker symbol (e.g., 'LULU')
        target_date: Earnings date in YYYY-MM-DD format (e.g., '2025-09-04')
        tolerance_days: Maximum days difference to consider a match (default: 30)

    Returns:
        Dict with 'year', 'quarter', and 'matched_date' if found, None otherwise.
        Example: {'year': 2025, 'quarter': 2, 'matched_date': '2025-09-04'}
    """
    from datetime import datetime, timedelta

    transcript_dates = get_transcript_dates(symbol)
    if not transcript_dates:
        return None

    target_dt = datetime.strptime(target_date, "%Y-%m-%d")

    # First try exact match
    for item in transcript_dates:
        if item.get("date") == target_date:
            return {"year": item["year"], "quarter": item["quarter"], "matched_date": target_date}

    # If no exact match, find closest date within tolerance
    best_match = None
    min_diff = float('inf')

    for item in transcript_dates:
        item_date = item.get("date")
        if not item_date:
            continue
        try:
            item_dt = datetime.strptime(item_date, "%Y-%m-%d")
            diff = abs((target_dt - item_dt).days)
            if diff < min_diff and diff <= tolerance_days:
                min_diff = diff
                best_match = item
        except ValueError:
            continue

    if best_match:
        return {
            "year": best_match["year"],
            "quarter": best_match["quarter"],
            "matched_date": best_match["date"]
        }

    return None


class NoTranscriptError(ValueError):
    """Raised when no transcript is available after retries."""
    pass


def get_transcript(symbol: str, year: int, quarter: int, max_retries: int = 3) -> Dict:
    """
    Call FMP Earnings Transcript API with retry logic.
    Checks AWS FMP DB first, then falls back to FMP API.

    Retries up to max_retries times if transcript content is empty.
    Raises NoTranscriptError immediately if no transcript found (no 600s wait).
    """
    # Try AWS FMP DB first
    if FMP_DB_ENABLED:
        try:
            aws_transcript = pg_client.get_transcript(symbol, year, quarter)
            if aws_transcript and aws_transcript.get("content") and aws_transcript["content"].strip():
                logger.debug("Transcript for %s FY%s Q%s found in AWS FMP DB", symbol, year, quarter)
                return aws_transcript
        except Exception as e:
            logger.debug("AWS FMP DB transcript lookup failed: %s", e)

    _require_api_key()
    cache_ttl = int(os.getenv("TRANSCRIPT_CACHE_MIN", "10080"))  # default 7 days
    cache_key = f"fmp:transcript:{symbol.upper()}:{year}:{quarter}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached:
        # Validate cached content is not empty
        if cached.get("content") and cached["content"].strip():
            return cached
        # Clear invalid cache entry
        logger.warning("Cached transcript for %s FY%s Q%s is empty, refetching", symbol, year, quarter)

    client = _get_client()
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            resp = _get(
                client,
                "earning-call-transcript",
                params={"symbol": symbol, "year": year, "quarter": quarter, "apikey": FMP_API_KEY},
            )
            resp.raise_for_status()
            data = resp.json() or []

            if not data:
                last_error = NoTranscriptError(f"No transcript found for {symbol} FY{year} Q{quarter}")
                if attempt < max_retries - 1:
                    logger.warning("No transcript data for %s FY%s Q%s, retry %d/%d",
                                   symbol, year, quarter, attempt + 1, max_retries)
                    time.sleep(1.0 * (attempt + 1))  # Exponential backoff
                    continue
                raise last_error

            first = data[0]
            content = first.get("content") or ""

            # Validate content is not empty
            if not content.strip():
                last_error = NoTranscriptError(f"No transcript content for {symbol} FY{year} Q{quarter} (empty response)")
                if attempt < max_retries - 1:
                    logger.warning("Empty transcript content for %s FY%s Q%s, retry %d/%d",
                                   symbol, year, quarter, attempt + 1, max_retries)
                    time.sleep(1.0 * (attempt + 1))
                    continue
                raise last_error

            out = {
                "symbol": symbol,
                "year": year,
                "quarter": quarter,
                "date": first.get("date") or first.get("reportDate"),
                "content": content,
            }
            set_fmp_cache(cache_key, out)
            return out

        except NoTranscriptError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                logger.warning("Error fetching transcript for %s FY%s Q%s: %s, retry %d/%d",
                               symbol, year, quarter, exc, attempt + 1, max_retries)
                time.sleep(1.0 * (attempt + 1))
                continue
            raise

    # Should not reach here, but just in case
    if last_error:
        raise last_error
    raise NoTranscriptError(f"No transcript available for {symbol} FY{year} Q{quarter}")


async def _get_transcript_async(symbol: str, year: int, quarter: int, max_retries: int = 3) -> Dict:
    """
    Async transcript fetch with cache and retry logic.

    Retries up to max_retries times if transcript content is empty.
    Raises NoTranscriptError immediately if no transcript found.
    """
    # Try AWS FMP DB first (sync call in thread to not block event loop)
    if FMP_DB_ENABLED:
        try:
            aws_transcript = await asyncio.to_thread(pg_client.get_transcript, symbol, year, quarter)
            if aws_transcript and aws_transcript.get("content") and aws_transcript["content"].strip():
                logger.debug("Async transcript for %s FY%s Q%s found in AWS FMP DB", symbol, year, quarter)
                return aws_transcript
        except Exception as e:
            logger.debug("AWS FMP DB async transcript lookup failed: %s", e)

    _require_api_key()
    cache_ttl = int(os.getenv("TRANSCRIPT_CACHE_MIN", "10080"))  # default 7 days
    cache_key = f"fmp:transcript:{symbol.upper()}:{year}:{quarter}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached:
        # Validate cached content is not empty
        if cached.get("content") and cached["content"].strip():
            return cached
        logger.warning("Cached transcript for %s FY%s Q%s is empty, refetching", symbol, year, quarter)

    client = _get_async_client()
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            resp = await _aget(
                client,
                "earning-call-transcript",
                params={"symbol": symbol, "year": year, "quarter": quarter, "apikey": FMP_API_KEY},
            )
            data = resp.json() or []

            if not data:
                last_error = NoTranscriptError(f"No transcript found for {symbol} FY{year} Q{quarter}")
                if attempt < max_retries - 1:
                    logger.warning("No transcript data for %s FY%s Q%s, retry %d/%d",
                                   symbol, year, quarter, attempt + 1, max_retries)
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                raise last_error

            first = data[0]
            content = first.get("content") or ""

            if not content.strip():
                last_error = NoTranscriptError(f"No transcript content for {symbol} FY{year} Q{quarter} (empty response)")
                if attempt < max_retries - 1:
                    logger.warning("Empty transcript content for %s FY%s Q%s, retry %d/%d",
                                   symbol, year, quarter, attempt + 1, max_retries)
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                raise last_error

            out = {
                "symbol": symbol,
                "year": year,
                "quarter": quarter,
                "date": first.get("date") or first.get("reportDate"),
                "content": content,
            }
            set_fmp_cache(cache_key, out)
            return out

        except NoTranscriptError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < max_retries - 1:
                logger.warning("Error fetching transcript for %s FY%s Q%s: %s, retry %d/%d",
                               symbol, year, quarter, exc, attempt + 1, max_retries)
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
            raise

    if last_error:
        raise last_error
    raise NoTranscriptError(f"No transcript available for {symbol} FY{year} Q{quarter}")


def get_quarterly_financials(symbol: str, limit: int = 4) -> Dict:
    """
    Fetch recent quarterly financial statements.
    Checks AWS FMP DB first, then falls back to FMP API.
    """
    # Try AWS FMP DB first
    if FMP_DB_ENABLED:
        try:
            aws_fin = pg_client.get_quarterly_financials(symbol, limit)
            if aws_fin and (aws_fin.get("income") or aws_fin.get("balance") or aws_fin.get("cashFlow")):
                logger.debug("Financials for %s found in AWS FMP DB", symbol)
                return aws_fin
        except Exception as e:
            logger.debug("AWS FMP DB financials lookup failed: %s", e)

    _require_api_key()
    cache_ttl = int(os.getenv("FIN_CACHE_MIN", "1440"))
    cache_key = f"fmp:financials:{symbol.upper()}:{limit}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached:
        return cached

    params = {"symbol": symbol, "period": "quarter", "limit": limit, "apikey": FMP_API_KEY}
    client = _get_client()
    income = _get(client, "income-statement", params=params)
    balance = _get(client, "balance-sheet-statement", params=params)
    cash_flow = _get(client, "cash-flow-statement", params=params)

    income.raise_for_status()
    balance.raise_for_status()
    cash_flow.raise_for_status()

    out = {
        "income": income.json() or [],
        "balance": balance.json() or [],
        "cashFlow": cash_flow.json() or [],
    }
    set_fmp_cache(cache_key, out)
    return out


async def _get_quarterly_financials_async(symbol: str, limit: int = 4) -> Dict:
    """
    Async financial statements fetch with cache.
    """
    # Try AWS FMP DB first
    if FMP_DB_ENABLED:
        try:
            aws_fin = await asyncio.to_thread(pg_client.get_quarterly_financials, symbol, limit)
            if aws_fin and (aws_fin.get("income") or aws_fin.get("balance") or aws_fin.get("cashFlow")):
                logger.debug("Async financials for %s found in AWS FMP DB", symbol)
                return aws_fin
        except Exception as e:
            logger.debug("AWS FMP DB async financials lookup failed: %s", e)

    _require_api_key()
    cache_ttl = int(os.getenv("FIN_CACHE_MIN", "1440"))
    cache_key = f"fmp:financials:{symbol.upper()}:{limit}"
    cached = get_fmp_cache(cache_key, max_age_minutes=cache_ttl)
    if cached:
        return cached

    params = {"symbol": symbol, "period": "quarter", "limit": limit, "apikey": FMP_API_KEY}
    client = _get_async_client()
    income, balance, cash_flow = await asyncio.gather(
        _aget(client, "income-statement", params=params),
        _aget(client, "balance-sheet-statement", params=params),
        _aget(client, "cash-flow-statement", params=params),
    )

    out = {
        "income": income.json() or [],
        "balance": balance.json() or [],
        "cashFlow": cash_flow.json() or [],
    }
    set_fmp_cache(cache_key, out)
    return out


def get_earnings_context(symbol: str, year: int, quarter: int) -> Dict:
    """
    Aggregate transcript and financials into a single context used by the analysis engine.
    """
    profile = get_company_profile(symbol)
    transcript = get_transcript(symbol, year, quarter)
    financials = get_quarterly_financials(symbol, limit=4)

    # TODO: Incorporate historical price context via FMP price APIs.
    price_window = []
    post_earnings = compute_post_return(symbol, transcript.get("date") or "", days=RETURN_HORIZON_DAYS)
    post_earnings_return = post_earnings.get("return")

    calendar_year = None
    calendar_quarter = None
    if transcript.get("date"):
        try:
            dt = datetime.fromisoformat(transcript["date"][:10])
            calendar_year = dt.year
            calendar_quarter = (dt.month - 1) // 3 + 1
        except Exception:
            pass

    return {
        "symbol": symbol,
        "year": year,
        "quarter": quarter,
        "company": profile.get("company"),
        "sector": profile.get("sector"),
        "exchange": profile.get("exchange"),
        "transcript_text": _truncate_transcript_text(transcript.get("content", "")),
        "transcript_date": transcript.get("date"),
        "calendar_year": calendar_year,
        "calendar_quarter": calendar_quarter,
        "financials": financials,
        "price_window": price_window,
        "post_earnings_return": post_earnings_return,
        "post_return_meta": post_earnings,
    }


async def get_earnings_context_async(symbol: str, year: int, quarter: int) -> Dict:
    """
    Async version: fetch profile/transcript/financials in parallel, then compute post-return in a thread.
    """
    profile_task = asyncio.create_task(_get_company_profile_async(symbol))
    transcript_task = asyncio.create_task(_get_transcript_async(symbol, year, quarter))
    financials_task = asyncio.create_task(_get_quarterly_financials_async(symbol, limit=4))

    profile, transcript, financials = await asyncio.gather(profile_task, transcript_task, financials_task)

    # post-return uses sync httpx client; move to thread
    post_earnings = await asyncio.to_thread(compute_post_return, symbol, transcript.get("date") or "", RETURN_HORIZON_DAYS)
    post_earnings_return = post_earnings.get("return")

    calendar_year = None
    calendar_quarter = None
    if transcript.get("date"):
        try:
            dt = datetime.fromisoformat(transcript["date"][:10])
            calendar_year = dt.year
            calendar_quarter = (dt.month - 1) // 3 + 1
        except Exception:
            pass

    return {
        "symbol": symbol,
        "year": year,
        "quarter": quarter,
        "company": profile.get("company"),
        "sector": profile.get("sector"),
        "exchange": profile.get("exchange"),
        "transcript_text": _truncate_transcript_text(transcript.get("content", "")),
        "transcript_date": transcript.get("date"),
        "calendar_year": calendar_year,
        "calendar_quarter": calendar_quarter,
        "financials": financials,
        "price_window": [],
        "post_earnings_return": post_earnings_return,
        "post_return_meta": post_earnings,
    }
