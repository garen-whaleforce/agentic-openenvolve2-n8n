"""
Earnings Backtest Service

計算 earnings call 後 30 個交易日的股價變化 (T+30)：
- from_date: earnings call 後第一個交易日
- to_date: from_date 後第 30 個交易日
- change_pct: 30 天報酬率百分比

同時保留 session (BMO/AMC) 資訊供參考。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List, Literal, Optional, TypedDict

logger = logging.getLogger(__name__)

# Type definitions
EarningsSession = Literal["BMO", "AMC", "UNKNOWN"]


class EarningsBacktest(TypedDict, total=False):
    """Earnings backtest result structure."""
    earnings_date: str          # 'YYYY-MM-DD'
    session: EarningsSession    # BMO, AMC, or UNKNOWN
    from_date: str              # 比較起始日
    to_date: str                # 比較結束日
    from_close: Optional[float] # 起始收盤價
    to_close: Optional[float]   # 結束收盤價
    change_pct: Optional[float] # 漲跌百分比 (e.g., -0.67 means -0.67%)


def get_session_from_pg_db(symbol: str, year: int, quarter: int) -> EarningsSession:
    """
    從 PostgreSQL DB 的 market_timing 欄位取得 BMO/AMC。

    PostgreSQL DB values: 'before_market', 'after_market', 'undetermine'

    Returns: 'BMO', 'AMC', or 'UNKNOWN'
    """
    try:
        from pg_client import get_market_timing
        timing = get_market_timing(symbol, year, quarter)
        if timing:
            timing_lower = timing.lower()
            if "before" in timing_lower:
                return "BMO"
            elif "after" in timing_lower:
                return "AMC"
    except Exception:
        pass
    return "UNKNOWN"


def infer_earnings_session_from_transcript(transcript: str) -> EarningsSession:
    """
    從 transcript 內容推斷 earnings call 是盤前 (BMO) 還是盤後 (AMC)。

    注意：此函數已被 get_session_from_pg_db() 取代，保留作為 fallback。

    規則：
    1. 把 transcript 轉成小寫，掃描全文
    2. 找 "good morning", "good afternoon", "good evening" 第一次出現的位置
    3. 取三者中 index 最小且非 -1 的作為判定依據：
       - good morning → BMO (盤前)
       - good afternoon / good evening → AMC (盤後)
    4. 如果都沒出現 → UNKNOWN
    """
    if not transcript:
        return "UNKNOWN"

    text_lower = transcript.lower()

    # Find first occurrence of each greeting
    idx_morning = text_lower.find("good morning")
    idx_afternoon = text_lower.find("good afternoon")
    idx_evening = text_lower.find("good evening")

    # Collect valid (non -1) indices with their session type
    candidates: List[tuple[int, EarningsSession]] = []
    if idx_morning != -1:
        candidates.append((idx_morning, "BMO"))
    if idx_afternoon != -1:
        candidates.append((idx_afternoon, "AMC"))
    if idx_evening != -1:
        candidates.append((idx_evening, "AMC"))

    if not candidates:
        return "UNKNOWN"

    # Return session type of the earliest occurrence
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _get_trading_days_around(
    symbol: str,
    target_date: str,
    days_before: int = 5,
    days_after: int = 5,
) -> List[dict]:
    """
    取得指定日期前後的交易日價格資料。

    複用 fmp_client._historical_prices()。
    """
    # Lazy import to avoid circular dependency
    from fmp_client import _historical_prices

    try:
        dt = datetime.fromisoformat(target_date)
    except ValueError:
        try:
            dt = datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            return []

    start = dt - timedelta(days=days_before + 5)  # Extra buffer for weekends/holidays
    end = dt + timedelta(days=days_after + 5)

    return _historical_prices(symbol, start, end)


def get_previous_trading_day(symbol: str, date: str) -> Optional[str]:
    """
    取得指定日期的前一個交易日。

    Returns:
        前一交易日的日期字串 (YYYY-MM-DD)，若找不到回傳 None
    """
    prices = _get_trading_days_around(symbol, date, days_before=10, days_after=0)
    if not prices:
        return None

    # Find the last trading day before the target date
    for p in reversed(prices):
        if p.get("date", "") < date:
            return p.get("date")

    return None


def get_next_trading_day(symbol: str, date: str) -> Optional[str]:
    """
    取得指定日期的下一個交易日。

    Returns:
        下一交易日的日期字串 (YYYY-MM-DD)，若找不到回傳 None
    """
    prices = _get_trading_days_around(symbol, date, days_before=0, days_after=10)
    if not prices:
        return None

    # Find the first trading day after the target date
    for p in prices:
        if p.get("date", "") > date:
            return p.get("date")

    return None


def get_close_price(symbol: str, date: str) -> Optional[float]:
    """
    取得指定 symbol 在指定日期的收盤價。

    Returns:
        收盤價，若該日無資料回傳 None
    """
    prices = _get_trading_days_around(symbol, date, days_before=3, days_after=3)
    if not prices:
        return None

    # Find exact date match
    for p in prices:
        if p.get("date") == date:
            try:
                return float(p.get("close"))
            except (TypeError, ValueError):
                return None

    return None


def compute_earnings_backtest(
    symbol: str,
    earnings_date: str,
    transcript: str,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
) -> Optional[EarningsBacktest]:
    """
    計算 earnings call 後 30 個交易日的報酬率 (T+30)。

    Args:
        symbol: 股票代號
        earnings_date: Earnings call 日期 (YYYY-MM-DD)
        transcript: Earnings call 全文
        year: 財報年度 (用於查詢 AWS DB market_timing)
        quarter: 財報季度 (用於查詢 AWS DB market_timing)

    Returns:
        EarningsBacktest 結構，包含：
        - earnings_date: 原始日期
        - session: BMO/AMC/UNKNOWN (保留供參考)
        - from_date: earnings call 後第一個交易日
        - to_date: from_date 後第 30 個交易日
        - from_close/to_close: 兩個日期的收盤價
        - change_pct: 30 天報酬率百分比
    """
    if not symbol or not earnings_date:
        return None

    try:
        # 檢查日期格式
        dt = datetime.fromisoformat(earnings_date[:10])
        earnings_date = dt.strftime("%Y-%m-%d")
    except ValueError:
        logger.warning(f"Invalid earnings_date format: {earnings_date}")
        return None

    # 1. 優先從 PostgreSQL DB 取得 session，若失敗則 fallback 到 transcript 解析
    session: EarningsSession = "UNKNOWN"
    if year and quarter:
        session = get_session_from_pg_db(symbol, year, quarter)

    # Fallback to transcript parsing if PostgreSQL DB returns UNKNOWN
    if session == "UNKNOWN" and transcript:
        session = infer_earnings_session_from_transcript(transcript)

    # 2. 使用 fmp_client.compute_post_return 計算 T+30
    from fmp_client import compute_post_return

    t30_result = compute_post_return(symbol, earnings_date, days=30)

    from_date = t30_result.get("start_date") or ""
    to_date = t30_result.get("end_date") or ""
    from_close = t30_result.get("start_price")
    to_close = t30_result.get("end_price")
    ret = t30_result.get("return")

    # 計算漲跌百分比 (return 是小數，需要轉成百分比)
    change_pct: Optional[float] = None
    if ret is not None:
        change_pct = round(ret * 100, 2)

    return EarningsBacktest(
        earnings_date=earnings_date,
        session=session,
        from_date=from_date,
        to_date=to_date,
        from_close=round(from_close, 2) if from_close is not None else None,
        to_close=round(to_close, 2) if to_close is not None else None,
        change_pct=change_pct,
    )
