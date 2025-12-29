import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional
from uuid import uuid4

from agentic_rag_bridge import AgenticRagBridgeError, run_single_call_from_context
from neo4j_ingest import Neo4jIngestError, ingest_context_into_neo4j, ingest_recent_history_into_neo4j
from fmp_client import get_earnings_context, get_earnings_context_async, NoTranscriptError
from storage import (
    record_analysis,
    get_cached_payload,
    set_cached_payload,
)
from redis_cache import cache_get_json, cache_set_json
from earnings_backtest import compute_earnings_backtest

# Whaleforce Services Integration
from services.performance_metrics_client import PerformanceMetricsClient, get_performance_metrics_client
from services.backtester_client import BacktesterClient, get_backtester_client

logger = logging.getLogger(__name__)
PAYLOAD_CACHE_MINUTES = int(os.getenv("PAYLOAD_CACHE_MINUTES", "1440"))  # DB cache validity in minutes (default 1 day)
REDIS_PAYLOAD_TTL_SECONDS = int(os.getenv("REDIS_PAYLOAD_TTL_SECONDS", "3600"))  # Redis TTL in seconds (default 1 hour)

# Feature flags for service integrations
ENABLE_PERFORMANCE_METRICS = os.getenv("ENABLE_PERFORMANCE_METRICS", "true").lower() == "true"
ENABLE_BACKTESTER_VALIDATION = os.getenv("ENABLE_BACKTESTER_VALIDATION", "true").lower() == "true"


def run_agentic_rag(
    context: Dict,
    main_model: Optional[str] = None,
    helper_model: Optional[str] = None,
) -> Dict:
    """
    Call the real Agentic RAG pipeline via the bridge module.
    """
    # Early check: fail fast if transcript is empty (don't wait 600s timeout)
    transcript_text = context.get("transcript_text") or ""
    if not transcript_text.strip():
        symbol = context.get("symbol") or context.get("ticker") or "UNKNOWN"
        year = context.get("year")
        quarter = context.get("quarter")
        raise NoTranscriptError(f"No transcript available for {symbol} FY{year} Q{quarter}")

    def _add_ingest_warning(msg: str) -> None:
        if not msg:
            return
        if context.get("ingest_warning"):
            context["ingest_warning"] = f"{context['ingest_warning']} | {msg}"
        else:
            context["ingest_warning"] = msg

    # Skip Neo4j ingest when using PostgreSQL DB agents (no vector search needed)
    skip_neo4j_ingest = os.getenv("USE_PG_DB_AGENTS", "false").lower() == "true"

    if not skip_neo4j_ingest:
        # First, backfill recent historical quarters so helper agents have past facts.
        history_quarters = 4
        try:
            history_quarters = int(os.getenv("INGEST_HISTORY_QUARTERS", "4"))
        except Exception:
            history_quarters = 4

        try:
            _retry(lambda: ingest_recent_history_into_neo4j(context, max_quarters=history_quarters))
        except Neo4jIngestError as exc:
            logger.warning("Historical ingest failed: %s", exc)
            _add_ingest_warning(f"Historical ingest failed: {exc}")
        except Exception as exc:  # noqa: BLE001
            logger.warning("Historical ingest failed: %s", exc)
            _add_ingest_warning(f"Historical ingest failed: {exc}")

        # On-the-fly Neo4j ingestion so helper agents have facts to use.
        try:
            _retry(lambda: ingest_context_into_neo4j(context))
        except Neo4jIngestError as exc:
            # Keep analyzing even if ingestion failed; surface a hint in metadata.
            context.setdefault("ingest_warning", str(exc))
            logger.warning("Neo4j ingestion failed: %s", exc)
        except Exception as exc:  # noqa: BLE001
            context.setdefault("ingest_warning", f"Neo4j ingestion failed: {exc}")
            logger.warning("Neo4j ingestion failed: %s", exc)

    try:
        result = run_single_call_from_context(
            context,
            main_model=main_model,
            helper_model=helper_model,
        )
    except AgenticRagBridgeError:
        # Propagate bridge-specific errors directly for clearer API feedback
        raise
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Agentic RAG pipeline failure: {exc}") from exc

    if not isinstance(result, dict):
        result = {"raw_output": result}

    metadata = result.setdefault("metadata", {})
    metadata.setdefault("generated_at", datetime.utcnow().isoformat() + "Z")
    metadata.setdefault("engine", "EarningsCallAgenticRag")
    metadata.setdefault("transcript_excerpt", (context.get("transcript_text") or "")[:280])
    if context.get("ingest_warning"):
        metadata.setdefault("ingest_warning", context["ingest_warning"])
    return result


def analyze_earnings(
    symbol: str,
    year: int,
    quarter: int,
    main_model: Optional[str] = None,
    helper_model: Optional[str] = None,
) -> Dict:
    """
    High-level orchestration: build context and run the Agentic RAG bridge.
    """
    job_id = str(uuid4())
    context = get_earnings_context(symbol, year, quarter)
    agentic_result = run_agentic_rag(
        context,
        main_model=main_model,
        helper_model=helper_model,
    )
    if not isinstance(agentic_result, dict):
        agentic_result = {"raw_output": agentic_result}

    # Persist summary for listing/detail pages
    try:
        raw = agentic_result.get("raw") if isinstance(agentic_result, dict) else {}
        token_usage = raw.get("token_usage") if isinstance(raw, dict) else None
        notes = raw.get("notes") if isinstance(raw, dict) else None
        # Determine correctness if post return is available
        post_ret = context.get("post_earnings_return")
        pred = agentic_result.get("prediction") if isinstance(agentic_result, dict) else None
        correct = None
        if post_ret is not None and pred:
            pred_upper = str(pred).upper()
            if pred_upper == "UP":
                correct = post_ret > 0
            elif pred_upper == "DOWN":
                correct = post_ret < 0
            elif pred_upper == "NEUTRAL":
                correct = abs(post_ret) < 0.01

        record_analysis(
            job_id=job_id,
            symbol=symbol,
            fiscal_year=year,
            fiscal_quarter=quarter,
            call_date=context.get("transcript_date"),
            sector=context.get("sector"),
            exchange=context.get("exchange"),
            post_return=post_ret,
            prediction=pred,
            confidence=agentic_result.get("confidence") if isinstance(agentic_result, dict) else None,
            correct=correct,
            agent_result=agentic_result if isinstance(agentic_result, dict) else {},
            token_usage=token_usage,
            agent_notes=str(notes) if notes else None,
            company=context.get("company"),
        )
    except Exception as exc:
        # Do not block API if persistence fails
        logger.exception("record_analysis failed", exc_info=exc)

    # Compute earnings backtest (BMO/AMC price change)
    backtest = None
    try:
        backtest = compute_earnings_backtest(
            symbol,
            context.get("transcript_date") or "",
            context.get("transcript_text") or "",
            year=year,
            quarter=quarter,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("compute_earnings_backtest failed for %s: %s", symbol, exc)

    payload = {
        "symbol": symbol,
        "year": year,
        "quarter": quarter,
        "transcript_date": context.get("transcript_date"),
        "calendar_year": context.get("calendar_year"),
        "calendar_quarter": context.get("calendar_quarter"),
        "post_return_meta": context.get("post_return_meta"),
        "post_earnings_return": context.get("post_earnings_return"),
        "job_id": job_id,
        "agentic_result": agentic_result,
        "context": context,
        "backtest": backtest,
    }

    return payload


async def analyze_earnings_async(
    symbol: str,
    year: int,
    quarter: int,
    main_model: Optional[str] = None,
    helper_model: Optional[str] = None,
    skip_cache: bool = False,
) -> Dict:
    """
    Async wrapper: fetch context in parallel and run agentic pipeline in thread to avoid blocking event loop.
    """
    cache_key = f"call:{symbol.upper()}:{year}:Q{quarter}"

    if not skip_cache:
        # 1) Redis cache
        cached_payload = await cache_get_json(cache_key)
        if isinstance(cached_payload, dict) and cached_payload.get("symbol"):
            return cached_payload

        # 2) DB cache
        try:
            db_cached = get_cached_payload(
                symbol=symbol,
                fiscal_year=year,
                fiscal_quarter=quarter,
                max_age_minutes=PAYLOAD_CACHE_MINUTES,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("get_cached_payload failed, ignoring cache", exc_info=exc)
            db_cached = None

        if isinstance(db_cached, dict) and db_cached.get("symbol"):
            try:
                await cache_set_json(cache_key, db_cached, REDIS_PAYLOAD_TTL_SECONDS)
            except Exception:
                pass
            return db_cached

    job_id = str(uuid4())
    context = await get_earnings_context_async(symbol, year, quarter)
    agentic_result = await asyncio.to_thread(
        run_agentic_rag,
        context,
        main_model,
        helper_model,
    )
    if not isinstance(agentic_result, dict):
        agentic_result = {"raw_output": agentic_result}

    try:
        raw = agentic_result.get("raw") if isinstance(agentic_result, dict) else {}
        token_usage = raw.get("token_usage") if isinstance(raw, dict) else None
        notes = raw.get("notes") if isinstance(raw, dict) else None
        post_ret = context.get("post_earnings_return")
        pred = agentic_result.get("prediction") if isinstance(agentic_result, dict) else None
        correct = None
        if post_ret is not None and pred:
            pred_upper = str(pred).upper()
            if pred_upper == "UP":
                correct = post_ret > 0
            elif pred_upper == "DOWN":
                correct = post_ret < 0
            elif pred_upper == "NEUTRAL":
                correct = abs(post_ret) < 0.01

        await asyncio.to_thread(
            record_analysis,
            job_id,
            symbol,
            year,
            quarter,
            context.get("transcript_date"),
            context.get("sector"),
            context.get("exchange"),
            post_ret,
            pred,
            agentic_result.get("confidence") if isinstance(agentic_result, dict) else None,
            correct,
            agentic_result if isinstance(agentic_result, dict) else {},
            token_usage,
            str(notes) if notes else None,
            context.get("company"),
        )
    except Exception as exc:
        logger.exception("record_analysis failed", exc_info=exc)

    # Compute earnings backtest (BMO/AMC price change)
    backtest = None
    try:
        backtest = await asyncio.to_thread(
            compute_earnings_backtest,
            symbol,
            context.get("transcript_date") or "",
            context.get("transcript_text") or "",
            year,
            quarter,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("compute_earnings_backtest failed for %s: %s", symbol, exc)

    payload = {
        "symbol": symbol,
        "year": year,
        "quarter": quarter,
        "transcript_date": context.get("transcript_date"),
        "calendar_year": context.get("calendar_year"),
        "calendar_quarter": context.get("calendar_quarter"),
        "post_return_meta": context.get("post_return_meta"),
        "post_earnings_return": context.get("post_earnings_return"),
        "job_id": job_id,
        "agentic_result": agentic_result,
        "context": context,
        "backtest": backtest,
    }

    try:
        set_cached_payload(symbol, year, quarter, payload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("set_cached_payload failed, ignoring", exc_info=exc)

    try:
        await cache_set_json(cache_key, payload, REDIS_PAYLOAD_TTL_SECONDS)
    except Exception:
        pass

    return payload
# Simple retry helper for Neo4j ingest
def _retry(func, attempts: int = 3, delay: float = 1.0):
    import time

    last_exc = None
    for _ in range(attempts):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(delay)
    if last_exc:
        raise last_exc


# =============================================================================
# Whaleforce Services Integration Functions
# =============================================================================

async def get_performance_metrics_for_earnings(
    symbol: str,
    earnings_date: str,
    holding_days: int = 30,
) -> Optional[Dict]:
    """
    Get performance metrics (Sharpe Ratio, excess return) for post-earnings period.

    Args:
        symbol: Stock ticker symbol
        earnings_date: Earnings announcement date (YYYY-MM-DD)
        holding_days: Number of trading days after earnings

    Returns:
        Performance metrics or None if service unavailable
    """
    if not ENABLE_PERFORMANCE_METRICS:
        return None

    try:
        client = get_performance_metrics_client()
        metrics = await client.get_post_earnings_metrics(
            ticker=symbol,
            earnings_date=earnings_date,
            holding_days=holding_days,
        )
        return metrics
    except Exception as exc:
        logger.warning(f"Performance metrics fetch failed for {symbol}: {exc}")
        return None


async def get_backtester_validation(
    symbol: str,
    earnings_date: str,
    prediction: str,
    holding_days: int = 30,
) -> Optional[Dict]:
    """
    Validate prediction using Backtester API.

    Args:
        symbol: Stock ticker symbol
        earnings_date: Earnings announcement date (YYYY-MM-DD)
        prediction: Predicted direction (UP, DOWN, NEUTRAL)
        holding_days: Number of trading days after earnings

    Returns:
        Validation result with actual return and correctness
    """
    if not ENABLE_BACKTESTER_VALIDATION:
        return None

    try:
        client = get_backtester_client()
        result = await client.calculate_post_earnings_return(
            ticker=symbol,
            earnings_date=earnings_date,
            holding_days=holding_days,
        )

        if result.get("error"):
            return result

        actual_return = result.get("return_pct", 0)
        prediction_upper = str(prediction).upper()

        # Determine if prediction was correct
        correct = None
        if prediction_upper == "UP":
            correct = actual_return > 0
        elif prediction_upper == "DOWN":
            correct = actual_return < 0
        elif prediction_upper == "NEUTRAL":
            correct = abs(actual_return) < 1.0  # Within 1%

        result["prediction"] = prediction
        result["prediction_correct"] = correct

        return result
    except Exception as exc:
        logger.warning(f"Backtester validation failed for {symbol}: {exc}")
        return None


async def enrich_analysis_with_services(
    payload: Dict,
    symbol: str,
    earnings_date: str,
    prediction: str,
    holding_days: int = 30,
) -> Dict:
    """
    Enrich analysis payload with data from Whaleforce services.

    Args:
        payload: Original analysis payload
        symbol: Stock ticker symbol
        earnings_date: Earnings announcement date
        prediction: Predicted direction
        holding_days: Holding period in trading days

    Returns:
        Enriched payload with service data
    """
    # Run service calls in parallel
    metrics_task = get_performance_metrics_for_earnings(symbol, earnings_date, holding_days)
    validation_task = get_backtester_validation(symbol, earnings_date, prediction, holding_days)

    metrics, validation = await asyncio.gather(
        metrics_task,
        validation_task,
        return_exceptions=True,
    )

    # Add service results to payload
    if isinstance(metrics, dict):
        payload["performance_metrics"] = metrics
    elif isinstance(metrics, Exception):
        logger.warning(f"Performance metrics error: {metrics}")
        payload["performance_metrics"] = {"error": str(metrics)}

    if isinstance(validation, dict):
        payload["backtester_validation"] = validation
    elif isinstance(validation, Exception):
        logger.warning(f"Backtester validation error: {validation}")
        payload["backtester_validation"] = {"error": str(validation)}

    return payload


async def analyze_earnings_with_services(
    symbol: str,
    year: int,
    quarter: int,
    main_model: Optional[str] = None,
    helper_model: Optional[str] = None,
    skip_cache: bool = False,
    holding_days: int = 30,
) -> Dict:
    """
    Full analysis with Whaleforce services integration.

    This extends analyze_earnings_async with additional data from:
    - Performance Metrics Service (Sharpe Ratio, excess returns)
    - Backtester API (post-earnings return validation)

    Args:
        symbol: Stock ticker symbol
        year: Fiscal year
        quarter: Fiscal quarter
        main_model: Main LLM model
        helper_model: Helper LLM model
        skip_cache: Skip cache lookup
        holding_days: Post-earnings holding period

    Returns:
        Enriched analysis payload
    """
    # Run base analysis
    payload = await analyze_earnings_async(
        symbol=symbol,
        year=year,
        quarter=quarter,
        main_model=main_model,
        helper_model=helper_model,
        skip_cache=skip_cache,
    )

    # Get earnings date and prediction from result
    earnings_date = payload.get("transcript_date")
    agentic_result = payload.get("agentic_result", {})
    prediction = agentic_result.get("prediction", "NEUTRAL")

    if not earnings_date:
        logger.warning(f"No earnings date found for {symbol} {year}Q{quarter}")
        return payload

    # Enrich with service data
    enriched_payload = await enrich_analysis_with_services(
        payload=payload,
        symbol=symbol,
        earnings_date=earnings_date,
        prediction=prediction,
        holding_days=holding_days,
    )

    return enriched_payload
