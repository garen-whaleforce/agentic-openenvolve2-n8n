from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from openai import AsyncOpenAI
from pydantic import BaseModel, Field
from uuid import uuid4

from analysis_engine import (
    analyze_earnings,
    analyze_earnings_async,
    analyze_earnings_with_services,
    get_performance_metrics_for_earnings,
    get_backtester_validation,
)
from prompt_service import (
    save_prompt_override,
    reset_prompt_override,
    list_prompt_profiles,
    get_prompt_profile,
    save_prompt_profile,
    delete_prompt_profile,
)
from EarningsCallAgenticRag.agents.prompts.prompts import (
    get_main_agent_system_message,
    get_extraction_system_message,
    get_delegation_system_message,
    get_comparative_system_message,
    get_historical_earnings_system_message,
    get_financials_system_message,
    get_default_system_prompts,
    get_all_default_prompts,
)
from EarningsCallAgenticRag.utils.config import (
    MAIN_MODEL,
    HELPER_MODEL,
)
from prompt_service import get_prompt_override
from fmp_client import (
    close_fmp_client,
    close_fmp_async_client,
    get_earnings_calendar_for_date,
    get_earnings_calendar_for_range,
    get_transcript_dates,
    search_symbols,
    _require_api_key,
)
from agentic_rag_bridge import verify_agentic_repo
from storage import ensure_db_writable, get_call, init_db, list_calls

# Whaleforce Services Integration
from services.sec_filings_client import SECFilingsClient, get_sec_filings_client
from services.backtester_client import BacktesterClient, get_backtester_client
from services.performance_metrics_client import PerformanceMetricsClient, get_performance_metrics_client
from EarningsCallAgenticRag.agents.secFilingsAgent import get_sec_context

# Optional: semantic_backtest_service (may not be present in all deployments)
try:
    from semantic_backtest_service.api import router as backtest_router
    HAS_BACKTEST_SERVICE = True
except ImportError:
    backtest_router = None
    HAS_BACKTEST_SERVICE = False

load_dotenv()

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Route B: Real-time Earnings Call Analysis")

# LiteLLM Configuration
LITELLM_ENDPOINT = os.getenv("LITELLM_ENDPOINT", "https://litellm.whaleforce.dev")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY", "")
# Model defaults imported from EarningsCallAgenticRag.utils.config
MAIN_MODEL_DEFAULT = MAIN_MODEL
HELPER_MODEL_DEFAULT = HELPER_MODEL

# All 15 editable prompts: 6 System Messages + 9 Prompt Templates
EDITABLE_PROMPT_KEYS = [
    # System Messages (6)
    "MAIN_AGENT_SYSTEM_MESSAGE",
    "EXTRACTION_SYSTEM_MESSAGE",
    "DELEGATION_SYSTEM_MESSAGE",
    "COMPARATIVE_SYSTEM_MESSAGE",
    "HISTORICAL_EARNINGS_SYSTEM_MESSAGE",
    "FINANCIALS_SYSTEM_MESSAGE",
    # Prompt Templates (9)
    "COMPARATIVE_AGENT_PROMPT",
    "HISTORICAL_EARNINGS_AGENT_PROMPT",
    "FINANCIALS_STATEMENT_AGENT_PROMPT",
    "MAIN_AGENT_PROMPT",
    "FACTS_EXTRACTION_PROMPT",
    "FACTS_DELEGATION_PROMPT",
    "PEER_DISCOVERY_TICKER_PROMPT",
    "MEMORY_PROMPT",
    "BASELINE_PROMPT",
]


def _get_current_prompts_dict():
    """
    回傳目前生效中的 15 個 prompts（default + DB override）。
    包含 6 個 system messages + 9 個 prompt templates。
    """
    defaults = get_all_default_prompts()
    result = {}
    for key in EDITABLE_PROMPT_KEYS:
        result[key] = get_prompt_override(key, defaults.get(key, ""))
    return result


def _detect_active_profile_name() -> str:
    """
    判斷目前生效的 prompts 是 default / 某個 profile / custom。
    回傳值:
      - "default": 完全等於 hard-coded 預設值
      - "<profile_name>": 完全等於某個已存 profile
      - "custom": 與 default 及所有 profile 都不同
    """
    current = _get_current_prompts_dict()
    defaults = get_all_default_prompts()

    # Check if current matches default
    if current == defaults:
        return "default"

    # Check if current matches any saved profile
    profiles = list_prompt_profiles()
    for p in profiles:
        profile_data = get_prompt_profile(p["name"])
        if profile_data and profile_data.get("prompts") == current:
            return p["name"]

    return "custom"


def _build_litellm_client() -> Optional[AsyncOpenAI]:
    """Build OpenAI-compatible client for LiteLLM proxy."""
    if not LITELLM_API_KEY:
        logging.warning("LITELLM_API_KEY not set, LLM features will be disabled")
        return None

    return AsyncOpenAI(
        api_key=LITELLM_API_KEY,
        base_url=LITELLM_ENDPOINT,
    )


litellm_client = _build_litellm_client()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("shutdown")
async def _shutdown_clients():
    # Ensure shared HTTP client is closed on app shutdown
    close_fmp_client()
    await close_fmp_async_client()


@app.on_event("startup")
def _startup_checks():
    # Fail fast if required config is missing
    _require_api_key()
    # Ensure DB path exists/writable
    ensure_db_writable()
    # Ensure external repo/credentials exist
    verify_agentic_repo()


@app.on_event("startup")
async def _schedule_earnings_calendar_prefetch():
    tz = None
    try:
        tz = ZoneInfo("America/New_York")
    except Exception:
        pass

    async def prefetch_loop():
        first_run = True
        while True:
            try:
                if tz is not None:
                    now = datetime.now(tz)
                else:
                    now = datetime.utcnow()
                if first_run:
                    target = now
                    wait_seconds = 0
                    first_run = False
                else:
                    target = now.replace(hour=6, minute=0, second=0, microsecond=0)
                    if target <= now:
                        target = target + timedelta(days=1)
                    wait_seconds = (target - now).total_seconds()
                    if wait_seconds < 0:
                        wait_seconds = 60.0
                await asyncio.sleep(wait_seconds)

                if tz is not None:
                    today = target.date()
                else:
                    today = datetime.utcnow().date()
                start = today - timedelta(days=7)
                try:
                    # Run blocking function in thread pool to avoid blocking event loop
                    await asyncio.to_thread(
                        get_earnings_calendar_for_range,
                        start_date=start.isoformat(),
                        end_date=today.isoformat(),
                        min_market_cap=1_000_000_000,
                        skip_cache=True,
                    )
                except Exception as exc:
                    logging.exception("Failed to prefetch earnings calendar range: %s", exc)
            except Exception as exc:
                logging.exception("Unexpected error in prefetch_loop: %s", exc)
                await asyncio.sleep(60.0)

    asyncio.create_task(prefetch_loop())


class AnalyzeRequest(BaseModel):
    symbol: str = Field(..., description="Ticker symbol, e.g., AAPL")
    year: Optional[int] = Field(None, description="Fiscal year (required if date not provided)")
    quarter: Optional[int] = Field(None, description="Fiscal quarter 1-4 (required if date not provided)")
    date: Optional[str] = Field(None, description="Earnings date YYYY-MM-DD. If provided, fiscal year/quarter will be looked up from FMP")
    main_model: Optional[str] = Field(
        None, description="Main agent model override (e.g., gpt-4o-mini, gpt-4o, claude-3-5-sonnet)"
    )
    helper_model: Optional[str] = Field(
        None, description="Helper agents model override (e.g., gpt-4o-mini, gpt-4o)"
    )
    refresh: bool = Field(False, description="Skip cache and force re-analysis")

class BatchAnalyzeRequest(BaseModel):
    tickers: list[str] = Field(..., description="List of ticker symbols")
    latest_only: bool = Field(True, description="Always pick latest available quarter")


class PromptItem(BaseModel):
    key: str
    content: str


class PromptUpdate(BaseModel):
    key: str
    content: str


class PromptStatus(BaseModel):
    active_profile: str


class ProfileCreate(BaseModel):
    name: str
    prompts: Optional[Dict[str, str]] = None


class ProfileApply(BaseModel):
    name: str


class BacktestRequest(BaseModel):
    """回測請求參數"""
    symbols: Optional[List[str]] = Field(None, description="指定股票列表，若為空則使用全部資料")
    year_from: Optional[int] = Field(None, description="起始年份")
    year_to: Optional[int] = Field(None, description="結束年份")
    quarters: Optional[List[int]] = Field(None, description="指定季度 [1,2,3,4]，若為空則全部")
    up_threshold: int = Field(6, description="UP 預測閾值 (direction_score >= threshold)")
    only_up: bool = Field(True, description="只做 UP 預測，DOWN 視為 NEUTRAL")
    limit: Optional[int] = Field(None, description="最多回傳筆數")


# In-memory batch job registry for background batch processing
_BATCH_JOBS: Dict[str, Dict[str, Any]] = {}
_BATCH_LOCK: Optional[asyncio.Lock] = None


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _get_batch_lock() -> asyncio.Lock:
    global _BATCH_LOCK
    if _BATCH_LOCK is None:
        _BATCH_LOCK = asyncio.Lock()
    return _BATCH_LOCK


async def _create_batch_job(tickers: List[str], latest_only: bool) -> str:
    """
    Register a job and schedule background execution.
    """
    job_id = str(uuid4())
    job = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "latest_only": latest_only,
        "total": len(tickers),
        "completed": 0,
        "results": [],
        "error": None,
        "current": None,
    }
    async with _get_batch_lock():
        _BATCH_JOBS[job_id] = job
    asyncio.create_task(_run_batch_job(job_id, tickers, latest_only))
    return job_id


async def _get_batch_job(job_id: str) -> Optional[Dict[str, Any]]:
    async with _get_batch_lock():
        job = _BATCH_JOBS.get(job_id)
        if not job:
            return None
        # shallow copy is fine for read-only responses; results list will be reused
        return dict(job)


async def _update_batch_job(job_id: str, **kwargs) -> Optional[Dict[str, Any]]:
    async with _get_batch_lock():
        job = _BATCH_JOBS.get(job_id)
        if not job:
            return None
        job.update(kwargs)
        job["updated_at"] = _now_iso()
        return dict(job)


async def _append_batch_result(job_id: str, row: Dict[str, Any]) -> None:
    async with _get_batch_lock():
        job = _BATCH_JOBS.get(job_id)
        if not job:
            return
        job.setdefault("results", []).append(row)
        job["completed"] = len(job["results"])
        total = job.get("total") or 0
        job["progress"] = job["completed"] / total if total else 1.0
        job["updated_at"] = _now_iso()


async def _resolve_latest_quarter(sym: str) -> Tuple[int, int]:
    """
    Pick the latest available fiscal year/quarter; fall back to calendar year/quarter.
    """
    dates = get_transcript_dates(sym)
    valid: List[Tuple[int, int]] = []
    for d in dates:
        y = d.get("year") or d.get("calendar_year")
        q = d.get("quarter") or d.get("calendar_quarter")
        if y is None or q is None:
            continue
        try:
            valid.append((int(y), int(q)))
        except Exception:
            continue
    if not valid:
        raise ValueError("no transcript dates")
    valid.sort(reverse=True)
    return valid[0]


async def _run_batch_job(job_id: str, tickers: List[str], latest_only: bool) -> None:
    """
    Execute batch sequentially in background to avoid request timeout on hosting platforms.
    """
    try:
        await _update_batch_job(job_id, status="running")
        for sym in tickers:
            sym = sym.strip().upper()
            await _update_batch_job(job_id, current=sym)
            try:
                year = None
                quarter = None
                if latest_only:
                    year, quarter = await _resolve_latest_quarter(sym)
                res = (
                    await analyze_earnings_async(sym, year, quarter)
                    if year and quarter
                    else None
                )
                row = {"symbol": sym, "status": "ok", "payload": res}
            except Exception as exc:  # noqa: BLE001
                row = {"symbol": sym, "status": "error", "error": str(exc)}
            await _append_batch_result(job_id, row)
        await _update_batch_job(job_id, status="completed", current=None)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Batch job %s failed: %s", job_id, exc)
        await _update_batch_job(job_id, status="failed", error=str(exc), current=None)


@app.get("/api/symbols")
def api_symbols(q: str = Query("", description="Search term for company name or ticker")) -> List[dict]:
    try:
        return search_symbols(q)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/earnings-calendar/range")
def api_earnings_calendar_range(
    start_date: Optional[str] = Query(
        None,
        description="Start date YYYY-MM-DD; default: today-7 (US/Eastern)",
    ),
    end_date: Optional[str] = Query(
        None,
        description="End date YYYY-MM-DD; default: today (US/Eastern)",
    ),
    min_market_cap: float = Query(1_000_000_000, description="Minimum market cap filter"),
    refresh: bool = Query(
        False,
        description="Skip cache and refetch underlying data",
    ),
):
    try:
        tz = ZoneInfo("America/New_York")
    except Exception:
        # fallback: UTC
        tz = None

    if tz is not None:
        now = datetime.now(tz).date()
    else:
        now = datetime.utcnow().date()
    default_end = now
    default_start = now - timedelta(days=7)

    if end_date:
        try:
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="end_date must be YYYY-MM-DD")
    else:
        end = default_end

    if start_date:
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="start_date must be YYYY-MM-DD")
    else:
        start = default_start

    if start > end:
        start, end = end, start

    try:
        return get_earnings_calendar_for_range(
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            min_market_cap=min_market_cap,
            skip_cache=refresh,
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("Error in api_earnings_calendar_range: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/transcript-dates")
def api_transcript_dates(symbol: str = Query(..., description="Ticker symbol")) -> List[dict]:
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    try:
        return get_transcript_dates(symbol)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/earnings-calendar/today")
def api_earnings_calendar_today(
    min_market_cap: float = Query(1_000_000_000, description="Minimum market cap filter"),
    date: Optional[str] = Query(None, description="Target date YYYY-MM-DD; defaults to today (UTC)"),
    refresh: bool = Query(False, description="Skip cache and refetch"),
):
    date_str = None
    if date:
        try:
            datetime.strptime(date, "%Y-%m-%d")
            date_str = date
        except ValueError:
            raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    try:
        return get_earnings_calendar_for_date(
            target_date=date_str, min_market_cap=min_market_cap, skip_cache=refresh
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/analyze")
async def api_analyze(payload: AnalyzeRequest):
    try:
        year = payload.year
        quarter = payload.quarter

        # If date is provided, look up fiscal year/quarter from FMP
        if payload.date:
            from fmp_client import get_fiscal_quarter_by_date
            fiscal_info = get_fiscal_quarter_by_date(payload.symbol, payload.date)
            if fiscal_info is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No transcript found for {payload.symbol} on date {payload.date}"
                )
            year = fiscal_info["year"]
            quarter = fiscal_info["quarter"]
        elif year is None or quarter is None:
            raise HTTPException(
                status_code=400,
                detail="Either 'date' or both 'year' and 'quarter' must be provided"
            )

        result = await analyze_earnings_async(
            payload.symbol,
            year,
            quarter,
            payload.main_model,
            payload.helper_model,
            skip_cache=payload.refresh,
        )
        return JSONResponse(result)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/batch-analyze")
async def api_batch_analyze(payload: BatchAnalyzeRequest):
    """
    Enqueue a batch job and return job_id immediately. Processing happens in background.
    """
    tickers = [t.strip().upper() for t in payload.tickers if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="tickers is required")

    job_id = await _create_batch_job(tickers, payload.latest_only)
    return {"job_id": job_id, "status": "queued", "total": len(tickers)}


@app.get("/api/batch-analyze/{job_id}")
async def api_batch_status(job_id: str):
    """
    Poll batch job status/results.
    """
    job = await _get_batch_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="batch job not found")
    return job


@app.get("/api/prompts", response_model=List[PromptItem])
async def api_get_prompts():
    current = _get_current_prompts_dict()
    return [PromptItem(key=k, content=v) for k, v in current.items() if k in EDITABLE_PROMPT_KEYS]


@app.put("/api/prompts")
async def api_update_prompt(payload: PromptUpdate):
    if payload.key not in EDITABLE_PROMPT_KEYS:
        raise HTTPException(status_code=400, detail="Unknown prompt key")

    save_prompt_override(payload.key, payload.content)
    return {"status": "ok"}


@app.get("/api/prompts/status", response_model=PromptStatus)
async def api_prompt_status():
    """回傳目前生效的 prompt 組合名稱（default / custom / profile_name）。"""
    return PromptStatus(active_profile=_detect_active_profile_name())


@app.delete("/api/prompts/{key}")
async def api_delete_prompt(key: str):
    """刪除某個 prompt 的 override，恢復為 default。"""
    if key not in EDITABLE_PROMPT_KEYS:
        raise HTTPException(status_code=400, detail="Unknown prompt key")
    reset_prompt_override(key)
    return {"status": "ok"}


@app.get("/api/prompt_profiles")
async def api_list_profiles():
    """列出所有已存 profile（name + updated_at）。"""
    return list_prompt_profiles()


@app.post("/api/prompt_profiles")
async def api_create_profile(payload: ProfileCreate):
    """
    建立或更新一個 profile。
    如果沒傳 prompts，就以「目前生效的 prompts」存檔。
    """
    prompts = payload.prompts or _get_current_prompts_dict()
    save_prompt_profile(payload.name, prompts)
    return {"status": "ok", "name": payload.name}


@app.get("/api/prompt_profiles/{name}")
async def api_get_profile(name: str):
    """取得指定 profile 的詳細內容（含 prompts dict）。"""
    data = get_prompt_profile(name)
    if not data:
        raise HTTPException(status_code=404, detail="Profile not found")
    return data


@app.post("/api/prompt_profiles/apply")
async def api_apply_profile(payload: ProfileApply):
    """
    套用某個 profile 到目前設定。
    若 name == "default"，清除所有 override。
    """
    if payload.name == "default":
        # Reset all prompts to default
        for key in EDITABLE_PROMPT_KEYS:
            reset_prompt_override(key)
        return {"status": "ok", "applied": "default"}

    data = get_prompt_profile(payload.name)
    if not data:
        raise HTTPException(status_code=404, detail="Profile not found")

    prompts = data.get("prompts", {})
    defaults = get_default_system_prompts()
    for key in EDITABLE_PROMPT_KEYS:
        if key in prompts and prompts[key] != defaults.get(key):
            save_prompt_override(key, prompts[key])
        else:
            reset_prompt_override(key)
    return {"status": "ok", "applied": payload.name}


@app.delete("/api/prompt_profiles/{name}")
async def api_delete_profile(name: str):
    """刪除指定 profile。"""
    delete_prompt_profile(name)
    return {"status": "ok"}


# ============================================================
# 回測 API - 供外部程式呼叫
# ============================================================

# 載入 3500 筆 garen1212v4 測試結果 (啟動時載入一次)
_BACKTEST_DATA: Optional[List[Dict[str, Any]]] = None
_BACKTEST_DATA_PATH = BASE_DIR / "scripts" / "batch_3500_garen1212v4_final_20251214_113657.json"


def _load_backtest_data() -> List[Dict[str, Any]]:
    """載入並快取回測資料"""
    global _BACKTEST_DATA
    if _BACKTEST_DATA is None:
        if _BACKTEST_DATA_PATH.exists():
            with open(_BACKTEST_DATA_PATH, "r") as f:
                _BACKTEST_DATA = json.load(f)
        else:
            _BACKTEST_DATA = []
    return _BACKTEST_DATA


@app.get("/api/backtest/info")
async def api_backtest_info():
    """
    回測系統資訊
    - profile: garen1212v4
    - 資料筆數、時間範圍等
    """
    data = _load_backtest_data()
    success = [d for d in data if d.get("status") == "success"]

    years = sorted(set(d.get("year") for d in success if d.get("year")))
    quarters = sorted(set(d.get("quarter") for d in success if d.get("quarter")))
    symbols = sorted(set(d.get("symbol") for d in success if d.get("symbol")))

    return {
        "profile": "garen1212v4",
        "total_records": len(data),
        "success_records": len(success),
        "years": years,
        "quarters": quarters,
        "symbol_count": len(symbols),
        "data_file": str(_BACKTEST_DATA_PATH),
    }


@app.post("/api/backtest/run")
async def api_backtest_run(payload: BacktestRequest):
    """
    執行回測分析

    參數:
    - symbols: 指定股票列表，若為空則使用全部
    - year_from/year_to: 年份範圍
    - quarters: 指定季度
    - up_threshold: UP 預測閾值 (預設 6，可改為 7)
    - only_up: 只做 UP 預測 (預設 True)
    - limit: 最多回傳筆數

    回傳:
    - 勝率統計
    - 每筆詳細結果
    """
    data = _load_backtest_data()
    success = [d for d in data if d.get("status") == "success" and d.get("t30_actual") is not None]

    # 過濾條件
    filtered = success

    if payload.symbols:
        symbols_upper = [s.upper() for s in payload.symbols]
        filtered = [d for d in filtered if d.get("symbol", "").upper() in symbols_upper]

    if payload.year_from:
        filtered = [d for d in filtered if d.get("year", 0) >= payload.year_from]

    if payload.year_to:
        filtered = [d for d in filtered if d.get("year", 9999) <= payload.year_to]

    if payload.quarters:
        filtered = [d for d in filtered if d.get("quarter") in payload.quarters]

    # 套用閾值策略
    results = []
    hit_count = 0
    miss_count = 0
    skip_count = 0

    for d in filtered:
        score = d.get("direction_score", 5)
        t30 = d["t30_actual"]

        # 決定預測方向
        if score >= payload.up_threshold:
            prediction = "UP"
        elif payload.only_up:
            prediction = "NEUTRAL"
        elif score <= 4:
            prediction = "DOWN"
        else:
            prediction = "NEUTRAL"

        # 計算結果
        if prediction == "UP":
            hit_result = "HIT" if t30 > 0 else "MISS"
            if t30 > 0:
                hit_count += 1
            else:
                miss_count += 1
        elif prediction == "DOWN":
            hit_result = "HIT" if t30 < 0 else "MISS"
            if t30 < 0:
                hit_count += 1
            else:
                miss_count += 1
        else:
            hit_result = "SKIP"
            skip_count += 1

        results.append({
            "symbol": d.get("symbol"),
            "year": d.get("year"),
            "quarter": d.get("quarter"),
            "earnings_date": d.get("earnings_date"),
            "direction_score": score,
            "prediction": prediction,
            "t30_actual": round(t30, 2),
            "hit_result": hit_result,
        })

    # 限制回傳筆數
    if payload.limit and len(results) > payload.limit:
        results = results[:payload.limit]

    # 計算統計
    total_valid = hit_count + miss_count
    accuracy = (hit_count / total_valid * 100) if total_valid > 0 else 0
    coverage = (total_valid / len(filtered) * 100) if filtered else 0

    return {
        "profile": "garen1212v4",
        "strategy": {
            "up_threshold": payload.up_threshold,
            "only_up": payload.only_up,
        },
        "filters": {
            "symbols": payload.symbols,
            "year_from": payload.year_from,
            "year_to": payload.year_to,
            "quarters": payload.quarters,
        },
        "statistics": {
            "total_samples": len(filtered),
            "hit": hit_count,
            "miss": miss_count,
            "skip": skip_count,
            "valid_predictions": total_valid,
            "accuracy": round(accuracy, 2),
            "coverage": round(coverage, 2),
        },
        "results": results,
    }


@app.get("/api/backtest/strategies")
async def api_backtest_strategies():
    """
    列出預設策略及其歷史表現
    """
    data = _load_backtest_data()
    success = [d for d in data if d.get("status") == "success" and d.get("t30_actual") is not None]

    strategies = [
        {"name": "original", "up_threshold": 6, "only_up": False, "description": "原始 (UP>=6, DOWN<=4)"},
        {"name": "up_only_6", "up_threshold": 6, "only_up": True, "description": "只UP (>=6)"},
        {"name": "up_only_7", "up_threshold": 7, "only_up": True, "description": "只UP (>=7)"},
        {"name": "up_only_8", "up_threshold": 8, "only_up": True, "description": "只UP (>=8)"},
    ]

    results = []
    for strat in strategies:
        hit = 0
        miss = 0
        skip = 0

        for d in success:
            score = d.get("direction_score", 5)
            t30 = d["t30_actual"]

            if score >= strat["up_threshold"]:
                if t30 > 0:
                    hit += 1
                else:
                    miss += 1
            elif not strat["only_up"] and score <= 4:
                if t30 < 0:
                    hit += 1
                else:
                    miss += 1
            else:
                skip += 1

        total = hit + miss
        acc = (hit / total * 100) if total > 0 else 0
        cov = (total / len(success) * 100) if success else 0

        results.append({
            "name": strat["name"],
            "description": strat["description"],
            "up_threshold": strat["up_threshold"],
            "only_up": strat["only_up"],
            "hit": hit,
            "miss": miss,
            "skip": skip,
            "accuracy": round(acc, 2),
            "coverage": round(cov, 2),
        })

    return {"profile": "garen1212v4", "strategies": results}


# Include backtest router (語義反轉回測系統) - only if available
if HAS_BACKTEST_SERVICE and backtest_router:
    app.include_router(backtest_router)


@app.get("/api/calls")
def api_calls(
    symbol: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    ret_min: Optional[float] = Query(None),
    ret_max: Optional[float] = Query(None),
    prediction: Optional[str] = Query(None),
    correct: Optional[bool] = Query(None),
    sort: str = Query("date_desc"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        rows = list_calls(
            symbol=symbol,
            sector=sector,
            date_from=date_from,
            date_to=date_to,
            ret_min=ret_min,
            ret_max=ret_max,
            prediction=prediction,
            correct=correct,
            sort=sort,
            limit=limit,
            offset=offset,
        )
        return rows
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/call/{job_id}")
def api_call_detail(job_id: str):
    try:
        row = get_call(job_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not row:
        raise HTTPException(status_code=404, detail="call not found")
    return row


# =============================================================================
# Whaleforce Services Integration API Endpoints
# =============================================================================

@app.get("/api/services/health")
async def api_services_health():
    """Check health of all integrated Whaleforce services."""
    results = {}

    # Check SEC Filings Service
    try:
        sec_client = get_sec_filings_client()
        sec_health = await sec_client.health_check()
        results["sec_filings"] = {"status": "healthy", "response": sec_health}
    except Exception as e:
        results["sec_filings"] = {"status": "unhealthy", "error": str(e)}

    # Check Backtester API
    try:
        bt_client = get_backtester_client()
        bt_health = await bt_client.health_check()
        results["backtester"] = {"status": "healthy", "response": bt_health}
    except Exception as e:
        results["backtester"] = {"status": "unhealthy", "error": str(e)}

    # Check Performance Metrics Service
    try:
        pm_client = get_performance_metrics_client()
        pm_health = await pm_client.health_check()
        results["performance_metrics"] = {"status": "healthy", "response": pm_health}
    except Exception as e:
        results["performance_metrics"] = {"status": "unhealthy", "error": str(e)}

    healthy_count = sum(1 for r in results.values() if r.get("status") == "healthy")
    total_count = len(results)

    return {
        "overall_status": "healthy" if healthy_count == total_count else "degraded",
        "healthy_services": healthy_count,
        "total_services": total_count,
        "services": results,
    }


class AnalyzeWithServicesRequest(BaseModel):
    """Request for analysis with Whaleforce services integration."""
    symbol: str = Field(..., description="Stock ticker symbol")
    year: int = Field(..., description="Fiscal year")
    quarter: int = Field(..., ge=1, le=4, description="Fiscal quarter (1-4)")
    main_model: Optional[str] = Field(None, description="Main LLM model")
    helper_model: Optional[str] = Field(None, description="Helper LLM model")
    skip_cache: bool = Field(False, description="Skip cache lookup")
    holding_days: int = Field(30, ge=1, le=252, description="Post-earnings holding period in trading days")


@app.post("/api/analyze-with-services")
async def api_analyze_with_services(req: AnalyzeWithServicesRequest):
    """
    Analyze earnings call with full Whaleforce services integration.

    This endpoint extends the standard analysis with:
    - Performance Metrics (Sharpe Ratio, excess returns)
    - Backtester validation (actual post-earnings returns)
    - SEC Filings context (10-K, 10-Q data)
    """
    try:
        result = await analyze_earnings_with_services(
            symbol=req.symbol.upper(),
            year=req.year,
            quarter=req.quarter,
            main_model=req.main_model,
            helper_model=req.helper_model,
            skip_cache=req.skip_cache,
            holding_days=req.holding_days,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/sec-filings/search")
async def api_sec_search(query: str = Query(..., description="CIK or ticker symbol")):
    """Search for a company by CIK or ticker symbol."""
    try:
        client = get_sec_filings_client()
        result = await client.search(query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/sec-filings/filings")
async def api_sec_filings(
    ticker: str = Query(..., description="Stock ticker symbol"),
    form: Optional[str] = Query(None, description="Form type (10-K, 10-Q, 8-K)"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """Get SEC filings for a ticker."""
    try:
        client = get_sec_filings_client()
        search_result = await client.search(ticker)
        if not search_result.get("cik"):
            raise HTTPException(status_code=404, detail=f"CIK not found for {ticker}")

        cik = search_result["cik"]
        filings = await client.get_filings(
            cik=[cik],
            form=form,
            from_date=from_date,
            to_date=to_date,
        )
        return filings
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/sec-filings/context")
async def api_sec_context(
    ticker: str = Query(..., description="Stock ticker symbol"),
    year: int = Query(..., description="Fiscal year"),
    quarter: int = Query(..., ge=1, le=4, description="Fiscal quarter"),
):
    """Get SEC filings context for earnings analysis."""
    try:
        context = await get_sec_context(ticker, year, quarter)
        return context
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/performance-metrics")
async def api_performance_metrics(
    ticker: str = Query(..., description="Stock ticker symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
):
    """Get performance metrics (Sharpe Ratio, excess return) for a ticker."""
    try:
        client = get_performance_metrics_client()
        metrics = await client.get_metrics(ticker, start_date, end_date)
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/performance-metrics/post-earnings")
async def api_performance_metrics_post_earnings(
    ticker: str = Query(..., description="Stock ticker symbol"),
    earnings_date: str = Query(..., description="Earnings date (YYYY-MM-DD)"),
    holding_days: int = Query(30, ge=1, le=252, description="Holding period in trading days"),
):
    """Get performance metrics for post-earnings period."""
    try:
        metrics = await get_performance_metrics_for_earnings(ticker, earnings_date, holding_days)
        if metrics is None:
            raise HTTPException(status_code=503, detail="Performance metrics service unavailable")
        return metrics
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/backtester/ohlcv")
async def api_backtester_ohlcv(
    ticker: str = Query(..., description="Stock ticker symbol"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    interval: str = Query("1d", description="Data interval"),
):
    """Get OHLCV data from Backtester API."""
    try:
        client = get_backtester_client()
        data = await client.get_ohlcv(ticker, start_date, end_date, interval)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/services/backtester/post-earnings-return")
async def api_backtester_post_earnings_return(
    ticker: str = Query(..., description="Stock ticker symbol"),
    earnings_date: str = Query(..., description="Earnings date (YYYY-MM-DD)"),
    holding_days: int = Query(30, ge=1, le=252, description="Holding period in trading days"),
):
    """Calculate post-earnings return using Backtester API."""
    try:
        client = get_backtester_client()
        result = await client.calculate_post_earnings_return(ticker, earnings_date, holding_days)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/services/backtester/validate-prediction")
async def api_backtester_validate_prediction(
    ticker: str = Query(..., description="Stock ticker symbol"),
    earnings_date: str = Query(..., description="Earnings date (YYYY-MM-DD)"),
    prediction: str = Query(..., description="Prediction (UP, DOWN, NEUTRAL)"),
    holding_days: int = Query(30, ge=1, le=252, description="Holding period in trading days"),
):
    """Validate a prediction using Backtester API."""
    try:
        result = await get_backtester_validation(ticker, earnings_date, prediction, holding_days)
        if result is None:
            raise HTTPException(status_code=503, detail="Backtester service unavailable")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
