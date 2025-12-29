from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional


class AgenticRagBridgeError(RuntimeError):
    """Custom error for Agentic RAG bridge failures."""


REPO_NAME = "EarningsCallAgenticRag"


def _resolve_repo_path() -> Path:
    """Locate the external repo; raise with actionable guidance if missing."""
    base = Path(__file__).resolve().parent
    env_path = os.getenv("EARNINGS_RAG_PATH")
    repo_path = Path(env_path) if env_path else base / REPO_NAME
    if not repo_path.exists():
        raise AgenticRagBridgeError(
            f"找不到外部研究庫資料夾：{repo_path}. "
            "請先執行 `git clone https://github.com/la9806958/EarningsCallAgenticRag.git EarningsCallAgenticRag` "
            "並確認與本專案並排。"
        )
    return repo_path


def _ensure_sys_path(repo_path: Path) -> None:
    repo_str = str(repo_path)
    if repo_str not in sys.path:
        sys.path.insert(0, repo_str)


def _env_credentials() -> Optional[Dict[str, Any]]:
    """Build credentials from environment variables using LiteLLM proxy."""
    # LiteLLM configuration
    litellm_endpoint = os.getenv("LITELLM_ENDPOINT", "https://litellm.whaleforce.dev")
    litellm_api_key = os.getenv("LITELLM_API_KEY")

    # Neo4j configuration
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_username = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    neo4j_db = os.getenv("NEO4J_DATABASE") or "neo4j"

    if not all([litellm_api_key, neo4j_uri, neo4j_username, neo4j_password]):
        return None

    creds: Dict[str, Any] = {
        # Use LiteLLM as OpenAI-compatible endpoint
        "openai_api_key": litellm_api_key,
        "openai_api_base": litellm_endpoint,
        # Neo4j settings
        "neo4j_uri": neo4j_uri,
        "neo4j_username": neo4j_username,
        "neo4j_password": neo4j_password,
        "neo4j_database": neo4j_db,
    }

    return creds


def _credentials_path(repo_path: Path) -> Path:
    cred = repo_path / "credentials.json"
    if not cred.exists():
        env_creds = _env_credentials()
        if env_creds:
            try:
                # Avoid race: create only if missing
                cred.write_text(json.dumps(env_creds, indent=2))
            except FileExistsError:
                # Another process wrote it; keep going
                pass
        else:
            raise AgenticRagBridgeError(
                f"外部庫的 credentials.json 未找到：{cred}. "
                "請依照 EarningsCallAgenticRag README 填入 LiteLLM 與 Neo4j 設定，或在環境變數提供 LITELLM_API_KEY / NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD。"
            )
    return cred


def _load_sector_map(repo_path: Path) -> Dict[str, str]:
    """Best-effort load and merge all GICS sector maps (NYSE + NASDAQ + MAEC)."""
    candidates = [
        repo_path / "gics_sector_map_nyse.csv",
        repo_path / "gics_sector_map_nasdaq.csv",
        repo_path / "gics_sector_map_maec.csv",
    ]
    import pandas as pd  # Lazy import; included in requirements

    merged: Dict[str, str] = {}
    for csv_path in candidates:
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                cols = {c.lower(): c for c in df.columns}
                ticker_col = cols.get("ticker") or cols.get("symbol")
                sector_col = cols.get("sector") or cols.get("gics_sector")
                if ticker_col and sector_col:
                    for t, s in zip(df[ticker_col], df[sector_col]):
                        if pd.notna(t) and pd.notna(s):
                            merged[str(t).upper()] = str(s)
            except Exception:
                continue
    return merged


def _summarize_financials(financials: Optional[Dict[str, Any]]) -> str:
    """Create a compact string for the main agent prompt."""
    if not financials:
        return "No structured financials supplied."

    parts: List[str] = []
    income = financials.get("income") or []
    balance = financials.get("balance") or []
    cash = financials.get("cashFlow") or []

    def _line(label: str, rows: List[dict], keys: List[str]) -> Optional[str]:
        if not rows:
            return None
        latest = rows[0] if isinstance(rows[0], dict) else {}
        date = (
            latest.get("date")
            or latest.get("calendarYear")
            or latest.get("fillingDate")
            or latest.get("period")
        )
        metrics = []
        for k in keys:
            if k in latest and latest[k] not in (None, ""):
                metrics.append(f"{k}={latest[k]}")
        if not metrics:
            metrics.append("no key metrics detected")
        return f"{label} [{date or 'n/a'}]: " + ", ".join(metrics)

    income_line = _line("Income", income, ["revenue", "netIncome", "eps", "grossProfit"])
    balance_line = _line("Balance", balance, ["totalAssets", "totalLiabilities", "cashAndCashEquivalents"])
    cash_line = _line("CashFlow", cash, ["operatingCashFlow", "freeCashFlow"])

    for ln in (income_line, balance_line, cash_line):
        if ln:
            parts.append(ln)

    return "\n".join(parts) if parts else "Financial statements present but could not summarize."


@contextmanager
def _push_dir(path: Path):
    cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(cwd)


def _resolve_models(main_model: Optional[str], helper_model: Optional[str]) -> Dict[str, Any]:
    """Return sanitized models and matching temperatures for main/helper agents.

    Uses LiteLLM proxy which supports various models. Default to gpt-4o-mini.
    """
    # Default temperature for all models via LiteLLM
    default_temp = 0.7

    # Use environment variable defaults or fall back to gpt-5-mini
    default_main = os.getenv("MAIN_MODEL", "gpt-5-mini")
    default_helper = os.getenv("HELPER_MODEL", "gpt-5-mini")

    chosen_main = main_model if main_model else default_main
    chosen_helper = helper_model if helper_model else default_helper

    return {
        "main_model": chosen_main,
        "main_temperature": default_temp,
        "helper_model": chosen_helper,
        "helper_temperature": default_temp,
    }


def run_single_call_from_context(
    context: Dict[str, Any],
    main_model: Optional[str] = None,
    helper_model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the real Agentic RAG pipeline from the external repo for a single earnings call.

    Returns a result dict with at least: prediction, confidence, summary, reasons, raw.
    """
    repo_path = _resolve_repo_path()
    _ensure_sys_path(repo_path)
    cred_path = _credentials_path(repo_path)

    try:
        from agents.mainAgent import MainAgent
        from agents.agent_factory import (
            get_historical_performance_agent,
            get_historical_earnings_agent,
            get_comparative_agent,
        )
    except Exception as exc:  # noqa: BLE001
        raise AgenticRagBridgeError(f"匯入 Agentic RAG 模組失敗：{exc}") from exc

    symbol = (context.get("symbol") or context.get("ticker") or "").upper()
    year = context.get("year")
    quarter = context.get("quarter")
    transcript_text = context.get("transcript_text") or ""
    transcript_date = context.get("transcript_date") or ""

    if not symbol or not year or not quarter:
        raise AgenticRagBridgeError("context 缺少必填欄位：symbol、year、quarter。")

    # ------------------------------------------------------------------
    # Fetch market anchors (eps surprise, earnings day return, pre-earnings momentum)
    # ------------------------------------------------------------------
    market_anchors: Dict[str, Any] = {}
    try:
        import pg_client

        # Get EPS surprise
        eps_data = pg_client.get_earnings_surprise(symbol, year, quarter)
        if eps_data:
            market_anchors["eps_surprise"] = eps_data.get("eps_surprise")
            market_anchors["eps_actual"] = eps_data.get("eps_actual")
            market_anchors["eps_estimated"] = eps_data.get("eps_estimated")

        # Get price analysis (earnings day return)
        price_analysis = pg_client.get_price_analysis(symbol, year, quarter)
        if price_analysis:
            market_anchors["earnings_day_return"] = price_analysis.get("pct_change_t")

        # Get pre-earnings momentum (5-day)
        if transcript_date:
            momentum = pg_client.get_pre_earnings_momentum(symbol, transcript_date, days=5)
            if momentum:
                market_anchors["pre_earnings_5d_return"] = momentum.get("return_pct")

        # Get market timing (BMO/AMC)
        timing = pg_client.get_market_timing(symbol, year, quarter)
        if timing:
            market_anchors["market_timing"] = timing
    except Exception:
        pass  # Silently continue if market anchors unavailable

    quarter_label = f"{year}-Q{quarter}"
    sector_map = _load_sector_map(repo_path)
    sector = context.get("sector")

    # FMP API fallback: If symbol not in sector_map CSV, query FMP for sector
    if symbol not in sector_map:
        try:
            from fmp_client import get_company_profile
            profile = get_company_profile(symbol)
            if profile and profile.get("sector"):
                sector_map[symbol] = profile["sector"]
                sector = sector or profile["sector"]
        except Exception:
            pass  # Silently fall back to full DB scan if FMP fails

    model_cfg = _resolve_models(main_model, helper_model)

    with _push_dir(repo_path):
        # Use agent factory to get the appropriate agent implementations
        comparative_agent = get_comparative_agent(
            credentials_file=str(cred_path),
            model=model_cfg["helper_model"],
            temperature=model_cfg["helper_temperature"],
            sector_map=sector_map or None,
        )
        financials_agent = get_historical_performance_agent(
            credentials_file=str(cred_path),
            model=model_cfg["helper_model"],
            temperature=model_cfg["helper_temperature"],
        )
        past_calls_agent = get_historical_earnings_agent(
            credentials_file=str(cred_path),
            model=model_cfg["helper_model"],
            temperature=model_cfg["helper_temperature"],
        )
        main_agent = MainAgent(
            credentials_file=str(cred_path),
            model=model_cfg["main_model"],
            temperature=model_cfg["main_temperature"],
            comparative_agent=comparative_agent,
            financials_agent=financials_agent,
            past_calls_agent=past_calls_agent,
        )

        # Extract and annotate facts from transcript
        facts = main_agent.extract(transcript_text)
        for f in facts:
            f.setdefault("ticker", symbol)
            f.setdefault("quarter", quarter_label)

        row = {
            "ticker": symbol,
            "q": quarter_label,
            "transcript": transcript_text,
            "sector": sector,
        }
        financials_text = _summarize_financials(context.get("financials"))

        agent_output = main_agent.run(
            facts,
            row,
            mem_txt=None,
            original_transcript=transcript_text,
            financial_statements_facts=financials_text,
            market_anchors=market_anchors if market_anchors else None,
        )

    if not isinstance(agent_output, dict):
        agent_output = {"raw_output": agent_output}

    def _infer_direction(summary: Optional[str]) -> tuple[str, Optional[float]]:
        if not summary:
            return "UNKNOWN", None
        import re

        match = re.search(r"Direction\s*:\s*(\d+)", summary, re.IGNORECASE)
        if match:
            score = int(match.group(1))
            # Updated mapping (prompt_v2 - no NEUTRAL allowed):
            # - Direction >= 6 視為 UP
            # - Direction <= 5 視為 DOWN (Direction 5 now maps to DOWN, not NEUTRAL)
            # This aligns with the prompt instruction to never use Direction 5
            # and lean DOWN when uncertain
            if score >= 6:
                return "UP", score / 10
            # Direction 5 or below maps to DOWN (lean bearish when uncertain)
            return "DOWN", score / 10

        lowered = summary.lower()
        if any(k in lowered for k in ["up", "increase", "growth", "record", "beat"]):
            return "UP", 0.6
        if any(k in lowered for k in ["down", "decline", "miss", "pressure", "headwind"]):
            return "DOWN", 0.4
        return "UNKNOWN", None

    def _extract_long_eligible_json(summary: Optional[str]) -> Optional[Dict[str, Any]]:
        """Extract the LongEligible JSON block from the main agent output."""
        if not summary:
            return None
        import re

        # Try to find JSON block in markdown code fence
        json_match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", summary)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try to find raw JSON block at the end
        json_match = re.search(r'\{\s*"DirectionScore"[\s\S]*?"AnchorNotes"[\s\S]*?\}', summary)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except json.JSONDecodeError:
                pass

        return None

    def _compute_trade_long(long_json: Optional[Dict[str, Any]], sector: Optional[str] = None) -> bool:
        """Compute trade_long based on LongEligible criteria.

        Rules:
        - DirectionScore >= 8
        - HardVetoCount == 0
        - HardPositivesCount >= 2
        - PricedInRisk != High
        - Tech sector: require GuidanceRaised OR (DemandAcceleration AND VisibilityImproving)
        """
        if not long_json:
            return False

        try:
            direction_score = int(long_json.get("DirectionScore", 0))
            hard_veto_count = int(long_json.get("HardVetoCount", 5))
            hard_positives_count = int(long_json.get("HardPositivesCount", 0))
            priced_in_risk = str(long_json.get("PricedInRisk", "High")).lower()
            long_eligible_str = str(long_json.get("LongEligible", "NO")).upper()

            # Basic criteria
            basic_eligible = (
                direction_score >= 8 and
                hard_veto_count == 0 and
                hard_positives_count >= 2 and
                priced_in_risk != "high"
            )

            if not basic_eligible:
                return False

            # Tech sector additional rule
            is_tech = sector and "technology" in sector.lower()
            if is_tech:
                guidance_raised = str(long_json.get("GuidanceRaised", "NO")).upper() == "YES"
                demand_accel = str(long_json.get("DemandAcceleration", "NO")).upper() == "YES"
                visibility_improving = str(long_json.get("VisibilityImproving", "NO")).upper() == "YES"

                if not (guidance_raised or (demand_accel and visibility_improving)):
                    return False

            return True

        except (ValueError, TypeError):
            return False

    notes = agent_output.get("notes") or {}

    def _keep(val: Optional[str]) -> Optional[str]:
        if not val:
            return None
        normalized = str(val).strip()
        if normalized.lower() in {"n/a", "na", "none"}:
            return None
        return normalized

    reasons = [
        f"financials: {notes.get('financials')}" if _keep(notes.get("financials")) else None,
        f"past calls: {notes.get('past')}" if _keep(notes.get("past")) else None,
        f"peers: {notes.get('peers')}" if _keep(notes.get("peers")) else None,
    ]
    reasons = [r for r in reasons if r]

    if not reasons:
        # Fallback：取前 3 條提取的事實做理由摘要
        top_facts = facts[:3]
        for f in top_facts:
            metric = f.get("metric") or "metric"
            val = f.get("value") or ""
            ctx = f.get("context") or f.get("reason") or ""
            reasons.append(f"{metric}: {val} {ctx}".strip())

    prediction, confidence = _infer_direction(agent_output.get("summary"))

    # Extract LongEligible JSON and compute trade_long
    long_eligible_json = _extract_long_eligible_json(agent_output.get("summary"))
    trade_long = _compute_trade_long(long_eligible_json, sector)

    meta = agent_output.setdefault("metadata", {})
    meta.setdefault(
        "models",
        {
            "main": model_cfg["main_model"],
            "helpers": model_cfg["helper_model"],
            "main_temperature": model_cfg["main_temperature"],
            "helper_temperature": model_cfg["helper_temperature"],
        },
    )

    return {
        "prediction": prediction,
        "confidence": confidence,
        "summary": agent_output.get("summary"),
        "reasons": reasons,
        "raw": agent_output,
        "trade_long": trade_long,
        "long_eligible_json": long_eligible_json,
    }


def verify_agentic_repo() -> bool:
    """
    Quick healthcheck: ensure external repo & credentials.json exist and are readable.
    """
    repo_path = _resolve_repo_path()
    _ensure_sys_path(repo_path)
    _credentials_path(repo_path)
    return True
