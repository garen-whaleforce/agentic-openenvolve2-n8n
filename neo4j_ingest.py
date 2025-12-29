from __future__ import annotations

import os
import sys
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging


class Neo4jIngestError(RuntimeError):
    """Raised when on-the-fly Neo4j ingestion fails."""


REPO_NAME = "EarningsCallAgenticRag"
logger = logging.getLogger(__name__)


def _resolve_repo_path() -> Path:
    base = Path(__file__).resolve().parent
    env_path = os.getenv("EARNINGS_RAG_PATH")
    repo_path = Path(env_path) if env_path else base / REPO_NAME
    if not repo_path.exists():
        raise Neo4jIngestError(
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
                cred.write_text(json.dumps(env_creds, indent=2))
            except FileExistsError:
                pass
        else:
            raise Neo4jIngestError(
                f"外部庫的 credentials.json 未找到：{cred}. "
                "請依照 EarningsCallAgenticRag README 填入 LiteLLM 與 Neo4j 設定，或在環境變數提供 LITELLM_API_KEY / NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD。"
            )
    return cred


def _financial_facts(financials: Optional[Dict[str, Any]], ticker: str, quarter_label: str) -> List[Dict[str, Any]]:
    """Create lightweight facts from the latest financial statement rows."""
    if not financials:
        return []

    income = (financials.get("income") or [{}])[0]
    balance = (financials.get("balance") or [{}])[0]
    cash = (financials.get("cashFlow") or [{}])[0]

    def pick(row: dict, key: str) -> Optional[Any]:
        return row.get(key) if isinstance(row, dict) else None

    candidates = [
        ("Revenue", pick(income, "revenue")),
        ("Net Income", pick(income, "netIncome")),
        ("EPS", pick(income, "eps")),
        ("Gross Profit", pick(income, "grossProfit")),
        ("Total Assets", pick(balance, "totalAssets")),
        ("Total Liabilities", pick(balance, "totalLiabilities")),
        ("Cash & Equivalents", pick(balance, "cashAndCashEquivalents")),
        ("Operating Cash Flow", pick(cash, "operatingCashFlow")),
        ("Free Cash Flow", pick(cash, "freeCashFlow")),
    ]

    facts: List[Dict[str, Any]] = []
    for metric, value in candidates:
        if value is None or value == "":
            continue
        facts.append(
            {
                "ticker": ticker,
                "quarter": quarter_label,
                "type": "Result",
                "metric": metric,
                "value": value,
                "reason": "Latest quarter financial snapshot",
            }
            )
    return facts


def ingest_recent_history_into_neo4j(context: Dict[str, Any], max_quarters: int = 4) -> None:
    """
    Best-effort: 抓取同一 ticker 過去幾季的 transcript，寫入 Neo4j，讓歷史型 Agent 有可用 Facts。
    """
    symbol = (context.get("symbol") or context.get("ticker") or "").upper()
    year = context.get("year")
    quarter = context.get("quarter")
    if not symbol or not year or not quarter:
        raise Neo4jIngestError("context 缺少必填欄位：symbol、year、quarter。")

    repo_path = _resolve_repo_path()
    _ensure_sys_path(repo_path)
    cred_path = _credentials_path(repo_path)

    from utils.indexFacts import IndexFacts
    from fmp_client import get_transcript_dates, get_transcript

    def _q_key(y: int, q: int) -> tuple[int, int]:
        return (int(y), int(q))

    def _has_facts(driver, ticker: str, quarter_label: str) -> bool:
        try:
            with driver.session() as ses:
                rec = ses.run(
                    "MATCH (f:Fact {ticker:$t, quarter:$q}) RETURN COUNT(f) AS cnt",
                    {"t": ticker, "q": quarter_label},
                ).single()
                return bool(rec and rec.get("cnt", 0) > 0)
        except Exception:
            return False

    try:
        all_dates = get_transcript_dates(symbol)
    except Exception as exc:  # noqa: BLE001
        raise Neo4jIngestError(f"取得過往 transcript 日期失敗：{exc}") from exc

    if not all_dates:
        return

    # 挑出目標季度之前的資料，取最近 max_quarters 筆
    past = []
    for item in all_dates:
        try:
            fy = int(item.get("year") or item.get("fiscalYear") or item.get("calendar_year"))
            fq = int(item.get("quarter") or item.get("fiscalQuarter") or item.get("calendar_quarter"))
        except Exception:
            continue
        if fy < year or (fy == year and fq < quarter):
            past.append({"year": fy, "quarter": fq})
    past_sorted = sorted(past, key=lambda x: _q_key(x["year"], x["quarter"]), reverse=True)[:max_quarters]

    if not past_sorted:
        return

    idx = IndexFacts(credentials_file=str(cred_path), prefer_openai=False)
    try:
        for pq in past_sorted:
            q_label = f"{pq['year']}-Q{pq['quarter']}"
            if _has_facts(idx.driver, symbol, q_label):
                continue
            try:
                t = get_transcript(symbol, pq["year"], pq["quarter"])
                transcript_text = t.get("content") or ""
                if not transcript_text.strip():
                    continue
                facts = idx.extract_facts_with_context(transcript_text, symbol, q_label)
                if not facts:
                    continue
                triples = idx._to_triples(facts, symbol, q_label)
                idx._push(triples)
            except Exception as exc:  # noqa: BLE001
                logger.warning("略過歷史匯入 %s %s：%s", symbol, q_label, exc)
                continue
    finally:
        try:
            idx.driver.close()
        except Exception:
            pass


def ingest_context_into_neo4j(context: Dict[str, Any]) -> None:
    """
    On-demand ingestion: extract transcript facts and minimal financial facts into Neo4j.
    This gives the helper agents data to work with without offline batch runs.
    """
    repo_path = _resolve_repo_path()
    _ensure_sys_path(repo_path)
    cred_path = _credentials_path(repo_path)

    try:
        from utils.indexFacts import IndexFacts
    except Exception as exc:  # noqa: BLE001
        raise Neo4jIngestError(f"匯入 IndexFacts 失敗：{exc}") from exc

    symbol = (context.get("symbol") or context.get("ticker") or "").upper()
    year = context.get("year")
    quarter = context.get("quarter")
    transcript_text = context.get("transcript_text") or ""

    if not symbol or not year or not quarter:
        raise Neo4jIngestError("context 缺少必填欄位：symbol、year、quarter。")

    quarter_label = f"{year}-Q{quarter}"

    idx = IndexFacts(credentials_file=str(cred_path), prefer_openai=False)
    try:
        facts: List[Dict[str, Any]] = []
        if transcript_text:
            facts.extend(idx.extract_facts_with_context(transcript_text, symbol, quarter_label))

        facts.extend(_financial_facts(context.get("financials"), symbol, quarter_label))

        if facts:
            triples = idx._to_triples(facts, symbol, quarter_label)
            idx._push(triples)
    finally:
        try:
            idx.driver.close()
        except Exception:
            pass
