"""
Analysis Results Storage
========================
Stores analysis results and caches.
Uses PostgreSQL connection pool from pg_client when available,
falls back to SQLite for local development.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DateAwareEncoder(json.JSONEncoder):
    """JSON encoder that handles date, datetime, and Decimal objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _get_db_config():
    """Lazy initialization of DB config to allow dotenv to load first."""
    pg_dsn = os.getenv("POSTGRES_DSN")
    if pg_dsn:
        return "postgres", pg_dsn, None
    db_path = Path(os.getenv("ANALYSIS_DB_PATH", "data/analysis_results.db"))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return "sqlite", None, db_path


def _db_kind():
    """Return current db kind (postgres or sqlite)."""
    return _get_db_config()[0]


# Thread-safe initialization
_init_lock = threading.Lock()
_db_initialized = False


@contextmanager
def _get_conn():
    """Get database connection using pg_client pool for PostgreSQL."""
    db_kind, pg_dsn, db_path = _get_db_config()

    if db_kind == "postgres":
        # Use pg_client's connection pool
        try:
            from pg_client import _get_pool
            pool = _get_pool()
            conn = pool.getconn()
            conn.autocommit = True
            try:
                yield conn
            finally:
                pool.putconn(conn)
            return
        except ImportError:
            # Fallback to direct connection if pg_client not available
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(pg_dsn, cursor_factory=RealDictCursor)
            conn.autocommit = True
            try:
                yield conn
            finally:
                conn.close()
            return

    # SQLite fallback
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
    finally:
        conn.close()


def _prepare_sql(sql: str) -> str:
    if _db_kind() == "postgres":
        return sql.replace("?", "%s")
    return sql


def _fetchall(cursor):
    rows = cursor.fetchall()
    if _db_kind() == "postgres":
        # Handle both RealDictCursor (list of dict) and regular cursor (list of tuple)
        if rows and isinstance(rows[0], dict):
            return rows
        # Convert tuples to dicts using cursor description
        if rows and cursor.description:
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        return rows
    return [dict(r) for r in rows]


def _fetchone(cursor):
    row = cursor.fetchone()
    if row is None:
        return None
    if _db_kind() == "postgres":
        # Handle both RealDictCursor (dict) and regular cursor (tuple)
        if isinstance(row, dict):
            return row
        # Convert tuple to dict using cursor description
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            return dict(zip(columns, row))
        return row
    return dict(row) if row else None


def _created_column_def() -> str:
    if _db_kind() == "postgres":
        return "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    return "TEXT DEFAULT (datetime('now'))"


def _ensure_prompts_table(cursor):
    """Ensure prompt_configs table exists for prompt overrides."""
    if _db_kind() == "postgres":
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_configs (
                key TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    else:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_configs (
                key TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def _to_utc(dt_value: Any) -> Optional[datetime]:
    """
    Normalize datetime/string to UTC-aware datetime for comparisons.
    """
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        if dt_value.tzinfo is None:
            return dt_value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc)
    if isinstance(dt_value, str):
        try:
            parsed = datetime.fromisoformat(dt_value)
        except Exception:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def init_db() -> None:
    global _db_initialized
    # Fast path: skip if already initialized
    if _db_initialized:
        return
    with _init_lock:
        # Double-check after acquiring lock
        if _db_initialized:
            return
        created_col = _created_column_def()
        correct_type = "BOOLEAN" if _db_kind() == "postgres" else "INTEGER"
        with _get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS call_results (
                    job_id TEXT PRIMARY KEY,
                    symbol TEXT,
                    company TEXT,
                    fiscal_year INTEGER,
                    fiscal_quarter INTEGER,
                    call_date TEXT,
                    sector TEXT,
                    exchange TEXT,
                    post_return REAL,
                    prediction TEXT,
                    confidence REAL,
                    correct {correct_type},
                    agent_result_json TEXT,
                    token_usage_json TEXT,
                    agent_notes TEXT,
                    created_at {created_col}
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_call_date ON call_results(call_date)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON call_results(symbol)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_prediction ON call_results(prediction)")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS call_cache (
                    symbol TEXT NOT NULL,
                    fiscal_year INTEGER NOT NULL,
                    fiscal_quarter INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at {created_col},
                    PRIMARY KEY (symbol, fiscal_year, fiscal_quarter)
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS fmp_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    created_at {created_col}
                )
                """
            )
            _ensure_prompts_table(cur)
        _db_initialized = True


def ensure_db_writable() -> None:
    """
    Ensure the DB directory exists and is writable; raise if not.
    """
    if _db_kind() == "postgres":
        # Managed externally by Postgres.
        return
    try:
        _, _, db_path = _get_db_config()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path.touch(exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.error("DB path not writable: %s", exc)
        raise


def record_analysis(
    job_id: str,
    symbol: str,
    fiscal_year: Optional[int],
    fiscal_quarter: Optional[int],
    call_date: Optional[str],
    sector: Optional[str],
    exchange: Optional[str],
    post_return: Optional[float],
    prediction: Optional[str],
    confidence: Optional[float],
    correct: Optional[bool],
    agent_result: Dict[str, Any],
    token_usage: Optional[Dict[str, Any]],
    agent_notes: Optional[str],
    company: Optional[str] = None,
) -> None:
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute(
                """
                INSERT INTO call_results (
                    job_id, symbol, company, fiscal_year, fiscal_quarter, call_date, sector, exchange,
                    post_return, prediction, confidence, correct, agent_result_json, token_usage_json, agent_notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    company = EXCLUDED.company,
                    fiscal_year = EXCLUDED.fiscal_year,
                    fiscal_quarter = EXCLUDED.fiscal_quarter,
                    call_date = EXCLUDED.call_date,
                    sector = EXCLUDED.sector,
                    exchange = EXCLUDED.exchange,
                    post_return = EXCLUDED.post_return,
                    prediction = EXCLUDED.prediction,
                    confidence = EXCLUDED.confidence,
                    correct = EXCLUDED.correct,
                    agent_result_json = EXCLUDED.agent_result_json,
                    token_usage_json = EXCLUDED.token_usage_json,
                    agent_notes = EXCLUDED.agent_notes,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    job_id,
                    symbol,
                    company,
                    fiscal_year,
                    fiscal_quarter,
                    call_date,
                    sector,
                    exchange,
                    post_return,
                    prediction,
                    confidence,
                    correct,
                    json.dumps(agent_result or {}),
                    json.dumps(token_usage or {}),
                    agent_notes or "",
                ),
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO call_results (
                    job_id, symbol, company, fiscal_year, fiscal_quarter, call_date, sector, exchange,
                    post_return, prediction, confidence, correct, agent_result_json, token_usage_json, agent_notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    symbol,
                    company,
                    fiscal_year,
                    fiscal_quarter,
                    call_date,
                    sector,
                    exchange,
                    post_return,
                    prediction,
                    confidence,
                    1 if correct else 0 if correct is not None else None,
                    json.dumps(agent_result or {}),
                    json.dumps(token_usage or {}),
                    agent_notes or "",
                ),
            )


def list_calls(
    symbol: Optional[str] = None,
    sector: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    ret_min: Optional[float] = None,
    ret_max: Optional[float] = None,
    prediction: Optional[str] = None,
    correct: Optional[bool] = None,
    sort: str = "date_desc",
    limit: int = 50,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    init_db()
    where: List[str] = []
    params: List[Any] = []

    def add_clause(cond: str, val: Any):
        where.append(cond)
        params.append(val)

    if symbol:
        add_clause("symbol = ?", symbol.upper())
    if sector:
        add_clause("sector = ?", sector)
    if date_from:
        add_clause("call_date >= ?", date_from)
    if date_to:
        add_clause("call_date <= ?", date_to)
    if ret_min is not None:
        add_clause("post_return >= ?", ret_min)
    if ret_max is not None:
        add_clause("post_return <= ?", ret_max)
    if prediction:
        add_clause("prediction = ?", prediction.upper())
    if correct is not None:
        add_clause("correct = ?", 1 if correct else 0)

    order_by = {
        "date_desc": "call_date DESC",
        "date_asc": "call_date ASC",
        "abs_return_desc": "ABS(post_return) DESC",
        "return_desc": "post_return DESC",
        "conf_desc": "confidence DESC",
    }.get(sort, "call_date DESC")

    sql = "SELECT job_id, symbol, company, fiscal_year, fiscal_quarter, call_date, sector, exchange, post_return, prediction, confidence, correct FROM call_results"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {order_by} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(_prepare_sql(sql), params)
        rows = _fetchall(cur)
    return [dict(r) for r in rows]


def get_call(job_id: str) -> Optional[Dict[str, Any]]:
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(_prepare_sql("SELECT * FROM call_results WHERE job_id = ?"), (job_id,))
        row = _fetchone(cur)
    if not row:
        return None
    data = dict(row)
    data["agent_result"] = json.loads(data.pop("agent_result_json") or "{}")
    data["token_usage"] = json.loads(data.pop("token_usage_json") or "{}")
    return data


def get_cached_payload(symbol: str, fiscal_year: int, fiscal_quarter: int, max_age_minutes: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Return cached full analysis payload for the given symbol/year/quarter if present.
    """
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            _prepare_sql(
                "SELECT payload_json, created_at FROM call_cache WHERE symbol=? AND fiscal_year=? AND fiscal_quarter=?"
            ),
            (symbol.upper(), fiscal_year, fiscal_quarter),
        )
        row = _fetchone(cur)
    if not row:
        return None
    if max_age_minutes is not None:
        created = _to_utc(row.get("created_at"))
        if created:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
            if created < cutoff:
                return None
    try:
        return json.loads(row["payload_json"])
    except Exception:
        return None


def set_cached_payload(symbol: str, fiscal_year: int, fiscal_quarter: int, payload: Dict[str, Any]) -> None:
    """
    Store the full analysis payload for reuse.
    """
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute(
                """
                INSERT INTO call_cache (symbol, fiscal_year, fiscal_quarter, payload_json)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol, fiscal_year, fiscal_quarter)
                DO UPDATE SET payload_json = EXCLUDED.payload_json, created_at = CURRENT_TIMESTAMP
                """,
                (symbol.upper(), fiscal_year, fiscal_quarter, json.dumps(payload or {}, cls=DateAwareEncoder)),
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO call_cache (symbol, fiscal_year, fiscal_quarter, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (symbol.upper(), fiscal_year, fiscal_quarter, json.dumps(payload or {}, cls=DateAwareEncoder)),
            )


def get_fmp_cache(cache_key: str, max_age_minutes: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch cached FMP payload by key. If max_age_minutes is provided, skip stale entries.
    """
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            _prepare_sql(
                "SELECT payload_json, created_at FROM fmp_cache WHERE cache_key=?"
            ),
            (cache_key,),
        )
        row = _fetchone(cur)
    if not row:
        return None
    if max_age_minutes is not None:
        created = _to_utc(row.get("created_at"))
        if created:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
            if created < cutoff:
                return None
    try:
        return json.loads(row["payload_json"])
    except Exception:
        return None


def set_fmp_cache(cache_key: str, payload: Dict[str, Any]) -> None:
    """
    Store FMP payload by key.
    """
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute(
                """
                INSERT INTO fmp_cache (cache_key, payload_json)
                VALUES (%s, %s)
                ON CONFLICT (cache_key)
                DO UPDATE SET payload_json = EXCLUDED.payload_json, created_at = CURRENT_TIMESTAMP
                """,
                (cache_key, json.dumps(payload or {})),
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO fmp_cache (cache_key, payload_json)
                VALUES (?, ?)
                """,
                (cache_key, json.dumps(payload or {})),
            )


def get_all_prompts() -> Dict[str, str]:
    """Return all prompt overrides."""
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT key, content FROM prompt_configs")
        rows = _fetchall(cur)
    return {row["key"]: row["content"] for row in rows}


def get_prompt(key: str) -> Optional[str]:
    """Return a prompt override by key, or None."""
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute("SELECT content FROM prompt_configs WHERE key = %s", (key,))
        else:
            cur.execute("SELECT content FROM prompt_configs WHERE key = ?", (key,))
        row = _fetchone(cur)
    return row["content"] if row else None


def set_prompt(key: str, content: str) -> None:
    """Upsert a prompt override."""
    init_db()
    now_iso = datetime.utcnow().isoformat()
    with _get_conn() as conn:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute(
                """
                INSERT INTO prompt_configs(key, content, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE
                SET content = EXCLUDED.content,
                    updated_at = NOW()
                """,
                (key, content),
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO prompt_configs(key, content, updated_at)
                VALUES (?, ?, ?)
                """,
                (key, content, now_iso),
            )


def delete_prompt(key: str) -> None:
    """Delete a prompt override, effectively resetting to default."""
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        if _db_kind() == "postgres":
            cur.execute("DELETE FROM prompt_configs WHERE key = %s", (key,))
        else:
            cur.execute("DELETE FROM prompt_configs WHERE key = ?", (key,))


# ============================================================================
# PROMPT PROFILES
# ============================================================================


def _ensure_profile_table(cursor) -> None:
    """Ensure prompt_profiles table exists for storing profile sets."""
    if _db_kind() == "postgres":
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_profiles (
                name TEXT PRIMARY KEY,
                prompts_json TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    else:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_profiles (
                name TEXT PRIMARY KEY,
                prompts_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def list_prompt_profiles() -> List[Dict[str, Any]]:
    """Return all profile names with updated_at timestamps."""
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        _ensure_profile_table(cur)
        cur.execute("SELECT name, updated_at FROM prompt_profiles ORDER BY name")
        rows = _fetchall(cur)
    return [{"name": row["name"], "updated_at": row["updated_at"]} for row in rows]


def get_prompt_profile(name: str) -> Optional[Dict[str, Any]]:
    """Return a profile by name, including its prompts dict."""
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        _ensure_profile_table(cur)
        if _db_kind() == "postgres":
            cur.execute(
                "SELECT name, prompts_json, updated_at FROM prompt_profiles WHERE name = %s",
                (name,),
            )
        else:
            cur.execute(
                "SELECT name, prompts_json, updated_at FROM prompt_profiles WHERE name = ?",
                (name,),
            )
        row = _fetchone(cur)
    if not row:
        return None
    try:
        prompts = json.loads(row["prompts_json"])
    except Exception:
        prompts = {}
    return {
        "name": row["name"],
        "prompts": prompts,
        "updated_at": row["updated_at"],
    }


def set_prompt_profile(name: str, prompts: Dict[str, str]) -> None:
    """Upsert a prompt profile."""
    init_db()
    now_iso = datetime.utcnow().isoformat()
    prompts_json = json.dumps(prompts or {})
    with _get_conn() as conn:
        cur = conn.cursor()
        _ensure_profile_table(cur)
        if _db_kind() == "postgres":
            cur.execute(
                """
                INSERT INTO prompt_profiles(name, prompts_json, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (name) DO UPDATE
                SET prompts_json = EXCLUDED.prompts_json,
                    updated_at = NOW()
                """,
                (name, prompts_json),
            )
        else:
            cur.execute(
                """
                INSERT OR REPLACE INTO prompt_profiles(name, prompts_json, updated_at)
                VALUES (?, ?, ?)
                """,
                (name, prompts_json, now_iso),
            )


def delete_prompt_profile(name: str) -> None:
    """Delete a prompt profile."""
    init_db()
    with _get_conn() as conn:
        cur = conn.cursor()
        _ensure_profile_table(cur)
        if _db_kind() == "postgres":
            cur.execute("DELETE FROM prompt_profiles WHERE name = %s", (name,))
        else:
            cur.execute("DELETE FROM prompt_profiles WHERE name = ?", (name,))
