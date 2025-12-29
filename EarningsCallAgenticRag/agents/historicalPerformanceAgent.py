"""historical_performance_agent.py – Batch‑aware version
=======================================================
`run()` now accepts a **list of facts** so the model can compare all of them to
past financial statements in a single prompt.
• **Added:** Token usage tracking for cost monitoring
• **Updated:** PostgreSQL DB first, Neo4j fallback for historical data
• **Updated:** Neo4j import is now lazy-loaded (only when needed)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import numpy as np

# Lazy import Neo4j - only when actually needed
if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

from agents.prompts.prompts import get_financials_system_message, financials_statement_agent_prompt
from utils.llm import build_chat_client, build_embeddings
from utils.token_tracker import TokenTracker
from utils.neo4j_utils import get_neo4j_driver
from utils.config import (
    MAX_FACTS_FOR_FINANCIALS,
    MAX_FINANCIAL_FACTS,
    HELPER_MODEL as DEFAULT_HELPER_MODEL,
    MIN_SIMILARITY_SCORE,
)

# Add parent directory to path for pg_client import
_parent = Path(__file__).resolve().parent.parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))


class HistoricalPerformanceAgent:
    """Compare current-quarter facts with prior financial statements."""

    def __init__(
        self,
        credentials_file: str = "credentials.json",
        model: str = DEFAULT_HELPER_MODEL,
        temperature: float = 0.7,
    ) -> None:
        creds = json.loads(Path(credentials_file).read_text())
        self.client, resolved_model = build_chat_client(creds, model)
        self.model = resolved_model
        self.temperature = temperature
        self.credentials_file = credentials_file
        self._creds = creds
        self._driver = None  # Lazy initialized
        self._embedder = None  # Lazy initialized
        self.token_tracker = TokenTracker()

    @property
    def driver(self):
        """Lazy load Neo4j driver only when needed."""
        if self._driver is None:
            self._driver = get_neo4j_driver(self._creds)
        return self._driver

    @property
    def embedder(self):
        """Lazy load embedder only when needed."""
        if self._embedder is None:
            self._embedder = build_embeddings(self._creds)
        return self._embedder

    # ------------------------------------------------------------------
    # PostgreSQL historical data lookup (primary source)
    # ------------------------------------------------------------------
    def _get_historical_facts_from_pg(
        self,
        ticker: str,
        quarter: str,
        num_quarters: int = 8
    ) -> List[Dict[str, Any]]:
        """Get historical financial facts from PostgreSQL DB as primary data source.

        Returns list of fact dicts compatible with Neo4j format.
        """
        try:
            from pg_client import get_historical_financials_facts, is_available

            if not is_available():
                logger.info("PostgreSQL DB not available, will use Neo4j")
                return []

            facts = get_historical_financials_facts(ticker, quarter, num_quarters)
            if not facts:
                logger.debug("No historical data in PostgreSQL for %s", ticker)
                return []

            logger.debug("Got %d historical facts from PostgreSQL for %s", len(facts), ticker)
            return facts

        except ImportError:
            logger.warning("pg_client not available")
            return []
        except Exception as e:
            logger.error("PostgreSQL DB error: %s", e)
            return []

    # ------------------------------------------------------------------
    @staticmethod
    def _pretty_json(obj: Any) -> str:
        if obj in (None, "", "[]"):
            return "[]"
        try:
            return json.dumps(json.loads(obj) if isinstance(obj, str) else obj, indent=2)
        except Exception:
            return str(obj)

    # ------------------------------------------------------------------
    def get_similar_facts_by_embedding(self, fact: Dict[str, Any], ticker: str, quarter: str, top_n: int = 5) -> List[Dict[str, Any]]:
        """Find top-N most similar facts for the same ticker and type='Result' using embedding cosine similarity, fetching value via HAS_VALUE."""
        try:
            if isinstance(fact, str):
                return []
            embedding = fact.get("embedding")
            if embedding is None:
                try:
                    text = f"{fact['ticker']} | {fact['metric']} | {fact['type']}"
                except Exception:
                    return []
                try:
                    embedding = self.embedder.embed_query(text)
                except Exception as e:
                    logger.error("Embedding creation failed in get_similar_facts_by_embedding: %s", e)
                    return []
            with self.driver.session() as session:
                try:
                    # Get previous year's quarter for YoY comparison
                    prev_year_quarter = self._get_prev_year_quarter(quarter)
                    
                    result = session.run(
                        """
                        CALL db.index.vector.queryNodes('fact_index', $top_n, $embedding) YIELD node, score
                        WHERE node.ticker = $ticker AND node.type = 'Result' AND score > $min_score
                        OPTIONAL MATCH (node)-[:HAS_VALUE]->(v:Value)
                        OPTIONAL MATCH (node)-[:EXPLAINED_BY]->(r:Reason)
                        RETURN node.metric AS metric, v.content AS value, r.content AS reason, node.embedding AS embedding, node.quarter AS quarter, node.type AS type, score
                        ORDER BY score DESC
                        LIMIT 10
                        """,
                        embedding=embedding,
                        top_n=top_n,
                        ticker=ticker,
                        min_score=MIN_SIMILARITY_SCORE
                    )
                    all_facts = [r.data() for r in result]
                    
                    # Also search specifically for previous year's quarter
                    prev_year_result = session.run(
                        """
                        MATCH (f:Fact {ticker: $ticker, quarter: $prev_year_quarter, type: 'Result'})
                        OPTIONAL MATCH (f)-[:HAS_VALUE]->(v:Value)
                        OPTIONAL MATCH (f)-[:EXPLAINED_BY]->(r:Reason)
                        RETURN f.metric AS metric, v.content AS value, r.content AS reason, f.embedding AS embedding, f.quarter AS quarter, f.type AS type, 1.0 AS score
                        """,
                        ticker=ticker,
                        prev_year_quarter=prev_year_quarter
                    )
                    prev_year_facts = [r.data() for r in prev_year_result]
                    
                    # Combine and filter facts
                    combined_facts = all_facts + prev_year_facts
                    
                    # Keep facts from strictly earlier quarters AND previous year's quarter
                    # Explicitly exclude the current quarter
                    filtered_facts = [
                        f for f in combined_facts
                        if f.get("quarter") and (
                            (self._q_sort_key(f.get("quarter")) < self._q_sort_key(quarter) or
                            f.get("quarter") == prev_year_quarter) and
                            f.get("quarter") != quarter  # Explicitly exclude current quarter
                        )
                    ]
                    
                    return filtered_facts
                except Exception as e:
                    logger.error("Neo4j vector query failed in get_similar_facts_by_embedding: %s", e)
                    # Fallback: fetch all and compute similarity in Python
                    try:
                        result = session.run(
                            """
                            MATCH (f:Fact {ticker: $ticker, type: 'Result'})
                            OPTIONAL MATCH (f)-[:HAS_VALUE]->(v:Value)
                            OPTIONAL MATCH (f)-[:EXPLAINED_BY]->(r:Reason)
                            WHERE exists(f.embedding)
                            RETURN f.metric AS metric, v.content AS value, r.content AS reason, f.embedding AS embedding, f.quarter AS quarter, f.type AS type
                            """,
                            ticker=ticker
                        )
                        all_facts = [r.data() for r in result]

                        def cosine_sim(a, b):
                            a = np.array(a)
                            b = np.array(b)
                            return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

                        for f in all_facts:
                            f["score"] = cosine_sim(embedding, f["embedding"])
                        all_facts.sort(key=lambda x: x["score"], reverse=True)
                        return all_facts[:top_n]
                    except Exception as e2:
                        logger.error("Fallback similarity search failed in get_similar_facts_by_embedding: %s", e2)
                        return []
        except Exception as e:
            logger.error("get_similar_facts_by_embedding failed: %s", e)
            return []

    # Helper for quarter comparison
    @staticmethod
    def _q_sort_key(q: str):
        m = re.match(r"(\d{4})-Q(\d)", q)
        return (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    
    # Helper to get previous year's same quarter
    @staticmethod
    def _get_prev_year_quarter(quarter: str) -> str:
        m = re.match(r"(\d{4})-Q(\d)", quarter)
        if m:
            year, q = int(m.group(1)), int(m.group(2))
            return f"{year-1}-Q{q}"
        return quarter

    def run(self, facts: List[Dict[str, str]], row, quarter, ticker: Optional[str] = None, top_n: int = 5) -> str:  # Lowered from 50 to 10
        """Batch: Compare all facts to all top-N similar past facts.

        Data source priority:
        1. PostgreSQL DB (primary) - faster, no embedding cost
        2. Neo4j vector search (fallback) - if PostgreSQL has no data
        """
        facts = list(facts)[:MAX_FACTS_FOR_FINANCIALS]
        if not facts:
            return "No facts provided."

        # Reset token tracker for this run
        self.token_tracker = TokenTracker()
        resolved_ticker = ticker or row.get("ticker")

        # --- Step 1: Try PostgreSQL DB first (primary source) ---
        all_similar = self._get_historical_facts_from_pg(resolved_ticker, quarter, num_quarters=8)

        # --- Step 2: Fallback to Neo4j if PostgreSQL has no data ---
        if not all_similar:
            logger.info("Falling back to Neo4j for %s/%s", resolved_ticker, quarter)
            for fact in facts:
                similar_facts = self.get_similar_facts_by_embedding(fact, resolved_ticker, quarter, top_n=top_n)
                # Filter out facts with no value or reason, and only keep those from previous quarters
                # Explicitly exclude the current quarter
                filtered_similar = [
                    f for f in similar_facts
                    if f.get("quarter") and (
                        self._q_sort_key(f.get("quarter")) < self._q_sort_key(quarter) and
                        f.get("quarter") != quarter  # Explicitly exclude current quarter
                    )
                ]
                if not filtered_similar:
                    filtered_similar = []
                # Attach the metric of the current fact to each similar fact for context
                for sim in filtered_similar:
                    sim["current_metric"] = fact.get("metric", "")
                    if "embedding" in sim:
                        del sim["embedding"]
                all_similar.extend(filtered_similar)

        logger.debug("HistoricalPerformanceAgent.run historical_facts len=%d", len(all_similar))
        if not all_similar:
            return None
        all_similar = all_similar[:MAX_FINANCIAL_FACTS]

        # Call the prompt ONCE for the batch
        prompt = financials_statement_agent_prompt(
            fact=facts,  # pass the list of facts
            similar_facts=all_similar,  # pass the aggregated similar facts
        )

        # GPT-5 models only support temperature=1; others use configured temperature
        kwargs = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": get_financials_system_message()},
                {"role": "user", "content": prompt},
            ],
        }
        if "gpt-5" not in self.model.lower():
            kwargs["temperature"] = self.temperature

        resp = self.client.chat.completions.create(**kwargs)
        
        # Track token usage
        if hasattr(resp, 'usage') and resp.usage:
            self.token_tracker.add_usage(
                input_tokens=resp.usage.prompt_tokens,
                output_tokens=resp.usage.completion_tokens,
                model=self.model
            )
        
        return resp.choices[0].message.content.strip()

    def generate_embeddings_for_facts(self, batch_size: int = 50):
        """Generate and store embeddings for Fact nodes using ticker, metric, and type as input."""
        creds = json.loads(Path(self.credentials_file).read_text())
        embedder = build_embeddings(creds)

        with self.driver.session() as session:
            # Fetch all Fact nodes missing an embedding
            result = session.run("""
                MATCH (f:Fact)
                WHERE f.embedding IS NULL
                RETURN id(f) AS id, f.ticker AS ticker, f.metric AS metric, f.type AS type
            """)
            facts = [r.data() for r in result]

            for i in range(0, len(facts), batch_size):
                batch = facts[i:i+batch_size]
                texts = [
                    f"{f['ticker']} | {f['metric']} | {f['type']}" for f in batch
                ]
                embeddings = embedder.embed_documents(texts)

                # Write embeddings back to Neo4j
                for fact, emb in zip(batch, embeddings):
                    session.run(
                        """
                        MATCH (f:Fact) WHERE id(f) = $id
                        SET f.embedding = $embedding
                        """,
                        id=fact["id"],
                        embedding=emb
                    )
