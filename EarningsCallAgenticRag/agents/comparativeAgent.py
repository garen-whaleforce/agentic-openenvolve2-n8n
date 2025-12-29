"""comparative_agent.py – Batch‑aware ComparativeAgent
======================================================
This rewrite lets `run()` accept a **list of fact‑dicts** (rather than one) so
all related facts can be analysed in a single LLM prompt.
• **Added:** Token usage tracking for cost monitoring
• **Updated:** PostgreSQL DB first, Neo4j fallback for peer data
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)

from agents.prompts.prompts import get_comparative_system_message, comparative_agent_prompt
from utils.llm import build_chat_client, build_embeddings
from utils.token_tracker import TokenTracker
from utils.neo4j_utils import get_neo4j_driver
from utils.config import (
    MAX_FACTS_FOR_PEERS,
    MAX_PEER_FACTS,
    HELPER_MODEL as DEFAULT_HELPER_MODEL,
    MIN_SIMILARITY_SCORE,
    PEER_SCORE_WEIGHTS,
)

# Add parent directory to path for pg_client import
_parent = Path(__file__).resolve().parent.parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))


class ComparativeAgent:
    """Compare a batch of facts against peer data stored in Neo4j or AWS DB."""

    def __init__(
        self,
        credentials_file: str = "credentials.json",
        model: str = DEFAULT_HELPER_MODEL,
        sector_map: dict = None,
        temperature: float = 0.7,
        **kwargs,
    ) -> None:
        self._creds = json.loads(Path(credentials_file).read_text())
        self.client, resolved_model = build_chat_client(self._creds, model)
        self.model = resolved_model
        self.temperature = temperature
        # Lazy-loaded Neo4j driver and embedder
        self._driver: "Driver" | None = None
        self._embedder = None
        self.token_tracker = TokenTracker()
        self.sector_map = sector_map or {}

    @property
    def driver(self) -> "Driver":
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
    # PostgreSQL peer lookup (primary source)
    # ------------------------------------------------------------------
    def _get_peer_facts_from_pg(
        self,
        ticker: str,
        quarter: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get peer facts from PostgreSQL DB as primary data source.

        Returns list of dicts compatible with Neo4j search results format:
        - metric, value, reason, ticker, quarter, score
        """
        try:
            from pg_client import get_peer_facts_summary, is_available

            if not is_available():
                logger.info("PostgreSQL DB not available, will use Neo4j")
                return []

            peer_data = get_peer_facts_summary(ticker, quarter, limit=limit)
            if not peer_data:
                logger.debug("No peer data in PostgreSQL for %s/%s", ticker, quarter)
                return []

            # Convert PostgreSQL format to expected format
            results = []
            for peer in peer_data:
                # Create fact-like entries from financial metrics
                if peer.get("revenue"):
                    results.append({
                        "metric": "Revenue",
                        "value": f"${peer['revenue']:,.0f}" if peer['revenue'] else "N/A",
                        "reason": f"{peer['name']} ({peer['symbol']}) sector: {peer.get('sector', 'N/A')}",
                        "ticker": peer["symbol"],
                        "quarter": quarter,
                        "score": PEER_SCORE_WEIGHTS["revenue"],
                    })
                if peer.get("net_income"):
                    results.append({
                        "metric": "Net Income",
                        "value": f"${peer['net_income']:,.0f}" if peer['net_income'] else "N/A",
                        "reason": f"{peer['name']} ({peer['symbol']})",
                        "ticker": peer["symbol"],
                        "quarter": quarter,
                        "score": PEER_SCORE_WEIGHTS["net_income"],
                    })
                if peer.get("eps"):
                    results.append({
                        "metric": "EPS",
                        "value": f"${peer['eps']:.2f}" if peer['eps'] else "N/A",
                        "reason": f"{peer['name']} ({peer['symbol']})",
                        "ticker": peer["symbol"],
                        "quarter": quarter,
                        "score": PEER_SCORE_WEIGHTS["eps"],
                    })
                if peer.get("revenue_growth"):
                    results.append({
                        "metric": "Revenue Growth",
                        "value": f"{peer['revenue_growth']:.1%}" if peer['revenue_growth'] else "N/A",
                        "reason": f"{peer['name']} ({peer['symbol']})",
                        "ticker": peer["symbol"],
                        "quarter": quarter,
                        "score": PEER_SCORE_WEIGHTS["revenue_growth"],
                    })
                if peer.get("earnings_day_return") is not None:
                    results.append({
                        "metric": "Earnings Day Return",
                        "value": f"{peer['earnings_day_return']:.2f}%",
                        "reason": f"{peer['name']} post-earnings performance",
                        "ticker": peer["symbol"],
                        "quarter": quarter,
                        "score": PEER_SCORE_WEIGHTS["earnings_day_return"],
                    })

            logger.debug("Got %d peer facts from PostgreSQL for %s/%s", len(results), ticker, quarter)
            return results

        except ImportError:
            logger.warning("pg_client not available")
            return []
        except Exception as e:
            logger.error("PostgreSQL DB error: %s", e)
            return []

    # ------------------------------------------------------------------
    # Vector search helper
    # ------------------------------------------------------------------
    def _search_similar(
        self,
        query: str,
        exclude_ticker: str,
        top_k: int = 10,
        sector: str | None = None,
        ticker: str | None = None,
        peers: Sequence[str] | None = None,
        use_batch_peer_query: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        If sector_map is provided, run the query for every ticker in the same sector (excluding exclude_ticker).
        If sector is not provided, infer it from ticker using the sector_map.
        Otherwise, default to original behavior.
        If use_batch_peer_query is True, use a single query with IN $peer_ticker_list.
        """
        tickers_in_sector = None
        exclude_upper = exclude_ticker.upper()
        if self.sector_map:
            # Infer sector if not provided
            if not sector and ticker:
                sector = self.sector_map.get(ticker)
            if sector:
                # Get all tickers in the same sector
                tickers_in_sector = [t for t, s in self.sector_map.items() if s == sector and t != exclude_upper]
        
        peer_candidates: list[str] = []
        if peers:
            seen_peers = set()
            for peer in peers:
                sym = str(peer).upper().strip()
                if not sym or sym == exclude_upper:
                    continue
                if sym not in seen_peers:
                    seen_peers.add(sym)
                    peer_candidates.append(sym)
            if tickers_in_sector:
                filtered = [p for p in peer_candidates if p in tickers_in_sector]
                if filtered:
                    peer_candidates = filtered

        try:
            vec = self.embedder.embed_query(query)
            with self.driver.session() as ses:
                all_results = []
                if peer_candidates:
                    if use_batch_peer_query or len(peer_candidates) > 1:
                        res = ses.run(
                            """
                            CALL db.index.vector.queryNodes('fact_index', $topK, $vec)
                            YIELD node, score
                            WHERE node.ticker IN $peer_ticker_list AND score > $min_score
                            OPTIONAL MATCH (node)-[:HAS_VALUE]->(v:Value)
                            OPTIONAL MATCH (node)-[:EXPLAINED_BY]->(r:Reason)
                            RETURN node.text AS text, node.metric AS metric, v.content AS value,
                                   r.content AS reason, node.ticker AS ticker,
                                   node.quarter AS quarter, score
                            ORDER BY score DESC
                            LIMIT 10
                            """,
                            {"topK": top_k, "vec": vec, "peer_ticker_list": peer_candidates, "min_score": MIN_SIMILARITY_SCORE},
                        )
                        all_results.extend([dict(r) for r in res])
                    else:
                        for peer_ticker in peer_candidates:
                            res = ses.run(
                                """
                                CALL db.index.vector.queryNodes('fact_index', $topK, $vec)
                                YIELD node, score
                                WHERE node.ticker = $peer_ticker AND score > $min_score
                                OPTIONAL MATCH (node)-[:HAS_VALUE]->(v:Value)
                                OPTIONAL MATCH (node)-[:EXPLAINED_BY]->(r:Reason)
                                RETURN node.text AS text, node.metric AS metric, v.content AS value,
                                       r.content AS reason, node.ticker AS ticker,
                                       node.quarter AS quarter, score
                                ORDER BY score DESC
                                LIMIT 10
                                """,
                                {"topK": top_k, "vec": vec, "peer_ticker": peer_ticker, "min_score": MIN_SIMILARITY_SCORE},
                            )
                            all_results.extend([dict(r) for r in res])
                    all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
                    return all_results

                if tickers_in_sector:
                    # Always use batch query for sector-based searches to avoid N individual queries
                    res = ses.run(
                        """
                        CALL db.index.vector.queryNodes('fact_index', $topK, $vec)
                        YIELD node, score
                        WHERE node.ticker IN $peer_ticker_list AND score > $min_score
                        OPTIONAL MATCH (node)-[:HAS_VALUE]->(v:Value)
                        OPTIONAL MATCH (node)-[:EXPLAINED_BY]->(r:Reason)
                        RETURN node.text AS text, node.metric AS metric, v.content AS value,
                               r.content AS reason, node.ticker AS ticker,
                               node.quarter AS quarter, score
                        ORDER BY score DESC
                        LIMIT 10
                        """,
                        {"topK": top_k, "vec": vec, "peer_ticker_list": tickers_in_sector, "min_score": MIN_SIMILARITY_SCORE},
                    )
                    all_results.extend([dict(r) for r in res])
                    all_results.sort(key=lambda x: x.get('score', 0), reverse=True)
                    return all_results

                else:
                    res = ses.run(
                        """
                        CALL db.index.vector.queryNodes('fact_index', $topK, $vec)
                        YIELD node, score
                        WHERE node.ticker <> $exclude_ticker AND score > $min_score
                        OPTIONAL MATCH (node)-[:HAS_VALUE]->(v:Value)
                        OPTIONAL MATCH (node)-[:EXPLAINED_BY]->(r:Reason)
                        RETURN node.text AS text, node.metric AS metric, v.content AS value,
                               r.content AS reason, node.ticker AS ticker,
                               node.quarter AS quarter, score
                        ORDER BY score DESC
                        LIMIT 10
                        """,
                        {"topK": top_k, "vec": vec, "exclude_ticker": exclude_upper, "min_score": MIN_SIMILARITY_SCORE},
                    )
                return [dict(r) for r in res]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Vector search helper for sector peers
    # ------------------------------------------------------------------
    def _search_similar_sector(self, query: str, sector: str, quarter: str, exclude_ticker: str, top_k: int = 10) -> List[Dict[str, Any]]:
        try:
            vec = self.embedder.embed_query(query)
            with self.driver.session() as ses:
                res = ses.run(
                    """
                    CALL db.index.vector.queryNodes('fact_index', $topK, $vec)
                    YIELD node, score
                    WHERE node.sector = $sector AND node.quarter = $quarter AND node.ticker <> $exclude_ticker
                    OPTIONAL MATCH (node)-[:HAS_VALUE]->(v:Value)
                    OPTIONAL MATCH (node)-[:EXPLAINED_BY]->(r:Reason)
                    RETURN node.text AS text, node.metric AS metric, v.content AS value,
                           r.content AS reason, node.ticker AS ticker,
                           node.quarter AS quarter, node.sector AS sector, score
                    ORDER BY score DESC
                    LIMIT 10
                    """,
                    {"topK": top_k, "vec": vec, "sector": sector, "quarter": quarter, "exclude_ticker": exclude_ticker},
                )
                return [dict(r) for r in res]
        except Exception:
            return []

    # ------------------------------------------------------------------
    def _to_query(self, fact: Dict[str, str]) -> str:
        parts = []
        if fact.get("metric"):
            parts.append(f"Metric: {fact['metric']}")
        if fact.get("value"):
            parts.append(f"Value: {fact['value']}")
        if fact.get("context"):
            parts.append(f"Reason: {fact['context']}")
        return " | ".join(parts)

    # ------------------------------------------------------------------
    def run(
        self,
        facts: List[Dict[str, str]],
        ticker: str,
        quarter: str,
        peers: list[str] | None = None,
        sector: str | None = None,
        top_k: int = 8,  # Lowered from 50 to 10
    ) -> str:
        """Analyse a batch of facts; return one consolidated LLM answer.

        Data source priority:
        1. PostgreSQL DB (primary) - faster, no embedding cost
        2. Neo4j vector search (fallback) - if PostgreSQL has no data
        """
        facts = list(facts)[:MAX_FACTS_FOR_PEERS]
        if not facts:
            return "No facts supplied."
        # Reset token tracker for this run
        self.token_tracker = TokenTracker()
        peers_len = len(peers) if peers else 0

        # --- Step 1: Try PostgreSQL DB first (primary source) ---
        deduped_similar = self._get_peer_facts_from_pg(ticker, quarter, limit=10)

        # --- Step 2: Fallback to Neo4j if PostgreSQL has no data ---
        if not deduped_similar:
            logger.info("Falling back to Neo4j for %s/%s", ticker, quarter)
            all_similar = []
            for fact in facts:
                query = self._to_query(fact)
                similar = self._search_similar(
                    query,
                    ticker,
                    top_k=top_k,
                    sector=sector,
                    peers=peers,
                    use_batch_peer_query=bool(peers),
                )
                # Optionally, attach the current metric for context
                for sim in similar:
                    sim["current_metric"] = fact.get("metric", "")
                all_similar.extend(similar)

            # Deduplicate similar facts (by metric, value, ticker, quarter)
            seen = set()
            deduped_similar = []
            for sim in all_similar:
                key = (sim.get("metric"), sim.get("value"), sim.get("ticker"), sim.get("quarter"))
                if key not in seen:
                    deduped_similar.append(sim)
                    seen.add(key)

        logger.debug("ComparativeAgent.run peers len=%d, related_facts len=%d", peers_len, len(deduped_similar))
        deduped_similar = deduped_similar[:MAX_PEER_FACTS]
        if not deduped_similar:
            return None

        # --- Craft prompt --------------------------------------------------
        # NOTE: facts are already included in comparative_agent_prompt via {{facts}} placeholder
        # Removed duplicate json.dumps(facts) to save tokens
        prompt = comparative_agent_prompt(facts, deduped_similar, self_ticker=ticker)
        
        # Print the full prompt for debugging
        #print(f"\n{'='*80}")
        #print(f"COMPARATIVE AGENT PROMPT for {ticker}/{quarter}")
        #print(f"{'='*80}")
        #print(prompt)
        #print(f"{'='*80}\n")

        try:
            # GPT-5 models only support temperature=1; others use configured temperature
            kwargs = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": get_comparative_system_message()},
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
        except Exception as exc:
            return f"❌ ComparativeAgent error: {exc}"

    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
