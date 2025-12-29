"""main_agent.py – End‑to‑end earnings‑call RAG pipeline
======================================================
• Accepts `credentials_file=` or `credentials_path=` (back‑compat)
• Uses shared prompt templates for extraction, delegation, and verdicts
• **Fix:** reuses the existing OpenAI client instead of re‑instantiating without API key
• Removed duplicate stray code at bottom of file
• **Added:** Token usage tracking for cost monitoring
"""

from __future__ import annotations

import json
import re
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Sequence, Union

# ---- Centralised prompt imports -------------------------------------------
from agents.prompts.prompts import (
    get_extraction_system_message,
    get_main_agent_system_message,
    facts_extraction_prompt,
    main_agent_prompt,
)
from utils.llm import build_chat_client
from utils.token_tracker import TokenTracker
from utils.config import (
    MAX_FACTS_PER_HELPER,
    MAX_PEERS,
    MAX_TOKENS_EXTRACTION,
    MAX_TOKENS_SUMMARY,
)

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
ITEM_HEADER = re.compile(r"### (?:Item|Fact) No\. (\d+)")
FIELD = re.compile(r"\*\*(.+?):\*\*\s*(.+)")

DEFAULT_TRANSCRIPT_CHARS_PER_CHUNK = 8000


def _parse_items(raw: str) -> List[Dict[str, str]]:
    """Convert markdown blocks into structured dicts."""
    pieces = ITEM_HEADER.split(raw)
    out: List[Dict[str, str]] = []
    for i in range(1, len(pieces), 2):
        num = int(pieces[i])
        body = pieces[i + 1]
        fields = {k.strip(): v.strip() for k, v in FIELD.findall(body)}
        out.append(
            {
                "fact_no": num,
                "type": fields.get("Type", ""),
                "metric": fields.get("Metric", ""),
                "value": fields.get("Value", ""),
                "context": fields.get("Context", fields.get("Reason", "")),
            }
        )
    return out

# ---------------------------------------------------------------------------
# Helper‑agent protocol
# ---------------------------------------------------------------------------

class BaseHelperAgent:
    """Protocol for helper agents used by the main decision maker."""

    def run(self, facts: List[Dict[str, Any]], ticker: str, quarter: str, *args) -> str:
        """Return a short analysis covering *all* supplied facts."""
        raise NotImplementedError

# ---------------------------------------------------------------------------
# MainAgent
# ---------------------------------------------------------------------------

DEFAULT_MODEL = os.getenv("MAIN_MODEL", "gpt-5-mini")


@dataclass
class MainAgent:
    credentials_file: Union[str, Path] | None = None
    credentials_path: Union[str, Path] | None = None
    model: str = DEFAULT_MODEL
    temperature: float = 0.7

    financials_agent: BaseHelperAgent | None = None
    past_calls_agent: BaseHelperAgent | None = None
    comparative_agent: BaseHelperAgent | None = None

    client: OpenAI = field(init=False)
    token_tracker: TokenTracker = field(default_factory=TokenTracker)

    # ------------ init ----------------------------------------------------
    def __post_init__(self) -> None:
        cred_path = Path(self.credentials_file or self.credentials_path or "")
        if not cred_path.exists():
            raise FileNotFoundError("Credentials file not found.")
        creds = json.loads(cred_path.read_text())
        self.client, resolved_model = build_chat_client(creds, self.model)
        self.model = resolved_model

    # ------------ internal LLM helper ------------------------------------
    def _chat(self, prompt: str, system: str = "", max_tokens: int | None = None) -> str:
        """Wrapper around OpenAI chat completion with token tracking.

        Args:
            prompt: User prompt
            system: System message
            max_tokens: Maximum tokens for response (None = no limit, uses model default)
        """
        msgs = [{"role": "system", "content": system}] if system else []
        msgs.append({"role": "user", "content": prompt})

        kwargs = {"model": self.model, "messages": msgs}
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens

        # GPT-5 models only support temperature=1; others use configured temperature
        if "gpt-5" not in self.model.lower():
            kwargs["temperature"] = self.temperature

        resp = self.client.chat.completions.create(**kwargs)

        if hasattr(resp, "usage") and resp.usage:
            self.token_tracker.add_usage(
                input_tokens=resp.usage.prompt_tokens,
                output_tokens=resp.usage.completion_tokens,
                model=self.model,
            )

        return resp.choices[0].message.content.strip()

    # ---------------------------------------------------------------------
    # 1) Extraction (single call, no chunking)
    # ---------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # Transcript pre-processing (remove boilerplate)
    # -------------------------------------------------------------------------
    _BOILERPLATE_PATTERNS = [
        # Safe harbor statements
        r"(?i)safe[- ]?harbor",
        r"(?i)forward[- ]?looking statements?",
        r"(?i)this (call|presentation|webcast) (may )?(contain|include)s? forward[- ]?looking",
        r"(?i)actual results may (differ|vary) materially",
        r"(?i)risk factors? (described|set forth|contained) in our",
        r"(?i)we undertake no obligation to update",
        r"(?i)sec fil(ing|ed)",
        # Operator instructions
        r"(?i)operator:?\s*(good (morning|afternoon|evening)|thank you|please)",
        r"(?i)ladies and gentlemen",
        r"(?i)welcome to (the )?.*earnings (call|conference)",
        r"(?i)this call is being recorded",
        r"(?i)(press|push) (\*|star)?\s*\d+ (to|for)",
        r"(?i)please (stand by|hold)",
        r"(?i)your line is (now )?open",
        r"(?i)please go ahead",
        # Generic closings
        r"(?i)this concludes (the|our|today'?s)",
        r"(?i)thank you for (your participation|joining|attending)",
        r"(?i)you may now disconnect",
    ]

    def _preprocess_transcript(self, transcript: str) -> str:
        """Remove boilerplate text (safe harbor, operator chatter) to save tokens.

        Removes:
        - Safe harbor / forward-looking statements disclaimers
        - Operator instructions and opening/closing remarks
        - Generic conference call boilerplate

        Preserves:
        - Management prepared remarks
        - Q&A content
        - Financial metrics and guidance
        """
        if not transcript:
            return transcript

        # Split into paragraphs
        paragraphs = transcript.split("\n\n")
        cleaned = []

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            # Check if paragraph matches any boilerplate pattern
            is_boilerplate = False
            for pattern in self._BOILERPLATE_PATTERNS:
                if re.search(pattern, para):
                    is_boilerplate = True
                    break

            # Skip short paragraphs that are likely operator cues
            if len(para) < 50 and any(kw in para.lower() for kw in ["operator", "thank you", "please", "next question"]):
                is_boilerplate = True

            if not is_boilerplate:
                cleaned.append(para)

        return "\n\n".join(cleaned)

    def _chunk_transcript(self, transcript: str, max_chars: int = DEFAULT_TRANSCRIPT_CHARS_PER_CHUNK) -> List[str]:
        """
        Split a long transcript into smaller chunks based on paragraphs.
        The goal is to keep each chunk under `max_chars` characters while
        preserving paragraph boundaries where possible.
        """
        if not transcript:
            return []

        chunks: List[str] = []
        current_parts: List[str] = []
        current_len = 0

        for para in transcript.split("\n\n"):
            para = para.strip()
            if not para:
                continue
            para_len = len(para) + 2  # include spacing/newlines

            if current_parts and current_len + para_len > max_chars:
                chunks.append("\n\n".join(current_parts))
                current_parts = [para]
                current_len = para_len
            else:
                current_parts.append(para)
                current_len += para_len

        if current_parts:
            chunks.append("\n\n".join(current_parts))

        return chunks

    def extract(self, transcript: str) -> List[Dict[str, str]]:
        """Extract facts from a transcript, chunking long transcripts to reduce context size."""
        # Preprocess to remove boilerplate (saves ~10-20% tokens)
        cleaned_transcript = self._preprocess_transcript(transcript)
        chunks = self._chunk_transcript(cleaned_transcript, max_chars=DEFAULT_TRANSCRIPT_CHARS_PER_CHUNK)
        if not chunks:
            chunks = [transcript] if transcript else []

        all_items: List[Dict[str, str]] = []
        for chunk in chunks:
            if not chunk:
                continue
            raw = self._chat(facts_extraction_prompt(chunk), system=get_extraction_system_message(), max_tokens=MAX_TOKENS_EXTRACTION)
            items = _parse_items(raw)
            all_items.extend(items)
        return all_items

    # ---------------------------------------------------------------------
    # 2) Delegation (batched)
    # ---------------------------------------------------------------------
    @staticmethod
    def _bucket_by_tool(tool_map: Dict[int, List[str]], items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        id2fact = {i: f for i, f in enumerate(items)}
        buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for fid, tools in tool_map.items():
            for t in tools:
                if fid in id2fact:
                    buckets[t].append(id2fact[fid])
        return buckets

    # -------------------------------------------------------------------------
    # Rule-based fact routing (no LLM call)
    # -------------------------------------------------------------------------
    FACT_TYPE_ROUTING = {
        # Result facts (financial metrics) → compare with historical statements
        "Result": ["InspectPastStatements"],
        # Forward-Looking → compare with past guidance to validate credibility
        "Forward-Looking": ["QueryPastCalls"],
        # Risk Disclosure → check if recurring issue from past calls
        "Risk Disclosure": ["QueryPastCalls"],
        # Sentiment → compare tone evolution over time
        "Sentiment": ["QueryPastCalls"],
        # Macro → compare sector-wide impact with peers
        "Macro": ["CompareWithPeers"],
        # Warning Sign → check financials AND past mentions (high priority)
        "Warning Sign": ["InspectPastStatements", "QueryPastCalls"],
    }

    def _route_fact_by_rules(self, fact: Dict[str, Any]) -> List[str]:
        """Route a fact to tools based on its type (rule-based, no LLM)."""
        fact_type = fact.get("type", "").strip()
        # Check for exact match first
        if fact_type in self.FACT_TYPE_ROUTING:
            return self.FACT_TYPE_ROUTING[fact_type]
        # Fallback: check metric keywords for financial-like facts
        metric = (fact.get("metric") or "").lower()
        if any(kw in metric for kw in ["revenue", "eps", "margin", "profit", "income", "cash", "ebitda"]):
            return ["InspectPastStatements"]
        if any(kw in metric for kw in ["guidance", "outlook", "forecast", "expect"]):
            return ["QueryPastCalls"]
        # Default: send to past calls agent
        return ["QueryPastCalls"]

    def delegate(
        self,
        items: List[Dict[str, Any]],
        ticker: str,
        quarter: str,
        peers: Sequence[str],
        row: Dict[str, Any],
    ) -> None:
        """Route facts using rule-based engine, call each helper in parallel."""

        def _ensure_list_of_dicts(facts: Any) -> List[Dict[str, Any]]:
            if isinstance(facts, str):
                try:
                    facts = json.loads(facts)
                except Exception:
                    print(f"❌ Could not parse string as JSON: {facts[:200]}")
                    raise TypeError(f"Expected list of dicts, got string: {facts[:200]}")
            if not isinstance(facts, list) or (facts and not isinstance(facts[0], dict)):
                print(
                    "❌ Expected list of dicts, got: "
                    f"{type(facts)} with first element {type(facts[0]) if facts else 'empty'}"
                )
                raise TypeError(f"Expected list of dicts, got: {type(facts)} with first element {type(facts[0]) if facts else 'empty'}")
            return facts

        # Step 1: Route facts using rule-based engine (no LLM call - saves ~500 tokens/call)
        tool_map: Dict[int, List[str]] = {}
        for i, fact in enumerate(items):
            tool_map[i] = self._route_fact_by_rules(fact)

        # Step 3: Bucket facts by tool
        buckets = self._bucket_by_tool(tool_map, items)
        if "CompareWithPeers" in buckets:
            print(f"[DEBUG] CompareWithPeers bucket size before helper: {len(buckets.get('CompareWithPeers', []))}")
        else:
            print("[DEBUG] CompareWithPeers bucket size before helper: 0")

        # Step 4: Run agents in parallel, with chunking (batch size 10)
        self._batch_notes: Dict[str, str] = {}
        tasks = []
        future_keys: Dict[Any, str] = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            if self.financials_agent and "InspectPastStatements" in buckets:
                def run_financials():
                    facts_for_financials = _ensure_list_of_dicts(buckets["InspectPastStatements"])
                    facts_for_financials = facts_for_financials[:MAX_FACTS_PER_HELPER]
                    res = self.financials_agent.run(facts_for_financials, row, quarter, ticker)
                    return ("financials", res)
                fut = executor.submit(run_financials)
                tasks.append(fut)
                future_keys[fut] = "financials"

            if self.past_calls_agent and "QueryPastCalls" in buckets:
                facts_for_past = _ensure_list_of_dicts(buckets["QueryPastCalls"])
                facts_for_past = facts_for_past[:MAX_FACTS_PER_HELPER]

                def run_past_calls():
                    res = self.past_calls_agent.run(facts_for_past, ticker, quarter)
                    return ("past", res)
                fut = executor.submit(run_past_calls)
                tasks.append(fut)
                future_keys[fut] = "past"
            
            if self.comparative_agent and "CompareWithPeers" in buckets:
                facts_for_peers = _ensure_list_of_dicts(buckets["CompareWithPeers"])
                facts_for_peers = facts_for_peers[:MAX_FACTS_PER_HELPER]
                sector = row.get("sector") if isinstance(row, dict) else getattr(row, "sector", None)

                def run_comparative():
                    res = self.comparative_agent.run(facts_for_peers, ticker, quarter, peers, sector=sector)
                    return ("peers", res)
                fut = executor.submit(run_comparative)
                tasks.append(fut)
                future_keys[fut] = "peers"

            for future in as_completed(tasks):
                key = future_keys.get(future, "unknown")
                try:
                    result = future.result()
                    if isinstance(result, tuple) and len(result) == 2:
                        task_key, payload = result
                        key = task_key or key
                        result = payload
                    self._batch_notes[key] = result
                    if result is None:
                        print(f"[AGENT LOG] Agent '{key}' failed or timed out for ticker={ticker}, quarter={quarter} (returned None)")
                except Exception as e:
                    print(f"[AGENT LOG] Agent '{key}' failed with exception for ticker={ticker}, quarter={quarter}: {e}")
                    print(f"\u26a0\ufe0f Agent failed: {e}")
                    self._batch_notes[key] = f"[error] {e}"

        # Optional: attach tool usage to each fact
        for i, f in enumerate(items):
            f["tools"] = tool_map.get(i, [])
        if "CompareWithPeers" in buckets:
            print(f"[DEBUG] CompareWithPeers bucket size after helper: {len(buckets.get('CompareWithPeers', []))}")
        else:
            print("[DEBUG] CompareWithPeers bucket size after helper: 0")

    # ---------------------------------------------------------------------
    # 3) Summary
    # ---------------------------------------------------------------------

    """
    def _fact_verdict(self, fact: Dict[str, Any]) -> str:
        a = fact.get("agent_analysis", {})
        return self._chat(
            main_agent_prompt(
                metric=fact["metric"],
                value=fact["value"],
                reason=fact["context"],
                financials_summary=a.get("financials_agent", "N/A"),
                past_calls_summary=a.get("past_calls_agent", "N/A"),
                comparative_summary=a.get("comparative_agent", "N/A"),
            )
        
        )
    """
    
    def _flatten_notes(self, note):
        if isinstance(note, list):
            return "\n\n".join([n for n in note if n])
        return note if note is not None else ""

    def summarise(self, items: list[dict[str, Any]],
                  memory_txt: str | None = None,
                  original_transcript: str | None = None,
                  financial_statements_facts: str | None = None,
                  market_anchors: Dict[str, Any] | None = None,
                  key_facts: List[Dict[str, Any]] | None = None) -> tuple[Dict[str, str], str]:
        notes = {
            "financials": self._flatten_notes(self._batch_notes.get("financials", None)),
            "past"      : self._flatten_notes(self._batch_notes.get("past", None)),
            "peers"     : self._flatten_notes(self._batch_notes.get("peers", None)),
        }
        # Ensure each note has a fallback value
        for k, v in list(notes.items()):
            if v is None or v == "":
                notes[k] = "N/A"

        # Format and include QoQChange facts as a dedicated section
        qoq_facts = [f for f in items if f.get('type') == 'YoYChange']
        def format_qoq_facts(qoq_facts):
            if not qoq_facts:
                return "No YoY changes available."
            lines = []
            for f in qoq_facts:
                metric = f.get('metric', '?')
                value = f.get('value', '?')
                quarter = f.get('quarter', '?')
                reason = f.get('reason', '')
                # Format value as a percentage with 2 decimal places
                try:
                    pct_value = float(value) * 100
                    value_str = f"{pct_value:.2f}%"
                except Exception:
                    value_str = str(value)
                lines.append(f"• {metric}: {value_str} ({quarter}) {reason}")
            return '\n'.join(lines)
        qoq_section = format_qoq_facts(qoq_facts)

        core_prompt = main_agent_prompt(notes, original_transcript=original_transcript, memory_txt=memory_txt, financial_statements_facts=financial_statements_facts, market_anchors=market_anchors, key_facts=key_facts)

        if core_prompt is None:
            core_prompt = "No summary available."
        final_prompt = f"{memory_txt}\n\n{core_prompt}" if memory_txt else core_prompt

        # TODO: remove
        final_prompt = core_prompt
        # Debug print disabled to improve performance
        # print("\n==== MAIN AGENT FULL PROMPT ====")
        # print(final_prompt)
        # print("===============================\n")
        return notes, self._chat(final_prompt, system=get_main_agent_system_message(), max_tokens=MAX_TOKENS_SUMMARY)

    # ---------------------------------------------------------------------
    # 4) Orchestrator
    # ---------------------------------------------------------------------
    def run(
        self,
        facts: List[Dict[str, Any]],
        row: Dict[str, Any],
        mem_txt: str | None = None,
        original_transcript: str | None = None,
        financial_statements_facts: str | None = None,
        market_anchors: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """End-to-end execution: peer discovery, helper delegation, and summary."""
        self.token_tracker = TokenTracker()

        # ------------------------------------------------------------------
        # 1) Peer discovery via DB (fast, no LLM call)
        # ------------------------------------------------------------------
        peers: List[str] = []
        try:
            import pg_client
            sector = row.get("sector") if isinstance(row, dict) else getattr(row, "sector", None)
            ticker = row.get("ticker") if isinstance(row, dict) else getattr(row, "ticker", "")
            if sector:
                # Use DB lookup instead of LLM - much faster and cheaper
                peers = pg_client.get_peers_by_sector(sector, exclude_symbol=ticker, limit=MAX_PEERS)
        except Exception:
            peers = []

        # ------------------------------------------------------------------
        # 2) Delegate facts to helper agents
        # ------------------------------------------------------------------
        self.delegate(facts, row["ticker"], row["q"], peers, row)

        # ------------------------------------------------------------------
        # 3) Summarise with memory and market anchors
        # ------------------------------------------------------------------
        # NOTE: original_transcript removed to save tokens - key_facts + notes already contain all needed info
        notes, decision = self.summarise(facts, memory_txt=mem_txt, original_transcript=None, financial_statements_facts=financial_statements_facts, market_anchors=market_anchors, key_facts=facts)

        # ------------------------------------------------------------------
        # 4) Return everything (memory included for logging/debug)
        # ------------------------------------------------------------------
        return {
            "items": facts,
            "notes": notes,
            "summary": decision,
            "memory": mem_txt or "",
            "token_usage": self.token_tracker.get_summary()
        }
