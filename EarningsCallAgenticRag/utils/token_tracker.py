"""Shared TokenTracker module for all agents.

Centralized token usage and cost tracking to avoid code duplication.
"""

from __future__ import annotations

import os
from typing import Any, Dict

# =============================================================================
# Pricing Configuration (can be overridden via environment variables)
# =============================================================================
# Format: MODEL_PREFIX_INPUT_PRICE and MODEL_PREFIX_OUTPUT_PRICE in USD per token

# Pricing per token (from LiteLLM /model/info endpoint)
# All values are in USD per token
PRICING_CONFIG = {
    # GPT-4o: $2.50/1M input, $10/1M output
    "gpt-4o": {
        "input": float(os.getenv("GPT4O_INPUT_PRICE", "0.0000025")),
        "output": float(os.getenv("GPT4O_OUTPUT_PRICE", "0.00001")),
    },
    # GPT-4o-mini: $0.165/1M input, $0.66/1M output
    "gpt-4o-mini": {
        "input": float(os.getenv("GPT4O_MINI_INPUT_PRICE", "0.000000165")),
        "output": float(os.getenv("GPT4O_MINI_OUTPUT_PRICE", "0.00000066")),
    },
    # GPT-4 (legacy): $30/1M input, $60/1M output
    "gpt-4": {
        "input": float(os.getenv("GPT4_INPUT_PRICE", "0.00003")),
        "output": float(os.getenv("GPT4_OUTPUT_PRICE", "0.00006")),
    },
    # GPT-3.5: $1.5/1M input, $2/1M output
    "gpt-3.5": {
        "input": float(os.getenv("GPT35_INPUT_PRICE", "0.0000015")),
        "output": float(os.getenv("GPT35_OUTPUT_PRICE", "0.000002")),
    },
    # Claude: $3/1M input, $15/1M output
    "claude": {
        "input": float(os.getenv("CLAUDE_INPUT_PRICE", "0.000003")),
        "output": float(os.getenv("CLAUDE_OUTPUT_PRICE", "0.000015")),
    },
    # GPT-5-mini (Azure/OpenAI via LiteLLM): $0.25/1M input, $2/1M output
    "gpt-5-mini": {
        "input": float(os.getenv("GPT5_MINI_INPUT_PRICE", "0.00000025")),
        "output": float(os.getenv("GPT5_MINI_OUTPUT_PRICE", "0.000002")),
    },
    # GPT-5: $1.25/1M input, $10/1M output
    "gpt-5": {
        "input": float(os.getenv("GPT5_INPUT_PRICE", "0.00000125")),
        "output": float(os.getenv("GPT5_OUTPUT_PRICE", "0.00001")),
    },
    # GPT-5-nano: $0.05/1M input, $0.4/1M output
    "gpt-5-nano": {
        "input": float(os.getenv("GPT5_NANO_INPUT_PRICE", "0.00000005")),
        "output": float(os.getenv("GPT5_NANO_OUTPUT_PRICE", "0.0000004")),
    },
    # CLI-GPT-5.2 (estimate based on similar models)
    "cli-gpt-5.2": {
        "input": float(os.getenv("CLI_GPT52_INPUT_PRICE", "0.0000001")),
        "output": float(os.getenv("CLI_GPT52_OUTPUT_PRICE", "0.0000008")),
    },
}


def get_model_pricing(model: str) -> Dict[str, float]:
    """Get pricing for a model based on its name prefix.

    Args:
        model: Model name (e.g., "gpt-4o-mini", "gpt-4-turbo")

    Returns:
        Dict with "input" and "output" prices per token
    """
    if not model:
        return {"input": 0.0, "output": 0.0}

    lowered = model.lower()

    # Check each pricing config prefix
    for prefix, prices in PRICING_CONFIG.items():
        if prefix in lowered:
            return prices

    # Default: no cost tracking for unknown models
    return {"input": 0.0, "output": 0.0}


class TokenTracker:
    """Aggregate token usage and cost estimation per run.

    Usage:
        tracker = TokenTracker()
        tracker.add_usage(input_tokens=100, output_tokens=50, model="gpt-4o-mini")
        summary = tracker.get_summary()
    """

    def __init__(self) -> None:
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost_usd = 0.0
        self.model_used = "gpt-4o-mini"  # default

    def add_usage(
        self,
        input_tokens: int,
        output_tokens: int,
        model: str | None = None
    ) -> None:
        """Add token usage from an API call.

        Args:
            input_tokens: Number of input/prompt tokens
            output_tokens: Number of output/completion tokens
            model: Model name for cost calculation
        """
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens

        if model is not None:
            self.model_used = model

        # Calculate cost
        if model:
            pricing = get_model_pricing(model)
            self.total_cost_usd += (
                input_tokens * pricing["input"] +
                output_tokens * pricing["output"]
            )

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of token usage and costs.

        Returns:
            Dict with model, input_tokens, output_tokens, total_tokens, cost_usd
        """
        return {
            "model": self.model_used,
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "total_tokens": self.total_input_tokens + self.total_output_tokens,
            "cost_usd": round(self.total_cost_usd, 6),
        }

    def reset(self) -> None:
        """Reset all counters to zero."""
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost_usd = 0.0

    def __repr__(self) -> str:
        return (
            f"TokenTracker(model={self.model_used}, "
            f"tokens={self.total_input_tokens + self.total_output_tokens}, "
            f"cost=${self.total_cost_usd:.4f})"
        )
