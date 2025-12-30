"""Shared LLM client helpers with LiteLLM-first, Azure-fallback selection."""
from __future__ import annotations

import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Dict, Tuple, Callable, TypeVar, Any

from langchain_openai import OpenAIEmbeddings, AzureOpenAIEmbeddings
from openai import AzureOpenAI, OpenAI

DEFAULT_AZURE_VERSION = "2024-12-01-preview"

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Retry configuration
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_INITIAL_BACKOFF = float(os.getenv("RETRY_INITIAL_BACKOFF", "2.0"))
RETRY_MAX_BACKOFF = float(os.getenv("RETRY_MAX_BACKOFF", "30.0"))

# Track which provider is currently active (for fallback state)
_current_provider = "litellm"  # or "azure"


def _is_provider_error(exc: Exception) -> bool:
    """Check if exception indicates provider unavailability (503, 401, connection error)."""
    error_str = str(exc).lower()
    return any(x in error_str for x in [
        "503", "service unavailable", "502", "bad gateway",
        "401", "authentication", "unauthorized",
        "connection", "timeout", "connect call failed",
    ])


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if exception is a 429 rate limit error."""
    error_str = str(exc).lower()
    return "429" in error_str or "rate limit" in error_str or "too many requests" in error_str


def with_fallback_and_retry(
    primary_fn: Callable[[], T],
    fallback_fn: Callable[[], T] | None = None,
    max_retries: int = RETRY_MAX_ATTEMPTS,
) -> T:
    """
    Execute primary_fn with retry for rate limits.
    If primary fails with provider error, try fallback_fn.
    """
    global _current_provider

    last_exc = None
    backoff = RETRY_INITIAL_BACKOFF

    # Try primary with retries for rate limits
    for attempt in range(1, max_retries + 1):
        try:
            result = primary_fn()
            return result
        except Exception as exc:
            last_exc = exc

            # Provider unavailable - try fallback immediately
            if _is_provider_error(exc):
                logger.warning(f"Primary provider error: {exc}")
                break

            # Rate limit - retry with backoff
            if _is_rate_limit_error(exc):
                if attempt == max_retries:
                    logger.warning(f"Rate limit retry exhausted after {max_retries} attempts")
                    break
                jitter = random.uniform(0, backoff * 0.5)
                sleep_time = min(backoff + jitter, RETRY_MAX_BACKOFF)
                logger.info(f"Rate limit (attempt {attempt}/{max_retries}), retrying in {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                backoff = min(backoff * 2, RETRY_MAX_BACKOFF)
                continue

            # Other error - don't retry, try fallback
            logger.warning(f"Non-retryable error: {exc}")
            break

    # Try fallback if available
    if fallback_fn is not None:
        logger.info("Attempting fallback provider...")
        try:
            result = fallback_fn()
            _current_provider = "azure"
            logger.info("Fallback provider succeeded")
            return result
        except Exception as fb_exc:
            logger.error(f"Fallback also failed: {fb_exc}")
            # Raise original error
            raise last_exc from fb_exc

    if last_exc:
        raise last_exc
    raise RuntimeError("No result from primary or fallback")


def _azure_settings(creds: Dict[str, str]) -> Tuple[str | None, str | None, str, Dict[str, str], str | None]:
    """Collect Azure config from creds + env and return (key, endpoint, version, deployments, embedding_deployment)."""
    key = creds.get("azure_api_key") or os.getenv("AZURE_OPENAI_API_KEY")
    endpoint = creds.get("azure_endpoint") or os.getenv("AZURE_OPENAI_ENDPOINT")
    version = creds.get("azure_api_version") or os.getenv("AZURE_OPENAI_API_VERSION") or DEFAULT_AZURE_VERSION
    deployments = creds.get("azure_deployments") or {}

    env_deployments = {}
    gpt51 = os.getenv("AZURE_OPENAI_DEPLOYMENT_GPT51")
    if gpt51:
        env_deployments["gpt-5.1"] = gpt51
    gpt5 = os.getenv("AZURE_OPENAI_DEPLOYMENT_GPT5")
    if gpt5:
        env_deployments["gpt-5-mini"] = gpt5
    gpt4o = os.getenv("AZURE_OPENAI_DEPLOYMENT_GPT4O")
    if gpt4o:
        env_deployments["gpt-4o-mini"] = gpt4o
    deployments = deployments or env_deployments

    embedding_dep = creds.get("azure_embedding_deployment") or os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")

    return key, endpoint, version, deployments, embedding_dep


def build_chat_client(
    creds: Dict[str, str],
    requested_model: str,
    prefer_openai: bool = False,
) -> Tuple[OpenAI | AzureOpenAI, str]:
    """
    Return (client, model_name) where model_name is mapped to Azure deployment if available.

    Priority:
    1. If _current_provider == "azure", use Azure directly (fallback mode active)
    2. If openai_api_base is set (LiteLLM proxy), use it with OpenAI client
    3. Azure settings if available
    4. Fallback to public OpenAI key
    """
    global _current_provider

    # If we've already switched to Azure fallback, use Azure directly
    if _current_provider == "azure":
        azure_key = os.getenv("AZURE_OPENAI_API_KEY")
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        if azure_key and azure_endpoint:
            logger.debug(f"Using Azure fallback for model: {requested_model}")
            # Use build_azure_client which handles gpt-5 wrapper
            return build_azure_client(requested_model)

    # Check for LiteLLM / custom OpenAI-compatible endpoint first
    api_base = creds.get("openai_api_base") or os.getenv("LITELLM_ENDPOINT")
    api_key = creds.get("openai_api_key") or os.getenv("LITELLM_API_KEY") or os.getenv("OPENAI_API_KEY")

    if api_base and api_key:
        # Use LiteLLM or custom OpenAI-compatible endpoint
        return OpenAI(api_key=api_key, base_url=api_base), requested_model

    if prefer_openai:
        if not api_key:
            raise RuntimeError("No OpenAI/Azure API key configured.")
        return OpenAI(api_key=api_key), requested_model

    azure_key, azure_endpoint, azure_version, deployments, _ = _azure_settings(creds)
    if azure_key and azure_endpoint:
        deployment = deployments.get(requested_model, requested_model)
        client = AzureOpenAI(
            api_key=azure_key,
            azure_endpoint=azure_endpoint,
            api_version=azure_version,
        )
        return client, deployment

    if not api_key:
        raise RuntimeError("No OpenAI/Azure API key configured.")
    return OpenAI(api_key=api_key), requested_model


def switch_to_azure_fallback():
    """Manually switch to Azure fallback provider."""
    global _current_provider
    _current_provider = "azure"
    logger.info("Switched to Azure OpenAI fallback provider")


def reset_to_primary_provider():
    """Reset to primary LiteLLM provider."""
    global _current_provider
    _current_provider = "litellm"
    logger.info("Reset to primary LiteLLM provider")


def build_embeddings(creds: Dict[str, str], model: str = "text-embedding-ada", prefer_openai: bool = False) -> OpenAIEmbeddings:
    """Return embeddings client; use Azure deployment if configured, else OpenAI."""
    # Check for LiteLLM / custom OpenAI-compatible endpoint first
    api_base = creds.get("openai_api_base") or os.getenv("LITELLM_ENDPOINT")
    api_key = creds.get("openai_api_key") or os.getenv("LITELLM_API_KEY") or os.getenv("OPENAI_API_KEY")

    if api_base and api_key:
        return OpenAIEmbeddings(openai_api_key=api_key, openai_api_base=api_base, model=model)

    if prefer_openai:
        if not api_key:
            raise RuntimeError("No OpenAI API key configured.")
        return OpenAIEmbeddings(openai_api_key=api_key, model=model)

    azure_key, azure_endpoint, azure_version, deployments, embedding_dep = _azure_settings(creds)
    if azure_key and azure_endpoint:
        deployment = embedding_dep or deployments.get(model)
        # If no explicit Azure embedding deployment is configured, fall back to OpenAI.
        if deployment:
            return AzureOpenAIEmbeddings(
                model=deployment,
                azure_deployment=deployment,
                api_key=azure_key,
                azure_endpoint=azure_endpoint,
                api_version=azure_version,
            )

    if not api_key:
        raise RuntimeError("No OpenAI API key configured.")
    return OpenAIEmbeddings(openai_api_key=api_key, model=model)


def build_embedding_client(
    creds: Dict[str, str],
    prefer_openai: bool = False,
) -> Tuple[OpenAI | AzureOpenAI, str]:
    """
    Return (client, model_name) for embedding operations.
    Priority: LiteLLM > Azure > OpenAI
    """
    # Check for LiteLLM / custom OpenAI-compatible endpoint first
    api_base = creds.get("openai_api_base") or os.getenv("LITELLM_ENDPOINT")
    api_key = creds.get("openai_api_key") or os.getenv("LITELLM_API_KEY") or os.getenv("OPENAI_API_KEY")

    if api_base and api_key:
        return OpenAI(api_key=api_key, base_url=api_base), "text-embedding-ada"

    if prefer_openai:
        if not api_key:
            raise RuntimeError("No OpenAI API key configured.")
        return OpenAI(api_key=api_key), "text-embedding-ada"

    azure_key, azure_endpoint, azure_version, _, embedding_dep = _azure_settings(creds)
    if azure_key and azure_endpoint and embedding_dep:
        client = AzureOpenAI(
            api_key=azure_key,
            azure_endpoint=azure_endpoint,
            api_version=azure_version,
        )
        return client, embedding_dep

    # Fallback to OpenAI direct
    if not api_key:
        raise RuntimeError("No OpenAI/Azure API key configured.")
    return OpenAI(api_key=api_key), "text-embedding-ada"


def load_credentials(path: str | Path) -> Dict[str, str]:
    """Load JSON credentials file."""
    return json.loads(Path(path).read_text())


def build_litellm_client(requested_model: str) -> Tuple[OpenAI, str]:
    """Build LiteLLM client from env vars."""
    api_base = os.getenv("LITELLM_ENDPOINT")
    api_key = os.getenv("LITELLM_API_KEY")
    if not api_base or not api_key:
        raise RuntimeError("LiteLLM not configured (missing LITELLM_ENDPOINT or LITELLM_API_KEY)")
    return OpenAI(api_key=api_key, base_url=api_base), requested_model


class AzureGPT5ChatCompletions:
    """Wrapper for Azure chat completions that auto-converts max_tokens to max_completion_tokens for gpt-5 models."""

    def __init__(self, client: AzureOpenAI):
        self._client = client

    def create(self, **kwargs):
        # Azure gpt-5-mini uses max_completion_tokens instead of max_tokens
        if "max_tokens" in kwargs:
            max_tokens = kwargs.pop("max_tokens")
            if max_tokens is not None:
                # Cap at Azure's limit of 16384
                kwargs["max_completion_tokens"] = min(max_tokens, 16384)
        return self._client.chat.completions.create(**kwargs)


class AzureGPT5Chat:
    """Wrapper for Azure chat that provides auto-converting completions."""

    def __init__(self, client: AzureOpenAI):
        self.completions = AzureGPT5ChatCompletions(client)


class AzureGPT5ClientWrapper:
    """Wrapper for AzureOpenAI client that auto-converts gpt-5 specific parameters."""

    def __init__(self, client: AzureOpenAI):
        self._client = client
        self.chat = AzureGPT5Chat(client)

    def __getattr__(self, name):
        return getattr(self._client, name)


def build_azure_client(requested_model: str) -> Tuple[AzureOpenAI, str]:
    """Build Azure OpenAI client from env vars, wrapped for gpt-5 compatibility."""
    azure_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    azure_version = os.getenv("AZURE_OPENAI_API_VERSION", DEFAULT_AZURE_VERSION)
    azure_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", requested_model)

    if not azure_key or not azure_endpoint:
        raise RuntimeError("Azure OpenAI not configured (missing AZURE_OPENAI_API_KEY or AZURE_OPENAI_ENDPOINT)")

    raw_client = AzureOpenAI(
        api_key=azure_key,
        azure_endpoint=azure_endpoint,
        api_version=azure_version,
    )

    # Wrap the client to auto-convert max_tokens for gpt-5 models
    if "gpt-5" in azure_deployment.lower():
        wrapped_client = AzureGPT5ClientWrapper(raw_client)
        return wrapped_client, azure_deployment

    return raw_client, azure_deployment


def get_current_provider() -> str:
    """Return current active provider ('litellm' or 'azure')."""
    return _current_provider


def chat_completion_with_fallback(
    messages: list,
    model: str,
    temperature: float = 0.0,
    max_tokens: int | None = None,
    **kwargs,
) -> Any:
    """
    Execute chat completion with LiteLLM primary, Azure fallback.
    Returns the response object.
    """
    def litellm_call():
        client, model_name = build_litellm_client(model)
        return client.chat.completions.create(
            model=model_name,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )

    def azure_call():
        client, deployment = build_azure_client(model)
        # Azure gpt-5-mini uses max_completion_tokens instead of max_tokens
        azure_kwargs = {k: v for k, v in kwargs.items() if k != "max_tokens"}
        if max_tokens is not None:
            azure_kwargs["max_completion_tokens"] = max_tokens
        return client.chat.completions.create(
            model=deployment,
            messages=messages,
            temperature=temperature,
            **azure_kwargs,
        )

    # Check if Azure is configured for fallback
    azure_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    fallback_fn = azure_call if (azure_key and azure_endpoint) else None

    return with_fallback_and_retry(litellm_call, fallback_fn)
