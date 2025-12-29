"""Shared LLM client helpers with Azure-first, OpenAI-fallback selection."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Tuple

from langchain_openai import OpenAIEmbeddings, AzureOpenAIEmbeddings
from openai import AzureOpenAI, OpenAI

DEFAULT_AZURE_VERSION = "2024-12-01-preview"


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
    1. If openai_api_base is set (LiteLLM proxy), use it with OpenAI client
    2. Azure settings if available
    3. Fallback to public OpenAI key
    """
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
