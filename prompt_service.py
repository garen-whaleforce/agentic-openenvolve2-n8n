from typing import Any, Dict, List, Optional

from storage import (
    get_prompt,
    set_prompt,
    get_all_prompts,
    delete_prompt,
    list_prompt_profiles as _list_prompt_profiles,
    get_prompt_profile as _get_prompt_profile,
    set_prompt_profile as _set_prompt_profile,
    delete_prompt_profile as _delete_prompt_profile,
)


def get_prompt_override(key: str, default: str) -> str:
    """
    從 DB 讀取 override（prompt_configs），如果沒有值就回傳 default。
    """
    content = get_prompt(key)
    return content if content not in (None, "") else default


def get_all_prompt_overrides() -> Dict[str, str]:
    """
    回傳 DB 中所有 key -> content 的 mapping。
    """
    return get_all_prompts()


def save_prompt_override(key: str, content: str) -> None:
    """
    更新（或新增）某個 prompt 的 override。
    """
    set_prompt(key, content)


def reset_prompt_override(key: str) -> None:
    """
    刪除某個 prompt 的 override，讓它恢復為 default。
    """
    delete_prompt(key)


# ============================================================================
# PROFILE HELPERS
# ============================================================================


def list_prompt_profiles() -> List[Dict[str, Any]]:
    """回傳所有 profile 的 name + updated_at。"""
    return _list_prompt_profiles()


def get_prompt_profile(name: str) -> Optional[Dict[str, Any]]:
    """回傳指定 profile 的詳細內容（含 prompts dict）。"""
    return _get_prompt_profile(name)


def save_prompt_profile(name: str, prompts: Dict[str, str]) -> None:
    """儲存（或更新）一個 profile。"""
    _set_prompt_profile(name, prompts)


def delete_prompt_profile(name: str) -> None:
    """刪除指定 profile。"""
    _delete_prompt_profile(name)
