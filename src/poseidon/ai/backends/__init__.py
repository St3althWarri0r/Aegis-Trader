"""Pluggable LLM backends for the AI layer.

``build_backend`` selects the concrete backend from config. The Anthropic path
is the default and reads its key from the vault; the OpenAI-compatible path
targets a local endpoint and needs no secret.
"""
from __future__ import annotations

from collections.abc import Callable

from ...core.config import AIConfig
from .anthropic_backend import AnthropicBackend
from .base import ChatBackend, LLMResponse, ToolCall, ToolResult
from .openai_backend import OpenAICompatibleBackend

__all__ = ["CURATED_CLAUDE_MODELS", "KNOWN_BROKEN_CLAUDE_MODELS", "ChatBackend", "LLMResponse",
           "ToolCall", "ToolResult", "add_usage", "build_backend", "build_backends", "sum_usage"]

# Curated Claude model ids offered by the dashboard model selector (GET
# /api/models → anthropic.models). Seeded from the canonical in-repo ids
# (docs/api-configuration.md). Every id here must support adaptive thinking +
# the effort control, because the anthropic backend always sends
# thinking={"type":"adaptive"} + output_config={"effort"} on the trading
# decision (force_tool is None) — a non-adaptive id would 400. A custom id can
# still be typed in the UI; this list is only the curated menu. Extend it as
# Anthropic ships adaptive-capable ids (verify via the claude-api skill).
# Haiku 4.5 is deliberately ABSENT: it takes the legacy ``budget_tokens``
# thinking form and rejects ``output_config.effort``, so it 400s on the first
# completion — as the primary AND as ``ai.utility_model`` (every role but the
# forced-tool reviewer sends that shape). Fable 5.1 is absent because it
# rejects the forced tool_choice the algorithm reviewer relies on.
CURATED_CLAUDE_MODELS: tuple[str, ...] = (
    "claude-opus-5",
    "claude-opus-4-8",
    "claude-sonnet-5",
)

# Ids removed from the curated menu above because they 400 on the request
# shapes this backend always sends — kept here (never just deleted) so
# `poseidon config validate`/`poseidon doctor` can warn an operator whose
# ``ai.model``/``ai.utility_model`` still names one, instead of the config
# silently validating while every completion 400s. AIConfig itself accepts
# any string for these fields (a local/openai_compatible id is legitimate),
# so this is a warning list, never a hard validation gate.
KNOWN_BROKEN_CLAUDE_MODELS: dict[str, str] = {
    "claude-haiku-4-5-20251001": (
        "takes the legacy budget_tokens thinking form and rejects "
        "output_config.effort — 400s on every completion"
    ),
}


def add_usage(acc: list[dict[str, int]] | None, usage: object) -> None:
    """Meter one completion's ``LLMResponse.usage`` into an accumulator.

    No-op when the caller is not metering (``acc is None``) or the call
    reported no usage dict (some failures). Advisory helpers call this after
    every ``complete`` so spend is counted even on partially-failed pipelines.
    """
    if acc is None or not isinstance(usage, dict):
        return
    acc.append({str(k): v for k, v in usage.items() if isinstance(v, int)})


def sum_usage(calls: list[dict[str, int]]) -> dict[str, int]:
    """Fold per-completion usage dicts into one ``ai_usage`` payload.

    ``api_calls`` counts the metered completions, so any nonempty accumulator
    produces a row the monthly budget estimate can see.
    """
    total = {"input_tokens": 0, "output_tokens": 0,
             "cache_read_tokens": 0, "cache_write_tokens": 0}
    for u in calls:
        for key in total:
            total[key] += u.get(key, 0)
    total["api_calls"] = len(calls)
    return total


def build_backend(cfg: AIConfig, resolve_secret: Callable[[str], str]) -> ChatBackend:
    if cfg.backend == "anthropic":
        return AnthropicBackend(cfg, api_key=resolve_secret(cfg.api_key_credential))
    return OpenAICompatibleBackend(cfg)


def build_backends(cfg: AIConfig,
                   resolve_secret: Callable[[str], str]) -> tuple[ChatBackend, ChatBackend]:
    """(primary, utility) backends. Utility IS the primary object unless
    ``cfg.utility_model`` is set, in which case it is the same backend/endpoint
    with the model swapped — for the tolerant auxiliary roles (chat, reflection).
    The trading decision must always use the primary."""
    primary = build_backend(cfg, resolve_secret)
    if not cfg.utility_model:
        return primary, primary
    utility = build_backend(cfg.model_copy(update={"model": cfg.utility_model}), resolve_secret)
    return primary, utility
