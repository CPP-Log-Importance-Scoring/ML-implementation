"""
dashboard/llm_summary.py
========================
Groq-powered batch LLM summary generation and cache management.

Two public entry points (signatures unchanged — pipeline.py and the dashboard
depend on them):
  generate_all_summaries()  — called at end of pipeline.py, batches all new incidents
  regenerate_summary()      — called only from the dashboard Regenerate button

Hardening over the previous Gemini implementation:
  * Retry with exponential backoff on rate-limit / transient errors (429 / 5xx).
  * Empty / blocked response is detected and raised, never silently `.text`-crashes.
  * Tolerant JSON extraction — accepts a bare array, a {"summaries": [...]} object,
    or prose-wrapped JSON, then falls back to per-incident calls.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Groq's non-reasoning Llama models don't burn output budget on hidden thinking,
# which is what made gemini-2.5-flash return empty responses intermittently.
DEFAULT_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.5
MAX_OUTPUT_TOKENS = 1024

# ---------------------------------------------------------------------------
# Lazy Groq initialisation — avoids import errors when GROQ_API_KEY is absent
# ---------------------------------------------------------------------------

_client = None


def _get_client():
    global _client
    if _client is None:
        try:
            from groq import Groq

            api_key = os.environ.get("GROQ_API_KEY", "")
            if not api_key:
                logger.warning("GROQ_API_KEY not set — LLM summaries will be unavailable.")
                return None

            _client = Groq(api_key=api_key)
        except Exception as exc:
            logger.warning("Failed to initialise Groq client: %s", exc)
            return None
    return _client


# ---------------------------------------------------------------------------
# Low-level generation with retry + empty-response guard
# ---------------------------------------------------------------------------

def _generate(prompt: str, *, json_mode: bool = False, max_tokens: int = MAX_OUTPUT_TOKENS) -> str:
    """
    Single Groq chat completion. Returns the assistant message text.

    Retries on rate-limit / transient errors with exponential backoff.
    Raises on persistent failure or empty response — callers handle the
    exception (batch falls back to per-incident, regenerate returns an
    error string).
    """
    client = _get_client()
    if client is None:
        raise RuntimeError("Groq client not available — GROQ_API_KEY not configured.")

    kwargs = {
        "model": DEFAULT_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.chat.completions.create(**kwargs)
            if not resp.choices:
                raise ValueError("Groq returned no choices")
            content = resp.choices[0].message.content
            if not content or not content.strip():
                raise ValueError("Groq returned empty content")
            return content.strip()
        except Exception as exc:
            last_exc = exc
            if not _is_retryable(exc) or attempt == MAX_RETRIES - 1:
                break
            sleep_s = BASE_BACKOFF_SECONDS * (2 ** attempt)
            logger.warning(
                "Groq call failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1, MAX_RETRIES, exc, sleep_s,
            )
            time.sleep(sleep_s)

    raise last_exc if last_exc else RuntimeError("Groq generation failed")


def _is_retryable(exc: Exception) -> bool:
    """Retry on rate limits (429), timeouts, connection errors and 5xx."""
    status = getattr(exc, "status_code", None)
    if status in (408, 429, 500, 502, 503, 504):
        return True
    name = type(exc).__name__.lower()
    return any(k in name for k in ("ratelimit", "timeout", "connection", "apiconnection", "internalserver"))


# ---------------------------------------------------------------------------
# Tolerant JSON extraction
# ---------------------------------------------------------------------------

def _parse_summary_list(text: str) -> list[dict]:
    """
    Accept any of:
      * a bare JSON array  [ {...}, {...} ]
      * an object          { "summaries": [ {...} ] }
      * prose-wrapped JSON (extract the first array/object block)
    Returns a list of {"correlation_id", "summary_text"} dicts.
    Raises ValueError if nothing usable is found.
    """
    cleaned = re.sub(r"```json|```", "", text).strip()

    def _coerce(obj) -> list[dict]:
        if isinstance(obj, dict):
            obj = obj.get("summaries", obj.get("incidents", []))
        if isinstance(obj, list) and all(isinstance(r, dict) and "correlation_id" in r for r in obj):
            return obj
        raise ValueError("JSON did not contain a list of summary objects")

    try:
        return _coerce(json.loads(cleaned))
    except Exception:
        pass

    # Last resort: pull the first [...] or {...} block out of surrounding prose.
    match = re.search(r"(\[.*\]|\{.*\})", cleaned, re.DOTALL)
    if match:
        return _coerce(json.loads(match.group(1)))
    raise ValueError("No JSON array/object found in response")


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def _build_context(cid: str, scored_df: pd.DataFrame, root_causes_df: pd.DataFrame) -> dict:
    cluster = scored_df[scored_df["correlation_id"] == cid]
    ordered = cluster.sort_values("timestamp") if "timestamp" in cluster.columns else cluster

    templates = (
        ordered["template_id"].dropna().tolist()
        if "template_id" in ordered.columns
        else []
    )
    seq = " → ".join(templates[:10])
    if len(templates) > 10:
        seq += f" ... (+{len(templates) - 10} more)"

    rc_col = "incident_id" if "incident_id" in root_causes_df.columns else None
    rc = pd.DataFrame()
    if rc_col:
        rc = root_causes_df[root_causes_df[rc_col] == cid]

    rc_str = "none identified"
    if not rc.empty and "root_cause_log_id" in rc.columns:
        rc_str = ", ".join(
            f"{r['root_cause_log_id']} (conf: {r.get('confidence_score', 0):.2f})"
            for _, r in rc.iterrows()
        )

    hosts = (
        ", ".join(cluster["host"].dropna().unique())
        if "host" in cluster.columns
        else "unknown"
    )
    log_count = len(cluster)
    is_cross = bool(cluster["is_cross_system"].any()) if "is_cross_system" in cluster.columns else False

    duration_s = 0
    if "timestamp" in ordered.columns and len(ordered) > 1:
        try:
            duration_s = int(
                (ordered["timestamp"].iloc[-1] - ordered["timestamp"].iloc[0]).total_seconds()
            )
        except Exception:
            pass

    # Top-3 highest-scoring logs — gives the model the most salient events.
    top3_logs = "N/A"
    if "final_score" in cluster.columns and "template_id" in cluster.columns and not cluster.empty:
        top = cluster.sort_values("final_score", ascending=False).head(3)
        top3_logs = "; ".join(
            f"{r['template_id']} ({float(r.get('final_score', 0)):.2f})"
            for _, r in top.iterrows()
        )

    return {
        "correlation_id": cid,
        "host": hosts,
        "start_time": str(ordered["timestamp"].iloc[0]) if "timestamp" in ordered.columns and len(ordered) else "unknown",
        "duration_seconds": duration_s,
        "log_count": log_count,
        "is_cross_system": is_cross,
        "template_sequence": seq or "(no templates)",
        "root_causes": rc_str,
        "top3_logs": top3_logs,
    }


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

_RULES = """IMPORTANT RULES:
* Use ONLY the information explicitly provided in the incident data.
* Never invent hosts, timestamps, root causes, failure patterns, impacts, or remediation steps.
* Never assume a failure pattern unless it is clearly supported by the template sequence or root-cause information.
* If there is insufficient evidence, explicitly state that the available data is insufficient to determine the failure pattern or root cause.
* If log_count is 0, state that no logs are associated with the incident and that a meaningful root-cause analysis cannot be performed.
* If root causes are listed as "none identified", state that no root cause candidate has been identified.
* If template information is missing, state that no template sequence is available.
* Do not exaggerate severity or impact.
* Do not mention technologies (OSPF, BGP, STP, etc.) unless they are clearly indicated by the provided data.
* No markdown. No bullet points. Write 3-5 concise, factual, conservative sentences."""


def _build_single_prompt(ctx: dict) -> str:
    return (
        "You are a network operations assistant for HPE CX network switches. "
        "Summarise the following incident in 3-5 plain English sentences for a network engineer.\n\n"
        + _RULES
        + "\n\nIncident "
        + str(ctx.get("correlation_id", "")) + ":\n"
        + "  Host: " + str(ctx.get("host", "unknown")) + "\n"
        + "  Duration: " + str(ctx.get("duration_seconds", 0)) + "s | Logs: " + str(ctx.get("log_count", 0)) + "\n"
        + "  Templates: " + str(ctx.get("template_sequence", "(unknown)")) + "\n"
        + "  Top logs: " + str(ctx.get("top3_logs", "N/A")) + "\n"
        + "  Root causes: " + str(ctx.get("root_causes", "none")) + "\n"
        + "  Cross-system: " + str(ctx.get("is_cross_system", False)) + "\n"
    )


def _build_batch_prompt(batch: list[dict]) -> str:
    incidents_text = ""
    for ctx in batch:
        incidents_text += (
            "\n---\n"
            + str(ctx["correlation_id"]) + "\n"
            + "Host: " + str(ctx["host"])
            + " | Duration: " + str(ctx["duration_seconds"]) + "s"
            + " | Logs: " + str(ctx["log_count"]) + "\n"
            + "Templates: " + str(ctx["template_sequence"]) + "\n"
            + "Top logs: " + str(ctx["top3_logs"]) + "\n"
            + "Root causes: " + str(ctx["root_causes"]) + "\n"
            + "Cross-system: " + str(ctx["is_cross_system"]) + "\n"
        )

    # NOTE: built via concatenation (not an f-string) so the literal JSON braces
    # in the example are never re-parsed as format fields — the bug that
    # corrupted the old Gemini prompt.
    return (
        "You are a network operations assistant for HPE CX network switches. "
        "Summarise each incident below for a network engineer.\n\n"
        + _RULES
        + "\n\nReturn ONLY a valid JSON object with a single key \"summaries\" whose value "
        "is an array of objects. No markdown, no code fences, no extra text.\n"
        "Format:\n"
        '{"summaries": [{"correlation_id": "INC-0001", "summary_text": "..."}]}\n\n'
        "Incidents:\n"
        + incidents_text
    )


# ---------------------------------------------------------------------------
# Batch call + per-incident fallback
# ---------------------------------------------------------------------------

def _call_batch(batch: list[dict]) -> list[dict]:
    try:
        text = _generate(_build_batch_prompt(batch), json_mode=True)
        parsed = _parse_summary_list(text)
        # Keep only ids we asked for; preserve any returned.
        wanted = {ctx["correlation_id"] for ctx in batch}
        valid = [r for r in parsed if r.get("correlation_id") in wanted]
        if valid:
            return valid
        raise ValueError("Batch response contained no matching incident ids")
    except Exception as exc:
        logger.warning("Batch generation failed (%s) — falling back to individual calls", exc)
        return _fallback_individual(batch)


def _fallback_individual(batch: list[dict]) -> list[dict]:
    """Fallback when batch fails — one call per incident, never raises."""
    results = []
    for ctx in batch:
        try:
            text = _generate(_build_single_prompt(ctx))
            results.append({"correlation_id": ctx["correlation_id"], "summary_text": text})
        except Exception as exc:
            logger.warning("Individual summary failed for %s: %s", ctx["correlation_id"], exc)
            results.append(
                {"correlation_id": ctx["correlation_id"], "summary_text": "Summary unavailable."}
            )
    return results


# ---------------------------------------------------------------------------
# Public entry point 1: pipeline batch generation
# ---------------------------------------------------------------------------

def generate_all_summaries(
    scored_df: pd.DataFrame,
    root_causes_df: pd.DataFrame,
    batch_size: int = 20,
) -> None:
    """
    Batch-generates LLM summaries for all new incidents in scored_df.
    Skips incidents that already have a cached summary in Postgres.
    Writes all new summaries via write_summaries_batch() — single transaction.
    """
    # Resolve whether the dashboard dir (data.db) or project root
    # (dashboard.data.db) is on sys.path — both runtime contexts occur.
    try:
        from data.db import get_summary, write_summaries_batch
    except ImportError:
        from dashboard.data.db import get_summary, write_summaries_batch

    if "correlation_id" not in scored_df.columns:
        logger.warning("scored_df has no correlation_id column — skipping summary generation")
        return

    incident_ids = scored_df["correlation_id"].dropna().unique().tolist()
    if not incident_ids:
        logger.info("No incidents in scored_df — skipping summary generation")
        return

    uncached = [cid for cid in incident_ids if get_summary(cid) is None]
    if not uncached:
        logger.info("All incident summaries already cached — skipping Groq calls")
        return

    logger.info(
        "Generating summaries for %d incidents in batches of %d",
        len(uncached),
        batch_size,
    )

    contexts = [_build_context(cid, scored_df, root_causes_df) for cid in uncached]

    all_summaries: list[dict] = []
    for i in range(0, len(contexts), batch_size):
        batch = contexts[i: i + batch_size]
        summaries = _call_batch(batch)
        all_summaries.extend(summaries)
        logger.info("Batch %d: %d summaries generated", i // batch_size + 1, len(summaries))

    write_summaries_batch(all_summaries)
    logger.info("Cached %d summaries to Postgres", len(all_summaries))


# ---------------------------------------------------------------------------
# Public entry point 2: dashboard Regenerate button
# ---------------------------------------------------------------------------

def regenerate_summary(correlation_id: str, incident_data: dict) -> str:
    """
    Bypass cache, call Groq, write new text back to summaries table.
    Called only from the dashboard Regenerate button. Never raises.
    """
    try:
        from data.db import write_summary
    except ImportError:
        from dashboard.data.db import write_summary

    ctx = {
        "correlation_id": incident_data.get("correlation_id", correlation_id),
        "host": incident_data.get("host", "unknown"),
        "duration_seconds": incident_data.get("duration_seconds", 0),
        "log_count": incident_data.get("log_count", 0),
        "template_sequence": incident_data.get("template_sequence", "(unknown)"),
        "top3_logs": incident_data.get("top3_logs", "N/A"),
        "root_causes": incident_data.get("root_causes", "none"),
        "is_cross_system": incident_data.get("is_cross_system", False),
    }

    try:
        text = _generate(_build_single_prompt(ctx))
        write_summary(correlation_id, text)
        return text
    except Exception as exc:
        if _get_client() is None:
            return "Summary unavailable — GROQ_API_KEY not configured. Set it in your .env file."
        logger.warning("regenerate_summary failed: %s", exc)
        return "Summary unavailable — API unreachable. Click Regenerate to try again."
