"""Multi-key Gemini API client with per-key cooldown and safe telemetry."""
import asyncio
import os
import re
import time

import aiohttp

BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MAX_KEYS = 10
_COOLDOWN_SECONDS = 90
_STATE = {}
_TOTAL = {"requests": 0, "success": 0, "rate_limited": 0, "invalid": 0, "errors": 0}
_NEXT_KEY_INDEX = 0


def _configured_keys():
    """Read up to ten unique keys from Render Environment; never log key values."""
    values = []
    combined = os.environ.get("GEMINI_API_KEYS", "")
    values.extend(x.strip() for x in re.split(r"[,;\n\r]+", combined) if x.strip())
    # Numbered variables are convenient when adding keys separately in Render.
    for index in range(1, MAX_KEYS + 1):
        value = os.environ.get(f"GEMINI_API_KEY_{index}", "").strip()
        if value:
            values.append(value)
    # Keep the original single-key variable compatible with existing deployments.
    legacy = os.environ.get("GEMINI_API_KEY", "").strip()
    if legacy:
        values.insert(0, legacy)
    unique = []
    seen = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            unique.append(value)
        if len(unique) >= MAX_KEYS:
            break
    return unique


def gemini_status():
    keys = _configured_keys()
    now = time.monotonic()
    rows = []
    for slot, key in enumerate(keys, 1):
        state = _STATE.get(key, {})
        until = float(state.get("cooldown_until", 0))
        rows.append({
            "slot": slot,
            "label": f"Gemini API kaliti {slot}",
            "masked": "…" + key[-4:] if len(key) >= 4 else "****",
            "state": "cooldown" if until > now else ("disabled" if state.get("disabled") else "ready"),
            "cooldown_seconds": max(0, int(until - now)),
            "success": int(state.get("success", 0)),
            "errors": int(state.get("errors", 0)),
        })
    return {
        "configured": len(keys),
        "max_keys": MAX_KEYS,
        "keys": rows,
        "totals": dict(_TOTAL),
    }


async def gemini_generate(model, payload, timeout_seconds=100):
    """Try configured keys in rotation; rate-limited keys cool down independently."""
    global _NEXT_KEY_INDEX
    keys = _configured_keys()
    if not keys:
        return None, "not_configured"

    now = time.monotonic()
    ordered = keys[_NEXT_KEY_INDEX % len(keys):] + keys[:_NEXT_KEY_INDEX % len(keys)]
    eligible = []
    cooling = []
    for key in ordered:
        state = _STATE.setdefault(key, {})
        if state.get("disabled"):
            continue
        if float(state.get("cooldown_until", 0)) > now:
            cooling.append(key)
        else:
            eligible.append(key)
    if not eligible:
        return None, "rate_limited" if cooling else "invalid_api_key"

    last_status = "network_error"
    timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=15)
    for key in eligible:
        state = _STATE.setdefault(key, {})
        _TOTAL["requests"] += 1
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                url = BASE_URL.format(model=model)
                async with session.post(url, params={"key": key}, json=payload) as response:
                    data = await response.json(content_type=None)
                    if response.status == 200:
                        state["success"] = int(state.get("success", 0)) + 1
                        _TOTAL["success"] += 1
                        state.pop("cooldown_until", None)
                        _NEXT_KEY_INDEX = (keys.index(key) + 1) % len(keys)
                        return data, "ok"
                    state["errors"] = int(state.get("errors", 0)) + 1
                    if response.status == 429:
                        state["cooldown_until"] = time.monotonic() + _COOLDOWN_SECONDS
                        _TOTAL["rate_limited"] += 1
                        last_status = "rate_limited"
                        continue
                    if response.status in (401, 403):
                        state["disabled"] = True
                        _TOTAL["invalid"] += 1
                        last_status = "invalid_api_key"
                        continue
                    if response.status >= 500:
                        state["cooldown_until"] = time.monotonic() + 20
                        _TOTAL["errors"] += 1
                        last_status = "provider_error_" + str(response.status)
                        continue
                    _TOTAL["errors"] += 1
                    return None, "provider_error_" + str(response.status)
        except asyncio.TimeoutError:
            state["cooldown_until"] = time.monotonic() + 20
            state["errors"] = int(state.get("errors", 0)) + 1
            _TOTAL["errors"] += 1
            last_status = "timeout"
        except Exception:
            state["cooldown_until"] = time.monotonic() + 20
            state["errors"] = int(state.get("errors", 0)) + 1
            _TOTAL["errors"] += 1
            last_status = "network_error"

    # If all eligible keys failed, report a useful status without exposing secrets.
    if last_status in ("rate_limited", "invalid_api_key") and cooling:
        return None, "rate_limited"
    return None, last_status
