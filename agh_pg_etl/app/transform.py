"""
AGH /querylog API response → normalised dns_queries row dict.

AGH API field reference (confirmed against live instance):
  client          — client IP string
  client_id       — persistent client ID (if configured)
  client_info     — {name, whois, disallowed, disallowed_rule}
  client_proto    — "" / "doh" / "dot" / "doq" / "doh3"
  question        — {name, type, class}
  time            — RFC3339 with nanoseconds
  elapsedMs       — string, already in milliseconds
  reason          — "NotFilteredNotFound" | "FilteredBlackList" | "NotFilteredWhiteList" | ...
  status          — DNS rcode string: "NOERROR" | "NXDOMAIN" | "SERVFAIL" | ...
  rules           — [{filter_list_id, text}]
  rule            — shorthand of matched rule text
  answer          — [{type, value, ttl}] or absent
  cached          — bool
  upstream        — upstream resolver URL/addr

All AGH field names are centralised in _field_map().
Nothing outside this module should reference raw API keys.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import tldextract
from pydantic import BaseModel, field_validator

log = logging.getLogger(__name__)

# tldextract with local suffix list only (no live fetch in production)
_tld = tldextract.TLDExtract(suffix_list_urls=[])

# AGH reason strings that indicate a blocked query
_BLOCKED_REASONS = {
    "FilteredBlackList",
    "FilteredBlockedService",
    "FilteredParental",
    "FilteredSafeBrowsing",
    "FilteredSafeSearch",
    "FilteredInvalid",
    "Rewrite",           # custom rewrite (treat as modified, not blocked)
}
_TRULY_BLOCKED = {
    "FilteredBlackList",
    "FilteredBlockedService",
    "FilteredParental",
    "FilteredSafeBrowsing",
    "FilteredSafeSearch",
    "FilteredInvalid",
}


# ──────────────────────────────────────────────────────────────────────────
# Time helpers
# ──────────────────────────────────────────────────────────────────────────

def get_time_segment(hour: int) -> str:
    if 0 <= hour <= 5:
        return "late_night"
    elif 6 <= hour <= 11:
        return "morning"
    elif 12 <= hour <= 17:
        return "afternoon"
    else:
        return "evening"


def parse_event_time(raw: dict) -> datetime:
    raw_t: str = raw.get("time") or ""
    if not raw_t:
        log.warning("Missing 'time' field in record")
        return datetime.now(tz=timezone.utc)
    # AGH emits RFC3339 with nanoseconds e.g. "2026-04-17T12:19:25.596380346Z"
    # Python fromisoformat handles up to microseconds — truncate sub-µs
    ts = raw_t[:26].rstrip("Z") + "+00:00"
    return datetime.fromisoformat(ts)


# ──────────────────────────────────────────────────────────────────────────
# Field mapper — all AGH API field names live here
# ──────────────────────────────────────────────────────────────────────────

def _field_map(raw: dict) -> dict:
    """
    Normalise AGH /querylog API JSON into a stable intermediate dict.
    AGH API field names are ONLY referenced in this function.
    """
    ci: dict        = raw.get("client_info") or {}
    question: dict  = raw.get("question") or {}
    rules: list     = raw.get("rules") or []

    # block_reason: prefer 'rule' shorthand, fall back to first rules[].text
    block_text: str = (
        raw.get("rule")
        or (rules[0].get("text") if rules else "")
        or ""
    )

    return {
        # client
        "client_ip":    raw.get("client") or "",
        "client_name":  ci.get("name") or "",
        "client_id":    raw.get("client_id") or "",  # persistent AGH client ID if set
        "protocol":     raw.get("client_proto") or "",

        # query
        "qname":        question.get("name") or "",
        "qtype":        question.get("type") or "UNKNOWN",

        # result
        "reason":       raw.get("reason") or "",
        "rcode":        raw.get("status") or "",      # DNS rcode: NOERROR, NXDOMAIN…
        "block_text":   block_text,
        "cached":       bool(raw.get("cached")),

        # timing — already in ms as string
        "elapsed_ms_str": str(raw.get("elapsedMs") or "0"),

        # upstream resolver
        "upstream":     raw.get("upstream") or "",

        # answer payload (decoded list or None)
        "answer":       raw.get("answer"),
    }


# ──────────────────────────────────────────────────────────────────────────
# Derived field builders
# ──────────────────────────────────────────────────────────────────────────

def _make_client_key(client_id: str, client_name: str, client_ip: str) -> str:
    if client_id:
        return f"agh:{client_id}"
    if client_name:
        return f"name:{client_name}"
    if client_ip:
        return f"ip:{client_ip}"
    digest = hashlib.sha256(f"{client_id}{client_name}{client_ip}".encode()).hexdigest()[:8]
    return f"unknown:{digest}"


def _make_root_domain(qname: str) -> str:
    if not qname:
        return ""
    try:
        ext = _tld(qname)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
    except Exception:
        pass
    return qname


def _make_response_status(reason: str, cached: bool) -> str:
    if reason in _TRULY_BLOCKED:
        return "blocked"
    if reason == "Rewrite":
        return "rewrite"
    if cached:
        return "cached"
    if reason.startswith("NotFiltered"):
        return "allowed"
    return reason.lower() or "allowed"


def _make_elapsed_ms(elapsed_str: str) -> float | None:
    try:
        v = float(elapsed_str)
        return round(v, 3) if v > 0 else None
    except (ValueError, TypeError):
        return None


def _make_fingerprint(
    event_time: datetime,
    client_key: str,
    qname: str,
    qtype: str,
    response_status: str,
) -> str:
    raw = f"{event_time.isoformat()}{client_key}{qname}{qtype}{response_status}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────────────────
# Pydantic output schema
# ──────────────────────────────────────────────────────────────────────────

class DnsQueryRow(BaseModel):
    event_time:         datetime
    event_date:         str       # YYYY-MM-DD string for PG date
    event_hour:         int
    day_of_week:        int       # 1=Mon … 7=Sun
    is_weekend:         bool
    time_segment:       str
    client_key:         str
    client_name:        str | None
    client_ip:          str | None
    qname:              str
    root_domain:        str
    qtype:              str
    response_status:    str
    block_reason:       str | None
    rcode:              str | None
    upstream:           str | None
    elapsed_ms:         float | None
    answers_json:       str | None
    raw_json:           str
    event_fingerprint:  str

    @field_validator("elapsed_ms")
    @classmethod
    def round_elapsed(cls, v: float | None) -> float | None:
        return round(v, 3) if v is not None else None


# ──────────────────────────────────────────────────────────────────────────
# Public entry point
# ──────────────────────────────────────────────────────────────────────────

def transform_record(raw: dict) -> DnsQueryRow | None:
    """
    Convert one raw AGH /querylog API dict into a DnsQueryRow.
    Returns None if the record is invalid/unparseable.
    """
    try:
        event_time = parse_event_time(raw)
        fm = _field_map(raw)

        client_key      = _make_client_key(fm["client_id"], fm["client_name"], fm["client_ip"])
        root_domain     = _make_root_domain(fm["qname"])
        response_status = _make_response_status(fm["reason"], fm["cached"])
        block_reason    = (fm["block_text"][:500] or None) if fm["block_text"] else None
        elapsed_ms      = _make_elapsed_ms(fm["elapsed_ms_str"])

        answers_json = json.dumps(fm["answer"]) if fm["answer"] is not None else None
        fingerprint  = _make_fingerprint(
            event_time, client_key, fm["qname"], fm["qtype"], response_status
        )

        hour = event_time.hour
        dow  = event_time.isoweekday()   # 1=Mon … 7=Sun

        return DnsQueryRow(
            event_time=event_time,
            event_date=event_time.strftime("%Y-%m-%d"),
            event_hour=hour,
            day_of_week=dow,
            is_weekend=(dow >= 6),
            time_segment=get_time_segment(hour),
            client_key=client_key,
            client_name=fm["client_name"] or None,
            client_ip=fm["client_ip"] or None,
            qname=fm["qname"],
            root_domain=root_domain,
            qtype=fm["qtype"],
            response_status=response_status,
            block_reason=block_reason,
            rcode=fm["rcode"] or None,
            upstream=fm["upstream"] or None,
            elapsed_ms=elapsed_ms,
            answers_json=answers_json,
            raw_json=json.dumps(raw, ensure_ascii=False),
            event_fingerprint=fingerprint,
        )

    except Exception as exc:
        log.warning("transform_record failed: %s | raw=%s", exc, str(raw)[:200])
        return None
