"""Tests for transform.py — uses AGH /querylog API format"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from transform import transform_record, get_time_segment, _make_root_domain, _make_client_key


# ── Fixtures (actual AGH API format) ─────────────────────────────────────

SAMPLE_ALLOWED = {
    "answer": [{"type": "A", "value": "47.237.132.218", "ttl": 12}],
    "answer_dnssec": False,
    "cached": True,
    "client": "10.10.10.220",
    "client_info": {"whois": {}, "name": "小米AX3600", "disallowed": False, "disallowed_rule": ""},
    "client_proto": "",
    "elapsedMs": "0.102384",
    "question": {"class": "IN", "name": "api.miwifi.com", "type": "A"},
    "reason": "NotFilteredNotFound",
    "rules": [],
    "status": "NOERROR",
    "time": "2026-04-17T12:19:30.228702003Z",
    "upstream": "https://dns10.quad9.net:443/dns-query",
}

SAMPLE_BLOCKED = {
    "answer_dnssec": False,
    "cached": False,
    "client": "10.10.10.54",
    "client_info": {"whois": {}, "name": "Mi Camera", "disallowed": False, "disallowed_rule": ""},
    "client_proto": "",
    "elapsedMs": "0.14994",
    "filterId": 0,
    "question": {"class": "IN", "name": "37.146.120.61.in-addr.arpa", "type": "PTR"},
    "reason": "FilteredBlackList",
    "rule": "||*.in-addr.arpa^$client=10.10.10.54",
    "rules": [{"filter_list_id": 0, "text": "||*.in-addr.arpa^$client=10.10.10.54"}],
    "status": "NOERROR",
    "time": "2026-04-17T12:20:03.512044212Z",
    "upstream": "",
}

SAMPLE_NXDOMAIN = {
    "answer_dnssec": False,
    "cached": True,
    "client": "10.10.10.228",
    "client_info": {"whois": {}, "name": "johnny.lan", "disallowed": False, "disallowed_rule": ""},
    "client_proto": "",
    "elapsedMs": "0.157166",
    "question": {"class": "IN", "name": "immich-machine-learning.lan", "type": "A"},
    "reason": "NotFilteredNotFound",
    "rules": [],
    "status": "NXDOMAIN",
    "time": "2026-04-17T12:19:25.596380346Z",
    "upstream": "1.1.1.1:53",
}


# ── time_segment ──────────────────────────────────────────────────────────

def test_time_segment_late_night():
    assert get_time_segment(0) == "late_night"
    assert get_time_segment(5) == "late_night"

def test_time_segment_morning():
    assert get_time_segment(6) == "morning"
    assert get_time_segment(11) == "morning"

def test_time_segment_afternoon():
    assert get_time_segment(12) == "afternoon"
    assert get_time_segment(17) == "afternoon"

def test_time_segment_evening():
    assert get_time_segment(18) == "evening"
    assert get_time_segment(23) == "evening"


# ── root_domain ───────────────────────────────────────────────────────────

def test_root_domain_subdomain():
    assert _make_root_domain("www.youtube.com") == "youtube.com"
    assert _make_root_domain("m.youtube.com") == "youtube.com"

def test_root_domain_apex():
    assert _make_root_domain("youtube.com") == "youtube.com"

def test_root_domain_deep():
    assert _make_root_domain("a.b.c.google.com") == "google.com"

def test_root_domain_empty():
    assert _make_root_domain("") == ""


# ── client_key ────────────────────────────────────────────────────────────

def test_client_key_agh_id():
    assert _make_client_key("myphone", "iPhone", "192.168.1.1") == "agh:myphone"

def test_client_key_name_fallback():
    assert _make_client_key("", "M1", "192.168.1.5") == "name:M1"

def test_client_key_ip_fallback():
    assert _make_client_key("", "", "10.0.0.1") == "ip:10.0.0.1"

def test_client_key_unknown():
    key = _make_client_key("", "", "")
    assert key.startswith("unknown:")


# ── full record transform ─────────────────────────────────────────────────

def test_transform_allowed_cached():
    row = transform_record(SAMPLE_ALLOWED)
    assert row is not None
    assert row.response_status == "cached"
    assert row.qname == "api.miwifi.com"
    assert row.root_domain == "miwifi.com"
    assert row.qtype == "A"
    assert row.client_ip == "10.10.10.220"
    assert row.client_name == "小米AX3600"
    assert row.client_key == "name:小米AX3600"
    assert row.rcode == "NOERROR"
    assert abs(row.elapsed_ms - 0.102) < 0.01
    assert row.event_fingerprint


def test_transform_blocked():
    row = transform_record(SAMPLE_BLOCKED)
    assert row is not None
    assert row.response_status == "blocked"
    assert row.block_reason == "||*.in-addr.arpa^$client=10.10.10.54"
    assert row.client_name == "Mi Camera"
    assert row.client_key == "name:Mi Camera"


def test_transform_nxdomain():
    row = transform_record(SAMPLE_NXDOMAIN)
    assert row is not None
    assert row.response_status == "cached"
    assert row.rcode == "NXDOMAIN"
    assert row.qname == "immich-machine-learning.lan"


def test_transform_dedup_deterministic():
    r1 = transform_record(SAMPLE_BLOCKED)
    r2 = transform_record(SAMPLE_BLOCKED)
    assert r1 is not None and r2 is not None
    assert r1.event_fingerprint == r2.event_fingerprint


def test_transform_answers_json():
    row = transform_record(SAMPLE_ALLOWED)
    assert row is not None
    import json
    answers = json.loads(row.answers_json)
    assert isinstance(answers, list)
    assert answers[0]["type"] == "A"


def test_transform_raw_json_preserved():
    row = transform_record(SAMPLE_BLOCKED)
    assert row is not None
    import json
    raw = json.loads(row.raw_json)
    assert raw["reason"] == "FilteredBlackList"
