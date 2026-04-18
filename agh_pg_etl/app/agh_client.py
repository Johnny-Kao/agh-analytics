"""AdGuard Home HTTP API client."""

import logging
from typing import Any, Generator

import requests
from requests.adapters import HTTPAdapter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import config

log = logging.getLogger(__name__)

_TIMEOUT = (10, 30)  # (connect, read) seconds


def _build_session() -> requests.Session:
    s = requests.Session()
    s.auth = (config.AGH_USERNAME, config.AGH_PASSWORD)
    adapter = HTTPAdapter(max_retries=0)  # tenacity handles retries
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_session = _build_session()


@retry(
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def _get(path: str, params: dict | None = None) -> Any:
    url = f"{config.AGH_BASE_URL}{path}"
    resp = _session.get(url, params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_querylog_batch(
    older_than: str | None = None,
    limit: int | None = None,
) -> dict:
    """
    Fetch one page from /querylog.

    AGH returns records newest-first.
    Use `older_than` (RFC3339 timestamp) for cursor-based pagination.
    Returns the raw API dict: {"data": [...], "oldest": "...", ...}
    """
    params: dict = {"limit": limit or config.BATCH_SIZE}
    if older_than:
        params["older_than"] = older_than
    return _get("/querylog", params)


def iter_querylog(
    start_older_than: str | None = None,
    max_batches: int = 500,
) -> Generator[tuple[list[dict], str | None], None, None]:
    """
    Yield (records, next_older_than) batches walking backwards through time.

    Stop when:
    - API returns empty data
    - API returns no 'oldest' cursor
    - max_batches exhausted
    """
    cursor = start_older_than
    for _ in range(max_batches):
        try:
            page = fetch_querylog_batch(older_than=cursor)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (401, 403):
                log.error("AGH auth failure – check credentials")
                raise
            log.warning("AGH HTTP error %s, skipping batch", exc)
            return
        except (requests.Timeout, requests.ConnectionError) as exc:
            log.error("AGH connection failed after retries: %s", exc)
            raise

        records: list[dict] = page.get("data") or []
        if not records:
            log.debug("AGH querylog: no more records")
            return

        # oldest is the RFC3339 timestamp of the last (oldest) record in this page
        next_cursor: str | None = page.get("oldest") or None

        yield records, next_cursor

        if not next_cursor:
            return
        cursor = next_cursor


def fetch_stats() -> dict:
    """Fetch /stats for sanity-check purposes only."""
    return _get("/stats")
