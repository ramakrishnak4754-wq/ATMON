#!/usr/bin/env python3
"""
atmon_crawler.py

Sharded crawler for GitHub Actions.

Features:
- SHARD_ID: 0-4
- NUM_SHARDS: 5
- TARGET_PAGES: 3000 per shard
- ThreadPoolExecutor: 8 workers
- REQUEST_DELAY: 0.5 seconds per origin, per process
- robots.txt compliance (fail closed if unavailable)
- Wikipedia, BBC, CNN, Reuters domain allowlist
- HTML text cleaning
- Minimum cleaned text length: 400 characters
- Snippets up to 900 characters
- Flat-list metadata.json format
- Downloads existing Hugging Face metadata.json
- Merges existing and newly crawled records
- Deduplicates by normalized URL
- Keeps the latest 100,000 records
- Uploads merged metadata.json to the repository root
- Maximum discovered URL queue: 50,000

Required packages:
    requests
    beautifulsoup4
    huggingface_hub

Required GitHub Actions secret:
    HF_TOKEN
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import sys
import threading
import time

from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import (
    parse_qsl,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# Configuration
# ============================================================

USER_AGENT = (
    "AtmonResearchCrawler/1.0 "
    "(educational research; contact: crawler-maintainer)"
)

ALLOWED_DOMAINS = (
    "wikipedia.org",
    "bbc.com",
    "bbc.co.uk",
    "cnn.com",
    "reuters.com",
)

SHARD_ID = int(os.getenv("SHARD_ID", "0"))
NUM_SHARDS = int(os.getenv("NUM_SHARDS", "5"))

TARGET_PAGES = int(os.getenv("TARGET_PAGES", "3000"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "100000"))

MAX_QUEUE_SIZE = 50000
WORKERS = 8

MIN_TEXT_CHARS = 400
SNIPPET_CHARS = 900

REQUEST_DELAY = 0.5
REQUEST_TIMEOUT = (5, 20)

MAX_CRAWL_DEPTH = 2
MAX_HTML_BYTES = 3_000_000

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "metadata.json")

HF_TOKEN = os.getenv("HF_TOKEN", "").strip()
HF_DATASET = os.getenv(
    "HF_DATASET",
    "HarshaAtmon10/Crawler",
).strip()

HF_FILENAME = "metadata.json"

TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref_src",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}

SEED_URLS = [
    "https://en.wikipedia.org/wiki/Main_Page",
    "https://www.bbc.com/news",
    "https://www.bbc.co.uk/news",
    "https://edition.cnn.com/",
    "https://www.reuters.com/",
]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
)

logger = logging.getLogger("atmon_crawler")

if NUM_SHARDS != 5:
    logger.warning(
        "NUM_SHARDS is %s; the recommended configuration is 5.",
        NUM_SHARDS,
    )

if not 0 <= SHARD_ID < NUM_SHARDS:
    raise ValueError(
        f"SHARD_ID must be between 0 and {NUM_SHARDS - 1}"
    )

if TARGET_PAGES < 1:
    raise ValueError("TARGET_PAGES must be at least 1.")

if MAX_PAGES < 1:
    raise ValueError("MAX_PAGES must be at least 1.")


# ============================================================
# URL helpers
# ============================================================

def get_domain(url: str) -> str:
    """Return the hostname without the www. prefix."""
    hostname = (urlsplit(url).hostname or "").lower()

    if hostname.startswith("www."):
        hostname = hostname[4:]

    return hostname


def is_allowed_domain(url: str) -> bool:
    """Check whether a URL belongs to an allowed publisher."""
    try:
        parts = urlsplit(url)

        if parts.scheme not in {"http", "https"}:
            return False

        if not parts.hostname:
            return False

        if parts.username is not None or parts.password is not None:
            return False

        hostname = parts.hostname.lower()

        return any(
            hostname == domain
            or hostname.endswith("." + domain)
            for domain in ALLOWED_DOMAINS
        )

    except ValueError:
        return False


def normalize_url(url: str) -> str | None:
    """Normalize a URL and remove common tracking parameters."""
    try:
        parts = urlsplit(url.strip())

        if parts.scheme.lower() not in {"http", "https"}:
            return None

        if not parts.hostname:
            return None

        if parts.username is not None or parts.password is not None:
            return None

        if not is_allowed_domain(url):
            return None

        hostname = parts.hostname.lower()

        if hostname.startswith("www."):
            hostname = hostname[4:]

        port = parts.port

        if (
            port is None
            or (parts.scheme.lower() == "https" and port == 443)
            or (parts.scheme.lower() == "http" and port == 80)
        ):
            netloc = hostname
        else:
            netloc = f"{hostname}:{port}"

        path = re.sub(r"/{2,}", "/", parts.path or "/")

        if path != "/":
            path = path.rstrip("/")

        query_parameters = [
            (key, value)
            for key, value in parse_qsl(
                parts.query,
                keep_blank_values=True,
            )
            if key.lower() not in TRACKING_PARAMETERS
            and not key.lower().startswith("utm_")
        ]

        query_parameters.sort()

        query = urlencode(query_parameters, doseq=True)

        return urlunsplit(
            (
                parts.scheme.lower(),
                netloc,
                path,
                query,
                "",
            )
        )

    except (TypeError, ValueError):
        return None


def belongs_to_shard(url: str) -> bool:
    """Assign URLs deterministically across shards."""
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()

    return int(digest[:16], 16) % NUM_SHARDS == SHARD_ID


# ============================================================
# HTTP sessions
# ============================================================

_thread_local = threading.local()


def make_session() -> requests.Session:
    """Create a requests session with bounded retries."""
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=2,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=WORKERS,
        pool_maxsize=WORKERS,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9",
            "Accept-Language": "en-US,en;q=0.8",
        }
    )

    return session


def get_session() -> requests.Session:
    """Use one session per worker thread."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_session()

    return _thread_local.session


# ============================================================
# robots.txt and rate limiting
# ============================================================

class RobotsManager:
    """Cache robots.txt and apply per-origin request delays."""

    def __init__(self) -> None:
        self._cache: dict[str, RobotFileParser] = {}
        self._lock = threading.Lock()

        self._delay_lock = threading.Lock()
        self._next_request_time: dict[str, float] = {}

    def get_parser(self, url: str) -> RobotFileParser | None:
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"

        with self._lock:
            cached = self._cache.get(origin)

            if cached is not None:
                return cached

        parser = RobotFileParser()
        robots_url = urljoin(origin + "/", "robots.txt")
        parser.set_url(robots_url)

        try:
            with get_session().get(
                robots_url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=False,
                stream=True,
            ) as response:

                if response.status_code == 200:
                    content = response.raw.read(
                        512_000,
                        decode_content=True,
                    ).decode("utf-8", errors="replace")

                    parser.parse(content.splitlines())

                elif response.status_code == 404:
                    parser.parse(
                        ["User-agent: *", "Allow: /"]
                    )

                else:
                    # Fail closed when permission is uncertain.
                    parser.parse(
                        ["User-agent: *", "Disallow: /"]
                    )

        except requests.RequestException as exc:
            logger.warning(
                "robots.txt check failed for %s: %s",
                origin,
                exc,
            )

            # Do not crawl an origin if its robots policy cannot be read.
            parser.parse(
                ["User-agent: *", "Disallow: /"]
            )

        with self._lock:
            parser = self._cache.setdefault(origin, parser)

        return parser

    def can_fetch(self, url: str) -> bool:
        parser = self.get_parser(url)

        if parser is None:
            return False

        try:
            return parser.can_fetch(USER_AGENT, url)
        except Exception:
            return False

    def wait_turn(self, url: str) -> None:
        """
        Coordinate a minimum request interval per origin
        within this process.
        """
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"

        with self._delay_lock:
            now = time.monotonic()
            next_allowed = self._next_request_time.get(origin, now)

            delay = max(0.0, next_allowed - now)

            self._next_request_time[origin] = (
                max(now, next_allowed) + REQUEST_DELAY
            )

        if delay:
            time.sleep(delay + random.uniform(0.0, 0.1))


robots = RobotsManager()


# ============================================================
# HTML cleaning and extraction
# ============================================================

REMOVE_TAGS = [
    "script",
    "style",
    "nav",
    "footer",
    "header",
    "aside",
    "noscript",
    "iframe",
    "svg",
    "canvas",
    "form",
    "button",
    "input",
    "select",
    "textarea",
]


def clean_html(
    html: str,
    page_url: str,
) -> tuple[str, str, list[str]]:
    """Extract a title, cleaned text, and same-allowlist links."""
    soup = BeautifulSoup(html, "html.parser")

    title = ""

    if soup.title:
        title = soup.title.get_text(" ", strip=True)

    for element in soup.find_all(REMOVE_TAGS):
        element.decompose()

    for element in soup.select(
        '[aria-hidden="true"], '
        '[hidden], '
        '[role="navigation"], '
        '[role="banner"], '
        '[role="contentinfo"]'
    ):
        element.decompose()

    main = (
        soup.find("article")
        or soup.find("main")
        or soup.find(id="content")
        or soup.body
        or soup
    )

    text = main.get_text(" ", strip=True)

    text = re.sub(
        r"[\x00-\x08\x0b\x0c\x0e-\x1f]",
        " ",
        text,
    )

    text = re.sub(r"\s+", " ", text).strip()

    links = []

    for anchor in soup.find_all("a", href=True):
        absolute = urljoin(page_url, anchor["href"])
        normalized = normalize_url(absolute)

        if normalized:
            links.append(normalized)

    links = list(dict.fromkeys(links))

    return title, text, links


# ============================================================
# Fetch pages
# ============================================================

def fetch_page(
    url: str,
) -> tuple[dict | None, list[str]]:
    """Fetch a permitted page and return its metadata and links."""
    normalized = normalize_url(url)

    if not normalized:
        return None, []

    if not robots.can_fetch(normalized):
        logger.debug("Blocked by robots.txt: %s", normalized)
        return None, []

    robots.wait_turn(normalized)

    try:
        with get_session().get(
            normalized,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            stream=True,
        ) as response:

            final_url = normalize_url(response.url)

            if not final_url:
                return None, []

            if not is_allowed_domain(response.url):
                return None, []

            if response.status_code != 200:
                logger.debug(
                    "HTTP %s for %s",
                    response.status_code,
                    normalized,
                )
                return None, []

            content_type = response.headers.get(
                "Content-Type",
                "",
            ).lower()

            if (
                "text/html" not in content_type
                and "application/xhtml+xml" not in content_type
            ):
                return None, []

            content_length = response.headers.get("Content-Length")

            if content_length:
                try:
                    if int(content_length) > MAX_HTML_BYTES:
                        return None, []
                except ValueError:
                    pass

            body = bytearray()

            for chunk in response.iter_content(
                chunk_size=65536,
            ):
                if not chunk:
                    continue

                body.extend(chunk)

                if len(body) > MAX_HTML_BYTES:
                    return None, []

            encoding = response.encoding or "utf-8"

            html = bytes(body).decode(
                encoding,
                errors="replace",
            )

        title, text, links = clean_html(html, final_url)

        if len(text) < MIN_TEXT_CHARS:
            return None, links

        record = {
            "title": (title or final_url)[:500],
            "url": final_url,
            "snippet": text[:SNIPPET_CHARS],
            "domain": get_domain(final_url),
        }

        return record, links

    except (
        requests.RequestException,
        UnicodeError,
        ValueError,
    ) as exc:
        logger.debug("Fetch failed for %s: %s", normalized, exc)
        return None, []


# ============================================================
# Crawl orchestration
# ============================================================

class Crawler:
    def __init__(self) -> None:
        self.seen_urls: set[str] = set()
        self.records: dict[str, dict] = {}

        self.queue: deque[tuple[str, int]] = deque()
        self.lock = threading.Lock()

    def enqueue(self, url: str, depth: int) -> None:
        normalized = normalize_url(url)

        if not normalized or not belongs_to_shard(normalized):
            return

        with self.lock:
            if normalized in self.seen_urls:
                return

            if len(self.seen_urls) >= MAX_QUEUE_SIZE:
                return

            self.seen_urls.add(normalized)
            self.queue.append((normalized, depth))

    def initialize(self) -> None:
        for seed in SEED_URLS:
            self.enqueue(seed, 0)

    def crawl(self) -> list[dict]:
        self.initialize()

        logger.info(
            "Starting shard %s/%s: target=%s workers=%s queue_cap=%s",
            SHARD_ID,
            NUM_SHARDS,
            TARGET_PAGES,
            WORKERS,
            MAX_QUEUE_SIZE,
        )

        with ThreadPoolExecutor(
            max_workers=WORKERS,
            thread_name_prefix="crawler",
        ) as executor:

            while True:
                with self.lock:
                    if len(self.records) >= TARGET_PAGES:
                        logger.info("Shard page target reached.")
                        break

                    if not self.queue:
                        logger.info(
                            "Queue empty; stopping gracefully."
                        )
                        break

                    batch = []

                    while self.queue and len(batch) < WORKERS:
                        batch.append(self.queue.popleft())

                futures = {
                    executor.submit(fetch_page, url): (url, depth)
                    for url, depth in batch
                }

                for future in as_completed(futures):
                    url, depth = futures[future]

                    try:
                        record, links = future.result()
                    except Exception:
                        logger.exception(
                            "Worker failed for %s",
                            url,
                        )
                        continue

                    if record is not None:
                        normalized = normalize_url(record["url"])

                        if normalized:
                            record["url"] = normalized

                            with self.lock:
                                if normalized not in self.records:
                                    self.records[normalized] = record

                        logger.info(
                            "Accepted %s/%s: %s",
                            len(self.records),
                            TARGET_PAGES,
                            url,
                        )

                    if depth < MAX_CRAWL_DEPTH:
                        for link in links:
                            self.enqueue(link, depth + 1)

                logger.info(
                    "Progress: accepted=%s queue=%s discovered=%s",
                    len(self.records),
                    len(self.queue),
                    len(self.seen_urls),
                )

        return list(self.records.values())


# ============================================================
# Existing Hugging Face metadata
# ============================================================

def download_existing_metadata() -> list[dict]:
    """
    Download root metadata.json from the Hugging Face dataset.

    Returns an empty list if the file does not exist.

    Raises an error for invalid JSON or an unexpected file structure
    rather than silently discarding existing dataset records.
    """
    if not HF_TOKEN:
        raise RuntimeError(
            "HF_TOKEN is required to download and merge the dataset."
        )

    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import EntryNotFoundError

    try:
        downloaded_path = hf_hub_download(
            repo_id=HF_DATASET,
            filename=HF_FILENAME,
            repo_type="dataset",
            token=HF_TOKEN,
        )

    except EntryNotFoundError:
        logger.info(
            "Root metadata.json does not exist yet; starting a new dataset."
        )
        return []

    with open(
        downloaded_path,
        "r",
        encoding="utf-8",
    ) as file:
        existing = json.load(file)

    if not isinstance(existing, list):
        raise ValueError(
            "Expected root metadata.json to contain a flat JSON list."
        )

    valid_records = []

    for item in existing:
        if not isinstance(item, dict):
            continue

        if not all(
            isinstance(item.get(key), str)
            for key in ("title", "url", "snippet", "domain")
        ):
            continue

        normalized = normalize_url(item["url"])

        if not normalized:
            # Preserve only URLs belonging to the configured domains.
            continue

        valid_records.append(
            {
                "title": item["title"],
                "url": normalized,
                "snippet": item["snippet"][:SNIPPET_CHARS],
                "domain": get_domain(normalized),
            }
        )

    logger.info(
        "Downloaded %s valid existing records.",
        len(valid_records),
    )

    return valid_records


# ============================================================
# Merge and save
# ============================================================

def merge_records(
    existing: list[dict],
    new_records: list[dict],
) -> list[dict]:
    """
    Merge existing records and new crawl results.

    Ordering:
    - Existing records first.
    - New records afterward.
    - A newly crawled record replaces an older record with the same URL.
    - Keep the final MAX_PAGES records.

    The returned value is a flat list of dictionaries.
    """
    merged: dict[str, dict] = {}

    for record in existing:
        normalized = normalize_url(record.get("url", ""))

        if not normalized:
            continue

        merged[normalized] = {
            "title": str(record.get("title", "")),
            "url": normalized,
            "snippet": str(record.get("snippet", ""))[:SNIPPET_CHARS],
            "domain": get_domain(normalized),
        }

    for record in new_records:
        normalized = normalize_url(record.get("url", ""))

        if not normalized:
            continue

        # Remove and reinsert so refreshed records count as newest.
        merged.pop(normalized, None)

        merged[normalized] = {
            "title": str(record.get("title", "")),
            "url": normalized,
            "snippet": str(record.get("snippet", ""))[:SNIPPET_CHARS],
            "domain": get_domain(normalized),
        }

    # Preserve the most recently inserted MAX_PAGES records.
    result = list(merged.values())[-MAX_PAGES:]

    logger.info(
        "Merge complete: existing=%s new=%s final=%s",
        len(existing),
        len(new_records),
        len(result),
    )

    return result


def save_metadata(records: list[dict]) -> None:
    """Save a flat JSON list for the frontend."""
    temporary_path = OUTPUT_FILE + ".tmp"

    with open(
        temporary_path,
        "w",
        encoding="utf-8",
    ) as output:
        json.dump(
            records,
            output,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(temporary_path, OUTPUT_FILE)

    logger.info(
        "Saved %s records to %s",
        len(records),
        OUTPUT_FILE,
    )


# ============================================================
# Hugging Face upload
# ============================================================

def upload_metadata() -> None:
    """Upload metadata.json to the dataset repository root."""
    if not HF_TOKEN:
        raise RuntimeError("HF_TOKEN is required for dataset upload.")

    from huggingface_hub import HfApi

    api = HfApi(token=HF_TOKEN)

    api.create_repo(
        repo_id=HF_DATASET,
        repo_type="dataset",
        exist_ok=True,
        private=True,
    )

    api.upload_file(
        path_or_fileobj=OUTPUT_FILE,
        path_in_repo="metadata.json",
        repo_id=HF_DATASET,
        repo_type="dataset",
        commit_message=(
            f"Update merged crawler metadata from shard {SHARD_ID}"
        ),
    )

    logger.info(
        "Uploaded root metadata.json to %s",
        HF_DATASET,
    )


# ============================================================
# Main
# ============================================================

def main() -> int:
    started = time.monotonic()

    try:
        crawler = Crawler()
        new_records = crawler.crawl()

        # Fetch the latest root dataset file after crawling.
        existing_records = download_existing_metadata()

        # Merge, deduplicate, and retain at most 100,000 records.
        merged_records = merge_records(
            existing_records,
            new_records,
        )

        # The frontend expects a flat list, not a wrapper object.
        save_metadata(merged_records)

        # Upload the merged file to the repository root.
        upload_metadata()

        logger.info(
            "Shard %s finished: crawled=%s total_dataset=%s elapsed=%.1fs",
            SHARD_ID,
            len(new_records),
            len(merged_records),
            time.monotonic() - started,
        )

        return 0

    except KeyboardInterrupt:
        logger.warning("Crawler interrupted.")
        return 130

    except Exception:
        logger.exception("Crawler failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
