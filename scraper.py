"""GoodFirms category scraper.

Crawls every page of one or more GoodFirms category listings
(e.g. https://www.goodfirms.co/companies/web-development-agency), extracts each
firm's details and website, and stores them as JSON.

Features
- Resumable: progress is saved after every page; re-running continues where it stopped.
- De-duplication: firms already scraped (in any category) are skipped by entity-id.
- Polite crawling: randomised delays, periodic long breaks, browser-like TLS/headers,
  exponential backoff on 429/403/5xx, and a clean stop if the site keeps refusing.
- Logging to console and to logs/scraper.log.

Usage
    uv run scraper.py https://www.goodfirms.co/companies/web-development-agency
    uv run scraper.py                      # reads URLs from categories.txt
    uv run scraper.py --max-pages 5        # small test run
    uv run scraper.py --refresh            # re-crawl finished categories for new firms
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import signal
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from curl_cffi import requests
from selectolax.lexbor import LexborHTMLParser as HTMLParser, LexborNode as Node

BASE_URL = "https://www.goodfirms.co"
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
STATE_FILE = DATA_DIR / "state.json"

TRACKING_PARAM = re.compile(r"^(utm[_a-z]*|gclid|fbclid|ref|source|campaign)$", re.I)
BLOCK_STATUSES = {403, 429, 503}

log = logging.getLogger("goodfirms")


class Blocked(Exception):
    """Raised when the site keeps refusing requests and we should stop for now."""


# --------------------------------------------------------------------------- setup


def setup_logging(verbose: bool) -> None:
    LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)

    file = RotatingFileHandler(LOG_DIR / "scraper.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    file.setFormatter(fmt)
    file.setLevel(logging.DEBUG)

    log.setLevel(logging.DEBUG)
    log.addHandler(console)
    log.addHandler(file)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write_json(path: Path, obj) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- storage


def read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class Store:
    """Per-category JSONL part files (append-only, crash-safe) plus a resumable state file.

    Firms go to data/<slug>/<slug>-001.jsonl; once a part holds `chunk_size` firms the
    next one (-002, -003, ...) is started.
    """

    def __init__(self, chunk_size: int) -> None:
        DATA_DIR.mkdir(exist_ok=True)
        self.chunk_size = chunk_size
        self.state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {"categories": {}}
        self.seen_ids: set[str] = set()
        self.dirty_parts: set[Path] = set()
        for f in DATA_DIR.glob("*/*.jsonl"):
            self.seen_ids.update(r["entity_id"] for r in read_jsonl(f))
        log.info("Loaded %d already-scraped firms from %s", len(self.seen_ids), DATA_DIR)

    def parts(self, slug: str) -> list[Path]:
        return sorted((DATA_DIR / slug).glob(f"{slug}-*.jsonl"))

    def current_part(self, slug: str) -> tuple[Path, int]:
        """The part to append to and how many firms it already holds."""
        folder = DATA_DIR / slug
        folder.mkdir(exist_ok=True)
        parts = self.parts(slug)
        if parts:
            last = parts[-1]
            count = len(read_jsonl(last))
            if count < self.chunk_size:
                return last, count
            index = int(last.stem.rsplit("-", 1)[1]) + 1
        else:
            index = 1
        return folder / f"{slug}-{index:03d}.jsonl", 0

    def category(self, slug: str) -> dict:
        return self.state["categories"].setdefault(
            slug, {"last_page_done": 0, "total_pages": None, "completed": False, "firms": 0}
        )

    def save_state(self) -> None:
        atomic_write_json(STATE_FILE, self.state)

    def append(self, slug: str, firms: list[dict]) -> int:
        """Append firms not seen before. Returns how many were new."""
        new = [f for f in firms if f["entity_id"] not in self.seen_ids]
        remaining = new
        while remaining:
            part, count = self.current_part(slug)
            batch, remaining = remaining[: self.chunk_size - count], remaining[self.chunk_size - count:]
            if count == 0:
                log.info("[%s] starting new file %s", slug, part.relative_to(ROOT))
            with part.open("a", encoding="utf-8") as fh:
                for f in batch:
                    fh.write(json.dumps(f, ensure_ascii=False) + "\n")
            self.dirty_parts.add(part)
            # Export a part as pretty JSON as soon as it is full.
            if count + len(batch) >= self.chunk_size:
                self.export_part(part)
        self.seen_ids.update(f["entity_id"] for f in new)
        return len(new)

    def export_part(self, part: Path) -> None:
        rows = read_jsonl(part)
        atomic_write_json(part.with_suffix(".json"), rows)
        self.dirty_parts.discard(part)
        log.info("Exported %d firms to %s", len(rows), part.with_suffix(".json").relative_to(ROOT))

    def export_json(self, slug: str) -> None:
        """Write pretty .json copies of the parts changed since their last export."""
        for part in self.parts(slug):
            if part in self.dirty_parts or not part.with_suffix(".json").exists():
                self.export_part(part)


# --------------------------------------------------------------------------- parsing


def text(node: Node | None) -> str | None:
    if node is None:
        return None
    value = " ".join(node.text(deep=True).split())
    return value or None


def clean_url(url: str | None) -> str | None:
    """Drop utm_* and other tracking params from a firm's website link."""
    if not url:
        return None
    parts = urlparse(url.strip())
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not TRACKING_PARAM.match(k)]
    path = re.sub(r"/{2,}", "/", parts.path)
    return urlunparse(parts._replace(path=path, query=urlencode(query)))


def domain_of(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).netloc.lower()
    return host.removeprefix("www.") or None


def to_int(value: str | None) -> int | None:
    m = re.search(r"\d+", value or "")
    return int(m.group()) if m else None


def to_float(value: str | None) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None


def parse_firm(li: Node, slug: str) -> dict | None:
    entity_id = li.attributes.get("entity-id")
    if not entity_id:
        return None

    web_link = li.css_first("a.web-url") or li.css_first("a.visit-website")
    website_raw = web_link.attributes.get("href") if web_link else None
    website = clean_url(website_raw)

    profile_link = li.css_first("a.profile-link") or li.css_first("a.visit-profile")
    profile_url = None
    if profile_link and profile_link.attributes.get("href"):
        profile_url = urljoin(BASE_URL, profile_link.attributes["href"].split("#")[0])

    desc_node = li.css_first("p.firm-short-description--full")
    if desc_node is not None:
        for a in desc_node.css("a"):
            a.decompose()
    description = text(desc_node)
    if description:
        description = description.removesuffix("...").rstrip()

    ai_tools = [t for t in (text(n) for n in li.css(".ai-tool-box:not(.more-tool) .aitool-box-title")) if t]
    more = li.css_first(".ai-tool-box.more-tool")
    if more and more.attributes.get("data-content"):
        ai_tools += [t.strip() for t in more.attributes["data-content"].split(",") if t.strip()]

    review_link = li.css_first(".firm-rating a.visit-profile")
    location = text(li.css_first(".firm-location span"))
    city, _, country = (location or "").rpartition(", ")

    return {
        "entity_id": entity_id,
        "name": li.attributes.get("entity-name") or text(li.css_first("h3.firm-name")),
        "website": website,
        "domain": domain_of(website),
        "website_raw": website_raw,
        "profile_url": profile_url,
        "rating": to_float(text(li.css_first(".rating-number"))),
        "reviews": to_int(text(review_link)),
        "pro_verified": li.css_first(".firm-badge--verified") is not None,
        "hourly_rate": text(li.css_first(".firm-pricing span")),
        "employees": text(li.css_first(".firm-employees span")),
        "founded": to_int(text(li.css_first(".firm-founded span"))),
        "location": location,
        "city": city or None,
        "country": country or location,
        "description": description,
        "why_choose": [t for t in (text(n) for n in li.css(".firm-why-choice li")) if t],
        "ai_tools": ai_tools,
        "category": slug,
        "scraped_at": now_iso(),
    }


def parse_page(html: str, slug: str) -> tuple[list[dict], int | None, bool]:
    """Returns (firms, last_page_number, has_next_page)."""
    tree = HTMLParser(html)
    items = tree.css("#service-providers ul.firm-directory-list > li.firm-wrapper")
    if not items:  # fallback in case the section id changes
        items = tree.css("ul.firm-directory-list > li.firm-wrapper")
    firms = [f for f in (parse_firm(li, slug) for li in items) if f]

    last_page = None
    last = tree.css_first(".pagination li.last-page a")
    if last:
        m = re.search(r"page=(\d+)", last.attributes.get("href", ""))
        last_page = int(m.group(1)) if m else None
    has_next = tree.css_first('.pagination li.next-page a[rel="next"]') is not None
    return firms, last_page, has_next


# --------------------------------------------------------------------------- fetching


class Fetcher:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.session = requests.Session(impersonate="chrome")
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if args.proxy:
            self.session.proxies = {"http": args.proxy, "https": args.proxy}
        self.requests_made = 0
        self.next_break_at = random.randint(*args.break_every)

    def polite_wait(self) -> None:
        if self.requests_made == 0:
            return
        if self.requests_made >= self.next_break_at:
            pause = random.uniform(*self.args.break_length)
            log.info("Taking a longer break: %.0fs", pause)
            time.sleep(pause)
            self.next_break_at = self.requests_made + random.randint(*self.args.break_every)
        else:
            time.sleep(random.uniform(self.args.min_delay, self.args.max_delay))

    def get(self, url: str, referer: str | None) -> str:
        backoff = self.args.backoff
        for attempt in range(1, self.args.max_retries + 1):
            self.polite_wait()
            self.requests_made += 1
            headers = {"Referer": referer} if referer else {}
            try:
                resp = self.session.get(url, headers=headers, timeout=45)
            except Exception as exc:  # network error / timeout: the site was slow, not refusing
                wait = random.uniform(*self.args.timeout_wait)
                log.warning("Network error on %s (attempt %d/%d), retrying in %.0fs: %s",
                            url, attempt, self.args.max_retries, wait, exc)
                time.sleep(wait)
                continue

            body = resp.text
            challenged = "Just a moment..." in body[:5000] or "cf-chl" in body[:5000]
            if resp.status_code == 200 and not challenged and "firm-directory-list" in body:
                log.debug("GET %s -> 200 (%d bytes)", url, len(body))
                return body

            wait = backoff
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait = max(wait, int(retry_after))
            reason = "challenge page" if challenged else f"HTTP {resp.status_code}"
            if resp.status_code == 404:
                raise FileNotFoundError(url)
            level = logging.WARNING if resp.status_code in BLOCK_STATUSES or challenged else logging.INFO
            log.log(level, "%s on %s (attempt %d/%d) - backing off %.0fs",
                    reason, url, attempt, self.args.max_retries, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, 900)

        raise Blocked(f"Gave up on {url} after {self.args.max_retries} attempts")


# --------------------------------------------------------------------------- crawl


def category_slug(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1] if path else "unknown"


def page_url(base: str, page: int) -> str:
    base = base.split("?")[0].rstrip("/")
    return base if page == 1 else f"{base}?page={page}"


def crawl_category(url: str, store: Store, fetcher: Fetcher, args: argparse.Namespace) -> None:
    slug = category_slug(url)
    cat = store.category(slug)
    if cat["completed"] and not args.refresh:
        log.info("[%s] already completed (%d firms). Use --refresh to look for new firms.", slug, cat["firms"])
        return

    page = 1 if args.refresh else cat["last_page_done"] + 1
    if args.refresh:
        cat["completed"] = False
    cat["url"] = url.split("?")[0]
    cat["status"] = "in_progress"
    log.info("[%s] starting at page %d%s", slug, page,
             f" of {cat['total_pages']}" if cat["total_pages"] else "")

    pages_this_run = 0
    referer = page_url(url, page - 1) if page > 1 else BASE_URL + "/"
    while True:
        if args.max_pages and pages_this_run >= args.max_pages:
            log.info("[%s] reached --max-pages %d, stopping here (resume later).", slug, args.max_pages)
            break

        target = page_url(url, page)
        try:
            html = fetcher.get(target, referer)
        except FileNotFoundError:
            log.info("[%s] page %d returned 404 - treating as end of listing.", slug, page)
            cat["completed"] = True
            cat["status"] = "completed"
            break

        firms, last_page, has_next = parse_page(html, slug)
        if last_page:
            cat["total_pages"] = max(cat["total_pages"] or 0, last_page)
        added = store.append(slug, firms)
        cat["firms"] += added
        cat["last_page_done"] = page
        cat["updated_at"] = now_iso()
        store.save_state()
        pages_this_run += 1

        log.info("[%s] page %d/%s: %d firms, %d new, %d skipped (already scraped) | total new: %d",
                 slug, page, cat["total_pages"] or "?", len(firms), added, len(firms) - added, cat["firms"])
        if not firms:
            log.warning("[%s] page %d had no firms - the page layout may have changed.", slug, page)

        if not has_next or not firms:
            cat["completed"] = True
            cat["status"] = "completed"
            log.info("[%s] finished - no more pages.", slug)
            break
        referer = target
        page += 1

    store.save_state()


def load_urls(args: argparse.Namespace) -> list[str]:
    if args.urls:
        return args.urls
    path = Path(args.file)
    if not path.exists():
        sys.exit(f"No URLs given and {path} not found.")
    lines = [ln.strip() for ln in path.read_text().splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape GoodFirms category listings into JSON.")
    p.add_argument("urls", nargs="*", help="Category URLs (default: read from --file)")
    p.add_argument("--file", default="categories.txt", help="File with one category URL per line")
    p.add_argument("--min-delay", type=float, default=2.0, help="Min seconds between requests")
    p.add_argument("--max-delay", type=float, default=5.0, help="Max seconds between requests")
    p.add_argument("--break-every", type=int, nargs=2, default=[40, 60], metavar=("MIN", "MAX"),
                   help="Take a long break after a random number of requests in this range")
    p.add_argument("--break-length", type=float, nargs=2, default=[30, 90], metavar=("MIN", "MAX"),
                   help="Long break duration range in seconds")
    p.add_argument("--backoff", type=float, default=60, help="Initial backoff seconds after a refusal")
    p.add_argument("--timeout-wait", type=float, nargs=2, default=[15, 30], metavar=("MIN", "MAX"),
                   help="Seconds to wait before retrying after a timeout/network error")
    p.add_argument("--max-retries", type=int, default=6, help="Attempts per page before stopping the run")
    p.add_argument("--max-pages", type=int, default=0, help="Max pages per category this run (0 = all)")
    p.add_argument("--chunk-size", type=int, default=3500, help="Firms per output file before starting the next")
    p.add_argument("--refresh", action="store_true", help="Re-crawl from page 1, still skipping known firms")
    p.add_argument("--proxy", help="Optional proxy URL, e.g. http://user:pass@host:port")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug output on console")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)
    urls = load_urls(args)
    store = Store(args.chunk_size)
    fetcher = Fetcher(args)

    # Turn SIGTERM into KeyboardInterrupt so state is exported either way.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    done_slugs: list[str] = []
    try:
        for url in urls:
            slug = category_slug(url)
            done_slugs.append(slug)
            crawl_category(url, store, fetcher, args)
    except Blocked as exc:
        log.error("%s. Progress is saved - wait a while (e.g. 30-60 min) and re-run to resume.", exc)
    except KeyboardInterrupt:
        log.warning("Interrupted - progress saved, re-run to resume.")
    finally:
        store.save_state()
        for slug in done_slugs:
            store.export_json(slug)
        log.info("Requests made this run: %d", fetcher.requests_made)


if __name__ == "__main__":
    main()
