#!/usr/bin/env python3
"""Convert the Yelp academic business dump (JSON Lines) into clean JSON files.

The raw file has one JSON object per line, and several fields are stored as
Python-literal strings: attributes look like "u'free'", "True" or
"{'garage': False, 'lot': True}", categories are one comma-separated string,
and hours look like "8:0-18:30". This script streams the file line by line
(it never loads the whole dump), turns those into real JSON types, and writes
the result in fixed-size parts:

    data/yelp/businesses/yelp-businesses-001.json   (a JSON array, pretty-printed)
    data/yelp/businesses/yelp-businesses-002.json
    ...

Usage:
    uv run yelp_convert.py                       # all businesses
    uv run yelp_convert.py --open-only           # skip closed businesses
    uv run yelp_convert.py --state PA --state NJ --category Restaurants
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT / "data" / "yelp" / "yelp_academic_dataset_business.json"
DEFAULT_OUTPUT = ROOT / "data" / "yelp" / "businesses"
DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

log = logging.getLogger("yelp")


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_literal(value: Any) -> Any:
    """Turn Yelp's Python-literal strings into JSON values.

    "True" -> True, "None" -> None, "u'free'" -> "free", "'casual'" -> "casual",
    "{'garage': False, 'lot': True}" -> {"garage": False, "lot": True}.
    Anything that isn't a valid literal is returned unchanged.
    """
    if not isinstance(value, str):
        return value
    try:
        parsed = ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return value
    if isinstance(parsed, dict):
        return {k: parse_literal(v) for k, v in parsed.items()}
    return parsed


def parse_attributes(attributes: dict | None) -> dict:
    return {key: parse_literal(value) for key, value in (attributes or {}).items()}


def parse_categories(categories: str | None) -> list[str]:
    return [c.strip() for c in (categories or "").split(",") if c.strip()]


def hhmm(part: str) -> str:
    hour, _, minute = part.partition(":")
    return f"{int(hour):02d}:{int(minute or 0):02d}"


def parse_hours(hours: dict | None) -> dict[str, dict[str, str]]:
    """{"Monday": "8:0-18:30"} -> {"Monday": {"open": "08:00", "close": "18:30"}}, in weekday order.

    Yelp's "0:0-0:0" is kept as open 00:00 / close 00:00; the dump doesn't say
    whether it means closed or open all day.
    """
    out: dict[str, dict[str, str]] = {}
    for day in DAYS:
        span = (hours or {}).get(day)
        if not span:
            continue
        try:
            start, end = span.split("-")
            out[day] = {"open": hhmm(start), "close": hhmm(end)}
        except ValueError:
            log.debug("unparsed hours %r for %s", span, day)
    return out


def clean_business(raw: dict) -> dict:
    return {
        "business_id": raw.get("business_id"),
        "name": (raw.get("name") or "").strip(),
        "categories": parse_categories(raw.get("categories")),
        "stars": raw.get("stars"),
        "review_count": raw.get("review_count"),
        "is_open": bool(raw.get("is_open")),
        "address": (raw.get("address") or "").strip() or None,
        "city": (raw.get("city") or "").strip() or None,
        "state": (raw.get("state") or "").strip() or None,
        "postal_code": (raw.get("postal_code") or "").strip() or None,
        "latitude": raw.get("latitude"),
        "longitude": raw.get("longitude"),
        "hours": parse_hours(raw.get("hours")),
        "attributes": parse_attributes(raw.get("attributes")),
        "source": "yelp_academic_dataset",
    }


def read_jsonl(path: Path) -> Iterator[tuple[int, dict]]:
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield line_no, json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("line %d: invalid JSON (%s), skipped", line_no, e)


def keep(business: dict, args: argparse.Namespace) -> bool:
    if args.open_only and not business["is_open"]:
        return False
    if args.state and business["state"] not in args.state:
        return False
    if args.category:
        wanted = {c.lower() for c in args.category}
        if not wanted & {c.lower() for c in business["categories"]}:
            return False
    return True


def show(path: Path) -> str:
    """Path relative to the project when it is inside it, for shorter log lines."""
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def write_part(out_dir: Path, index: int, records: list[dict]) -> Path:
    path = out_dir / f"yelp-businesses-{index:03d}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    log.info("wrote %s (%d businesses)", show(path), len(records))
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="raw Yelp business JSONL file")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="folder for the JSON parts")
    p.add_argument("--chunk-size", type=int, default=3500, help="businesses per output file (default 3500)")
    p.add_argument("--open-only", action="store_true", help="skip businesses marked closed")
    p.add_argument("--state", action="append", help="keep only this state code (repeatable), e.g. PA")
    p.add_argument("--category", action="append", help="keep businesses with this category (repeatable)")
    p.add_argument("--limit", type=int, default=0, help="stop after this many kept businesses (0 = all)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging(args.verbose)
    if not args.input.exists():
        log.error("input not found: %s", args.input)
        return 1

    args.output.mkdir(parents=True, exist_ok=True)
    stale = sorted(args.output.glob("yelp-businesses-*.json"))
    for old in stale:
        old.unlink()
    if stale:
        log.info("removed %d old part file(s) from %s", len(stale), show(args.output))

    started = time.monotonic()
    seen: set[str] = set()
    batch: list[dict] = []
    part = read = kept = duplicates = 0
    log.info("reading %s", args.input)
    for _, raw in read_jsonl(args.input):
        read += 1
        business = clean_business(raw)
        if business["business_id"] in seen:
            duplicates += 1
            continue
        if not keep(business, args):
            continue
        seen.add(business["business_id"])
        batch.append(business)
        kept += 1
        if len(batch) >= args.chunk_size:
            part += 1
            write_part(args.output, part, batch)
            batch = []
        if read % 25000 == 0:
            log.info("progress: %d read, %d kept", read, kept)
        if args.limit and kept >= args.limit:
            break
    if batch:
        part += 1
        write_part(args.output, part, batch)

    log.info(
        "done in %.1fs: %d read, %d kept, %d duplicates skipped, %d file(s) in %s",
        time.monotonic() - started, read, kept, duplicates, part, show(args.output),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
