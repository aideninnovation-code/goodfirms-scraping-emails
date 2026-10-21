
import json
import logging
import random
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==========================================
# CONFIGURATION
# ==========================================

BASE_URL = "https://ca.kompass.com"
SEARCH_PATH = "/searchCompanies/scroll"

OUTPUT_FILE = Path("kompass_companies.json")
LOG_FILE = Path("scraper.log")

START_PAGE = 1
MAX_PAGES = None  # None = continue until the final results page

REQUEST_TIMEOUT = 30
MIN_DELAY = 4
MAX_DELAY = 8


# ==========================================
# LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("kompass")


# ==========================================
# HTTP SESSION
# ==========================================

session = requests.Session()

# Retry temporary server errors only.
# Do not automatically retry 403 or 429 responses.
retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"],
    respect_retry_after_header=True,
)

adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)

session.headers.update({
    "User-Agent": "KompassResearchClient/1.0",
    "Accept": "text/html,application/xhtml+xml",
})


# ==========================================
# JSON STORAGE
# ==========================================

def load_data():
    if not OUTPUT_FILE.exists():
        return {}

    with OUTPUT_FILE.open("r", encoding="utf-8") as f:
        records = json.load(f)

    if not isinstance(records, list):
        raise ValueError("Existing JSON must contain a list.")

    result = {}

    for record in records:
        company_id = record.get("company_id")
        if company_id:
            result[company_id] = record

    logger.info("Loaded %d existing records.", len(result))
    return result


def save_data(companies):
    temp_file = OUTPUT_FILE.with_suffix(".tmp")

    with temp_file.open("w", encoding="utf-8") as f:
        json.dump(
            list(companies.values()),
            f,
            ensure_ascii=False,
            indent=2,
        )
        f.flush()

    temp_file.replace(OUTPUT_FILE)


# ==========================================
# FETCH PAGE
# ==========================================

def fetch_page(page_number):
    url = BASE_URL
    params = {"pageNbre": page_number}

    logger.info("Requesting page %s", page_number)

    try:
        response = session.get(
            url,
            params=params,
            timeout=30,
        )

        # Only access response after the request returns.
        logger.info("HTTP status: %s", response.status_code)
        logger.info("Final URL: %s", response.url)
        logger.info(
            "Content-Type: %s",
            response.headers.get("Content-Type"),
        )

        if response.status_code == 403:
            preview = response.text[:1500].replace("\n", " ")
            logger.error("403 response preview: %s", preview)
            logger.error(
                "Kompass denied access. Check authorization "
                "or use an approved access method."
            )
            return None

        if response.status_code in (401, 429):
            logger.error(
                "HTTP %s returned. Stopping the scraper.",
                response.status_code,
            )
            return None

        response.raise_for_status()
        return response.text

    except requests.exceptions.Timeout:
        logger.error("Request timed out on page %s.", page_number)
        return None

    except requests.exceptions.RequestException as exc:
        logger.exception(
            "Request failed on page %s: %s",
            page_number,
            exc,
        )
        return None


# ==========================================
# PARSING
# ==========================================

def text_or_none(element):
    if element is None:
        return None

    value = " ".join(element.stripped_strings)
    return value or None


def parse_company(card):
    link = card.select_one(".blockTitle a.titleSpan")

    if not link:
        return None

    company_id = (
        link.get("name")
        or card.get("id", "").replace("product-list-", "")
    )

    if not company_id:
        return None

    location = text_or_none(card.select_one(".placeText"))
    city = None
    country = None

    if location:
        if " - " in location:
            city, country = [
                part.strip()
                for part in location.rsplit(" - ", 1)
            ]
        else:
            city = location

    return {
        "company_id": company_id.strip(),
        "company_name": (
            link.get("title") or text_or_none(link)
        ),
        "profile_url": (
            urljoin(BASE_URL, link["href"])
            if link.get("href")
            else None
        ),
        "location": location,
        "city": city,
        "country": country,
        "company_types": [
            text_or_none(tag)
            for tag in card.select(".blockTags .tagOrange")
            if text_or_none(tag)
        ],
        "activities": [
            text_or_none(tag)
            for tag in card.select(".listActivity .tagGrey")
            if text_or_none(tag)
        ],
        "description": text_or_none(
            card.select_one("p.description")
        ),
    }


def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")

    container = soup.select_one("#resultatDivId")

    # Do not treat an unexpected response as an empty final page.
    if container is None:
        raise ValueError(
            "Expected results container was not found. "
            "The page structure may have changed or the "
            "response may not be a company listing."
        )

    cards = container.select(".prod_list.cardCompany")

    return [
        company
        for card in cards
        if (company := parse_company(card)) is not None
    ]


# ==========================================
# MAIN
# ==========================================

def main():
    companies = load_data()

    pages_processed = 0
    new_records = 0

    try:
        page_number = START_PAGE

        while MAX_PAGES is None or pages_processed < MAX_PAGES:
            html = fetch_page(page_number)

            if html is None:
                logger.error(
                    "Stopping at page %d. "
                    "Existing JSON data will be preserved.",
                    page_number,
                )
                break

            try:
                page_companies = parse_page(html)
            except ValueError as exc:
                logger.error(
                    "Could not validate page %d: %s",
                    page_number,
                    exc,
                )
                break

            if not page_companies:
                logger.info(
                    "No companies found on page %d. "
                    "End of results, assuming a valid results page.",
                    page_number,
                )
                break

            page_new = 0

            for company in page_companies:
                company_id = company["company_id"]

                if company_id not in companies:
                    companies[company_id] = company
                    page_new += 1
                    new_records += 1
                else:
                    # Update existing record without duplicating it.
                    companies[company_id].update({
                        key: value
                        for key, value in company.items()
                        if value is not None and value != []
                    })

            save_data(companies)
            pages_processed += 1

            logger.info(
                "Page %d: scraped=%d, new=%d, total_unique=%d",
                page_number,
                len(page_companies),
                page_new,
                len(companies),
            )

            page_number += 1
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")

    except Exception:
        logger.exception("Unexpected scraper error.")

    finally:
        save_data(companies)
        session.close()

        logger.info(
            "Finished. Pages processed=%d, new records=%d, "
            "total unique records=%d",
            pages_processed,
            new_records,
            len(companies),
        )
        logger.info("JSON: %s", OUTPUT_FILE.resolve())
        logger.info("Log: %s", LOG_FILE.resolve())


if __name__ == "__main__":
    main()
