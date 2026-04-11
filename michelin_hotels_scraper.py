#!/usr/bin/env python3
"""Scrape hotel listings from the MICHELIN Guide.

This script uses Playwright to render JavaScript-heavy pages and bypasses the
limitations of simple HTTP scrapers on guide.michelin.com.

Usage examples:
  python michelin_hotels_scraper.py --country us --language en --max-hotels 200
  python michelin_hotels_scraper.py --all-countries --search-letters abcdefghijklmnopqrstuvwxyz
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import string
from itertools import product
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import BrowserContext, Error, Page, async_playwright

BASE_URL = "https://guide.michelin.com"
HOTELS_PATH = "{country}/{language}/hotels-stays"
CARD_LINK_SELECTOR = (
    "a[href*='/hotel/'], "
    "a[href*='/hotel-stay/'], "
    "a[href*='/hotels-stays/'][aria-label^='Open '], "
    "a[href*='/hotels-stays/'][data-position-x]"
)
NEXT_BUTTON_SELECTORS = [
    "button[aria-label='Next']",
    "button:has-text('Next')",
    "a[rel='next']",
]


@dataclass
class Hotel:
    locale: str
    name: str | None
    url: str
    city: str | None
    country: str | None
    address: str | None
    latitude: float | None
    longitude: float | None
    description: str | None
    phone: str | None
    email: str | None
    stars: float | None
    amenities: list[str]
    scraped_at_utc: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape MICHELIN Guide hotels")
    parser.add_argument("--country", default="us", help="Country segment in URL (default: us)")
    parser.add_argument("--language", default="en", help="Language segment in URL (default: en)")
    parser.add_argument(
        "--all-countries",
        action="store_true",
        help="Use search seeding to discover hotels across countries",
    )
    parser.add_argument("--max-pages", type=int, default=500, help="Max listing pages to scan")
    parser.add_argument("--max-hotels", type=int, default=0, help="Stop after N hotels (0 = no limit)")
    parser.add_argument("--output", default="michelin_hotels.jsonl", help="JSONL output file")
    parser.add_argument("--csv", default="michelin_hotels.csv", help="CSV output file")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Playwright timeout")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode")
    parser.add_argument(
        "--search-letters",
        default=string.ascii_lowercase,
        help="Characters used to build search queries (default: a-z)",
    )
    parser.add_argument(
        "--search-prefix-length",
        type=int,
        default=3,
        help="Length of generated search prefixes (default: 3; e.g. aaa..zzz)",
    )
    parser.add_argument(
        "--max-search-pages",
        type=int,
        default=20,
        help="Maximum search result pages to traverse per seed letter",
    )
    parser.add_argument(
        "--search-url-template",
        default=(
            f"{BASE_URL}"
            + "/{country}/{language}/hotels-stays/page/{page}?q={query}&nA=1&nC=0&nR=1"
        ),
        help="Template for search page URL; supports {country}, {language}, {query}, {page}",
    )
    return parser.parse_args()


async def accept_cookie_banner(page: Page) -> None:
    candidates = [
        "button:has-text('Accept All')",
        "button:has-text('Accept all')",
        "button:has-text('I agree')",
        "button#onetrust-accept-btn-handler",
    ]
    for selector in candidates:
        try:
            button = page.locator(selector).first
            if await button.is_visible(timeout=1500):
                await button.click(timeout=2000)
                return
        except Error:
            continue


def is_abort_navigation_error(exc: Error) -> bool:
    message = str(exc)
    return "ERR_ABORTED" in message or "Navigation failed because page was closed" in message


async def goto_with_retries(
    page: Page,
    url: str,
    *,
    wait_until: str = "domcontentloaded",
    retries: int = 2,
) -> Any:
    for attempt in range(1, retries + 1):
        try:
            return await page.goto(url, wait_until=wait_until)
        except Error as exc:
            if not is_abort_navigation_error(exc) or attempt == retries:
                raise
            print(f"[search-retry] attempt={attempt}/{retries} url={url} reason=ERR_ABORTED")
            await page.wait_for_timeout(750 * attempt)
    return None


def is_hotel_detail_url(url: str) -> bool:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 5:
        return False
    if parts[2] != "hotels-stays":
        return False
    if parts[3] in {"page", "search"}:
        return False
    return True


async def collect_hotel_urls(page: Page, max_pages: int, max_hotels: int) -> list[str]:
    urls: set[str] = set()

    for page_index in range(1, max_pages + 1):
        await page.wait_for_timeout(1200)
        links = page.locator(CARD_LINK_SELECTOR)
        count = await links.count()

        for i in range(count):
            href = await links.nth(i).get_attribute("href")
            if not href:
                continue
            absolute = href if href.startswith("http") else f"{BASE_URL}{href}"
            if is_hotel_detail_url(absolute) or "/hotel/" in absolute or "/hotel-stay/" in absolute:
                urls.add(absolute.split("?")[0].rstrip("/"))

        print(f"[listing] page={page_index} hotels_seen={len(urls)}")
        if max_hotels and len(urls) >= max_hotels:
            break

        moved = False
        for selector in NEXT_BUTTON_SELECTORS:
            candidate = page.locator(selector).first
            try:
                if await candidate.is_visible(timeout=1000) and await candidate.is_enabled():
                    await candidate.click()
                    moved = True
                    break
            except Error:
                continue

        if moved:
            continue

        prev_count = len(urls)
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(1500)
        links_after_scroll = await page.locator(CARD_LINK_SELECTOR).count()
        if links_after_scroll <= count and len(urls) == prev_count:
            break

    ordered = sorted(urls)
    if max_hotels:
        return ordered[:max_hotels]
    return ordered


def infer_locale_from_url(url: str, fallback: str) -> str:
    match = re.search(r"guide\\.michelin\\.com/([a-z]{2}/[a-z]{2})/", url)
    return match.group(1) if match else fallback


def infer_stars_from_text(text: str | None) -> float | None:
    if not text:
        return None
    match = re.search(r"(\\d(?:\\.\\d)?)\\s*(?:star|stars)\\b", text.lower())
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


async def extract_hotels_from_current_page(
    page: Page,
    fallback_locale: str,
    seen_urls: set[str],
) -> tuple[list[Hotel], int]:
    hotels: list[Hotel] = []
    candidate_urls_on_page: set[str] = set()
    links = page.locator(CARD_LINK_SELECTOR)
    count = await links.count()
    for i in range(count):
        link = links.nth(i)
        href = await link.get_attribute("href")
        if not href:
            continue
        absolute = href if href.startswith("http") else f"{BASE_URL}{href}"
        if not is_hotel_detail_url(absolute) and "/hotel/" not in absolute and "/hotel-stay/" not in absolute:
            continue
        absolute = absolute.split("?")[0].rstrip("/")
        candidate_urls_on_page.add(absolute)
        if absolute in seen_urls:
            continue
        seen_urls.add(absolute)

        name = await link.inner_text()
        if not name:
            name = await link.get_attribute("title")
        if not name:
            name = await link.get_attribute("aria-label")
        name = normalize_text(name)

        card = link.locator("xpath=ancestor::*[self::article or self::li or @role='article'][1]")
        card_text = None
        try:
            card_text = normalize_text(await card.inner_text(timeout=800))
        except Error:
            card_text = None

        locale = infer_locale_from_url(absolute, fallback_locale)
        hotels.append(
            Hotel(
                locale=locale,
                name=name,
                url=absolute,
                city=None,
                country=None,
                address=None,
                latitude=None,
                longitude=None,
                description=card_text,
                phone=None,
                email=None,
                stars=infer_stars_from_text(card_text),
                amenities=[],
                scraped_at_utc=datetime.now(timezone.utc).isoformat(),
            )
        )
    return hotels, len(candidate_urls_on_page)


def parse_search_letters(raw: str) -> list[str]:
    letters = [ch.lower() for ch in raw if ch.strip() and ch.isalpha()]
    deduped: list[str] = []
    seen: set[str] = set()
    for letter in letters:
        if letter in seen:
            continue
        seen.add(letter)
        deduped.append(letter)
    return deduped or list(string.ascii_lowercase)


def build_search_queries(raw_letters: str, prefix_length: int) -> list[str]:
    letters = parse_search_letters(raw_letters)
    safe_length = max(1, prefix_length)
    return ["".join(chars) for chars in product(letters, repeat=safe_length)]


async def collect_hotels_from_search_cards(context: BrowserContext, args: argparse.Namespace) -> list[Hotel]:
    hotels: list[Hotel] = []
    seen_urls: set[str] = set()
    page = await context.new_page()
    page.set_default_timeout(args.timeout_ms)
    fallback_locale = f"{args.country}/{args.language}"
    try:
        for query in build_search_queries(args.search_letters, args.search_prefix_length):
            seen_result_pages: set[str] = set()
            for page_num in range(1, args.max_search_pages + 1):
                search_url = args.search_url_template.format(
                    country=args.country,
                    language=args.language,
                    query=query,
                    page=page_num,
                )
                print(f"[search] seed={query} page={page_num} url={search_url}")
                try:
                    response = await goto_with_retries(page, search_url, wait_until="domcontentloaded")
                except Error as exc:
                    print(f"[search-skip] seed={query} page={page_num} navigation_error={exc}")
                    break
                if not response or response.status >= 400:
                    status = response.status if response else "no-response"
                    print(f"[search-skip] seed={query} page={page_num} status={status}")
                    break
                final_url = page.url.split("#")[0]
                if final_url in seen_result_pages:
                    print(f"[search-stop] seed={query} page={page_num} repeated_page={final_url}")
                    break
                seen_result_pages.add(final_url)

                if page_num == 1:
                    await accept_cookie_banner(page)
                page_hotels, candidate_hotel_links = await extract_hotels_from_current_page(
                    page,
                    fallback_locale,
                    seen_urls,
                )
                if candidate_hotel_links == 0:
                    break
                hotels.extend(page_hotels)
                print(
                    "[search-page] "
                    f"seed={query} page={page_num} "
                    f"new_on_page={len(page_hotels)} "
                    f"candidate_links={candidate_hotel_links} "
                    f"hotels_seen={len(hotels)}"
                )
                if page_num > 1 and len(page_hotels) == 0:
                    print(f"[search-stop] seed={query} page={page_num} no_new_hotels")
                    break
                if args.max_hotels and len(hotels) >= args.max_hotels:
                    return hotels[: args.max_hotels]
    finally:
        await page.close()
    if args.max_hotels:
        return hotels[: args.max_hotels]
    return hotels


def parse_jsonld_objects(raw_html: str) -> list[dict[str, Any]]:
    pattern = re.compile(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        re.IGNORECASE | re.DOTALL,
    )
    objects: list[dict[str, Any]] = []
    for blob in pattern.findall(raw_html):
        text = blob.strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            objects.extend(item for item in data if isinstance(item, dict))
        elif isinstance(data, dict):
            objects.append(data)
    return objects


def pick_hotel_schema(objects: list[dict[str, Any]]) -> dict[str, Any]:
    for item in objects:
        kind = item.get("@type")
        if kind == "Hotel" or (isinstance(kind, list) and "Hotel" in kind):
            return item
    for item in objects:
        graph = item.get("@graph")
        if not isinstance(graph, list):
            continue
        for node in graph:
            if not isinstance(node, dict):
                continue
            kind = node.get("@type")
            if kind == "Hotel" or (isinstance(kind, list) and "Hotel" in kind):
                return node
    return {}


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        compact = " ".join(value.split())
        return compact or None
    return str(value)


def parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def scrape_hotel_page(page: Page, url: str, locale: str) -> Hotel:
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(1000)
    html = await page.content()

    schema = pick_hotel_schema(parse_jsonld_objects(html))
    address_data = schema.get("address") if isinstance(schema.get("address"), dict) else {}
    geo_data = schema.get("geo") if isinstance(schema.get("geo"), dict) else {}
    amenities_raw = schema.get("amenityFeature")

    amenities: list[str] = []
    if isinstance(amenities_raw, list):
        for amenity in amenities_raw:
            if isinstance(amenity, dict):
                name = normalize_text(amenity.get("name"))
                if name:
                    amenities.append(name)

    rating = schema.get("starRating")
    stars = None
    if isinstance(rating, dict):
        value = rating.get("ratingValue")
        try:
            stars = float(value)
        except (TypeError, ValueError):
            stars = None

    return Hotel(
        locale=locale,
        name=normalize_text(schema.get("name")),
        url=url,
        city=normalize_text(address_data.get("addressLocality")),
        country=normalize_text(address_data.get("addressCountry")),
        address=normalize_text(address_data.get("streetAddress")),
        latitude=parse_float(geo_data.get("latitude")),
        longitude=parse_float(geo_data.get("longitude")),
        description=normalize_text(schema.get("description")),
        phone=normalize_text(schema.get("telephone")),
        email=normalize_text(schema.get("email")),
        stars=stars,
        amenities=sorted(set(amenities)),
        scraped_at_utc=datetime.now(timezone.utc).isoformat(),
    )


async def scrape_locale(context: BrowserContext, args: argparse.Namespace, locale: str) -> list[Hotel]:
    country, language = locale.split("/", maxsplit=1)
    listing_page = await context.new_page()
    listing_page.set_default_timeout(args.timeout_ms)

    listing_url = f"{BASE_URL}/{HOTELS_PATH.format(country=country, language=language)}"
    print(f"[start] locale={locale} {listing_url}")
    response = await listing_page.goto(listing_url, wait_until="domcontentloaded")
    if not response or response.status >= 400:
        status = response.status if response else "no-response"
        print(f"[skip] locale={locale} listing_status={status}")
        await listing_page.close()
        return []
    await accept_cookie_banner(listing_page)

    urls = await collect_hotel_urls(listing_page, args.max_pages, args.max_hotels)
    await listing_page.close()

    print(f"[collected] locale={locale} hotels={len(urls)}")
    hotels: list[Hotel] = []
    detail_page = await context.new_page()
    detail_page.set_default_timeout(args.timeout_ms)

    for idx, url in enumerate(urls, start=1):
        try:
            hotel = await scrape_hotel_page(detail_page, url, locale=locale)
            hotels.append(hotel)
            print(f"[hotel] {locale} {idx}/{len(urls)} {hotel.name or '(unknown)'}")
        except Error as exc:
            print(f"[warn] failed {url}: {exc}")

    await detail_page.close()
    return hotels


async def scrape_all(context: BrowserContext, args: argparse.Namespace) -> list[Hotel]:
    hotels: list[Hotel] = []
    seen_urls: set[str] = set()
    if args.all_countries:
        hotels = await collect_hotels_from_search_cards(context, args)
        print(f"[search-collected] unique_hotels={len(hotels)}")
        return hotels

    locale = f"{args.country}/{args.language}"
    locale_hotels = await scrape_locale(context, args, locale)
    for hotel in locale_hotels:
        if hotel.url in seen_urls:
            continue
        seen_urls.add(hotel.url)
        hotels.append(hotel)
    return hotels


def write_outputs(hotels: list[Hotel], jsonl_path: Path, csv_path: Path) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with jsonl_path.open("w", encoding="utf-8") as f:
        for hotel in hotels:
            f.write(json.dumps(asdict(hotel), ensure_ascii=False) + "\n")

    fields = [
        "locale",
        "name",
        "url",
        "city",
        "country",
        "address",
        "latitude",
        "longitude",
        "description",
        "phone",
        "email",
        "stars",
        "amenities",
        "scraped_at_utc",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for hotel in hotels:
            row = asdict(hotel)
            row["amenities"] = " | ".join(hotel.amenities)
            writer.writerow(row)


async def amain(args: argparse.Namespace) -> int:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.headful)
        context = await browser.new_context()
        try:
            hotels = await scrape_all(context, args)
            write_outputs(hotels, Path(args.output), Path(args.csv))
            print(f"[done] hotels={len(hotels)} jsonl={args.output} csv={args.csv}")
            return 0
        finally:
            await context.close()
            await browser.close()


def main() -> int:
    args = parse_args()
    return asyncio.run(amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
