#!/usr/bin/env python3
"""Scrape hotel listings from the MICHELIN Guide.

This script uses Playwright to render JavaScript-heavy pages and bypasses the
limitations of simple HTTP scrapers on guide.michelin.com.

Usage examples:
  python michelin_hotels_scraper.py --country us --language en --max-hotels 200
  python michelin_hotels_scraper.py --all-countries --languages en,es,fr --max-hotels 0
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import BrowserContext, Error, Page, async_playwright

BASE_URL = "https://guide.michelin.com"
HOTELS_PATH = "{country}/{language}/hotels-stays"
CARD_LINK_SELECTOR = "a[href*='/hotel/'], a[href*='/hotel-stay/']"
NEXT_BUTTON_SELECTORS = [
    "button[aria-label='Next']",
    "button:has-text('Next')",
    "a[rel='next']",
]
ISO_ALPHA2_COUNTRY_CODES = [
    "ad", "ae", "af", "ag", "ai", "al", "am", "ao", "aq", "ar", "as", "at", "au", "aw", "ax", "az",
    "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bl", "bm", "bn", "bo", "bq", "br", "bs",
    "bt", "bv", "bw", "by", "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn",
    "co", "cr", "cu", "cv", "cw", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee",
    "eg", "eh", "er", "es", "et", "fi", "fj", "fk", "fm", "fo", "fr", "ga", "gb", "gd", "ge", "gf",
    "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm",
    "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir", "is", "it", "je", "jm",
    "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc",
    "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mf", "mg", "mh", "mk",
    "ml", "mm", "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na",
    "nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg",
    "ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt", "pw", "py", "qa", "re", "ro", "rs", "ru", "rw",
    "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "ss",
    "st", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to",
    "tr", "tt", "tv", "tw", "tz", "ua", "ug", "um", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi",
    "vn", "vu", "wf", "ws", "ye", "yt", "za", "zm", "zw",
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
        help="Sweep 2-letter country codes and probe supported locales before scraping",
    )
    parser.add_argument(
        "--languages",
        default="en,es,fr,it,de,pt,ja,ko,zh,th",
        help="Comma-separated language codes to probe per country when --all-countries is set",
    )
    parser.add_argument("--max-pages", type=int, default=500, help="Max listing pages to scan")
    parser.add_argument("--max-hotels", type=int, default=0, help="Stop after N hotels (0 = no limit)")
    parser.add_argument("--output", default="michelin_hotels.jsonl", help="JSONL output file")
    parser.add_argument("--csv", default="michelin_hotels.csv", help="CSV output file")
    parser.add_argument("--timeout-ms", type=int, default=45000, help="Playwright timeout")
    parser.add_argument("--headful", action="store_true", help="Run browser in headed mode")
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
            if "/hotel/" in absolute or "/hotel-stay/" in absolute:
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


def parse_languages(raw: str) -> list[str]:
    values = [value.strip().lower() for value in raw.split(",")]
    return [value for value in values if value and re.fullmatch(r"[a-z]{2}", value)]


async def find_supported_locales(context: BrowserContext, languages: list[str], timeout_ms: int) -> list[str]:
    locales: list[str] = []
    for country in ISO_ALPHA2_COUNTRY_CODES:
        for language in languages:
            url = f"{BASE_URL}/{country}/{language}/hotels-stays"
            try:
                response = await context.request.get(url, timeout=timeout_ms)
            except Error:
                continue
            if response.status != 200:
                continue
            final_url = response.url.lower()
            if "404" in final_url or "not-found" in final_url:
                continue
            locales.append(f"{country}/{language}")
            break
    return locales


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
    locales = [f"{args.country}/{args.language}"]
    if args.all_countries:
        languages = parse_languages(args.languages)
        if not languages:
            languages = [args.language.lower()]
        locales = await find_supported_locales(context, languages, args.timeout_ms)
        print(f"[locales] supported={len(locales)} from_countries={len(ISO_ALPHA2_COUNTRY_CODES)}")

    hotels: list[Hotel] = []
    seen_urls: set[str] = set()
    for locale in locales:
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
