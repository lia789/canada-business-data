#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import re
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime
from pathlib import Path

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import Request

# ========== EDIT ME ==========
INPUT_CSV  = "1_54k-business_category.csv"


timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
OUTPUT_CSV = f"output-part-1-54k---{timestamp}.csv"


APPEND_HEADER = not Path(OUTPUT_CSV).exists()
SKIP_ROWS  = 0           # <--- how many CSV rows to skip before starting
# =============================

SOURCE_WEBSITE = "yellowpages.ca"





def clean_website_url(raw: str) -> Optional[str]:
    try:
        if not isinstance(raw, str):
            return None
        s = raw.strip()
        if not s:
            return None
        s = s.split()[0].strip(",;")
        if "/gourl/" in s:
            tmp = s if s.startswith(("http://", "https://")) else "https://dummy.local" + s
            parsed = urlparse(tmp)
            q = parse_qs(parsed.query)
            redirect_val = (q.get("redirect") or (q.get("url")) or q.get("dest") or [None])[0]
            if not redirect_val:
                m = re.search(r"(?:redirect|url|dest)=([^&]+)", s)
                redirect_val = m.group(1) if m else None
            if redirect_val:
                s = unquote(redirect_val)
        s = unquote(s)
        if s.startswith("//"):
            s = "http:" + s
        if s.startswith("www."):
            s = "http://" + s
        p = urlparse(s)
        if not p.scheme:
            s = "http://" + s
            p = urlparse(s)
        if not p.netloc:
            return None
        cleaned = p._replace(params="", query="", fragment="").geturl()
        if len(cleaned) > 2048:
            return None
        return cleaned
    except Exception:
        return None


class KlStateSpider(scrapy.Spider):
    name = "kl_state"
    handle_httpstatus_all = True   # receive non-200 pages (e.g., 403) in parse

    custom_settings = {
        "USER_AGENT": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ),
        "HTTPCACHE_ENABLED": True,

        # Keep logs minimal: only WARNING+ and disable noisy extensions
        "LOG_ENABLED": True,
        "LOG_LEVEL": "WARNING",
        "EXTENSIONS": {
            "scrapy.extensions.telnet.TelnetConsole": None,
            "scrapy.extensions.logstats.LogStats": None,
        },

        # "CONCURRENT_REQUESTS": 10,
        "AUTOTHROTTLE_ENABLED": True,

        # CSV output
        "FEEDS": {
            OUTPUT_CSV: {
                "format": "csv",
                "encoding": "utf-8",
                "overwrite": True,
                "fields": [
                    "row_index","province","city_name","business_category","business_name","street","city",
                    "postal_code","phone","website","rating","review","logo_url","source_url","source_website",
                ],
                "item_export_kwargs": {"include_headers_line": APPEND_HEADER},
            }
        },
    }

    input_csv = INPUT_CSV

    def start_requests(self):
        with open(self.input_csv, "r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))

        self.total_rows = len(rows)
        self.rows_done = 0
        self.items_total = 0

        if SKIP_ROWS >= self.total_rows:
            self.logger.warning("[DONE] rows_done=%d / total_rows=%d | items_total=%d",
                                self.rows_done, self.total_rows, self.items_total)
            return

        for idx, row in enumerate(rows, start=1):
            if idx <= SKIP_ROWS:
                continue

            province = (row.get("province") or "").strip()
            city_name = (row.get("city_name") or "").strip()
            business_category = (row.get("business_category") or "").strip()
            business_category_url = (row.get("business_category_url") or "").strip()
            if not business_category_url:
                # treat an empty url row as "done"
                self.rows_done += 1
                self.logger.warning("[DONE] rows_done=%d / total_rows=%d | items_total=%d",
                                    self.rows_done, self.total_rows, self.items_total)
                continue

            yield Request(
                business_category_url,
                callback=self.parse_listing,
                cb_kwargs={
                    "row_index": idx,
                    "province": province,
                    "city_name": city_name,
                    "business_category": business_category,
                },
                meta={
                    "row_index": idx,
                    "province": province,
                    "city_name": city_name,
                    "business_category": business_category,
                },
            )

    def parse_listing(self, response, row_index: int, province: str, city_name: str, business_category: str):
        # Log HTTP status if not 200; highlight 403
        if response.status != 200:
            level = self.logger.warning
            tag = "403" if response.status == 403 else str(response.status)
            level("[HTTP] row=%d status=%s url=%s", row_index, tag, response.url)

        items = response.xpath("//div[contains(@class, 'listingInfo')]")
        page_count = 0

        for l in items:
            business_name = l.xpath("normalize-space(.//a[contains(@class, 'ListingName')]/text())").get() or ""
            source_url_row = l.xpath("normalize-space(.//a[contains(@class, 'ListingName')]/@href)").get()
            source_url = response.urljoin(source_url_row) if source_url_row else ""
            street = l.xpath("normalize-space(.//span[contains(@class, 'listing__address--full')]//span[@itemprop='streetAddress']/text())").get() or ""
            city   = l.xpath("normalize-space(.//span[contains(@class, 'listing__address--full')]//span[@itemprop='addressLocality']/text())").get() or ""
            postal = l.xpath("normalize-space(.//span[contains(@class, 'listing__address--full')]//span[@itemprop='postalCode']/text())").get() or ""
            phone  = l.xpath(".//a[contains(@title, 'Get the Phone Number')]/@data-phone").get() or ""
            website_raw = l.xpath(".//li[contains(@class, 'website') or contains(@class, 'website ')]//a/@href").get()
            website = clean_website_url(website_raw) or ""
            rating  = l.xpath("normalize-space(.//span[contains(@title, 'out of 5 stars')]/@aria-label)").get() or ""
            review  = l.xpath("normalize-space(.//a[contains(@class, 'listing__ratings__count')]/text())").get() or ""
            logo_url = l.xpath("normalize-space(.//img[contains(@class, 'MerchantLogo')]/@src)").get() or ""

            page_count += 1
            yield {
                "row_index": row_index,
                "province": province,
                "city_name": city_name,
                "business_category": business_category,
                "business_name": business_name,
                "street": street,
                "city": city,
                "postal_code": postal,
                "phone": phone,
                "website": website,
                "rating": rating,
                "review": review,
                "logo_url": logo_url,
                "source_url": source_url,
                "source_website": SOURCE_WEBSITE,
            }

        # update global item counter
        self.items_total += page_count

        # pagination
        next_url = response.xpath("//a[contains(., 'Next')]/@href").get() or response.xpath("//a[@rel='next']/@href").get()
        if next_url:
            yield response.follow(
                next_url,
                callback=self.parse_listing,
                cb_kwargs={
                    "row_index": row_index,
                    "province": province,
                    "city_name": city_name,
                    "business_category": business_category,
                },
                meta={
                    "row_index": row_index,
                    "province": province,
                    "city_name": city_name,
                    "business_category": business_category,
                },
            )
        else:
            # finished this input row (no more pages)
            self.rows_done += 1
            self.logger.warning("[DONE] rows_done=%d / total_rows=%d | items_total=%d",
                                self.rows_done, self.total_rows, self.items_total)


# ---------- run as a single file ----------
if __name__ == "__main__":
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"scrapy_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

    process = CrawlerProcess(settings={
        "LOG_FILE": str(log_file),
        "LOG_LEVEL": "WARNING",  # only our warnings appear
        "LOG_FORMAT": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "LOG_DATEFORMAT": "%Y-%m-%d %H:%M:%S",
    })
    process.crawl(KlStateSpider)
    process.start()
    print(f"\nLog saved to: {log_file}\n")
