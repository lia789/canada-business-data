#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrapy + scrapy-playwright conversion of your async Playwright script
- Rotating User-Agent middleware (works for both Scrapy + Playwright context)
- CSV output with timestamp (append-safe)
- Progress resume via .progress file (contiguous index tracking)
- Concurrency control via Scrapy settings
- Google Maps place-page selectors preserved
"""

import os
import csv
import re
from pathlib import Path
from datetime import datetime

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import Request
from parsel import Selector


# ---------- USER AGENTS (rotate) ----------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0",
    "Mozilla/5.0 (iPad; CPU OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/135.0.7049.83 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/136.0.7103.91 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 15; SM-S931B Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/132.0.6834.163 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-S928B/DS) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.230 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; SM-F9560 Build/UP1A.231005.007; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/127.0.6533.103 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 9 Build/AD1A.240411.003.A5; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/124.0.6367.54 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Mobile Safari/537.36 Dalvik/2.1.0 (Linux; U; Android 13; SM-S911B Build/TP1A.220624.014)",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SAMSUNG SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/21.0 Chrome/110.0.5481.154 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone17,5; CPU iPhone OS 18_3_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 FireKeepers/1.7.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3.1 Safari/605.1.15",
]

# ---------- FILES / CONFIG ----------
INPUT_FILE = "Second-Google-Listing-URL-V2.csv"  # must contain column: listing_url
now = datetime.now()
formatted_date = now.strftime("%Y-%m-%d_%H-%M-%S")
OUTPUT_FILE = f"google_map_data_1_{formatted_date}.csv"
PROGRESS_FILE = f"{OUTPUT_FILE}.progress"  # stores last *contiguously* processed index (0-based)

# If you set SKIP_RAW, resume logic uses max(SKIP_RAW, last_done+1)
SKIP_RAW = 0

# Timeouts / concurrency
TIMEOUT_MS = 10_000
CONCURRENCY = 4  # overall concurrency


# ---------- Helpers for progress ----------
def read_progress(progress_path: str) -> int:
    if os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                return max(0, int((f.read() or "0").strip()))
        except Exception:
            return 0
    return 0


def write_progress(progress_path: str, idx: int) -> None:
    with open(progress_path, "w", encoding="utf-8") as f:
        f.write(str(idx))


def init_output(output_path: str, fieldnames):
    # your extra columns (will be auto-added)
    new_cols = ["company_name", "address", "website", "phone", "google_map_category"]
    all_fields = list(fieldnames) + [c for c in new_cols if c not in fieldnames]
    need_header = not Path(output_path).exists()
    if need_header:
        with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_fields)
            w.writeheader()
    return all_fields


# ---------- UA Rotation Middleware ----------
class RotateUserAgentMiddleware:
    """
    - Rotates UA per request (or sticky per cookiejar).
    - Also sets Playwright context UA if meta['playwright'] is True.
    """

    def __init__(self, user_agents, sticky=False):
        self.user_agents = user_agents or []
        self.sticky = sticky
        self._ua_by_cookiejar = {}

    @classmethod
    def from_crawler(cls, crawler):
        uas = crawler.settings.getlist("USER_AGENTS", [])
        sticky = crawler.settings.getbool("USER_AGENT_STICKY", True)
        return cls(uas, sticky)

    def _pick_ua(self, request):
        if not self.user_agents:
            return None
        if self.sticky:
            jar = request.meta.get("cookiejar", 0)
            if jar not in self._ua_by_cookiejar:
                import random
                self._ua_by_cookiejar[jar] = random.choice(self.user_agents)
            return self._ua_by_cookiejar[jar]
        else:
            import random
            return random.choice(self.user_agents)

    def process_request(self, request, spider):
        ua = self._pick_ua(request)
        if not ua:
            return
        request.headers["User-Agent"] = ua
        request.headers.setdefault("Accept-Language", "en-US,en;q=0.9")

        if request.meta.get("playwright"):
            ctx_kwargs = request.meta.setdefault("playwright_context_kwargs", {})
            ctx_kwargs["user_agent"] = ua


# ---------- The Spider ----------
class GoogleMapsSpider(scrapy.Spider):
    name = "gmaps_playwright"

    custom_settings = {
        # Enable scrapy-playwright
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",

        # Concurrency & politeness
        "CONCURRENT_REQUESTS": CONCURRENCY,
        "PLAYWRIGHT_MAX_PAGES": max(4, CONCURRENCY),  # small cushion
        "DOWNLOAD_TIMEOUT": (TIMEOUT_MS / 1000.0),

        # Playwright launch options (match your original)
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": False,
            "channel": "chrome",
            "args": [
                "--disable-blink-features",
                "--disable-blink-features=AutomationControlled",
            ],
        },
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": TIMEOUT_MS,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_WAIT": "load",  # or "networkidle"

        # UA rotation middleware
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            __name__ + ".RotateUserAgentMiddleware": 400,
        },
        "USER_AGENTS": USER_AGENTS,
        "USER_AGENT_STICKY": True,  # keep same UA per cookiejar/session

        # Retries & throttle (optional but helpful)
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 4,
        "RETRY_HTTP_CODES": [403, 429, 500, 502, 503, 504],
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,

        # Logging
        "LOG_LEVEL": "INFO",

        # CSV FEEDS (no fields specified → include all keys)
        "FEEDS": {
            OUTPUT_FILE: {
                "format": "csv",
                "encoding": "utf-8-sig",
                "overwrite": False,  # append-safe when file exists
                # We'll manage header ourselves with init_output()
            }
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load input rows + prepare header
        if not Path(INPUT_FILE).exists():
            raise RuntimeError(f"Input file not found: {INPUT_FILE}")

        with open(INPUT_FILE, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            self.rows = list(r)
            self.orig_fields = r.fieldnames or []

        if not self.rows:
            raise RuntimeError("No input rows found.")

        self.all_fieldnames = init_output(OUTPUT_FILE, self.orig_fields or [])
        self.total = len(self.rows)

        # Resume logic
        last_done = read_progress(PROGRESS_FILE)  # contiguous done up to this 0-based index
        self.start_at = max(SKIP_RAW, last_done + 1)
        self.completed = set()          # indices completed (success or failure)
        self.progress_last = self.start_at - 1  # last contiguous done we’ve written

        self.logger.info(f"Loaded {self.total} rows | start_at={self.start_at}")

    def start_requests(self):
        if self.start_at >= self.total:
            self.logger.info("Nothing to do. Already completed.")
            return

        # Schedule all remaining with limited concurrency handled by Scrapy
        # We'll use cookiejar=index for sticky UA per row (session realism)
        for i in range(self.start_at, self.total):
            url = (self.rows[i].get("listing_url") or "").strip()
            if not url:
                # mark as done (nothing to visit)
                self._mark_done(i)
                continue

            # Wait for <h1>, then lightly sleep for content settling (like your 1.2s wait)
            yield Request(
                url,
                callback=self.parse_place,
                errback=self.on_error,
                meta={
                    "playwright": True,
                    "playwright_page_methods": [
                        ("wait_for_selector", {"selector": "h1"}),
                        ("wait_for_timeout", {"timeout": 1200}),
                    ],
                    "cookiejar": i,  # sticky UA per row/session
                    "row_index": i,
                },
            )

    def parse_place(self, response):
        i = response.meta["row_index"]
        row = self.rows[i]

        sel = Selector(text=response.text)
        # --- Same selectors as your script ---
        company_name = sel.xpath("normalize-space(//h1/text())").get()
        address = sel.xpath("//button[starts-with(@aria-label,'Address:')]/@aria-label").get()
        website = sel.xpath("//a[@data-item-id='authority' and starts-with(@href,'http')]/@href").get()
        phone = sel.xpath("//a[starts-with(@href,'tel:')]/@href").get()

        google_map_category = sel.xpath("//button[@class='DkEaL ']/text()").get()

        if phone and phone.lower().startswith("tel:"):
            phone = phone[4:]

        out_row = dict(row)
        out_row.update({
            "company_name": company_name,
            "address": address,
            "website": website,
            "phone": phone,
            "google_map_category": google_map_category,
        })

        # Write one row immediately (append)
        with open(OUTPUT_FILE, "a", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.all_fieldnames)
            w.writerow(out_row)

        self.logger.info(f"[{i}] Saved ✓ {company_name or ''} | {response.url}")

        # progress bookkeeping
        self._mark_done(i)

    def on_error(self, failure):
        # Log and still mark as done so progress can advance
        request = failure.request
        i = request.meta.get("row_index", -1)
        self.logger.warning(f"[{i}] Error: {failure.value} | {request.url if request else ''}")
        self._mark_done(i)

    # ---------- progress helpers ----------
    def _mark_done(self, i: int):
        if i < 0:
            return
        self.completed.add(i)
        # advance contiguous pointer and persist
        while (self.progress_last + 1) in self.completed:
            self.progress_last += 1
        write_progress(PROGRESS_FILE, self.progress_last)

    def closed(self, reason):
        self.logger.info(
            f"Closed: {reason} | progress_last={self.progress_last} | progress_file={PROGRESS_FILE}"
        )


# ---------- Run as a single file ----------
if __name__ == "__main__":
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"scrapy_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

    process = CrawlerProcess(settings={
        "LOG_FILE": str(log_file),
        "LOG_LEVEL": "DEBUG",  # Spider uses INFO; you can set WARNING for long runs
        "LOG_FORMAT": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "LOG_DATEFORMAT": "%Y-%m-%d %H:%M:%S",
    })
    process.crawl(GoogleMapsSpider)
    process.start()
    print(f"\nLog saved to: {log_file}\n")
