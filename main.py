#!/usr/bin/env python3
"""
Groww Open==High / Open==Low Alert Scanner
Single entry point — handles login + scanning automatically.
Designed for 24/7 unattended operation.

Usage:
    python main.py              # continuous mode
    python main.py --once       # single sweep and exit
    python main.py --debug      # verbose XHR + traceback
"""

import asyncio
import json
import logging
import random
import re
import sys
import traceback
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page, Browser

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
STORAGE_FILE  = Path("groww_state.json")
ALERT_FILE    = Path("alerts.json")
LOG_FILE      = Path("scanner.log")
SCAN_INTERVAL = 90    # seconds between sweeps
STRIKE_RANGE  = 12    # ATM ± this many strikes
PAGE_SETTLE   = 6.0   # seconds after domcontentloaded
EXPIRY_SETTLE = 3.5   # seconds after expiry click

INSTRUMENTS = [
    dict(name="NIFTY",    step=50,  n_expiries=2,
         url="https://groww.in/options/nifty",
         price_xhr_pattern=None, is_mcx=False),
    dict(name="SENSEX",   step=100, n_expiries=2,
         url="https://groww.in/options/sp-bse-sensex",
         price_xhr_pattern=None, is_mcx=False),
    dict(name="RELIANCE", step=10,  n_expiries=1,
         url="https://groww.in/options/reliance-industries-ltd",
         price_xhr_pattern=None, is_mcx=False),
    dict(name="SBIN",     step=5,   n_expiries=1,
         url="https://groww.in/options/state-bank-of-india",
         price_xhr_pattern=None, is_mcx=False),
    dict(name="GOLDM",    step=100, n_expiries=1,
         url="https://groww.in/commodities/options/mcx_goldm?exchange=MCX",
         price_xhr_pattern="COMMODITY/487819/latest", is_mcx=True),
    dict(name="SILVERM",  step=500, n_expiries=1,
         url="https://groww.in/commodities/options/mcx_silverm?exchange=MCX",
         price_xhr_pattern="COMMODITY/457533/latest", is_mcx=True),
]

CAPTURE_PATTERNS = (
    "tr_live_prices", "tr_live_indices", "option_chain",
    "latest_prices", "latest_indices", "latest_prices_ohlc",
    "latest_indices_ohlc", "accord_points", "nearest_expiries",
    "commodity_fo", "commodity_router", "market_timing",
)

_INDEX_ALIASES = {
    "NIFTY":  ["NIFTY"],
    "SENSEX": ["SENSEX", "BSE SENSEX", "S&P BSE"],
}

_CHROME_VERSIONS = ["124.0.0.0", "123.0.0.0", "125.0.0.0", "126.0.0.0", "122.0.0.0"]
_VIEWPORTS       = [(1920, 1080), (1792, 1120), (1856, 1056), (1680, 1050), (1536, 864)]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logging() -> logging.Logger:
    fmt    = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                               datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger("scanner")
    logger.setLevel(logging.DEBUG)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3,
                             encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

log = _setup_logging()

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class Alert:
    symbol:     str
    instrument: str
    expiry:     str
    strike:     int
    opt_type:   str
    condition:  str
    open:       float
    high:       float
    low:        float
    ltp:        Optional[float] = None
    seen_at:    str  = field(default_factory=lambda: datetime.now().isoformat())
    broken:     bool = False
    broken_at:  Optional[str] = None

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------
def _sf(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None

def _has_ohlc(d: dict) -> bool:
    return bool(d.get("open") or d.get("ltp"))

def _key_matches_index(key: str, name_upper: str) -> bool:
    needles = _INDEX_ALIASES.get(name_upper, [name_upper])
    ku = key.upper()
    return any(n in ku for n in needles)

# ---------------------------------------------------------------------------
# Date / symbol parsing
# ---------------------------------------------------------------------------
def _code_to_date(code: str) -> Optional[str]:
    if code.isdigit():
        if len(code) == 6:
            try:
                return datetime.strptime(f"20{code}", "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        if len(code) == 5:
            yy = code[:2]
            for split in (1, 2):
                try:
                    mm = int(code[2: 2 + split])
                    dd = int(code[2 + split:])
                    return datetime.strptime(f"20{yy}{mm:02d}{dd:02d}",
                                             "%Y%m%d").strftime("%Y-%m-%d")
                except (ValueError, IndexError):
                    pass
        if len(code) == 4:
            try:
                return datetime.strptime(f"20{code}01", "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
    m = re.match(r"^(\d{2})([A-Z]{3})$", code)
    if m:
        yy, mon = m.groups()
        try:
            return datetime.strptime(f"20{yy} {mon} 01",
                                     "%Y %b %d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def _parse_sym_flexible(sym: str):
    sym = sym.upper()
    for opt in ("CE", "PE"):
        if not sym.endswith(opt):
            continue
        body = sym[:-2]
        for name_len in range(2, min(13, len(body) - 4)):
            name = body[:name_len]
            if not name.isalpha():
                continue
            remainder = body[name_len:]
            for code_len in range(4, min(7, len(remainder))):
                code     = remainder[:code_len]
                strike_s = remainder[code_len:]
                if not strike_s.isdigit():
                    continue
                strike = int(strike_s)
                if strike < 50:
                    continue
                expiry = _code_to_date(code)
                if expiry:
                    return (name, expiry, strike, opt)
    return None

def build_symbol_candidates(name: str, expiry: str, strike: int, opt: str) -> list[str]:
    dt         = datetime.strptime(expiry, "%Y-%m-%d")
    year_short = dt.strftime("%y")
    month_abbr = dt.strftime("%b").upper()
    month_full = dt.strftime("%m")
    day_2      = dt.strftime("%d")
    code_6     = f"{year_short}{month_full}{day_2}"
    code_5     = f"{year_short}{dt.month}{day_2}"
    code_4     = f"{year_short}{month_full}"
    s          = str(strike)
    return [
        f"{name}{code_6}{s}{opt}",
        f"{name}{code_5}{s}{opt}",
        f"{name}{code_4}{s}{opt}",
        f"{name}{year_short}{month_abbr}{s}{opt}",
    ]

# ---------------------------------------------------------------------------
# Price extraction
# ---------------------------------------------------------------------------
def _search_index_price(data, name_upper: str) -> Optional[float]:
    PRICE_FIELDS = ("value", "ltp", "open", "close", "lastPrice")
    if isinstance(data, dict):
        for key, val in data.items():
            if _key_matches_index(key, name_upper) and isinstance(val, dict):
                for f in PRICE_FIELDS:
                    v = _sf(val.get(f))
                    if v:
                        return v
        sym = str(data.get("symbol", "") or data.get("name", "") or "").upper()
        if _key_matches_index(sym, name_upper):
            for f in PRICE_FIELDS:
                v = _sf(data.get(f))
                if v:
                    return v
        typ = str(data.get("type", "") or "").upper()
        if "INDEX" in typ:
            v = _sf(data.get("value") or data.get("ltp"))
            if v and 10_000 < v < 100_000:
                return v
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                v = _search_index_price(item, name_upper)
                if v:
                    return v
    return None

def _extract_first_price(data,
                         min_val: float = 100.0,
                         max_val: float = 10_000_000.0) -> Optional[float]:
    if isinstance(data, (int, float)):
        v = float(data)
        if min_val <= v <= max_val:
            return v
    elif isinstance(data, dict):
        for key in ("ltp", "close", "price", "value", "open"):
            v = _sf(data.get(key))
            if v and min_val <= v <= max_val:
                return v
    elif isinstance(data, list):
        for item in data:
            v = _extract_first_price(item, min_val, max_val)
            if v:
                return v
    return None

def find_underlying_price(captured: dict, name: str,
                          price_xhr_pattern: Optional[str]) -> Optional[float]:
    nu = name.upper()
    if price_xhr_pattern:
        for url, data in captured.items():
            if price_xhr_pattern in url:
                v = _sf(data.get("ltp") or data.get("close"))
                if v:
                    return v
    for url, data in captured.items():
        if "indices" in url:
            v = _search_index_price(data, nu)
            if v:
                return v
    for url, data in captured.items():
        if "live_prices" in url or "accord_points" in url:
            v = _extract_first_price(data)
            if v:
                return v
    return None

async def scrape_price_from_dom(page: Page) -> Optional[float]:
    try:
        v = await page.evaluate("""() => {
            const sels = [
                '[class*="currentPrice"]','[class*="spotPrice"]',
                '[class*="ltp"]','[class*="price"]','h1','h2'
            ];
            for (const s of sels) {
                for (const el of document.querySelectorAll(s)) {
                    const t = el.textContent.replace(/[Rs,\\s]/g,'');
                    const n = parseFloat(t);
                    if (n > 100 && n < 10000000) return n;
                }
            }
            return null;
        }""")
        return float(v) if v else None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Contract extraction
# ---------------------------------------------------------------------------
def extract_contracts(captured: dict, name: str) -> dict:
    result = {}
    for data in captured.values():
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, dict) and _has_ohlc(val):
                    if _key_matches_index(key, name):
                        result[key] = val
                    else:
                        parsed = _parse_sym_flexible(key)
                        if parsed and parsed[0] == name.upper():
                            result[key] = val
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    for key, val in item.items():
                        if isinstance(val, dict) and _has_ohlc(val):
                            parsed = _parse_sym_flexible(key)
                            if parsed and parsed[0] == name.upper():
                                result[key] = val
    return result

def extract_expiries_from_contracts(contracts: dict, name: str) -> list[str]:
    expiries: set[str] = set()
    for sym in contracts:
        parsed = _parse_sym_flexible(sym)
        if parsed and parsed[0] == name.upper():
            expiries.add(parsed[1])
    return sorted(expiries)

# ---------------------------------------------------------------------------
# MCX helpers
# FIX 1: use page.content() (full HTML incl. <head>) not inner_html("body")
# FIX 2: re.DOTALL so the regex matches __NEXT_DATA__ across newlines
# ---------------------------------------------------------------------------
async def _load_mcx_chain_data(page: Page, url: str) -> Optional[dict]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(5)                      # extra settle for MCX JS

        # Full page source — __NEXT_DATA__ lives in <head>, not <body>
        html = await page.content()

        m = re.search(
            r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL,                              # FIX: match across newlines
        )
        if not m:
            log.debug(f"MCX: __NEXT_DATA__ not found  (html len={len(html)})")
            # Dump first 300 chars of <head> to log so we can see what's there
            head_m = re.search(r'<head[^>]*>(.*?)</head>', html, re.DOTALL | re.IGNORECASE)
            if head_m:
                log.debug(f"MCX head snippet: {head_m.group(1)[:300]!r}")
            return None

        data       = json.loads(m.group(1))
        chain_root = (data.get("props", {})
                          .get("pageProps", {})
                          .get("optionChainPageData", {}))
        chain          = chain_root.get("optionChain", {})
        option_chains  = chain.get("optionChains", [])
        expiry_details = chain.get("expiryDetailsDto", {})
        live_price     = chain_root.get("livePrice", {})
        underlying     = live_price.get("ltp") or live_price.get("close")
        current_expiry = expiry_details.get("currentExpiry") or (
            expiry_details.get("expiryDates", [None])[0]
        )

        if not option_chains or not current_expiry:
            log.debug(f"MCX: parsed OK but chain empty  "
                      f"chains={len(option_chains)}  expiry={current_expiry!r}")
            return None

        return {
            "chains":     option_chains,
            "expiry":     current_expiry,
            "underlying": underlying,
        }
    except Exception as e:
        log.debug(f"MCX chain load error: {e}")
        return None

def _extract_mcx_chain_prices(chain_data: dict,
                               atm_strikes_paisa: set[int]) -> dict[str, dict]:
    result      = {}
    chains      = chain_data["chains"]
    expiry      = chain_data["expiry"]
    strike_unit = 100
    for item in chains:
        sp = int(item.get("strikePrice", 0))
        if sp not in atm_strikes_paisa:
            continue
        strike_inr = sp // strike_unit
        for opt_type, side_key in (("CE", "callOption"), ("PE", "putOption")):
            side  = item.get(side_key, {})
            ltp   = _sf(side.get("ltp"))
            close = _sf(side.get("close"))
            o     = _sf(side.get("open")) or close
            h     = _sf(side.get("high")) or ltp
            l     = _sf(side.get("low"))  or ltp
            if ltp is None:
                continue
            sym = f"MCX_{expiry}_{strike_inr}_{opt_type}"
            result[sym] = {"open": o, "high": h, "low": l, "ltp": ltp}
    return result

# ---------------------------------------------------------------------------
# Anti-detection
# ---------------------------------------------------------------------------
def _random_ua() -> str:
    ver = random.choice(_CHROME_VERSIONS)
    return (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{ver} Safari/537.36")

async def _human_delay(min_s: float = 0.3, max_s: float = 1.2) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))

async def _make_context(browser: Browser) -> BrowserContext:
    vp_w, vp_h = random.choice(_VIEWPORTS)
    ua = _random_ua()
    ctx = await browser.new_context(
        storage_state=str(STORAGE_FILE),
        user_agent=ua,
        viewport={"width": vp_w, "height": vp_h},
        screen={"width": vp_w, "height": vp_h},
        locale=random.choice(["en-US", "en-IN", "en-GB"]),
        timezone_id=random.choice(["Asia/Kolkata", "Asia/Dubai"]),
        color_scheme="light",
        has_touch=False,
        java_script_enabled=True,
        extra_http_headers={
            "Accept-Language": random.choice(["en-US,en;q=0.9", "en-IN,en;q=0.9"]),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    await ctx.add_init_script(f"""
        Object.defineProperty(navigator, 'webdriver', {{get: () => undefined}});
        Object.defineProperty(navigator, 'plugins', {{get: () => [1,2,3,4,5]}});
        Object.defineProperty(navigator, 'languages', {{get: () => ['en-US','en']}});
        Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => 8}});
        Object.defineProperty(navigator, 'deviceMemory', {{get: () => 8}});
        window.chrome = {{runtime: {{}}, loadTimes: {{}}, csi: {{}}}};
        Permissions.prototype.query = x => Promise.resolve({{state: 'granted'}});
    """)
    return ctx

# ---------------------------------------------------------------------------
# Browser navigation
# ---------------------------------------------------------------------------
async def navigate_and_capture(page: Page, url: str,
                                settle: float = PAGE_SETTLE) -> dict:
    captured: dict = {}

    async def _on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                captured[resp.url] = await resp.json()
        except Exception:
            pass

    page.on("response", _on_response)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await _human_delay(0.5, 1.5)
        with suppress(Exception):
            await page.mouse.wheel(0, random.randint(100, 400))
    except Exception as e:
        log.debug(f"goto error ({url[:60]}): {e}")
    await asyncio.sleep(settle + random.uniform(0, 0.8))
    page.remove_listener("response", _on_response)
    return captured

async def _click_expiry_in_dom(page: Page, expiry_date: str) -> bool:
    """
    Try <select> first (dispatches change event), then buttons/tabs/spans.
    Also checks data-value / data-expiry / data-date attributes.
    """
    try:
        dt    = datetime.strptime(expiry_date, "%Y-%m-%d")
        human = [
            dt.strftime("%-d %b %Y"), dt.strftime("%d %b %Y"),
            dt.strftime("%-d %B %Y"), dt.strftime("%d %B %Y"),
        ]
    except Exception:
        human = []

    result = await page.evaluate(
        """([iso, human]) => {
            for (const sel of document.querySelectorAll('select')) {
                for (const opt of sel.options) {
                    const v = (opt.value || opt.textContent || '').trim();
                    if (v === iso || v.startsWith(iso) ||
                            human.some(h => v.includes(h))) {
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                        return 'select:' + opt.value;
                    }
                }
            }
            const sels = document.querySelectorAll(
                '[role="tab"],[role="button"],[role="option"],button,td,th,li,span,div'
            );
            for (const el of sels) {
                const text = (
                    el.getAttribute('data-value') ||
                    el.getAttribute('data-expiry') ||
                    el.getAttribute('data-date') ||
                    el.textContent || ''
                ).trim();
                if (text === iso || text.startsWith(iso) ||
                        human.some(h => text === h || text.includes(h))) {
                    el.click();
                    return 'click:' + text;
                }
            }
            return null;
        }""",
        [expiry_date, human],
    )
    log.debug(f"expiry click result: {result}")
    return result is not None

async def capture_after_expiry_click(page: Page, expiry_date: str,
                                     settle: float = EXPIRY_SETTLE) -> dict:
    new_xhr: dict = {}

    async def _on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                new_xhr[resp.url] = await resp.json()
        except Exception:
            pass

    page.on("response", _on_response)
    await _click_expiry_in_dom(page, expiry_date)
    await asyncio.sleep(settle + random.uniform(0, 0.5))
    page.remove_listener("response", _on_response)
    return new_xhr

# ---------------------------------------------------------------------------
# Session check
# ---------------------------------------------------------------------------
async def _session_valid(browser: Browser) -> bool:
    """Check if current session is valid by attempting to access options page."""
    ctx = None
    page = None
    try:
        ctx = await _make_context(browser)
        page = await ctx.new_page()
        
        resp = await page.goto("https://groww.in/options/nifty",
            wait_until="domcontentloaded", timeout=20_000)
        await asyncio.sleep(3)
        
        title = await page.title()
        cur_url = page.url.lower()
        
        # Simple check like backup version
        if "login" in title.lower() or "sign" in title.lower():
            log.info(f"session invalid: title={title!r}")
            return False
        
        log.debug(f"session check OK: url={cur_url[:50]} title={title[:30]!r}")
        return True
    except Exception as e:
        log.warning(f"session check error: {e} — assuming expired")
        return False
    finally:
        with suppress(Exception):
            if page: await page.close()
        with suppress(Exception):
            if ctx: await ctx.close()

# ---------------------------------------------------------------------------
# Warmup
# ---------------------------------------------------------------------------
async def _warmup(browser: Browser) -> None:
    ctx = None
    page = None
    try:
        log.info("warming up session (loading options/nifty) ...")
        ctx = await _make_context(browser)
        page = await ctx.new_page()
        await page.goto("https://groww.in/options/nifty",
                        wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(4)
        log.info("warmup done")
    except Exception as e:
        log.warning(f"warmup failed (non-fatal): {e}")
    finally:
        with suppress(Exception):
            if page: await page.close()
        with suppress(Exception):
            if ctx: await ctx.close()

# ---------------------------------------------------------------------------
# Interactive login
# ---------------------------------------------------------------------------
async def _interactive_login(browser: Browser) -> bool:
    print("\n" + "="*60)
    print(" GROWW LOGIN REQUIRED")
    print("="*60)
    print("""
 A browser will open. Please:
 1. Login with your email/password
 2. Enter your PIN when prompted  
 3. Wait for the page to fully load
 4. Press ENTER here when done
""")

    ctx = None
    page = None
    try:
        ctx = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )
        page = await ctx.new_page()
        
        await page.goto("https://groww.in/options/nifty",
            wait_until="domcontentloaded", timeout=30_000)
        
        input("\n[Press ENTER after completing login + PIN]... ")

        title = await page.title()
        url = page.url.lower()
        
        if "login" in url or "sign" in title.lower():
            print("\n[!] Still on login page. Login failed.")
            return False
        
        await asyncio.sleep(2)
        await ctx.storage_state(path=str(STORAGE_FILE))
        
        state = json.loads(STORAGE_FILE.read_text())
        print(f"\n[OK] Session saved: {len(state.get('cookies', []))} cookies")
        return True
        
    except EOFError:
        print("\n[!] Non-interactive mode - waiting 30s...")
        await asyncio.sleep(30)
        return True
    except Exception as e:
        print(f"\n[!] Login error: {e}")
        return False
    finally:
        with suppress(Exception):
            if page: await page.close()
        with suppress(Exception):
            if ctx: await ctx.close()

# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------
class Scanner:
    def __init__(self):
        self.alerts:    list[Alert]      = []
        self.watchlist: dict[str, Alert] = {}

    def atm_strikes(self, price: float, step: int) -> set[int]:
        atm = round(price / step) * step
        return {atm + i * step for i in range(-STRIKE_RANGE, STRIKE_RANGE + 1)}

    async def sweep(self, browser: Browser) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        log.info(f"SWEEP {ts}")

        for inst in INSTRUMENTS:
            ctx = page = None
            try:
                ctx = await _make_context(browser)
                page = await ctx.new_page()
                await self._scan_instrument(page, inst)
            except Exception as e:
                log.warning(f"  {inst['name']}: {e}")
                if "--debug" in sys.argv:
                    traceback.print_exc()
            finally:
                with suppress(Exception):
                    if page: await page.close()
                with suppress(Exception):
                    if ctx: await ctx.close()

            await asyncio.sleep(random.uniform(1, 3))

        log.info(f"Summary: watching={len(self.watchlist)}, total={len(self.alerts)}")
        self._save()

    async def _scan_instrument(self, page: Page, inst: dict) -> None:
        if inst.get("is_mcx"):
            await self._scan_mcx(page, inst)
        else:
            await self._scan_nse(page, inst)

    async def _scan_nse(self, page: Page, inst: dict) -> None:
        name = inst["name"]
        url  = inst["url"]

        print(f"    loading ...", end="", flush=True)
        captured = await navigate_and_capture(page, url)
        print(f"  {len(captured)} XHR", flush=True)

        if not captured:
            log.warning(f"    {name}: no XHR captured")
            return

        price = (find_underlying_price(captured, name, inst.get("price_xhr_pattern"))
                 or await scrape_price_from_dom(page))
        if not price:
            log.warning(f"    {name}: could not find underlying price")
            if "--debug" in sys.argv:
                for u in sorted(captured):
                    log.debug(f"      XHR: {u}")
            return
        log.info(f"    {name}: price={price:,.2f}")

        all_contracts = extract_contracts(captured, name)
        log.info(f"    {name}: {len(all_contracts)} contracts from initial load")

        expiries = extract_expiries_from_contracts(all_contracts, name)
        if not expiries:
            log.warning(f"    {name}: no expiry dates found in contracts")
            return

        expiries = expiries[: inst["n_expiries"]]
        log.info(f"    {name}: expiries={expiries}")

        for exp in expiries[1:]:
            await _human_delay(0.3, 0.8)
            print(f"    -> {exp} ...", end="", flush=True)
            new_xhr = await capture_after_expiry_click(page, exp)
            new_c   = extract_contracts(new_xhr, name)
            all_contracts.update(new_c)
            print(f"  +{len(new_c)}", flush=True)

        log.info(f"    {name}: {len(all_contracts)} total contracts")

        allowed  = self.atm_strikes(price, inst["step"])
        new_hits = 0
        checked  = 0
        for expiry in expiries:
            for strike in sorted(allowed):
                for opt in ("CE", "PE"):
                    ohlc = key = None
                    for cand in build_symbol_candidates(name, expiry, strike, opt):
                        if cand in all_contracts:
                            ohlc = all_contracts[cand]
                            key  = cand
                            break
                    if ohlc is None or key is None:
                        continue
                    checked  += 1
                    new_hits += self._process(key, name, expiry, strike, opt, ohlc)

        log.info(f"    {name}: checked={checked}  new_alerts={new_hits}")

    async def _scan_mcx(self, page: Page, inst: dict) -> None:
        name = inst["name"]

        print(f"    loading MCX chain ...", end="", flush=True)
        chain_data = await _load_mcx_chain_data(page, inst["url"])
        if not chain_data:
            log.warning(f"    {name}: could not load option chain from page")
            return

        expiry           = chain_data["expiry"]
        underlying_price = chain_data.get("underlying")

        if not underlying_price:
            log.warning(f"    {name}: no underlying price in chain data")
            return

        print(f"  price={underlying_price:,.2f}  expiry={expiry}", flush=True)

        atm_strikes = self.atm_strikes(float(underlying_price), inst["step"])
        atm_paisa   = {s * 100 for s in atm_strikes}
        log.debug(f"    {name}: ATM strikes={sorted(atm_strikes)}")

        contracts = _extract_mcx_chain_prices(chain_data, atm_paisa)
        log.info(f"    {name}: {len(contracts)} ATM contracts with data")

        new_hits = 0
        checked  = 0
        for sym, ohlc in contracts.items():
            parts    = sym.split("_")
            strike   = int(parts[2])
            opt      = parts[3]
            checked  += 1
            new_hits += self._process(sym, name.upper(), expiry, strike, opt, ohlc)

        log.info(f"    {name}: checked={checked}  new_alerts={new_hits}")

    def _process(self, key: str, name: str, expiry: str,
                 strike: int, opt: str, ohlc: dict) -> int:
        o   = ohlc.get("open")
        h   = ohlc.get("high")
        l   = ohlc.get("low")
        ltp = ohlc.get("ltp")

        if o is None or h is None or l is None:
            return 0
        try:
            o, h, l = float(o), float(h), float(l)
        except (TypeError, ValueError):
            return 0

        if key in self.watchlist:
            cond = self.watchlist[key].condition
            if ltp is not None:
                try:
                    ltp_f = float(ltp)
                    if cond == "Open==High" and ltp_f > h:
                        self._mark_broken(key, ltp_f)
                    elif cond == "Open==Low" and ltp_f < l:
                        self._mark_broken(key, ltp_f)
                except (TypeError, ValueError):
                    pass
            return 0

        cond = None
        if o > 0 and o == h and l < h:   # O==H, doji guard: low must be below
            cond = "Open==High"
        elif o > 0 and o == l and h > l: # O==L, doji guard: high must be above
            cond = "Open==Low"
        if not cond:
            return 0

        alert = Alert(
            symbol     = f"{name}{expiry}{strike}{opt}"[:50],
            instrument = name,
            expiry     = expiry,
            strike     = strike,
            opt_type   = opt,
            condition  = cond,
            open       = o,
            high       = h,
            low        = l,
            ltp        = float(ltp) if ltp is not None else None,
        )
        self.alerts.append(alert)
        self.watchlist[key] = alert
        self._print_alert(alert)
        return 1

    def _mark_broken(self, key: str, ltp: float) -> None:
        a = self.watchlist.pop(key, None)
        if a:
            a.broken    = True
            a.broken_at = datetime.now().isoformat()
            a.ltp       = ltp
            self._print_breakout(a, ltp)

    @staticmethod
    def _print_alert(a: Alert) -> None:
        arrow = "^" if a.condition == "Open==High" else "v"
        log.info(f"[!] {arrow} NEW  {a.instrument} {a.strike}{a.opt_type}"
                 f"  exp={a.expiry}  {a.condition}"
                 f"  O={a.open:.2f}  H={a.high:.2f}  L={a.low:.2f}  LTP={a.ltp}")

    @staticmethod
    def _print_breakout(a: Alert, ltp: float) -> None:
        level = a.high if a.condition == "Open==High" else a.low
        side  = "HIGH ^" if a.condition == "Open==High" else "LOW v"
        log.info(f"[!] BROKEN {side}  {a.instrument} {a.strike}{a.opt_type}"
                 f"  exp={a.expiry}  level={level:.2f}  ltp={ltp:.2f}")

    def _save(self) -> None:
        payload = {
            "updated":  datetime.now().isoformat(),
            "watching": len(self.watchlist),
            "broken":   sum(1 for a in self.alerts if a.broken),
            "alerts":   [asdict(a) for a in self.alerts],
        }
        try:
            tmp = ALERT_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2, default=str),
                           encoding="utf-8")
            tmp.replace(ALERT_FILE)
        except Exception as e:
            log.error(f"could not save alerts: {e}")

# ---------------------------------------------------------------------------
# Main run loop - single browser, multiple tabs
# ---------------------------------------------------------------------------
async def _run_loop(once: bool) -> None:
    scanner = Scanner()

    async with async_playwright() as pw:
        # Launch browser
        log.info("launching browser...")
        browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"],
        )
        
        # Create context with storage
        vp_w, vp_h = random.choice(_VIEWPORTS)
        ctx = await browser.new_context(
            storage_state=str(STORAGE_FILE) if STORAGE_FILE.exists() else None,
            user_agent=_random_ua(),
            viewport={"width": vp_w, "height": vp_h},
            screen={"width": vp_w, "height": vp_h},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )
        
        # Apply stealth patches
        await ctx.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
            window.chrome = {runtime: {}, loadTimes: {}, csi: {}};
            Permissions.prototype.query = x => Promise.resolve({state: 'granted'});
        """)
        
        log.info("browser ready")

        # Open tabs for each instrument
        pages = []
        for inst in INSTRUMENTS:
            p = await ctx.new_page()
            await p.goto(inst["url"], wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(3)
            pages.append((inst, p))
            log.info(f"  opened tab: {inst['name']}")

        log.info(f"{len(pages)} tabs ready")

        if not once:
            log.info(f"continuous mode  interval={SCAN_INTERVAL}s")

        try:
            while True:
                ts = datetime.now().strftime("%H:%M:%S")
                log.info(f"SWEEP {ts}")

                for inst, page in pages:
                    name = inst["name"]
                    log.info(f"> {name}")
                    
                    # Reload to get fresh data
                    await page.reload(wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(PAGE_SETTLE)
                    
                    try:
                        await scanner._scan_instrument(page, inst)
                    except Exception as e:
                        log.warning(f"  {name}: {e}")
                        if "--debug" in sys.argv:
                            traceback.print_exc()

                log.info(f"Summary: watching={len(scanner.watchlist)}, total={len(scanner.alerts)}")
                scanner._save()

                if once:
                    break
                await asyncio.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log.info("interrupted by user")
        finally:
            # Save session
            with suppress(Exception):
                await ctx.storage_state(path=str(STORAGE_FILE))
            with suppress(Exception):
                await ctx.close()
            with suppress(Exception):
                await browser.close()
            log.info("done")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    once = "--once" in sys.argv
    try:
        asyncio.run(_run_loop(once))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
