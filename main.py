#!/usr/bin/env python3
"""
Groww Open==High / Open==Low Alert Scanner
Single entry point - handles login + scanning automatically.
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
import traceback
import urllib.request
import urllib.error
from logging.handlers import RotatingFileHandler
from urllib import parse as urllib_parse
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8003139162:AAHJOyWOzNRuNxhMLaulF6XBcXeXjyAP33g")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "-4926416519")

STORAGE_FILE  = Path("groww_state.json")
ALERT_FILE    = Path("alerts.json")
LOG_FILE      = Path("scanner.log")
SCAN_INTERVAL = 90
STRIKE_RANGE  = 12
PAGE_SETTLE   = 6.0
EXPIRY_SETTLE = 3.5
HEARTBEAT_EVERY = 40

log = logging.getLogger(__name__)

IST_TZ = None

def _init_tz():
    global IST_TZ
    try:
        from zoneinfo import ZoneInfo
        IST_TZ = ZoneInfo("Asia/Kolkata")
    except ImportError:
        import pytz
        IST_TZ = pytz.timezone("Asia/Kolkata")

_init_tz()

def _setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in root.handlers[:]:
        root.removeHandler(h)
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(file_handler)
    root.addHandler(stream_handler)

def _send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.debug("Telegram: no token/chat_id configured")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = urllib_parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception as e:
        log.warning("Telegram send failed: %s", e)
        return False

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
    xhr_key:    str = ""
    ltp:        Optional[float] = None
    seen_at:    str = field(default_factory=lambda: datetime.now().isoformat())
    broken:     bool = False
    broken_at:  Optional[str] = None

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

async def scrape_option_chain_from_dom(page: Page) -> dict:
    try:
        result = await page.evaluate("""() => {
            const script = document.querySelector('#__NEXT_DATA__');
            if (!script) return {contracts: {}, error: 'No __NEXT_DATA__ found'};
            
            const data = JSON.parse(script.textContent);
            const pageProps = data.props?.pageProps;
            if (!pageProps) return {contracts: {}, error: 'No pageProps'};
            
            const optionChain = pageProps.data?.optionChain;
            if (!optionChain) return {contracts: {}, error: 'No optionChain in pageProps'};
            
            const contracts = {};
            const optionContracts = optionChain.optionContracts || [];
            
            optionContracts.forEach(item => {
                const strikePrice = item.strikePrice;
                const strike = strikePrice / 100;
                
                if (item.ce) {
                    contracts[item.ce.growwContractId] = {
                        strike,
                        opt: 'CE',
                        growwId: item.ce.growwContractId,
                        close: item.ce.liveData?.close,
                        ltp: item.ce.liveData?.ltp
                    };
                }
                
                if (item.pe) {
                    contracts[item.pe.growwContractId] = {
                        strike,
                        opt: 'PE',
                        growwId: item.pe.growwContractId,
                        close: item.pe.liveData?.close,
                        ltp: item.pe.liveData?.ltp
                    };
                }
            });
            
            return {
                contracts,
                contractCount: Object.keys(contracts).length,
                expiry: optionChain.expiryDetailsDto?.currentExpiry,
                strikeCount: optionContracts.length
            };
        }""")
        return result
    except Exception as e:
        return {"error": str(e)}

async def expand_option_chain(page: Page) -> int:
    try:
        result = await page.evaluate("""() => {
            const container = document.querySelector('[class*="SplitScrollTable"]');
            if (!container) return {scrolled: 0, height: 0};
            
            const total = container.scrollHeight;
            let scrolled = 0;
            
            for (let pos = 0; pos <= total; pos += 200) {
                container.scrollTop = pos;
                scrolled++;
            }
            
            container.scrollTop = 0;
            
            return {scrolled, height: total};
        }""")
        return result.get("scrolled", 0)
    except Exception:
        return 0

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

async def _load_mcx_chain_data(page: Page, url: str) -> Optional[dict]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(5)
        html = await page.content()
        m = re.search(
            r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not m:
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
            return None
        return {
            "chains":     option_chains,
            "expiry":     current_expiry,
            "underlying": underlying,
        }
    except Exception:
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

def _random_ua() -> str:
    ver = random.choice(_CHROME_VERSIONS)
    return (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{ver} Safari/537.36")

async def _human_delay(min_s: float = 0.3, max_s: float = 1.2) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))

async def _make_context(browser) -> BrowserContext:
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
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
        Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
        Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
        window.chrome = {runtime: {}, loadTimes: {}, csi: {}};
        Permissions.prototype.query = x => Promise.resolve({state: 'granted'});
    """)
    return ctx

async def navigate_and_capture(page: Page, url: str,
                                settle: float = PAGE_SETTLE) -> dict:
    captured: dict = {}

    async def _on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                data = await resp.json()
                if resp.url in captured:
                    captured[resp.url].update(data)
                else:
                    captured[resp.url] = data
        except Exception:
            pass

    page.on("response", _on_response)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        log.warning("goto error: %s", e)
    
    await asyncio.sleep(settle)
    
    await _scroll_option_chain(page, captured)
    
    page.remove_listener("response", _on_response)
    return captured

async def _scroll_option_chain(page: Page, captured: dict) -> None:
    try:
        await page.evaluate("""() => {
            const container = document.querySelector('[class*="SplitScrollTable"]');
            if (!container) return;
            
            const total = container.scrollHeight;
            const step = Math.max(100, total / 100);
            
            for (let pos = 0; pos <= total; pos += step) {
                container.scrollTop = pos;
            }
            
            container.scrollTop = 0;
        }""")
        await asyncio.sleep(4)
    except Exception:
        pass

async def _click_expiry_in_dom(page: Page, expiry_date: str) -> bool:
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

def _is_market_open_equity() -> bool:
    now = datetime.now(IST_TZ)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return time(9, 14) <= t <= time(15, 31)

def _is_market_open_mcx() -> bool:
    now = datetime.now(IST_TZ)
    if now.weekday() >= 5:
        return False
    t = now.time()
    return time(9, 14) <= t <= time(23, 31)

async def _check_session_valid(browser) -> bool:
    ctx = page = None
    try:
        ctx = await _make_context(browser)
        page = await ctx.new_page()
        await page.goto("https://groww.in/options/nifty",
                        wait_until="domcontentloaded", timeout=20_000)
        await asyncio.sleep(3)
        title = await page.title()
        url = page.url.lower()
        if "login" in url or "login" in title.lower() or "sign" in title.lower():
            log.info("Session invalid (login page detected)")
            return False
        log.info("Session valid")
        return True
    except Exception as e:
        log.warning("Session check error: %s", e)
        return False
    finally:
        with suppress(Exception):
            if page: await page.close()
        with suppress(Exception):
            if ctx: await ctx.close()

async def _handle_session_expired() -> bool:
    _send_telegram(
        "⚠️ Groww session expired. Open groww.in, login, save cookies, "
        "then restart scanner."
    )
    log.warning("Session expired — waiting 10 min before retry")
    await asyncio.sleep(600)
    return True

class Scanner:
    def __init__(self):
        self.alerts:    list[Alert]      = []
        self.watchlist: dict[str, Alert] = {}
        self._new_alerts_batch: list[Alert] = []
        self._broken_alerts_batch: list[Alert] = []
        self._sweep_count = 0
        self._last_sweep_time: Optional[datetime] = None
        self._load()

    def _load(self) -> None:
        if not ALERT_FILE.exists():
            log.info("No alerts file found — starting fresh")
            return
        try:
            data = json.loads(ALERT_FILE.read_text(encoding="utf-8"))
            for d in data.get("alerts", []):
                if d.get("broken"):
                    continue
                try:
                    a = Alert(**d)
                except Exception:
                    continue
                xk = d.get("xhr_key", d.get("symbol", ""))
                if xk:
                    self.watchlist[xk] = a
                    self.alerts.append(a)
            log.info(
                "Loaded %d alerts from %s (watching=%d)",
                len(self.alerts), ALERT_FILE, len(self.watchlist)
            )
        except Exception as e:
            log.warning("Could not load alerts: %s", e)

    def atm_strikes(self, price: float, step: int) -> set[int]:
        atm = round(price / step) * step
        return {atm + i * step for i in range(-STRIKE_RANGE, STRIKE_RANGE + 1)}

    async def sweep(self, browser) -> None:
        self._sweep_count += 1
        ts = datetime.now().strftime("%H:%M:%S")
        log.info("=" * 56)
        log.info("SWEEP  #%d  %s", self._sweep_count, ts)
        log.info("=" * 56)
        self._new_alerts_batch.clear()
        self._broken_alerts_batch.clear()

        equity_open  = _is_market_open_equity()
        mcx_open     = _is_market_open_mcx()
        log.debug("Market status — equity=%s, mcx=%s", equity_open, mcx_open)

        for inst in INSTRUMENTS:
            if inst["is_mcx"] and not mcx_open:
                log.debug("Skipping %s (MCX closed)", inst["name"])
                continue
            if not inst["is_mcx"] and not equity_open:
                log.debug("Skipping %s (equity closed)", inst["name"])
                continue

            ctx = page = None
            try:
                ctx = await _make_context(browser)
                page = await ctx.new_page()
                await self._scan_instrument(page, inst)
            except Exception as e:
                log.error("%s: error — %s", inst["name"], e)
                if "--debug" in sys.argv:
                    traceback.print_exc()
            finally:
                with suppress(Exception):
                    if page: await page.close()
                with suppress(Exception):
                    if ctx: await ctx.close()

            await asyncio.sleep(random.uniform(1.5, 3.5))

        self._last_sweep_time = datetime.now()
        log.info(
            "Summary: watching=%d, broken=%d, total=%d",
            len(self.watchlist),
            sum(1 for a in self.alerts if a.broken),
            len(self.alerts),
        )
        self._send_batch_telegram()
        self._save()
        self._heartbeat()

    def _heartbeat(self) -> None:
        if self._sweep_count % HEARTBEAT_EVERY == 0:
            ts = (
                self._last_sweep_time.strftime("%H:%M")
                if self._last_sweep_time else "N/A"
            )
            msg = (
                f"🟢 alive | watching={len(self.watchlist)} "
                f"| sweeps={self._sweep_count} | last={ts}"
            )
            log.info("HEARTBEAT: %s", msg)
            _send_telegram(msg)

    async def _scan_instrument(self, page: Page, inst: dict) -> None:
        name = inst["name"]

        if inst.get("is_mcx"):
            await self._scan_mcx(page, inst)
            return

        log.info("%s: loading ...", name)
        captured = await navigate_and_capture(page, inst["url"])
        log.info("%s: captured %d XHR responses", name, len(captured))

        if not captured:
            log.info("%s: no XHR captured", name)
            return

        price = (find_underlying_price(captured, name, inst.get("price_xhr_pattern"))
                 or await scrape_price_from_dom(page))
        if not price:
            log.info("%s: could not find underlying price", name)
            if "--debug" in sys.argv:
                for u in sorted(captured):
                    log.debug("  XHR: %s", u)
            return
        log.info("%s: price=%s", name, f"{price:,.2f}")

        all_contracts = extract_contracts(captured, name)
        xhr_count = len(all_contracts)
        log.info("%s: %d contracts from XHR", name, xhr_count)

        expiries = extract_expiries_from_contracts(all_contracts, name)
        if not expiries:
            log.info("%s: no expiry dates found", name)
            return

        expiries = expiries[: inst["n_expiries"]]
        log.info("%s: expiries=%s", name, expiries)

        for exp in expiries[1:]:
            await _human_delay(0.3, 0.8)
            log.info("%s -> %s ...", name, exp)
            new_xhr = await capture_after_expiry_click(page, exp)
            new_c   = extract_contracts(new_xhr, name)
            all_contracts.update(new_c)
            log.info("%s -> %s: +%d contracts", name, exp, len(new_c))

        log.info("%s: %d contracts before DOM scrape", name, len(all_contracts))

        await _human_delay(0.5, 1.0)
        expanded = await expand_option_chain(page)
        if expanded > 0:
            log.info("%s: expanded (clicked %d buttons)", name, expanded)

        dom_data = await scrape_option_chain_from_dom(page)
        dom_expiry = dom_data.get("expiry")
        if dom_expiry:
            expiries = [dom_expiry] + [e for e in expiries if e != dom_expiry][:inst["n_expiries"]-1]
            log.info("%s: DOM expiry=%s", name, dom_expiry)
        
        dom_contracts = dom_data.get("contracts", {})
        dom_strike_count = dom_data.get("strikeCount", 0)
        log.info("%s: %d strikes from DOM", name, dom_strike_count)
        
        dom_count = 0
        for groww_id, dom_val in dom_contracts.items():
            if groww_id not in all_contracts:
                all_contracts[groww_id] = {
                    "open": None,
                    "high": None,
                    "low": None,
                    "ltp": dom_val.get("ltp"),
                    "close": dom_val.get("close"),
                }
                dom_count += 1
        
        if dom_count > 0:
            log.info("%s: +%d contracts from DOM", name, dom_count)

        log.info("%s: %d total contracts", name, len(all_contracts))

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

        log.info("%s: checked=%d new_alerts=%d", name, checked, new_hits)

    async def _scan_mcx(self, page: Page, inst: dict) -> None:
        name = inst["name"]

        log.info("%s: loading MCX chain ...", name)
        chain_data = await _load_mcx_chain_data(page, inst["url"])
        if not chain_data:
            log.info("%s: MCX chain load failed", name)
            return

        expiry           = chain_data["expiry"]
        underlying_price = chain_data.get("underlying")

        if not underlying_price:
            log.info("%s: no underlying price", name)
            return

        log.info(
            "%s: price=%s expiry=%s",
            name, f"{underlying_price:,.2f}", expiry
        )

        atm_strikes = self.atm_strikes(float(underlying_price), inst["step"])
        atm_paisa   = {s * 100 for s in atm_strikes}

        contracts = _extract_mcx_chain_prices(chain_data, atm_paisa)
        log.info("%s: %d ATM contracts", name, len(contracts))

        new_hits = 0
        checked  = 0
        for sym, ohlc in contracts.items():
            parts    = sym.split("_")
            strike   = int(parts[2])
            opt      = parts[3]
            checked  += 1
            new_hits += self._process(sym, name.upper(), expiry, strike, opt, ohlc)

        log.info("%s: checked=%d new_alerts=%d", name, checked, new_hits)

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
                    if cond == "Open==High" and ltp_f > h + 0.001:
                        self._mark_broken(key, ltp_f)
                    elif cond == "Open==Low" and ltp_f < l - 0.001:
                        self._mark_broken(key, ltp_f)
                except (TypeError, ValueError):
                    pass
            return 0

        cond = None
        if o > 0 and abs(o - h) <= 0.001 and l < h:
            cond = "Open==High"
        elif o > 0 and abs(o - l) <= 0.001 and h > l:
            cond = "Open==Low"
        if not cond:
            return 0

        alert = Alert(
            symbol     = f"{name}{expiry}{strike}{opt}"[:50],
            xhr_key    = key,
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
        self.watchlist[alert.xhr_key] = alert
        self._new_alerts_batch.append(alert)
        self._print_alert(alert)
        return 1

    def _mark_broken(self, key: str, ltp: float) -> None:
        a = self.watchlist.pop(key, None)
        if a:
            a.broken    = True
            a.broken_at = datetime.now().isoformat()
            a.ltp       = ltp
            self._broken_alerts_batch.append(a)
            self._print_breakout(a, ltp)

    @staticmethod
    def _print_alert(a: Alert) -> None:
        arrow = "^" if a.condition == "Open==High" else "v"
        log.info(
            "[!] %s NEW  %s %s%s  exp=%s  %s  O=%.2f  H=%.2f  L=%.2f",
            arrow, a.instrument, a.strike, a.opt_type,
            a.expiry, a.condition, a.open, a.high, a.low
        )

    @staticmethod
    def _print_breakout(a: Alert, ltp: float) -> None:
        level = a.high if a.condition == "Open==High" else a.low
        side  = "HIGH ^" if a.condition == "Open==High" else "LOW v"
        log.info(
            "[!] BROKEN %s  %s %s%s  exp=%s  level=%.2f  ltp=%.2f",
            side, a.instrument, a.strike, a.opt_type,
            a.expiry, level, ltp
        )

    def _send_batch_telegram(self) -> None:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        
        lines = []
        if self._new_alerts_batch:
            lines.append("🔔 <b>NEW ALERTS</b>")
            for a in self._new_alerts_batch:
                lines.append(f"{a.instrument} {a.strike}{a.opt_type} | {a.condition}")
        
        if self._broken_alerts_batch:
            if lines:
                lines.append("")
            lines.append("💥 <b>BROKEN ALERTS</b>")
            for a in self._broken_alerts_batch:
                lines.append(
                    f"{a.instrument} {a.strike}{a.opt_type} | "
                    f"{a.condition} | LTP={a.ltp:.2f}"
                )
        
        if lines:
            ts = datetime.now().strftime("%H:%M")
            msg = f"📊 <b>Scanner Update {ts}</b>\n\n" + "\n".join(lines)
            _send_telegram(msg)

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
            log.error("Could not save alerts: %s", e)


async def _run_loop(once: bool = False, headless: bool = True):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled",
                  "--no-sandbox", "--disable-dev-shm-usage"],
        )

        session_valid = False
        if STORAGE_FILE.exists():
            log.info("Checking existing session ...")
            session_valid = await _check_session_valid(browser)
        else:
            log.info("No session file found.")

        if not session_valid:
            if not await _interactive_login(browser):
                log.error("Login failed!")
                await browser.close()
                return

        scanner = Scanner()

        if once:
            await scanner.sweep(browser)
        else:
            log.info(
                "Continuous mode (interval=%ds, Ctrl+C to stop)", SCAN_INTERVAL
            )
            while True:
                await scanner.sweep(browser)
                await asyncio.sleep(SCAN_INTERVAL)

        await browser.close()
        log.info("Done!")


async def _interactive_login(browser) -> bool:
    log.info("=" * 56)
    log.info("LOGIN REQUIRED")
    log.info("=" * 56)
    log.info(
        "1. Browser will open groww.in\n"
        "2. Login with email/password\n"
        "3. Complete PIN verification if asked\n"
        "4. Wait for page to fully load\n"
        "5. Press ENTER here when done"
    )

    ctx = page = None
    try:
        ctx = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )
        page = await ctx.new_page()
        await page.goto("https://groww.in/login",
                        wait_until="domcontentloaded", timeout=30_000)

        log.info("Press ENTER after completing login + PIN ... ")
        try:
            input()
        except EOFError:
            await asyncio.sleep(30)

        url = page.url.lower()
        title = await page.title()
        if "login" in url or "sign" in title.lower():
            log.error("Still on login page — login failed")
            return False

        await ctx.storage_state(path=str(STORAGE_FILE))
        state = json.loads(STORAGE_FILE.read_text())
        log.info("Session saved: %d cookies", len(state.get("cookies", [])))
        return True

    except Exception as e:
        log.error("Login error: %s", e)
        return False
    finally:
        with suppress(Exception):
            if page: await page.close()
        with suppress(Exception):
            if ctx: await ctx.close()


async def main():
    _setup_logging()
    log.info("=" * 56)
    log.info("Groww Open==High / Open==Low Alert Scanner")
    log.info("=" * 56)

    once     = "--once" in sys.argv
    headless = "--visible" not in sys.argv

    if not headless:
        log.info("VISIBLE mode (pass --visible to enable window)")

    browser_crashes = 0
    sweep_crashes   = 0

    while True:
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=headless,
                    args=["--disable-blink-features=AutomationControlled",
                          "--no-sandbox", "--disable-dev-shm-usage"],
                )

                session_valid = False
                if STORAGE_FILE.exists():
                    log.info("Checking existing session ...")
                    session_valid = await _check_session_valid(browser)
                else:
                    log.info("No session file found.")

                if not session_valid:
                    if not await _handle_session_expired():
                        await browser.close()
                        continue
                    if not await _interactive_login(browser):
                        log.error("Login failed!")
                        await browser.close()
                        continue

                scanner = Scanner()

                if once:
                    await scanner.sweep(browser)
                    await browser.close()
                    log.info("Done!")
                    break

                log.info(
                    "Continuous mode (interval=%ds, Ctrl+C to stop)", SCAN_INTERVAL
                )
                while True:
                    try:
                        await scanner.sweep(browser)
                        await asyncio.sleep(SCAN_INTERVAL)
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        sweep_crashes += 1
                        log.error("Sweep-level crash (#%d): %s", sweep_crashes, e,
                                  exc_info=True)
                        _send_telegram(
                            f"⚠️ Scanner sweep crashed ({sweep_crashes}): {e}\n"
                            f"Resuming in {SCAN_INTERVAL}s"
                        )
                        await asyncio.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            log.info("Interrupted — shutting down gracefully")
            break
        except Exception as e:
            browser_crashes += 1
            log.error("Browser-level crash (#%d): %s", browser_crashes, e,
                      exc_info=True)
            _send_telegram(
                f"⚠️ Browser crashed ({browser_crashes}): {e}\n"
                f"Restarting in 30s"
            )
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
