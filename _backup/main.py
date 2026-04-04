#!/usr/bin/env python3
"""
Groww Open==High / Open==Low Alert Scanner
Single entry point - handles login + scanning automatically.
"""
import asyncio
import json
import re
import sys
import random
import traceback
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import suppress

from playwright.async_api import async_playwright, BrowserContext, Page

# -- Config --------------------------------------------------------------------
STORAGE_FILE  = Path("groww_state.json")
ALERT_FILE    = Path("alerts.json")
SCAN_INTERVAL = 90
STRIKE_RANGE  = 12
PAGE_SETTLE   = 6.0
EXPIRY_SETTLE = 3.0

INSTRUMENTS = [
    dict(name="NIFTY",    step=50,  n_expiries=2, url="https://groww.in/options/nifty",               price_xhr_pattern=None,     is_mcx=False),
    dict(name="SENSEX",   step=100, n_expiries=2, url="https://groww.in/options/sp-bse-sensex",      price_xhr_pattern=None,     is_mcx=False),
    dict(name="RELIANCE", step=10,  n_expiries=1, url="https://groww.in/options/reliance-industries-ltd", price_xhr_pattern=None, is_mcx=False),
    dict(name="SBIN",     step=5,   n_expiries=1, url="https://groww.in/options/state-bank-of-india", price_xhr_pattern=None,     is_mcx=False),
    dict(name="GOLDM",    step=100, n_expiries=1, url="https://groww.in/commodities/options/mcx_goldm?exchange=MCX",   price_xhr_pattern="COMMODITY/487819/latest", is_mcx=True),
    dict(name="SILVERM",  step=500, n_expiries=1, url="https://groww.in/commodities/options/mcx_silverm?exchange=MCX", price_xhr_pattern="COMMODITY/457533/latest", is_mcx=True),
]

CAPTURE_PATTERNS = (
    "tr_live_prices", "tr_live_indices", "option_chain",
    "latest_prices_batch", "nearest_expiries", "commodity_fo",
    "commodity_router", "market_timing",
)

_INDEX_ALIASES = {
    "NIFTY":  ["NIFTY"],
    "SENSEX": ["SENSEX", "BSE SENSEX", "S&P BSE"],
}

# -- Anti-detection fingerprints -------------------------------------------------
_CHROME_VERSIONS = ["124.0.0.0", "123.0.0.0", "125.0.0.0", "126.0.0.0", "122.0.0.0"]
_VIEWPORTS = [(1920, 1080), (1792, 1120), (1856, 1056), (1680, 1050), (1536, 864)]

# -- Data model ----------------------------------------------------------------
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
    seen_at:    str = field(default_factory=lambda: datetime.now().isoformat())
    broken:     bool = False
    broken_at:  Optional[str] = None

# -- Utility functions ---------------------------------------------------------
def _sf(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None

def _is_ohlc(d: dict) -> bool:
    return bool(d.get("open") or d.get("ltp") or d.get("close"))

# -- Date code parsing ---------------------------------------------------------
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
                    mm = int(code[2 : 2 + split])
                    dd = int(code[2 + split :])
                    return datetime.strptime(f"20{yy}{mm:02d}{dd:02d}", "%Y%m%d").strftime("%Y-%m-%d")
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
            return datetime.strptime(f"20{yy} {mon} 01", "%Y %b %d").strftime("%Y-%m-%d")
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
    dt = datetime.strptime(expiry, "%Y-%m-%d")
    year_short = dt.strftime("%y")
    month_abbr = dt.strftime("%b").upper()
    month_full = dt.strftime("%m")
    day_2 = dt.strftime("%d")

    code_6 = f"{year_short}{month_full}{day_2}"
    code_5 = f"{year_short}{dt.month}{day_2}"
    code_4 = f"{year_short}{month_full}"

    strike_s = str(strike)
    candidates = [
        f"{name}{code_6}{strike_s}{opt}",
        f"{name}{code_5}{strike_s}{opt}",
        f"{name}{code_4}{strike_s}{opt}",
        f"{name}{year_short}{month_abbr}{strike_s}{opt}",
    ]
    return candidates

# -- Price extraction ----------------------------------------------------------
def _key_matches_index(key: str, name_upper: str) -> bool:
    needles = _INDEX_ALIASES.get(name_upper, [name_upper])
    ku = key.upper()
    return any(n in ku for n in needles)

def _search_index_price(data, name_upper: str) -> Optional[float]:
    PRICE_FIELDS = ("value", "ltp", "open", "close", "lastPrice")
    if isinstance(data, dict):
        for key, val in data.items():
            if _key_matches_index(key, name_upper) and isinstance(val, dict):
                for f in PRICE_FIELDS:
                    v = _sf(val.get(f))
                    if v and v > 0:
                        return v
        sym = str(data.get("symbol", "") or data.get("name", "") or "").upper()
        if _key_matches_index(sym, name_upper):
            for f in PRICE_FIELDS:
                v = _sf(data.get(f))
                if v and v > 0:
                    return v
        typ = str(data.get("type", "") or "").upper()
        if "INDEX" in typ:
            v = _sf(data.get("value") or data.get("ltp"))
            if v and 10000 < v < 100000:
                return v
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                v = _search_index_price(item, name_upper)
                if v:
                    return v
    return None

def _extract_first_price(data, min_val: float = 100.0, max_val: float = 10_000_000.0) -> Optional[float]:
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

def find_underlying_price(captured: dict, name: str, price_xhr_pattern: Optional[str]) -> Optional[float]:
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
        if "live_prices" in url:
            v = _extract_first_price(data)
            if v:
                return v
    return None

async def scrape_price_from_dom(page: Page) -> Optional[float]:
    try:
        v = await page.evaluate(
            """() => {
                const sels = ['[class*="currentPrice"]','[class*="spotPrice"]','[class*="ltp"]','[class*="price"]','h1','h2'];
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

# -- Contract extraction --------------------------------------------------------
_OPT_SYM_RE = re.compile(
    r'^([A-Z]{2,12})(\d{4,8}|[0-9]{2}[A-Z]{3})(\d{2,6})(CE|PE)$|'
    r'^([A-Z]{2,12})(\d{4,8}|[0-9]{2}[A-Z]{3})(CE|PE)(\d{2,6})$'
)

def extract_contracts(captured: dict, name: str) -> dict:
    result = {}
    for data in captured.values():
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, dict) and _is_ohlc(val):
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
                        if isinstance(val, dict) and _is_ohlc(val):
                            parsed = _parse_sym_flexible(key)
                            if parsed and parsed[0] == name.upper():
                                result[key] = val
    return result

def extract_expiries_from_contracts(contracts: dict, name: str) -> set:
    expiries = set()
    for sym in contracts:
        parsed = _parse_sym_flexible(sym)
        if parsed and parsed[0] == name.upper():
            expiries.add(parsed[1])
    return expiries

# -- MCX helpers ---------------------------------------------------------------
async def _load_mcx_chain_data(page: Page, url: str) -> Optional[dict]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(4)
        html = await page.inner_html("body")
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html)
        if not m:
            return None
        data = json.loads(m.group(1))
        chain_root = (data.get("props", {}).get("pageProps", {})
                      .get("optionChainPageData", {}))
        chain = chain_root.get("optionChain", {})
        option_chains = chain.get("optionChains", [])
        expiry_details = chain.get("expiryDetailsDto", {})
        live_price = chain_root.get("livePrice", {})
        underlying_price = live_price.get("ltp") or live_price.get("close")
        current_expiry = expiry_details.get("currentExpiry") or (
            expiry_details.get("expiryDates", [None])[0]
        )
        if not option_chains or not current_expiry:
            return None
        return {
            "chains": option_chains,
            "expiry": current_expiry,
            "underlying": underlying_price,
        }
    except Exception:
        return None

def _extract_mcx_chain_prices(chain_data: Optional[dict], atm_strikes_paisa: set[int]) -> dict[str, dict]:
    if not chain_data:
        return {}
    result = {}
    chains = chain_data["chains"]
    expiry = chain_data["expiry"]
    strike_unit = 100

    for item in chains:
        sp = int(item.get("strikePrice", 0))
        if sp not in atm_strikes_paisa:
            continue
        strike_inr = sp // strike_unit

        for opt_type, side_key in (("CE", "callOption"), ("PE", "putOption")):
            side = item.get(side_key, {})
            ltp = _sf(side.get("ltp"))
            close = _sf(side.get("close"))
            open_val = _sf(side.get("open")) or close
            high_val = _sf(side.get("high")) or ltp
            low_val = _sf(side.get("low")) or ltp
            if ltp is None:
                continue
            sym = f"MCX_GOLDM_{expiry}_{strike_inr}_{opt_type}"
            result[sym] = {
                "open": open_val,
                "high": high_val,
                "low":  low_val,
                "ltp":  ltp,
            }
    return result

# -- Anti-detection & human behavior -------------------------------------------
def _random_ua() -> str:
    ver = random.choice(_CHROME_VERSIONS)
    return (f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{ver} Safari/537.36")

async def _human_delay(min_s: float = 0.3, max_s: float = 1.2) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))

async def _make_context(browser) -> BrowserContext:
    vp_w, vp_h = random.choice(_VIEWPORTS)
    ctx = await browser.new_context(
        storage_state   = str(STORAGE_FILE),
        user_agent      = _random_ua(),
        viewport        = {"width": vp_w, "height": vp_h},
        locale          = random.choice(["en-US", "en-IN", "en-GB"]),
        timezone_id     = random.choice(["Asia/Kolkata", "Asia/Dubai"]),
        extra_http_headers = {
            "Accept-Language": random.choice(["en-US,en;q=0.9", "en-IN,en;q=0.9"]),
            "Accept": random.choice(["text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "*/*"]),
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        },
    )
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        window.chrome = {runtime: {}};
    """)
    return ctx

# -- Browser navigation ---------------------------------------------------------
async def navigate_and_capture(page: Page, url: str, settle: float = PAGE_SETTLE) -> dict:
    captured: dict = {}

    async def _on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                captured[resp.url] = await resp.json()
        except Exception:
            pass

    page.on("response", _on_response)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await _human_delay(0.5, 1.5)
        with suppress(Exception):
            await page.mouse.wheel(0, random.randint(100, 400))
    except Exception as e:
        print(f"    ! goto error: {e}")
    jitter = random.uniform(0, 0.8)
    await asyncio.sleep(settle + jitter)
    page.remove_listener("response", _on_response)
    return captured

async def _click_expiry_in_dom(page: Page, expiry_date: str) -> bool:
    try:
        dt = datetime.strptime(expiry_date, "%Y-%m-%d")
        human = [dt.strftime("%-d %b %Y"), dt.strftime("%d %b %Y"),
                 dt.strftime("%-d %B %Y"), dt.strftime("%d %B %Y")]
        iso = expiry_date
        result = await page.evaluate(
            """([iso, human]) => {
            const sels = document.querySelectorAll('[role="tab"], [role="button"], td, th, span, div');
            for (const el of sels) {
                const text = el.textContent.trim();
                if (text === iso || text.startsWith(iso) ||
                    human.some(h => text === h || text.includes(h))) {
                    el.click();
                    return 'click:' + text;
                }
            }
            return null;
        }""", [expiry_date, human])
        print(f"    -> click result: {result}")
        return result is not None
    except Exception as e:
        print(f"    ! click error: {e}")
        return False

async def capture_after_expiry_click(page: Page, expiry_date: str, settle: float = EXPIRY_SETTLE) -> dict:
    new_xhr: dict = {}

    async def _on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                new_xhr[resp.url] = await resp.json()
        except Exception:
            pass

    page.on("response", _on_response)
    await _click_expiry_in_dom(page, expiry_date)
    jitter = random.uniform(0, 0.5)
    await asyncio.sleep(settle + jitter)
    page.remove_listener("response", _on_response)
    return new_xhr

# -- Login ---------------------------------------------------------------------
async def _interactive_login(browser) -> bool:
    print("\n  LOGIN REQUIRED - Opening browser for interactive login...")
    print("  Steps:")
    print("    1. Login to groww.in if not already logged in")
    print("    2. Wait for page to fully load")
    print("    3. Press ENTER here when done\n")
    ctx = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        locale="en-IN",
        timezone_id="Asia/Kolkata",
    )
    page = await ctx.new_page()
    try:
        await page.goto("https://groww.in/webstock", wait_until="domcontentloaded", timeout=30000)
        try:
            input("\n  Press ENTER after completing login... ")
        except (EOFError, IOError):
            print("  (auto-detected login, continuing...)")
            await asyncio.sleep(5)
        await ctx.storage_state(path=str(STORAGE_FILE))
        print(f"  Session saved to {STORAGE_FILE}")
        return True
    finally:
        await page.close()
        await ctx.close()

# -- Scanner -------------------------------------------------------------------
class Scanner:
    def __init__(self):
        self.alerts:    list[Alert] = []
        self.watchlist: dict[str, Alert] = {}

    def atm_strikes(self, price: float, step: int) -> set[int]:
        if STRIKE_RANGE == 0:
            return set()
        atm = round(price / step) * step
        return {atm + i * step for i in range(-STRIKE_RANGE, STRIKE_RANGE + 1)}

    async def sweep(self, browser) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'=' * 72}\n  SWEEP  {ts}\n{'=' * 72}")

        instrument_order = INSTRUMENTS.copy()
        random.shuffle(instrument_order)

        for i, inst in enumerate(instrument_order):
            print(f"\n  > {inst['name']}")
            ctx = await _make_context(browser)
            try:
                page = await ctx.new_page()
                try:
                    await self._scan_instrument(page, inst)
                except Exception as e:
                    print(f"    - unhandled error: {e}")
                    if "--debug" in sys.argv:
                        traceback.print_exc()
                finally:
                    await page.close()
            finally:
                await ctx.close()

            if i < len(instrument_order) - 1:
                delay = random.uniform(1.5, 4.0)
                print(f"    (~ {delay:.1f}s pause)")
                await asyncio.sleep(delay)

        active = len(self.watchlist)
        broken = sum(1 for a in self.alerts if a.broken)
        print(f"\n  Summary -> watching: {active}  broken: {broken}  total: {len(self.alerts)}")
        self._save()

    async def _scan_instrument(self, page: Page, inst: dict) -> None:
        name              = inst["name"]
        url               = inst["url"]
        price_xhr_pattern = inst.get("price_xhr_pattern")
        is_mcx            = inst.get("is_mcx", False)

        print(f"    loading page ...", end="", flush=True)
        captured = await navigate_and_capture(page, url)
        print(f"  {len(captured)} XHR captured")

        price = find_underlying_price(captured, name, price_xhr_pattern)
        if not price:
            price = await scrape_price_from_dom(page)
        if not price:
            print("    - could not find underlying price")
            if "--debug" in sys.argv:
                self._dump_xhr_urls(captured)
            return
        print(f"    price = {price:,.2f}")

        if is_mcx:
            await self._scan_mcx(page, inst, captured, price)
            return

        all_contracts = extract_contracts(captured, name)
        print(f"    contracts captured: {len(all_contracts)}")
        if "--debug" in sys.argv and all_contracts:
            sample = list(all_contracts.keys())[:5]
            print(f"    sample: {sample}")

        expiries = extract_expiries_from_contracts(all_contracts, name)
        if not expiries:
            print("    - no expiry dates found")
            if "--debug" in sys.argv:
                self._dump_raw_option_keys(captured)
            return

        expiries = sorted(expiries)[: inst["n_expiries"]]
        print(f"    expiries: {expiries}")

        for exp in expiries[1:]:
            await _human_delay(0.3, 0.8)
            print(f"    -> expiry {exp} ...", end="", flush=True)
            new_xhr = await capture_after_expiry_click(page, exp)
            new_c   = extract_contracts(new_xhr, name)
            all_contracts.update(new_c)
            print(f"  +{len(new_c)} contracts")

        print(f"    total: {len(all_contracts)} contracts")

        allowed  = self.atm_strikes(price, inst["step"])
        new_hits = 0
        checked  = 0
        for expiry in expiries:
            for strike in sorted(allowed):
                for opt in ("CE", "PE"):
                    ohlc = None
                    key  = None
                    for cand in build_symbol_candidates(name, expiry, strike, opt):
                        if cand in all_contracts:
                            ohlc = all_contracts[cand]
                            key  = cand
                            break
                    if ohlc is None or key is None:
                        continue
                    checked  += 1
                    new_hits += self._process(key, name, expiry, strike, opt, ohlc)

        print(f"    checked: {checked}  new alerts: {new_hits}")

    async def _scan_mcx(self, page: Page, inst: dict, captured: dict, price: float) -> None:
        name = inst["name"]
        step = inst["step"]

        chain_data = await _load_mcx_chain_data(page, inst["url"])
        if not chain_data:
            print(f"    - could not load options chain from page")
            return

        expiry = chain_data["expiry"]
        underlying_price = chain_data.get("underlying") or price
        print(f"    expiry: {expiry}, underlying: {underlying_price}")

        atm_strikes = self.atm_strikes(underlying_price, step)
        atm_paisa = {s * 100 for s in atm_strikes}
        print(f"    ATM strikes: {sorted(atm_strikes)}")

        contracts = _extract_mcx_chain_prices(chain_data, atm_paisa)
        print(f"    ATM contracts with data: {len(contracts)}")

        new_hits = 0
        checked = 0
        for sym, ohlc in contracts.items():
            parts = sym.split("_")
            strike = int(parts[3])
            opt = parts[4]
            checked += 1
            new_hits += self._process(sym, name.upper(), expiry, strike, opt, ohlc)

        print(f"    checked: {checked}  new alerts: {new_hits}")

    def _process(self, key: str, name: str, expiry: str,
                 strike: int, opt: str, ohlc: dict) -> int:
        o, h, l, ltp = ohlc["open"], ohlc["high"], ohlc["low"], ohlc.get("ltp")

        if key in self.watchlist:
            cond = self.watchlist[key].condition
            if cond == "Open==High" and ltp and ltp > h:
                self._mark_broken(key, ltp)
            elif cond == "Open==Low" and ltp and ltp < l:
                self._mark_broken(key, ltp)
            return 0

        cond = None
        if o > 0 and o == h and l < h and l < o:
            cond = "Open==High"
        elif o > 0 and o == l and h > l and h > o:
            cond = "Open==Low"
        if not cond:
            return 0

        alert = Alert(
            symbol=f"{name}{expiry}{strike}{opt}"[:50],
            instrument=name, expiry=expiry, strike=strike,
            opt_type=opt, condition=cond,
            open=o, high=h, low=l, ltp=ltp,
        )
        self.alerts.append(alert)
        self.watchlist[key] = alert
        self._print_alert(alert)
        return 1

    def _mark_broken(self, key: str, ltp: float) -> None:
        a = self.watchlist.pop(key, None)
        if a:
            a.broken = True
            a.broken_at = datetime.now().isoformat()
            a.ltp = ltp
            self._print_breakout(a, ltp)

    @staticmethod
    def _print_alert(a: Alert) -> None:
        arrow = "^" if a.condition == "Open==High" else "v"
        print(
            f"\n    [!] {arrow} NEW  {a.instrument} {a.strike}{a.opt_type}"
            f"  exp={a.expiry}  {a.condition}"
            f"  O={a.open:.2f}  H={a.high:.2f}  L={a.low:.2f}  LTP={a.ltp}"
        )

    @staticmethod
    def _print_breakout(a: Alert, ltp: float) -> None:
        level = a.high if a.condition == "Open==High" else a.low
        side  = "HIGH ^" if a.condition == "Open==High" else "LOW v"
        print(
            f"\n    [!] BROKEN {side}  {a.instrument} {a.strike}{a.opt_type}"
            f"  exp={a.expiry}  level={level:.2f}  ltp={ltp:.2f}  -> removed"
        )

    def _save(self) -> None:
        try:
            with open(ALERT_FILE, "w") as f:
                json.dump([asdict(a) for a in self.alerts], f, indent=2)
        except Exception as e:
            print(f"    ! could not save alerts: {e}")

    def _dump_xhr_urls(self, captured: dict) -> None:
        print(f"    XHR URLs:")
        for url in sorted(captured.keys()):
            short = url.split("groww.in")[1][:70] if "groww.in" in url else url[:70]
            print(f"      {short}")

    def _dump_raw_option_keys(self, captured: dict) -> None:
        print(f"    raw keys in captured XHR:")
        for url, data in captured.items():
            if isinstance(data, dict):
                keys = list(data.keys())[:20]
                short = url.split("groww.in")[1][:60] if "groww.in" in url else url[:60]
                print(f"      {short}: {keys}")

# -- Main ---------------------------------------------------------------------
async def main():
    print("\n" + "=" * 72)
    print("  Groww Open==High / Open==Low Alert Scanner")
    print("=" * 72)

    once = "--once" in sys.argv

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)

        session_valid = False
        if STORAGE_FILE.exists():
            print("\n  Session found. Validating...")
            ctx = await _make_context(browser)
            test_page = await ctx.new_page()
            try:
                await test_page.goto("https://groww.in/webstock", wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(3)
                title = await test_page.title()
                if "login" not in title.lower() and "sign" not in title.lower():
                    session_valid = True
                    print(f"  Session valid! (page: {title})")
                else:
                    print(f"  Session expired (page title: {title})")
            except Exception as e:
                print(f"  Session check error: {e}")
                print("  Attempting to use existing session...")
                session_valid = True
            finally:
                await test_page.close()
                await ctx.close()
        else:
            print("\n  No session found.")

        if not session_valid:
            if not await _interactive_login(browser):
                print("  Login failed!")
                await browser.close()
                return

        # Step 2: Run scanner
        scanner = Scanner()

        if once:
            await scanner.sweep(browser)
        else:
            print(f"\n  Starting continuous mode (interval: {SCAN_INTERVAL}s, Ctrl+C to stop)")
            while True:
                await scanner.sweep(browser)
                await asyncio.sleep(SCAN_INTERVAL)

        await browser.close()
        print("\n  Done!")

if __name__ == "__main__":
    asyncio.run(main())
