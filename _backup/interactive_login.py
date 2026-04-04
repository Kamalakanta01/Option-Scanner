#!/usr/bin/env python3
"""
Open==High / Open==Low Alert Scanner -- Parallel, Stealth, No NetworkIdle
"""

import asyncio
import json
import re
import sys
import random
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page

# -- Config --------------------------------------------------------------------
STORAGE_FILE = Path("groww_state.json")
ALERT_FILE = Path("alerts.json")
SCAN_INTERVAL = 90
STRIKE_RANGE = 12
MAX_CONCURRENT = 2

INSTRUMENTS = [
    dict(name="NIFTY",   symbol="NIFTY",   step=50,  n_expiries=2, is_mcx=False, url="https://groww.in/options/nifty"),
    dict(name="RELIANCE",symbol="RELIANCE",step=10,  n_expiries=1, is_mcx=False, url="https://groww.in/options/reliance-industries-ltd"),
    dict(name="SBIN",    symbol="SBIN",    step=5,   n_expiries=1, is_mcx=False, url="https://groww.in/options/state-bank-of-india"),
    dict(name="GOLDM",   symbol="GOLDM",   step=100, n_expiries=1, is_mcx=True,  url="https://groww.in/commodities/futures/mcx_goldm/mcx_goldm03apr26fut"),
    dict(name="SILVERM", symbol="SILVERM", step=500, n_expiries=1, is_mcx=True,  url="https://groww.in/commodities/futures/mcx_silverm/mcx_silverm30apr26fut"),
]

CAPTURE_PATTERNS = (
    "tr_live_prices", "tr_live_indices", "option_chain",
    "latest_prices_batch", "nearest_expiries", "commodity_fo"
)

_OPT_SYM_RE = re.compile(
    r'^([A-Z]{2,12})(\d{4,8}|[0-9]{2}[A-Z]{3})(\d{2,6})(CE|PE)$|'
    r'^([A-Z]{2,12})(\d{4,8}|[0-9]{2}[A-Z]{3})(CE|PE)(\d{2,6})$'
)

@dataclass
class Alert:
    symbol: str
    instrument: str
    expiry: str
    strike: int
    opt_type: str
    condition: str
    open: float
    high: float
    low: float
    ltp: Optional[float] = None
    seen_at: str = field(default_factory=lambda: datetime.now().isoformat())
    broken: bool = False
    broken_at: Optional[str] = None

# -- Helper: random delay ------------------------------------------------------
async def human_delay(min_sec=0.5, max_sec=1.5):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

# -- XHR capture without networkidle -----------------------------------------
async def navigate_and_capture(page: Page, url: str, settle: float = 5.0) -> dict:
    """Navigate, wait for DOM, then sleep for `settle` seconds collecting XHR."""
    captured = {}
    async def on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                captured[resp.url] = await resp.json()
        except Exception:
            pass
    page.on("response", on_response)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        print(f"    ! goto error: {e}")
    await asyncio.sleep(settle)   # let XHRs arrive
    page.remove_listener("response", on_response)
    return captured

async def capture_after_expiry_click(page: Page, expiry_date: str) -> dict:
    new_xhr = {}
    async def on_response(resp):
        try:
            if resp.status == 200 and any(p in resp.url for p in CAPTURE_PATTERNS):
                new_xhr[resp.url] = await resp.json()
        except Exception:
            pass
    page.on("response", on_response)
    await _click_expiry_in_dom(page, expiry_date)
    await human_delay(4, 6)
    page.remove_listener("response", on_response)
    return new_xhr

async def _click_expiry_in_dom(page: Page, expiry_date: str) -> bool:
    try:
        dt = datetime.strptime(expiry_date, "%Y-%m-%d")
        human = [dt.strftime("%-d %b %Y"), dt.strftime("%d %b %Y"),
                 dt.strftime("%-d %B %Y"), dt.strftime("%d %B %Y")]
    except Exception:
        human = []
    result = await page.evaluate(
        """([isoDate, humanVariants]) => {
            for (const sel of document.querySelectorAll('select')) {
                for (const opt of sel.options) {
                    const v = (opt.value || opt.textContent || '').trim();
                    if (v === isoDate || v.startsWith(isoDate) ||
                            humanVariants.some(h => v.includes(h))) {
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                        return 'select:' + opt.value;
                    }
                }
            }
            for (const el of document.querySelectorAll('button,[role="tab"],[role="option"],li,span,div')) {
                const text = (el.getAttribute('data-value') || el.getAttribute('data-expiry') ||
                              el.getAttribute('data-date')  || el.textContent || '').trim();
                if (text === isoDate || text.startsWith(isoDate) ||
                        humanVariants.some(h => text === h || text.includes(h))) {
                    el.click();
                    return 'click:' + text;
                }
            }
            return null;
        }""",
        [expiry_date, human],
    )
    return result is not None

# -- Expiry extraction from contracts (most reliable) -------------------------
def extract_expiries_from_contracts(contracts: dict, instrument_name: str) -> list:
    """Extract unique expiry dates from contract symbols using parse_symbol_info."""
    expiries = set()
    for sym in contracts.keys():
        info = parse_symbol_info(sym)
        if info and info[0] == instrument_name:
            expiries.add(info[1])   # expiry YYYY-MM-DD
    return sorted(list(expiries))

# -- Contract OHLC extraction --------------------------------------------------
def _sf(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None

def _is_ohlc(d: dict) -> bool:
    return {"open", "high", "low", "ltp"}.issubset(d.keys())

def extract_contracts(captured: dict) -> dict:
    contracts = {}
    def _store(sym: str, obj: dict):
        o = _sf(obj.get("open"))
        h = _sf(obj.get("high"))
        l = _sf(obj.get("low"))
        ltp = _sf(obj.get("ltp"))
        if o is None or h is None or l is None:
            return
        contracts[sym.upper()] = {"open": o, "high": h, "low": l, "ltp": ltp}
    def _walk(node, parent_key=""):
        if isinstance(node, dict):
            opt_keys = [k for k in node if _OPT_SYM_RE.match(str(k))]
            if len(opt_keys) >= 2:
                for sym_key in opt_keys:
                    val = node[sym_key]
                    if isinstance(val, dict) and _is_ohlc(val):
                        _store(sym_key, val)
                for k, v in node.items():
                    if k not in opt_keys:
                        _walk(v, k)
                return
            if _is_ohlc(node):
                raw = (node.get("symbol") or node.get("tradingSymbol") or
                       node.get("instrumentName") or parent_key)
                if raw and _OPT_SYM_RE.match(str(raw).upper()):
                    _store(str(raw), node)
            if "strikePrice" in node or "strike_price" in node:
                strike = node.get("strikePrice") or node.get("strike_price", "")
                for side_key, opt in (("callOptions","CE"),("putOptions","PE"),("ce","CE"),("pe","PE")):
                    side = node.get(side_key)
                    if isinstance(side, dict) and _is_ohlc(side):
                        sym = side.get("symbol") or side.get("tradingSymbol") or f"__CHAIN_{strike}{opt}"
                        _store(str(sym), side)
            for k, v in node.items():
                _walk(v, k)
        elif isinstance(node, list):
            for item in node:
                _walk(item, parent_key)
    for data in captured.values():
        _walk(data)
    return contracts

def find_underlying_price(captured: dict, instrument_name: str) -> Optional[float]:
    name_upper = instrument_name.upper()
    def _try(node: dict) -> Optional[float]:
        sym = str(node.get("symbol", "")).upper()
        typ = str(node.get("type", "")).upper()
        if name_upper not in sym:
            return None
        if "INDEX" in typ:
            return _sf(node.get("open") or node.get("value") or node.get("close"))
        return _sf(node.get("open") or node.get("ltp"))
    def _walk(node):
        if isinstance(node, dict):
            v = _try(node)
            if v and v > 0:
                return v
            for child in node.values():
                v = _walk(child)
                if v:
                    return v
        elif isinstance(node, list):
            for item in node:
                v = _walk(item)
                if v:
                    return v
        return None
    for data in captured.values():
        v = _walk(data)
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
            }"""
        )
        return float(v) if v else None
    except Exception:
        return None

# -- Symbol parsing ----------------------------------------------------------
def parse_symbol_info(sym: str):
    m = re.match(r'^([A-Z]{2,12})(\d{4,8}|[0-9]{2}[A-Z]{3})(\d{2,6})(CE|PE)$', sym)
    if m:
        name, code, strike_s, opt = m.groups()
        expiry = _code_to_date(code)
        if expiry:
            return (name, expiry, int(strike_s), opt)
    m = re.match(r'^([A-Z]{2,12})(\d{4,8}|[0-9]{2}[A-Z]{3})(CE|PE)(\d{2,6})$', sym)
    if m:
        name, code, opt, strike_s = m.groups()
        expiry = _code_to_date(code)
        if expiry:
            return (name, expiry, int(strike_s), opt)
    return None

def _code_to_date(code: str) -> Optional[str]:
    if code.isdigit():
        if len(code) == 6:
            try:
                return datetime.strptime(f"20{code}", "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        if len(code) == 5:
            yy = code[:2]
            for split in (1,2):
                try:
                    mm = int(code[2:2+split])
                    dd = int(code[2+split:])
                    return datetime.strptime(f"20{yy}{mm:02d}{dd:02d}", "%Y%m%d").strftime("%Y-%m-%d")
                except (ValueError, IndexError):
                    pass
    m = re.match(r"(\d{2})([A-Z]{3})", code)
    if m:
        yy, mon = m.groups()
        try:
            dt = datetime.strptime(f"20{yy} {mon} 01", "%Y %b %d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def build_symbol_candidates(name: str, expiry: str, strike: int, opt: str) -> list:
    try:
        y, m, d = expiry.split("-")
    except ValueError:
        return []
    yy = y[2:]
    mm_num = f"{int(m):02d}"
    m1 = str(int(m))
    dd = f"{int(d):02d}"
    numeric_codes = [f"{yy}{m1}{dd}", f"{yy}{mm_num}{dd}"]
    month_abbr = datetime.strptime(expiry, "%Y-%m-%d").strftime("%y%b").upper()
    all_codes = numeric_codes + [month_abbr]
    candidates = []
    for code in all_codes:
        candidates.extend([
            f"{name}{code}{strike}{opt}",
            f"{name}{code}{opt}{strike}",
            f"{name}{code}{strike:05d}{opt}",
            f"{name}{code}{opt}{strike:05d}",
        ])
    return candidates

# -- MCX direct page scraping -------------------------------------------------
async def fetch_mcx_option_ohlc(page: Page, option_url: str) -> Optional[dict]:
    try:
        captured = await navigate_and_capture(page, option_url, settle=4.0)
        for data in captured.values():
            if isinstance(data, dict) and _is_ohlc(data):
                return {"open": _sf(data["open"]), "high": _sf(data["high"]),
                        "low": _sf(data["low"]), "ltp": _sf(data["ltp"])}
            for v in data.values():
                if isinstance(v, dict) and _is_ohlc(v):
                    return {"open": _sf(v["open"]), "high": _sf(v["high"]),
                            "low": _sf(v["low"]), "ltp": _sf(v["ltp"])}
        return None
    except Exception:
        return None

def build_mcx_option_url(base_symbol: str, expiry_date: str, strike: int, opt_type: str) -> str:
    dt = datetime.strptime(expiry_date, "%Y-%m-%d")
    exp_code = dt.strftime("%d%b%y").lower()
    symbol_lower = base_symbol.lower()
    return f"https://groww.in/commodities/options/mcx_{symbol_lower}/mcx_{symbol_lower}{exp_code}{strike}{opt_type.lower()}"

async def get_mcx_future_price(page: Page, future_url: str) -> tuple[Optional[float], Optional[str]]:
    captured = await navigate_and_capture(page, future_url, settle=4.0)
    expiry = None
    for data in captured.values():
        if isinstance(data, dict):
            if "expiry" in data:
                expiry = _normalise_date(str(data["expiry"]))
            if "open" in data and data["open"]:
                return _sf(data["open"]), expiry
            if "ltp" in data and data["ltp"]:
                return _sf(data["ltp"]), expiry
            for v in data.values():
                if isinstance(v, dict):
                    if "expiry" in v:
                        expiry = _normalise_date(str(v["expiry"]))
                    if "open" in v and v["open"]:
                        return _sf(v["open"]), expiry
                    if "ltp" in v and v["ltp"]:
                        return _sf(v["ltp"]), expiry
    return None, expiry

def _normalise_date(raw: str) -> Optional[str]:
    raw = raw.strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    m = re.match(r"(\d{1,2})[-/](\d{2})[-/](\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    for fmt in ("%d %b %Y", "%d %B %Y", "%-d %b %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.match(r"(\d{2})([A-Z]{3})", raw)
    if m:
        yy, mon = m.groups()
        try:
            dt = datetime.strptime(f"20{yy} {mon} 01", "%Y %b %d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

# -- Scanner (parallel) -------------------------------------------------------
class Scanner:
    def __init__(self):
        self.alerts = []
        self.watchlist = {}
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    def atm_strikes(self, price: float, step: int) -> set:
        if STRIKE_RANGE == 0:
            return set()
        atm = round(price / step) * step
        return {atm + i * step for i in range(-STRIKE_RANGE, STRIKE_RANGE + 1)}

    async def sweep(self, ctx: BrowserContext):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*72}\n  SWEEP  {ts}\n{'='*72}")
        tasks = [self._scan_instrument_wrapper(ctx, inst) for inst in INSTRUMENTS]
        await asyncio.gather(*tasks)
        active = len(self.watchlist)
        broken = sum(1 for a in self.alerts if a.broken)
        print(f"\n  Summary -> watching: {active}  broken: {broken}  total: {len(self.alerts)}")
        self._save()

    async def _scan_instrument_wrapper(self, ctx: BrowserContext, inst: dict):
        async with self.semaphore:
            await self._scan_instrument(ctx, inst)

    async def _scan_instrument(self, ctx: BrowserContext, inst: dict):
        name = inst["name"]
        is_mcx = inst.get("is_mcx", False)
        print(f"\n  > {name}")
        page = await ctx.new_page()
        try:
            if is_mcx:
                await self._scan_mcx(page, inst)
            else:
                await self._scan_nse(page, inst)
        except Exception as e:
            print(f"    - error: {e}")
            if "--debug" in sys.argv:
                import traceback; traceback.print_exc()
        finally:
            await page.close()

    async def _scan_nse(self, page: Page, inst: dict):
        name = inst["name"]
        url = inst["url"]
        print(f"    loading page ...", end="", flush=True)
        captured = await navigate_and_capture(page, url, settle=6.0)
        print(f"  {len(captured)} XHR captured")
        price = find_underlying_price(captured, name) or await scrape_price_from_dom(page)
        if not price:
            print("    - could not find underlying price")
            return
        print(f"    price = {price:,.2f}")

        # Extract contracts from initial load
        all_contracts = extract_contracts(captured)

        # Switch to other expiries (for NIFTY which has multiple)
        # But first, get expiries from contracts (most reliable)
        expiries = extract_expiries_from_contracts(all_contracts, name)
        if not expiries:
            print("    - no expiry dates found from contracts")
            return
        expiries = sorted(expiries)[:inst["n_expiries"]]
        print(f"    expiries: {expiries}")

        # For additional expiries (e.g., NIFTY's next weekly), click the expiry tab
        for exp in expiries[1:]:
            print(f"    -> switching to {exp} ...", end="", flush=True)
            new_xhr = await capture_after_expiry_click(page, exp)
            new_c = extract_contracts(new_xhr)
            all_contracts.update(new_c)
            print(f"  +{len(new_c)} contracts")
        print(f"    contracts in memory: {len(all_contracts)}")

        allowed = self.atm_strikes(price, inst["step"])
        new_hits = 0
        checked = 0
        for expiry in expiries:
            for strike in sorted(allowed):
                for opt in ("CE", "PE"):
                    ohlc = None
                    key = None
                    for cand in build_symbol_candidates(name, expiry, strike, opt):
                        if cand in all_contracts:
                            ohlc = all_contracts[cand]
                            key = cand
                            break
                    if ohlc is None or key is None:
                        continue
                    checked += 1
                    new_hits += self._process(key, name, expiry, strike, opt, ohlc)
        print(f"    checked: {checked}  new alerts: {new_hits}")

    async def _scan_mcx(self, page: Page, inst: dict):
        name = inst["name"]
        future_url = inst["url"]
        print(f"    fetching future price ...", end="", flush=True)
        future_price, expiry = await get_mcx_future_price(page, future_url)
        if not future_price:
            print("  - failed")
            return
        if not expiry:
            # fallback: next monthly expiry (last Thursday of current month)
            today = datetime.now()
            if today.day > 25:
                next_month = today.replace(day=28) + timedelta(days=4)
                expiry = (next_month - timedelta(days=next_month.weekday() + 1 if next_month.weekday() != 3 else 0)).strftime("%Y-%m-%d")
            else:
                last_day = today.replace(day=28) + timedelta(days=4)
                expiry = (last_day - timedelta(days=last_day.weekday() + 1 if last_day.weekday() != 3 else 0)).strftime("%Y-%m-%d")
        print(f"  price = {future_price:,.2f}, expiry = {expiry}")

        strikes = self.atm_strikes(future_price, inst["step"])
        print(f"    checking {len(strikes)} strikes x 2 = {len(strikes)*2} contracts")
        new_hits = 0
        checked = 0
        for strike in sorted(strikes):
            for opt in ("CE", "PE"):
                option_url = build_mcx_option_url(name.lower(), expiry, strike, opt)
                ohlc = await fetch_mcx_option_ohlc(page, option_url)
                if ohlc is None:
                    continue
                checked += 1
                sym = f"MCX_{name}_{expiry}_{strike}_{opt}"
                new_hits += self._process(sym, name, expiry, strike, opt, ohlc)
                await human_delay(0.3, 0.8)
        print(f"    checked: {checked}  new alerts: {new_hits}")

    def _process(self, key: str, name: str, expiry: str,
                 strike: int, opt: str, ohlc: dict) -> int:
        o, h, l, ltp = ohlc["open"], ohlc["high"], ohlc["low"], ohlc["ltp"]
        if key in self.watchlist:
            self._check_breakout(key, ltp)
            return 0
        cond = None
        if o > 0 and o == h:
            cond = "Open==High"
        elif o > 0 and o == l:
            cond = "Open==Low"
        if not cond:
            return 0
        alert = Alert(key, name, expiry, strike, opt, cond, o, h, l, ltp)
        self.alerts.append(alert)
        self.watchlist[key] = alert
        self._print_alert(alert)
        return 1

    def _check_breakout(self, key: str, ltp: Optional[float]):
        alert = self.watchlist.get(key)
        if not alert or ltp is None:
            return
        broken = ((alert.condition == "Open==High" and ltp > alert.high) or
                  (alert.condition == "Open==Low" and ltp < alert.low))
        if broken:
            alert.broken = True
            alert.broken_at = datetime.now().isoformat()
            del self.watchlist[key]
            self._print_breakout(alert, ltp)

    @staticmethod
    def _print_alert(a: Alert):
        arrow = "^" if a.condition == "Open==High" else "v"
        print(f"\n    [!] {arrow} NEW  {a.instrument} {a.strike}{a.opt_type}  {a.expiry}"
              f"  {a.condition}  O={a.open:.2f}  H={a.high:.2f}  L={a.low:.2f}  LTP={a.ltp}")

    @staticmethod
    def _print_breakout(a: Alert, ltp: float):
        level = a.high if a.condition == "Open==High" else a.low
        side = "HIGH ^" if a.condition == "Open==High" else "LOW v"
        print(f"\n    [!] BROKEN {side}  {a.instrument} {a.strike}{a.opt_type}  {a.expiry}"
              f"  level={level:.2f}  ltp={ltp:.2f}  -> removed")

    def _save(self):
        ALERT_FILE.write_text(json.dumps({
            "updated": datetime.now().isoformat(),
            "watching": len(self.watchlist),
            "broken": sum(1 for a in self.alerts if a.broken),
            "alerts": [asdict(a) for a in self.alerts],
        }, indent=2, default=str))

    # -- run modes ------------------------------------------------------------
    async def _run(self, once: bool = False):
        if not STORAGE_FILE.exists():
            print(f"ERROR: {STORAGE_FILE} not found. Run interactive_login.py first.")
            sys.exit(1)
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                ]
            )
            ctx = await browser.new_context(
                storage_state=str(STORAGE_FILE),
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='Asia/Kolkata',
                extra_http_headers={'Accept-Language': 'en-US,en;q=0.9'}
            )
            await ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            try:
                while True:
                    await self.sweep(ctx)
                    if once:
                        break
                    print(f"\n  sleeping {SCAN_INTERVAL}s ...  (Ctrl+C to stop)")
                    await asyncio.sleep(SCAN_INTERVAL)
            except KeyboardInterrupt:
                print("\n  Stopped.")
            finally:
                await ctx.close()
                await browser.close()

    async def run_probe(self):
        print("Probe mode not needed. Use normal run with --once to test.")
        sys.exit(0)

def main():
    if not STORAGE_FILE.exists():
        print(f"ERROR: {STORAGE_FILE} not found. Run interactive_login.py first.")
        sys.exit(1)
    scanner = Scanner()
    if "--probe" in sys.argv:
        asyncio.run(scanner.run_probe())
    elif "--once" in sys.argv:
        asyncio.run(scanner._run(once=True))
    else:
        asyncio.run(scanner._run(once=False))

if __name__ == "__main__":
    main()