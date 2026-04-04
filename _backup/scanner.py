#!/usr/bin/env python3
"""
Open==High / Open==Low Alert Scanner
Monitors NSE options (NIFTY, RELIANCE, SBIN) and MCX options (GOLDM, SILVERM).
Alerts on condition; removes contract from watchlist on breakout.

Usage:
    python scanner.py           # continuous mode (default)
    python scanner.py --once    # single scan and exit
    python scanner.py --probe   # test all API endpoints and exit (debug)
"""

import asyncio
import json
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, BrowserContext, Page

# -- Config ---------------------------------------------------------------------
STORAGE_FILE  = Path("groww_state.json")
ALERT_FILE    = Path("alerts.json")
SCAN_INTERVAL = 30
STRIKE_RANGE  = 12
BASE          = "https://groww.in/v1/api"

INSTRUMENTS = [
    dict(name="NIFTY",    symbol="NIFTY",    step=50,  n_expiries=2, is_mcx=False),
    dict(name="RELIANCE", symbol="RELIANCE", step=10,  n_expiries=1, is_mcx=False),
    dict(name="SBIN",     symbol="SBIN",     step=5,   n_expiries=1, is_mcx=False),
    dict(name="GOLDM",    symbol="GOLDM",    step=100, n_expiries=1, is_mcx=True),
    dict(name="SILVERM",  symbol="SILVERM",  step=500, n_expiries=1, is_mcx=True),
]

# -- Data -----------------------------------------------------------------------
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


# -- API helpers ----------------------------------------------------------------
_auth_failed = False
_verbose     = False   # set True via --probe to show HTTP status on every call

async def api_get(ctx: BrowserContext, url: str, label: str = "") -> Optional[dict]:
    """GET url via browser session. Returns parsed JSON or None."""
    global _auth_failed
    try:
        r = await ctx.request.get(url)
        if _verbose:
            print(f"    HTTP {r.status}  {label or url.split('groww.in')[1][:80]}")
        if r.status == 200:
            try:
                return await r.json()
            except Exception:
                text = await r.text()
                if _verbose:
                    print(f"    body (non-JSON): {text[:200] if text else '(empty)'}")
                return None
        if r.status in (401, 403):
            if not _auth_failed:
                _auth_failed = True
                print("\n  ! AUTH FAILED -- session expired. Run interactive_login.py then restart.\n")
        elif r.status == 429:
            print(f"  ! Rate limited (429) -- backing off 5s")
            await asyncio.sleep(5)
        elif r.status != 404:
            # 404 = contract doesn't exist (normal for far OTM) -- silent
            if _verbose:
                text = await r.text()
                print(f"    body: {text[:200]}")
    except Exception as e:
        if _verbose:
            print(f"    EXCEPTION: {e}")
    return None


async def warmup(ctx: BrowserContext):
    """
    Visit the Groww options page once so the browser context gets all
    session cookies and headers that Groww's API endpoints require.
    Without this, stocks_fo_data and commodities_fo_data return 401/empty.
    """
    print("  ~ warming up session (loading groww.in/options/nifty) ...", end="", flush=True)
    page = await ctx.new_page()
    try:
        await page.goto("https://groww.in/options/nifty", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)   # let JS set cookies
        print("  OK")
    except Exception as e:
        print(f"  FAIL ({e})")
    finally:
        await page.close()


# -- Open price -----------------------------------------------------------------
async def get_open_price(ctx: BrowserContext, inst: dict) -> Optional[float]:
    if inst["is_mcx"]:
        # Try every known MCX future URL pattern
        candidates = [
            (f"{BASE}/commodities_fo_data/v1/tr_live_prices/exchange/MCX/segment/FUT/{inst['symbol']}/latest",   "FUT"),
            (f"{BASE}/commodities_fo_data/v1/tr_live_prices/exchange/MCX/segment/FUTCOM/{inst['symbol']}/latest", "FUTCOM"),
            (f"{BASE}/commodities_data/v1/tr_live_prices/exchange/MCX/segment/FUT/{inst['symbol']}/latest",       "commodities_data/FUT"),
        ]
        for url, lbl in candidates:
            data = await api_get(ctx, url, f"{inst['name']} open ({lbl})")
            if data:
                val = data.get("open") if data.get("open") is not None else data.get("ltp")
                if val and float(val) > 0:
                    return float(val)
        return None

    if inst["symbol"] == "NIFTY":
        url = f"{BASE}/stocks_data/v1/tr_live_indices/exchange/NSE/segment/CASH/NIFTY/latest"
    else:
        url = f"{BASE}/stocks_data/v1/tr_live_prices/exchange/NSE/segment/CASH/{inst['symbol']}/latest"

    data = await api_get(ctx, url, f"{inst['name']} open")
    if data:
        val = data.get("open") if data.get("open") is not None else data.get("ltp")
        if val and float(val) > 0:
            return float(val)
    return None


# -- Expiries -------------------------------------------------------------------
def _parse_expiry_items(items: list, n: int) -> list:
    """Convert raw expiry items -> [{code, date}]. Handles multiple field name variants."""
    out = []
    for item in items:
        raw = (item.get("expiry") or item.get("expiryDate") or
               item.get("expiry_date") or item.get("nearestExpiry") or "")
        if not raw:
            if _verbose:
                print(f"    expiry item with no date field: {item}")
            continue
        date_str = str(raw)[:10]
        try:
            y, m, d = date_str.split("-")
            code = f"{y[2:]}{int(m)}{int(d):02d}"
            out.append({"code": code, "date": date_str})
            if len(out) == n:
                break
        except ValueError:
            if _verbose:
                print(f"    could not parse expiry date: {raw!r}")
    return out


async def get_expiries(ctx: BrowserContext, inst: dict) -> list:
    if inst["is_mcx"]:
        # Try commodity-specific expiry endpoint first
        candidates = [
            f"{BASE}/commodities_fo_data/v1/nearest_expiries?symbol={inst['symbol']}",
            f"{BASE}/commodities_fo_data/v1/nearest_expiries?underlying={inst['symbol']}",
            f"{BASE}/commodities_fo_data/v1/nearest_expiries",
        ]
        for url in candidates:
            data = await api_get(ctx, url, f"{inst['name']} expiries")
            if not data:
                continue
            # Unwrap: could be list at root, or under 'data'
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                continue
            # Filter by symbol if unfiltered endpoint
            sym = inst["symbol"].upper()
            filtered = [
                i for i in items
                if sym in str(i.get("symbol", "")).upper()
                or sym in str(i.get("underlying", "")).upper()
                or sym in str(i.get("name", "")).upper()
            ] or items   # fallback to all if filter empty
            result = _parse_expiry_items(filtered, inst["n_expiries"])
            if result:
                return result
        return []

    if inst["symbol"] == "NIFTY":
        url = f"{BASE}/stocks_fo_data/v1/nearest_expiries?instrumentType=INDEX"
        data = await api_get(ctx, url, f"{inst['name']} expiries")
    else:
        data = None
        for it in ("OPTIONS", "FNO", "EQ"):
            url = f"{BASE}/stocks_fo_data/v1/nearest_expiries?instrument={inst['symbol']}&instrumentType={it}"
            r = await ctx.request.get(url)
            if r.status == 200:
                data = await r.json()
                break
            elif r.status == 500:
                break  # endpoint does not support equity options - try page scrape fallback
        if data is None:
            return []
    if not data:
        return []

    # Response shape variants:
    #   { "data": [ { "expiry": "2026-04-09T..." } ] }
    #   [ { "expiry": "2026-04-09T..." } ]
    #   { "data": { "expiries": [...] } }
    if isinstance(data, list):
        items = data
    elif "data" in data:
        inner = data["data"]
        if isinstance(inner, list):
            items = inner
        elif isinstance(inner, dict):
            items = inner.get("expiries") or inner.get("items") or []
        else:
            items = []
    elif "nearestExpiryDtoList" in data:
        # NIFTY index response: { "nearestExpiryDtoList": [{ "nearestExpiry": "2026-04-07", "symbol": "NIFTY" }] }
        items = data["nearestExpiryDtoList"]
    else:
        items = []

    if _verbose and not items:
        print(f"    expiry response shape: {json.dumps(data)[:300]}")

    return _parse_expiry_items(items, inst["n_expiries"])


# -- Contract OHLC --------------------------------------------------------------
async def get_contract_ohlc(ctx: BrowserContext, symbol: str, is_mcx: bool) -> Optional[dict]:
    if is_mcx:
        url = f"{BASE}/commodities_fo_data/v1/tr_live_prices/exchange/MCX/segment/OPT/{symbol}/latest"
    else:
        url = f"{BASE}/stocks_fo_data/v1/tr_live_prices/exchange/NSE/segment/FNO/{symbol}/latest"

    data = await api_get(ctx, url)
    if not data or "open" not in data:
        return None
    try:
        o   = data.get("open")
        h   = data.get("high")
        l   = data.get("low")
        ltp = data.get("ltp")
        if o is None or h is None or l is None:
            return None
        return {
            "open": float(o), "high": float(h),
            "low":  float(l), "ltp":  float(ltp) if ltp is not None else None
        }
    except (TypeError, ValueError):
        return None


# -- Scanner --------------------------------------------------------------------
class Scanner:
    def __init__(self):
        self.alerts:    list = []
        self.watchlist: dict = {}

    def strikes(self, open_price: float, step: int) -> list:
        atm = round(open_price / step) * step
        return [atm + i * step for i in range(-STRIKE_RANGE, STRIKE_RANGE + 1)]

    def build_symbol(self, inst: dict, exp_code: str, strike: int, opt: str) -> str:
        return f"{inst['symbol']}{exp_code}{strike}{opt}"

    async def sweep(self, ctx: BrowserContext):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'='*70}")
        print(f"  SWEEP  {ts}")
        print(f"{'='*70}")
        for inst in INSTRUMENTS:
            await self._scan_instrument(ctx, inst)
        active = len(self.watchlist)
        broken = sum(1 for a in self.alerts if a.broken)
        print(f"\n  Summary -> watching: {active}  broken: {broken}  total: {len(self.alerts)}")
        self._save()

    async def _scan_instrument(self, ctx: BrowserContext, inst: dict):
        name = inst["name"]
        print(f"\n  > {name}")

        open_price = await get_open_price(ctx, inst)
        if not open_price:
            print(f"    - could not fetch open price")
            return
        print(f"    open={open_price:,.2f}")

        expiries = await get_expiries(ctx, inst)
        if not expiries:
            print(f"    - no expiries found")
            return
        print(f"    expiries: {[e['date'] for e in expiries]}")

        s_list = self.strikes(open_price, inst["step"])
        total  = len(expiries) * len(s_list) * 2
        print(f"    {len(expiries)}exp x {len(s_list)}strikes x 2 = {total} contracts", end="", flush=True)

        new_hits  = 0
        fetched   = 0
        for exp in expiries:
            for strike in s_list:
                for opt in ("CE", "PE"):
                    sym  = self.build_symbol(inst, exp["code"], strike, opt)
                    await asyncio.sleep(0.05)
                    ohlc = await get_contract_ohlc(ctx, sym, inst["is_mcx"])
                    if ohlc is None:
                        continue
                    fetched += 1
                    o, h, l, ltp = ohlc["open"], ohlc["high"], ohlc["low"], ohlc["ltp"]
                    if sym in self.watchlist:
                        self._check_breakout(sym, ltp)
                    else:
                        cond = None
                        if   o == h: cond = "Open==High"
                        elif o == l: cond = "Open==Low"
                        if cond:
                            alert = Alert(sym, name, exp["date"], strike, opt, cond, o, h, l, ltp)
                            self.alerts.append(alert)
                            self.watchlist[sym] = alert
                            new_hits += 1
                            self._print_alert(alert)

        print(f"  ->  fetched {fetched}/{total}  new alerts: {new_hits}")

    def _check_breakout(self, sym: str, ltp: Optional[float]):
        alert = self.watchlist.get(sym)
        if not alert or ltp is None:
            return
        broken = (
            (alert.condition == "Open==High" and ltp > alert.high) or
            (alert.condition == "Open==Low"  and ltp < alert.low)
        )
        if broken:
            alert.broken    = True
            alert.broken_at = datetime.now().isoformat()
            del self.watchlist[sym]
            self._print_breakout(alert, ltp)

    @staticmethod
    def _print_alert(a: Alert):
        arrow = "^" if a.condition == "Open==High" else "v"
        print(f"\n    ! {arrow} NEW  {a.instrument} {a.strike}{a.opt_type}  {a.expiry}"
              f"  {a.condition}  O={a.open:.2f}  H={a.high:.2f}  L={a.low:.2f}  LTP={a.ltp}")

    @staticmethod
    def _print_breakout(a: Alert, ltp: float):
        level = a.high if a.condition == "Open==High" else a.low
        side  = "HIGH ^" if a.condition == "Open==High" else "LOW v"
        print(f"\n    * BROKEN {side}  {a.instrument} {a.strike}{a.opt_type}  {a.expiry}"
              f"  level={level:.2f}  ltp={ltp:.2f}  -> removed")

    def _save(self):
        payload = {
            "updated":  datetime.now().isoformat(),
            "watching": len(self.watchlist),
            "broken":   sum(1 for a in self.alerts if a.broken),
            "alerts":   [asdict(a) for a in self.alerts],
        }
        ALERT_FILE.write_text(json.dumps(payload, indent=2, default=str))

    # -- entry points ----------------------------------------------------------
    async def _run(self, once: bool = False):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx     = await browser.new_context(storage_state=STORAGE_FILE)
            try:
                await warmup(ctx)
                while True:
                    await self.sweep(ctx)
                    if _auth_failed:
                        print("\n  Auth failed -- stopping.")
                        break
                    if once:
                        break
                    print(f"\n  sleeping {SCAN_INTERVAL}s ...  (Ctrl+C to stop)")
                    await asyncio.sleep(SCAN_INTERVAL)
            except KeyboardInterrupt:
                print("\n  Stopped.")
            finally:
                await browser.close()

    async def run_probe(self):
        """Print raw response for every endpoint -- use to debug API issues."""
        global _verbose
        _verbose = True

        def show(label, data):
            print(f"\n  -- {label}")
            if data is None:
                print("    - no response")
                return
            txt = json.dumps(data, indent=4)
            print(txt[:1200] + ("\n    ... (truncated)" if len(txt) > 1200 else ""))

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx     = await browser.new_context(storage_state=STORAGE_FILE)
            await warmup(ctx)

            # Index & cash prices
            show("NIFTY index",    await api_get(ctx, f"{BASE}/stocks_data/v1/tr_live_indices/exchange/NSE/segment/CASH/NIFTY/latest"))
            show("RELIANCE cash",  await api_get(ctx, f"{BASE}/stocks_data/v1/tr_live_prices/exchange/NSE/segment/CASH/RELIANCE/latest"))
            show("SBIN cash",      await api_get(ctx, f"{BASE}/stocks_data/v1/tr_live_prices/exchange/NSE/segment/CASH/SBIN/latest"))

            # NSE expiries
            show("NIFTY expiries",    await api_get(ctx, f"{BASE}/stocks_fo_data/v1/nearest_expiries?instrumentType=INDEX"))
            show("RELIANCE expiries", await api_get(ctx, f"{BASE}/stocks_fo_data/v1/nearest_expiries?instrument=RELIANCE&instrumentType=EQUITY"))
            show("SBIN expiries",     await api_get(ctx, f"{BASE}/stocks_fo_data/v1/nearest_expiries?instrument=SBIN&instrumentType=EQUITY"))

            # MCX prices
            for seg in ("FUT", "FUTCOM"):
                show(f"GOLDM  {seg}", await api_get(ctx, f"{BASE}/commodities_fo_data/v1/tr_live_prices/exchange/MCX/segment/{seg}/GOLDM/latest"))
                show(f"SILVERM {seg}", await api_get(ctx, f"{BASE}/commodities_fo_data/v1/tr_live_prices/exchange/MCX/segment/{seg}/SILVERM/latest"))

            # MCX expiries
            show("MCX expiries (all)",            await api_get(ctx, f"{BASE}/commodities_fo_data/v1/nearest_expiries"))
            show("MCX expiries (GOLDM filter)",   await api_get(ctx, f"{BASE}/commodities_fo_data/v1/nearest_expiries?symbol=GOLDM"))
            show("MCX expiries (SILVERM filter)",  await api_get(ctx, f"{BASE}/commodities_fo_data/v1/nearest_expiries?symbol=SILVERM"))

            # One NIFTY option (dynamic ATM)
            nd = await api_get(ctx, f"{BASE}/stocks_data/v1/tr_live_indices/exchange/NSE/segment/CASH/NIFTY/latest")
            ed = await api_get(ctx, f"{BASE}/stocks_fo_data/v1/nearest_expiries?instrumentType=INDEX")
            if nd and ed and ed.get("data"):
                ltp = nd.get("ltp") or nd.get("close", 24000)
                atm = round(float(ltp) / 50) * 50
                raw = ed["data"][0].get("expiry", "")[:10]
                try:
                    y, m, d = raw.split("-")
                    code = f"{y[2:]}{int(m)}{int(d):02d}"
                    show(f"NIFTY option {code}{atm}CE", await api_get(ctx, f"{BASE}/stocks_fo_data/v1/tr_live_prices/exchange/NSE/segment/FNO/NIFTY{code}{atm}CE/latest"))
                except Exception:
                    pass

            # One MCX option guess
            show("GOLDM option (guess)", await api_get(ctx, f"{BASE}/commodities_fo_data/v1/tr_live_prices/exchange/MCX/segment/OPT/GOLDM2641794000CE/latest"))

            await browser.close()
        print("\n  --- probe complete ---")


# -- Main ----------------------------------------------------------------------
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
        try:
            asyncio.run(scanner._run(once=False))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
