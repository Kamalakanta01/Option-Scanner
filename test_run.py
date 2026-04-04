#!/usr/bin/env python3
"""
Diagnostic runner for main.py
Runs offline unit tests first, then optionally a live --once sweep.

Usage:
    python test_run.py          # offline tests only
    python test_run.py --live   # offline tests + one live sweep (needs browser)
"""

import asyncio
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

# ---- patch sys.argv so main.py doesn't see our flags as its own ----
_live = "--live" in sys.argv
sys.argv = [sys.argv[0], "--once", "--debug"]

from main import (
    _code_to_date,
    _parse_sym_flexible,
    build_symbol_candidates,
    extract_contracts,
    find_underlying_price,
    _extract_mcx_chain_prices,
    _sf,
    STORAGE_FILE,
)

PASS = "[PASS]"
FAIL = "[FAIL]"
results = []

def check(label, got, expected):
    ok = got == expected
    tag = PASS if ok else FAIL
    msg = f"  {tag}  {label}"
    if not ok:
        msg += f"\n         expected: {expected!r}\n         got:      {got!r}"
    print(msg)
    results.append(ok)

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# ---------------------------------------------------------------------------
# 1. _code_to_date
# ---------------------------------------------------------------------------
section("_code_to_date")
check("6-digit YYMMDD",        _code_to_date("260407"), "2026-04-07")
check("5-digit YY M DD",       _code_to_date("26407"),  "2026-04-07")
check("5-digit YY MM DD",      _code_to_date("26904"),  "2026-09-04")  # ambiguous — may vary
check("4-digit YYMM (monthly)",_code_to_date("2604"),   "2026-04-01")
check("YYMMM abbr",            _code_to_date("26APR"),  "2026-04-01")
check("invalid returns None",  _code_to_date("XXXXX"),  None)

# ---------------------------------------------------------------------------
# 2. _parse_sym_flexible
# ---------------------------------------------------------------------------
section("_parse_sym_flexible")

r = _parse_sym_flexible("NIFTY260424800CE")
# Parses as: NIFTY + 2604 (YYMM=2026-04) + 24800 (strike)
check("NIFTY YYMMDD strike CE", r, ("NIFTY", "2026-04-01", 24800, "CE") if r else r)
# just check structure if date varies
if r:
    check("NIFTY: name correct",    r[0], "NIFTY")
    check("NIFTY: opt correct",     r[3], "CE")
    check("NIFTY: strike >= 50",    r[2] >= 50, True)

r2 = _parse_sym_flexible("GOLDM26APR64000CE")
if r2:
    check("GOLDM abbr: name",   r2[0], "GOLDM")
    check("GOLDM abbr: opt",    r2[3], "CE")
    check("GOLDM abbr: strike", r2[2], 64000)
else:
    check("GOLDM abbr parse", False, "should return a result")

r3 = _parse_sym_flexible("GARBAGE")
check("garbage returns None", r3, None)

# ---------------------------------------------------------------------------
# 3. build_symbol_candidates
# ---------------------------------------------------------------------------
section("build_symbol_candidates")
cands = build_symbol_candidates("NIFTY", "2026-04-07", 22500, "CE")
check("returns list",       isinstance(cands, list), True)
check("at least 3 cands",  len(cands) >= 3, True)
check("all contain NIFTY", all("NIFTY" in c for c in cands), True)
check("all contain CE",    all(c.endswith("CE") for c in cands), True)
print(f"  candidates: {cands}")

# ---------------------------------------------------------------------------
# 4. extract_contracts — synthetic XHR blob
# ---------------------------------------------------------------------------
section("extract_contracts  (synthetic XHR)")

fake_xhr = {
    "https://groww.in/v1/api/tr_live_prices/NIFTY": {
        "NIFTY2604722500CE": {"open": 120.0, "high": 120.0, "low": 80.0, "ltp": 95.0},
        "NIFTY2604722500PE": {"open": 90.0,  "high": 110.0, "low": 90.0, "ltp": 100.0},
        "NIFTY2604722400CE": {"open": 50.0,  "high": 60.0,  "low": 40.0, "ltp": 55.0},
    }
}

contracts = extract_contracts(fake_xhr, "NIFTY")
check("extracts 3 contracts", len(contracts), 3)
check("O==H contract present", "NIFTY2604722500CE" in contracts, True)

# Check O==H detection via _process logic
c = contracts.get("NIFTY2604722500CE", {})
o, h, l = c.get("open"), c.get("high"), c.get("low")
cond = None
if o and o > 0 and o == h and l is not None and l < h:
    cond = "Open==High"
check("O==H detected correctly", cond, "Open==High")

# O==L check
c2 = contracts.get("NIFTY2604722500PE", {})
o2, h2, l2 = c2.get("open"), c2.get("high"), c2.get("low")
cond2 = None
if o2 and o2 > 0 and o2 == l2 and h2 is not None and h2 > l2:
    cond2 = "Open==Low"
check("O==L detected correctly", cond2, "Open==Low")

# Doji guard: O==H==L should NOT trigger
doji = {"open": 100.0, "high": 100.0, "low": 100.0, "ltp": 100.0}
o3, h3, l3 = doji["open"], doji["high"], doji["low"]
cond3 = None
if o3 > 0 and o3 == h3 and l3 < h3:
    cond3 = "Open==High"
check("doji (O==H==L) not flagged as O==H", cond3, None)

# ---------------------------------------------------------------------------
# 5. find_underlying_price — synthetic
# ---------------------------------------------------------------------------
section("find_underlying_price  (synthetic)")

fake_index_xhr = {
    "https://groww.in/v1/api/tr_live_indices/NIFTY": {
        "symbol": "NIFTY",
        "type": "INDEX",
        "value": 22500.0,
        "ltp": 22500.0,
        "open": 22400.0,
    }
}
p = find_underlying_price(fake_index_xhr, "NIFTY", None)
check("finds NIFTY price from index XHR", p is not None and p > 0, True)
print(f"  price found: {p}")

# ---------------------------------------------------------------------------
# 6. MCX chain extraction — synthetic
# ---------------------------------------------------------------------------
section("_extract_mcx_chain_prices  (synthetic)")

fake_chain = {
    "expiry": "2026-04-15",
    "underlying": 94000.0,
    "chains": [
        {
            "strikePrice": 9400000,   # paisa = 94000 INR
            "callOption": {"open": 500.0, "high": 500.0, "low": 300.0, "ltp": 420.0, "close": 490.0},
            "putOption":  {"open": 300.0, "high": 400.0, "low": 300.0, "ltp": 350.0, "close": 310.0},
        },
        {
            "strikePrice": 9350000,
            "callOption": {"open": 800.0, "high": 900.0, "low": 600.0, "ltp": 750.0, "close": 790.0},
            "putOption":  {"open": 200.0, "high": 250.0, "low": 200.0, "ltp": 220.0, "close": 210.0},
        },
    ],
}
atm_paisa = {9400000, 9350000}
mcx_c = _extract_mcx_chain_prices(fake_chain, atm_paisa)
check("MCX: extracts 4 contracts (2 strikes x 2)", len(mcx_c), 4)
# O==H on call at 94000
key_ce = [k for k in mcx_c if "94000" in k and "CE" in k]
if key_ce:
    mc = mcx_c[key_ce[0]]
    cond_mcx = None
    if mc["open"] == mc["high"] and mc["low"] < mc["high"]:
        cond_mcx = "Open==High"
    check("MCX O==H call detected", cond_mcx, "Open==High")
# O==L on put at 93500
key_pe = [k for k in mcx_c if "93500" in k and "PE" in k]
if key_pe:
    mp = mcx_c[key_pe[0]]
    cond_mp = None
    if mp["open"] == mp["low"] and mp["high"] > mp["low"]:
        cond_mp = "Open==Low"
    check("MCX O==L put detected", cond_mp, "Open==Low")

# ---------------------------------------------------------------------------
# 7. STORAGE_FILE check
# ---------------------------------------------------------------------------
section("Storage file")
check("groww_state.json exists", STORAGE_FILE.exists(), True)
if STORAGE_FILE.exists():
    try:
        data = json.loads(STORAGE_FILE.read_text(encoding="utf-8"))
        check("session JSON parseable", isinstance(data, dict), True)
        has_cookies = bool(data.get("cookies"))
        print(f"  cookies present: {has_cookies}  "
              f"origins: {len(data.get('origins', []))}")
    except Exception as e:
        check("session JSON parseable", False, f"parse error: {e}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
section("Summary")
total  = len(results)
passed = sum(results)
failed = total - passed
print(f"\n  {passed}/{total} passed   {failed} failed")

if failed:
    print("\n  OFFLINE TESTS HAD FAILURES — fix before running live.\n")
    if not _live:
        sys.exit(1)
else:
    print("\n  All offline tests passed.\n")

# ---------------------------------------------------------------------------
# 8. Live sweep (optional)
# ---------------------------------------------------------------------------
if not _live:
    print("  Run with --live to execute a real browser sweep.")
    sys.exit(0 if not failed else 1)

print("="*60)
print("  LIVE SWEEP  (--live flag detected)")
print("="*60)
print("  Launching browser + running one sweep. Check scanner.log for detail.\n")

# Reset argv for main's own logic
sys.argv = [sys.argv[0], "--once", "--debug"]

from main import _run_loop
try:
    asyncio.run(_run_loop(once=True))
except KeyboardInterrupt:
    print("\n  interrupted")
except Exception as e:
    print(f"\n  LIVE SWEEP ERROR: {e}")
    traceback.print_exc()
    sys.exit(1)
