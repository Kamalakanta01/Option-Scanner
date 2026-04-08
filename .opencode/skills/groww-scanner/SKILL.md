---
name: groww-scanner
description: Groww options scanner - Playwright-based trading alert system for detecting Open==High/Open==Low conditions
license: MIT
compatibility: opencode
metadata:
  audience: developers
  domain: trading
  project: groww-scanner
---

## Project overview

This is a Python application that monitors Groww.in for stock/commodity options trading alerts. It detects when the Open price equals the High or Low price, which can signal trading opportunities.

## Key components

- **main.py**: Single entry point (817 lines). Contains all logic including:
  - `Scanner` class: Core scanning logic
  - `Alert` dataclass: Represents detected alerts
  - Functions for parsing contracts, extracting prices, handling MCX commodities

- **Key instruments scanned**: NIFTY, SENSEX, RELIANCE, SBIN, GOLDM, SILVERM

- **Technologies**: 
  - `playwright` (async_api) for browser automation
  - `asyncio` for concurrent operations
  - JSON for state persistence

## Important conventions

- Uses `async with async_playwright()` context manager
- Creates fresh `BrowserContext` per instrument scan
- Captures XHR responses via `page.on("response")` event listener
- Symbols follow pattern: `{NAME}{DATE_CODE}{STRIKE}{CE/PE}`
- Date codes can be 4, 5, or 6 digits with various formats
- ATM strikes calculated as `round(price/step) * step ± STRIKE_RANGE`

## File locations

- `groww_state.json`: Browser session state (cookies, etc.)
- `alerts.json`: Detected alerts output
- `scanner.log`: Application logs

## Commands

- `python main.py`: Continuous scanning mode
- `python main.py --once`: Single scan
- `python main.py --debug`: Verbose debugging output

## When working on this codebase

1. Understand async/await patterns in Python
2. Note the anti-detection measures in `_make_context()` 
3. Symbol parsing is flexible (see `_parse_sym_flexible`)
4. MCX commodities have special handling in `_scan_mcx()`
5. Use `websearch` to find Playwright updates or best practices
