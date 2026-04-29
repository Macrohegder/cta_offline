from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class VtSymbolInfo:
    vt_symbol: str
    symbol: str
    exchange: str
    product: str


def parse_vt_symbol(vt_symbol: str) -> VtSymbolInfo:
    symbol, exchange = vt_symbol.split(".", 1)
    product = "".join(ch for ch in symbol if not ch.isdigit()).upper()
    return VtSymbolInfo(vt_symbol=vt_symbol, symbol=symbol, exchange=exchange, product=product)


def dominant_to_vt_symbol(code: str, exchange: str) -> str:
    ex = exchange.upper()
    c = code
    if ex in {"CFFEX", "CZCE"}:
        c = c.upper()
        if ex == "CZCE":
            alpha = "".join(filter(str.isalpha, c))
            digits = "".join(filter(str.isdigit, c))
            c = alpha + digits[-3:]
    else:
        c = c.lower()
    return f"{c}.{ex}"


def guess_next_trading_date(rqdatac, base_date: date) -> date:
    try:
        get_trading_dates = getattr(rqdatac, "get_trading_dates", None)
        if callable(get_trading_dates):
            dates = list(get_trading_dates(base_date, base_date.replace(year=base_date.year + 1)))
            after = [d for d in dates if d > base_date]
            if after:
                return after[0]
    except Exception:
        pass

    d = base_date
    while True:
        d = d.fromordinal(d.toordinal() + 1)
        if d.weekday() < 5:
            return d


def fetch_dominant_vt_symbol(rqdatac, product: str, exchange: str, trading_date: date) -> str:
    p = product.upper()
    series = rqdatac.futures.get_dominant(p, trading_date)
    if series is None or len(series) == 0:
        return ""
    code = str(series.values[0])
    return dominant_to_vt_symbol(code, exchange)


def is_czce(vt_symbol: str) -> bool:
    return bool(re.search(r"\.CZCE$", vt_symbol, re.IGNORECASE))

