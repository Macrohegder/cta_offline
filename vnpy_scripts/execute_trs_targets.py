from __future__ import annotations

import json
from pathlib import Path

from vnpy.trader.constant import OrderType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "output"
TARGETS_PATHS = [
    OUTPUT_DIR / "targets_latest.json",
    OUTPUT_DIR / "trs_targets_latest.json",
]
ORDER_TYPE = OrderType.LIMIT
PRICE_MODE = "OPP"
DEFAULT_PRICE_ADD_RATE = 0.01


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load_targets() -> dict:
    for path in TARGETS_PATHS:
        p = Path(path)
        if not p.exists():
            continue
        payload = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    return {}


def _get_positions_map(engine) -> dict[str, float]:
    positions = engine.get_all_positions(use_df=False)
    pos_map: dict[str, float] = {}
    for pos in positions:
        vt_symbol = getattr(pos, "vt_symbol", "")
        volume = float(getattr(pos, "volume", 0.0))
        if vt_symbol:
            pos_map[vt_symbol] = volume
    return pos_map


def _get_last_price(engine, vt_symbol: str) -> float:
    try:
        tick = engine.get_tick(vt_symbol, use_df=False)
        if tick:
            last_price = getattr(tick, "last_price", 0.0)
            if last_price:
                return float(last_price)
            ask_price_1 = getattr(tick, "ask_price_1", 0.0)
            bid_price_1 = getattr(tick, "bid_price_1", 0.0)
            if ask_price_1 and bid_price_1:
                return (float(ask_price_1) + float(bid_price_1)) / 2
            if ask_price_1:
                return float(ask_price_1)
            if bid_price_1:
                return float(bid_price_1)
            pre_close = getattr(tick, "pre_close", 0.0)
            if pre_close:
                return float(pre_close)
    except Exception:
        pass
    return 0.0


def _calc_price(ref_price: float, delta: float, add_rate: float) -> float:
    if ref_price <= 0:
        return 0.0
    if PRICE_MODE == "MID":
        return ref_price
    if delta > 0:
        return ref_price * (1.0 + add_rate)
    if delta < 0:
        return ref_price * (1.0 - add_rate)
    return ref_price


def run(engine) -> None:
    payload = _load_targets()
    records = payload.get("portfolio_records")
    if not isinstance(records, list) or not records:
        engine.write_log("targets 文件 portfolio_records 缺失或为空")
        return

    vt_symbols: list[str] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        vt_symbol = str(rec.get("dominant_vt_symbol") or rec.get("configured_vt_symbol") or "").strip()
        if vt_symbol:
            vt_symbols.append(vt_symbol)
    if vt_symbols:
        try:
            engine.subscribe(vt_symbols)
        except Exception:
            pass

    positions = _get_positions_map(engine)

    for rec in records:
        if not isinstance(rec, dict):
            continue

        vt_symbol = str(rec.get("dominant_vt_symbol") or rec.get("configured_vt_symbol") or "").strip()
        if not vt_symbol:
            continue

        target = _safe_int(rec.get("target", 0), 0)
        status = str(rec.get("status", "")).strip()
        if status and status != "ok":
            engine.write_log(f"{vt_symbol} status={status} 跳过")
            continue

        current = _safe_float(positions.get(vt_symbol, 0.0), 0.0)
        delta = float(target) - float(current)
        if abs(delta) < 1e-7:
            engine.write_log(f"{vt_symbol} 当前持仓={current} 目标={target} 无需调整")
            continue

        ref_price = _get_last_price(engine, vt_symbol)
        add_rate = _safe_float(rec.get("price_add_rate", DEFAULT_PRICE_ADD_RATE), DEFAULT_PRICE_ADD_RATE)
        price = _calc_price(ref_price, delta, add_rate)
        if price <= 0:
            engine.write_log(f"{vt_symbol} 无可用行情价格，跳过下单（delta={delta}）")
            continue

        if delta > 0:
            if current < 0:
                cover_volume = min(delta, abs(current))
                engine.write_log(f"{vt_symbol} cover {cover_volume} @ {price}")
                engine.cover(vt_symbol, price, cover_volume, order_type=ORDER_TYPE)
                delta -= cover_volume
            if delta > 0:
                engine.write_log(f"{vt_symbol} buy {delta} @ {price}")
                engine.buy(vt_symbol, price, delta, order_type=ORDER_TYPE)
        else:
            if current > 0:
                sell_volume = min(abs(delta), current)
                engine.write_log(f"{vt_symbol} sell {sell_volume} @ {price}")
                engine.sell(vt_symbol, price, sell_volume, order_type=ORDER_TYPE)
                delta += sell_volume
            if delta < 0:
                engine.write_log(f"{vt_symbol} short {abs(delta)} @ {price}")
                engine.short(vt_symbol, price, abs(delta), order_type=ORDER_TYPE)
