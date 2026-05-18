from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trs_offline.io_utils import read_json, write_csv, write_json_atomic
from trs_offline.paths import get_default_paths
from trs_offline.rqdatac_client import init_rqdatac
from trs_offline.trs_logic import (
    calculate_atr_wilder,
    compute_pullback_mr_target_from_history,
    compute_target_from_history,
)
from trs_offline.vnpy_symbol import fetch_dominant_vt_symbol, guess_next_trading_date, parse_vt_symbol


CZCE_CONTRACT_MULTIPLIER_FALLBACK: dict[str, float] = {
    "AP": 10.0,
    "CF": 5.0,
    "CJ": 5.0,
    "CY": 5.0,
    "FG": 20.0,
    "PK": 5.0,
    "UR": 20.0,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    defaults = get_default_paths()
    p.add_argument("--cta-setting", default=str(defaults.cta_strategy_setting))
    p.add_argument("--output-dir", default=str(defaults.output_dir))
    p.add_argument("--suffix", default="888")
    p.add_argument("--end-date", default="")
    p.add_argument("--exec-date", default="")
    p.add_argument("--history-buffer", type=int, default=60)
    p.add_argument("--include", nargs="*", default=[])
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def normalize_include(values: list[str]) -> set[str]:
    out: set[str] = set()
    for item in values:
        for part in item.replace("，", ",").split(","):
            v = part.strip().upper()
            if v:
                out.add(v)
    return out


def parse_day(value: str) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def get_contract_multiplier(rqdatac, order_book_id: str, exchange: str = "") -> tuple[float, bool]:
    candidates = [
        "contract_multiplier",
        "contract_size",
        "multiplier",
        "contract_unit",
        "trading_unit",
        "size",
    ]

    keys_to_try: list[str] = []
    oid = str(order_book_id or "").strip()
    ex = str(exchange or "").strip().upper()
    if oid:
        keys_to_try.extend([oid, oid.upper()])
        if ex and "." not in oid:
            keys_to_try.extend([f"{oid.upper()}.{ex}", f"{oid}.{ex}"])

    seen: set[str] = set()
    for instrument_key in keys_to_try:
        k = instrument_key.strip()
        if not k or k in seen:
            continue
        seen.add(k)

        try:
            inst = rqdatac.instruments(k)
        except Exception:
            continue

        for key in candidates:
            try:
                val = getattr(inst, key, None)
            except Exception:
                val = None
            if isinstance(val, (int, float)) and float(val) > 0:
                return float(val), True

        for method in ["to_dict", "as_dict", "dict"]:
            fn = getattr(inst, method, None)
            if not callable(fn):
                continue
            try:
                payload = fn()
            except Exception:
                payload = None
            if isinstance(payload, dict):
                for key in candidates:
                    val = payload.get(key)
                    if isinstance(val, (int, float)) and float(val) > 0:
                        return float(val), True

    return 0.0, False


def compute_atr_position_size(
    atr_value: float,
    contract_multiplier: float,
    *,
    base_capital: float,
    risk_rate: float,
    min_size: int,
) -> int:
    if atr_value <= 0 or contract_multiplier <= 0:
        return int(min_size)
    risk_capital = float(base_capital) * float(risk_rate)
    raw = risk_capital / (float(atr_value) * float(contract_multiplier))
    size = int(raw + 0.5)
    return max(int(min_size), int(size))


def fetch_daily_bars(
    rqdatac, order_book_id: str, start_date: date, end_date: date
) -> tuple[list[float], list[float], list[float], str]:
    data = rqdatac.get_price(
        order_book_ids=order_book_id,
        start_date=start_date,
        end_date=end_date,
        frequency="1d",
        fields=["high", "low", "close"],
    )
    if data is None or len(data) == 0:
        return [], [], [], ""

    if hasattr(data, "columns") and all(col in data.columns for col in ["high", "low", "close"]):
        df = data[["high", "low", "close"]]
    else:
        df = data

    df = df.dropna()
    if len(df) == 0:
        return [], [], [], ""

    highs = [float(v) for v in df["high"].tolist()]
    lows = [float(v) for v in df["low"].tolist()]
    closes = [float(v) for v in df["close"].tolist()]
    last_index = df.index[-1]
    index_value = last_index[-1] if isinstance(last_index, tuple) and last_index else last_index
    trade_date = index_value.date().isoformat() if hasattr(index_value, "date") else str(index_value)[:10]
    return highs, lows, closes, trade_date


def main() -> int:
    args = parse_args()
    include = normalize_include(args.include)

    cta_setting_path = Path(args.cta_setting)
    output_dir = Path(args.output_dir)

    settings: dict[str, Any] = read_json(cta_setting_path)

    rqdatac = init_rqdatac()

    requested_end_date = parse_day(args.end_date)
    requested_exec_date = parse_day(args.exec_date) if args.exec_date else None

    rows: list[dict[str, Any]] = []
    rows_trs: list[dict[str, Any]] = []
    rows_pbmr: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []

    for strategy_name, item in settings.items():
        if not isinstance(item, dict):
            continue
        class_name = str(item.get("class_name") or "").strip()
        if class_name not in {"TripleRsiLongShortStrategy", "PullbackMrStrategy"}:
            continue

        vt_symbol = str(item.get("vt_symbol", "")).strip()
        if not vt_symbol:
            continue

        info = parse_vt_symbol(vt_symbol)
        if include and info.product.upper() not in include:
            continue

        stg_setting: dict[str, Any] = item.get("setting") if isinstance(item.get("setting"), dict) else {}

        order_book_id = f"{info.product.upper()}{args.suffix}"

        max_needed = 0
        if class_name == "TripleRsiLongShortStrategy":
            rsi_period = int(stg_setting.get("rsi_period", 5))
            ma_period = int(stg_setting.get("ma_period", 200))
            max_needed = max(ma_period, rsi_period + 10) + args.history_buffer
        else:
            sma_200_period = int(stg_setting.get("sma_200_period", 200))
            sma_20_period = int(stg_setting.get("sma_20_period", 20))
            rsi_period = int(stg_setting.get("rsi_period", 5))
            max_needed = max(sma_200_period, sma_20_period, rsi_period + 10) + args.history_buffer

        lookback_days = max(max_needed * 3, max_needed + 90)
        start_date = requested_end_date - timedelta(days=lookback_days)

        highs, lows, closes, trade_date_str = fetch_daily_bars(rqdatac, order_book_id, start_date, requested_end_date)
        signal_date = date.fromisoformat(trade_date_str) if trade_date_str else None

        effective_allow_long = ""
        effective_allow_short = ""
        snapshot: dict[str, Any] = {}

        fixed_size = int(stg_setting.get("fixed_size", 1))
        atr_period = int(stg_setting.get("atr_period", 14))
        sizing_base_capital = float(stg_setting.get("sizing_base_capital", 10_000_000))
        sizing_risk_rate = float(stg_setting.get("sizing_risk_rate", 0.0005))
        sizing_min_size = int(stg_setting.get("sizing_min_size", 1))
        price_add_rate = float(stg_setting.get("price_add_rate", 0.01))

        atr_value = calculate_atr_wilder(highs, lows, closes, atr_period) if highs and lows and closes else 0.0

        exec_date = requested_exec_date
        if exec_date is None and signal_date is not None:
            exec_date = guess_next_trading_date(rqdatac, signal_date)

        dominant_vt_symbol = ""
        for d in [exec_date, signal_date]:
            if d is None:
                continue
            try:
                dominant_vt_symbol = fetch_dominant_vt_symbol(rqdatac, info.product, info.exchange, d)
            except Exception:
                dominant_vt_symbol = ""
            if dominant_vt_symbol:
                break

        dominant_order_book_id = dominant_vt_symbol.split(".", 1)[0].upper() if dominant_vt_symbol else ""
        dominant_exchange = (
            dominant_vt_symbol.split(".", 1)[1].upper() if dominant_vt_symbol and "." in dominant_vt_symbol else str(info.exchange).upper()
        )
        contract_multiplier = 0.0
        contract_multiplier_ready = False
        if dominant_order_book_id:
            contract_multiplier, contract_multiplier_ready = get_contract_multiplier(
                rqdatac, dominant_order_book_id, exchange=dominant_exchange
            )
        if not contract_multiplier_ready:
            try:
                override = float(stg_setting.get("contract_multiplier_override", 0.0))
            except Exception:
                override = 0.0
            if override > 0:
                contract_multiplier = float(override)
                contract_multiplier_ready = True
            elif dominant_exchange == "CZCE":
                fallback = CZCE_CONTRACT_MULTIPLIER_FALLBACK.get(info.product.upper(), 0.0)
                if fallback > 0:
                    contract_multiplier = float(fallback)
                    contract_multiplier_ready = True

        effective_size = fixed_size
        sizing_used = 0
        if contract_multiplier_ready and atr_value > 0:
            effective_size = compute_atr_position_size(
                atr_value,
                contract_multiplier,
                base_capital=sizing_base_capital,
                risk_rate=sizing_risk_rate,
                min_size=sizing_min_size,
            )
            sizing_used = 1

        if class_name == "TripleRsiLongShortStrategy":
            rsi_period = int(stg_setting.get("rsi_period", 5))
            ma_period = int(stg_setting.get("ma_period", 200))
            rsi_entry_long = int(stg_setting.get("rsi_entry_long", 30))
            rsi_exit_long = int(stg_setting.get("rsi_exit_long", 50))
            rsi_prev_threshold_long = int(stg_setting.get("rsi_prev_threshold_long", 60))
            rsi_entry_short = int(stg_setting.get("rsi_entry_short", 70))
            rsi_exit_short = int(stg_setting.get("rsi_exit_short", 50))
            rsi_prev_threshold_short = int(stg_setting.get("rsi_prev_threshold_short", 40))

            snapshot = compute_target_from_history(
                closes=closes,
                rsi_period=rsi_period,
                ma_period=ma_period,
                rsi_entry_long=rsi_entry_long,
                rsi_exit_long=rsi_exit_long,
                rsi_prev_threshold_long=rsi_prev_threshold_long,
                rsi_entry_short=rsi_entry_short,
                rsi_exit_short=rsi_exit_short,
                rsi_prev_threshold_short=rsi_prev_threshold_short,
                fixed_size=effective_size,
            )
            effective_allow_long = int(snapshot.get("allow_long") or 0) if snapshot.get("status") == "ok" else ""
            effective_allow_short = int(snapshot.get("allow_short") or 0) if snapshot.get("status") == "ok" else ""
        else:
            sma_200_period = int(stg_setting.get("sma_200_period", 200))
            sma_20_period = int(stg_setting.get("sma_20_period", 20))
            rsi_period = int(stg_setting.get("rsi_period", 5))
            rsi_entry = float(stg_setting.get("rsi_entry", 45))
            rsi_exit = float(stg_setting.get("rsi_exit", 65))

            snapshot = compute_pullback_mr_target_from_history(
                closes=closes,
                sma_200_period=sma_200_period,
                sma_20_period=sma_20_period,
                rsi_period=rsi_period,
                rsi_entry=rsi_entry,
                rsi_exit=rsi_exit,
                fixed_size=effective_size,
            )
        record: dict[str, Any] = {
            "strategy_name": strategy_name,
            "class_name": class_name,
            "product": info.product,
            "exchange": info.exchange,
            "configured_vt_symbol": info.vt_symbol,
            "order_book_id_888": order_book_id,
            "signal_date": signal_date.isoformat() if signal_date else "",
            "exec_date": exec_date.isoformat() if exec_date else "",
            "dominant_vt_symbol": dominant_vt_symbol,
            "dominant_order_book_id": dominant_order_book_id,
            "fixed_size": fixed_size,
            "effective_size": int(effective_size),
            "atr_period": int(atr_period),
            "atr_value": float(atr_value),
            "contract_multiplier": float(contract_multiplier) if contract_multiplier_ready else 0.0,
            "contract_multiplier_ready": int(contract_multiplier_ready),
            "sizing_used": int(sizing_used),
            "sizing_base_capital": float(sizing_base_capital),
            "sizing_risk_rate": float(sizing_risk_rate),
            "sizing_min_size": int(sizing_min_size),
            "price_add_rate": price_add_rate,
            "params": dict(stg_setting),
            "external_filter": {},
            "status": snapshot.get("status", ""),
            "target": int(snapshot.get("target", 0)),
            "history_count": len(closes),
        }
        if class_name == "TripleRsiLongShortStrategy":
            record["effective_allow_long"] = effective_allow_long
            record["effective_allow_short"] = effective_allow_short
            for k in [
                "close",
                "rsi_value",
                "rsi_1day_ago",
                "rsi_2day_ago",
                "rsi_3day_ago",
                "ma_value",
                "allow_long",
                "allow_short",
                "entry_count_long",
                "entry_count_short",
            ]:
                if k in snapshot and snapshot.get(k) is not None:
                    record[k] = snapshot.get(k)
        else:
            for k in ["close", "rsi_value", "sma200_value", "sma20_value"]:
                if k in snapshot and snapshot.get(k) is not None:
                    record[k] = snapshot.get(k)
        records.append(record)

        rows.append(
            {
                "strategy_name": strategy_name,
                "class_name": class_name,
                "product": info.product,
                "signal_date": record["signal_date"],
                "exec_date": record["exec_date"],
                "dominant_vt_symbol": dominant_vt_symbol,
                "effective_size": record["effective_size"],
                "atr_value": record["atr_value"],
                "contract_multiplier": record["contract_multiplier"],
                "target": record["target"],
                "status": record["status"],
                "price_add_rate": price_add_rate,
            }
        )
        if class_name == "TripleRsiLongShortStrategy":
            rows_trs.append(
                {
                    "strategy_name": strategy_name,
                    "product": info.product,
                    "signal_date": record["signal_date"],
                    "exec_date": record["exec_date"],
                    "dominant_vt_symbol": dominant_vt_symbol,
                    "effective_size": record["effective_size"],
                    "atr_value": record["atr_value"],
                    "contract_multiplier": record["contract_multiplier"],
                    "target": record["target"],
                    "status": record["status"],
                    "close": record.get("close"),
                    "ma_value": record.get("ma_value"),
                    "rsi_value": record.get("rsi_value"),
                    "allow_long": record.get("allow_long"),
                    "allow_short": record.get("allow_short"),
                    "entry_count_long": record.get("entry_count_long"),
                    "entry_count_short": record.get("entry_count_short"),
                    "price_add_rate": price_add_rate,
                }
            )
        else:
            rows_pbmr.append(
                {
                    "strategy_name": strategy_name,
                    "product": info.product,
                    "signal_date": record["signal_date"],
                    "exec_date": record["exec_date"],
                    "dominant_vt_symbol": dominant_vt_symbol,
                    "effective_size": record["effective_size"],
                    "atr_value": record["atr_value"],
                    "contract_multiplier": record["contract_multiplier"],
                    "target": record["target"],
                    "status": record["status"],
                    "close": record.get("close"),
                    "sma200_value": record.get("sma200_value"),
                    "sma20_value": record.get("sma20_value"),
                    "rsi_value": record.get("rsi_value"),
                    "price_add_rate": price_add_rate,
                }
            )

        allow_str = "-"
        if class_name == "TripleRsiLongShortStrategy":
            allow_str = f"{effective_allow_long}/{effective_allow_short}"
        print(
            f"{strategy_name:<16} {class_name:<26} {info.product:<4} target={record['target']:<4} "
            f"sig={record['signal_date']} exec={record['exec_date']} dom={dominant_vt_symbol or '-'} "
            f"allow(L/S)={allow_str} status={record['status']}"
        )

    by_product: dict[str, dict[str, Any]] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        product = str(rec.get("product") or "").strip().upper()
        if not product:
            continue
        status = str(rec.get("status") or "").strip()
        if status and status != "ok":
            continue

        slot = by_product.get(product)
        if slot is None:
            slot = {
                "strategy_name": "PORTFOLIO_NET",
                "class_name": "PortfolioNet",
                "product": product,
                "exchange": str(rec.get("exchange") or ""),
                "configured_vt_symbol": "",
                "dominant_vt_symbol": "",
                "order_book_id_888": str(rec.get("order_book_id_888") or ""),
                "signal_date": str(rec.get("signal_date") or ""),
                "exec_date": str(rec.get("exec_date") or ""),
                "fixed_size": "",
                "effective_size": rec.get("effective_size", ""),
                "atr_value": rec.get("atr_value", ""),
                "contract_multiplier": rec.get("contract_multiplier", ""),
                "sizing_base_capital": rec.get("sizing_base_capital", ""),
                "sizing_risk_rate": rec.get("sizing_risk_rate", ""),
                "sizing_used": rec.get("sizing_used", ""),
                "price_add_rate": float(rec.get("price_add_rate") or 0.01),
                "status": "ok",
                "target": 0,
                "components": [],
            }
            by_product[product] = slot
        else:
            try:
                s0 = int(slot.get("effective_size") or 0)
                s1 = int(rec.get("effective_size") or 0)
                if s1 > s0:
                    slot["effective_size"] = int(s1)
            except Exception:
                pass

        slot["target"] = int(slot.get("target", 0)) + int(rec.get("target") or 0)
        slot["components"].append(
            {
                "strategy_name": str(rec.get("strategy_name") or ""),
                "class_name": str(rec.get("class_name") or ""),
                "target": int(rec.get("target") or 0),
            }
        )

        dom = str(rec.get("dominant_vt_symbol") or "").strip()
        cfg = str(rec.get("configured_vt_symbol") or "").strip()
        if dom:
            slot["dominant_vt_symbol"] = dom
        elif cfg and not slot.get("dominant_vt_symbol"):
            slot["dominant_vt_symbol"] = cfg

        sig = str(rec.get("signal_date") or "").strip()
        if sig and sig > str(slot.get("signal_date") or ""):
            slot["signal_date"] = sig
        ex = str(rec.get("exec_date") or "").strip()
        if ex and ex > str(slot.get("exec_date") or ""):
            slot["exec_date"] = ex

        try:
            slot["price_add_rate"] = max(float(slot.get("price_add_rate") or 0.0), float(rec.get("price_add_rate") or 0.0))
        except Exception:
            pass

    portfolio_records = [by_product[k] for k in sorted(by_product.keys())]

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "requested_end_date": requested_end_date.isoformat(),
        "requested_exec_date": requested_exec_date.isoformat() if requested_exec_date else "",
        "source": {"cta_setting": str(cta_setting_path), "suffix": args.suffix},
        "records": records,
        "portfolio_records": portfolio_records,
    }

    if args.dry_run:
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"targets_{ts}.json"
    csv_path = output_dir / f"targets_{ts}.csv"
    trs_csv_path = output_dir / f"targets_trs_{ts}.csv"
    pbmr_csv_path = output_dir / f"targets_pullback_{ts}.csv"
    latest_json_path = output_dir / "targets_latest.json"
    latest_csv_path = output_dir / "targets_latest.csv"
    latest_trs_csv_path = output_dir / "targets_trs_latest.csv"
    latest_pbmr_csv_path = output_dir / "targets_pullback_latest.csv"
    prev_json_path = output_dir / "targets_prev.json"
    prev_csv_path = output_dir / "targets_prev.csv"

    if latest_json_path.exists():
        try:
            prev_json_path.write_bytes(latest_json_path.read_bytes())
        except Exception:
            pass
    if latest_csv_path.exists():
        try:
            prev_csv_path.write_bytes(latest_csv_path.read_bytes())
        except Exception:
            pass

    write_json_atomic(json_path, payload)
    write_json_atomic(latest_json_path, payload)
    write_csv(
        csv_path,
        rows=rows,
        fieldnames=[
            "strategy_name",
            "class_name",
            "product",
            "signal_date",
            "exec_date",
            "dominant_vt_symbol",
            "effective_size",
            "atr_value",
            "contract_multiplier",
            "target",
            "status",
            "price_add_rate",
        ],
    )
    write_csv(
        latest_csv_path,
        rows=rows,
        fieldnames=[
            "strategy_name",
            "class_name",
            "product",
            "signal_date",
            "exec_date",
            "dominant_vt_symbol",
            "effective_size",
            "atr_value",
            "contract_multiplier",
            "target",
            "status",
            "price_add_rate",
        ],
    )
    write_csv(
        trs_csv_path,
        rows=rows_trs,
        fieldnames=[
            "strategy_name",
            "product",
            "signal_date",
            "exec_date",
            "dominant_vt_symbol",
            "effective_size",
            "atr_value",
            "contract_multiplier",
            "target",
            "status",
            "close",
            "ma_value",
            "rsi_value",
            "allow_long",
            "allow_short",
            "entry_count_long",
            "entry_count_short",
            "price_add_rate",
        ],
    )
    write_csv(
        latest_trs_csv_path,
        rows=rows_trs,
        fieldnames=[
            "strategy_name",
            "product",
            "signal_date",
            "exec_date",
            "dominant_vt_symbol",
            "effective_size",
            "atr_value",
            "contract_multiplier",
            "target",
            "status",
            "close",
            "ma_value",
            "rsi_value",
            "allow_long",
            "allow_short",
            "entry_count_long",
            "entry_count_short",
            "price_add_rate",
        ],
    )
    write_csv(
        pbmr_csv_path,
        rows=rows_pbmr,
        fieldnames=[
            "strategy_name",
            "product",
            "signal_date",
            "exec_date",
            "dominant_vt_symbol",
            "effective_size",
            "atr_value",
            "contract_multiplier",
            "target",
            "status",
            "close",
            "sma200_value",
            "sma20_value",
            "rsi_value",
            "price_add_rate",
        ],
    )
    write_csv(
        latest_pbmr_csv_path,
        rows=rows_pbmr,
        fieldnames=[
            "strategy_name",
            "product",
            "signal_date",
            "exec_date",
            "dominant_vt_symbol",
            "effective_size",
            "atr_value",
            "contract_multiplier",
            "target",
            "status",
            "close",
            "sma200_value",
            "sma20_value",
            "rsi_value",
            "price_add_rate",
        ],
    )
    print(f"已写入: {json_path}")
    print(f"已写入: {csv_path}")
    print(f"已写入: {trs_csv_path}")
    print(f"已写入: {pbmr_csv_path}")
    print(f"已写入: {latest_json_path}")
    print(f"已写入: {latest_csv_path}")
    print(f"已写入: {latest_trs_csv_path}")
    print(f"已写入: {latest_pbmr_csv_path}")
    if prev_json_path.exists():
        print(f"已更新: {prev_json_path}")
    if prev_csv_path.exists():
        print(f"已更新: {prev_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
