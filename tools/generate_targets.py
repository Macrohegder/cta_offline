from __future__ import annotations

import argparse
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trs_offline.io_utils import read_json, write_csv, write_json_atomic
from trs_offline.paths import get_default_paths
from trs_offline.rqdatac_client import init_rqdatac
from trs_offline.trs_logic import (
    compute_pullback_mr_target_from_history,
    compute_target_from_history,
    decide_external_filter,
)
from trs_offline.vnpy_symbol import fetch_dominant_vt_symbol, guess_next_trading_date, parse_vt_symbol


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    defaults = get_default_paths()
    p.add_argument("--cta-setting", default=str(defaults.cta_strategy_setting))
    p.add_argument("--filters", default=str(defaults.triple_rsi_filters))
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


def fetch_daily_closes(rqdatac, order_book_id: str, start_date: date, end_date: date) -> tuple[list[float], str]:
    data = rqdatac.get_price(
        order_book_ids=order_book_id,
        start_date=start_date,
        end_date=end_date,
        frequency="1d",
        fields=["close"],
    )
    if data is None or len(data) == 0:
        return [], ""

    if hasattr(data, "columns") and "close" in data.columns:
        series = data["close"]
    else:
        series = data.squeeze()

    series = series.dropna()
    if len(series) == 0:
        return [], ""

    closes = [float(v) for v in series.tolist()]
    last_index = series.index[-1]
    index_value = last_index[-1] if isinstance(last_index, tuple) and last_index else last_index
    trade_date = index_value.date().isoformat() if hasattr(index_value, "date") else str(index_value)[:10]
    return closes, trade_date


def main() -> int:
    args = parse_args()
    include = normalize_include(args.include)

    cta_setting_path = Path(args.cta_setting)
    filters_path = Path(args.filters)
    output_dir = Path(args.output_dir)

    settings: dict[str, Any] = read_json(cta_setting_path)
    filters_payload: dict[str, Any] | None = None
    if filters_path.exists():
        filters_payload = read_json(filters_path)

    rqdatac = init_rqdatac()

    requested_end_date = parse_day(args.end_date)
    requested_exec_date = parse_day(args.exec_date) if args.exec_date else None

    rows: list[dict[str, Any]] = []
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

        closes, trade_date_str = fetch_daily_closes(rqdatac, order_book_id, start_date, requested_end_date)
        signal_date = date.fromisoformat(trade_date_str) if trade_date_str else None

        ext = None
        effective_allow_long = ""
        effective_allow_short = ""
        snapshot: dict[str, Any] = {}

        fixed_size = int(stg_setting.get("fixed_size", 1))
        price_add_rate = float(stg_setting.get("price_add_rate", 0.01))

        if class_name == "TripleRsiLongShortStrategy":
            rsi_period = int(stg_setting.get("rsi_period", 5))
            ma_period = int(stg_setting.get("ma_period", 200))
            allow_long_entry = int(stg_setting.get("allow_long_entry", 1))
            allow_short_entry = int(stg_setting.get("allow_short_entry", 1))
            use_external_filter = int(stg_setting.get("use_external_filter", 1))
            external_filter_strict = int(stg_setting.get("external_filter_strict", 1))
            external_filter_check_date = int(stg_setting.get("external_filter_check_date", 0))
            rsi_entry_long = int(stg_setting.get("rsi_entry_long", 30))
            rsi_exit_long = int(stg_setting.get("rsi_exit_long", 50))
            rsi_prev_threshold_long = int(stg_setting.get("rsi_prev_threshold_long", 60))
            rsi_entry_short = int(stg_setting.get("rsi_entry_short", 70))
            rsi_exit_short = int(stg_setting.get("rsi_exit_short", 50))
            rsi_prev_threshold_short = int(stg_setting.get("rsi_prev_threshold_short", 40))

            ext = decide_external_filter(
                product=info.product,
                manual_allow_long=allow_long_entry,
                manual_allow_short=allow_short_entry,
                use_external_filter=use_external_filter,
                external_filter_strict=external_filter_strict,
                external_filter_check_date=external_filter_check_date,
                filters_payload=filters_payload,
                signal_date=signal_date,
            )

            effective_allow_long = int(bool(allow_long_entry > 0) and bool(ext.allow_long > 0))
            effective_allow_short = int(bool(allow_short_entry > 0) and bool(ext.allow_short > 0))

            snapshot = compute_target_from_history(
                closes=closes,
                rsi_period=rsi_period,
                ma_period=ma_period,
                allow_long_entry=allow_long_entry,
                allow_short_entry=allow_short_entry,
                rsi_entry_long=rsi_entry_long,
                rsi_exit_long=rsi_exit_long,
                rsi_prev_threshold_long=rsi_prev_threshold_long,
                rsi_entry_short=rsi_entry_short,
                rsi_exit_short=rsi_exit_short,
                rsi_prev_threshold_short=rsi_prev_threshold_short,
                fixed_size=fixed_size,
                effective_allow_long=effective_allow_long,
                effective_allow_short=effective_allow_short,
            )
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
                fixed_size=fixed_size,
            )

        exec_date = requested_exec_date
        if exec_date is None and signal_date is not None:
            exec_date = guess_next_trading_date(rqdatac, signal_date)

        dominant_vt_symbol = ""
        if exec_date is not None:
            try:
                dominant_vt_symbol = fetch_dominant_vt_symbol(rqdatac, info.product, info.exchange, exec_date)
            except Exception:
                dominant_vt_symbol = ""

        record = {
            "strategy_name": strategy_name,
            "class_name": class_name,
            "product": info.product,
            "exchange": info.exchange,
            "configured_vt_symbol": info.vt_symbol,
            "order_book_id_888": order_book_id,
            "signal_date": signal_date.isoformat() if signal_date else "",
            "exec_date": exec_date.isoformat() if exec_date else "",
            "dominant_vt_symbol": dominant_vt_symbol,
            "fixed_size": fixed_size,
            "price_add_rate": price_add_rate,
            "params": dict(stg_setting),
            "external_filter": asdict(ext) if ext is not None else {},
            "effective_allow_long": effective_allow_long,
            "effective_allow_short": effective_allow_short,
            "status": snapshot.get("status", ""),
            "target": int(snapshot.get("target", 0)),
            "rsi_value": snapshot.get("rsi_value"),
            "rsi_1day_ago": snapshot.get("rsi_1day_ago"),
            "rsi_2day_ago": snapshot.get("rsi_2day_ago"),
            "rsi_3day_ago": snapshot.get("rsi_3day_ago"),
            "ma_value": snapshot.get("ma_value"),
            "sma200_value": snapshot.get("sma200_value"),
            "sma20_value": snapshot.get("sma20_value"),
            "entry_count_long": snapshot.get("entry_count_long"),
            "entry_count_short": snapshot.get("entry_count_short"),
            "history_count": len(closes),
        }
        records.append(record)

        rows.append(
            {
                "strategy_name": strategy_name,
                "class_name": class_name,
                "product": info.product,
                "signal_date": record["signal_date"],
                "exec_date": record["exec_date"],
                "dominant_vt_symbol": dominant_vt_symbol,
                "target": record["target"],
                "status": record["status"],
                "effective_allow_long": effective_allow_long,
                "effective_allow_short": effective_allow_short,
                "rsi_value": record["rsi_value"],
                "ma_value": record["ma_value"],
                "sma200_value": record["sma200_value"],
                "sma20_value": record["sma20_value"],
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
                "price_add_rate": float(rec.get("price_add_rate") or 0.01),
                "status": "ok",
                "target": 0,
                "components": [],
            }
            by_product[product] = slot

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
        "source": {"cta_setting": str(cta_setting_path), "filters": str(filters_path), "suffix": args.suffix},
        "records": records,
        "portfolio_records": portfolio_records,
    }

    if args.dry_run:
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"targets_{ts}.json"
    csv_path = output_dir / f"targets_{ts}.csv"
    latest_json_path = output_dir / "targets_latest.json"
    latest_csv_path = output_dir / "targets_latest.csv"
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
            "target",
            "status",
            "effective_allow_long",
            "effective_allow_short",
            "rsi_value",
            "ma_value",
            "sma200_value",
            "sma20_value",
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
            "target",
            "status",
            "effective_allow_long",
            "effective_allow_short",
            "rsi_value",
            "ma_value",
            "sma200_value",
            "sma20_value",
        ],
    )
    print(f"已写入: {json_path}")
    print(f"已写入: {csv_path}")
    print(f"已写入: {latest_json_path}")
    print(f"已写入: {latest_csv_path}")
    if prev_json_path.exists():
        print(f"已更新: {prev_json_path}")
    if prev_csv_path.exists():
        print(f"已更新: {prev_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

