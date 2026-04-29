from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trs_offline.io_utils import read_json, write_csv
from trs_offline.paths import get_default_paths
from trs_offline.rqdatac_client import init_rqdatac

try:
    from vnpy.trader.constant import Direction, Offset

    DIRECTION_LONG = Direction.LONG.value
    DIRECTION_SHORT = Direction.SHORT.value
    OFFSET_OPEN = Offset.OPEN.value
    OFFSET_CLOSE = Offset.CLOSE.value
except Exception:
    DIRECTION_LONG = "多"
    DIRECTION_SHORT = "空"
    OFFSET_OPEN = "开"
    OFFSET_CLOSE = "平"


@dataclass(frozen=True)
class TargetState:
    product: str
    vt_symbol: str
    signal_date: str
    target: int
    price_add_rate: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    defaults = get_default_paths()
    p.add_argument(
        "--targets",
        default=str(defaults.output_dir / "targets_latest.json"),
    )
    p.add_argument(
        "--prev-targets",
        default=str(defaults.output_dir / "targets_prev.json"),
    )
    p.add_argument("--algo", default="TwapAlgo", choices=["TwapAlgo", "BestLimitAlgo", "IcebergAlgo", "SniperAlgo"])
    p.add_argument("--time", type=int, default=600)
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--display-volume", type=float, default=1.0)
    p.add_argument("--min-volume", type=int, default=1)
    p.add_argument("--max-volume", type=int, default=1)
    p.add_argument("--include", nargs="*", default=[])
    p.add_argument("--close-offset", default=OFFSET_CLOSE)
    p.add_argument("--open-offset", default=OFFSET_OPEN)
    p.add_argument("--allow-initial", action="store_true")
    p.add_argument("--output", default="")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


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


def load_target_states(path: Path) -> dict[str, TargetState]:
    payload = read_json(path)
    records = payload.get("portfolio_records")
    if not isinstance(records, list) or not records:
        raise SystemExit(
            f"targets 文件缺少 portfolio_records 或为空: {path}（交易清单仅允许按 portfolio_records 生成；请先运行 generate_targets.py）"
        )
    out: dict[str, TargetState] = {}

    for rec in records:
        if not isinstance(rec, dict):
            continue
        product = str(rec.get("product", "")).strip().upper()
        dominant = str(rec.get("dominant_vt_symbol") or "").strip()
        configured = str(rec.get("configured_vt_symbol") or "").strip()
        vt_symbol = (dominant or configured).strip()
        if not product or not vt_symbol:
            continue
        status = str(rec.get("status", "")).strip()
        if status and status != "ok":
            continue
        signal_date = str(rec.get("signal_date", "")).strip()
        target = _safe_int(rec.get("target", 0), 0)
        price_add_rate = _safe_float(rec.get("price_add_rate", 0.01), 0.01)

        prev = out.get(product)
        if prev is None:
            out[product] = TargetState(
                product=product,
                vt_symbol=vt_symbol,
                signal_date=signal_date,
                target=target,
                price_add_rate=price_add_rate,
            )
            continue

        merged_symbol = prev.vt_symbol
        if dominant:
            merged_symbol = dominant
        merged_signal_date = prev.signal_date
        if signal_date and signal_date > merged_signal_date:
            merged_signal_date = signal_date
        merged_target = int(prev.target) + int(target)
        merged_add_rate = max(float(prev.price_add_rate), float(price_add_rate))
        out[product] = TargetState(
            product=product,
            vt_symbol=merged_symbol,
            signal_date=merged_signal_date,
            target=merged_target,
            price_add_rate=merged_add_rate,
        )
    return out


def fetch_close(rqdatac, order_book_id: str, signal_date: date) -> float:
    start_date = signal_date - timedelta(days=10)
    base = str(order_book_id).split(".", 1)[0]
    candidates = [
        str(order_book_id),
        base.upper(),
        base.lower(),
        str(order_book_id).upper(),
        str(order_book_id).lower(),
    ]

    for oid in candidates:
        try:
            data = rqdatac.get_price(
                order_book_ids=oid,
                start_date=start_date,
                end_date=signal_date,
                frequency="1d",
                fields=["close"],
            )
        except Exception:
            continue

        if data is None or len(data) == 0:
            continue

        if hasattr(data, "columns") and "close" in data.columns:
            series = data["close"]
        else:
            series = data.squeeze()

        series = series.dropna()
        if len(series) == 0:
            continue
        return float(series.iloc[-1])

    return 0.0


def build_order_rows(
    rqdatac,
    now: dict[str, TargetState],
    prev: dict[str, TargetState],
    include: set[str],
    close_offset: str,
    open_offset: str,
    algo: str,
    time: int,
    interval: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    products = sorted(set(now.keys()) | set(prev.keys()))

    for product in products:
        if include and product not in include:
            continue
        n = now.get(product)
        p = prev.get(product)

        prev_target = p.target if p else 0
        prev_symbol = p.vt_symbol if p else ""
        now_target = n.target if n else 0
        now_symbol = n.vt_symbol if n else ""
        signal_date_str = n.signal_date if n else (p.signal_date if p else "")
        add_rate = n.price_add_rate if n else (p.price_add_rate if p else 0.01)

        if not now_symbol and not prev_symbol:
            continue

        if prev_symbol and now_symbol and prev_symbol != now_symbol:
            rows.extend(
                _orders_for_target_change(
                    rqdatac=rqdatac,
                    vt_symbol=prev_symbol,
                    prev_target=prev_target,
                    now_target=0,
                    signal_date_str=signal_date_str,
                    fallback_close=0.0,
                    close_offset=close_offset,
                    open_offset=open_offset,
                    price_add_rate=add_rate,
                    algo=algo,
                    time=time,
                    interval=interval,
                    comment=f"rollover_close {product}",
                )
            )
            rows.extend(
                _orders_for_target_change(
                    rqdatac=rqdatac,
                    vt_symbol=now_symbol,
                    prev_target=0,
                    now_target=now_target,
                    signal_date_str=signal_date_str,
                    fallback_close=0.0,
                    close_offset=close_offset,
                    open_offset=open_offset,
                    price_add_rate=add_rate,
                    algo=algo,
                    time=time,
                    interval=interval,
                    comment=f"rollover_open {product}",
                )
            )
            continue

        vt_symbol = now_symbol or prev_symbol
        rows.extend(
            _orders_for_target_change(
                rqdatac=rqdatac,
                vt_symbol=vt_symbol,
                prev_target=prev_target,
                now_target=now_target,
                signal_date_str=signal_date_str,
                fallback_close=0.0,
                close_offset=close_offset,
                open_offset=open_offset,
                price_add_rate=add_rate,
                algo=algo,
                time=time,
                interval=interval,
                comment=f"rebalance {product}",
            )
        )

    return rows


def validate_order_rows(
    rows: list[dict[str, Any]],
    close_offset: str,
    open_offset: str,
    algo: str,
    time: int,
    interval: int,
) -> None:
    allowed_directions = {DIRECTION_LONG, DIRECTION_SHORT}
    allowed_offsets = {str(close_offset), str(open_offset)}

    for i, row in enumerate(rows):
        direction = str(row.get("direction", "")).strip()
        offset = str(row.get("offset", "")).strip()
        vt_symbol = str(row.get("vt_symbol", "")).strip()
        volume = _safe_int(row.get("volume", 0), 0)

        if "\ufffd" in direction or "\ufffd" in offset:
            raise SystemExit(f"第 {i + 1} 行指令存在替换字符，无法导入（vt_symbol={vt_symbol}, direction={direction}, offset={offset}）")
        if direction not in allowed_directions:
            raise SystemExit(f"第 {i + 1} 行 direction 非法，无法导入（vt_symbol={vt_symbol}, direction={direction}）")
        if offset not in allowed_offsets:
            raise SystemExit(f"第 {i + 1} 行 offset 非法，无法导入（vt_symbol={vt_symbol}, offset={offset}）")
        if volume <= 0:
            raise SystemExit(f"第 {i + 1} 行 volume<=0，无法导入（vt_symbol={vt_symbol}, volume={volume}）")

        try:
            direction.encode("gbk")
            offset.encode("gbk")
        except Exception:
            raise SystemExit(
                f"第 {i + 1} 行 direction/offset 无法用 GBK 编码，导入端按 GBK 解码会乱码（vt_symbol={vt_symbol}, direction={direction}, offset={offset}）"
            )

    if algo == "TwapAlgo":
        if interval <= 0 or time <= 0:
            raise SystemExit("TwapAlgo 的 time/interval 必须 > 0")
        if time <= interval:
            raise SystemExit("TwapAlgo 要求 time > interval（否则还没到下单间隔就会因 time 到期结束）")
        for row in rows:
            v = _safe_int(row.get("volume", 0), 0)
            if v <= 0:
                continue
            if (time / interval) > v:
                raise SystemExit(
                    f"TwapAlgo 在该参数下会把 volume={v} 切成 {time}/{interval} 片，单笔小于最小下单单位导致无法成交；建议改用 BestLimitAlgo 或 IcebergAlgo"
                )


def _build_actions(prev_target: int, now_target: int, close_offset: str, open_offset: str) -> list[tuple[str, str, int]]:
    p = int(prev_target)
    n = int(now_target)
    if p == n:
        return []

    actions: list[tuple[str, str, int]] = []

    if p == 0:
        if n > 0:
            actions.append((DIRECTION_LONG, open_offset, n))
        elif n < 0:
            actions.append((DIRECTION_SHORT, open_offset, abs(n)))
        return actions

    if n == 0:
        if p > 0:
            actions.append((DIRECTION_SHORT, close_offset, p))
        elif p < 0:
            actions.append((DIRECTION_LONG, close_offset, abs(p)))
        return actions

    if p > 0 and n > 0:
        if n > p:
            actions.append((DIRECTION_LONG, open_offset, n - p))
        else:
            actions.append((DIRECTION_SHORT, close_offset, p - n))
        return actions

    if p < 0 and n < 0:
        if abs(n) > abs(p):
            actions.append((DIRECTION_SHORT, open_offset, abs(n) - abs(p)))
        else:
            actions.append((DIRECTION_LONG, close_offset, abs(p) - abs(n)))
        return actions

    if p > 0 and n < 0:
        actions.append((DIRECTION_SHORT, close_offset, p))
        actions.append((DIRECTION_SHORT, open_offset, abs(n)))
        return actions

    if p < 0 and n > 0:
        actions.append((DIRECTION_LONG, close_offset, abs(p)))
        actions.append((DIRECTION_LONG, open_offset, n))
        return actions

    return actions


def _orders_for_target_change(
    rqdatac,
    vt_symbol: str,
    prev_target: int,
    now_target: int,
    signal_date_str: str,
    fallback_close: float,
    close_offset: str,
    open_offset: str,
    price_add_rate: float,
    algo: str,
    time: int,
    interval: int,
    comment: str,
) -> list[dict[str, Any]]:
    actions = _build_actions(prev_target, now_target, close_offset=close_offset, open_offset=open_offset)
    if not actions:
        return []

    signal_date: Optional[date] = None
    if signal_date_str:
        try:
            signal_date = date.fromisoformat(signal_date_str)
        except Exception:
            signal_date = None

    ref_close = 0.0
    if signal_date is not None:
        try:
            ref_close = fetch_close(rqdatac, vt_symbol, signal_date)
        except Exception:
            ref_close = 0.0
    if ref_close <= 0 and fallback_close:
        ref_close = float(fallback_close)

    rows: list[dict[str, Any]] = []
    for direction, offset, volume in actions:
        price = 0.0
        if ref_close > 0:
            if direction == DIRECTION_LONG:
                price = ref_close * (1.0 + price_add_rate)
            else:
                price = ref_close * (1.0 - price_add_rate)

        row: dict[str, Any] = {
            "vt_symbol": vt_symbol,
            "direction": direction,
            "offset": offset,
            "price": round(price, 6),
            "volume": int(volume),
            "comment": comment,
            "ref_close": round(ref_close, 6) if ref_close else "",
            "price_add_rate": price_add_rate,
        }
        rows.append(row)
    return rows


def main() -> int:
    args = parse_args()
    targets_path = Path(args.targets)
    prev_targets_path = Path(args.prev_targets)
    if not args.allow_initial and not prev_targets_path.exists():
        raise SystemExit(f"未找到上一份 targets: {prev_targets_path}（为避免误开仓，已终止；如首次生成请加 --allow-initial）")

    now = load_target_states(targets_path)
    prev = load_target_states(prev_targets_path) if prev_targets_path.exists() else {}

    rqdatac = init_rqdatac()

    include = set([str(s).strip().upper() for s in args.include]) if args.include else set()

    rows = build_order_rows(
        rqdatac=rqdatac,
        now=now,
        prev=prev,
        include=include,
        close_offset=str(args.close_offset),
        open_offset=str(args.open_offset),
        algo=str(args.algo),
        time=int(args.time),
        interval=int(args.interval),
    )

    algo_name = str(args.algo)
    fieldnames = ["vt_symbol", "direction", "offset", "price", "volume"]
    if algo_name == "TwapAlgo":
        fieldnames += ["time", "interval"]
        for row in rows:
            row["time"] = int(args.time)
            row["interval"] = int(args.interval)
    elif algo_name == "IcebergAlgo":
        fieldnames += ["display_volume", "interval"]
        for row in rows:
            row["display_volume"] = float(args.display_volume)
            row["interval"] = int(args.interval)
    elif algo_name == "BestLimitAlgo":
        for row in rows:
            row["price"] = 0.0
        fieldnames += ["min_volume", "max_volume"]
        for row in rows:
            row["min_volume"] = int(args.min_volume)
            row["max_volume"] = int(args.max_volume)
    elif algo_name == "SniperAlgo":
        pass
    else:
        raise SystemExit(f"未知 algo: {algo_name}")
    fieldnames += ["comment", "ref_close", "price_add_rate"]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(args.output) if args.output else targets_path.with_name(f"algotrading_{args.algo}_{ts}.csv")
    latest_path = targets_path.with_name(f"algotrading_{args.algo}_latest.csv")

    for row in rows:
        print(
            f"{row['vt_symbol']:<16} {row['direction']}{row['offset']} vol={row['volume']} price={row['price']} {row.get('comment','')}"
        )
    if not rows:
        print("无调仓指令（targets 未变化或全部为空）")

    if args.dry_run:
        return 0

    validate_order_rows(
        rows,
        close_offset=str(args.close_offset),
        open_offset=str(args.open_offset),
        algo=algo_name,
        time=int(args.time),
        interval=int(args.interval),
    )
    write_csv(output_path, rows=rows, fieldnames=fieldnames, encoding="gbk")
    write_csv(latest_path, rows=rows, fieldnames=fieldnames, encoding="gbk")
    print(f"已写入: {output_path}")
    print(f"已写入: {latest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
