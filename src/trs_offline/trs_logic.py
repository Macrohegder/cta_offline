from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional


@dataclass(frozen=True)
class ExternalFilterDecision:
    ready: bool
    allow_long: int
    allow_short: int
    trade_date: str
    status: str


def calculate_rsi(prices: list[float], period: int) -> float:
    series = calculate_rsi_series(prices, period)
    return float(series[-1]) if series else 50.0


def calculate_rsi_series(prices: list[float], period: int) -> list[float]:
    n = int(period)
    if n <= 0:
        return [50.0 for _ in prices]
    if len(prices) < n + 1:
        return [50.0 for _ in prices]

    deltas = [float(prices[i]) - float(prices[i - 1]) for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]

    out = [50.0 for _ in prices]
    avg_gain = sum(gains[:n]) / n
    avg_loss = sum(losses[:n]) / n

    if avg_loss == 0:
        out[n] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[n] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(n + 1, len(prices)):
        gain = gains[i - 1]
        loss = losses[i - 1]
        avg_gain = (avg_gain * (n - 1) + gain) / n
        avg_loss = (avg_loss * (n - 1) + loss) / n
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))

    return out


def calculate_sma(values: list[float], period: int) -> float:
    if period <= 0 or len(values) < period:
        return 0.0
    window = values[-period:]
    return float(sum(window) / period)


def compute_pullback_mr_target_from_history(
    closes: list[float],
    sma_200_period: int,
    sma_20_period: int,
    rsi_period: int,
    rsi_entry: float,
    rsi_exit: float,
    fixed_size: int,
) -> dict[str, Any]:
    min_needed = max(sma_200_period, sma_20_period, rsi_period + 1) + 3
    if len(closes) < min_needed:
        return {"target": 0, "status": "insufficient_history"}

    rsi_series = calculate_rsi_series(closes, rsi_period)

    target = 0
    last_snapshot: dict[str, Any] = {}

    for i in range(len(closes)):
        close = float(closes[i])
        if i + 1 < max(sma_200_period, sma_20_period, rsi_period + 1):
            continue

        sma200_value = float(sum(closes[i - sma_200_period + 1 : i + 1]) / sma_200_period)
        sma20_value = float(sum(closes[i - sma_20_period + 1 : i + 1]) / sma_20_period)
        rsi_value = float(rsi_series[i])

        if target > 0:
            if rsi_value > float(rsi_exit):
                target = 0
            else:
                target = int(fixed_size)
        else:
            if close > sma200_value and close < sma20_value and rsi_value < float(rsi_entry):
                target = int(fixed_size)
            else:
                target = 0

        last_snapshot = {
            "target": int(target),
            "status": "ok",
            "close": float(close),
            "sma200_value": float(sma200_value),
            "sma20_value": float(sma20_value),
            "rsi_value": float(rsi_value),
        }

    if not last_snapshot:
        return {"target": 0, "status": "insufficient_history"}
    return last_snapshot


def compute_target_from_history(
    closes: list[float],
    rsi_period: int,
    ma_period: int,
    rsi_entry_long: int,
    rsi_exit_long: int,
    rsi_prev_threshold_long: int,
    rsi_entry_short: int,
    rsi_exit_short: int,
    rsi_prev_threshold_short: int,
    fixed_size: int,
) -> dict[str, Any]:
    if len(closes) < max(rsi_period + 5, ma_period + 3):
        return {"target": 0, "status": "insufficient_history"}

    rsi_series = calculate_rsi_series(closes, rsi_period)

    target = 0
    last_snapshot: dict[str, Any] = {}

    for i in range(3, len(closes)):
        close = float(closes[i])
        rsi_value = rsi_series[i]
        rsi_1day_ago = rsi_series[i - 1]
        rsi_2day_ago = rsi_series[i - 2]
        rsi_3day_ago = rsi_series[i - 3]
        ma_value = sum(closes[i - ma_period + 1 : i + 1]) / ma_period if i + 1 >= ma_period else 0.0

        allow_long = bool(ma_value > 0) and bool(close > ma_value)
        allow_short = bool(ma_value > 0) and bool(close < ma_value)

        condition_long_1 = rsi_value < rsi_entry_long
        condition_long_2 = rsi_value < rsi_1day_ago and rsi_1day_ago < rsi_2day_ago
        condition_long_3 = rsi_3day_ago < rsi_prev_threshold_long
        condition_long_4 = allow_long

        condition_short_1 = rsi_value > rsi_entry_short
        condition_short_2 = rsi_value > rsi_1day_ago and rsi_1day_ago > rsi_2day_ago
        condition_short_3 = rsi_3day_ago > rsi_prev_threshold_short
        condition_short_4 = allow_short

        entry_signal_long = condition_long_1 and condition_long_2 and condition_long_3 and condition_long_4
        entry_signal_short = condition_short_1 and condition_short_2 and condition_short_3 and condition_short_4

        exit_signal_long = rsi_value >= rsi_exit_long and rsi_1day_ago < rsi_exit_long
        exit_signal_short = rsi_value <= rsi_exit_short and rsi_1day_ago > rsi_exit_short

        if target == 0:
            if entry_signal_long and not entry_signal_short:
                target = int(fixed_size)
            elif entry_signal_short and not entry_signal_long:
                target = -int(fixed_size)
            elif entry_signal_long and entry_signal_short:
                target = -int(fixed_size) if rsi_value >= 50 else int(fixed_size)
        elif target > 0:
            if exit_signal_long:
                target = 0
        elif target < 0:
            if exit_signal_short:
                target = 0

        last_snapshot = {
            "close": float(close),
            "rsi_value": float(rsi_value),
            "rsi_1day_ago": float(rsi_1day_ago),
            "rsi_2day_ago": float(rsi_2day_ago),
            "rsi_3day_ago": float(rsi_3day_ago),
            "ma_value": float(ma_value),
            "allow_long": int(bool(allow_long)),
            "allow_short": int(bool(allow_short)),
            "entry_count_long": int(sum([condition_long_1, condition_long_2, condition_long_3, condition_long_4])),
            "entry_count_short": int(sum([condition_short_1, condition_short_2, condition_short_3, condition_short_4])),
        }

    last_snapshot["target"] = int(target)
    last_snapshot["status"] = "ok"
    return last_snapshot


def decide_external_filter(
    product: str,
    manual_allow_long: int,
    manual_allow_short: int,
    use_external_filter: int,
    external_filter_strict: int,
    external_filter_check_date: int,
    filters_payload: Optional[dict[str, Any]],
    signal_date: Optional[date],
) -> ExternalFilterDecision:
    manual_long = int(manual_allow_long > 0)
    manual_short = int(manual_allow_short > 0)

    if not use_external_filter:
        return ExternalFilterDecision(
            ready=False,
            allow_long=manual_long,
            allow_short=manual_short,
            trade_date="",
            status="disabled",
        )

    if not filters_payload:
        return ExternalFilterDecision(False, manual_long, manual_short, "", "missing_filters")

    symbols = filters_payload.get("symbols")
    if not isinstance(symbols, dict):
        if external_filter_strict:
            return ExternalFilterDecision(False, 0, 0, "", "invalid_filters")
        return ExternalFilterDecision(False, manual_long, manual_short, "", "invalid_filters")

    record = symbols.get(product.upper())
    if not isinstance(record, dict):
        if external_filter_strict:
            return ExternalFilterDecision(False, 0, 0, "", "product_not_found")
        return ExternalFilterDecision(False, manual_long, manual_short, "", "product_not_found")

    trade_date = str(record.get("trade_date") or filters_payload.get("trade_date") or "")
    if external_filter_check_date and signal_date is not None:
        if trade_date and trade_date != signal_date.isoformat():
            if external_filter_strict:
                return ExternalFilterDecision(False, 0, 0, trade_date, "date_mismatch")
            return ExternalFilterDecision(False, manual_long, manual_short, trade_date, "date_mismatch")

    status = str(record.get("status", "ok"))
    if status != "ok":
        if external_filter_strict:
            return ExternalFilterDecision(False, 0, 0, trade_date, status)
        return ExternalFilterDecision(False, manual_long, manual_short, trade_date, status)

    allow_long = int(record.get("allow_long_entry", 0) > 0)

    allow_short = 0
    close = record.get("close")
    ma_value = record.get("ma_value")
    if close is not None and ma_value is not None:
        allow_short = int(float(close) < float(ma_value))

    return ExternalFilterDecision(True, allow_long, allow_short, trade_date, status)
