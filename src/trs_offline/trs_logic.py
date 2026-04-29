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
    if len(prices) < period + 1:
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    for i in range(1, period + 1):
        change = prices[-i] - prices[-i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


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

    rsi_series: list[float] = []
    for i in range(len(closes)):
        rsi_series.append(calculate_rsi(closes[: i + 1], rsi_period))

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
    allow_long_entry: int,
    allow_short_entry: int,
    rsi_entry_long: int,
    rsi_exit_long: int,
    rsi_prev_threshold_long: int,
    rsi_entry_short: int,
    rsi_exit_short: int,
    rsi_prev_threshold_short: int,
    fixed_size: int,
    effective_allow_long: int,
    effective_allow_short: int,
) -> dict[str, Any]:
    if len(closes) < max(rsi_period + 5, ma_period + 3):
        return {"target": 0, "status": "insufficient_history"}

    rsi_series: list[float] = []
    for i in range(len(closes)):
        rsi_series.append(calculate_rsi(closes[: i + 1], rsi_period))

    target = 0
    last_snapshot: dict[str, Any] = {}

    for i in range(3, len(closes)):
        rsi_value = rsi_series[i]
        rsi_1day_ago = rsi_series[i - 1]
        rsi_2day_ago = rsi_series[i - 2]
        rsi_3day_ago = rsi_series[i - 3]
        ma_value = sum(closes[i - ma_period + 1 : i + 1]) / ma_period if i + 1 >= ma_period else 0.0

        condition_long_1 = rsi_value < rsi_entry_long
        condition_long_2 = rsi_value < rsi_1day_ago and rsi_1day_ago < rsi_2day_ago
        condition_long_3 = rsi_3day_ago < rsi_prev_threshold_long
        condition_long_4 = bool(effective_allow_long > 0) and bool(allow_long_entry > 0)

        condition_short_1 = rsi_value > rsi_entry_short
        condition_short_2 = rsi_value > rsi_1day_ago and rsi_1day_ago > rsi_2day_ago
        condition_short_3 = rsi_3day_ago > rsi_prev_threshold_short
        condition_short_4 = bool(effective_allow_short > 0) and bool(allow_short_entry > 0)

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
            "rsi_value": float(rsi_value),
            "rsi_1day_ago": float(rsi_1day_ago),
            "rsi_2day_ago": float(rsi_2day_ago),
            "rsi_3day_ago": float(rsi_3day_ago),
            "ma_value": float(ma_value),
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
        if external_filter_strict:
            return ExternalFilterDecision(False, 0, 0, "", "missing_filters")
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
