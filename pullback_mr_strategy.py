"""
Pullback Mean Reversion Strategy (QuantifiedStrategies)
=======================================================
基于 QuantifiedStrategies Substack "Pullback Trading Strategies"。

入场规则:
  - 收盘价 > 200-period SMA (长期上升趋势过滤)
  - 收盘价 < 20-period SMA (短期回调)
  - 5-period RSI < 45 (短期超卖)
  - 入场: 当日在收盘价买入

出场规则:
  - 5-period RSI > 65
  - 出场: 当日在收盘价卖出

参数优化:
  - sma_200_period: 50-300 (长期均线周期)
  - sma_20_period: 10-60 (短期均线周期)
  - rsi_period: 2-14 (RSI周期)
  - rsi_entry: 20-55 (RSI入场阈值)
  - rsi_exit: 50-80 (RSI出场阈值)

作者: Raymond Hsiao (refactored)
来源: strategy_factory 自动生成 + 人工重构为 Signal/Factor 架构
重构时间: 2025-04

架构说明:
    Strategy (CtaTemplate) -> Signal (交易逻辑) -> Factor (指标计算)
    
    三层分离优势:
    1. 指标计算与交易执行解耦，方便单元测试
    2. 信号逻辑独立，可复用于多个策略
    3. UI变量与核心逻辑分离，实盘监控更清晰
"""
from datetime import time
from typing import Callable, Optional

from vnpy.trader.object import BarData, TickData, TradeData, OrderData
from vnpy_ctastrategy import StopOrder
from vnpy.trader.constant import Interval
from vnpy_ctastrategy import (
    CtaTemplate,
    BarGenerator,
    ArrayManager,
)


# =============================================================================
# 策略主类 - 负责交易执行与UI交互
# =============================================================================
class PullbackMrStrategy(CtaTemplate):
    """
    回调均值回归策略主类
    
    职责:
    - 接收行情数据 (on_tick/on_bar)
    - 调用 Signal 计算交易目标
    - 执行下单逻辑 (send_orders)
    - 记录运行日志 (write_log)
    - 更新UI变量 (put_event)
    
    交易逻辑:
    - 入场: 上升趋势 + 短期回调 + RSI超卖
    - 出场: RSI回升到阈值之上
    """
    
    author = "Raymond Hsiao (refactored)"

    # === 策略参数 ===
    sma_200_period = 200
    sma_20_period = 20
    rsi_period = 5
    rsi_entry = 45
    rsi_exit = 65
    fixed_size = 1
    price_add_rate = 0.0
    daily_end_minute = 59

    parameters = [
        "sma_200_period", "sma_20_period",
        "rsi_period", "rsi_entry", "rsi_exit",
        "fixed_size", "price_add_rate", "daily_end_minute",
    ]
    signal_parameters = [
        "sma_200_period", "sma_20_period",
        "rsi_period", "rsi_entry", "rsi_exit", "daily_end_minute",
    ]
    position_parameters = [
        "fixed_size", "price_add_rate",
    ]

    # === 运行变量 ===
    target: int = 0
    sma200_value: float = 0.0
    sma20_value: float = 0.0
    rsi_value: float = 0.0
    trading_signal: str = ""

    variables = ["target", "sma200_value", "sma20_value", "rsi_value", "trading_signal"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        
        self.bg = BarGenerator(self.on_bar)
        self.daily_bg = DailyBarGenerator(self.on_daily_bar, time(14, self.daily_end_minute))
        
        key = get_product_name(vt_symbol)
        contract_size = cta_engine.get_size(self) if hasattr(cta_engine, 'get_size') else 1
        self.write_log(
            f"[初始化] 回调均值回归策略 | 品种: {key}, 合约乘数: {contract_size}, 参数: {setting}"
        )
        
        self.signal = PullbackMrSignal(
            vt_symbol=vt_symbol,
            sma_200_period=self.sma_200_period,
            sma_20_period=self.sma_20_period,
            rsi_period=self.rsi_period,
            rsi_entry=self.rsi_entry,
            rsi_exit=self.rsi_exit,
            fixed_size=self.fixed_size,
            daily_end_minute=self.daily_end_minute,
        )
        self.target = 0
        
        self.daily_bar_count: int = 0
        self.last_daily_dt = None

    def on_init(self):
        self.write_log(
            f"[on_init] 策略初始化，SMA200窗口: {self.sma_200_period}, "
            f"SMA20窗口: {self.sma_20_period}, RSI窗口: {self.rsi_period}, "
            f"RSI入场: {self.rsi_entry}, RSI出场: {self.rsi_exit}, "
            f"end_time: {time(14, self.daily_end_minute)}"
        )
        self.load_bar(200)

    def on_start(self):
        self.write_log("[on_start] 策略启动")

    def on_stop(self):
        self.write_log(f"[on_stop] 策略停止 | 当前持仓: {self.pos}")

    def on_tick(self, tick: TickData) -> None:
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        self.cancel_all()

        prev_target = self.target

        self.calculate_targets(bar)
        self._update_ui_variables()

        dt_str = bar.datetime.strftime("%Y-%m-%d %H:%M:%S")
        if self.target != prev_target:
            if self.target > 0:
                self.trading_signal = "BUY"
                self.write_log(
                    f"[{dt_str}] [信号] 回调均值回归做多 | "
                    f"Close: {bar.close_price:.2f}, "
                    f"SMA200: {self.sma200_value:.2f}, SMA20: {self.sma20_value:.2f}, "
                    f"RSI({self.rsi_period}): {self.rsi_value:.1f} < {self.rsi_entry}, "
                    f"手数: {self.fixed_size}"
                )
            else:
                self.trading_signal = "EXIT_LONG"
                self.write_log(
                    f"[{dt_str}] [信号] 平仓 | "
                    f"RSI({self.rsi_period}): {self.rsi_value:.1f} > {self.rsi_exit}"
                )
        else:
            if self.target > 0:
                self.trading_signal = "HOLD"
            else:
                self.trading_signal = ""

        self.send_orders(bar, dt_str)
        self.put_event()

    def on_daily_bar(self, bar: BarData) -> None:
        self.daily_bar_count += 1
        self.last_daily_dt = bar.datetime
        
        dt_str = bar.datetime.strftime('%Y-%m-%d')
        
        self.write_log(
            f"[日线到达] #{self.daily_bar_count} | "
            f"日期={dt_str} | "
            f"O={bar.open_price:.2f} H={bar.high_price:.2f} "
            f"L={bar.low_price:.2f} C={bar.close_price:.2f} | "
            f"V={bar.volume}"
        )
        
        self.cancel_all()
        
        prev_target = self.target
        
        self.calculate_targets(bar)
        
        self._update_ui_variables()
        
        if self.target != prev_target:
            self._log_signal_change(dt_str, bar, prev_target)
        
        self.send_orders(bar, dt_str)
        self.put_event()

    def _update_ui_variables(self):
        self.sma200_value = self.signal.factor.sma200_value
        self.sma20_value = self.signal.factor.sma20_value
        self.rsi_value = self.signal.factor.rsi_value

    def _log_signal_change(self, dt_str: str, bar: BarData, prev_target: int):
        if self.target > 0:
            self.trading_signal = "BUY"
            self.write_log(
                f"[{dt_str}] [信号变化] 做多入场 | "
                f"Close={bar.close_price:.2f} > SMA200={self.sma200_value:.2f} | "
                f"Close < SMA20={self.sma20_value:.2f} | "
                f"RSI({self.rsi_period})={self.rsi_value:.1f} < {self.rsi_entry} | "
                f"prev_target={prev_target} -> target={self.target}"
            )
        elif self.target == 0 and prev_target > 0:
            self.trading_signal = "EXIT_LONG"
            self.write_log(
                f"[{dt_str}] [信号变化] 平多出场 | "
                f"RSI({self.rsi_period})={self.rsi_value:.1f} > {self.rsi_exit} | "
                f"价格={bar.close_price:.2f} | "
                f"prev_target={prev_target} -> target={self.target}"
            )
        else:
            self.trading_signal = "HOLD"

    def calculate_targets(self, bar: BarData) -> None:
        self.signal.on_bar(bar)
        self.target = self.signal.get_target()

    def send_orders(self, bar: BarData, dt_str: str = None) -> None:
        if dt_str is None:
            dt_str = bar.datetime.strftime("%Y-%m-%d %H:%M:%S")
            
        if self.target == self.pos:
            return
        
        if self.target > self.pos:
            diff = self.target - self.pos
            price = bar.close_price * (1.0 + self.price_add_rate)
            if self.trading:
                self.write_log(f"[{dt_str}] [下单] 开多仓 | 价格: {price:.2f}, 数量: {diff}")
            self.buy(price, diff)
        else:
            diff = self.pos - self.target
            price = bar.close_price * (1.0 - self.price_add_rate)
            if self.trading:
                self.write_log(f"[{dt_str}] [下单] 平多仓 | 价格: {price:.2f}, 数量: {diff}")
            self.sell(price, diff)

    def on_trade(self, trade: TradeData) -> None:
        if abs(self.pos) <= 1e-7:
            self.write_log(f"[碎单处理] 持仓量 {self.pos} 清零")
            self.pos = 0
        
        direction = '多' if trade.direction.value == '多' else '空'
        offset = trade.offset.value
        
        self.write_log(
            f"[成交] 方向={direction} | "
            f"开平={offset} | "
            f"价格={trade.price:.2f} | "
            f"数量={trade.volume} | "
            f"最新持仓={self.pos}"
        )
        self.put_event()

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass


# =============================================================================
# 信号类 - 封装交易逻辑
# =============================================================================
class PullbackMrSignal:
    """
    回调均值回归策略信号处理器
    
    职责:
    - 接收Bar数据（分钟级或日线）
    - 调用 Factor 计算技术指标
    - 根据指标值生成交易信号 (target)
    """
    
    def __init__(
        self,
        vt_symbol: str,
        sma_200_period: int,
        sma_20_period: int,
        rsi_period: int,
        rsi_entry: float,
        rsi_exit: float,
        fixed_size: int,
        daily_end_minute: int,
    ):
        self.vt_symbol = vt_symbol
        self.sma_200_period = sma_200_period
        self.sma_20_period = sma_20_period
        self.rsi_period = rsi_period
        self.rsi_entry = rsi_entry
        self.rsi_exit = rsi_exit
        self.fixed_size = fixed_size
        self.daily_end_minute = daily_end_minute
        
        self.factor = PullbackMrFactor(
            sma_200_period=sma_200_period,
            sma_20_period=sma_20_period,
            rsi_period=rsi_period,
            rsi_entry=rsi_entry,
            rsi_exit=rsi_exit,
            fixed_size=fixed_size,
            daily_end_minute=daily_end_minute,
        )
    
    def on_bar(self, bar: BarData) -> None:
        self.factor.on_bar(bar)
    
    def on_daily_bar(self, bar: BarData) -> None:
        self.factor.on_daily_bar(bar)
    
    def get_target(self) -> int:
        return self.factor.get_target()


# =============================================================================
# 因子类 - 封装指标计算
# =============================================================================
class PullbackMrFactor:
    """
    回调均值回归策略因子计算器
    
    职责:
    - 管理ArrayManager（K线数据缓存）
    - 计算SMA200, SMA20, RSI
    - 判断交易信号
    """
    
    def __init__(
        self,
        sma_200_period: int,
        sma_20_period: int,
        rsi_period: int,
        rsi_entry: float,
        rsi_exit: float,
        fixed_size: int,
        daily_end_minute: int,
    ):
        self.sma_200_period = sma_200_period
        self.sma_20_period = sma_20_period
        self.rsi_period = rsi_period
        self.rsi_entry = rsi_entry
        self.rsi_exit = rsi_exit
        self.fixed_size = fixed_size
        self.daily_end_minute = daily_end_minute
        
        self.am = ArrayManager(size=max(sma_200_period, sma_20_period, rsi_period) + 50)
        
        self.bg = DailyBarGenerator(self.on_daily_bar, time(14, daily_end_minute))
        
        self.target = 0
        self.sma200_value = 0.0
        self.sma20_value = 0.0
        self.rsi_value = 0.0
    
    def on_bar(self, bar: BarData) -> None:
        if bar.interval == Interval.DAILY:
            self.on_daily_bar(bar)
        else:
            self.bg.update_bar(bar)
    
    def on_daily_bar(self, bar: BarData) -> None:
        self.am.update_bar(bar)
        
        if not self.am.inited:
            return
        
        close = self.am.close[-1]
        
        # 计算指标
        sma200_arr = self.am.sma(self.sma_200_period, array=True)
        sma20_arr = self.am.sma(self.sma_20_period, array=True)
        rsi_arr = self.am.rsi(self.rsi_period, array=True)
        
        self.sma200_value = sma200_arr[-1]
        self.sma20_value = sma20_arr[-1]
        self.rsi_value = rsi_arr[-1]
        
        # 持仓状态
        if self.target > 0:
            if self.rsi_value > self.rsi_exit:
                self.target = 0
            else:
                self.target = self.fixed_size
            return
        
        # 空仓状态
        if self.target == 0:
            if close > self.sma200_value and close < self.sma20_value and self.rsi_value < self.rsi_entry:
                self.target = self.fixed_size
            else:
                self.target = 0
    
    def get_target(self) -> int:
        return self.target


# =============================================================================
# 工具函数
# =============================================================================
def get_product_name(vt_symbol: str) -> str:
    """从合约代码提取品种名称（去除数字部分）"""
    if '.' in vt_symbol:
        symbol = vt_symbol.split('.')[0]
    else:
        symbol = vt_symbol
    product = ''.join([c for c in symbol if not c.isdigit()])
    return product.upper()


class DailyBarGenerator:
    """
    日线Bar生成器
    
    将分钟级Bar数据合成为日线Bar，
    用于日内策略的日线判断。
    """
    
    def __init__(self, on_daily_bar: Callable[[BarData], None], end_time: time) -> None:
        self.on_daily_bar: Callable[[BarData], None] = on_daily_bar
        self.end_time: time = end_time
        self.daily_bar: Optional[BarData] = None
    
    def update_bar(self, bar: BarData) -> None:
        """更新分钟Bar到日线Bar"""
        if not self.daily_bar:
            self.daily_bar = BarData(
                gateway_name=bar.gateway_name,
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=bar.datetime.replace(hour=0, minute=0),
                interval=Interval.DAILY,
                volume=bar.volume,
                turnover=bar.turnover,
                open_interest=bar.open_interest,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
            )
        else:
            d = self.daily_bar
            d.volume += bar.volume
            d.turnover += bar.turnover
            d.open_interest = bar.open_interest
            d.high_price = max(d.high_price, bar.high_price)
            d.low_price = min(d.low_price, bar.low_price)
            d.close_price = bar.close_price
            d.datetime = bar.datetime
        if bar.datetime.time() == self.end_time:
            self.daily_bar.datetime = self.daily_bar.datetime.replace(hour=0, minute=0)
            self.on_daily_bar(self.daily_bar)
            self.daily_bar = None
