## cta_offline（离线生成多策略净仓位交易清单）

目标：作为独立项目，在本目录内管理策略与参数配置（仅依赖 vnpy 框架），并离线生成交易清单：

- 收盘后用米筐（rqdatac）888 指数日线复刻策略信号逻辑与参数（目前支持）
  - `TripleRsiLongShortStrategy`（原 TRS）
  - `PullbackMrStrategy`（回调均值回归）
- 读取本项目 `config/cta_strategy_setting.json` 中的策略配置（每个策略可独立配置参数与品种）
- 可选读取本项目 `config/triple_rsi_filters.json` 外部过滤结果
- 生成“目标仓位清单”（JSON/CSV），并按品种汇总多策略 target 得到净仓位（用于交易清单）
- 进一步生成 AlgoTrading 可导入的 CSV（用于批量启动算法下单）

### 目录结构

- `tools/`：离线生成器（命令行脚本）
- `vnpy_scripts/`：保留（如果未来仍想走 ScriptTrader），当前流程不需要
- `src/trs_offline/`：可复用的逻辑模块
- `output/`：生成结果默认输出目录（可改）
- `config/`：策略与过滤配置（本项目内统一管理）

### 前置条件

- 本机已可用 rqdatac（米筐），并能成功拉取 `*888` 日线数据
- 若要在 vn.py 内自动执行，还需安装并启用 ScriptTrader（vnpy_scripttrader）

### 命名约定（长期维护）

- 策略级输出：`records`（每条记录对应一个策略实例）
- 组合级输出：`portfolio_records`（按品种汇总后的净仓位，用于生成交易清单）
- 文件命名：
  - `output\targets_latest.json`：最新目标仓位（含 records + portfolio_records）
  - `output\targets_prev.json`：上一份目标仓位（用于生成调仓单）
  - `output\algotrading_*.csv`：AlgoTrading 导入用交易清单
- 脚本命名：
  - `generate_targets.py`：生成 targets（收盘后运行）
  - `generate_orders_csv.py`：由 targets diff 生成 AlgoTrading CSV（开盘前运行）
  - 旧脚本名（`generate_trs_targets.py` / `generate_algotrading_csv.py`）保留为兼容入口，内部会转到新脚本执行

### 1) 生成目标仓位清单（收盘后运行）

在 PowerShell 里运行：

```powershell
python tools\generate_targets.py --dry-run
```

去掉 `--dry-run` 会在 `output/` 写入文件。

### 2) 生成 AlgoTrading CSV（用于导入 AlgoTrading 批量启动）

先生成 targets（步骤 1），然后生成 AlgoTrading CSV：

```powershell
python tools\generate_orders_csv.py --algo TwapAlgo
```

默认读取：

- 当前 targets：`output\targets_latest.json`
- 上一份 targets：`output\targets_prev.json`（由 generate_targets.py 在每次覆盖 latest 之前自动备份生成）

生成逻辑：

- `targets_latest.json` 同时包含：
  - `records`：逐策略输出（便于排查每个策略的信号与参数）
  - `portfolio_records`：按品种汇总后的净仓位（用于生成交易清单）
- `generate_algotrading_csv.py` 优先使用 `portfolio_records` 生成交易清单；若不存在则回退为对 `records` 做按品种汇总。

输出：

- `output\algotrading_TwapAlgo_latest.csv`（GBK 编码，适配 AlgoTrading 的 CSV 启动读取方式）

如需首次生成（没有 prev targets），可以显式允许以“上一份=0”作为基准：

```powershell
python tools\generate_orders_csv.py --algo TwapAlgo --allow-initial
```

在 vn.py 的 AlgoTrading 模块中，先选择对应算法（例如 TWAP 时间加权平均），再点击【CSV启动】导入该 CSV。

也可以直接运行批处理（先生成 targets，再生成 AlgoTrading CSV）：

`tools\run_generate_algotrading_csv.bat`

注意：第一次运行时还没有 `targets_prev.json`，请至少先运行两次 `generate_targets.py`（形成上一份 targets），再生成 AlgoTrading CSV；否则批处理会直接退出以避免误开仓。
