#!/bin/bash
set -euo pipefail

# 获取脚本所在目录的绝对路径
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# 默认配置
PYTHON_EXE="${PYTHON_EXE:-python3}"
ALLOW_INITIAL=""
DRY_RUN=""

# 解析参数
while [[ $# -gt 0 ]]; do
    case "$1" in
        --allow-initial)
            ALLOW_INITIAL="--allow-initial"
            shift
            ;;
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --allow-initial   首次运行时使用（无 prev targets 也继续生成订单）"
            echo "  --dry-run         仅打印结果，不写入文件"
            echo "  -h, --help        显示此帮助信息"
            echo ""
            echo "Examples:"
            echo "  $0                              # 标准流程：生成 targets + 生成订单 CSV"
            echo "  $0 --allow-initial              # 首次运行，允许以空仓为基准生成订单"
            echo "  $0 --dry-run                    # 仅预览，不写入任何文件"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use '$0 --help' for usage information."
            exit 1
            ;;
    esac
done

cd "${SCRIPT_DIR}"

echo "========================================"
echo "Step 1/2: 生成目标仓位 (generate_targets.py)"
echo "========================================"
${PYTHON_EXE} "${SCRIPT_DIR}/generate_targets.py" ${DRY_RUN}

# 检查 prev targets 是否存在（避免首次误开仓）
PREV_TARGETS="${PROJECT_ROOT}/output/targets_prev.json"
if [[ -z "${ALLOW_INITIAL}" && ! -f "${PREV_TARGETS}" ]]; then
    echo ""
    echo "ERROR: prev targets not found: ${PREV_TARGETS}"
    echo "       为避免误开仓，已终止。"
    echo "       请先至少运行两次 generate_targets.py，"
    echo "       或在首次运行时加 --allow-initial 参数。"
    exit 2
fi

echo ""
echo "========================================"
echo "Step 2/2: 生成 AlgoTrading CSV (generate_orders_csv.py)"
echo "========================================"
${PYTHON_EXE} "${SCRIPT_DIR}/generate_orders_csv.py" --algo TwapAlgo ${ALLOW_INITIAL} ${DRY_RUN}

echo ""
echo "========================================"
echo "全部完成"
echo "========================================"
