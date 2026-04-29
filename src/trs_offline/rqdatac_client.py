from __future__ import annotations

import os
from typing import Any


def init_rqdatac() -> Any:
    try:
        import rqdatac
    except ImportError as exc:
        raise RuntimeError("未安装 rqdatac，请先安装并配置米筐客户端") from exc

    try:
        from vnpy.trader.datafeed import get_datafeed

        datafeed = get_datafeed()
        datafeed.init()
    except Exception:
        pass

    try:
        from rqdatac.client import get_client

        get_client()
        return rqdatac
    except Exception:
        pass

    uri = os.getenv("RQDATAC_URI", "").strip()
    if uri:
        rqdatac.init(uri=uri)
        return rqdatac

    username = (os.getenv("RQDATA_USERNAME", "") or os.getenv("RQDATAC_USERNAME", "")).strip()
    password = (os.getenv("RQDATA_PASSWORD", "") or os.getenv("RQDATAC_PASSWORD", "")).strip()
    addr = os.getenv("RQDATAC_ADDR", "").strip()

    if username and password:
        if addr:
            host = addr
            port = ""
            if ":" in addr:
                host, port = addr.rsplit(":", 1)
            rqdatac.init(username, password, addr=(host, int(port)) if port else host)
            return rqdatac
        rqdatac.init(username, password)
        return rqdatac

    raise RuntimeError(
        "rqdatac 未初始化。请先配置米筐客户端，或设置环境变量 RQDATAC_URI，"
        "或设置 RQDATA_USERNAME/RQDATA_PASSWORD（可选 RQDATAC_ADDR=host:port）。"
    )
