from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DefaultPaths:
    cta_strategy_setting: Path
    triple_rsi_filters: Path
    output_dir: Path


def get_default_paths() -> DefaultPaths:
    project_root = Path(__file__).resolve().parents[2]
    config_dir = project_root / "config"
    return DefaultPaths(
        cta_strategy_setting=config_dir / "cta_strategy_setting.json",
        triple_rsi_filters=config_dir / "triple_rsi_filters.json",
        output_dir=project_root / "output",
    )
