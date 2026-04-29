from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from generate_orders_csv import main


if __name__ == "__main__":
    raise SystemExit(main())
