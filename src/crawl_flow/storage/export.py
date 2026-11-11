"""统计结果 CSV 导出：仅依赖标准库，兼容中文与特殊字符。"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from crawl_flow.errors import StoreError

CSV_HEADER = (
    "dimension",
    "dimension_value",
    "record_count",
    "last_updated_at",
    "raw_total",
    "raw_failed",
    "raw_failed_ratio",
)


def write_dimension_csv(path: str | Path, result: dict[str, Any]) -> Path:
    """把 :meth:`Database.dimension_stats` 的结果写成 CSV。

    - 使用 ``utf-8-sig`` 编码，Excel 直接打开中文不乱码；
    - 标准库 :class:`csv.writer` 以 QUOTE_MINIMAL 自动转义逗号、引号、
      换行等特殊字符；
    - 无匹配数据时仍写出只含表头的文件，由调用方决定如何提示。
    """

    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(CSV_HEADER)
            for row in result.get("rows", []):
                last_updated = row["last_updated_at"]
                writer.writerow(
                    [
                        row["dimension"],
                        row["dimension_value"],
                        row["record_count"],
                        last_updated.isoformat(sep=" ") if last_updated else "",
                        row["raw_total"],
                        row["raw_failed"],
                        f"{row['raw_failed_ratio']:.4f}",
                    ]
                )
    except OSError as exc:
        raise StoreError(f"CSV 导出失败 path={target} error={exc}") from exc
    return target
