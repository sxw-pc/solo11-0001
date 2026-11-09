"""日志配置：标准库 logging + key=value 结构化 formatter。"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False
FORMAT = "%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


class KeyValueFormatter(logging.Formatter):
    """输出 ``timestamp level=... logger=... key=value ...`` 的单行日志。"""

    def __init__(self) -> None:
        super().__init__(fmt=FORMAT, datefmt=DATE_FORMAT)


def configure_logging(verbose: bool = False) -> logging.Logger:
    """初始化根日志器，重复调用不会叠加 handler。

    默认 INFO；``verbose=True`` 时 DEBUG。日志统一走 stderr，避免污染
    命令的标准输出（例如 ``db stats`` 的表格）。
    """

    global _CONFIGURED
    root = logging.getLogger()
    if not _CONFIGURED:
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setFormatter(KeyValueFormatter())
        root.addHandler(handler)
        _CONFIGURED = True
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    # 抑制 httpx 每请求一条的 INFO 日志，保留本项目的单条采集日志。
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return logging.getLogger("crawl_flow")


def log_item(
    logger: logging.Logger,
    source: str,
    external_id: str,
    status: str,
    elapsed_ms: int,
) -> None:
    """每条采集结果固定输出一行，便于日志检索。"""

    logger.info(
        "source=%s external_id=%s status=%s elapsed_ms=%d",
        source,
        external_id,
        status,
        elapsed_ms,
    )
