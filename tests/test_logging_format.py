"""观测层回归：key=value 结构化日志格式不允许被破坏。"""

from __future__ import annotations

import io
import logging

from crawl_flow.observability.logging import FORMAT, KeyValueFormatter, configure_logging, log_item


def test_key_value_formatter_emits_structured_line() -> None:
    formatter = KeyValueFormatter()
    record = logging.LogRecord(
        name="crawl_flow.pipeline",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="source=%s external_id=%s status=%s elapsed_ms=%d",
        args=("demo_api", "api-1", "ok", 42),
        exc_info=None,
    )
    line = formatter.format(record)
    assert "level=INFO" in line
    assert "logger=crawl_flow.pipeline" in line
    assert "source=demo_api external_id=api-1 status=ok elapsed_ms=42" in line
    # 前缀模板保持不变。
    assert FORMAT == "%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"


def test_configure_logging_writes_key_value_to_stderr() -> None:
    stream = io.StringIO()
    logger = logging.getLogger("crawl_flow.kv.test")
    handler = logging.StreamHandler(stream)
    handler.setFormatter(KeyValueFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        log_item(logger, "demo_html", "html-7", "failed", 9)
    finally:
        logger.removeHandler(handler)
    line = stream.getvalue().strip()
    assert "level=INFO" in line
    assert "logger=crawl_flow.kv.test" in line
    assert "source=demo_html external_id=html-7 status=failed elapsed_ms=9" in line


def test_configure_logging_verbose_switches_level() -> None:
    assert configure_logging(False).name == "crawl_flow"
    root = logging.getLogger()
    assert root.level == logging.INFO
    configure_logging(True)
    assert root.level == logging.DEBUG
    configure_logging(False)
    assert root.level == logging.INFO
