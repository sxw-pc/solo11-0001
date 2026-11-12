"""采集管道。"""

from crawl_flow.pipeline.diff import (
    STATUS_ADDED,
    STATUS_ANOMALY,
    STATUS_MODIFIED,
    STATUS_UNCHANGED,
    DiffEntry,
    DiffReport,
    FieldChange,
    compare_snapshot,
)
from crawl_flow.pipeline.normalize import (
    clean_multiline,
    clean_text,
    html_to_text,
    normalize_record,
    parse_datetime,
    split_tags,
)
from crawl_flow.pipeline.run import ItemOutcome, RunSummary, process_item, run_source

__all__ = [
    "DiffEntry",
    "DiffReport",
    "FieldChange",
    "ItemOutcome",
    "RunSummary",
    "STATUS_ADDED",
    "STATUS_ANOMALY",
    "STATUS_MODIFIED",
    "STATUS_UNCHANGED",
    "clean_multiline",
    "clean_text",
    "compare_snapshot",
    "html_to_text",
    "normalize_record",
    "parse_datetime",
    "process_item",
    "run_source",
    "split_tags",
]
