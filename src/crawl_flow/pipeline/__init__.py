"""采集管道。"""

from crawl_flow.pipeline.correlation import CorrelationResult, Correlator, coerce_join_value
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
    "CorrelationResult",
    "Correlator",
    "ItemOutcome",
    "RunSummary",
    "clean_multiline",
    "clean_text",
    "coerce_join_value",
    "html_to_text",
    "normalize_record",
    "parse_datetime",
    "process_item",
    "run_source",
    "split_tags",
]
