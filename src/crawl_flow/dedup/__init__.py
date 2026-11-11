"""内容指纹与去重模块。

独立于采集管道的可选组件：只读 ``records`` 表，指纹与去重标记写入
模块自有数据表，不修改原有入库逻辑、不删除任何原始数据。
"""

from crawl_flow.dedup.config import DedupConfig, load_dedup_config
from crawl_flow.dedup.fingerprint import Fingerprint, compute_fingerprint
from crawl_flow.dedup.service import DedupService, DedupSummary
from crawl_flow.dedup.store import DedupStore

__all__ = [
    "DedupConfig",
    "DedupService",
    "DedupStore",
    "DedupSummary",
    "Fingerprint",
    "compute_fingerprint",
    "load_dedup_config",
]
