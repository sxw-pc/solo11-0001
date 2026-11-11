"""配置文件热重载：仅使用 Python 标准库轮询文件内容。

不依赖 watchdog 等第三方库；轮询间隔由调用方控制，检测到变更时重新执行
与首次加载相同的 YAML + pydantic 校验。重载失败不会清空当前内存配置。
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from pathlib import Path

from crawl_flow.config import SourceConfig, load_sources
from crawl_flow.errors import ConfigError

LOGGER_NAME = "crawl_flow.config_watch"

ConfigLoader = Callable[[str | Path], list[SourceConfig]]


class ConfigWatcher:
    """按文件内容哈希判断配置是否变化，并在变化时安全重载。"""

    def __init__(
        self,
        path: str | Path,
        *,
        loader: ConfigLoader | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.path = Path(path)
        self._loader: ConfigLoader = loader or load_sources
        self._logger = logger or logging.getLogger(LOGGER_NAME)
        self._signature: str | None = self._current_signature()
        self._sources: list[SourceConfig] = []

    @property
    def sources(self) -> list[SourceConfig]:
        """当前一份可用配置的只读副本。"""

        return list(self._sources)

    def load(self) -> list[SourceConfig]:
        """首次加载；配置非法时原样抛出 :class:`ConfigError`。"""

        sources = list(self._loader(self.path))
        self._signature = self._current_signature()
        self._sources = sources
        return self.sources

    def poll(self) -> list[SourceConfig] | None:
        """检测并处理一次配置变更。

        - 文件未变：返回 ``None``；
        - 文件变更且重载成功：返回新的源列表，并替换内存中的配置；
        - 文件变更但重载失败：输出 WARNING，保留旧配置并返回 ``None``。
        """

        signature = self._current_signature()
        if signature == self._signature:
            return None

        # 先记录新签名，避免文件持续失败时每个轮询周期重复输出相同警告。
        self._signature = signature
        try:
            sources = list(self._loader(self.path))
        except ConfigError as exc:
            self._logger.warning(
                "config reload failed path=%s error=%s keep_previous=true",
                self.path,
                exc,
            )
            return None

        self._sources = sources
        self._logger.info(
            "config reloaded path=%s sources=%d",
            self.path,
            len(sources),
        )
        return self.sources

    def _current_signature(self) -> str | None:
        """读取文件 SHA-256；文件缺失时返回固定哨兵值。

        用内容哈希而不是仅用 mtime/size，可以避开不同平台时间戳精度差异，
        也能识别同秒内、相同大小的覆盖写入。
        """

        try:
            content = self.path.read_bytes()
        except OSError:
            return None
        return hashlib.sha256(content).hexdigest()


__all__ = ["ConfigWatcher"]
