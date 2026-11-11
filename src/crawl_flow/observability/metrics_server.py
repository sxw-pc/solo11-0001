"""指标暴露 HTTP 端点：stdlib http.server 实现，默认仅监听本机回环。

不引入任何 Web 框架；只响应 ``GET /metrics``（Prometheus 文本格式）与
``GET /healthz``（存活探针），其余路径一律 404。端点不输出任何配置信息，
默认绑定 ``127.0.0.1``，只接受本机访问。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from crawl_flow.observability.metrics import CONTENT_TYPE

LOGGER = logging.getLogger("crawl_flow.metrics_server")


class MetricsServer:
    """后台线程运行的 /metrics 服务。

    ``render`` 是无参可调用对象，每次请求时调用以获取最新的 Prometheus 文本。
    """

    def __init__(self, host: str, port: int, render: Callable[[], str]) -> None:
        self.host = host
        self._render = render
        handler_class = _make_handler(render)
        self._server = ThreadingHTTPServer((host, port), handler_class)
        self._server.daemon_threads = True
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        """实际监听的端口（传入 0 时由系统分配）。"""

        return int(self._server.server_address[1])

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="crawl-metrics-server",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info("metrics endpoint: http://%s:%d/metrics", self.host, self.port)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


def _make_handler(render: Callable[[], str]) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        server_version = "crawl-flow-metrics/0.1"

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler 约定
            path = self.path.split("?", 1)[0]
            if path == "/metrics":
                self._respond(200, render(), CONTENT_TYPE)
            elif path == "/healthz":
                self._respond(200, "ok\n", "text/plain; charset=utf-8")
            else:
                self._respond(404, "not found\n", "text/plain; charset=utf-8")

        def _respond(self, status: int, body: str, content_type: str) -> None:
            payload = body.encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                # 客户端提前断开不算服务异常。
                pass

        def log_message(self, format: str, *args: object) -> None:
            # 访问日志降级为 DEBUG，避免污染调度进程的标准输出。
            LOGGER.debug("metrics http %s", format % args)

    return _Handler
