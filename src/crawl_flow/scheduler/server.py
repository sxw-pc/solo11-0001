"""指标暴露端点：标准库 http.server，仅允许内部访问。

- 只暴露 ``GET /metrics`` 与 ``GET /healthz``，其余路径一律 404；
- 监听地址默认回环 ``127.0.0.1``，即使显式配置也拒绝公网地址，只允许
  回环与 RFC1918 私网段，保证端点不会暴露到公网；
- 输出仅含指标名、源名称、触发方式等运维标签，不包含任何源配置、URL、
  请求头或凭据信息。
"""

from __future__ import annotations

import ipaddress
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from crawl_flow.observability.metrics import CONTENT_TYPE, MetricsRegistry

LOGGER_NAME = "crawl_flow.scheduler.server"

# 显式允许的内网网段：回环 + RFC1918 + 链路本地
_ALLOWED_NETWORKS = (
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
)


def ensure_internal_host(host: str) -> str:
    """校验监听地址属于回环或内网，拒绝任何公网绑定。"""

    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError(f"指标端点监听地址必须是 IP 字面量: {host!r}") from exc
    if not any(address in network for network in _ALLOWED_NETWORKS):
        raise ValueError(
            f"指标端点仅允许监听回环或内网地址，拒绝公网地址 {host}"
        )
    return host


def _is_internal_client(ip_text: str) -> bool:
    try:
        address = ipaddress.ip_address(ip_text)
    except ValueError:
        return False
    return any(address in network for network in _ALLOWED_NETWORKS)


class MetricsServer:
    """线程化 HTTP 指标服务，:meth:`start` 非阻塞，:meth:`stop` 优雅关闭。"""

    def __init__(
        self,
        registry: MetricsRegistry,
        *,
        host: str = "127.0.0.1",
        port: int = 9108,
        logger: logging.Logger | None = None,
    ) -> None:
        ensure_internal_host(host)
        self._registry = registry
        self._host = host
        self._port = port
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        if self._httpd is None:
            return self._port
        return int(self._httpd.server_address[1])

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self.port}"

    def start(self) -> None:
        registry = self._registry
        log = self._log

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                log.debug("metrics endpoint %s - %s", self.address_string(), format % args)

            def _reject_if_external(self) -> bool:
                client_ip = self.client_address[0]
                if _is_internal_client(client_ip):
                    return False
                self.send_error(403, "metrics endpoint is internal only")
                log.warning("rejected external metrics client ip=%s", client_ip)
                return True

            def do_GET(self) -> None:  # noqa: N802 - http.server 约定方法名
                if self._reject_if_external():
                    return
                path = self.path.split("?", 1)[0]
                if path == "/metrics":
                    payload = registry.render_prometheus().encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", CONTENT_TYPE)
                    self.send_header("Content-Length", str(len(payload)))
                    # 禁止代理缓存指标响应
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(payload)
                elif path == "/healthz":
                    payload = b"ok\n"
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                else:
                    self.send_error(404, "not found")

        self._httpd = ThreadingHTTPServer((self._host, self._port), _Handler)
        self._thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="crawl-flow-metrics-http",
            daemon=True,
        )
        self._thread.start()
        self._log.info("metrics endpoint listening on %s", self.url)

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
