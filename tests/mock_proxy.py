"""测试用轻量 HTTP 代理与故障代理。

仅用于 pytest：实现 HTTP 正向代理的转发逻辑，并支持通过模式开关模拟
连接异常（拒绝连接的端口）、代理错误响应（502/403）等场景，
避免测试依赖任何外部代理或公网。
"""

from __future__ import annotations

import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class _ProxyHandler(BaseHTTPRequestHandler):
    server_version = "crawl-flow-test-proxy/1.0"

    @property
    def mode(self) -> str:
        return self.server.mode  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # 保持测试输出安静，失败计数由服务器对象自身维护。
        return

    def _record(self) -> None:
        self.server.requests.append(self.path)  # type: ignore[attr-defined]

    def do_GET(self) -> None:  # noqa: N802
        self._record()
        mode = self.mode
        if mode == "502":
            self.send_error(502, "Bad Gateway (simulated)")
            return
        if mode == "403":
            self.send_error(403, "Forbidden (simulated)")
            return
        if mode == "detail-502" and "detail-fail" in self.path:
            # 只对详情请求返回 502，列表页照常转发，模拟代理链路局部故障。
            self.send_error(502, "Bad Gateway on detail (simulated)")
            return

        # 转发模式：用绝对 URI 里的目标信息重新发起请求。
        try:
            upstream = http.client.HTTPConnection(self._upstream_host())
            upstream.request(
                "GET",
                self._upstream_path(),
                headers=self._forward_headers(),
            )
            response = upstream.getresponse()
            body = response.read()
        except OSError as exc:
            self.send_error(502, f"Upstream unavailable: {exc}")
            return

        self.send_response(response.status, response.reason)
        for key, value in response.getheaders():
            if key.lower() not in HOP_BY_HOP_HEADERS:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _upstream_host(self) -> str:
        host = self.headers.get("Host", "")
        return host.split(",")[0].strip()

    def _upstream_path(self) -> str:
        # BaseHTTPRequestHandler.path 在代理模式下是绝对 URI。
        marker = self.path.find("/", self.path.find("://") + 3)
        return self.path[marker:] if marker != -1 else "/"

    def _forward_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        for key, value in self.headers.items():
            if key.lower() not in HOP_BY_HOP_HEADERS:
                headers[key] = value
        return headers


class MockProxy:
    """带生命周期管理的测试代理，暴露代理 URL 与收到的请求记录。"""

    def __init__(self, mode: str = "forward") -> None:
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _ProxyHandler)
        self._server.mode = mode  # type: ignore[attr-defined]
        self._server.requests = []  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def request_count(self) -> int:
        return len(self._server.requests)  # type: ignore[attr-defined]

    def start(self) -> "MockProxy":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def dead_proxy_url() -> str:
    """返回一个保证拒绝连接的本地代理地址（未监听端口）。"""

    probe = ThreadingHTTPServer(("127.0.0.1", 0), _ProxyHandler)
    port = probe.server_address[1]
    probe.server_close()
    return f"http://127.0.0.1:{port}"
