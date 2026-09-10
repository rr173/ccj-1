"""模拟被开放接口保护的真实上游服务（标准库实现，无第三方依赖）。

  /v1/hello 等任意路径  正常业务：200
  /fail                 模拟上游故障：500（数据面应把这次占用退回，额度不扣）
  /slow?ms=N            模拟慢上游：N 毫秒后才 200（用于观察“占用中”的状态）
"""
from __future__ import annotations

import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        self._dispatch()

    def _dispatch(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/fail":
            self._reply(500, {"upstream": "error", "path": self.path})
            return
        if parsed.path == "/slow":
            ms = int((parse_qs(parsed.query).get("ms") or ["1000"])[0])
            time.sleep(min(ms, 30000) / 1000)
            self._reply(200, {"upstream": "ok", "path": self.path, "slept_ms": ms})
            return
        self._reply(200, {"upstream": "ok", "path": self.path})

    def _reply(self, status: int, obj: dict) -> None:
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:  # 安静一点
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
