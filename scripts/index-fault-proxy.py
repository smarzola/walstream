#!/usr/bin/env python3
"""Disposable HTTP S3 proxy: hold a selected root PUT before publication."""
import argparse
import http.client
import http.server
from pathlib import Path
import threading
from urllib.parse import urlsplit

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--listen", required=True)
parser.add_argument("--upstream", required=True)
parser.add_argument("--marker", required=True)
parser.add_argument("--suffix", required=True)
args = parser.parse_args()
upstream = urlsplit(args.upstream)
assert upstream.scheme == "http", "development HTTP endpoints only"
host, port = args.listen.rsplit(":", 1)
assert host == "127.0.0.1", "loopback only"


class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def forward(self):
        # object_store supplies fixed-length request bodies. Reject an unknown
        # framing mode rather than silently forwarding a partial signed body.
        if self.headers.get("Transfer-Encoding"):
            self.send_error(501, "chunked requests unsupported in fault probe")
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.command == "PUT" and urlsplit(self.path).path.endswith(args.suffix):
            Path(args.marker).write_text("root PUT body received; not forwarded\n")
            threading.Event().wait()  # harness kills this owned proxy
            return
        connection = http.client.HTTPConnection(upstream.hostname, upstream.port, timeout=30)
        try:
            # Preserve the signed Host header while connecting to the backend.
            connection.request(self.command, self.path, body=body, headers=dict(self.headers))
            response = connection.getresponse()
            payload = response.read()
            self.send_response(response.status)
            for name, value in response.getheaders():
                if name.lower() not in {"transfer-encoding", "content-length", "connection"}:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            connection.close()

    do_GET = do_HEAD = do_PUT = do_DELETE = do_POST = forward


http.server.ThreadingHTTPServer((host, int(port)), Proxy).serve_forever()
