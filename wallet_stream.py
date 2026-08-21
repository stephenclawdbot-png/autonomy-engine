"""
Wallet Stream - Sub-second wallet monitoring over Solana WebSocket RPC.

The polling WalletWatcher sees a trade up to poll_seconds + RPC lag
after it lands. For a scalper whose edge decays in seconds, that delay
is the difference between a copyable signal and a stale one. This
module replaces polling with a push subscription:

    logsSubscribe {mentions: [wallet]}  ->  signature arrives on commit
    -> one HTTP getTransaction fetch    ->  same signal pipeline

Implements a minimal RFC 6455 WebSocket client over the standard
library (TLS socket + handshake + frame codec), including CONNECT
tunneling when an HTTPS proxy is configured, so there are still no
external dependencies.

StreamingWalletWatcher subclasses copy_signal_engine.WalletWatcher:
the filters, risk manager, and paper book are identical — only the
transport changes. On stream failure it falls back to polling, so it
is a strict upgrade.

Usage:
    python wallet_stream.py <wallet>            # stream signals

    from wallet_stream import StreamingWalletWatcher
    StreamingWalletWatcher(wallet, on_signal=...).run()
"""

import base64
import json
import os
import secrets
import socket
import ssl
import struct
import time
from typing import Callable, Optional
from urllib.parse import urlparse
import logging

from copy_signal_engine import WalletWatcher, SignalConfig, Signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WalletStream")

DEFAULT_WS = "wss://api.mainnet-beta.solana.com"


class WebSocketClient:
    """Minimal RFC 6455 client: TLS, proxy CONNECT, text frames, ping."""

    def __init__(self, url: str, timeout: float = 15.0):
        self.url = url
        self.timeout = timeout
        self.sock: Optional[ssl.SSLSocket] = None
        self._buffer = b""

    # -- connection ----------------------------------------------------------

    def connect(self) -> None:
        parsed = urlparse(self.url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/"

        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        if proxy:
            p = urlparse(proxy)
            raw = socket.create_connection((p.hostname, p.port or 8080),
                                           timeout=self.timeout)
            connect_req = (f"CONNECT {host}:{port} HTTP/1.1\r\n"
                           f"Host: {host}:{port}\r\n\r\n").encode()
            raw.sendall(connect_req)
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = raw.recv(4096)
                if not chunk:
                    raise ConnectionError("proxy closed during CONNECT")
                resp += chunk
            status = resp.split(b"\r\n", 1)[0]
            if b" 200" not in status:
                raise ConnectionError(f"proxy CONNECT failed: {status!r}")
        else:
            raw = socket.create_connection((host, port), timeout=self.timeout)

        if parsed.scheme == "wss":
            cafile = os.environ.get("SSL_CERT_FILE")
            if not cafile and os.path.exists("/root/.ccr/ca-bundle.crt"):
                cafile = "/root/.ccr/ca-bundle.crt"
            ctx = ssl.create_default_context(cafile=cafile)
            self.sock = ctx.wrap_socket(raw, server_hostname=host)
        else:
            self.sock = raw

        key = base64.b64encode(secrets.token_bytes(16)).decode()
        handshake = (f"GET {path} HTTP/1.1\r\n"
                     f"Host: {host}\r\n"
                     "Upgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     f"Sec-WebSocket-Key: {key}\r\n"
                     "Sec-WebSocket-Version: 13\r\n\r\n").encode()
        self.sock.sendall(handshake)
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("closed during WS handshake")
            resp += chunk
        head, _, rest = resp.partition(b"\r\n\r\n")
        if b" 101" not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"WS handshake rejected: "
                                  f"{head.splitlines()[0]!r}")
        self._buffer = rest
        logger.info("WebSocket connected to %s", self.url)

    # -- frame codec ---------------------------------------------------------

    def _recv_exact(self, n: int) -> bytes:
        while len(self._buffer) < n:
            try:
                chunk = self.sock.recv(65536)
            except (socket.timeout, ssl.SSLError) as exc:
                if isinstance(exc, ssl.SSLError) and \
                        "timed out" not in str(exc).lower():
                    raise
                raise TimeoutError("recv timed out") from exc
            if not chunk:
                raise ConnectionError("socket closed")
            self._buffer += chunk
        out, self._buffer = self._buffer[:n], self._buffer[n:]
        return out

    def ping(self) -> None:
        self._send_control(0x9, b"ka")

    def send_text(self, text: str) -> None:
        payload = text.encode()
        mask = secrets.token_bytes(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        header = bytearray([0x81])          # FIN + text opcode
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        self.sock.sendall(bytes(header) + mask + masked)

    def _send_control(self, opcode: int, payload: bytes = b"") -> None:
        mask = secrets.token_bytes(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes([0x80 | opcode, 0x80 | len(payload)])
                          + mask + masked)

    def recv_message(self) -> Optional[str]:
        """Next complete text message, transparently answering pings.
        Returns None on a clean close."""
        fragments = []
        while True:
            # A timeout here is a clean frame boundary (nothing consumed:
            # _recv_exact only pops complete reads) -> safe to retry later.
            b1, b2 = self._recv_exact(2)
            opcode = b1 & 0x0F
            fin = b1 & 0x80
            length = b2 & 0x7F
            try:
                if length == 126:
                    length = struct.unpack(">H", self._recv_exact(2))[0]
                elif length == 127:
                    length = struct.unpack(">Q", self._recv_exact(8))[0]
                if b2 & 0x80:                # masked server frame (rare)
                    mask = self._recv_exact(4)
                    data = bytes(b ^ mask[i % 4] for i, b in
                                 enumerate(self._recv_exact(length)))
                else:
                    data = self._recv_exact(length)
            except TimeoutError as exc:
                # Header already consumed: resuming would desync the
                # framing, so force a reconnect instead.
                raise ConnectionError("timeout mid-frame") from exc

            if opcode == 0x9:                # ping -> pong
                self._send_control(0xA, data)
                continue
            if opcode == 0xA:                # pong
                continue
            if opcode == 0x8:                # close
                try:
                    self._send_control(0x8)
                finally:
                    self.close()
                return None
            fragments.append(data)
            if fin:
                return b"".join(fragments).decode("utf-8", "replace")

    def close(self) -> None:
        if self.sock:
            try:
                self.sock.close()
            finally:
                self.sock = None


class StreamingWalletWatcher(WalletWatcher):
    """WalletWatcher with a push transport. Same pipeline, less latency.

    Falls back to the inherited polling loop if the stream cannot be
    established or dies repeatedly.
    """

    def __init__(self, wallet: str, config: Optional[SignalConfig] = None,
                 ws_url: str = DEFAULT_WS,
                 on_signal: Optional[Callable[[Signal], None]] = None,
                 max_stream_failures: int = 5):
        super().__init__(wallet, config, on_signal=on_signal)
        self.ws_url = ws_url
        self.max_stream_failures = max_stream_failures

    def _handle_signature(self, signature: str) -> None:
        if signature in self._seen_set:
            return
        self._seen_set.add(signature)
        self.seen_signatures.append(signature)
        tx = self.rpc.get_transaction(signature)
        if tx is None:
            return
        parsed = self.profiler._parse_transaction(self.wallet, tx)
        if parsed is None:
            return
        trades, _programs, _ts = parsed
        for trade in trades:
            self._apply(self._evaluate(trade))
        self.enforce_time_stops()

    def run(self, duration_seconds: Optional[float] = None) -> None:
        started = time.time()
        failures = 0
        while not self._stop.is_set():
            if duration_seconds and time.time() - started > duration_seconds:
                return
            ws = WebSocketClient(self.ws_url)
            try:
                ws.connect()
                ws.send_text(json.dumps({
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": [self.wallet]},
                               {"commitment": "confirmed"}]}))
                ack = ws.recv_message()
                logger.info("Streaming %s (sub ack: %s)",
                            self.wallet, (ack or "")[:80])
                failures = 0
                while not self._stop.is_set():
                    if (duration_seconds
                            and time.time() - started > duration_seconds):
                        return
                    try:
                        msg = ws.recv_message()
                    except TimeoutError:
                        # Quiet wallet: keep the connection alive and
                        # re-check the stop/duration conditions.
                        ws.ping()
                        self.enforce_time_stops()
                        continue
                    if msg is None:
                        raise ConnectionError("stream closed")
                    payload = json.loads(msg)
                    value = (payload.get("params", {})
                             .get("result", {}).get("value", {}))
                    sig = value.get("signature")
                    if sig and value.get("err") is None:
                        self._handle_signature(sig)
            except Exception as exc:
                failures += 1
                logger.warning("stream error (%d/%d): %s", failures,
                               self.max_stream_failures, exc)
                if failures >= self.max_stream_failures:
                    logger.warning("falling back to polling transport")
                    return super().run(duration_seconds)
                time.sleep(min(2 ** failures, 30))
            finally:
                ws.close()


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else \
        "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS"
    watcher = StreamingWalletWatcher(target)
    try:
        watcher.run()
    except KeyboardInterrupt:
        watcher.stop()
