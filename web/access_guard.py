"""访问控制与反爬统计（HTTP + WebSocket 共用）。"""

from __future__ import annotations

from collections import deque
import hashlib
import hmac
import ipaddress
import secrets
import time


class AccessGuard:
    """统一访问控制器：限流、封禁、访问统计。"""

    def __init__(self):
        self._stats: dict[str, dict] = {}
        self._buckets: dict[str, deque[float]] = {}
        self._auto_block_until: dict[str, float] = {}
        self._auto_block_reason: dict[str, str] = {}
        self._manual_block_until: dict[str, float] = {}
        self._manual_block_reason: dict[str, str] = {}
        self._page_sessions: dict[str, dict] = {}
        self._page_session_by_client: dict[str, str] = {}

    @staticmethod
    def normalize_ip(raw: str) -> str:
        """将字符串规范化为 IP，失败返回空串。"""
        value = str(raw or "").strip().strip("\"'")
        if not value:
            return ""
        if "," in value:
            value = value.split(",", 1)[0].strip()
        if value.lower().startswith("for="):
            value = value[4:].strip().strip("\"'")
        if value.startswith("[") and "]" in value:
            value = value[1:value.index("]")]
        if value.count(":") == 1 and "." in value:
            host, port = value.rsplit(":", 1)
            if port.isdigit():
                value = host
        try:
            ipaddress.ip_address(value)
            return value[:64]
        except ValueError:
            return ""

    @staticmethod
    def _is_public_ip(ip: str) -> bool:
        """是否公网可路由 IP。"""
        try:
            return bool(ipaddress.ip_address(ip).is_global)
        except ValueError:
            return False

    @staticmethod
    def _identity_key(ip: str, ua: str, *, public_ip: bool) -> str:
        """
        生成限流身份键。
        - 公网 IP：按真实 IP 维度限流。
        - 内网/未知 IP：附加 UA 指纹，避免代理出口 IP 导致“连坐”。
        """
        if public_ip:
            return ip
        ua_raw = str(ua or "").strip().lower()
        if not ua_raw:
            return f"{ip}|ua:none"
        ua_hash = hashlib.md5(ua_raw.encode("utf-8")).hexdigest()[:12]
        return f"{ip}|ua:{ua_hash}"

    def _ensure_stat(self, ip: str) -> dict:
        stat = self._stats.get(ip)
        if stat:
            return stat
        stat = {
            "ip": ip,
            "access_count": 0,
            "http_count": 0,
            "ws_connect_count": 0,
            "ws_message_count": 0,
            "last_seen": 0.0,
            "last_path": "",
            "last_ua": "",
        }
        self._stats[ip] = stat
        return stat

    def _touch(self, ip: str, channel: str, path: str = "", ua: str = ""):
        now = time.time()
        stat = self._ensure_stat(ip)
        stat["access_count"] += 1
        if channel == "http":
            stat["http_count"] += 1
        elif channel == "ws_connect":
            stat["ws_connect_count"] += 1
        elif channel == "ws_message":
            stat["ws_message_count"] += 1
        stat["last_seen"] = now
        if path:
            stat["last_path"] = path[:128]
        if ua:
            stat["last_ua"] = ua[:200]

    @staticmethod
    def _page_signature(secret: str, page_id: str, issued_at: int) -> str:
        raw = f"{page_id}|{int(issued_at)}".encode("utf-8")
        return hmac.new(str(secret or "").encode("utf-8"), raw, hashlib.sha256).hexdigest()

    def _page_client_key(self, ip: str, ua: str, client_key: str = "") -> str:
        provided = str(client_key or "").strip()
        if provided:
            return f"page:{provided[:96]}"
        public_ip = self._is_public_ip(ip)
        return self._identity_key(ip, ua, public_ip=public_ip)

    def _cleanup_page_sessions(self, now: float | None = None):
        current = float(now if now is not None else time.time())
        dead_keys = [
            page_id
            for page_id, session in self._page_sessions.items()
            if float(session.get("expires_at", 0.0) or 0.0) <= current
        ]
        for page_id in dead_keys:
            session = self._page_sessions.pop(page_id, None)
            if not session:
                continue
            client_key = str(session.get("client_key", ""))
            if client_key and self._page_session_by_client.get(client_key) == page_id:
                self._page_session_by_client.pop(client_key, None)

    def issue_page_session(
        self,
        *,
        secret: str,
        ip: str,
        ua: str,
        client_key: str = "",
        ttl_seconds: int = 21600,
    ) -> dict:
        """签发页面级访问凭证；同一客户端再次打开页面会顶掉旧凭证。"""
        now = int(time.time())
        ttl = max(60, int(ttl_seconds or 0))
        self._cleanup_page_sessions(now)
        client_key_value = self._page_client_key(ip, ua, client_key=client_key)
        old_page_id = self._page_session_by_client.get(client_key_value, "")
        if old_page_id:
            self._page_sessions.pop(old_page_id, None)

        page_id = secrets.token_urlsafe(18)
        expires_at = now + ttl
        self._page_sessions[page_id] = {
            "page_id": page_id,
            "issued_at": now,
            "expires_at": expires_at,
            "ttl_seconds": ttl,
            "client_key": client_key_value,
        }
        self._page_session_by_client[client_key_value] = page_id
        return {
            "enabled": True,
            "page_id": page_id,
            "issued_at": now,
            "signature": self._page_signature(secret, page_id, now),
        }

    def validate_page_session(
        self,
        *,
        secret: str,
        page_id: str,
        issued_at: int | str,
        signature: str,
        ip: str,
        ua: str,
        client_key: str = "",
    ) -> tuple[bool, str]:
        """校验页面级访问凭证，成功时自动续期。"""
        secret_value = str(secret or "").strip()
        if not secret_value:
            return True, ""

        try:
            issued_at_int = int(issued_at)
        except (TypeError, ValueError):
            return False, "页面凭证缺失或格式错误"

        page_id_value = str(page_id or "").strip()
        signature_value = str(signature or "").strip()
        if not page_id_value or not signature_value:
            return False, "页面凭证缺失，请刷新页面"

        now = int(time.time())
        self._cleanup_page_sessions(now)
        session = self._page_sessions.get(page_id_value)
        if not session:
            return False, "页面凭证已失效，请刷新页面"

        expected_signature = self._page_signature(secret_value, page_id_value, issued_at_int)
        if not secrets.compare_digest(signature_value, expected_signature):
            return False, "页面凭证无效，请刷新页面"
        if issued_at_int != int(session.get("issued_at", 0) or 0):
            return False, "页面凭证无效，请刷新页面"

        client_key_value = self._page_client_key(ip, ua, client_key=client_key)
        if client_key_value != str(session.get("client_key", "")):
            return False, "页面凭证与当前访问环境不匹配"
        if self._page_session_by_client.get(client_key_value) != page_id_value:
            return False, "当前链接已在别处重新打开，请刷新页面"

        ttl = max(60, int(session.get("ttl_seconds", 21600) or 21600))
        session["expires_at"] = now + ttl
        return True, ""

    def _manual_active(self, ip: str, now: float) -> bool:
        if ip not in self._manual_block_reason:
            return False
        until = float(self._manual_block_until.get(ip, 0.0) or 0.0)
        if until > 0 and until <= now:
            self._manual_block_until.pop(ip, None)
            self._manual_block_reason.pop(ip, None)
            return False
        return True

    def _auto_active(self, ip: str, now: float) -> bool:
        until = float(self._auto_block_until.get(ip, 0.0) or 0.0)
        if until <= 0:
            return False
        if until <= now:
            self._auto_block_until.pop(ip, None)
            self._auto_block_reason.pop(ip, None)
            return False
        return True

    def _is_blocked(self, ip: str, now: float) -> tuple[bool, str]:
        if self._manual_active(ip, now):
            reason = self._manual_block_reason.get(ip, "该IP已被管理员封禁")
            return True, reason or "该IP已被管理员封禁"
        if self._auto_active(ip, now):
            reason = self._auto_block_reason.get(ip, "请求过于频繁，请稍后再试")
            return True, reason or "请求过于频繁，请稍后再试"
        return False, ""

    def _check_rate(
        self,
        *,
        bucket_key: str,
        now: float,
        limit: int,
        window: float,
        burst_count: int,
        burst_window: float,
    ) -> bool:
        bucket = self._buckets.setdefault(bucket_key, deque())
        while bucket and now - bucket[0] > window:
            bucket.popleft()
        bucket.append(now)

        if len(bucket) >= burst_count and now - bucket[-burst_count] <= burst_window:
            return False
        return len(bucket) <= limit

    def _auto_block(self, ip: str, seconds: float, reason: str):
        now = time.time()
        until = now + max(1.0, float(seconds or 0.0))
        prev = float(self._auto_block_until.get(ip, 0.0) or 0.0)
        if until > prev:
            self._auto_block_until[ip] = until
        self._auto_block_reason[ip] = reason or "请求过于频繁，请稍后再试"
        self._ensure_stat(ip)["last_seen"] = now

    def check_http(
        self,
        *,
        ip: str,
        path: str,
        ua: str,
        limit: int,
        window: float,
        burst_count: int,
        burst_window: float,
        block_seconds: float,
    ) -> tuple[bool, str]:
        now = time.time()
        self._touch(ip, "http", path=path, ua=ua)
        public_ip = self._is_public_ip(ip)
        identity_key = self._identity_key(ip, ua, public_ip=public_ip)
        blocked, reason = self._is_blocked(ip, now)
        if blocked:
            return False, reason
        ok = self._check_rate(
            bucket_key=f"http|{identity_key}|{path}",
            now=now,
            limit=limit,
            window=window,
            burst_count=burst_count,
            burst_window=burst_window,
        )
        if ok:
            return True, ""
        self._auto_block(ip, block_seconds, "请求过于频繁，请稍后再试")
        return False, "请求过于频繁，请稍后再试"

    def check_ws_connect(
        self,
        *,
        ip: str,
        limit: int,
        window: float,
        block_seconds: float,
    ) -> tuple[bool, str]:
        now = time.time()
        self._touch(ip, "ws_connect")
        public_ip = self._is_public_ip(ip)
        if public_ip:
            blocked, reason = self._is_blocked(ip, now)
            if blocked:
                return False, reason
        else:
            self._auto_block_until.pop(ip, None)
            self._auto_block_reason.pop(ip, None)
            limit = max(limit, 120)
        ok = self._check_rate(
            bucket_key=f"ws_connect|{ip}",
            now=now,
            limit=limit,
            window=window,
            burst_count=max(3, min(limit, 8)),
            burst_window=max(1.0, min(window, 6.0)),
        )
        if ok:
            return True, ""
        if public_ip:
            self._auto_block(ip, block_seconds, "连接过于频繁，请稍后再试")
        return False, "连接过于频繁，请稍后再试"

    def check_ws_message(
        self,
        *,
        ip: str,
        limit: int,
        window: float,
        burst_count: int,
        burst_window: float,
        block_seconds: float,
    ) -> tuple[bool, str]:
        now = time.time()
        self._touch(ip, "ws_message")
        public_ip = self._is_public_ip(ip)
        if public_ip:
            blocked, reason = self._is_blocked(ip, now)
            if blocked:
                return False, reason
        else:
            self._auto_block_until.pop(ip, None)
            self._auto_block_reason.pop(ip, None)
            limit = max(limit, 120)
            burst_count = max(burst_count, limit + 1)
        ok = self._check_rate(
            bucket_key=f"ws_msg|{ip}",
            now=now,
            limit=limit,
            window=window,
            burst_count=burst_count,
            burst_window=burst_window,
        )
        if ok:
            return True, ""
        if public_ip:
            self._auto_block(ip, block_seconds, "请求过于频繁，请稍后再试")
        return False, "请求过于频繁，请稍后再试"

    def manual_block(self, ip: str, seconds: int = 0, reason: str = "") -> bool:
        target = self.normalize_ip(ip)
        if not target:
            return False
        now = time.time()
        ttl = int(seconds or 0)
        until = now + ttl if ttl > 0 else 0.0
        self._manual_block_until[target] = until
        self._manual_block_reason[target] = str(reason or "该IP已被管理员封禁").strip()
        self._ensure_stat(target)["last_seen"] = now
        return True

    def manual_unblock(self, ip: str) -> bool:
        target = self.normalize_ip(ip)
        if not target:
            return False
        self._manual_block_until.pop(target, None)
        self._manual_block_reason.pop(target, None)
        self._auto_block_until.pop(target, None)
        self._auto_block_reason.pop(target, None)
        return True

    def list_ips(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        keyword: str = "",
        blocked_only: bool = False,
    ) -> dict:
        now = time.time()
        kw = str(keyword or "").strip().lower()
        rows: list[dict] = []
        for ip, stat in self._stats.items():
            manual_active = self._manual_active(ip, now)
            auto_active = self._auto_active(ip, now)
            blocked = manual_active or auto_active
            if blocked_only and not blocked:
                continue
            if kw and kw not in ip.lower() and kw not in str(stat.get("last_path", "")).lower():
                continue
            blocked_until = 0.0
            reason = ""
            if manual_active:
                blocked_until = float(self._manual_block_until.get(ip, 0.0) or 0.0)
                reason = self._manual_block_reason.get(ip, "")
            elif auto_active:
                blocked_until = float(self._auto_block_until.get(ip, 0.0) or 0.0)
                reason = self._auto_block_reason.get(ip, "")
            rows.append({
                "ip": ip,
                "access_count": int(stat.get("access_count", 0)),
                "http_count": int(stat.get("http_count", 0)),
                "ws_connect_count": int(stat.get("ws_connect_count", 0)),
                "ws_message_count": int(stat.get("ws_message_count", 0)),
                "last_seen": float(stat.get("last_seen", 0.0) or 0.0),
                "last_path": str(stat.get("last_path", "")),
                "blocked": blocked,
                "manual_blocked": manual_active,
                "auto_blocked": auto_active,
                "blocked_until": blocked_until,
                "block_reason": reason,
            })

        rows.sort(
            key=lambda x: (
                0 if x.get("blocked") else 1,
                -int(x.get("access_count", 0)),
                -float(x.get("last_seen", 0.0)),
            )
        )
        p_size = max(1, min(200, int(page_size or 20)))
        p = max(1, int(page or 1))
        total = len(rows)
        total_pages = max(1, (total + p_size - 1) // p_size)
        if p > total_pages:
            p = total_pages
        start = (p - 1) * p_size
        end = start + p_size
        return {
            "list": rows[start:end],
            "total": total,
            "page": p,
            "page_size": p_size,
            "total_pages": total_pages,
        }


_ACCESS_GUARD = AccessGuard()


def get_access_guard() -> AccessGuard:
    return _ACCESS_GUARD
