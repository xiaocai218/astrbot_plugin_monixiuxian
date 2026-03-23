"""认证管理器：密码、Web Token、聊天绑定密钥（SQLite 持久化）。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import time
from typing import Optional

import aiosqlite

TOKEN_EXPIRY = 7 * 24 * 3600  # 7 天（秒）


def _hash_password(password: str, salt: str = "") -> str:
    """SHA-256 加盐哈希。"""
    if not salt:
        salt = secrets.token_hex(8)
    h = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return f"{salt}${h}"


def _verify_password(password: str, stored_hash: str) -> bool:
    """验证密码是否匹配。"""
    if "$" not in stored_hash:
        return False
    salt, _ = stored_hash.split("$", 1)
    return _hash_password(password, salt) == stored_hash


class AuthManager:
    """管理 Web 登录 Token 和聊天绑定密钥（SQLite 持久化）。"""

    def __init__(self, db: aiosqlite.Connection, data_dir: str = ""):
        self._db = db
        self._data_dir = data_dir
        # 内存缓存（读优化）
        self._web_tokens: dict[str, dict] = {}
        self._bind_keys: dict[str, dict] = {}
        self._chat_bindings: dict[str, dict] = {}
        # 上次绑定失败的错误信息
        self.last_bind_error: str = ""
        # 串行化所有认证状态写入，防止并发修改+save()互相踩踏
        self._auth_lock = asyncio.Lock()

    async def initialize(self):
        """建表并加载数据到内存缓存。"""
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS web_tokens (
                token      TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS bind_keys (
                key        TEXT PRIMARY KEY,
                user_id    TEXT NOT NULL,
                expires_at REAL NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS chat_bindings (
                chat_user_id TEXT PRIMARY KEY,
                user_id      TEXT NOT NULL,
                expires_at   REAL NOT NULL
            )
        """)
        await self._db.commit()
        # 迁移旧 JSON 数据（如果存在）
        await self._migrate_json_data()
        # 加载到内存
        await self._load_from_db()
        self._cleanup_expired()

    async def _migrate_json_data(self):
        """若存在旧 auth_tokens.json 且数据库表未完全迁移，则自动迁移。"""
        if not self._data_dir:
            return
        json_file = os.path.join(self._data_dir, "auth_tokens.json")
        if not os.path.exists(json_file):
            return
        # 仅当三张表都已有数据时跳过迁移；否则补迁移缺失数据
        table_counts: list[int] = []
        for table in ("web_tokens", "bind_keys", "chat_bindings"):
            async with self._db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
                row = await cur.fetchone()
                table_counts.append(int(row[0]) if row else 0)
        if all(count > 0 for count in table_counts):
            return
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                content = f.read()
            if not content.strip():
                return
            data = json.loads(content)
            for token, info in data.get("web_tokens", {}).items():
                await self._db.execute(
                    "INSERT OR IGNORE INTO web_tokens (token, user_id, expires_at) VALUES (?, ?, ?)",
                    (token, info["user_id"], info["expires_at"]),
                )
            for key, info in data.get("bind_keys", {}).items():
                await self._db.execute(
                    "INSERT OR IGNORE INTO bind_keys (key, user_id, expires_at) VALUES (?, ?, ?)",
                    (key, info["user_id"], info["expires_at"]),
                )
            for cid, info in data.get("chat_bindings", {}).items():
                await self._db.execute(
                    "INSERT OR IGNORE INTO chat_bindings (chat_user_id, user_id, expires_at) VALUES (?, ?, ?)",
                    (cid, info["user_id"], info["expires_at"]),
                )
            await self._db.commit()
            backup = json_file + ".bak"
            if not os.path.exists(backup):
                os.rename(json_file, backup)
        except Exception:
            pass

    async def _load_from_db(self):
        """从数据库加载所有认证数据到内存。"""
        self._web_tokens.clear()
        async with self._db.execute("SELECT token, user_id, expires_at FROM web_tokens") as cur:
            async for row in cur:
                self._web_tokens[row[0]] = {"user_id": row[1], "expires_at": row[2]}

        self._bind_keys.clear()
        async with self._db.execute("SELECT key, user_id, expires_at FROM bind_keys") as cur:
            async for row in cur:
                self._bind_keys[row[0]] = {"user_id": row[1], "expires_at": row[2]}

        self._chat_bindings.clear()
        async with self._db.execute("SELECT chat_user_id, user_id, expires_at FROM chat_bindings") as cur:
            async for row in cur:
                self._chat_bindings[row[0]] = {"user_id": row[1], "expires_at": row[2]}

    async def _save_unlocked(self):
        """将内存缓存同步到数据库（调用方需已持有 _auth_lock）。"""
        self._cleanup_expired()
        # web_tokens
        await self._db.execute("DELETE FROM web_tokens")
        for token, info in self._web_tokens.items():
            await self._db.execute(
                "INSERT INTO web_tokens (token, user_id, expires_at) VALUES (?, ?, ?)",
                (token, info["user_id"], info["expires_at"]),
            )
        # bind_keys
        await self._db.execute("DELETE FROM bind_keys")
        for key, info in self._bind_keys.items():
            await self._db.execute(
                "INSERT INTO bind_keys (key, user_id, expires_at) VALUES (?, ?, ?)",
                (key, info["user_id"], info["expires_at"]),
            )
        # chat_bindings
        await self._db.execute("DELETE FROM chat_bindings")
        for cid, info in self._chat_bindings.items():
            await self._db.execute(
                "INSERT INTO chat_bindings (chat_user_id, user_id, expires_at) VALUES (?, ?, ?)",
                (cid, info["user_id"], info["expires_at"]),
            )
        await self._db.commit()

    async def save(self):
        """将内存缓存同步到数据库。"""
        async with self._auth_lock:
            await self._save_unlocked()

    # ==================== 密码 ====================

    @staticmethod
    def hash_password(password: str) -> str:
        return _hash_password(password)

    @staticmethod
    def verify_password(password: str, stored_hash: str) -> bool:
        return _verify_password(password, stored_hash)

    # ==================== Web Token ====================

    async def create_web_token(self, user_id: str) -> str:
        """为玩家生成 Web 登录 Token（7天有效）。"""
        async with self._auth_lock:
            # 清除该玩家旧 token
            self._web_tokens = {
                k: v for k, v in self._web_tokens.items()
                if v["user_id"] != user_id
            }
            token = secrets.token_urlsafe(32)
            self._web_tokens[token] = {
                "user_id": user_id,
                "expires_at": time.time() + TOKEN_EXPIRY,
            }
            await self._save_unlocked()
            return token

    def verify_web_token(self, token: str) -> Optional[str]:
        """验证 Web Token，返回 user_id 或 None。"""
        info = self._web_tokens.get(token)
        if not info:
            return None
        if time.time() > info["expires_at"]:
            return None
        return info["user_id"]

    # ==================== 聊天绑定密钥 ====================

    async def create_bind_key(self, user_id: str) -> str:
        """为玩家生成6位数绑定密钥（7天有效）。"""
        async with self._auth_lock:
            # 清除该玩家旧密钥
            self._bind_keys = {
                k: v for k, v in self._bind_keys.items()
                if v["user_id"] != user_id
            }
            # 生成不重复的6位数
            while True:
                key = str(secrets.randbelow(900000) + 100000)
                if key not in self._bind_keys:
                    break
            self._bind_keys[key] = {
                "user_id": user_id,
                "expires_at": time.time() + TOKEN_EXPIRY,
            }
            await self._save_unlocked()
            return key

    async def verify_bind_key(self, key: str, chat_user_id: str) -> Optional[str]:
        """验证绑定密钥，成功则创建聊天绑定，返回 user_id。"""
        async with self._auth_lock:
            self.last_bind_error = ""
            info = self._bind_keys.get(key)
            if not info:
                self.last_bind_error = "密钥无效或已过期"
                return None
            if time.time() > info["expires_at"]:
                del self._bind_keys[key]
                self.last_bind_error = "密钥无效或已过期"
                await self._save_unlocked()
                return None
            user_id = info["user_id"]
            # 消耗密钥
            del self._bind_keys[key]
            # 检查该角色是否已被其他QQ绑定
            now = time.time()
            for cid, binding in self._chat_bindings.items():
                if binding["user_id"] == user_id and cid != chat_user_id:
                    if now <= binding["expires_at"]:
                        self.last_bind_error = "该角色已绑定其他QQ，不可重复绑定"
                        await self._save_unlocked()
                        return None
            # 创建聊天绑定
            self._chat_bindings[chat_user_id] = {
                "user_id": user_id,
                "expires_at": time.time() + TOKEN_EXPIRY,
            }
            await self._save_unlocked()
            return user_id

    # ==================== 聊天绑定查询 ====================

    def get_player_id_for_chat(self, chat_user_id: str) -> Optional[str]:
        """根据聊天用户ID获取绑定的玩家ID。"""
        info = self._chat_bindings.get(chat_user_id)
        if not info:
            return None
        if time.time() > info["expires_at"]:
            return None
        return info["user_id"]

    async def unbind_chat(self, chat_user_id: str):
        """解除聊天绑定。"""
        async with self._auth_lock:
            if self._chat_bindings.pop(chat_user_id, None) is not None:
                await self._save_unlocked()

    async def revoke_user(self, user_id: str):
        """清除指定玩家的所有认证数据。"""
        await self.revoke_users([user_id])

    async def revoke_users(self, user_ids: list[str]):
        """批量清除玩家认证数据。"""
        async with self._auth_lock:
            target_ids = {str(uid) for uid in user_ids if uid}
            if not target_ids:
                return

            old_web = len(self._web_tokens)
            old_keys = len(self._bind_keys)
            old_chat = len(self._chat_bindings)

            self._web_tokens = {
                token: info
                for token, info in self._web_tokens.items()
                if info.get("user_id") not in target_ids
            }
            self._bind_keys = {
                key: info
                for key, info in self._bind_keys.items()
                if info.get("user_id") not in target_ids
            }
            self._chat_bindings = {
                chat_id: info
                for chat_id, info in self._chat_bindings.items()
                if info.get("user_id") not in target_ids
            }

            changed = (
                len(self._web_tokens) != old_web
                or len(self._bind_keys) != old_keys
                or len(self._chat_bindings) != old_chat
            )
            if changed:
                await self._save_unlocked()

    async def clear_all(self):
        """清空全部认证数据。"""
        async with self._auth_lock:
            if not self._web_tokens and not self._bind_keys and not self._chat_bindings:
                return
            self._web_tokens.clear()
            self._bind_keys.clear()
            self._chat_bindings.clear()
            await self._save_unlocked()

    # ==================== 内部 ====================

    def _cleanup_expired(self):
        """清除所有过期数据。"""
        now = time.time()
        self._web_tokens = {
            k: v for k, v in self._web_tokens.items()
            if v["expires_at"] > now
        }
        self._bind_keys = {
            k: v for k, v in self._bind_keys.items()
            if v["expires_at"] > now
        }
        self._chat_bindings = {
            k: v for k, v in self._chat_bindings.items()
            if v["expires_at"] > now
        }
