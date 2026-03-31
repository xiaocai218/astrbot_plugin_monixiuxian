"""WebSocket 连接管理与消息协议。"""

from __future__ import annotations

import asyncio
import logging
import re
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..game.constants import get_realm_name
from ..game.engine import GameEngine
from ..game.models import Player
from ..game.sect import ROLE_NAMES
from .access_guard import get_access_guard

ws_logger = logging.getLogger("xiuxian.websocket")

RANKINGS_PUSH_DELAY = 0.6
MARKET_PUSH_DELAY = 0.4
MARKET_PAGE_SIZE = 9

# ── 世界频道常量 ────────────────────────────────────────
WORLD_CHAT_MAX_HISTORY = 100
WORLD_CHAT_MAX_LEN = 100
WORLD_CHAT_COOLDOWN = 3.0  # 秒
WORLD_CHAT_MAX_AGE = 30 * 24 * 3600  # 1个月（秒）
WORLD_CHAT_CLEANUP_INTERVAL = 6 * 3600  # 每6小时清理一次
_RE_CHINESE_ONLY = re.compile(
    r"^[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef\u2000-\u206f\u0020-\u0040\u005b-\u0060\u007b-\u007e"
    r"\u3400-\u4dbf\U00020000-\U0002a6df\U0002a700-\U0002b73f"
    r"0-9a-zA-Z]+$"
)
_PENDING_DEATH_ALLOWED_TYPES = {
    "death_confirm_keep",
    "get_announcements",
    "get_inventory",
    "get_market",
    "get_my_listings",
    "get_panel",
    "get_rankings",
    "get_shop",
    "get_world_chat_history",
    "dungeon_state",
    "market_fee_preview",
    "pvp_state",
}


class ConnectionManager:
    """管理 WebSocket 连接。"""

    def __init__(self, engine: GameEngine):
        self._engine = engine
        self._connections: dict[str, WebSocket] = {}  # user_id -> websocket
        self._market_pages: dict[str, int] = {}
        self._market_my_watchers: set[str] = set()
        self._rankings_dirty = False
        self._rankings_flush_task: asyncio.Task | None = None
        self._market_dirty = False
        self._market_flush_task: asyncio.Task | None = None
        # 世界频道
        self._world_chat_cooldowns: dict[str, float] = {}  # user_id -> last_send_ts
        self._chat_cleanup_task: asyncio.Task | None = None

    async def connect(self, user_id: str, websocket: WebSocket):
        self._connections[user_id] = websocket

    def disconnect(self, user_id: str):
        self._connections.pop(user_id, None)
        self._market_pages.pop(user_id, None)
        self._market_my_watchers.discard(user_id)

    def online_count(self) -> int:
        return len(self._connections)

    async def send_to_player(self, user_id: str, data: dict):
        ws = self._connections.get(user_id)
        if ws:
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(user_id)

    async def broadcast(self, data: dict, exclude_user_id: str | None = None):
        """向所有在线连接广播消息。"""
        dead_users: list[str] = []
        for uid, ws in list(self._connections.items()):
            if exclude_user_id and uid == exclude_user_id:
                continue
            try:
                await ws.send_json(data)
            except Exception:
                dead_users.append(uid)
        for uid in dead_users:
            self.disconnect(uid)

    async def notify_player_update(self, player: Player):
        """游戏引擎调用：玩家状态变化时推送。"""
        await self.send_to_player(player.user_id, {
            "type": "state_update",
            "data": player.to_dict(),
        })

    def set_market_watch(self, user_id: str, *, enabled: bool, tab: str = "", page: int = 1):
        """记录当前连接关注的坊市视图，便于服务端合并推送。"""
        self._market_pages.pop(user_id, None)
        self._market_my_watchers.discard(user_id)
        if not enabled:
            return

        normalized_tab = str(tab or "").strip().lower()
        if normalized_tab == "browse":
            try:
                page_num = int(page)
            except (TypeError, ValueError):
                page_num = 1
            self._market_pages[user_id] = max(1, page_num)
        elif normalized_tab == "my":
            self._market_my_watchers.add(user_id)

    def queue_rankings_refresh(self, engine: GameEngine):
        """合并多个排行榜刷新请求，避免全员回拉。"""
        if not self._connections:
            return
        self._rankings_dirty = True
        if self._rankings_flush_task and not self._rankings_flush_task.done():
            return
        self._rankings_flush_task = asyncio.create_task(self._flush_rankings(engine))

    async def _flush_rankings(self, engine: GameEngine):
        try:
            while True:
                self._rankings_dirty = False
                await asyncio.sleep(RANKINGS_PUSH_DELAY)
                if self._connections:
                    await self.push_rankings_data(engine)
                if not self._rankings_dirty:
                    break
        finally:
            self._rankings_flush_task = None

    async def push_rankings_data(self, engine: GameEngine):
        base_payload, my_rank_map = _build_rankings_snapshot(engine)
        for user_id in list(self._connections.keys()):
            payload = dict(base_payload)
            payload["my_rank"] = my_rank_map.get(user_id)
            await self.send_to_player(user_id, {
                "type": "rankings_data",
                "data": payload,
            })

    def queue_market_refresh(self, engine: GameEngine):
        """合并坊市刷新并由服务端主动推送当前视图。"""
        if not self._connections or (not self._market_pages and not self._market_my_watchers):
            return
        self._market_dirty = True
        if self._market_flush_task and not self._market_flush_task.done():
            return
        self._market_flush_task = asyncio.create_task(self._flush_market(engine))

    async def _flush_market(self, engine: GameEngine):
        try:
            while True:
                self._market_dirty = False
                await asyncio.sleep(MARKET_PUSH_DELAY)
                if self._connections:
                    await self.push_market_data(engine)
                if not self._market_dirty:
                    break
        finally:
            self._market_flush_task = None

    async def push_market_data(self, engine: GameEngine):
        page_watchers: dict[int, list[str]] = {}
        for user_id, page in list(self._market_pages.items()):
            if user_id not in self._connections:
                continue
            page_watchers.setdefault(page, []).append(user_id)

        for page, user_ids in page_watchers.items():
            data = await engine.market_get_listings(
                page,
                page_size=MARKET_PAGE_SIZE,
                cleanup_expired=False,
            )
            for user_id in user_ids:
                await self.send_to_player(user_id, {
                    "type": "market_data",
                    "data": data,
                })

        for user_id in list(self._market_my_watchers):
            if user_id not in self._connections:
                continue
            listings = await engine.market_get_my_listings(
                user_id,
                cleanup_expired=False,
            )
            await self.send_to_player(user_id, {
                "type": "my_listings",
                "data": {"listings": listings},
            })

    # ── 世界频道 ──────────────────────────────────────────
    async def get_world_chat_history(self) -> list[dict]:
        """从数据库获取世界频道历史消息（最新在后）。"""
        return await self._engine._data_manager.load_chat_history(
            WORLD_CHAT_MAX_HISTORY,
            max_age_seconds=WORLD_CHAT_MAX_AGE,
        )

    def check_chat_cooldown(self, user_id: str) -> tuple[bool, float]:
        """检查发言冷却，返回 (ok, remaining_seconds)。"""
        now = time.time()
        last = self._world_chat_cooldowns.get(user_id, 0)
        remaining = WORLD_CHAT_COOLDOWN - (now - last)
        if remaining > 0:
            return False, remaining
        return True, 0

    def record_chat_send(self, user_id: str):
        self._world_chat_cooldowns[user_id] = time.time()

    async def add_chat_message(self, msg: dict):
        """保存消息到数据库。"""
        await self._engine._data_manager.save_chat_message(
            user_id=msg.get("user_id", ""),
            name=msg.get("name", ""),
            realm=msg.get("realm", ""),
            content=msg.get("content", ""),
            created_at=msg.get("time", time.time()),
            sect_name=msg.get("sect_name", ""),
            sect_role=msg.get("sect_role", ""),
            sect_role_name=msg.get("sect_role_name", ""),
        )

    async def broadcast_chat(self, msg: dict):
        """广播世界频道消息给所有在线玩家。"""
        await self.broadcast({"type": "world_chat_msg", "data": msg})

    def start_chat_cleanup_task(self):
        """启动定期清理过期世界频道消息的后台任务。"""
        if self._chat_cleanup_task and not self._chat_cleanup_task.done():
            return
        self._chat_cleanup_task = asyncio.create_task(self._chat_cleanup_loop())

    async def stop_chat_cleanup_task(self):
        """停止定期清理过期世界频道消息的后台任务。"""
        task = self._chat_cleanup_task
        if not task:
            return
        if task.done():
            self._chat_cleanup_task = None
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            self._chat_cleanup_task = None

    async def _chat_cleanup_loop(self):
        """每隔一段时间清理超过1个月的世界频道消息。"""
        logger = logging.getLogger("xiuxian.world_chat")
        while True:
            try:
                await asyncio.sleep(WORLD_CHAT_CLEANUP_INTERVAL)
                deleted = await self._engine._data_manager.cleanup_old_chat_messages(WORLD_CHAT_MAX_AGE)
                if deleted > 0:
                    logger.info("已清理 %d 条过期世界频道消息", deleted)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("世界频道消息清理失败")


def create_ws_router(
    engine: GameEngine,
    guard_token: str = "",
    command_prefix: str = "修仙",
    api_rate_limit_1s_count: int = 10000,
) -> APIRouter:
    router = APIRouter()
    ws_manager = ConnectionManager(engine)
    engine._ws_manager = ws_manager
    ws_manager.start_chat_cleanup_task()
    access_guard = get_access_guard()
    required_guard_token = (guard_token or "").strip()
    try:
        limit_1s_count = int(api_rate_limit_1s_count)
    except (TypeError, ValueError):
        limit_1s_count = 10000
    limit_1s_count = max(100, limit_1s_count)
    ws_conn_window = 60.0
    ws_conn_limit = 30
    ws_block_seconds = 120.0
    ws_msg_window = 1.0
    ws_msg_limit = limit_1s_count
    ws_msg_burst_count = limit_1s_count + 1
    ws_msg_burst_window = 1.0

    def _client_ip_from_ws(websocket: WebSocket) -> str:
        headers = websocket.headers
        for key in (
            "cf-connecting-ip",
            "x-real-ip",
            "x-forwarded-for",
            "x-client-ip",
            "x-cluster-client-ip",
            "forwarded",
        ):
            raw = str(headers.get(key, "")).strip()
            if not raw:
                continue
            if key == "forwarded":
                for part in raw.split(";"):
                    part = part.strip()
                    if part.lower().startswith("for="):
                        ip = access_guard.normalize_ip(part)
                        if ip:
                            return ip
                continue
            ip = access_guard.normalize_ip(raw)
            if ip:
                return ip
        if websocket.client and websocket.client.host:
            ip = access_guard.normalize_ip(str(websocket.client.host))
            if ip:
                return ip
            return str(websocket.client.host)[:64]
        return "unknown"

    @router.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        user_id = None
        client_ip = "unknown"
        client_ua = ""
        client_page_key = ""
        page_guard: dict[str, str | int] = {"page_id": "", "issued_at": 0, "signature": ""}

        try:
            client_ip = _client_ip_from_ws(websocket)
            client_ua = str(websocket.headers.get("user-agent", "")).strip().lower()
            client_page_key = str(websocket.cookies.get("xiuxian_page_client", "")).strip()
            ok, reason = access_guard.check_ws_connect(
                ip=client_ip,
                limit=ws_conn_limit,
                window=ws_conn_window,
                block_seconds=ws_block_seconds,
            )
            if not ok:
                await websocket.send_json({"type": "error", "message": reason or "连接过于频繁，请稍后再试"})
                await websocket.close()
                return

            # 等待登录消息（token 认证）
            raw = await websocket.receive_json()
            if raw.get("type") != "login":
                await websocket.send_json({"type": "error", "message": "请先登录"})
                await websocket.close()
                return

            token = raw.get("data", {}).get("token", "")
            page_guard = {
                "page_id": str(raw.get("data", {}).get("page_id", "")).strip(),
                "issued_at": raw.get("data", {}).get("issued_at", 0),
                "signature": str(raw.get("data", {}).get("signature", "")).strip(),
            }
            ok, reason = access_guard.validate_page_session(
                secret=required_guard_token,
                page_id=page_guard.get("page_id", ""),
                issued_at=page_guard.get("issued_at", 0),
                signature=page_guard.get("signature", ""),
                ip=client_ip,
                ua=client_ua,
                client_key=client_page_key,
            )
            if not ok:
                await websocket.send_json({"type": "error", "message": reason or "页面凭证无效，请刷新页面"})
                await websocket.close(code=1008)
                return
            if not token or not engine.auth:
                await websocket.send_json({"type": "error", "message": "认证信息缺失"})
                await websocket.close()
                return

            user_id = engine.auth.verify_web_token(token)
            if not user_id:
                await websocket.send_json({"type": "error", "message": "登录已过期，请重新登录"})
                await websocket.close()
                return

            player = await engine.get_player(user_id)
            if not player:
                await websocket.send_json({"type": "error", "message": "角色不存在"})
                await websocket.close()
                return

            await ws_manager.connect(user_id, websocket)

            # 发送初始状态
            panel = await engine.get_panel(user_id)
            await websocket.send_json({
                "type": "state_update",
                "data": panel or player.to_dict(),
            })
            await websocket.send_json({
                "type": "rankings_data",
                "data": _build_rankings_payload(engine, user_id),
            })
            ws_manager.queue_rankings_refresh(engine)
            await websocket.send_json({
                "type": "world_chat_history",
                "data": await ws_manager.get_world_chat_history(),
            })

            # 推送公告
            announcements = await engine.get_active_announcements()
            if announcements:
                await websocket.send_json({"type": "announcements", "data": announcements})

            # 主消息循环
            while True:
                msg = await websocket.receive_json()
                ok, reason = access_guard.validate_page_session(
                    secret=required_guard_token,
                    page_id=page_guard.get("page_id", ""),
                    issued_at=page_guard.get("issued_at", 0),
                    signature=page_guard.get("signature", ""),
                    ip=client_ip,
                    ua=client_ua,
                    client_key=client_page_key,
                )
                if not ok:
                    await websocket.send_json({"type": "error", "message": reason or "页面凭证已失效，请刷新页面"})
                    await websocket.close(code=1008)
                    break
                ok, reason = access_guard.check_ws_message(
                    ip=client_ip,
                    limit=ws_msg_limit,
                    window=ws_msg_window,
                    burst_count=ws_msg_burst_count,
                    burst_window=ws_msg_burst_window,
                    block_seconds=ws_block_seconds,
                )
                if not ok:
                    await websocket.send_json({"type": "error", "message": reason or "请求过于频繁，请稍后再试"})
                    await websocket.close()
                    break
                try:
                    result = await _handle_message(
                        engine,
                        user_id,
                        msg,
                        command_prefix=command_prefix,
                        ws_manager=ws_manager,
                    )
                except Exception:
                    detail = "unknown"
                    try:
                        import traceback
                        detail = traceback.format_exc(limit=5).strip().splitlines()[-1]
                    except Exception:
                        pass
                    ws_logger.exception(
                        "修仙世界：WS消息处理失败 user_id=%s msg_type=%s",
                        user_id,
                        msg.get("type", ""),
                    )
                    try:
                        await websocket.send_json({
                            "type": "error",
                            "message": f"服务器处理请求时发生异常：{detail}",
                        })
                    except Exception:
                        pass
                    continue
                if result and result.get("type") != "noop":
                    await websocket.send_json(result)

        except WebSocketDisconnect:
            pass
        except Exception:
            ws_logger.exception("修仙世界：WebSocket会话异常 user_id=%s", user_id)
        finally:
            if user_id:
                ws_manager.disconnect(user_id)
                ws_manager.queue_rankings_refresh(engine)

    return router


def _build_rankings_snapshot(engine: GameEngine) -> tuple[dict, dict[str, dict]]:
    """构造排行榜公共快照，并缓存每个在线用户的个人排名映射。"""
    all_rankings = engine.get_rankings(limit=999)
    death_rankings = engine.get_death_rankings(limit=10)
    online_rankings = engine.get_online_rankings(limit=50)
    online = 0
    if engine._ws_manager:
        online = len(engine._ws_manager._connections)

    my_rank_map: dict[str, dict] = {}
    for row in all_rankings:
        owner_id = engine._name_index.get(str(row.get("name", "")))
        if owner_id and owner_id not in my_rank_map:
            my_rank_map[owner_id] = row

    payload = {
        "success": True,
        "total_players": len(engine._players),
        "online_players": online,
        "rankings": all_rankings[:10],
        "death_rankings": death_rankings,
        "online_rankings": online_rankings,
    }
    return payload, my_rank_map


def _build_rankings_payload(engine: GameEngine, user_id: str) -> dict:
    """构造排行榜响应数据（WebSocket）。"""
    payload, my_rank_map = _build_rankings_snapshot(engine)
    data = dict(payload)
    data["my_rank"] = my_rank_map.get(user_id)
    return data


async def _review_chat_content(engine: GameEngine, content: str) -> dict:
    """调用 AI 审核世界频道消息内容。"""
    reviewer = getattr(engine, "_chat_reviewer", None)
    if not callable(reviewer):
        return {"allow": True, "reason": ""}
    try:
        result = await reviewer(content)
        if isinstance(result, dict):
            return {
                "allow": bool(result.get("allow", True)),
                "reason": str(result.get("reason", "")).strip(),
            }
        return {"allow": True, "reason": ""}
    except Exception:
        return {"allow": True, "reason": ""}


async def _push_player_snapshot(
    engine: GameEngine,
    ws_manager: ConnectionManager,
    user_id: str,
):
    """主动同步玩家面板与背包。"""
    panel = await engine.get_panel(user_id)
    if panel:
        await ws_manager.send_to_player(user_id, {
            "type": "state_update",
            "data": panel,
        })
    inventory = await engine.get_inventory(user_id)
    await ws_manager.send_to_player(user_id, {
        "type": "inventory",
        "data": inventory,
    })


async def _broadcast_pvp_result(
    engine: GameEngine,
    ws_manager: ConnectionManager,
    result: dict,
):
    """向双方推送 PvP 状态，并在结束时回写副本流。"""
    session_id = str(result.get("session_id", "")).strip()
    session_obj = engine.pvp._sessions.get(session_id)
    if not session_obj:
        return

    a_id = session_obj.player_a_id
    b_id = session_obj.player_b_id
    if result.get("ended"):
        await ws_manager.send_to_player(a_id, {
            "type": "pvp_result",
            "data": result["pvp_state_a"],
        })
        await ws_manager.send_to_player(b_id, {
            "type": "pvp_result",
            "data": result["pvp_state_b"],
        })
        dungeon_result = await engine.dungeon.resolve_pvp_result(session_obj)
        await _push_player_snapshot(engine, ws_manager, a_id)
        await _push_player_snapshot(engine, ws_manager, b_id)
        if dungeon_result and session_obj.dungeon_owner_id:
            await ws_manager.send_to_player(session_obj.dungeon_owner_id, {
                "type": "action_result",
                "action": "dungeon_pvp_result",
                "data": dungeon_result,
            })
        engine.pvp.cleanup_session(session_id)
        return

    await ws_manager.send_to_player(a_id, {
        "type": "pvp_update",
        "data": {"pvp_state": result["pvp_state_a"]},
    })
    await ws_manager.send_to_player(b_id, {
        "type": "pvp_update",
        "data": {"pvp_state": result["pvp_state_b"]},
    })


async def _broadcast_sect_changed(
    ws_manager: ConnectionManager | None,
    exclude_user_id: str | None = None,
):
    """广播宗门数据变更，让在线前端自行刷新宗门相关面板。"""
    if not ws_manager:
        return
    try:
        await ws_manager.broadcast({"type": "sect_changed"}, exclude_user_id=exclude_user_id)
    except Exception:
        ws_logger.exception("修仙世界：宗门变更广播失败")


def _schedule_pvp_challenge_timeout(
    engine: GameEngine,
    ws_manager: ConnectionManager,
    session_id: str,
):
    """在应战倒计时结束后自动判定为放弃。"""

    async def _runner():
        session = engine.pvp._sessions.get(session_id)
        if not session:
            return
        delay = max(0.0, float(session.countdown_deadline or 0.0) - time.time())
        if delay > 0:
            await asyncio.sleep(delay)
        payload = engine.pvp.expire_challenge(session_id)
        if not payload:
            return
        await _broadcast_pvp_result(engine, ws_manager, payload)

    asyncio.create_task(_runner())


async def _handle_message(
    engine: GameEngine,
    user_id: str,
    msg: dict,
    command_prefix: str = "修仙",
    ws_manager: ConnectionManager | None = None,
) -> dict | None:
    """处理客户端 WebSocket 消息。"""
    msg_type = msg.get("type", "")

    if engine.has_pending_death(user_id) and msg_type not in _PENDING_DEATH_ALLOWED_TYPES:
        return {
            "type": "error",
            "message": "道陨未决，请先完成携宝重生选择，当前无法执行其他操作",
        }

    if msg_type == "cultivate":
        result = await engine.cultivate(user_id)
        return {"type": "action_result", "action": "cultivate", "data": result}

    elif msg_type == "checkin":
        result = await engine.daily_checkin(user_id)
        return {"type": "action_result", "action": "checkin", "data": result}

    elif msg_type == "start_afk":
        minutes = msg.get("data", {}).get("minutes", 0)
        try:
            minutes = int(minutes)
        except (TypeError, ValueError):
            return {"type": "error", "message": "请输入有效的分钟数"}
        result = await engine.start_afk_cultivate(user_id, minutes)
        return {"type": "action_result", "action": "start_afk", "data": result}

    elif msg_type == "collect_afk":
        result = await engine.collect_afk_cultivate(user_id)
        return {"type": "action_result", "action": "collect_afk", "data": result}

    elif msg_type == "cancel_afk":
        result = await engine.cancel_afk_cultivate(user_id)
        return {"type": "action_result", "action": "cancel_afk", "data": result}

    elif msg_type == "adventure":
        result = await engine.adventure(user_id)
        return {"type": "action_result", "action": "dungeon_start", "data": result}

    elif msg_type == "get_announcements":
        announcements = await engine.get_active_announcements()
        return {"type": "announcements", "data": announcements}

    elif msg_type == "breakthrough":
        result = await engine.breakthrough(user_id)
        return {"type": "action_result", "action": "breakthrough", "data": result}

    elif msg_type == "use_item":
        item_id = msg.get("data", {}).get("item_id", "")
        raw_count = msg.get("data", {}).get("count", 1)
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            return {"type": "error", "message": "使用数量必须是整数"}
        if count < 1:
            return {"type": "error", "message": "使用数量至少为1"}
        result = await engine.use_item_action(user_id, item_id, count)
        return {"type": "action_result", "action": "use_item", "data": result}

    elif msg_type == "confirm_replace_heart_method":
        data = msg.get("data", {})
        new_method_id = data.get("new_method_id", "")
        source_item_id = data.get("source_item_id", "")
        raw_convert = data.get("convert_to_value", False)
        if isinstance(raw_convert, str):
            convert_to_value = raw_convert.strip().lower() in {"1", "true", "yes", "on"}
        else:
            convert_to_value = bool(raw_convert)
        result = await engine.confirm_replace_heart_method(
            user_id,
            new_method_id,
            convert_to_value,
            source_item_id,
        )
        return {"type": "action_result", "action": "confirm_replace_heart_method", "data": result}

    elif msg_type == "get_panel":
        panel = await engine.get_panel(user_id)
        if panel:
            return {"type": "state_update", "data": panel}
        return {"type": "error", "message": "角色不存在"}

    elif msg_type == "get_rankings":
        return {"type": "rankings_data", "data": _build_rankings_payload(engine, user_id)}

    elif msg_type == "get_inventory":
        inv = await engine.get_inventory(user_id)
        return {"type": "inventory", "data": inv}

    elif msg_type == "equip":
        equip_id = msg.get("data", {}).get("equip_id", "")
        result = await engine.equip_action(user_id, equip_id)
        return {"type": "action_result", "action": "equip", "data": result}

    elif msg_type == "unequip":
        slot = msg.get("data", {}).get("slot", "")
        result = await engine.unequip_action(user_id, slot)
        return {"type": "action_result", "action": "unequip", "data": result}

    elif msg_type == "learn_heart_method":
        return {
            "type": "action_result",
            "action": "learn_heart_method",
            "data": {
                "success": False,
                "message": "已取消直接选择心法，请通过历练掉落秘籍并在背包中使用。",
            },
        }

    elif msg_type == "get_heart_methods":
        return {
            "type": "heart_methods",
            "data": {
                "success": False,
                "methods": [],
                "message": "已取消直接选择心法，请通过历练掉落秘籍并在背包中使用。",
            },
        }

    elif msg_type == "get_bind_key":
        if engine.auth:
            key = await engine.auth.create_bind_key(user_id)
            return {
                "type": "bind_key",
                "data": {
                    "key": key,
                    "message": f"请在聊天平台中发送：/{command_prefix} 登录 {key}",
                },
            }
        return {"type": "error", "message": "认证系统未启用"}

    elif msg_type == "recycle":
        item_id = msg.get("data", {}).get("item_id", "")
        raw_count = msg.get("data", {}).get("count", 1)
        try:
            count = int(raw_count)
        except (TypeError, ValueError):
            return {"type": "error", "message": "回收数量必须是整数"}
        if count < 1:
            return {"type": "error", "message": "回收数量至少为1"}
        result = await engine.recycle_action(user_id, item_id, count)
        return {"type": "action_result", "action": "recycle", "data": result}

    # ── 天机阁（商店） ──────────────────────────────────────
    elif msg_type == "get_shop":
        data = await engine.shop_get_items(user_id)
        return {"type": "shop_data", "data": data}

    elif msg_type == "shop_buy":
        item_id = msg.get("data", {}).get("item_id", "")
        raw_qty = msg.get("data", {}).get("quantity", 1)
        try:
            quantity = int(raw_qty)
        except (TypeError, ValueError):
            return {"type": "error", "message": "购买数量必须是整数"}
        if quantity < 1:
            return {"type": "error", "message": "购买数量至少为1"}
        result = await engine.shop_buy(user_id, item_id, quantity)
        return {"type": "action_result", "action": "shop_buy", "data": result}

    # ── 坊市 ──────────────────────────────────────────────
    elif msg_type == "market_list":
        data = msg.get("data", {})
        item_id = data.get("item_id", "")
        try:
            quantity = int(data.get("quantity", 0))
            unit_price = int(data.get("unit_price", 0))
        except (TypeError, ValueError):
            return {"type": "error", "message": "数量和价格必须是整数"}
        result = await engine.market_list(user_id, item_id, quantity, unit_price)
        return {"type": "action_result", "action": "market_list", "data": result}

    elif msg_type == "market_buy":
        listing_id = msg.get("data", {}).get("listing_id", "")
        result = await engine.market_buy(user_id, listing_id)
        return {"type": "action_result", "action": "market_buy", "data": result}

    elif msg_type == "market_cancel":
        listing_id = msg.get("data", {}).get("listing_id", "")
        result = await engine.market_cancel(user_id, listing_id)
        return {"type": "action_result", "action": "market_cancel", "data": result}

    elif msg_type == "get_market":
        page = msg.get("data", {}).get("page", 1)
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        if ws_manager:
            ws_manager.set_market_watch(user_id, enabled=True, tab="browse", page=page)
        data = await engine.market_get_listings(page, page_size=9)
        return {"type": "market_data", "data": data}

    elif msg_type == "get_my_listings":
        if ws_manager:
            ws_manager.set_market_watch(user_id, enabled=True, tab="my")
        listings = await engine.market_get_my_listings(user_id)
        return {"type": "my_listings", "data": {"listings": listings}}

    elif msg_type == "market_watch":
        data = msg.get("data", {})
        raw_enabled = data.get("enabled", False)
        if isinstance(raw_enabled, str):
            enabled = raw_enabled.strip().lower() in {"1", "true", "yes", "on"}
        else:
            enabled = bool(raw_enabled)
        tab = str(data.get("tab", "")).strip().lower()
        page = data.get("page", 1)
        if ws_manager:
            ws_manager.set_market_watch(user_id, enabled=enabled, tab=tab, page=page)
        return {"type": "noop"}

    elif msg_type == "market_clear_history":
        include_expired = bool(msg.get("data", {}).get("include_expired", False))
        result = await engine.market_clear_my_history(user_id, include_expired=include_expired)
        return {"type": "action_result", "action": "market_clear_history", "data": result}

    elif msg_type == "market_fee_preview":
        data = msg.get("data", {})
        item_id = data.get("item_id", "")
        try:
            quantity = int(data.get("quantity", 0))
            unit_price = int(data.get("unit_price", 0))
        except (TypeError, ValueError):
            return {"type": "error", "message": "数量和价格必须是整数"}
        preview = await engine.market_fee_preview(item_id, quantity, unit_price)
        return {"type": "fee_preview", "data": preview}

    # ── 世界频道 ──────────────────────────────────────────
    elif msg_type == "world_chat_send":
        content = str(msg.get("data", {}).get("content", "")).strip()
        if not content:
            return {"type": "world_chat_blocked", "data": {"reason": "消息不能为空"}}
        if len(content) > WORLD_CHAT_MAX_LEN:
            return {"type": "world_chat_blocked", "data": {"reason": f"消息不能超过{WORLD_CHAT_MAX_LEN}字"}}
        if not _RE_CHINESE_ONLY.match(content):
            return {"type": "world_chat_blocked", "data": {"reason": "世界频道暂时只支持中文和数字"}}
        if not ws_manager:
            return {"type": "world_chat_blocked", "data": {"reason": "服务不可用"}}
        # 冷却检查
        ok, remaining = ws_manager.check_chat_cooldown(user_id)
        if not ok:
            return {"type": "world_chat_blocked", "data": {"reason": f"发言冷却中，请等待{remaining:.0f}秒"}}
        # AI 内容审核
        review = await _review_chat_content(engine, content)
        if not review["allow"]:
            return {"type": "world_chat_blocked", "data": {"reason": review["reason"]}}
        # 获取玩家名
        player = await engine.get_player(user_id)
        player_name = player.name if player else "未知"
        player_realm = get_realm_name(player.realm, player.sub_realm) if player else ""
        # 获取宗门名
        sect_name = ""
        sect_role = ""
        sect_role_name = ""
        membership = await engine._data_manager.load_player_sect(user_id)
        if membership:
            sect_role = membership.get("role", "")
            sect_role_name = ROLE_NAMES.get(sect_role, sect_role)
            sect = await engine._data_manager.load_sect(membership["sect_id"])
            if sect:
                sect_name = sect["name"]
        chat_msg = {
            "user_id": user_id,
            "name": player_name,
            "realm": player_realm,
            "sect_name": sect_name,
            "sect_role": sect_role,
            "sect_role_name": sect_role_name,
            "content": content,
            "time": time.time(),
        }
        ws_manager.record_chat_send(user_id)
        await ws_manager.add_chat_message(chat_msg)
        await ws_manager.broadcast_chat(chat_msg)
        return {"type": "noop"}

    elif msg_type == "get_world_chat_history":
        if not ws_manager:
            return {"type": "world_chat_history", "data": []}
        return {"type": "world_chat_history", "data": await ws_manager.get_world_chat_history()}

    # ── 死亡保留确认 ─────────────────────────────────────
    elif msg_type == "death_confirm_keep":
        kept_ids = msg.get("data", {}).get("kept_ids", [])
        if not isinstance(kept_ids, list):
            return {"type": "error", "message": "kept_ids 必须是列表"}
        result = await engine.confirm_death(user_id, kept_ids)
        return {"type": "action_result", "action": "death_confirm_keep", "data": result}

    # ── 功法遗忘 ─────────────────────────────────────────
    elif msg_type == "forget_gongfa":
        slot = msg.get("data", {}).get("slot", "")
        result = await engine.forget_gongfa(user_id, slot)
        return {"type": "action_result", "action": "forget_gongfa", "data": result}

    # ── 副本系统 ─────────────────────────────────────────
    elif msg_type == "dungeon_start":
        result = await engine.adventure(user_id)
        return {"type": "action_result", "action": "dungeon_start", "data": result}

    elif msg_type == "dungeon_advance":
        player = await engine.get_player(user_id)
        if not player:
            return {"type": "error", "message": "角色不存在"}
        result = await engine.dungeon.advance(player)
        if result.get("pvp_notice") and ws_manager and result.get("pvp_opponent_id"):
            await ws_manager.send_to_player(result["pvp_opponent_id"], {
                "type": "pvp_challenge_notice",
                    "data": {
                        "session_id": result.get("pvp_session_id"),
                        "countdown_deadline": result["pvp_notice"].get("countdown_deadline", 0),
                        "challenger_name": result["pvp_notice"].get("challenger_name", "未知修士"),
                        "layer_name": result["pvp_notice"].get("layer_name", "秘境"),
                        "source": "dungeon",
                        "message": "请在 10 秒内决定是否应战；若拒绝或超时，对方将直接夺得本层机缘。",
                    },
                })
            _schedule_pvp_challenge_timeout(engine, ws_manager, result["pvp_session_id"])
        return {"type": "action_result", "action": "dungeon_advance", "data": result}

    elif msg_type == "dungeon_combat":
        player = await engine.get_player(user_id)
        if not player:
            return {"type": "error", "message": "角色不存在"}
        action = msg.get("data", {}).get("action", "attack")
        data = msg.get("data", {})
        result = await engine.dungeon.combat_action(player, action, data)
        return {"type": "action_result", "action": "dungeon_combat", "data": result}

    elif msg_type == "dungeon_exit":
        player = await engine.get_player(user_id)
        if not player:
            return {"type": "error", "message": "角色不存在"}
        result = await engine.dungeon.exit_dungeon(player)
        return {"type": "action_result", "action": "dungeon_exit", "data": result}

    elif msg_type == "dungeon_state":
        session = engine.dungeon.get_session(user_id)
        if session:
            return {"type": "dungeon_state", "data": session.to_dict()}
        return {"type": "dungeon_state", "data": None}

    # ── PvP 系统 ──────────────────────────────────────────
    elif msg_type == "pvp_match":
        return {
            "type": "error",
            "message": "已取消主动切磋，请在副本中遭遇在线玩家",
        }

    elif msg_type == "pvp_action":
        session_id = msg.get("data", {}).get("session_id", "")
        action = msg.get("data", {}).get("action", {})
        player = await engine.get_player(user_id)
        result = await engine.pvp.submit_action(session_id, user_id, action, player)
        if result.get("pvp_state"):
            return {"type": "pvp_update", "data": result}
        if result.get("resolved") and ws_manager:
            await _broadcast_pvp_result(engine, ws_manager, result)
            return {"type": "noop"}
        return {"type": "action_result", "action": "pvp_action", "data": result}

    elif msg_type == "pvp_challenge_response":
        session_id = msg.get("data", {}).get("session_id", "")
        accept = bool(msg.get("data", {}).get("accept", False))
        player = await engine.get_player(user_id)
        result = engine.pvp.respond_challenge(session_id, user_id, accept, player)
        if result.get("started") and ws_manager:
            session = engine.pvp.get_session_for_player(user_id)
            if session:
                await ws_manager.send_to_player(session.player_a_id, {
                    "type": "pvp_start",
                    "data": result["pvp_state_a"],
                })
                await ws_manager.send_to_player(session.player_b_id, {
                    "type": "pvp_start",
                    "data": result["pvp_state_b"],
                })
                await _push_player_snapshot(engine, ws_manager, session.player_a_id)
                await _push_player_snapshot(engine, ws_manager, session.player_b_id)
            return {"type": "noop"}
        if result.get("ended") and ws_manager:
            await _broadcast_pvp_result(engine, ws_manager, result)
            return {"type": "noop"}
        return {"type": "action_result", "action": "pvp_challenge_response", "data": result}

    elif msg_type == "pvp_flee_offer":
        session_id = msg.get("data", {}).get("session_id", "")
        items = msg.get("data", {}).get("items", [])
        player = await engine.get_player(user_id)
        result = await engine.pvp.submit_flee_request(session_id, user_id, items, player)
        if result.get("pvp_state_a") and result.get("pvp_state_b") and ws_manager:
            await _broadcast_pvp_result(engine, ws_manager, result)
            return {"type": "noop"}
        return {"type": "action_result", "action": "pvp_flee_offer", "data": result}

    elif msg_type == "pvp_flee_response":
        session_id = msg.get("data", {}).get("session_id", "")
        accept = bool(msg.get("data", {}).get("accept", False))
        player = await engine.get_player(user_id)
        result = await engine.pvp.respond_flee_request(session_id, user_id, accept, player)
        if result.get("pvp_state_a") and result.get("pvp_state_b") and ws_manager:
            await _broadcast_pvp_result(engine, ws_manager, result)
            return {"type": "noop"}
        return {"type": "action_result", "action": "pvp_flee_response", "data": result}

    elif msg_type == "pvp_state":
        session = engine.pvp.get_session_for_player(user_id)
        if session:
            return {"type": "pvp_state", "data": session.to_dict(user_id)}
        return {"type": "pvp_state", "data": None}

    # ── 宗门系统 ─────────────────────────────────────────

    elif msg_type == "sect_create":
        data = msg.get("data", {})
        name = str(data.get("name", "")).strip()
        description = str(data.get("description", "")).strip()
        if not name:
            return {"type": "error", "message": "请输入宗门名称"}
        result = await engine.sect_create(user_id, name, description)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_create", "data": result}

    elif msg_type == "sect_list":
        page = msg.get("data", {}).get("page", 1)
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        data = await engine.sect_list(page, page_size=10)
        return {"type": "sect_list_data", "data": data}

    elif msg_type == "sect_my":
        data = await engine.sect_my(user_id)
        return {"type": "sect_my_data", "data": data}

    elif msg_type == "sect_detail":
        sect_id = msg.get("data", {}).get("sect_id", "")
        if not sect_id:
            return {"type": "error", "message": "缺少宗门ID"}
        data = await engine.sect_detail(sect_id)
        return {"type": "sect_detail_data", "data": data}

    elif msg_type == "sect_join":
        sect_id = msg.get("data", {}).get("sect_id", "")
        if not sect_id:
            return {"type": "error", "message": "缺少宗门ID"}
        result = await engine.sect_join(user_id, sect_id)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_join", "data": result}

    elif msg_type == "sect_leave":
        result = await engine.sect_leave(user_id)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_leave", "data": result}

    elif msg_type == "sect_kick":
        target_id = msg.get("data", {}).get("target_id", "")
        if not target_id:
            return {"type": "error", "message": "缺少目标玩家ID"}
        result = await engine.sect_kick(user_id, target_id)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_kick", "data": result}

    elif msg_type == "sect_set_role":
        data = msg.get("data", {})
        target_id = data.get("target_id", "")
        role = data.get("role", "")
        if not target_id or not role:
            return {"type": "error", "message": "缺少目标玩家或身份"}
        result = await engine.sect_set_role(user_id, target_id, role)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_set_role", "data": result}

    elif msg_type == "sect_update_info":
        data = msg.get("data", {})
        result = await engine.sect_update_info(user_id, data)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_update_info", "data": result}

    elif msg_type == "sect_transfer":
        target_id = msg.get("data", {}).get("target_id", "")
        if not target_id:
            return {"type": "error", "message": "缺少目标玩家ID"}
        result = await engine.sect_transfer(user_id, target_id)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_transfer", "data": result}

    elif msg_type == "sect_disband":
        result = await engine.sect_disband(user_id)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_disband", "data": result}

    # ── 宗门仓库 ──────────────────────────────────────────

    elif msg_type == "sect_warehouse_list":
        data = await engine.sect_warehouse_list(user_id)
        return {"type": "sect_warehouse_data", "data": data}

    elif msg_type == "sect_warehouse_deposit":
        d = msg.get("data", {})
        item_id = str(d.get("item_id", "")).strip()
        if not item_id:
            return {"type": "error", "message": "缺少物品ID"}
        try:
            count = max(1, int(d.get("count", 1)))
        except (TypeError, ValueError):
            count = 1
        result = await engine.sect_warehouse_deposit(user_id, item_id, count)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_warehouse_deposit", "data": result}

    elif msg_type == "sect_warehouse_exchange":
        d = msg.get("data", {})
        item_id = str(d.get("item_id", "")).strip()
        if not item_id:
            return {"type": "error", "message": "缺少物品ID"}
        try:
            count = max(1, int(d.get("count", 1)))
        except (TypeError, ValueError):
            count = 1
        result = await engine.sect_warehouse_exchange(user_id, item_id, count)
        if result.get("success"):
            await _broadcast_sect_changed(ws_manager, exclude_user_id=user_id)
        return {"type": "action_result", "action": "sect_warehouse_exchange", "data": result}

    elif msg_type == "sect_contribution_rules":
        data = await engine.sect_get_contribution_rules(user_id)
        return {"type": "sect_contribution_rules_data", "data": data}

    elif msg_type == "sect_shop_list":
        data = await engine.sect_shop_get_items(user_id)
        return {"type": "sect_shop_data", "data": data}

    elif msg_type == "sect_shop_buy":
        d = msg.get("data", {})
        item_id = str(d.get("item_id", "")).strip()
        quantity = max(1, int(d.get("quantity", 1) or 1))
        if not item_id:
            return {"type": "error", "message": "缺少商品ID"}
        result = await engine.sect_shop_buy(user_id, item_id, quantity)
        return {"type": "action_result", "action": "sect_shop_buy", "data": result}

    elif msg_type == "sect_set_submit_rule":
        d = msg.get("data", {})
        quality_key = str(d.get("quality_key", "")).strip()
        if not quality_key:
            return {"type": "error", "message": "缺少品质分类"}
        try:
            points = max(0, int(d.get("points", 0)))
        except (TypeError, ValueError):
            return {"type": "error", "message": "贡献点数必须为整数"}
        result = await engine.sect_set_submit_rule(user_id, quality_key, points)
        return {"type": "action_result", "action": "sect_set_submit_rule", "data": result}

    elif msg_type == "sect_set_exchange_rule":
        d = msg.get("data", {})
        target_key = str(d.get("target_key", "")).strip()
        if not target_key:
            return {"type": "error", "message": "缺少目标键"}
        try:
            points = max(0, int(d.get("points", 0)))
        except (TypeError, ValueError):
            return {"type": "error", "message": "贡献点数必须为整数"}
        raw_is_item = d.get("is_item", False)
        is_item = raw_is_item is True or (isinstance(raw_is_item, str) and raw_is_item.lower() in ("true", "1"))
        result = await engine.sect_set_exchange_rule(user_id, target_key, points, is_item=is_item)
        return {"type": "action_result", "action": "sect_set_exchange_rule", "data": result}

    # ── 灵田系统 ──────────────────────────────────────────
    elif msg_type == "spirit_field_status":
        data = await engine.spirit_field_status(user_id)
        return {"type": "spirit_field_data", "data": data}

    elif msg_type == "spirit_field_claim":
        result = await engine.spirit_field_claim(user_id)
        return {"type": "action_result", "action": "spirit_field_claim", "data": result}

    elif msg_type == "spirit_field_seeds":
        data = await engine.spirit_field_seeds(user_id)
        return {"type": "spirit_field_seeds_data", "data": data}

    elif msg_type == "spirit_field_plant":
        d = msg.get("data", {})
        try:
            plot_index = int(d.get("plot_index", -1))
        except (TypeError, ValueError):
            return {"type": "error", "message": "无效的格子编号"}
        seed_id = str(d.get("seed_id", "")).strip()
        if not seed_id:
            return {"type": "error", "message": "缺少种子ID"}
        result = await engine.spirit_field_plant(user_id, plot_index, seed_id)
        return {"type": "action_result", "action": "spirit_field_plant", "data": result}

    elif msg_type == "spirit_field_harvest":
        d = msg.get("data", {})
        try:
            plot_index = int(d.get("plot_index", -1))
        except (TypeError, ValueError):
            return {"type": "error", "message": "无效的格子编号"}
        result = await engine.spirit_field_harvest(user_id, plot_index)
        return {"type": "action_result", "action": "spirit_field_harvest", "data": result}

    elif msg_type == "spirit_field_warehouse":
        d = msg.get("data", {})
        filter_rarity = int(d.get("filter_rarity", -1))
        search = str(d.get("search", "")).strip()
        data = await engine.spirit_field_warehouse(user_id, filter_rarity, search)
        return {"type": "spirit_field_warehouse_data", "data": data}

    elif msg_type == "spirit_field_withdraw":
        d = msg.get("data", {})
        material_id = str(d.get("material_id", "")).strip()
        try:
            count = max(1, int(d.get("count", 1) or 1))
        except (TypeError, ValueError):
            return {"type": "error", "message": "数量无效"}
        if not material_id:
            return {"type": "error", "message": "缺少材料ID"}
        result = await engine.spirit_field_withdraw(user_id, material_id, count)
        return {"type": "action_result", "action": "spirit_field_withdraw", "data": result}

    else:
        return {"type": "error", "message": f"未知操作: {msg_type}"}
