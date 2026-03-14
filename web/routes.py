"""HTTP 路由：首页、认证 API、状态接口。"""

from __future__ import annotations

import json
import re
from pathlib import Path
import secrets
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from ..game.engine import GameEngine
from .access_guard import get_access_guard

ADMIN_TOKEN_EXPIRY = 7 * 24 * 3600
PAGE_GUARD_TTL_SECONDS = 6 * 3600


def create_router(
    engine: GameEngine,
    access_password: str = "",
    guard_token: str = "",
    admin_account: str = "",
    admin_password: str = "",
    command_prefix: str = "修仙",
    api_rate_limit_1s_count: int = 10000,
) -> APIRouter:
    router = APIRouter()
    static_dir = Path(__file__).parent.parent / "static"
    required_web_password = (access_password or "").strip()
    required_guard_token = (guard_token or "").strip()
    required_admin_account = (admin_account or "").strip()
    required_admin_password = (admin_password or "").strip()
    cmd_login = f"/{command_prefix} 登录"
    admin_tokens: dict[str, float] = {}
    access_guard = get_access_guard()

    # 反爬/限流配置：1 秒内超过阈值才封禁（默认 10000 次，封禁 60 秒）
    try:
        limit_1s_count = int(api_rate_limit_1s_count)
    except (TypeError, ValueError):
        limit_1s_count = 10000
    limit_1s_count = max(100, limit_1s_count)
    default_limit = (limit_1s_count, 1.0)
    auth_limit = (limit_1s_count, 1.0)
    admin_limit = (limit_1s_count, 1.0)
    public_limit = (limit_1s_count, 1.0)
    burst_guard_count = limit_1s_count + 1
    burst_guard_window = 1.0
    block_seconds = 60.0

    def _client_ip(request: Request) -> str:
        """获取客户端 IP（优先反向代理头）。"""
        headers = request.headers
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
                # RFC 7239: Forwarded: for=1.2.3.4;proto=https
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
        if request.client and request.client.host:
            ip = access_guard.normalize_ip(str(request.client.host))
            if ip:
                return ip
            return str(request.client.host)[:64]
        return "unknown"

    def _pick_limit(path: str, ua: str) -> tuple[int, float]:
        """按路径和 UA 选择限流阈值。"""
        if path in {"/api/register", "/api/login", "/api/admin/login"}:
            limit, window = auth_limit
        elif path in {"/api/rankings", "/api/status", "/api/adventure-scenes"}:
            limit, window = public_limit
        elif path.startswith("/api/admin/"):
            limit, window = admin_limit
        else:
            limit, window = default_limit

        return limit, window

    def _check_page_guard(request: Request):
        """校验 HTTP 请求携带的页面级凭证。"""
        if not required_guard_token:
            return

        ip = _client_ip(request)
        ua = str(request.headers.get("user-agent", "")).strip().lower()
        ok, reason = access_guard.validate_page_session(
            secret=required_guard_token,
            page_id=request.headers.get("x-xiuxian-page-id", ""),
            issued_at=request.headers.get("x-xiuxian-page-ts", ""),
            signature=request.headers.get("x-xiuxian-page-sign", ""),
            ip=ip,
            ua=ua,
            client_key=request.cookies.get("xiuxian_page_client", ""),
        )
        if not ok:
            raise HTTPException(status_code=403, detail=reason or "页面凭证无效，请刷新页面")

    async def _page_guard_required(request: Request):
        path = request.url.path
        if not path.startswith("/api/"):
            return
        _check_page_guard(request)

    async def _anti_crawl_guard(request: Request):
        """统一接口反爬守卫。"""
        path = request.url.path
        if not path.startswith("/api/"):
            return

        ip = _client_ip(request)
        ua = str(request.headers.get("user-agent", "")).strip().lower()
        limit, window = _pick_limit(path, ua)
        ok, reason = access_guard.check_http(
            ip=ip,
            path=path,
            ua=ua,
            limit=limit,
            window=window,
            burst_count=burst_guard_count,
            burst_window=burst_guard_window,
            block_seconds=block_seconds,
        )
        if not ok:
            raise HTTPException(status_code=429, detail=reason or "请求过于频繁，请稍后再试")

    router.dependencies.append(Depends(_page_guard_required))
    router.dependencies.append(Depends(_anti_crawl_guard))

    def _check_web_password(body: dict):
        """校验 Web 访问密码（配置为空时不校验）。"""
        if not required_web_password:
            return None
        provided = str(
            body.get("access_password", body.get("admin_password", ""))
        ).strip()
        if not secrets.compare_digest(provided, required_web_password):
            return JSONResponse(
                {"success": False, "message": "访问密码错误"},
                status_code=403,
            )
        return None

    def _create_admin_token() -> str:
        token = secrets.token_urlsafe(32)
        admin_tokens[token] = time.time() + ADMIN_TOKEN_EXPIRY
        return token

    def _verify_admin_token(token: str) -> bool:
        if not token:
            return False
        expires_at = admin_tokens.get(token)
        if not expires_at:
            return False
        if time.time() > expires_at:
            admin_tokens.pop(token, None)
            return False
        return True

    @router.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        """提供游戏主页。"""
        html = (static_dir / "index.html").read_text(encoding="utf-8")
        page_guard = {"enabled": False, "page_id": "", "issued_at": 0, "signature": ""}
        page_client_id = str(request.cookies.get("xiuxian_page_client", "")).strip()
        if not page_client_id:
            page_client_id = secrets.token_hex(16)
        if required_guard_token:
            ip = _client_ip(request)
            ua = str(request.headers.get("user-agent", "")).strip().lower()
            page_guard = access_guard.issue_page_session(
                secret=required_guard_token,
                ip=ip,
                ua=ua,
                client_key=page_client_id,
                ttl_seconds=PAGE_GUARD_TTL_SECONDS,
            )
        bootstrap = (
            "<script>"
            f"window.__XIUXIAN_PAGE_GUARD__ = {json.dumps(page_guard, ensure_ascii=False)};"
            "</script>"
        )
        html = re.sub(r'(<script\b)', f'{bootstrap}\n    \\1', html, count=1)
        response = HTMLResponse(html)
        response.set_cookie(
            key="xiuxian_page_client",
            value=page_client_id,
            max_age=30 * 24 * 3600,
            httponly=True,
            samesite="lax",
        )
        return response

    @router.get("/api/status")
    async def status():
        """健康检查和基础统计。"""
        online = 0
        if engine._ws_manager:
            online = len(engine._ws_manager._connections)
        return {
            "status": "ok",
            "players_total": len(engine._players),
            "players_online": online,
        }

    # ==================== 认证 API ====================

    @router.post("/api/register")
    async def register(request: Request):
        """注册新角色：道号 + 密码。"""
        body = await request.json()
        auth_error = _check_web_password(body)
        if auth_error:
            return auth_error
        name = body.get("name", "").strip()
        password = body.get("password", "")

        result = await engine.register_with_password(name, password)
        if not result["success"]:
            return JSONResponse(result, status_code=400)

        # 自动登录，生成 token
        user_id = result["user_id"]
        token = await engine.auth.create_web_token(user_id)
        return {
            "success": True,
            "message": result["message"],
            "token": token,
            "user_id": user_id,
            "is_admin": False,
        }

    @router.post("/api/login")
    async def login(request: Request):
        """登录：道号 + 密码。"""
        body = await request.json()
        auth_error = _check_web_password(body)
        if auth_error:
            return auth_error
        name = body.get("name", "").strip()
        password = body.get("password", "")

        player = engine.verify_login(name, password)
        if not player:
            return JSONResponse(
                {"success": False, "message": "道号或密码错误"},
                status_code=401,
            )

        token = await engine.auth.create_web_token(player.user_id)
        return {
            "success": True,
            "message": f"欢迎回来，{player.name}",
            "token": token,
            "user_id": player.user_id,
            "is_admin": False,
        }

    @router.post("/api/set-password")
    async def set_password(request: Request):
        """为已有角色设置密码（首次从聊天创建的角色）。"""
        body = await request.json()
        token = body.get("token", "")
        password = body.get("password", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "登录已过期，请重新登录"},
                status_code=401,
            )

        result = await engine.set_password(user_id, password)
        if not result["success"]:
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/bind-key")
    async def get_bind_key(request: Request):
        """获取6位数聊天绑定密钥。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "登录已过期，请重新登录"},
                status_code=401,
            )

        key = await engine.auth.create_bind_key(user_id)
        return {
            "success": True,
            "bind_key": key,
            "message": f"请在QQ中发送：{cmd_login} {key}",
            "expires_in": "7天",
        }

    @router.post("/api/verify-token")
    async def verify_token(request: Request):
        """验证 Web Token 是否有效。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "Token 无效或已过期"},
                status_code=401,
            )

        player = await engine.get_player(user_id)
        if not player:
            return JSONResponse(
                {"success": False, "message": "角色不存在"},
                status_code=404,
            )

        return {
            "success": True,
            "user_id": user_id,
            "name": player.name,
            "is_admin": False,
        }

    # ==================== 签到 API ====================

    @router.post("/api/checkin")
    async def daily_checkin(request: Request):
        """每日签到。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "Token 无效或已过期"},
                status_code=401,
            )

        result = await engine.daily_checkin(user_id)
        return result

    # ==================== 挂机修炼 API ====================

    @router.post("/api/start-afk")
    async def start_afk(request: Request):
        """开始挂机修炼。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "Token 无效或已过期"},
                status_code=401,
            )

        minutes = body.get("minutes", 0)
        try:
            minutes = int(minutes)
        except (TypeError, ValueError):
            return JSONResponse(
                {"success": False, "message": "请输入有效的分钟数"},
                status_code=400,
            )

        result = await engine.start_afk_cultivate(user_id, minutes)
        return result

    @router.post("/api/collect-afk")
    async def collect_afk(request: Request):
        """结算挂机修炼。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "Token 无效或已过期"},
                status_code=401,
            )

        result = await engine.collect_afk_cultivate(user_id)
        return result

    @router.post("/api/cancel-afk")
    async def cancel_afk(request: Request):
        """取消挂机修炼。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "Token 无效或已过期"},
                status_code=401,
            )

        result = await engine.cancel_afk_cultivate(user_id)
        return result

    # ==================== 历练 API ====================

    @router.post("/api/adventure")
    async def do_adventure(request: Request):
        """执行历练。"""
        body = await request.json()
        token = body.get("token", "")

        user_id = engine.auth.verify_web_token(token)
        if not user_id:
            return JSONResponse(
                {"success": False, "message": "Token 无效或已过期"},
                status_code=401,
            )

        result = await engine.adventure(user_id)
        return result

    @router.get("/api/adventure-scenes")
    async def get_scenes():
        """获取历练场景列表。"""
        scenes = await engine.get_adventure_scenes()
        return {"success": True, "scenes": scenes}

    # ==================== 管理员 API ====================

    @router.post("/api/admin/login")
    async def admin_login(request: Request):
        """管理员登录，返回管理员 Token。"""
        body = await request.json()
        auth_error = _check_web_password(body)
        if auth_error:
            return auth_error

        account = str(body.get("account", "")).strip()
        password = str(body.get("password", ""))
        if not required_admin_account or not required_admin_password:
            return JSONResponse(
                {"success": False, "message": "未配置管理员账号或密码"},
                status_code=403,
            )
        if (
            not secrets.compare_digest(account, required_admin_account)
            or not secrets.compare_digest(password, required_admin_password)
        ):
            return JSONResponse(
                {"success": False, "message": "管理员账号或密码错误"},
                status_code=403,
            )

        token = _create_admin_token()
        return {
            "success": True,
            "message": "管理员登录成功",
            "admin_token": token,
            "expires_in": "7天",
            "is_admin": True,
        }

    @router.post("/api/admin/verify-token")
    async def admin_verify_token(request: Request):
        """校验管理员 Token。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse(
                {"success": False, "message": "管理员登录已过期"},
                status_code=401,
            )
        return {"success": True, "is_admin": True}

    @router.post("/api/admin/adventure-scenes/list")
    async def admin_list_adventure_scenes(request: Request):
        """管理员接口：历练场景列表。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        scenes = await engine.admin_list_adventure_scenes()
        return {"success": True, "scenes": scenes}

    @router.post("/api/admin/adventure-scenes/create")
    async def admin_create_adventure_scene(request: Request):
        """管理员接口：新增历练场景。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        result = await engine.admin_create_adventure_scene(
            body.get("category", ""),
            body.get("name", ""),
            body.get("description", ""),
        )
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/adventure-scenes/update")
    async def admin_update_adventure_scene(request: Request):
        """管理员接口：修改历练场景。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        result = await engine.admin_update_adventure_scene(
            body.get("id"),
            body.get("category", ""),
            body.get("name", ""),
            body.get("description", ""),
        )
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/adventure-scenes/delete")
    async def admin_delete_adventure_scene(request: Request):
        """管理员接口：删除历练场景。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        result = await engine.admin_delete_adventure_scene(body.get("id"))
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    # ── 公告管理 ────────────────────────────────────────────
    @router.post("/api/admin/announcements/list")
    async def admin_list_announcements(request: Request):
        """管理员接口：公告列表。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        announcements = await engine.admin_list_announcements()
        return {"success": True, "announcements": announcements}

    @router.post("/api/admin/announcements/create")
    async def admin_create_announcement(request: Request):
        """管理员接口：新增公告。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        result = await engine.admin_create_announcement(
            body.get("title", ""),
            body.get("content", ""),
        )
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/announcements/update")
    async def admin_update_announcement(request: Request):
        """管理员接口：修改公告。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        result = await engine.admin_update_announcement(
            body.get("id"),
            body.get("title", ""),
            body.get("content", ""),
            body.get("enabled", 1),
        )
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/announcements/delete")
    async def admin_delete_announcement(request: Request):
        """管理员接口：删除公告。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        result = await engine.admin_delete_announcement(body.get("id"))
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/heart-methods/list")
    async def admin_list_heart_methods(request: Request):
        """管理员接口：心法列表。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        heart_methods = await engine.admin_list_heart_methods()
        return {"success": True, "heart_methods": heart_methods}

    @router.post("/api/admin/heart-methods/create")
    async def admin_create_heart_method(request: Request):
        """管理员接口：新增心法。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        payload = body.get("heart_method", {})
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "参数 heart_method 无效"}, status_code=400)
        result = await engine.admin_create_heart_method(payload)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/heart-methods/update")
    async def admin_update_heart_method(request: Request):
        """管理员接口：更新心法。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        method_id = str(body.get("method_id", "")).strip()
        payload = body.get("heart_method", {})
        if not method_id:
            return JSONResponse({"success": False, "message": "缺少 method_id"}, status_code=400)
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "参数 heart_method 无效"}, status_code=400)
        result = await engine.admin_update_heart_method(method_id, payload)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/heart-methods/delete")
    async def admin_delete_heart_method(request: Request):
        """管理员接口：删除心法。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        method_id = str(body.get("method_id", "")).strip()
        result = await engine.admin_delete_heart_method(method_id)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/weapons/list")
    async def admin_list_weapons(request: Request):
        """管理员接口：武器/护甲列表。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        weapons = await engine.admin_list_weapons()
        return {"success": True, "weapons": weapons}

    @router.post("/api/admin/weapons/create")
    async def admin_create_weapon(request: Request):
        """管理员接口：新增武器/护甲。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        payload = body.get("weapon", {})
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "参数 weapon 无效"}, status_code=400)
        result = await engine.admin_create_weapon(payload)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/weapons/update")
    async def admin_update_weapon(request: Request):
        """管理员接口：更新武器/护甲。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        equip_id = str(body.get("equip_id", "")).strip()
        payload = body.get("weapon", {})
        if not equip_id:
            return JSONResponse({"success": False, "message": "缺少 equip_id"}, status_code=400)
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "参数 weapon 无效"}, status_code=400)
        result = await engine.admin_update_weapon(equip_id, payload)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/weapons/delete")
    async def admin_delete_weapon(request: Request):
        """管理员接口：删除武器/护甲。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        equip_id = str(body.get("equip_id", "")).strip()
        result = await engine.admin_delete_weapon(equip_id)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/market/list")
    async def admin_list_market(request: Request):
        """管理员接口：坊市记录列表。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)

        try:
            page = int(body.get("page", 1))
        except (TypeError, ValueError):
            page = 1
        try:
            page_size = int(body.get("page_size", 20))
        except (TypeError, ValueError):
            page_size = 20
        status = str(body.get("status", "")).strip()
        keyword = str(body.get("keyword", "")).strip()

        data = await engine.admin_list_market_listings(
            page=page, page_size=page_size, status=status, keyword=keyword,
        )
        return {"success": True, "market": data}

    @router.post("/api/admin/market/create")
    async def admin_create_market(request: Request):
        """管理员接口：新增坊市记录。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        payload = body.get("market", {})
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "参数 market 无效"}, status_code=400)
        result = await engine.admin_create_market_listing(payload)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/market/update")
    async def admin_update_market(request: Request):
        """管理员接口：更新坊市记录。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        listing_id = str(body.get("listing_id", "")).strip()
        payload = body.get("market", {})
        if not listing_id:
            return JSONResponse({"success": False, "message": "缺少 listing_id"}, status_code=400)
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "参数 market 无效"}, status_code=400)
        result = await engine.admin_update_market_listing(listing_id, payload)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/market/delete")
    async def admin_delete_market(request: Request):
        """管理员接口：删除坊市记录。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        listing_id = str(body.get("listing_id", "")).strip()
        result = await engine.admin_delete_market_listing(listing_id)
        if not result.get("success"):
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/ip/list")
    async def admin_list_ips(request: Request):
        """管理员接口：访问IP统计列表。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)

        try:
            page = int(body.get("page", 1))
        except (TypeError, ValueError):
            page = 1
        try:
            page_size = int(body.get("page_size", 20))
        except (TypeError, ValueError):
            page_size = 20
        keyword = str(body.get("keyword", "")).strip()
        raw_blocked_only = body.get("blocked_only", False)
        if isinstance(raw_blocked_only, str):
            blocked_only = raw_blocked_only.strip().lower() in {"1", "true", "yes", "y", "on"}
        else:
            blocked_only = bool(raw_blocked_only)
        data = access_guard.list_ips(
            page=page,
            page_size=page_size,
            keyword=keyword,
            blocked_only=blocked_only,
        )
        return {"success": True, "ips": data}

    @router.post("/api/admin/ip/block")
    async def admin_block_ip(request: Request):
        """管理员接口：手动封禁IP。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        ip = str(body.get("ip", "")).strip()
        if not ip:
            return JSONResponse({"success": False, "message": "缺少IP"}, status_code=400)
        try:
            seconds = int(body.get("seconds", 0))
        except (TypeError, ValueError):
            return JSONResponse({"success": False, "message": "封禁时长必须是整数秒"}, status_code=400)
        if seconds < 0:
            return JSONResponse({"success": False, "message": "封禁时长不能为负数"}, status_code=400)
        reason = str(body.get("reason", "管理员手动封禁")).strip() or "管理员手动封禁"
        ok = access_guard.manual_block(ip, seconds=seconds, reason=reason)
        if not ok:
            return JSONResponse({"success": False, "message": "IP格式无效"}, status_code=400)
        return {"success": True, "message": f"已封禁IP：{ip}"}

    @router.post("/api/admin/ip/unblock")
    async def admin_unblock_ip(request: Request):
        """管理员接口：解除IP封禁。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        ip = str(body.get("ip", "")).strip()
        if not ip:
            return JSONResponse({"success": False, "message": "缺少IP"}, status_code=400)
        ok = access_guard.manual_unblock(ip)
        if not ok:
            return JSONResponse({"success": False, "message": "IP格式无效"}, status_code=400)
        return {"success": True, "message": f"已解除封禁：{ip}"}

    @router.post("/api/admin/overview")
    async def admin_overview(request: Request):
        """管理员首页概览。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse(
                {"success": False, "message": "管理员登录已过期"},
                status_code=401,
            )

        online = 0
        if engine._ws_manager:
            online = len(engine._ws_manager._connections)
        weapon_count = len(await engine.admin_list_weapons())
        heart_method_count = len(await engine.admin_list_heart_methods())
        scene_count = len(await engine.admin_list_adventure_scenes())
        announcement_count = len(await engine.admin_list_announcements())
        return {
            "success": True,
            "overview": {
                "players_total": len(engine._players),
                "players_online": online,
                "admin_account": required_admin_account,
                "weapons_total": weapon_count,
                "heart_methods_total": heart_method_count,
                "scenes_total": scene_count,
                "announcements_total": announcement_count,
            },
        }

    @router.post("/api/admin/players")
    async def admin_players(request: Request):
        """管理员接口：获取所有玩家列表和数据。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse(
                {"success": False, "message": "管理员登录已过期"},
                status_code=401,
            )

        from ..game.constants import get_realm_name

        players_list = []
        for uid, p in engine._players.items():
            players_list.append(
                {
                    "user_id": uid,
                    "name": p.name,
                    "realm": p.realm,
                    "realm_name": get_realm_name(p.realm, p.sub_realm),
                    "exp": p.exp,
                    "hp": p.hp,
                    "max_hp": p.max_hp,
                    "attack": p.attack,
                    "defense": p.defense,
                    "spirit_stones": p.spirit_stones,
                    "has_password": p.password_hash is not None,
                    "created_at": p.created_at,
                    "inventory_count": sum(p.inventory.values()),
                }
            )

        players_list.sort(key=lambda x: x["created_at"])

        # 在线状态标记
        online_ids = engine.get_online_user_ids()
        for p in players_list:
            p["is_online"] = p["user_id"] in online_ids

        return {
            "success": True,
            "total": len(players_list),
            "players": players_list,
        }

    @router.post("/api/admin/player-detail")
    async def admin_player_detail(request: Request):
        """管理员接口：获取单个玩家详细数据（含背包）。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        user_id = body.get("user_id", "")
        detail = engine.get_player_detail(user_id)
        if not detail:
            return JSONResponse({"success": False, "message": "玩家不存在"}, status_code=404)
        online_ids = engine.get_online_user_ids()
        detail["is_online"] = user_id in online_ids
        return {"success": True, "player": detail}

    @router.post("/api/admin/delete-player")
    async def admin_delete_player(request: Request):
        """管理员接口：删除单个玩家。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        user_id = body.get("user_id", "")
        result = await engine.delete_player(user_id)
        if not result["success"]:
            return JSONResponse(result, status_code=404)
        return result

    @router.post("/api/admin/batch-delete")
    async def admin_batch_delete(request: Request):
        """管理员接口：批量删除玩家。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        user_ids = body.get("user_ids", [])
        if not isinstance(user_ids, list) or not user_ids:
            return JSONResponse({"success": False, "message": "请提供要删除的玩家列表"}, status_code=400)
        result = await engine.batch_delete_players(user_ids)
        return result

    @router.post("/api/admin/update-player")
    async def admin_update_player(request: Request):
        """管理员接口：修改玩家数据。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        user_id = body.get("user_id", "")
        updates = body.get("updates", {})
        if not isinstance(updates, dict) or not updates:
            return JSONResponse({"success": False, "message": "无更新数据"}, status_code=400)
        result = await engine.update_player_data(user_id, updates)
        if not result["success"]:
            return JSONResponse(result, status_code=400)
        return result

    @router.post("/api/admin/wipe-data")
    async def admin_wipe_data(request: Request):
        """管理员接口：清空全部游戏数据。"""
        body = await request.json()
        admin_token = str(body.get("admin_token", ""))
        if not _verify_admin_token(admin_token):
            return JSONResponse({"success": False, "message": "管理员登录已过期"}, status_code=401)
        confirm = body.get("confirm", "")
        if confirm != "确认清档":
            return JSONResponse({"success": False, "message": "请传入 confirm='确认清档'"}, status_code=400)
        await engine.clear_all_data(remove_dir=False)
        return {"success": True, "message": "已清空全部游戏数据"}

    # ==================== 公开 API ====================

    @router.get("/api/rankings")
    async def rankings(user_id: str = ""):
        """公开排行榜接口。可传 user_id 获取自己的排名。"""
        all_rankings = engine.get_rankings(limit=999)
        death_rankings = engine.get_death_rankings(limit=10)
        online_rankings = engine.get_online_rankings(limit=50)
        online = 0
        if engine._ws_manager:
            online = len(engine._ws_manager._connections)
        my_rank = None
        if user_id:
            for r in all_rankings:
                player = engine.get_player_by_name(r["name"])
                if player and player.user_id == user_id:
                    my_rank = r
                    break
        return {
            "success": True,
            "total_players": len(engine._players),
            "online_players": online,
            "rankings": all_rankings[:10],
            "death_rankings": death_rankings,
            "online_rankings": online_rankings,
            "my_rank": my_rank,
        }

    return router
