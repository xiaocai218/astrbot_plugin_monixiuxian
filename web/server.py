"""FastAPI Web 服务器：嵌入 AstrBot 事件循环。"""

from __future__ import annotations

import asyncio
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ..game.engine import GameEngine


class WebServer:
    """管理 FastAPI 应用的生命周期。"""

    def __init__(
        self,
        game_engine: GameEngine,
        host: str = "0.0.0.0",
        port: int = 8088,
        access_password: str = "",
        guard_token: str = "",
        admin_account: str = "",
        admin_password: str = "",
        command_prefix: str = "修仙",
        api_rate_limit_1s_count: int = 10000,
        afk_cultivate_max_minutes: int = 60,
    ):
        self.host = host
        self.port = port
        self.game_engine = game_engine
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None

        # 创建 FastAPI 应用
        self.app = FastAPI(title="修仙世界", docs_url=None, redoc_url=None)

        # 静态文件目录
        static_dir = Path(__file__).parent.parent / "static"
        if static_dir.exists():
            self.app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        # 注册路由
        from .routes import create_router
        from .websocket_handler import create_ws_router

        self.app.include_router(
            create_router(
                game_engine,
                access_password=access_password,
                guard_token=guard_token,
                admin_account=admin_account,
                admin_password=admin_password,
                command_prefix=command_prefix,
                api_rate_limit_1s_count=api_rate_limit_1s_count,
                afk_cultivate_max_minutes=afk_cultivate_max_minutes,
            )
        )
        self.app.include_router(
            create_ws_router(
                game_engine,
                guard_token=guard_token,
                command_prefix=command_prefix,
                api_rate_limit_1s_count=api_rate_limit_1s_count,
            )
        )

    async def start(self):
        """在当前事件循环中启动 uvicorn。"""
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())

    async def stop(self):
        """优雅关闭。"""
        ws_manager = getattr(self.game_engine, "_ws_manager", None)
        if ws_manager and hasattr(ws_manager, "stop_chat_cleanup_task"):
            await ws_manager.stop_chat_cleanup_task()
        if self._server:
            self._server.should_exit = True
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                self._task.cancel()
