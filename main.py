"""修仙世界 — AstrBot 修仙游戏插件。"""

from __future__ import annotations

import os
import json
import re
import time
import asyncio

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register

from .game.data_manager import DataManager
from .game.engine import GameEngine
from .game.auth import AuthManager
from .game import renderer

_MISSING = object()
CMD_PREFIX = "修仙"


@register("astrbot_plugin_xiuxian", "xiuxian-dev", "修仙文字RPG游戏，支持聊天指令和网页界面", "0.5.2")
class XiuxianPlugin(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self._plugin_config = config
        self._engine: GameEngine | None = None
        self._web_server = None
        self._web_error_message = ""
        self._image_cache_dir = ""

    def _get_cfg(self, key: str, default):
        """读取插件配置，优先使用插件级配置，兼容不同 AstrBot 版本。"""
        sources = [
            getattr(self, "config", None),
            self._plugin_config,
            getattr(self.context, "config", None),
            getattr(self.context, "_config", None),
        ]
        for source in sources:
            if source is None:
                continue
            try:
                if hasattr(source, "__contains__") and key in source:
                    if hasattr(source, "get"):
                        return source.get(key)
                    return source[key]
            except (KeyError, TypeError, IndexError, AttributeError):
                pass
            try:
                if hasattr(source, "get"):
                    value = source.get(key, _MISSING)
                    if value is not _MISSING:
                        return value
            except (KeyError, TypeError, AttributeError):
                continue
        return default

    def _get_cfg_int(self, key: str, default: int) -> int:
        value = self._get_cfg(key, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            logger.warning(f"修仙世界：配置项 {key}={value} 非法，回退默认值 {default}")
            return default

    def _get_cfg_bool(self, key: str, default: bool) -> bool:
        value = self._get_cfg(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _get_checkin_config(self) -> dict:
        """读取签到、挂机、历练相关配置项。"""
        return {
            "checkin_stones_min": self._get_cfg_int("checkin_stones_min", 20),
            "checkin_stones_max": self._get_cfg_int("checkin_stones_max", 300),
            "checkin_exp_min": self._get_cfg_int("checkin_exp_min", 500),
            "checkin_exp_max": self._get_cfg_int("checkin_exp_max", 5000),
            "checkin_stones_with_pill_min": self._get_cfg_int("checkin_stones_with_pill_min", 10),
            "checkin_stones_with_pill_max": self._get_cfg_int("checkin_stones_with_pill_max", 100),
            "checkin_prob_stones": self._get_cfg_int("checkin_prob_stones", 60),
            "checkin_prob_exp": self._get_cfg_int("checkin_prob_exp", 25),
            "afk_cultivate_max_minutes": self._get_cfg_int("afk_cultivate_max_minutes", 60),
            "adventure_cooldown": self._get_cfg_int("adventure_cooldown", 1800),
        }

    def _cmd(self, sub_cmd: str = "") -> str:
        base = f"/{CMD_PREFIX}"
        if sub_cmd:
            return f"{base} {sub_cmd}"
        return base

    def _render_image_path(self, img_bytes: bytes, tag: str = "img") -> str | None:
        """将渲染后的图片 bytes 落盘，返回本地路径供 image_result 发送。"""
        if not isinstance(img_bytes, (bytes, bytearray)):
            logger.error(f"修仙世界：渲染结果不是 bytes，tag={tag}")
            return None

        cache_dir = self._image_cache_dir or os.path.join(
            "data", "plugin_data", "astrbot_plugin_xiuxian", "render_cache"
        )
        try:
            os.makedirs(cache_dir, exist_ok=True)
            file_name = f"{int(time.time() * 1000)}_{tag}.png"
            file_path = os.path.join(cache_dir, file_name)
            with open(file_path, "wb") as f:
                f.write(img_bytes)

            # 简单清理：避免目录无限增长
            try:
                files = sorted(
                    (
                        os.path.join(cache_dir, x)
                        for x in os.listdir(cache_dir)
                        if x.lower().endswith(".png")
                    ),
                    key=lambda p: os.path.getmtime(p),
                )
                if len(files) > 120:
                    for p in files[: len(files) - 80]:
                        try:
                            os.remove(p)
                        except OSError:
                            pass
            except OSError:
                logger.debug("修仙世界：缓存清理失败")

            return file_path
        except Exception:
            logger.exception(f"修仙世界：图片落盘失败，tag={tag}")
            return None

    async def initialize(self):
        """插件初始化：加载数据、启动游戏引擎和 Web 服务。"""
        # 数据目录
        data_dir = os.path.join("data", "plugin_data", "astrbot_plugin_xiuxian")
        self._image_cache_dir = os.path.join(data_dir, "render_cache")
        os.makedirs(self._image_cache_dir, exist_ok=True)
        data_manager = DataManager(data_dir)
        await data_manager.initialize()

        # 认证管理器（共享数据库连接）
        auth_manager = AuthManager(data_manager.db, data_dir)
        await auth_manager.initialize()

        # 游戏引擎
        cooldown = self._get_cfg_int("cultivate_cooldown", 60)
        self._engine = GameEngine(data_manager, cultivate_cooldown=cooldown)
        self._engine.auth = auth_manager
        self._engine.set_name_reviewer(self._review_name_with_ai)
        self._engine.set_chat_reviewer(self._review_chat_with_ai)
        self._engine.set_sect_name_reviewer(self._review_sect_name_with_ai)
        await self._engine.initialize()
        self._engine._checkin_config = self._get_checkin_config()
        logger.info("修仙世界：游戏引擎已初始化")

        # Web 服务（可配置开关）
        enable_web = self._get_cfg_bool("enable_web", False)
        if enable_web:
            try:
                from .web.server import WebServer
                host = str(self._get_cfg("web_host", "0.0.0.0"))
                port = self._get_cfg_int("web_port", 8088)
                access_pw = str(self._get_cfg("web_access_password", ""))
                guard_token = str(self._get_cfg("web_guard_token", "")).strip()
                if not access_pw:
                    access_pw = str(self._get_cfg("web_admin_password", ""))
                admin_account = str(
                    self._get_cfg("web_admin_account", self._get_cfg("master_account", "admin"))
                )
                admin_pw = str(self._get_cfg("web_admin_password", ""))
                api_rate_limit_1s_count = self._get_cfg_int("api_rate_limit_1s_count", 10000)
                logger.info(
                    f"修仙世界：Web 配置已加载 enable_web={enable_web}, host={host}, port={port}"
                )
                self._web_server = WebServer(
                    self._engine, host=host, port=port,
                    access_password=access_pw,
                    guard_token=guard_token,
                    admin_account=admin_account,
                    admin_password=admin_pw,
                    command_prefix=CMD_PREFIX,
                    api_rate_limit_1s_count=api_rate_limit_1s_count,
                )
                await self._web_server.start()
                self._web_error_message = ""
                logger.info(f"修仙世界：Web 服务已启动 http://{host}:{port}")
            except ModuleNotFoundError as e:
                self._web_server = None
                self._web_error_message = (
                    f"Web 依赖缺失：{e.name}。请在 AstrBot 环境安装 requirements.txt 中依赖后重载插件。"
                )
                logger.error(f"修仙世界：{self._web_error_message}")
            except Exception:
                self._web_server = None
                self._web_error_message = "Web 服务启动失败，请检查端口占用和依赖安装。"
                logger.exception("修仙世界：Web 服务启动失败")

    async def _review_name_with_ai(self, name: str) -> dict:
        """调用 AstrBot 当前模型对道号做安全审核。"""
        get_provider = getattr(self.context, "get_using_provider", None)
        if not callable(get_provider):
            return {"allow": True, "reason": "AI审核器未配置，按本地规则放行"}

        try:
            provider = get_provider()
        except Exception:
            return {"allow": True, "reason": "AI审核器初始化失败，按本地规则放行"}
        if hasattr(provider, "__await__"):
            try:
                provider = await provider
            except Exception:
                return {"allow": True, "reason": "AI审核器初始化失败，按本地规则放行"}

        if not provider or not hasattr(provider, "text_chat"):
            return {"allow": True, "reason": "AI审核器未就绪，按本地规则放行"}

        prompt = (
            "你是修仙游戏道号审核器。\n"
            "请判断该道号是否明显包含以下内容："
            "色情、侮辱谩骂、人身攻击、低俗挑逗。\n"
            "只有在“高度确定违规”时才拒绝；不确定时必须放行。\n"
            "只输出JSON，不要任何额外文本，格式："
            "{\"allow\": true/false, \"reason\": \"一句话原因\"}。\n"
            f"待审核道号：{name}"
        )

        try:
            llm_response = await provider.text_chat(prompt=prompt, contexts=[])
        except Exception:
            logger.exception("修仙世界：道号AI审核调用失败")
            return {"allow": True, "reason": "AI审核服务异常，按本地规则放行"}

        raw = str(
            getattr(llm_response, "completion_text", "")
            or getattr(llm_response, "text", "")
            or llm_response
            or ""
        ).strip()
        if not raw:
            return {"allow": True, "reason": "AI审核结果为空，按本地规则放行"}

        # 优先解析 JSON
        try:
            m = re.search(r"\{[\s\S]*\}", raw)
            if m:
                obj = json.loads(m.group(0))
                allow = bool(obj.get("allow", obj.get("ok", False)))
                reason = str(obj.get("reason", "")).strip()
                return {"allow": allow, "reason": reason}
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass

        # JSON 解析失败时做文本兜底（宽松策略：仅明确违规才拒绝）
        text = raw.lower()
        blocked_signals = (
            "\"allow\":false", "\"allow\": false",
            "判定违规", "明显违规", "拒绝通过", "不予通过",
        )
        passed_signals = (
            "\"allow\":true", "\"allow\": true",
            "allow:true", "allow = true",
            "通过", "合规", "正常", "可用",
        )
        if any(s in text for s in blocked_signals):
            return {"allow": False, "reason": "AI判定道号违规"}
        if any(s in text for s in passed_signals):
            return {"allow": True, "reason": ""}
        return {"allow": True, "reason": "AI审核结果不明确，按本地规则放行"}

    async def _review_sect_name_with_ai(self, name: str) -> dict:
        """调用 AstrBot 当前模型对宗门名称做安全审核。"""
        def local_review() -> dict:
            allow, reason = self._engine._local_name_risk_check(name)
            return {"allow": bool(allow), "reason": str(reason or "").strip()}

        get_provider = getattr(self.context, "get_using_provider", None)
        if not callable(get_provider):
            return local_review()

        try:
            provider = get_provider()
        except Exception:
            return local_review()
        if hasattr(provider, "__await__"):
            try:
                provider = await asyncio.wait_for(provider, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("修仙世界：宗门名AI审核器初始化超时，降级到本地规则")
                return local_review()
            except Exception:
                return local_review()

        if not provider or not hasattr(provider, "text_chat"):
            return local_review()

        prompt = (
            "你是修仙游戏宗门名称审核器。\n"
            "请判断该宗门名称是否明显包含以下内容："
            "色情、侮辱谩骂、歧视攻击、低俗挑逗。\n"
            "注意：修仙风格名称（如血刀门、杀生寺、灭世宗）属于正常风格，应予放行。\n"
            "只有在「高度确定违规」时才拒绝；不确定时必须放行。\n"
            "只输出JSON，不要任何额外文本，格式："
            "{\"allow\": true/false, \"reason\": \"一句话原因\"}。\n"
            f"待审核宗门名称：{name}"
        )

        try:
            llm_response = await asyncio.wait_for(
                provider.text_chat(prompt=prompt, contexts=[]),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            logger.warning("修仙世界：宗门名AI审核超时，降级到本地规则")
            return local_review()
        except Exception:
            logger.exception("修仙世界：宗门名AI审核调用失败")
            return local_review()

        raw = str(
            getattr(llm_response, "completion_text", "")
            or getattr(llm_response, "text", "")
            or llm_response
            or ""
        ).strip()
        if not raw:
            return {"allow": True, "reason": "AI审核结果为空，按本地规则放行"}

        try:
            m = re.search(r"\{[\s\S]*\}", raw)
            if m:
                obj = json.loads(m.group(0))
                allow = bool(obj.get("allow", obj.get("ok", False)))
                reason = str(obj.get("reason", "")).strip()
                return {"allow": allow, "reason": reason}
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass

        text = raw.lower()
        blocked_signals = (
            "\"allow\":false", "\"allow\": false",
            "判定违规", "明显违规", "拒绝通过", "不予通过",
        )
        passed_signals = (
            "\"allow\":true", "\"allow\": true",
            "allow:true", "allow = true",
            "通过", "合规", "正常", "可用",
        )
        if any(s in text for s in blocked_signals):
            return {"allow": False, "reason": "AI判定宗门名违规"}
        if any(s in text for s in passed_signals):
            return {"allow": True, "reason": ""}
        return {"allow": True, "reason": "AI审核结果不明确，按本地规则放行"}

    async def _review_chat_with_ai(self, content: str) -> dict:
        """调用 AstrBot 当前模型对世界频道消息做安全审核。"""
        get_provider = getattr(self.context, "get_using_provider", None)
        if not callable(get_provider):
            return {"allow": True, "reason": ""}

        try:
            provider = get_provider()
        except Exception:
            return {"allow": True, "reason": ""}
        if hasattr(provider, "__await__"):
            try:
                provider = await provider
            except Exception:
                return {"allow": True, "reason": ""}

        if not provider or not hasattr(provider, "text_chat"):
            return {"allow": True, "reason": ""}

        prompt = (
            "你是修仙游戏世界频道聊天内容审核器。\n"
            "请判断以下聊天内容是否包含："
            "脏话、骂人、色情、低俗挑逗、人身攻击。\n"
            "只有在\u201c高度确定违规\u201d时才拒绝；普通聊天和游戏讨论必须放行。\n"
            "只输出JSON，不要任何额外文本，格式："
            '{"allow": true/false, "reason": "一句话原因"}。\n'
            f"待审核内容：{content}"
        )

        try:
            llm_response = await provider.text_chat(prompt=prompt, contexts=[])
        except Exception:
            logger.exception("修仙世界：聊天AI审核调用失败")
            return {"allow": True, "reason": ""}

        raw = str(
            getattr(llm_response, "completion_text", "")
            or getattr(llm_response, "text", "")
            or llm_response
            or ""
        ).strip()
        if not raw:
            return {"allow": True, "reason": ""}

        try:
            m = re.search(r"\{[\s\S]*\}", raw)
            if m:
                obj = json.loads(m.group(0))
                allow = bool(obj.get("allow", obj.get("ok", False)))
                reason = str(obj.get("reason", "")).strip()
                return {"allow": allow, "reason": reason}
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            pass

        text = raw.lower()
        if any(s in text for s in ('"allow":false', '"allow": false', "违规", "拒绝")):
            return {"allow": False, "reason": "消息包含不当内容"}
        return {"allow": True, "reason": ""}

    # ==================== 指令组 ====================

    def _resolve_player_id(self, event: AstrMessageEvent) -> str | None:
        """从聊天事件解析绑定的玩家ID。"""
        chat_user_id = event.get_sender_id()
        if self._engine.auth:
            return self._engine.auth.get_player_id_for_chat(chat_user_id)
        return None

    @filter.command_group(CMD_PREFIX)
    def xiuxian_group(self):
        """修仙世界 — 文字修仙游戏"""
        pass

    # 指令定义表，用于动态帮助输出
    _CMD_HELP = [
        ("帮助", "显示所有可用指令"),
        ("签到", "每日签到领取奖励"),
        ("面板", "查看角色属性面板"),
        ("修炼", "修炼获取经验"),
        ("挂机 <分钟>", "挂机修炼(1~60分钟)"),
        ("结算", "领取挂机修炼收益"),
        ("取消挂机", "随时取消挂机并放弃本次收益"),
        ("历练", "进入秘境副本历练"),
        ("突破", "尝试突破当前境界"),
        ("背包", "查看背包物品"),
        ("使用 <物品名>", "使用消耗品"),
        ("装备 <装备名>", "装备武器或护甲"),
        ("卸下 <武器|护甲>", "卸下已装备的物品"),
        ("查看 <装备|物品|心法> <物品名>", "按类型精确查看详情"),
        ("回收 <装备|物品|心法> <物品名> [数量]", "回收物品获取灵石（推荐按类型精确命中）"),
        ("坊市 [页码]", "查看坊市商品列表"),
        ("上架 <装备|物品|心法> <物品名> <数量> <单价>", "上架物品到坊市（推荐按类型精确命中）"),
        ("购买 <编号>", "从坊市购买商品"),
        ("下架 <编号>", "取消上架商品"),
        ("排行", "查看修仙境界排行榜"),
        ("死亡排行", "查看死亡次数排行榜"),
        ("在线", "查看当前在线修士"),
        ("登录 <密钥>", "用Web端6位密钥绑定角色"),
        ("登出", "解除QQ角色绑定"),
        ("web", "获取网页版链接"),
    ]

    @xiuxian_group.command("帮助")
    async def show_help(self, event: AstrMessageEvent):
        """显示所有可用指令。"""
        img_bytes = renderer.render_help(self._CMD_HELP, CMD_PREFIX)
        img_path = self._render_image_path(img_bytes, "help")
        if not img_path:
            yield event.plain_result("帮助图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("面板")
    async def show_panel(self, event: AstrMessageEvent):
        """查看角色面板。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        panel = await self._engine.get_panel(player_id)
        if not panel:
            yield event.plain_result("角色数据异常，请联系管理员")
            return
        img_bytes = renderer.render_panel(panel)
        img_path = self._render_image_path(img_bytes, "panel")
        if not img_path:
            yield event.plain_result("面板图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("修炼")
    async def cultivate(self, event: AstrMessageEvent):
        """修炼获取经验。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.cultivate(player_id)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("签到")
    async def daily_checkin(self, event: AstrMessageEvent):
        """每日签到领取奖励。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.daily_checkin(player_id)
        if result["success"]:
            img_bytes = renderer.render_checkin(result)
            img_path = self._render_image_path(img_bytes, "checkin")
            if img_path:
                yield event.image_result(img_path)
                return
        yield event.plain_result(result["message"])

    @xiuxian_group.command("挂机")
    async def afk_cultivate(self, event: AstrMessageEvent, minutes: str = ""):
        """开始挂机修炼。"""
        if not minutes.strip():
            yield event.plain_result(f"请指定挂机时长(分钟)，如：{self._cmd('挂机 10')}")
            return
        try:
            mins = int(minutes.strip())
        except ValueError:
            yield event.plain_result("请输入有效的分钟数")
            return
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.start_afk_cultivate(player_id, mins)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("结算")
    async def collect_afk(self, event: AstrMessageEvent):
        """领取挂机修炼收益。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.collect_afk_cultivate(player_id)
        if result["success"]:
            img_bytes = renderer.render_afk_result(result)
            img_path = self._render_image_path(img_bytes, "afk")
            if img_path:
                yield event.image_result(img_path)
                return
        yield event.plain_result(result["message"])

    @xiuxian_group.command("取消挂机")
    async def cancel_afk(self, event: AstrMessageEvent):
        """取消挂机修炼。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.cancel_afk_cultivate(player_id)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("历练")
    async def do_adventure(self, event: AstrMessageEvent):
        """进入秘境副本历练。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.adventure(player_id)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("历练场景")
    async def show_scenes(self, event: AstrMessageEvent):
        """提示旧版历练场景已停用。"""
        yield event.plain_result("旧版历练场景已停用，历练现已改为 Web 端的秘境副本探索。")

    @xiuxian_group.command("突破")
    async def breakthrough(self, event: AstrMessageEvent):
        """尝试突破境界。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.breakthrough(player_id)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("背包")
    async def show_inventory(self, event: AstrMessageEvent):
        """查看背包物品。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        inv = await self._engine.get_inventory(player_id)
        img_bytes = renderer.render_inventory(inv)
        img_path = self._render_image_path(img_bytes, "inventory")
        if not img_path:
            yield event.plain_result("背包图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("使用")
    async def use_item_cmd(self, event: AstrMessageEvent, item_name: str = ""):
        """使用物品。"""
        if not item_name.strip():
            yield event.plain_result(f"请指定物品名，如：{self._cmd('使用 回血丹')}")
            return
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.use_item_by_name(player_id, item_name.strip())
        yield event.plain_result(result["message"])

    @xiuxian_group.command("装备")
    async def equip_cmd(self, event: AstrMessageEvent, item_name: str = ""):
        """装备武器或护甲。"""
        if not item_name.strip():
            yield event.plain_result(f"请指定装备名，如：{self._cmd('装备 铁剑')}")
            return
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.equip_by_name(player_id, item_name.strip())
        yield event.plain_result(result["message"])

    @xiuxian_group.command("卸下")
    async def unequip_cmd(self, event: AstrMessageEvent, slot_name: str = ""):
        """卸下装备。"""
        slot_map = {"武器": "weapon", "护甲": "armor"}
        slot = slot_map.get(slot_name.strip(), "")
        if not slot:
            yield event.plain_result(f"请指定槽位：{self._cmd('卸下 武器')} 或 {self._cmd('卸下 护甲')}")
            return
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        result = await self._engine.unequip_action(player_id, slot)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("查看")
    async def view_item(self, event: AstrMessageEvent, query_type: str = "", item_name: str = ""):
        """按类型查看物品/装备/心法详情。"""
        query_type = query_type.strip()
        item_name = item_name.strip()
        if not query_type or not item_name:
            yield event.plain_result(f"用法：{self._cmd('查看 装备 铁剑')} / {self._cmd('查看 心法 灭世雷诀秘籍')} / {self._cmd('查看 功法 基础剑法卷轴')}")
            return

        type_map = {
            "装备": "equipment",
            "物品": "item",
            "心法": "heart_method",
            "功法": "gongfa",
        }
        target_type = type_map.get(query_type)
        if not target_type:
            yield event.plain_result("类型仅支持：装备 / 物品 / 心法 / 功法")
            return

        detail = self._engine.get_item_detail(item_name, query_type=target_type)
        if not detail:
            yield event.plain_result(f"未找到{query_type}「{item_name}」，请检查名称和类型是否正确")
            return
        img_bytes = renderer.render_item_detail(detail)
        img_path = self._render_image_path(img_bytes, "item_detail")
        if not img_path:
            yield event.plain_result("物品详情渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("回收")
    async def recycle_cmd(self, event: AstrMessageEvent, args: str = ""):
        """回收物品获取灵石。"""
        args = args.strip()
        if not args:
            yield event.plain_result(
                f"用法：{self._cmd('回收 装备 铁剑')} / {self._cmd('回收 装备 铁剑 3')}（兼容旧格式：{self._cmd('回收 铁剑 3')}）"
            )
            return

        user_id = await self._resolve_player_id(event)
        if not user_id:
            yield event.plain_result("你还没有角色，请先创建")
            return

        type_map = {
            "装备": "equipment",
            "物品": "item",
            "心法": "heart_method",
            "功法": "gongfa",
        }
        tokens = args.split()
        if not tokens:
            yield event.plain_result("请输入要回收的物品")
            return

        query_type = None
        count = 1
        item_name = ""
        if tokens[0] in type_map:
            query_type = type_map[tokens[0]]
            if len(tokens) == 1:
                yield event.plain_result("请在类型后输入物品名")
                return
            if len(tokens) >= 3 and tokens[-1].isdigit():
                count = int(tokens[-1])
                item_name = " ".join(tokens[1:-1]).strip()
            else:
                item_name = " ".join(tokens[1:]).strip()
        else:
            if len(tokens) >= 2 and tokens[-1].isdigit():
                count = int(tokens[-1])
                item_name = " ".join(tokens[:-1]).strip()
            else:
                item_name = args

        if not item_name:
            yield event.plain_result("物品名不能为空")
            return

        result = await self._engine.recycle_by_name(user_id, item_name, count, query_type=query_type)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("坊市")
    async def show_market(self, event: AstrMessageEvent, args: str = ""):
        """查看坊市商品列表。"""
        player_id = self._resolve_player_id(event)
        if not player_id:
            yield event.plain_result(f"你还未登录，请先 {self._cmd('登录 <密钥>')}")
            return
        page = 1
        if args.strip().isdigit():
            page = max(1, int(args.strip()))
        data = await self._engine.market_get_listings(page, page_size=8)
        img_bytes = renderer.render_market(
            data["listings"], data["page"], data["total_pages"],
        )
        img_path = self._render_image_path(img_bytes, "market")
        if not img_path:
            yield event.plain_result("坊市图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("上架")
    async def market_list_cmd(self, event: AstrMessageEvent, args: str = ""):
        """上架物品到坊市。"""
        args = args.strip()
        if not args:
            yield event.plain_result(
                f"用法：{self._cmd('上架 装备 铁剑 1 100')}（兼容旧格式：{self._cmd('上架 铁剑 1 100')}）"
            )
            return

        user_id = self._resolve_player_id(event)
        if not user_id:
            yield event.plain_result("你还没有角色，请先创建")
            return

        tokens = args.split()
        if len(tokens) < 3:
            yield event.plain_result(
                f"用法：{self._cmd('上架 装备 铁剑 1 100')}（兼容旧格式：{self._cmd('上架 铁剑 1 100')}）"
            )
            return

        type_map = {
            "装备": "equipment",
            "物品": "item",
            "心法": "heart_method",
            "功法": "gongfa",
        }
        query_type = None

        # 最后两个 token 是数量和单价（支持可选类型前缀）
        try:
            unit_price = int(tokens[-1])
            quantity = int(tokens[-2])
        except ValueError:
            yield event.plain_result("数量和单价必须是整数")
            return

        if tokens[0] in type_map:
            query_type = type_map[tokens[0]]
            item_name = " ".join(tokens[1:-2]).strip()
        else:
            item_name = " ".join(tokens[:-2]).strip()

        if not item_name:
            yield event.plain_result("请输入物品名")
            return

        result = await self._engine.market_list_by_name(
            user_id, item_name, quantity, unit_price, query_type=query_type,
        )
        yield event.plain_result(result["message"])

    @xiuxian_group.command("购买")
    async def market_buy_cmd(self, event: AstrMessageEvent, args: str = ""):
        """从坊市购买商品。"""
        code = args.strip()
        if not code:
            yield event.plain_result(f"用法：{self._cmd('购买 <编号>')}（坊市商品编号）")
            return

        user_id = self._resolve_player_id(event)
        if not user_id:
            yield event.plain_result("你还没有角色，请先创建")
            return

        result = await self._engine.market_buy_by_prefix(user_id, code)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("下架")
    async def market_cancel_cmd(self, event: AstrMessageEvent, args: str = ""):
        """取消上架商品。"""
        code = args.strip()
        if not code:
            yield event.plain_result(f"用法：{self._cmd('下架 <编号>')}（坊市商品编号）")
            return

        user_id = self._resolve_player_id(event)
        if not user_id:
            yield event.plain_result("你还没有角色，请先创建")
            return

        result = await self._engine.market_cancel_by_prefix(user_id, code)
        yield event.plain_result(result["message"])

    @xiuxian_group.command("心法")
    async def list_heart_methods(self, event: AstrMessageEvent):
        """提示心法修炼入口已调整为秘籍掉落。"""
        yield event.plain_result("已取消直接选择心法。\n请通过历练掉落【普通心法秘籍】，再在背包中使用秘籍进行修炼。")

    @xiuxian_group.command("修炼心法")
    async def learn_heart_method_cmd(self, event: AstrMessageEvent, method_name: str = ""):
        """提示心法修炼入口已调整为秘籍掉落。"""
        yield event.plain_result("已取消直接选择心法。\n请在背包中使用历练掉落的【普通心法秘籍】进行修炼。")

    @xiuxian_group.command("web")
    async def web_link(self, event: AstrMessageEvent):
        """获取网页游戏链接。"""
        enable_web = self._get_cfg_bool("enable_web", False)
        if not enable_web:
            yield event.plain_result("Web 游戏界面未启用")
            return
        if not self._web_server:
            msg = self._web_error_message or "Web 服务未启动，请检查插件日志。"
            yield event.plain_result(msg)
            return
        port = self._get_cfg_int("web_port", 8088)
        default_msg = f"修仙世界网页版：http://<服务器IP>:{port}\n在浏览器中打开即可游玩"
        template = str(self._get_cfg("web_link_message", default_msg))
        yield event.plain_result(template.replace("{port}", str(port)))

    @xiuxian_group.command("登录")
    async def chat_login(self, event: AstrMessageEvent, key: str = ""):
        """用Web端密钥登录。"""
        if not key.strip():
            yield event.plain_result(
                "请输入从Web端获取的6位密钥\n"
                f"用法：{self._cmd('登录 123456')}\n"
                "密钥获取方式：在Web端登录后点击「获取QQ绑定密钥」"
            )
            return

        if not self._engine.auth:
            yield event.plain_result("认证系统未初始化")
            return

        chat_user_id = event.get_sender_id()
        player_id = await self._engine.auth.verify_bind_key(key.strip(), chat_user_id)
        if not player_id:
            err = self._engine.auth.last_bind_error or "密钥无效或已过期，请重新从Web端获取"
            yield event.plain_result(err)
            return

        player = await self._engine.get_player(player_id)
        if not player:
            yield event.plain_result("关联角色不存在，请联系管理员")
            return

        player.unified_msg_origin = event.unified_msg_origin
        await self._engine._save_player(player)

        yield event.plain_result(
            f"登录成功！已绑定角色「{player.name}」\n"
            f"绑定有效期：7天\n"
            f"现在可以使用 {self._cmd('面板')}、{self._cmd('修炼')} 等指令了"
        )

    @xiuxian_group.command("登出")
    async def chat_logout(self, event: AstrMessageEvent):
        """解除QQ绑定。"""
        if not self._engine.auth:
            yield event.plain_result("认证系统未初始化")
            return

        chat_user_id = event.get_sender_id()
        player_id = self._engine.auth.get_player_id_for_chat(chat_user_id)
        if not player_id:
            yield event.plain_result("你当前没有绑定角色")
            return

        await self._engine.auth.unbind_chat(chat_user_id)
        yield event.plain_result("已解除角色绑定")

    @xiuxian_group.command("排行")
    async def show_rankings(self, event: AstrMessageEvent):
        """查看修仙境界排行榜。"""
        rankings = self._engine.get_rankings(limit=10)
        total = len(self._engine._players)
        online_ids = self._engine.get_online_user_ids()
        # 找自己的排名
        my_rank = None
        player_id = self._resolve_player_id(event)
        if player_id:
            all_rankings = self._engine.get_rankings(limit=999)
            for r in all_rankings:
                p = self._engine.get_player_by_name(r["name"])
                if p and p.user_id == player_id:
                    my_rank = r
                    break
        img_bytes = renderer.render_ranking(rankings, total, len(online_ids), my_rank)
        img_path = self._render_image_path(img_bytes, "ranking")
        if not img_path:
            yield event.plain_result("排行榜图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("死亡排行")
    async def show_death_rankings(self, event: AstrMessageEvent):
        """查看死亡次数排行榜。"""
        rankings = self._engine.get_death_rankings(limit=10)
        img_bytes = renderer.render_death_ranking(rankings)
        img_path = self._render_image_path(img_bytes, "death_ranking")
        if not img_path:
            yield event.plain_result("死亡排行榜图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    @xiuxian_group.command("在线")
    async def show_online(self, event: AstrMessageEvent):
        """查看当前在线修士。"""
        online_ids = self._engine.get_online_user_ids()
        players_data = []
        for uid in online_ids:
            player = await self._engine.get_player(uid)
            if player:
                from .game.constants import get_realm_name
                players_data.append({
                    "name": player.name,
                    "realm_name": get_realm_name(player.realm, player.sub_realm),
                })
        img_bytes = renderer.render_online(players_data, len(online_ids))
        img_path = self._render_image_path(img_bytes, "online")
        if not img_path:
            yield event.plain_result("在线列表图片渲染失败，请稍后重试")
            return
        yield event.image_result(img_path)

    async def terminate(self):
        """插件销毁：停止 Web 服务、保存数据。"""
        if self._web_server:
            try:
                await self._web_server.stop()
                logger.info("修仙世界：Web 服务已停止")
            except Exception:
                logger.exception("修仙世界：Web 服务停止失败")
        if self._engine:
            await self._engine.shutdown()
            logger.info("修仙世界：数据已保存")
