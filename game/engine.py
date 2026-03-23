"""游戏引擎：调度所有游戏操作的中心。"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import random
import re
import time
from dataclasses import fields
from typing import Awaitable, Callable, Optional

from .constants import (
    REALM_CONFIG, ITEM_REGISTRY, CHECKIN_PILL_WEIGHTS,
    EQUIPMENT_REGISTRY, EQUIPMENT_TIER_NAMES, EquipmentTier,
    HEART_METHOD_REGISTRY, HEART_METHOD_QUALITY_NAMES, MASTERY_LEVELS,
    GONGFA_REGISTRY, GONGFA_TIER_NAMES, MASTERY_MAX,
    get_heart_method_bonus, get_heart_method_manual_id, get_gongfa_scroll_id,
    get_stored_heart_method_item_id, parse_heart_method_manual_id,
    parse_stored_heart_method_item_id, parse_gongfa_scroll_id,
    get_total_gongfa_bonus, can_cultivate_gongfa,
    get_realm_name, has_sub_realm, can_equip, get_realm_heart_methods,
    get_player_base_max_lingqi, get_player_base_stats, get_realm_base_stats, calc_gongfa_lingqi_cost,
    get_max_sub_realm, get_sub_realm_dao_yun_cost, is_high_realm,
    get_nearest_realm_level,
    RealmLevel,
)
from .cultivation import attempt_breakthrough, perform_cultivate
from .data_manager import DataManager
from .auth import AuthManager
from .dungeon import DungeonManager
from .pvp import PvPManager
from .inventory import (
    add_item,
    equip_item,
    unequip_item,
    find_item_id_by_name,
    find_item_ids_by_name,
    get_inventory_display,
    use_item,
    recycle_item,
)
from .models import Player
from . import market as market_mod
from . import shop as shop_mod
from . import sect as sect_mod

logger = logging.getLogger("xiuxian.engine")

_BAD_NAME_KEYWORDS = (
    # 仅保留“高置信度违规词”，避免误伤正常道号
    "色情", "淫秽", "淫荡", "约炮", "援交", "一夜情", "开房", "做爱",
    "嫖娼", "卖淫", "强奸", "轮奸",
    "鸡巴", "阴茎", "阴道", "龟头", "乳房", "大屌",
    "傻逼", "煞笔", "沙比", "脑残", "智障", "狗东西", "去死", "死妈",
    "操你妈", "cnm", "nmsl",
)


class GameEngine:
    """核心游戏引擎，聊天指令和 Web 共用。"""

    def __init__(self, data_manager: DataManager, cultivate_cooldown: int = 60):
        self._data_manager = data_manager
        self._players: dict[str, Player] = {}
        self._player_locks: dict[str, asyncio.Lock] = {}
        self._name_index: dict[str, str] = {}  # {道号: user_id} 用于按名查找
        self._cultivate_cooldown = cultivate_cooldown
        self._checkin_config: dict = {}  # 签到配置，由外部设置
        self._ws_manager = None  # 由 web 层设置
        self._name_reviewer: Callable[[str], Awaitable[dict | tuple | bool]] | None = None
        self._sect_name_reviewer: Callable[[str], Awaitable[dict]] | None = None
        self._chat_reviewer: Callable[[str], Awaitable[dict]] | None = None
        self.auth: AuthManager | None = None
        self._pending_deaths: dict[str, dict] = {}
        self.dungeon = DungeonManager(self)
        self.pvp = PvPManager(self)

    async def initialize(self):
        """加载所有玩家数据到内存。"""
        # 启动时先从独立心法表加载定义，再进入玩家数据归一化流程
        from .constants import set_heart_method_registry, set_equipment_registry, set_gongfa_registry, set_realm_config
        from .pills import clean_expired_buffs

        realms = await self._data_manager.load_realms()
        set_realm_config(realms)
        equipments = await self._data_manager.load_weapons()
        set_equipment_registry(equipments)
        heart_methods = await self._data_manager.load_heart_methods()
        set_heart_method_registry(heart_methods)
        gongfas = await self._data_manager.load_gongfas()
        set_gongfa_registry(gongfas)

        self._players = await self._data_manager.load_all_players()
        normalized = False
        # 构建道号索引
        for uid, player in self._players.items():
            self._name_index[player.name] = uid
            if self._normalize_player_realm_progress(player):
                normalized = True
            realm_stats = get_realm_base_stats(player.realm, player.sub_realm)
            if getattr(player, "permanent_max_hp_bonus", 0) <= 0 and player.max_hp > realm_stats["max_hp"]:
                player.permanent_max_hp_bonus = player.max_hp - realm_stats["max_hp"]
                normalized = True
            if getattr(player, "permanent_attack_bonus", 0) <= 0 and player.attack > realm_stats["attack"]:
                player.permanent_attack_bonus = player.attack - realm_stats["attack"]
                normalized = True
            if getattr(player, "permanent_defense_bonus", 0) <= 0 and player.defense > realm_stats["defense"]:
                player.permanent_defense_bonus = player.defense - realm_stats["defense"]
                normalized = True
            if getattr(player, "permanent_lingqi_bonus", 0) <= 0 and player.lingqi > realm_stats["max_lingqi"]:
                player.permanent_lingqi_bonus = player.lingqi - realm_stats["max_lingqi"]
                normalized = True

            max_lingqi = get_player_base_max_lingqi(player)
            if player.lingqi <= 0 or (player.lingqi == 50 and player.realm > 0):
                player.lingqi = max_lingqi
                normalized = True
            elif player.lingqi > max_lingqi:
                player.lingqi = max_lingqi
                normalized = True
            heart_fix = self._auto_unequip_invalid_heart_method(player, convert_ratio=0.6, force=False)
            removed_gongfas = self._auto_unequip_invalid_gongfa(player)
            if heart_fix.get("removed_name") or removed_gongfas:
                normalized = True
            if clean_expired_buffs(player):
                normalized = True
            # 清理过期心法道具
            if self._clean_expired_heart_methods(player):
                normalized = True
            if self._clamp_player_hp(player):
                normalized = True
        if normalized:
            await self._data_manager.save_all_players(self._players)

        # 启动定时清理任务（过期Token/绑定 + 过期坊市商品）
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

    @staticmethod
    def _clamp_player_hp(player: Player) -> bool:
        """兜底确保玩家气血始终处于合法范围内。"""
        changed = False
        max_hp = max(1, int(getattr(player, "max_hp", 1) or 1))
        if player.max_hp != max_hp:
            player.max_hp = max_hp
            changed = True

        hp = max(0, min(player.max_hp, int(getattr(player, "hp", 0) or 0)))
        if player.hp != hp:
            player.hp = hp
            changed = True
        return changed

    @staticmethod
    def _normalize_player_realm_progress(player: Player) -> bool:
        """将玩家境界进度纠正到当前已配置的合法范围内。"""
        changed = False
        normalized_realm = get_nearest_realm_level(getattr(player, "realm", 0) or 0)
        if player.realm != normalized_realm:
            player.realm = normalized_realm
            changed = True

        current_sub = int(getattr(player, "sub_realm", 0) or 0)
        if has_sub_realm(player.realm):
            normalized_sub = max(0, min(current_sub, get_max_sub_realm(player.realm)))
        else:
            normalized_sub = 0
        if player.sub_realm != normalized_sub:
            player.sub_realm = normalized_sub
            changed = True
        return changed

    @staticmethod
    def _sync_player_base_stats(player: Player) -> bool:
        """按当前境界重新同步玩家基础属性，不改变永久加成。"""
        changed = False
        base_stats = get_player_base_stats(player)
        new_max_hp = max(1, int(base_stats["max_hp"]))
        new_attack = max(0, int(base_stats["attack"]))
        new_defense = max(0, int(base_stats["defense"]))
        new_max_lingqi = max(0, int(base_stats["max_lingqi"]))

        if player.max_hp != new_max_hp:
            player.max_hp = new_max_hp
            changed = True
        if player.attack != new_attack:
            player.attack = new_attack
            changed = True
        if player.defense != new_defense:
            player.defense = new_defense
            changed = True

        hp = max(0, min(player.max_hp, int(getattr(player, "hp", 0) or 0)))
        if player.hp != hp:
            player.hp = hp
            changed = True

        lingqi = max(0, min(new_max_lingqi, int(getattr(player, "lingqi", 0) or 0)))
        if player.lingqi != lingqi:
            player.lingqi = lingqi
            changed = True
        return changed

    async def get_or_create_player(self, user_id: str, name: str) -> Player:
        """获取或创建玩家。"""
        if user_id in self._players:
            return self._players[user_id]
        start_realm = get_nearest_realm_level(RealmLevel.MORTAL)
        realm_cfg = REALM_CONFIG.get(start_realm, {})
        player = Player(
            user_id=user_id,
            name=name,
            realm=start_realm,
            hp=realm_cfg["base_hp"],
            max_hp=realm_cfg["base_hp"],
            attack=realm_cfg["base_attack"],
            defense=realm_cfg["base_defense"],
            lingqi=realm_cfg.get("base_lingqi", 50),
        )
        # 新玩家赠送一些物品
        player.inventory["healing_pill"] = 3
        player.inventory["exp_pill"] = 1
        self._players[user_id] = player
        self._name_index[name] = user_id
        await self._save_player(player)
        return player

    async def get_player(self, user_id: str) -> Optional[Player]:
        """获取玩家，不存在返回 None。"""
        return self._players.get(user_id)

    async def cultivate(self, user_id: str) -> dict:
        """修炼操作。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        # 挂机修炼中不可手动修炼
        if player.afk_cultivate_end > time.time():
            remaining = int(player.afk_cultivate_end - time.time())
            mins = remaining // 60
            secs = remaining % 60
            return {"success": False, "message": f"正在挂机修炼中，剩余{mins}分{secs}秒"}

        result = await perform_cultivate(player, self._cultivate_cooldown)
        if result["success"]:
            await self._save_player(player)
        return result

    async def daily_checkin(self, user_id: str) -> dict:
        """每日签到，随机获得灵石、丹药或修为。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        today = datetime.datetime.now().astimezone().date().isoformat()
        if player.last_checkin_date == today:
            return {"success": False, "message": "今日已签到，明天再来吧"}

        cfg = self._checkin_config or {}

        def _to_int(value, default: int) -> int:
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

        def _clamp(value: int, lo: int, hi: int) -> int:
            return max(lo, min(hi, value))

        prob_stones = _clamp(_to_int(cfg.get("checkin_prob_stones", 60), 60), 0, 100)
        prob_exp = _clamp(_to_int(cfg.get("checkin_prob_exp", 25), 25), 0, 100)
        if prob_stones + prob_exp > 100:
            prob_exp = max(0, 100 - prob_stones)

        realm_mult = 1.0 + player.realm * 0.3

        roll = random.randint(1, 100)

        if roll <= prob_stones:
            # 纯灵石
            base_min = _to_int(cfg.get("checkin_stones_min", 20), 20)
            base_max = _to_int(cfg.get("checkin_stones_max", 300), 300)
            base_min, base_max = sorted((max(0, base_min), max(0, base_max)))
            stones = random.randint(
                int(base_min * realm_mult), int(base_max * realm_mult)
            )
            player.spirit_stones += stones
            rewards = f"{stones}灵石"

        elif roll <= prob_stones + prob_exp:
            # 纯修为
            base_min = _to_int(cfg.get("checkin_exp_min", 500), 500)
            base_max = _to_int(cfg.get("checkin_exp_max", 5000), 5000)
            base_min, base_max = sorted((max(0, base_min), max(0, base_max)))
            exp = random.randint(
                int(base_min * realm_mult), int(base_max * realm_mult)
            )
            player.exp += exp
            rewards = f"{exp}修为"

        else:
            # 灵石 + 丹药
            base_min = _to_int(cfg.get("checkin_stones_with_pill_min", 10), 10)
            base_max = _to_int(cfg.get("checkin_stones_with_pill_max", 100), 100)
            base_min, base_max = sorted((max(0, base_min), max(0, base_max)))
            stones = random.randint(
                int(base_min * realm_mult), int(base_max * realm_mult)
            )
            player.spirit_stones += stones

            weighted_items = []
            for item_id, weight in CHECKIN_PILL_WEIGHTS:
                if item_id not in ITEM_REGISTRY:
                    continue
                w = _to_int(weight, 0)
                if w > 0:
                    weighted_items.append((item_id, w))
            if not weighted_items:
                weighted_items = [("healing_pill", 1)]
            pill_ids = [w[0] for w in weighted_items]
            pill_weights = [w[1] for w in weighted_items]
            pill_id = random.choices(pill_ids, weights=pill_weights, k=1)[0]
            await add_item(player, pill_id)
            pill_name = ITEM_REGISTRY[pill_id].name

            rewards = f"{stones}灵石和一颗{pill_name}"

        player.last_checkin_date = today
        await self._save_player(player)

        return {
            "success": True,
            "message": f"签到成功！今日获取{rewards}",
            "rewards": rewards,
        }

    async def start_afk_cultivate(self, user_id: str, minutes: int) -> dict:
        """开始挂机修炼。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        async with self._get_player_lock(user_id):
            if self.has_pending_death(user_id):
                return {"success": False, "message": "你已道陨，请先确认重生后再挂机修炼"}

            try:
                max_minutes = int(self._checkin_config.get("afk_cultivate_max_minutes", 60))
            except (TypeError, ValueError):
                max_minutes = 60
            if max_minutes < 1:
                max_minutes = 60
            if minutes < 1 or minutes > max_minutes:
                return {"success": False, "message": f"挂机时长须在1~{max_minutes}分钟之间"}

            now = time.time()
            if player.afk_cultivate_end > 0:
                if player.afk_cultivate_end > now:
                    remaining = int(player.afk_cultivate_end - now)
                    mins = remaining // 60
                    secs = remaining % 60
                    return {"success": False, "message": f"正在挂机修炼中，剩余{mins}分{secs}秒"}
                return {"success": False, "message": "你有已完成的挂机修炼尚未结算，请先使用「结算」领取收益"}

            player.afk_cultivate_start = now
            player.afk_cultivate_end = now + minutes * 60
            await self._save_player(player)

        return {
            "success": True,
            "message": f"开始挂机修炼{minutes}分钟，完成后请使用「结算」领取收益",
            "minutes": minutes,
            "end_time": player.afk_cultivate_end,
        }

    async def collect_afk_cultivate(self, user_id: str) -> dict:
        """结算挂机修炼收益。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        async with self._get_player_lock(user_id):
            # 在锁内校验死亡状态，防止与 confirm_death 竞态
            if self.has_pending_death(user_id):
                return {"success": False, "message": "你已道陨，请先确认重生后再结算挂机收益"}

            if player.afk_cultivate_end <= 0:
                return {"success": False, "message": "当前没有挂机修炼记录"}

            now = time.time()
            if now < player.afk_cultivate_end:
                remaining = int(player.afk_cultivate_end - now)
                mins = remaining // 60
                secs = remaining % 60
                return {"success": False, "message": f"挂机修炼尚未完成，剩余{mins}分{secs}秒"}

            # 计算挂机时长（分钟）
            duration_sec = player.afk_cultivate_end - player.afk_cultivate_start
            duration_min = max(1, int(duration_sec / 60))

            # 经验公式：取修炼单次经验的平均值 × 分钟数
            normalized_realm = get_nearest_realm_level(player.realm)
            if normalized_realm != player.realm:
                player.realm = normalized_realm
            if has_sub_realm(player.realm):
                player.sub_realm = max(0, min(int(player.sub_realm), get_max_sub_realm(player.realm)))
            else:
                player.sub_realm = 0
            realm_cfg = REALM_CONFIG.get(player.realm)
            if not realm_cfg:
                return {"success": False, "message": "当前境界配置无效，无法结算挂机修炼"}
            base_min = 10 + player.realm * 5 + player.sub_realm * 2
            base_max = 30 + player.realm * 10 + player.sub_realm * 4
            if player.realm >= RealmLevel.GOLDEN_CORE:
                base_min *= 10
                base_max *= 10
            avg_exp_per_min = (base_min + base_max) // 2
            total_exp = avg_exp_per_min * duration_min

            # 心法经验加成
            from .constants import get_heart_method_bonus, HEART_METHOD_REGISTRY, MASTERY_MAX
            hm_bonus = get_heart_method_bonus(player.heart_method, player.heart_method_mastery)
            if hm_bonus["exp_multiplier"] > 0:
                total_exp = int(total_exp * (1.0 + hm_bonus["exp_multiplier"]))

            player.exp += total_exp

            extra_msgs: list[str] = []

            # 挂机期间心法经验（每分钟1点基础）
            hm = HEART_METHOD_REGISTRY.get(player.heart_method)
            if hm and player.heart_method_mastery < MASTERY_MAX:
                hm_exp_gain = duration_min * (1 + hm.quality)
                player.heart_method_exp += hm_exp_gain
                while (player.heart_method_mastery < MASTERY_MAX
                       and player.heart_method_exp >= hm.mastery_exp):
                    player.heart_method_exp -= hm.mastery_exp
                    player.heart_method_mastery += 1
                    from .constants import MASTERY_LEVELS
                    extra_msgs.append(
                        f"心法【{hm.name}】修炼至{MASTERY_LEVELS[player.heart_method_mastery]}！"
                    )

            # 挂机期间功法经验（每分钟1点/功法，境界不够则不涨）
            for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
                gongfa_id = getattr(player, slot, "无")
                if not gongfa_id or gongfa_id == "无":
                    continue
                gf = GONGFA_REGISTRY.get(gongfa_id)
                if not gf:
                    continue
                if not can_cultivate_gongfa(player.realm, gf.tier):
                    extra_msgs.append(f"功法【{gf.name}】需更高境界方可继续修炼")
                    continue
                mastery_attr = f"{slot}_mastery"
                exp_attr = f"{slot}_exp"
                mastery = getattr(player, mastery_attr, 0)
                if mastery >= MASTERY_MAX:
                    continue
                player_exp = getattr(player, exp_attr, 0) + duration_min
                while player_exp >= gf.mastery_exp and mastery < MASTERY_MAX:
                    if gf.tier >= 2 and mastery == 2 and gf.dao_yun_cost > 0:
                        if player.dao_yun < gf.dao_yun_cost:
                            player_exp = gf.mastery_exp - 1
                            break
                        player.dao_yun -= gf.dao_yun_cost
                        extra_msgs.append(f"消耗道韵{gf.dao_yun_cost}，助功法【{gf.name}】突破")
                    player_exp -= gf.mastery_exp
                    mastery += 1
                    if mastery <= MASTERY_MAX:
                        from .constants import MASTERY_LEVELS
                        extra_msgs.append(f"功法【{gf.name}】修炼至{MASTERY_LEVELS[mastery]}！")
                setattr(player, mastery_attr, mastery)
                setattr(player, exp_attr, max(0, int(player_exp)))

            # 挂机期间功法回血/回灵
            gf_total = get_total_gongfa_bonus(player)
            if gf_total["hp_regen"] > 0 and player.hp < player.max_hp:
                heal = min(gf_total["hp_regen"] * duration_min, player.max_hp - player.hp)
                player.hp += heal
                extra_msgs.append(f"功法回血+{heal}")
            if gf_total["lingqi_regen"] > 0:
                regen = gf_total["lingqi_regen"] * duration_min
                max_lq = get_player_base_max_lingqi(player)
                actual = min(regen, max(0, max_lq - player.lingqi))
                if actual > 0:
                    player.lingqi += actual
                    extra_msgs.append(f"功法回灵+{actual}")

            # 挂机期间道韵（仅化神期及以上心法生效）
            if hm and hm.realm >= RealmLevel.DEITY_TRANSFORM and hm_bonus["dao_yun_rate"] > 0:
                dao_gain = int(duration_min * hm_bonus["dao_yun_rate"] * 0.3)
                if dao_gain > 0:
                    player.dao_yun += dao_gain
                    extra_msgs.append(f"感悟道韵+{dao_gain}")

            # 处理小境界自动升级
            sub_level_ups = 0
            if has_sub_realm(player.realm):
                sub_exp = realm_cfg.get("sub_exp_to_next", 0)
                max_sr = get_max_sub_realm(player.realm)
                while (sub_exp > 0
                       and player.sub_realm < max_sr
                       and player.exp >= sub_exp):
                    # 高阶境界小境界升级需要道韵
                    dao_cost = get_sub_realm_dao_yun_cost(player.realm, player.sub_realm)
                    if dao_cost > 0 and player.dao_yun < dao_cost:
                        break
                    if dao_cost > 0:
                        player.dao_yun -= dao_cost
                        extra_msgs.append(f"消耗道韵{dao_cost}")
                    player.exp -= sub_exp
                    player.sub_realm += 1
                    sub_level_ups += 1
                    hp_bonus = int(realm_cfg["base_hp"] * 0.08)
                    atk_bonus = int(realm_cfg["base_attack"] * 0.06)
                    def_bonus = int(realm_cfg["base_defense"] * 0.06)
                    lingqi_bonus = max(1, int(realm_cfg.get("base_lingqi", 0) * 0.08))
                    player.max_hp += hp_bonus
                    player.hp = player.max_hp
                    player.attack += atk_bonus
                    player.defense += def_bonus
                    player.lingqi += lingqi_bonus

            # 重置挂机状态
            player.afk_cultivate_start = 0.0
            player.afk_cultivate_end = 0.0
            await self._save_player(player)

        realm_name = get_realm_name(player.realm, player.sub_realm)
        msg = f"挂机修炼{duration_min}分钟完成！获得{total_exp}修为"
        if sub_level_ups > 0:
            extra_msgs.append(f"境界提升！当前：{realm_name}")
        if extra_msgs:
            msg += "\n" + "，".join(extra_msgs)

        return {
            "success": True,
            "message": msg,
            "exp_gained": total_exp,
            "duration_min": duration_min,
            "sub_level_ups": sub_level_ups,
            "realm_name": realm_name,
        }

    async def cancel_afk_cultivate(self, user_id: str) -> dict:
        """取消挂机修炼并放弃本次挂机收益。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        async with self._get_player_lock(user_id):
            if player.afk_cultivate_end <= 0:
                return {"success": False, "message": "当前没有挂机修炼记录"}

            now = time.time()
            if now < player.afk_cultivate_end:
                remaining = int(player.afk_cultivate_end - now)
                mins = remaining // 60
                secs = remaining % 60
                msg = f"已取消挂机修炼（原剩余{mins}分{secs}秒），本次挂机收益已放弃"
            else:
                msg = "已取消未结算的挂机修炼，本次挂机收益已放弃"

            player.afk_cultivate_start = 0.0
            player.afk_cultivate_end = 0.0
            await self._save_player(player)
        return {"success": True, "message": msg}

    async def adventure(self, user_id: str) -> dict:
        """历练操作：改为进入秘境副本探索。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        if player.realm < RealmLevel.QI_REFINING:
            return {"success": False, "message": "修为尚浅，至少需要练气期才能历练"}

        if self.dungeon.has_active_session(user_id) or self.pvp.get_session_for_player(user_id):
            return await self.dungeon.start(player)

        cfg = self._checkin_config or {}
        try:
            cooldown = int(cfg.get("adventure_cooldown", 1800))
        except (TypeError, ValueError):
            cooldown = 1800
        cooldown = max(0, cooldown)

        now = time.time()
        if cooldown > 0 and player.last_adventure_time > 0:
            remaining = cooldown - (now - player.last_adventure_time)
            if remaining > 0:
                remaining = int(remaining)
                mins = remaining // 60
                secs = remaining % 60
                if mins > 0:
                    return {"success": False, "message": f"历练冷却中，请等待{mins}分{secs}秒后再试"}
                return {"success": False, "message": f"历练冷却中，请等待{secs}秒后再试"}

        return await self.dungeon.start(player)

    async def get_adventure_scenes(self) -> list[dict]:
        """获取所有历练场景列表。"""
        return await self._data_manager.get_adventure_scenes()

    async def _reload_runtime_registries(self):
        """从数据库重载境界、装备、心法、功法定义到运行时注册表。"""
        from .constants import set_equipment_registry, set_heart_method_registry, set_gongfa_registry, set_realm_config

        realms = await self._data_manager.load_realms()
        set_realm_config(realms)
        equipments = await self._data_manager.load_weapons()
        set_equipment_registry(equipments)
        heart_methods = await self._data_manager.load_heart_methods()
        set_heart_method_registry(heart_methods)
        gongfas = await self._data_manager.load_gongfas()
        set_gongfa_registry(gongfas)

    async def _normalize_players_after_registry_change(self):
        """当定义表变化后，归一化玩家境界与装备/心法状态。"""
        changed = False
        for player in self._players.values():
            realm_changed = self._normalize_player_realm_progress(player)
            stats_changed = self._sync_player_base_stats(player)
            removed = self._auto_unequip_invalid_equipment(player)
            heart_fix = self._auto_unequip_invalid_heart_method(player, convert_ratio=0.0, force=False)
            removed_gongfas = self._auto_unequip_invalid_gongfa(player)
            hp_changed = self._clamp_player_hp(player)
            if realm_changed or stats_changed or removed or heart_fix.get("removed_name") or removed_gongfas or hp_changed:
                changed = True
        if changed:
            await self._data_manager.save_all_players(self._players)

    async def admin_list_adventure_scenes(self) -> list[dict]:
        return await self._data_manager.admin_list_adventure_scenes()

    async def admin_create_adventure_scene(self, category: str, name: str, description: str) -> dict:
        category = str(category or "").strip()
        name = str(name or "").strip()
        description = str(description or "").strip()
        if not category or not name or not description:
            return {"success": False, "message": "分类、场景名、描述不能为空"}
        if await self._data_manager.admin_has_adventure_scene_name(name):
            return {"success": False, "message": f"历练场景名称「{name}」已存在，禁止重名"}
        scene_id = await self._data_manager.admin_create_adventure_scene(category, name, description)
        return {"success": True, "message": "历练场景已新增", "id": scene_id}

    async def admin_update_adventure_scene(self, scene_id: int, category: str, name: str, description: str) -> dict:
        try:
            scene_id = int(scene_id)
        except (TypeError, ValueError):
            return {"success": False, "message": "场景ID无效"}
        category = str(category or "").strip()
        name = str(name or "").strip()
        description = str(description or "").strip()
        if not category or not name or not description:
            return {"success": False, "message": "分类、场景名、描述不能为空"}
        ok = await self._data_manager.admin_update_adventure_scene(scene_id, category, name, description)
        if not ok:
            return {"success": False, "message": "场景不存在"}
        return {"success": True, "message": "历练场景已更新"}

    async def admin_delete_adventure_scene(self, scene_id: int) -> dict:
        try:
            scene_id = int(scene_id)
        except (TypeError, ValueError):
            return {"success": False, "message": "场景ID无效"}
        ok = await self._data_manager.admin_delete_adventure_scene(scene_id)
        if not ok:
            return {"success": False, "message": "场景不存在"}
        return {"success": True, "message": "历练场景已删除"}

    # ── 公告管理 ────────────────────────────────────────────
    async def get_active_announcements(self) -> list[dict]:
        return await self._data_manager.get_active_announcements()

    async def admin_list_announcements(self) -> list[dict]:
        return await self._data_manager.admin_list_announcements()

    async def admin_create_announcement(self, title: str, content: str) -> dict:
        title = str(title or "").strip()
        content = str(content or "").strip()
        if not title or not content:
            return {"success": False, "message": "标题和内容不能为空"}
        ann_id = await self._data_manager.admin_create_announcement(title, content)
        return {"success": True, "message": "公告已创建", "id": ann_id}

    async def admin_update_announcement(self, ann_id: int, title: str, content: str, enabled: int) -> dict:
        try:
            ann_id = int(ann_id)
        except (TypeError, ValueError):
            return {"success": False, "message": "公告ID无效"}
        title = str(title or "").strip()
        content = str(content or "").strip()
        if not title or not content:
            return {"success": False, "message": "标题和内容不能为空"}
        enabled = 1 if int(enabled or 0) else 0
        ok = await self._data_manager.admin_update_announcement(ann_id, title, content, enabled)
        if not ok:
            return {"success": False, "message": "公告不存在"}
        return {"success": True, "message": "公告已更新"}

    async def admin_delete_announcement(self, ann_id: int) -> dict:
        try:
            ann_id = int(ann_id)
        except (TypeError, ValueError):
            return {"success": False, "message": "公告ID无效"}
        ok = await self._data_manager.admin_delete_announcement(ann_id)
        if not ok:
            return {"success": False, "message": "公告不存在"}
        return {"success": True, "message": "公告已删除"}

    async def admin_list_heart_methods(self) -> list[dict]:
        rows = await self._data_manager.admin_list_heart_methods()
        result = []
        for row in rows:
            item = dict(row)
            quality = int(item.get("quality", 0))
            realm = int(item.get("realm", 0))
            item["quality_name"] = HEART_METHOD_QUALITY_NAMES.get(quality, str(quality))
            item["realm_name"] = get_realm_name(realm, 0)
            item["enabled"] = 1 if int(item.get("enabled", 1)) else 0
            result.append(item)
        return result

    @staticmethod
    def _normalize_enabled_flag(value) -> int:
        if isinstance(value, str):
            v = value.strip().lower()
            if v in {"0", "false", "off", "no"}:
                return 0
            return 1
        return 0 if int(value or 0) == 0 else 1

    async def admin_create_heart_method(self, payload: dict) -> dict:
        method_id = str(payload.get("method_id", "")).strip()
        if not re.fullmatch(r"[a-z0-9_]{3,64}", method_id):
            return {"success": False, "message": "心法ID仅支持3-64位小写字母/数字/下划线"}
        data = {
            "method_id": method_id,
            "name": str(payload.get("name", "")).strip(),
            "realm": int(payload.get("realm", 0)),
            "quality": int(payload.get("quality", 0)),
            "exp_multiplier": float(payload.get("exp_multiplier", 0.0)),
            "attack_bonus": int(payload.get("attack_bonus", 0)),
            "defense_bonus": int(payload.get("defense_bonus", 0)),
            "dao_yun_rate": float(payload.get("dao_yun_rate", 0.0)),
            "description": str(payload.get("description", "")),
            "mastery_exp": int(payload.get("mastery_exp", 100)),
            "enabled": self._normalize_enabled_flag(payload.get("enabled", 1)),
        }
        if not data["name"]:
            return {"success": False, "message": "心法名称不能为空"}
        if data["realm"] not in REALM_CONFIG:
            return {"success": False, "message": "境界值无效"}
        if data["quality"] not in {0, 1, 2}:
            return {"success": False, "message": "品质值无效"}
        if data["mastery_exp"] <= 0:
            return {"success": False, "message": "修炼阶段经验阈值必须大于0"}
        if await self._data_manager.admin_has_heart_method_name(data["name"]):
            return {"success": False, "message": f"心法名称「{data['name']}」已存在，禁止重名"}
        ok = await self._data_manager.admin_create_heart_method(data)
        if not ok:
            return {"success": False, "message": "心法ID已存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "心法已新增"}

    async def admin_update_heart_method(self, method_id: str, payload: dict) -> dict:
        method_id = str(method_id or "").strip()
        if not method_id:
            return {"success": False, "message": "缺少心法ID"}
        data = {
            "name": str(payload.get("name", "")).strip(),
            "realm": int(payload.get("realm", 0)),
            "quality": int(payload.get("quality", 0)),
            "exp_multiplier": float(payload.get("exp_multiplier", 0.0)),
            "attack_bonus": int(payload.get("attack_bonus", 0)),
            "defense_bonus": int(payload.get("defense_bonus", 0)),
            "dao_yun_rate": float(payload.get("dao_yun_rate", 0.0)),
            "description": str(payload.get("description", "")),
            "mastery_exp": int(payload.get("mastery_exp", 100)),
            "enabled": self._normalize_enabled_flag(payload.get("enabled", 1)),
        }
        if not data["name"]:
            return {"success": False, "message": "心法名称不能为空"}
        if data["realm"] not in REALM_CONFIG:
            return {"success": False, "message": "境界值无效"}
        if data["quality"] not in {0, 1, 2}:
            return {"success": False, "message": "品质值无效"}
        if data["mastery_exp"] <= 0:
            return {"success": False, "message": "修炼阶段经验阈值必须大于0"}
        ok = await self._data_manager.admin_update_heart_method(method_id, data)
        if not ok:
            return {"success": False, "message": "心法不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "心法已更新"}

    async def admin_delete_heart_method(self, method_id: str) -> dict:
        method_id = str(method_id or "").strip()
        if not method_id:
            return {"success": False, "message": "缺少心法ID"}
        ok = await self._data_manager.admin_delete_heart_method(method_id)
        if not ok:
            return {"success": False, "message": "心法不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "心法已删除"}

    async def admin_list_gongfas(self) -> list[dict]:
        rows = await self._data_manager.admin_list_gongfas()
        result = []
        for row in rows:
            item = dict(row)
            tier = int(item.get("tier", 0))
            item["tier_name"] = GONGFA_TIER_NAMES.get(tier, str(tier))
            item["enabled"] = 1 if int(item.get("enabled", 1)) else 0
            result.append(item)
        return result

    async def admin_create_gongfa(self, payload: dict) -> dict:
        gongfa_id = str(payload.get("gongfa_id", "")).strip()
        if not re.fullmatch(r"[a-z0-9_]{3,64}", gongfa_id):
            return {"success": False, "message": "功法ID仅支持3-64位小写字母/数字/下划线"}
        data = {
            "gongfa_id": gongfa_id,
            "name": str(payload.get("name", "")).strip(),
            "tier": int(payload.get("tier", 0)),
            "attack_bonus": int(payload.get("attack_bonus", 0)),
            "defense_bonus": int(payload.get("defense_bonus", 0)),
            "hp_regen": int(payload.get("hp_regen", 0)),
            "lingqi_regen": int(payload.get("lingqi_regen", 0)),
            "description": str(payload.get("description", "")),
            "mastery_exp": int(payload.get("mastery_exp", 200)),
            "dao_yun_cost": int(payload.get("dao_yun_cost", 0)),
            "recycle_price": int(payload.get("recycle_price", 1000)),
            "lingqi_cost": int(payload.get("lingqi_cost", 0) or 0),
            "enabled": self._normalize_enabled_flag(payload.get("enabled", 1)),
        }
        if not data["name"]:
            return {"success": False, "message": "功法名称不能为空"}
        if data["tier"] not in {0, 1, 2, 3}:
            return {"success": False, "message": "品阶值无效（0-3）"}
        if data["mastery_exp"] <= 0:
            return {"success": False, "message": "修炼阶段经验阈值必须大于0"}
        if data["lingqi_cost"] <= 0:
            data["lingqi_cost"] = calc_gongfa_lingqi_cost(
                data["tier"],
                data["attack_bonus"],
                data["defense_bonus"],
                data["hp_regen"],
                data["lingqi_regen"],
            )
        if await self._data_manager.admin_has_gongfa_name(data["name"]):
            return {"success": False, "message": f"功法名称「{data['name']}」已存在，禁止重名"}
        ok = await self._data_manager.admin_create_gongfa(data)
        if not ok:
            return {"success": False, "message": "功法ID已存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "功法已新增"}

    async def admin_update_gongfa(self, gongfa_id: str, payload: dict) -> dict:
        gongfa_id = str(gongfa_id or "").strip()
        if not gongfa_id:
            return {"success": False, "message": "缺少功法ID"}
        data = {
            "name": str(payload.get("name", "")).strip(),
            "tier": int(payload.get("tier", 0)),
            "attack_bonus": int(payload.get("attack_bonus", 0)),
            "defense_bonus": int(payload.get("defense_bonus", 0)),
            "hp_regen": int(payload.get("hp_regen", 0)),
            "lingqi_regen": int(payload.get("lingqi_regen", 0)),
            "description": str(payload.get("description", "")),
            "mastery_exp": int(payload.get("mastery_exp", 200)),
            "dao_yun_cost": int(payload.get("dao_yun_cost", 0)),
            "recycle_price": int(payload.get("recycle_price", 1000)),
            "lingqi_cost": int(payload.get("lingqi_cost", 0) or 0),
            "enabled": self._normalize_enabled_flag(payload.get("enabled", 1)),
        }
        if not data["name"]:
            return {"success": False, "message": "功法名称不能为空"}
        if data["tier"] not in {0, 1, 2, 3}:
            return {"success": False, "message": "品阶值无效（0-3）"}
        if data["mastery_exp"] <= 0:
            return {"success": False, "message": "修炼阶段经验阈值必须大于0"}
        if data["lingqi_cost"] <= 0:
            data["lingqi_cost"] = calc_gongfa_lingqi_cost(
                data["tier"],
                data["attack_bonus"],
                data["defense_bonus"],
                data["hp_regen"],
                data["lingqi_regen"],
            )
        ok = await self._data_manager.admin_update_gongfa(gongfa_id, data)
        if not ok:
            return {"success": False, "message": "功法不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "功法已更新"}

    async def admin_delete_gongfa(self, gongfa_id: str) -> dict:
        gongfa_id = str(gongfa_id or "").strip()
        if not gongfa_id:
            return {"success": False, "message": "缺少功法ID"}
        ok = await self._data_manager.admin_delete_gongfa(gongfa_id)
        if not ok:
            return {"success": False, "message": "功法不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "功法已删除"}

    # ---- 境界管理 CRUD ----

    async def admin_list_realms(self) -> list[dict]:
        return await self._data_manager.admin_list_realms()

    async def admin_create_realm(self, payload: dict) -> dict:
        try:
            level = int(payload.get("level", -1))
        except (TypeError, ValueError):
            return {"success": False, "message": "境界等级必须为整数"}
        if level < 0:
            return {"success": False, "message": "境界等级不能为负数"}
        name = str(payload.get("name", "")).strip()
        if not name:
            return {"success": False, "message": "境界名称不能为空"}
        if await self._data_manager.admin_has_realm_name(name):
            return {"success": False, "message": f"境界名称「{name}」已存在，禁止重名"}
        data = {
            "level": level,
            "name": name,
            "has_sub_realm": self._normalize_enabled_flag(payload.get("has_sub_realm", 0)),
            "high_realm": self._normalize_enabled_flag(payload.get("high_realm", 0)),
            "exp_to_next": max(0, int(payload.get("exp_to_next", 100))),
            "sub_exp_to_next": max(0, int(payload.get("sub_exp_to_next", 0))),
            "base_hp": max(1, int(payload.get("base_hp", 100))),
            "base_attack": max(0, int(payload.get("base_attack", 10))),
            "base_defense": max(0, int(payload.get("base_defense", 5))),
            "base_lingqi": max(1, int(payload.get("base_lingqi", 50))),
            "breakthrough_rate": max(0.0, min(1.0, float(payload.get("breakthrough_rate", 1.0)))),
            "death_rate": max(0.0, min(1.0, float(payload.get("death_rate", 0.0)))),
            "sub_dao_yun_costs": str(payload.get("sub_dao_yun_costs", "")),
            "breakthrough_dao_yun_cost": max(0, int(payload.get("breakthrough_dao_yun_cost", 0))),
        }
        ok = await self._data_manager.admin_create_realm(data)
        if not ok:
            return {"success": False, "message": f"境界等级 {level} 已存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "境界已新增"}

    async def admin_update_realm(self, level: int, payload: dict) -> dict:
        name = str(payload.get("name", "")).strip()
        if not name:
            return {"success": False, "message": "境界名称不能为空"}
        if await self._data_manager.admin_has_realm_name(name, exclude_level=level):
            return {"success": False, "message": f"境界名称「{name}」已存在，禁止重名"}
        data = {
            "name": name,
            "has_sub_realm": self._normalize_enabled_flag(payload.get("has_sub_realm", 0)),
            "high_realm": self._normalize_enabled_flag(payload.get("high_realm", 0)),
            "exp_to_next": max(0, int(payload.get("exp_to_next", 100))),
            "sub_exp_to_next": max(0, int(payload.get("sub_exp_to_next", 0))),
            "base_hp": max(1, int(payload.get("base_hp", 100))),
            "base_attack": max(0, int(payload.get("base_attack", 10))),
            "base_defense": max(0, int(payload.get("base_defense", 5))),
            "base_lingqi": max(1, int(payload.get("base_lingqi", 50))),
            "breakthrough_rate": max(0.0, min(1.0, float(payload.get("breakthrough_rate", 1.0)))),
            "death_rate": max(0.0, min(1.0, float(payload.get("death_rate", 0.0)))),
            "sub_dao_yun_costs": str(payload.get("sub_dao_yun_costs", "")),
            "breakthrough_dao_yun_cost": max(0, int(payload.get("breakthrough_dao_yun_cost", 0))),
        }
        ok = await self._data_manager.admin_update_realm(level, data)
        if not ok:
            return {"success": False, "message": "境界不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "境界已更新"}

    async def admin_delete_realm(self, level: int) -> dict:
        ok = await self._data_manager.admin_delete_realm(level)
        if not ok:
            return {"success": False, "message": "境界不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "境界已删除"}

    async def get_realm_names(self) -> dict[int, str]:
        """获取境界等级→名称映射。"""
        return await self._data_manager.get_realm_names()

    async def admin_list_weapons(self) -> list[dict]:
        from .constants import EQUIPMENT_TIER_NAMES

        rows = await self._data_manager.admin_list_weapons()
        result = []
        for row in rows:
            item = dict(row)
            tier = int(item.get("tier", 0))
            item["tier_name"] = EQUIPMENT_TIER_NAMES.get(tier, str(tier))
            item["enabled"] = 1 if int(item.get("enabled", 1)) else 0
            result.append(item)
        return result

    async def admin_create_weapon(self, payload: dict) -> dict:
        equip_id = str(payload.get("equip_id", "")).strip()
        if not re.fullmatch(r"[a-z0-9_]{3,64}", equip_id):
            return {"success": False, "message": "武器ID仅支持3-64位小写字母/数字/下划线"}
        slot = str(payload.get("slot", "")).strip().lower()
        if slot not in {"weapon", "armor"}:
            return {"success": False, "message": "槽位仅支持 weapon 或 armor"}
        tier = int(payload.get("tier", 0))
        if tier not in {0, 1, 2, 3}:
            return {"success": False, "message": "品阶值无效"}
        data = {
            "equip_id": equip_id,
            "name": str(payload.get("name", "")).strip(),
            "tier": tier,
            "slot": slot,
            "attack": int(payload.get("attack", 0)),
            "defense": int(payload.get("defense", 0)),
            "element": str(payload.get("element", "无") or "无").strip(),
            "element_damage": int(payload.get("element_damage", 0)),
            "description": str(payload.get("description", "")),
            "enabled": self._normalize_enabled_flag(payload.get("enabled", 1)),
        }
        if not data["name"]:
            return {"success": False, "message": "装备名称不能为空"}
        if await self._data_manager.admin_has_weapon_name(data["name"]):
            return {"success": False, "message": f"装备名称「{data['name']}」已存在，禁止重名"}
        ok = await self._data_manager.admin_create_weapon(data)
        if not ok:
            return {"success": False, "message": "武器ID已存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "装备已新增"}

    async def admin_update_weapon(self, equip_id: str, payload: dict) -> dict:
        equip_id = str(equip_id or "").strip()
        if not equip_id:
            return {"success": False, "message": "缺少武器ID"}
        slot = str(payload.get("slot", "")).strip().lower()
        if slot not in {"weapon", "armor"}:
            return {"success": False, "message": "槽位仅支持 weapon 或 armor"}
        tier = int(payload.get("tier", 0))
        if tier not in {0, 1, 2, 3}:
            return {"success": False, "message": "品阶值无效"}
        data = {
            "name": str(payload.get("name", "")).strip(),
            "tier": tier,
            "slot": slot,
            "attack": int(payload.get("attack", 0)),
            "defense": int(payload.get("defense", 0)),
            "element": str(payload.get("element", "无") or "无").strip(),
            "element_damage": int(payload.get("element_damage", 0)),
            "description": str(payload.get("description", "")),
            "enabled": self._normalize_enabled_flag(payload.get("enabled", 1)),
        }
        if not data["name"]:
            return {"success": False, "message": "装备名称不能为空"}
        ok = await self._data_manager.admin_update_weapon(equip_id, data)
        if not ok:
            return {"success": False, "message": "装备不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "装备已更新"}

    async def admin_delete_weapon(self, equip_id: str) -> dict:
        equip_id = str(equip_id or "").strip()
        if not equip_id:
            return {"success": False, "message": "缺少武器ID"}
        ok = await self._data_manager.admin_delete_weapon(equip_id)
        if not ok:
            return {"success": False, "message": "装备不存在"}
        await self._reload_runtime_registries()
        await self._normalize_players_after_registry_change()
        return {"success": True, "message": "装备已删除"}

    async def admin_list_market_listings(
        self,
        page: int = 1,
        page_size: int = 20,
        status: str = "",
        keyword: str = "",
    ) -> dict:
        """管理员分页查询坊市记录。"""
        st = str(status or "").strip().lower()
        allowed_status = {"", "all", "active", "sold", "cancelled", "expired"}
        if st not in allowed_status:
            st = ""
        if st == "all":
            st = ""

        data = await self._data_manager.admin_list_market_listings(
            page=page,
            page_size=page_size,
            status=st,
            keyword=str(keyword or "").strip(),
        )

        for item in data.get("listings", []):
            item["item_name"] = market_mod.get_item_name(str(item.get("item_id", "")))
            seller = self._players.get(str(item.get("seller_id", "")))
            buyer = self._players.get(str(item.get("buyer_id", "")))
            item["seller_name"] = seller.name if seller else "未知"
            item["buyer_name"] = buyer.name if buyer else "--"
        return data

    async def admin_create_market_listing(self, payload: dict) -> dict:
        """管理员新增坊市记录。"""
        seller_id = str(payload.get("seller_id", "")).strip()
        if not seller_id:
            return {"success": False, "message": "卖家ID不能为空"}
        if seller_id not in self._players:
            return {"success": False, "message": "卖家ID不存在"}

        item_id = str(payload.get("item_id", "")).strip()
        if not item_id:
            return {"success": False, "message": "物品ID不能为空"}
        if item_id not in ITEM_REGISTRY and item_id not in EQUIPMENT_REGISTRY:
            return {"success": False, "message": "物品ID不存在"}

        try:
            quantity = int(payload.get("quantity", 0))
            unit_price = int(payload.get("unit_price", 0))
            fee = int(payload.get("fee", 0))
        except (TypeError, ValueError):
            return {"success": False, "message": "数量/价格/手续费必须为整数"}

        if quantity <= 0:
            return {"success": False, "message": "数量必须大于0"}
        if unit_price < market_mod.MIN_UNIT_PRICE:
            return {"success": False, "message": f"单价不能低于{market_mod.MIN_UNIT_PRICE}"}
        if fee < 0:
            return {"success": False, "message": "手续费不能小于0"}

        status = str(payload.get("status", "active")).strip().lower() or "active"
        if status not in {"active", "sold", "cancelled", "expired"}:
            return {"success": False, "message": "状态仅支持 active/sold/cancelled/expired"}

        now = time.time()
        try:
            listed_at = float(payload.get("listed_at", now))
            expires_at = float(payload.get("expires_at", listed_at + market_mod.LISTING_DURATION))
        except (TypeError, ValueError):
            return {"success": False, "message": "时间戳格式错误"}

        if expires_at <= listed_at:
            return {"success": False, "message": "过期时间必须大于上架时间"}

        buyer_id = str(payload.get("buyer_id", "")).strip()
        sold_at_raw = payload.get("sold_at")
        sold_at = None
        if sold_at_raw not in (None, ""):
            try:
                sold_at = float(sold_at_raw)
            except (TypeError, ValueError):
                return {"success": False, "message": "成交时间格式错误"}

        if status == "sold":
            if not buyer_id:
                return {"success": False, "message": "sold 状态必须提供买家ID"}
            if buyer_id not in self._players:
                return {"success": False, "message": "买家ID不存在"}
            if sold_at is None:
                sold_at = now
        else:
            buyer_id = ""
            sold_at = None

        listing_id = str(payload.get("listing_id", "")).strip().lower()
        if listing_id:
            if not re.fullmatch(r"[a-z0-9_]{6,32}", listing_id):
                return {"success": False, "message": "记录ID仅支持6-32位小写字母/数字/下划线"}
        else:
            import secrets as _s
            listing_id = _s.token_hex(6)

        total_price = quantity * unit_price
        ok = await self._data_manager.admin_create_market_listing({
            "listing_id": listing_id,
            "seller_id": seller_id,
            "item_id": item_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "fee": fee,
            "listed_at": listed_at,
            "expires_at": expires_at,
            "status": status,
            "buyer_id": buyer_id,
            "sold_at": sold_at,
        })
        if not ok:
            return {"success": False, "message": "记录ID已存在"}

        await self._notify_market_changed("admin_create")
        return {"success": True, "message": "坊市记录已新增"}

    async def admin_update_market_listing(self, listing_id: str, payload: dict) -> dict:
        """管理员更新坊市记录。"""
        listing_id = str(listing_id or "").strip().lower()
        if not listing_id:
            return {"success": False, "message": "缺少记录ID"}

        old = await self._data_manager.get_listing_by_id(listing_id)
        if not old:
            return {"success": False, "message": "坊市记录不存在"}

        seller_id = str(payload.get("seller_id", old.get("seller_id", ""))).strip()
        if seller_id not in self._players:
            return {"success": False, "message": "卖家ID不存在"}

        item_id = str(payload.get("item_id", old.get("item_id", ""))).strip()
        if item_id not in ITEM_REGISTRY and item_id not in EQUIPMENT_REGISTRY:
            return {"success": False, "message": "物品ID不存在"}

        try:
            quantity = int(payload.get("quantity", old.get("quantity", 0)))
            unit_price = int(payload.get("unit_price", old.get("unit_price", 0)))
            fee = int(payload.get("fee", old.get("fee", 0)))
        except (TypeError, ValueError):
            return {"success": False, "message": "数量/价格/手续费必须为整数"}

        if quantity <= 0:
            return {"success": False, "message": "数量必须大于0"}
        if unit_price < market_mod.MIN_UNIT_PRICE:
            return {"success": False, "message": f"单价不能低于{market_mod.MIN_UNIT_PRICE}"}
        if fee < 0:
            return {"success": False, "message": "手续费不能小于0"}

        status = str(payload.get("status", old.get("status", "active"))).strip().lower() or "active"
        if status not in {"active", "sold", "cancelled", "expired"}:
            return {"success": False, "message": "状态仅支持 active/sold/cancelled/expired"}

        try:
            listed_at = float(payload.get("listed_at", old.get("listed_at", time.time())))
            expires_at = float(payload.get("expires_at", old.get("expires_at", listed_at + market_mod.LISTING_DURATION)))
        except (TypeError, ValueError):
            return {"success": False, "message": "时间戳格式错误"}

        if expires_at <= listed_at:
            return {"success": False, "message": "过期时间必须大于上架时间"}

        buyer_id = str(payload.get("buyer_id", old.get("buyer_id", ""))).strip()
        sold_at_raw = payload.get("sold_at", old.get("sold_at"))
        sold_at = None
        if sold_at_raw not in (None, ""):
            try:
                sold_at = float(sold_at_raw)
            except (TypeError, ValueError):
                return {"success": False, "message": "成交时间格式错误"}

        if status == "sold":
            if not buyer_id:
                return {"success": False, "message": "sold 状态必须提供买家ID"}
            if buyer_id not in self._players:
                return {"success": False, "message": "买家ID不存在"}
            if sold_at is None:
                sold_at = time.time()
        else:
            buyer_id = ""
            sold_at = None

        total_price = quantity * unit_price
        ok = await self._data_manager.admin_update_market_listing(listing_id, {
            "seller_id": seller_id,
            "item_id": item_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "fee": fee,
            "listed_at": listed_at,
            "expires_at": expires_at,
            "status": status,
            "buyer_id": buyer_id,
            "sold_at": sold_at,
        })
        if not ok:
            return {"success": False, "message": "坊市记录不存在"}

        await self._notify_market_changed("admin_update")
        return {"success": True, "message": "坊市记录已更新"}

    async def admin_delete_market_listing(self, listing_id: str) -> dict:
        """管理员删除坊市记录。"""
        listing_id = str(listing_id or "").strip().lower()
        if not listing_id:
            return {"success": False, "message": "缺少记录ID"}
        ok = await self._data_manager.admin_delete_market_listing(listing_id)
        if not ok:
            return {"success": False, "message": "坊市记录不存在"}
        await self._notify_market_changed("admin_delete")
        return {"success": True, "message": "坊市记录已删除"}

    async def breakthrough(self, user_id: str) -> dict:
        """突破操作。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        # 检查是否有已服用的突破丹
        bonus = 0.0
        if getattr(player, 'breakthrough_pill_count', 0) > 0:
            bonus = 0.2
            player.breakthrough_pill_count -= 1

        # 检查是否有保命符
        prevent_death = False
        if player.inventory.get("life_talisman", 0) > 0:
            player.inventory["life_talisman"] -= 1
            if player.inventory["life_talisman"] <= 0:
                del player.inventory["life_talisman"]
            prevent_death = True

        result = await attempt_breakthrough(player, bonus, prevent_death)

        # 处理死亡
        if result.get("died"):
            death_items = await self.prepare_death(user_id)
            result["death_items"] = death_items
        else:
            await self._save_player(player)
        return result

    async def use_item_action(self, user_id: str, item_id: str, count: int = 1) -> dict:
        """使用物品。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        result = await use_item(player, item_id, count)
        if result["success"]:
            await self._save_player(player)
        return result

    async def use_item_by_name(self, user_id: str, item_name: str) -> dict:
        """通过物品名使用物品（聊天指令用）。"""
        item_id = find_item_id_by_name(item_name)
        if not item_id:
            return {"success": False, "message": f"找不到物品：{item_name}"}
        return await self.use_item_action(user_id, item_id)

    async def recycle_action(self, user_id: str, item_id: str, count: int = 1) -> dict:
        """回收物品（Web 端使用，传 item_id）。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        result = await recycle_item(player, item_id, count)
        if result["success"]:
            await self._save_player(player)
        return result

    async def recycle_by_name(
        self,
        user_id: str,
        item_name: str,
        count: int = 1,
        query_type: str | None = None,
    ) -> dict:
        """通过物品名回收物品（聊天指令用，可按类型精确匹配）。"""
        target_name = str(item_name or "").strip()
        if not target_name:
            return {"success": False, "message": "物品名不能为空"}

        matches = find_item_ids_by_name(target_name, query_type=query_type)
        if not matches:
            return {"success": False, "message": f"找不到物品：{target_name}"}
        if len(matches) > 1:
            return {
                "success": False,
                "message": f"存在重名物品「{target_name}」，请使用：回收 装备/物品/心法/功法 {target_name} [数量]",
            }
        return await self.recycle_action(user_id, matches[0], count)

    async def equip_action(self, user_id: str, equip_id: str) -> dict:
        """装备物品。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        result = await equip_item(player, equip_id)
        if result["success"]:
            await self._save_player(player)
        return result

    async def equip_by_name(self, user_id: str, item_name: str) -> dict:
        """通过物品名装备（聊天指令用）。"""
        item_id = find_item_id_by_name(item_name)
        if not item_id:
            return {"success": False, "message": f"找不到装备：{item_name}"}
        if item_id not in EQUIPMENT_REGISTRY:
            return {"success": False, "message": f"{item_name}不是装备"}
        return await self.equip_action(user_id, item_id)

    async def unequip_action(self, user_id: str, slot: str) -> dict:
        """卸下装备。slot: 'weapon' | 'armor'。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        result = await unequip_item(player, slot)
        if result["success"]:
            await self._save_player(player)
        return result

    # ── 功法遗忘 ─────────────────────────────────────────
    async def forget_gongfa(self, user_id: str, slot: str) -> dict:
        """遗忘指定槽位的功法，返回功法卷轴。slot: 'gongfa_1' | 'gongfa_2' | 'gongfa_3'。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        if slot not in ("gongfa_1", "gongfa_2", "gongfa_3"):
            return {"success": False, "message": "无效的功法槽位"}

        gongfa_id = getattr(player, slot, "无")
        if not gongfa_id or gongfa_id == "无":
            return {"success": False, "message": "该槽位没有装备功法"}

        gf = GONGFA_REGISTRY.get(gongfa_id)
        name = gf.name if gf else gongfa_id

        # 返回功法卷轴到背包
        scroll_id = get_gongfa_scroll_id(gongfa_id)
        player.inventory[scroll_id] = player.inventory.get(scroll_id, 0) + 1

        # 重置功法槽位和熟练度
        setattr(player, slot, "无")
        setattr(player, f"{slot}_mastery", 0)
        setattr(player, f"{slot}_exp", 0)
        await self._save_player(player)
        return {"success": True, "message": f"你已遗忘功法【{name}】，获得功法卷轴"}

    def _auto_unequip_invalid_gongfa(self, player) -> list[str]:
        """自动卸下不在注册表中的功法，返回被卸下的功法名列表。"""
        removed = []
        for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
            gid = getattr(player, slot, "无")
            if gid and gid != "无" and gid not in GONGFA_REGISTRY:
                removed.append(gid)
                setattr(player, slot, "无")
                setattr(player, f"{slot}_mastery", 0)
                setattr(player, f"{slot}_exp", 0)
        return removed

    def _clean_expired_heart_methods(self, player: Player) -> bool:
        """清理过期的心法道具，返回是否有清理。"""
        if not hasattr(player, "stored_heart_methods") or not isinstance(player.stored_heart_methods, dict):
            player.stored_heart_methods = {}
            return False
        current_time = time.time()
        changed = False
        kept: dict[str, float] = {}
        for mid, expire_time in player.stored_heart_methods.items():
            try:
                expire_at = float(expire_time)
            except (TypeError, ValueError):
                expire_at = 0.0
            item_id = get_stored_heart_method_item_id(mid)
            if current_time > expire_at:
                if item_id in player.inventory:
                    player.inventory.pop(item_id, None)
                    changed = True
                changed = True
                continue
            kept[mid] = expire_at
            if player.inventory.get(item_id, 0) <= 0:
                player.inventory[item_id] = 1
                changed = True
        if kept != player.stored_heart_methods:
            player.stored_heart_methods = kept
            changed = True
        return changed

    async def learn_heart_method(self, user_id: str, method_id: str) -> dict:
        """修炼心法。更换心法时若达到小成以上需用户确认。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        hm = HEART_METHOD_REGISTRY.get(method_id)
        if not hm:
            return {"success": False, "message": "无效的心法"}

        # 境界限制
        if hm.realm > player.realm:
            realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知")
            return {"success": False, "message": f"【{hm.name}】为{realm_name}心法，当前境界不足"}

        # 已经在修炼同一心法
        if player.heart_method == method_id:
            mastery_name = MASTERY_LEVELS[min(player.heart_method_mastery, len(MASTERY_LEVELS) - 1)]
            return {"success": False, "message": f"你已在修炼【{hm.name}】（{mastery_name}）"}

        old_hm = HEART_METHOD_REGISTRY.get(player.heart_method)

        # 若旧心法达到小成以上，需要用户确认
        if old_hm and player.heart_method_mastery >= 1:
            mastery_name = MASTERY_LEVELS[min(player.heart_method_mastery, len(MASTERY_LEVELS) - 1)]
            return {
                "success": False,
                "needs_confirmation": True,
                "old_method_id": old_hm.method_id,
                "old_method_name": old_hm.name,
                "old_mastery": player.heart_method_mastery,
                "old_mastery_name": mastery_name,
                "new_method_id": method_id,
                "new_method_name": hm.name,
                "message": f"你当前修炼的【{old_hm.name}】已达{mastery_name}，是否转换为心法值？"
            }

        # 直接替换（旧心法未达小成或无旧心法）
        old_name = old_hm.name if old_hm else None
        player.heart_method = method_id
        player.heart_method_mastery = 0
        player.heart_method_exp = 0
        await self._save_player(player)

        quality_name = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "")
        msg = f"开始修炼{quality_name}心法【{hm.name}】（入门）"
        if old_name:
            msg += f"\n（放弃了【{old_name}】的修炼进度）"
        return {"success": True, "message": msg}

    async def confirm_replace_heart_method(
        self,
        user_id: str,
        new_method_id: str,
        convert_to_value: bool,
        source_item_id: str,
    ) -> dict:
        """确认替换心法。convert_to_value=True转换为心法值，False保留为心法道具。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        new_hm = HEART_METHOD_REGISTRY.get(new_method_id)
        if not new_hm:
            return {"success": False, "message": "无效的心法"}
        if new_hm.realm > player.realm:
            realm_name = REALM_CONFIG.get(new_hm.realm, {}).get("name", "未知境界")
            return {"success": False, "message": f"【{new_hm.name}】需达到{realm_name}方可修炼，当前境界不足"}
        if player.heart_method == new_method_id:
            return {"success": False, "message": f"你已在修炼【{new_hm.name}】"}

        item = ITEM_REGISTRY.get(source_item_id)
        if not item or item.item_type not in ("consumable", "heart_method"):
            return {"success": False, "message": "心法秘籍不存在或已失效"}
        item_method_id = str(item.effect.get("learn_heart_method", ""))
        if item_method_id != new_method_id:
            return {"success": False, "message": "确认数据已失效，请重新使用秘籍"}
        if player.inventory.get(source_item_id, 0) <= 0:
            return {"success": False, "message": "心法秘籍已不存在，请重新检查背包"}

        old_hm = HEART_METHOD_REGISTRY.get(player.heart_method)
        if not old_hm or old_hm.method_id == new_method_id or player.heart_method_mastery < 1:
            return {"success": False, "message": "当前心法未达小成，无需确认"}

        old_name = old_hm.name
        old_mastery = player.heart_method_mastery
        old_exp = player.heart_method_exp
        old_method_id = old_hm.method_id
        stored_source_method_id = parse_stored_heart_method_item_id(source_item_id)

        player.inventory[source_item_id] -= 1
        if player.inventory[source_item_id] <= 0:
            del player.inventory[source_item_id]
        if stored_source_method_id:
            player.stored_heart_methods.pop(stored_source_method_id, None)

        from .inventory import _calc_heart_method_convert_points

        player.heart_method_value = max(0, int(getattr(player, "heart_method_value", 0)))
        messages: list[str] = []

        if convert_to_value:
            convert_points = _calc_heart_method_convert_points(old_hm, old_mastery, old_exp, new_hm)
            player.heart_method_value += convert_points
            if convert_points > 0:
                messages.append(f"转化【{old_name}】为{convert_points}点心法值")
            else:
                messages.append(f"转化【{old_name}】未获得可用心法值")
        else:
            expire_time = time.time() + 3 * 24 * 3600
            stored_item_id = get_stored_heart_method_item_id(old_method_id)
            if old_method_id not in player.stored_heart_methods:
                player.stored_heart_methods[old_method_id] = expire_time
                player.inventory[stored_item_id] = player.inventory.get(stored_item_id, 0) + 1
                messages.append(f"保留【{old_name}】为心法道具（三天期限）")
            else:
                if player.inventory.get(stored_item_id, 0) <= 0:
                    player.inventory[stored_item_id] = 1
                messages.append(f"保留【{old_name}】为心法道具（沿用原过期时间）")

        player.heart_method = new_method_id
        player.heart_method_mastery = 0
        player.heart_method_exp = 0

        absorbed_value = 0
        if new_hm.realm == player.realm and player.heart_method_value > 0:
            absorb_cap = max(1, int(new_hm.mastery_exp * 0.6))
            absorbed_value = min(player.heart_method_value, absorb_cap)
            player.heart_method_exp = min(new_hm.mastery_exp - 1, player.heart_method_exp + absorbed_value)
            player.heart_method_value -= absorbed_value

        await self._save_player(player)

        quality_name = HEART_METHOD_QUALITY_NAMES.get(new_hm.quality, "")
        messages.append(f"开始修炼{quality_name}心法【{new_hm.name}】（入门）")
        if absorbed_value > 0:
            messages.append(
                f"吸收预存心法值{absorbed_value}，当前进度{player.heart_method_exp}/{new_hm.mastery_exp}"
                f"（剩余心法值{player.heart_method_value}）"
            )
        return {"success": True, "message": "，".join(messages)}

    async def learn_heart_method_by_name(self, user_id: str, name: str) -> dict:
        """通过心法名修炼（聊天指令用）。"""
        for mid, hm in HEART_METHOD_REGISTRY.items():
            if hm.name == name:
                return await self.learn_heart_method(user_id, mid)
        return {"success": False, "message": f"找不到心法：{name}"}

    async def get_available_heart_methods(self, user_id: str) -> dict:
        """获取当前境界可学习的心法列表。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色", "methods": []}

        methods = get_realm_heart_methods(player.realm)
        result = []
        for hm in methods:
            is_current = (player.heart_method == hm.method_id)
            result.append({
                "method_id": hm.method_id,
                "name": hm.name,
                "quality": hm.quality,
                "quality_name": HEART_METHOD_QUALITY_NAMES.get(hm.quality, ""),
                "exp_multiplier": hm.exp_multiplier,
                "attack_bonus": hm.attack_bonus,
                "defense_bonus": hm.defense_bonus,
                "dao_yun_rate": hm.dao_yun_rate,
                "description": hm.description,
                "is_current": is_current,
            })
        return {"success": True, "methods": result}

    async def get_panel(self, user_id: str) -> Optional[dict]:
        """获取角色面板数据。"""
        player = self._players.get(user_id)
        if not player:
            return None
        self._clamp_player_hp(player)
        panel = player.to_dict()
        pending = self._pending_deaths.get(user_id)
        if pending:
            panel["pending_death_items"] = pending.get("items", [])
        return panel

    def has_pending_death(self, user_id: str) -> bool:
        """是否存在待确认的道陨重生选择。"""
        return user_id in self._pending_deaths

    async def get_inventory(self, user_id: str) -> list[dict]:
        """获取背包展示数据。"""
        player = self._players.get(user_id)
        if not player:
            return []
        return await get_inventory_display(player)

    def get_item_detail(self, item_name: str, query_type: str | None = None) -> dict | None:
        """根据物品名查询物品详情（静态数据查询，不需要 player_id）。

        支持三种类型：消耗品/材料、装备、心法秘籍。
        当出现重名条目时按优先级自动返回一个结果：
        装备 > 心法秘籍 > 普通物品。
        """
        target_name = str(item_name or "").strip()
        if not target_name:
            return None

        target_type = str(query_type or "").strip()
        candidates: list[dict] = []

        # 1) 在 EQUIPMENT_REGISTRY 中查找装备
        for eq in EQUIPMENT_REGISTRY.values():
            if target_type and target_type != "equipment":
                continue
            if eq.name != target_name:
                continue
            candidates.append({
                "type": "equipment",
                "name": eq.name,
                "tier_name": EQUIPMENT_TIER_NAMES.get(eq.tier, "未知"),
                "slot": "武器" if eq.slot == "weapon" else "护甲",
                "attack": eq.attack,
                "defense": eq.defense,
                "element": eq.element,
                "element_damage": eq.element_damage,
                "description": eq.description,
            })

        # 2) 在 ITEM_REGISTRY 中查找
        for item in ITEM_REGISTRY.values():
            if item.name != target_name:
                continue
            # 检查是否是心法秘籍
            method_id = parse_heart_method_manual_id(item.item_id)
            if not method_id:
                method_id = parse_stored_heart_method_item_id(item.item_id)
            if method_id:
                hm = HEART_METHOD_REGISTRY.get(method_id)
                if hm:
                    if target_type and target_type != "heart_method":
                        continue
                    realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界")
                    candidates.append({
                        "type": "heart_method",
                        "name": item.name,
                        "quality_name": HEART_METHOD_QUALITY_NAMES.get(hm.quality, "未知"),
                        "realm_name": realm_name,
                        "attack_bonus": hm.attack_bonus,
                        "defense_bonus": hm.defense_bonus,
                        "exp_multiplier": hm.exp_multiplier,
                        "dao_yun_rate": hm.dao_yun_rate,
                        "description": hm.description,
                    })
                    continue
            # 检查是否是功法卷轴
            gf_id = parse_gongfa_scroll_id(item.item_id)
            if gf_id:
                gf = GONGFA_REGISTRY.get(gf_id)
                if gf:
                    if target_type and target_type != "gongfa":
                        continue
                    candidates.append({
                        "type": "gongfa",
                        "name": item.name,
                        "tier_name": GONGFA_TIER_NAMES.get(gf.tier, "未知"),
                        "attack_bonus": gf.attack_bonus,
                        "defense_bonus": gf.defense_bonus,
                        "hp_regen": gf.hp_regen,
                        "lingqi_regen": gf.lingqi_regen,
                        "description": item.description,
                    })
                    continue
            # 普通消耗品/材料
            if target_type and target_type != "item":
                continue
            candidates.append({
                "type": "item",
                "name": item.name,
                "item_type": item.item_type,
                "description": item.description,
                "effect": item.effect,
            })

        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]

        type_priority = {
            "equipment": 0,
            "heart_method": 1,
            "gongfa": 2,
            "item": 3,
        }
        candidates.sort(key=lambda c: type_priority.get(str(c.get("type", "")), 9))
        return candidates[0]

    async def _random_drop(self, player: Player):
        """保留兼容：修炼/挂机修炼不再产出任何掉落。"""
        return

    async def _random_equip_drop(self, player: Player):
        """保留兼容：装备掉落已迁移到历练逻辑。"""
        return

    async def _save_player(self, player: Player):
        """保存玩家数据并通知 WebSocket。"""
        # 兜底：若心法境界不符则自动卸下为秘籍，并结转60%心法值
        self._auto_unequip_invalid_heart_method(player, convert_ratio=0.6, force=False)
        player.heart_method_value = max(0, int(getattr(player, "heart_method_value", 0)))
        self._clamp_player_hp(player)
        await self._data_manager.save_player(player)
        try:
            await self._notify_player_update(player)
        except Exception:
            logger.exception("修仙世界：玩家状态推送失败 user_id=%s", player.user_id)

    async def _notify_player_update(self, player: Player):
        """向前端推送玩家面板、背包与排行榜变化。"""
        if self._ws_manager:
            await self._ws_manager.notify_player_update(player)
            inv = await get_inventory_display(player)
            await self._ws_manager.send_to_player(player.user_id, {
                "type": "inventory",
                "data": inv,
            })
            if hasattr(self._ws_manager, "queue_rankings_refresh"):
                self._ws_manager.queue_rankings_refresh(self)
            elif hasattr(self._ws_manager, "broadcast"):
                await self._ws_manager.broadcast({
                    "type": "rankings_changed",
                    "data": {"user_id": player.user_id},
                })

    @staticmethod
    def _apply_player_snapshot(player: Player, snapshot: Player):
        """将快照状态覆盖回内存玩家对象。"""
        for field in fields(Player):
            value = getattr(snapshot, field.name)
            if isinstance(value, dict):
                value = dict(value)
            setattr(player, field.name, value)
        GameEngine._clamp_player_hp(player)

    def _get_player_lock(self, user_id: str) -> asyncio.Lock:
        """获取玩家级锁（按需创建），用于保护需要原子修改玩家状态的操作。"""
        return self._player_locks.setdefault(user_id, asyncio.Lock())

    @staticmethod
    def _snapshot_player(player: Player) -> Player:
        """创建玩家对象的深拷贝快照，用于在事务中操作而不影响共享内存。"""
        return Player.from_dict(player.to_dict(include_sensitive=True))

    async def _notify_market_changed(self, action: str, count: int = 0):
        """广播坊市列表变更，驱动前端实时刷新。"""
        if self._ws_manager and hasattr(self._ws_manager, "queue_market_refresh"):
            self._ws_manager.queue_market_refresh(self)
        elif self._ws_manager and hasattr(self._ws_manager, "broadcast"):
            await self._ws_manager.broadcast({
                "type": "market_changed",
                "data": {
                    "action": action,
                    "count": int(count or 0),
                    "ts": time.time(),
                },
            })

    async def _process_market_cleanup(self):
        """清理过期坊市商品：逐条在持锁事务内原子完成标记+退物。"""
        expired = await market_mod.cleanup_expired(self._data_manager)
        cleaned_count = 0

        for listing in expired:
            sid = listing["seller_id"]
            seller = self._players.get(sid)
            if not seller:
                # 卖家不在线/已删除，仅标记过期，物品无法退回
                await self._data_manager.update_listing_status(
                    listing["listing_id"], "expired", expected_status="active",
                )
                cleaned_count += 1
                continue

            async with self._get_player_lock(sid):
                snapshot = self._snapshot_player(seller)
                item_id = listing["item_id"]
                snapshot.inventory[item_id] = (
                    snapshot.inventory.get(item_id, 0) + listing["quantity"]
                )
                try:
                    async with self._data_manager.transaction() as tx:
                        cur = await tx.execute(
                            """UPDATE market_listings SET status = ?
                               WHERE listing_id = ? AND status = ?""",
                            ("expired", listing["listing_id"], "active"),
                        )
                        if cur.rowcount <= 0:
                            # 已被并发处理（购买/取消），跳过
                            continue
                        await self._data_manager._upsert_player(snapshot, db=tx)
                except Exception:
                    logger.exception(
                        "修仙世界：过期清理事务失败 listing=%s seller=%s",
                        listing["listing_id"], sid,
                    )
                    continue

                self._apply_player_snapshot(seller, snapshot)
                try:
                    await self._notify_player_update(seller)
                except Exception:
                    logger.exception("修仙世界：玩家状态推送失败 user_id=%s", sid)
                cleaned_count += 1

        if cleaned_count > 0:
            await self._notify_market_changed("cleanup", cleaned_count)

    async def _periodic_cleanup(self):
        """每5分钟清理过期数据（Token/绑定 + 坊市过期商品）。"""
        while True:
            await asyncio.sleep(300)
            try:
                if self.auth:
                    await self.auth.save()
            except Exception:
                logger.exception("修仙世界：定时清理认证数据异常")
            try:
                await self._process_market_cleanup()
            except Exception:
                logger.exception("修仙世界：定时清理坊市过期商品异常")

    def _auto_unequip_invalid_equipment(self, player: Player) -> list[str]:
        """自动卸下当前境界无法装备的物品并放回背包。"""
        removed: list[str] = []
        for slot in ("weapon", "armor"):
            equip_id = getattr(player, slot, "无")
            eq = EQUIPMENT_REGISTRY.get(equip_id)
            if not eq:
                if equip_id and equip_id != "无":
                    player.inventory[equip_id] = player.inventory.get(equip_id, 0) + 1
                    setattr(player, slot, "无")
                    removed.append(equip_id)
                continue
            if can_equip(player.realm, eq.tier):
                continue
            player.inventory[equip_id] = player.inventory.get(equip_id, 0) + 1
            setattr(player, slot, "无")
            removed.append(eq.name)
        return removed

    def _auto_unequip_invalid_heart_method(
        self, player: Player, convert_ratio: float = 0.0, force: bool = False
    ) -> dict:
        """自动卸下不符合条件的心法，并可按比例结转为心法值。"""
        method_id = getattr(player, "heart_method", "无")
        if not method_id or method_id == "无":
            return {"removed_name": "", "manual_name": "", "converted": 0}

        hm = HEART_METHOD_REGISTRY.get(method_id)
        if not hm:
            player.heart_method = "无"
            player.heart_method_mastery = 0
            player.heart_method_exp = 0
            return {"removed_name": method_id, "manual_name": "", "converted": 0}

        if not force and hm.realm <= player.realm:
            return {"removed_name": "", "manual_name": "", "converted": 0}

        mastery = max(0, min(int(getattr(player, "heart_method_mastery", 0)), len(MASTERY_LEVELS) - 1))
        exp = max(0, int(getattr(player, "heart_method_exp", 0)))
        raw_value = max(0, int(hm.mastery_exp) * mastery + exp)
        converted = max(0, int(raw_value * max(0.0, convert_ratio)))
        if converted > 0:
            player.heart_method_value = max(0, int(getattr(player, "heart_method_value", 0))) + converted

        manual_id = get_heart_method_manual_id(hm.method_id)
        manual = ITEM_REGISTRY.get(manual_id)
        manual_name = f"{hm.name}秘籍"
        if manual:
            player.inventory[manual_id] = player.inventory.get(manual_id, 0) + 1
            manual_name = manual.name

        player.heart_method = "无"
        player.heart_method_mastery = 0
        player.heart_method_exp = 0
        return {"removed_name": hm.name, "manual_name": manual_name, "converted": converted}

    async def _handle_player_death(self, user_id: str, player: Player):
        """处理角色陨落：重置为凡人，清空背包和灵石。"""
        player.death_count += 1
        player.realm = 0
        player.sub_realm = 0
        player.exp = 0
        base_stats = get_player_base_stats(player)
        player.hp = base_stats["max_hp"]
        player.max_hp = base_stats["max_hp"]
        player.attack = base_stats["attack"]
        player.defense = base_stats["defense"]
        player.lingqi = base_stats["max_lingqi"]
        player.dao_yun = 0
        player.spirit_stones = 0
        player.heart_method = "无"
        player.heart_method_mastery = 0
        player.heart_method_exp = 0
        player.heart_method_value = 0
        # 陨落后清空挂机修炼记录
        player.afk_cultivate_start = 0.0
        player.afk_cultivate_end = 0.0
        # 陨落后重置历练冷却，允许重新出发
        player.last_adventure_time = 0.0
        player.weapon = "无"
        player.armor = "无"
        player.gongfa_1 = "无"
        player.gongfa_2 = "无"
        player.gongfa_3 = "无"
        player.gongfa_1_mastery = 0
        player.gongfa_1_exp = 0
        player.gongfa_2_mastery = 0
        player.gongfa_2_exp = 0
        player.gongfa_3_mastery = 0
        player.gongfa_3_exp = 0
        player.inventory.clear()
        # 赠送一点基础物品以便重新开始
        player.inventory["healing_pill"] = 1
        await self._save_player(player)

    async def prepare_death(self, user_id: str) -> list[dict]:
        """收集死亡快照（心法、武器、护甲、背包物品），存入 _pending_deaths，返回快照列表。"""
        player = self._players.get(user_id)
        if not player:
            return []

        async with self._get_player_lock(user_id):
            # 已有快照则直接返回，禁止覆盖（防止选择性保留漏洞）
            existing = self._pending_deaths.get(user_id)
            if existing:
                return existing["items"]

            items: list[dict] = []
            snapshot_index = 0

            def make_snapshot_item(item_id: str, item_type: str, **extra) -> dict:
                nonlocal snapshot_index
                snapshot_index += 1
                return {
                    "id": f"death:{snapshot_index}:{item_type}:{item_id}",
                    "item_id": item_id,
                    "type": item_type,
                    **extra,
                }

            # 已装备的心法
            if player.heart_method and player.heart_method != "无":
                hm = HEART_METHOD_REGISTRY.get(player.heart_method)
                if hm:
                    items.append(make_snapshot_item(
                        player.heart_method,
                        "heart_method",
                        name=hm.name,
                        count=1,
                        description=hm.description,
                        quality_name=HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通"),
                    ))

            # 已装备的武器
            if player.weapon and player.weapon != "无":
                eq = EQUIPMENT_REGISTRY.get(player.weapon)
                if eq:
                    items.append(make_snapshot_item(
                        player.weapon,
                        "weapon",
                        name=eq.name,
                        count=1,
                        description=eq.description,
                        tier_name=EQUIPMENT_TIER_NAMES.get(eq.tier, "未知"),
                        slot=eq.slot,
                    ))

            # 已装备的护甲
            if player.armor and player.armor != "无":
                eq = EQUIPMENT_REGISTRY.get(player.armor)
                if eq:
                    items.append(make_snapshot_item(
                        player.armor,
                        "armor",
                        name=eq.name,
                        count=1,
                        description=eq.description,
                        tier_name=EQUIPMENT_TIER_NAMES.get(eq.tier, "未知"),
                        slot=eq.slot,
                    ))

            # 已装备的功法
            for gf_slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
                gongfa_id = getattr(player, gf_slot, "无")
                if gongfa_id and gongfa_id != "无":
                    gf = GONGFA_REGISTRY.get(gongfa_id)
                    if gf:
                        items.append(make_snapshot_item(
                            gongfa_id,
                            "gongfa",
                            name=gf.name,
                            count=1,
                            description=gf.description,
                            tier_name=GONGFA_TIER_NAMES.get(gf.tier, "未知"),
                            gongfa_slot=gf_slot,
                        ))

            # 背包物品
            for item_id, count in player.inventory.items():
                item_def = ITEM_REGISTRY.get(item_id)
                if not item_def:
                    continue
                entry = make_snapshot_item(
                    item_id,
                    "item",
                    name=item_def.name,
                    count=count,
                    description=item_def.description,
                )
                eq = EQUIPMENT_REGISTRY.get(item_id)
                if eq:
                    entry["tier_name"] = EQUIPMENT_TIER_NAMES.get(eq.tier, "未知")
                    entry["slot"] = eq.slot
                items.append(entry)

            self._pending_deaths[user_id] = {"items": items}
            return items

    async def confirm_death(self, user_id: str, kept_ids: list[str]) -> dict:
        """校验 kept_ids ≤ 3，执行死亡重置后把保留的物品/装备/心法还给玩家。"""
        async with self._get_player_lock(user_id):
            snapshot = self._pending_deaths.pop(user_id, None)
            if not snapshot:
                return {"success": False, "message": "没有待确认的死亡记录"}

            player = self._players.get(user_id)
            if not player:
                return {"success": False, "message": "角色不存在"}

            if len(kept_ids) > 3:
                self._pending_deaths[user_id] = snapshot  # 放回
                return {"success": False, "message": "最多只能保留3样物品"}

            # 从快照中找到要保留的物品
            snapshot_items = snapshot["items"]
            snapshot_map = {item["id"]: item for item in snapshot_items}
            invalid_ids = [snapshot_id for snapshot_id in kept_ids if snapshot_id not in snapshot_map]
            if invalid_ids:
                self._pending_deaths[user_id] = snapshot
                return {"success": False, "message": "存在无效的保留物品，请重新选择"}

            kept_items: list[dict] = []
            seen_ids: set[str] = set()
            for snapshot_id in kept_ids:
                if snapshot_id in seen_ids:
                    continue
                seen_ids.add(snapshot_id)
                kept_items.append(snapshot_map[snapshot_id])

            # 执行原有死亡重置
            await self._handle_player_death(user_id, player)

            # 把保留的物品还给玩家
            for ki in kept_items:
                item_id = ki.get("item_id", ki["id"])
                ktype = ki["type"]
                if ktype == "heart_method":
                    hm = HEART_METHOD_REGISTRY.get(item_id)
                    if hm:
                        player.heart_method = item_id
                        player.heart_method_mastery = 0
                        player.heart_method_exp = 0
                        player.heart_method_value = 0
                elif ktype == "gongfa":
                    # 恢复功法到原槽位（若槽位已被占用则找第一个空槽）
                    target_slot = str(ki.get("gongfa_slot", "")).strip() or "gongfa_1"
                    if target_slot not in {"gongfa_1", "gongfa_2", "gongfa_3"}:
                        target_slot = "gongfa_1"
                    if getattr(player, target_slot, "无") != "无":
                        for s in ("gongfa_1", "gongfa_2", "gongfa_3"):
                            if getattr(player, s, "无") == "无":
                                target_slot = s
                                break
                    if getattr(player, target_slot, "无") == "无" and item_id in GONGFA_REGISTRY:
                        setattr(player, target_slot, item_id)
                        setattr(player, f"{target_slot}_mastery", 0)
                        setattr(player, f"{target_slot}_exp", 0)
                elif ktype in ("weapon", "armor"):
                    # 装备放入背包（死亡后境界归零，可能无法穿戴）
                    player.inventory[item_id] = player.inventory.get(item_id, 0) + 1
                else:
                    player.inventory[item_id] = player.inventory.get(item_id, 0) + ki.get("count", 1)

            await self._save_player(player)
            await self._notify_player_update(player)
            return {"success": True, "message": "携宝重生成功"}

    async def shutdown(self):
        """关闭时保存所有数据并关闭数据库。"""
        # 取消定时清理任务
        if hasattr(self, "_cleanup_task"):
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        if self._ws_manager and hasattr(self._ws_manager, "stop_chat_cleanup_task"):
            await self._ws_manager.stop_chat_cleanup_task()
        for player in self._players.values():
            self._clamp_player_hp(player)
        await self._data_manager.save_all_players(self._players)
        if self.auth:
            await self.auth.save()
        await self._data_manager.close()

    async def clear_all_data(self, remove_dir: bool = False):
        """清空游戏数据并清理持久化文件。"""
        if self.auth:
            await self.auth.clear_all()
        if self._ws_manager and hasattr(self._ws_manager, "_connections"):
            self._ws_manager._connections.clear()
        self._players.clear()
        self._name_index.clear()
        await self._data_manager.clear_all_data(remove_dir=remove_dir)

    # ==================== 认证相关 ====================

    def get_player_by_name(self, name: str) -> Optional[Player]:
        """通过道号查找玩家。"""
        uid = self._name_index.get(name)
        if uid:
            return self._players.get(uid)
        return None

    def is_name_taken(self, name: str) -> bool:
        """检查道号是否已被使用。"""
        return name in self._name_index

    def set_name_reviewer(self, reviewer: Callable[[str], Awaitable[dict | tuple | bool]] | None):
        """设置道号审核器（由插件层注入 AI 审核实现）。"""
        self._name_reviewer = reviewer

    def set_chat_reviewer(self, reviewer: Callable[[str], Awaitable[dict]] | None):
        """设置世界频道消息审核器（由插件层注入 AI 审核实现）。"""
        self._chat_reviewer = reviewer

    def set_sect_name_reviewer(self, reviewer: Callable[[str], Awaitable[dict]] | None):
        """设置宗门名称审核器（由插件层注入 AI 审核实现）。"""
        self._sect_name_reviewer = reviewer

    def _local_name_risk_check(self, name: str) -> tuple[bool, str]:
        """本地敏感词兜底检测。"""
        # 基础字符校验：只允许中文、英文、数字、部分符号，长度1-12
        if not re.match(r'^[\u4e00-\u9fa5a-zA-Z0-9_·\-]{1,12}$', name or ""):
            return False, "道号仅允许中文、英文、数字，长度1-12"
        normalized = re.sub(r"[\s·•・_\-]", "", str(name or "")).lower()
        for kw in _BAD_NAME_KEYWORDS:
            if kw and kw in normalized:
                return False, "道号包含违规词汇"
        return True, ""

    async def _review_registration_name(self, name: str) -> tuple[bool, str]:
        """注册道号审核：本地拦截 + AI 审核。"""
        ok, reason = self._local_name_risk_check(name)
        if not ok:
            return False, reason

        if not self._name_reviewer:
            # AI 审核器未配置时，使用本地兜底规则即可
            return True, ""

        try:
            review = await self._name_reviewer(name)
        except Exception:
            # 审核服务异常时放行，避免误伤正常注册
            return True, ""

        allow = False
        ai_reason = ""
        if isinstance(review, bool):
            allow = review
        elif isinstance(review, dict):
            allow = bool(review.get("allow", review.get("ok", False)))
            ai_reason = str(review.get("reason", "")).strip()
        elif isinstance(review, (tuple, list)) and review:
            allow = bool(review[0])
            if len(review) > 1:
                ai_reason = str(review[1]).strip()
        else:
            ai_reason = str(review).strip()

        if allow:
            return True, ""
        return False, ai_reason or "道号包含不当内容"

    async def register_with_password(self, name: str, password: str) -> dict:
        """Web 端注册：创建角色并设置密码。"""
        name = name.strip()
        if not name:
            return {"success": False, "message": "道号不能为空"}
        # 仅允许中文汉字（基础区+扩展A），禁止中文标点和其他特殊字符
        if not re.fullmatch(r"[\u3400-\u4DBF\u4E00-\u9FFF]{2,12}", name):
            return {"success": False, "message": "道号仅支持2-12位中文汉字"}

        ok, reason = await self._review_registration_name(name)
        if not ok:
            return {"success": False, "message": reason}

        if self.is_name_taken(name):
            return {"success": False, "message": f"道号「{name}」已被使用"}
        if not re.fullmatch(r"\d{4,32}", password or ""):
            return {"success": False, "message": "密码仅支持数字，长度4-32位"}

        # 生成 user_id
        import secrets as _s
        user_id = "u_" + _s.token_hex(8)

        player = await self.get_or_create_player(user_id, name)
        player.password_hash = AuthManager.hash_password(password)
        await self._save_player(player)
        return {"success": True, "user_id": user_id, "message": f"注册成功，欢迎{name}"}

    async def set_password(self, user_id: str, password: str) -> dict:
        """为已有角色设置密码。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "角色不存在"}
        if not re.fullmatch(r"\d{4,32}", password or ""):
            return {"success": False, "message": "密码仅支持数字，长度4-32位"}
        player.password_hash = AuthManager.hash_password(password)
        await self._save_player(player)
        return {"success": True, "message": "密码设置成功"}

    def verify_login(self, name: str, password: str) -> Optional[Player]:
        """验证道号+密码登录，返回 Player 或 None。"""
        player = self.get_player_by_name(name)
        if not player:
            return None
        if not player.password_hash:
            return None
        if not AuthManager.verify_password(password, player.password_hash):
            return None
        return player

    # ==================== 管理员操作 ====================

    async def delete_player(self, user_id: str) -> dict:
        """删除玩家。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "玩家不存在"}
        name = player.name

        # 清理宗门成员记录
        try:
            sect_info = await self._data_manager.load_player_sect(user_id)
            if sect_info:
                sect_id = sect_info["sect_id"]
                await self._data_manager.delete_sect_member(user_id)
                # 若是宗主且无其他成员，解散宗门
                if sect_info.get("role") == "leader":
                    members = await self._data_manager.load_sect_members(sect_id)
                    remaining = [m for m in members if m.get("user_id") != user_id]
                    if not remaining:
                        await self._data_manager.delete_sect(sect_id)
        except Exception:
            logger.exception("修仙世界：删除玩家时清理宗门数据异常 user_id=%s", user_id)

        if self.auth:
            await self.auth.revoke_user(user_id)
        if self._ws_manager and hasattr(self._ws_manager, "disconnect"):
            self._ws_manager.disconnect(user_id)
        self._name_index.pop(name, None)
        del self._players[user_id]
        await self._data_manager.delete_player(user_id)
        return {"success": True, "message": f"已删除玩家「{name}」"}

    async def batch_delete_players(self, user_ids: list[str]) -> dict:
        """批量删除玩家。"""
        deleted = 0
        deleted_ids: list[str] = []
        for uid in user_ids:
            player = self._players.get(uid)
            if player:
                self._name_index.pop(player.name, None)
                del self._players[uid]
                deleted_ids.append(uid)
                if self._ws_manager and hasattr(self._ws_manager, "disconnect"):
                    self._ws_manager.disconnect(uid)
                deleted += 1
        if deleted_ids and self.auth:
            await self.auth.revoke_users(deleted_ids)
        for uid in deleted_ids:
            await self._data_manager.delete_player(uid)
        return {"success": True, "message": f"已删除 {deleted} 名玩家", "deleted": deleted}

    async def update_player_data(self, user_id: str, updates: dict) -> dict:
        """管理员修改玩家数据。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "玩家不存在"}
        allowed = {"realm", "sub_realm", "exp", "hp", "max_hp", "attack", "defense", "spirit_stones", "lingqi", "dao_yun", "name"}
        realm_changed = False
        old_realm = int(player.realm)
        for key, value in updates.items():
            if key not in allowed:
                continue
            if key == "name":
                new_name = str(value).strip()
                if not new_name:
                    continue
                if new_name != player.name and self.is_name_taken(new_name):
                    return {"success": False, "message": f"道号「{new_name}」已被使用"}
                self._name_index.pop(player.name, None)
                player.name = new_name
                self._name_index[new_name] = user_id
            else:
                try:
                    setattr(player, key, int(value))
                    if key in {"realm", "sub_realm"}:
                        realm_changed = True
                except (ValueError, TypeError):
                    pass
        removed = self._auto_unequip_invalid_equipment(player) if realm_changed else []
        heart_fix = {"removed_name": "", "manual_name": "", "converted": 0}
        if realm_changed:
            major_drop = int(player.realm) < old_realm
            heart_fix = self._auto_unequip_invalid_heart_method(
                player, convert_ratio=0.6 if major_drop else 0.0, force=major_drop
            )
        await self._save_player(player)
        msg_parts = [f"已更新玩家「{player.name}」的数据"]
        if removed:
            msg_parts.append(f"自动卸下装备：{'、'.join(removed)}")
        if heart_fix.get("removed_name"):
            msg_parts.append(f"自动卸下心法：{heart_fix['removed_name']}（已返还{heart_fix['manual_name']}）")
            if heart_fix.get("converted", 0) > 0:
                msg_parts.append(f"结转心法值+{heart_fix['converted']}")
        return {"success": True, "message": "；".join(msg_parts)}

    def get_player_detail(self, user_id: str) -> Optional[dict]:
        """获取玩家详细数据（含背包内容，供管理员查看）。"""
        player = self._players.get(user_id)
        if not player:
            return None
        self._clamp_player_hp(player)
        from .inventory import get_inventory_display_sync
        d = player.to_dict()
        d["inventory_detail"] = get_inventory_display_sync(player)
        d["user_id"] = user_id
        return d

    def get_online_user_ids(self) -> set[str]:
        """获取当前 WebSocket 在线的 user_id 集合。"""
        if self._ws_manager and hasattr(self._ws_manager, "_connections"):
            return set(self._ws_manager._connections.keys())
        return set()

    def get_rankings(self, limit: int = 50) -> list[dict]:
        """获取排行榜（按境界+经验排序）。"""
        players = list(self._players.values())
        players.sort(key=lambda p: (p.realm, p.sub_realm, p.exp), reverse=True)
        result = []
        for i, p in enumerate(players[:limit]):
            result.append({
                "rank": i + 1,
                "name": p.name,
                "realm": p.realm,
                "realm_name": get_realm_name(p.realm, p.sub_realm),
                "exp": p.exp,
                "attack": p.attack,
                "defense": p.defense,
                "spirit_stones": p.spirit_stones,
            })
        return result

    def get_death_rankings(self, limit: int = 50) -> list[dict]:
        """获取死亡排行榜（按死亡次数降序）。"""
        players = [p for p in self._players.values() if p.death_count > 0]
        players.sort(
            key=lambda p: (p.death_count, p.realm, p.sub_realm, p.exp),
            reverse=True,
        )
        result = []
        for i, p in enumerate(players[:limit]):
            result.append({
                "rank": i + 1,
                "name": p.name,
                "death_count": p.death_count,
                "realm": p.realm,
                "realm_name": get_realm_name(p.realm, p.sub_realm),
            })
        return result

    def get_online_rankings(self, limit: int = 50) -> list[dict]:
        """获取在线排行榜（仅在线玩家，按境界+经验排序）。"""
        online_ids = self.get_online_user_ids()
        players = [p for uid, p in self._players.items() if uid in online_ids]
        players.sort(key=lambda p: (p.realm, p.sub_realm, p.exp), reverse=True)
        result = []
        for i, p in enumerate(players[:limit]):
            result.append({
                "rank": i + 1,
                "name": p.name,
                "realm": p.realm,
                "realm_name": get_realm_name(p.realm, p.sub_realm),
                "exp": p.exp,
            })
        return result

    # ── 坊市 (Market) ───────────────────────────────────────

    async def market_list(
        self, user_id: str, item_id: str, quantity: int, unit_price: int,
    ) -> dict:
        """上架物品到坊市（Web 端使用，传 item_id）。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        async with self._get_player_lock(user_id):
            snapshot = self._snapshot_player(player)
            result = await market_mod.list_item(
                snapshot, item_id, quantity, unit_price, self._data_manager,
            )
            if result["success"]:
                self._apply_player_snapshot(player, snapshot)
                try:
                    await self._notify_player_update(player)
                except Exception:
                    logger.exception("修仙世界：玩家状态推送失败 user_id=%s", player.user_id)
                await self._notify_market_changed("list")
        return result

    async def market_list_by_name(
        self,
        user_id: str,
        item_name: str,
        quantity: int,
        unit_price: int,
        query_type: str | None = None,
    ) -> dict:
        """上架物品到坊市（聊天端使用，传物品名）。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        matches = find_item_ids_by_name(item_name, query_type=query_type)
        if not matches:
            return {"success": False, "message": f"找不到物品：{item_name}"}
        if len(matches) > 1:
            return {
                "success": False,
                "message": f"存在重名物品「{item_name}」，请指定类型",
            }
        return await self.market_list(user_id, matches[0], quantity, unit_price)

    async def market_buy(self, user_id: str, listing_id: str) -> dict:
        """从坊市购买物品。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        await self._process_market_cleanup()

        # 预查 listing 以确定卖家 ID，用于加锁
        listing = await self._data_manager.get_listing_by_id(listing_id)
        if not listing:
            return {"success": False, "message": "该商品不存在"}

        seller_id = listing["seller_id"]
        if seller_id == user_id:
            return {"success": False, "message": "不能购买自己的商品"}

        seller = self._players.get(seller_id)
        if not seller:
            return {"success": False, "message": "卖家信息异常，请稍后重试"}

        # 按 user_id 排序获取双方锁，避免死锁
        lock_ids = sorted({user_id, seller_id})
        lock_first = self._get_player_lock(lock_ids[0])
        lock_second = self._get_player_lock(lock_ids[1]) if len(lock_ids) > 1 else None

        async with lock_first:
            ctx = lock_second if lock_second else contextlib.nullcontext()
            async with ctx:
                buyer_snapshot = self._snapshot_player(player)
                seller_snapshot = self._snapshot_player(seller)
                result = await market_mod.buy_item(
                    buyer_snapshot, seller_snapshot, listing_id, self._data_manager,
                )
                if result["success"]:
                    self._apply_player_snapshot(player, buyer_snapshot)
                    self._apply_player_snapshot(seller, seller_snapshot)
                    try:
                        await self._notify_player_update(player)
                    except Exception:
                        logger.exception("修仙世界：玩家状态推送失败 user_id=%s", player.user_id)
                    try:
                        await self._notify_player_update(seller)
                    except Exception:
                        logger.exception("修仙世界：玩家状态推送失败 user_id=%s", seller_id)
                    await self._notify_market_changed("buy")
        return result

    async def market_buy_by_prefix(self, user_id: str, prefix: str) -> dict:
        """通过短编号购买（聊天端用）。"""
        listing = await self._data_manager.get_listing_by_id_prefix(prefix)
        if not listing:
            return {"success": False, "message": f"找不到编号为 {prefix} 的商品"}
        return await self.market_buy(user_id, listing["listing_id"])

    async def market_cancel(self, user_id: str, listing_id: str) -> dict:
        """下架商品。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        async with self._get_player_lock(user_id):
            snapshot = self._snapshot_player(player)
            result = await market_mod.cancel_listing(
                snapshot, listing_id, self._data_manager,
            )
            if result["success"]:
                self._apply_player_snapshot(player, snapshot)
                try:
                    await self._notify_player_update(player)
                except Exception:
                    logger.exception("修仙世界：玩家状态推送失败 user_id=%s", player.user_id)
                await self._notify_market_changed("cancel")
        return result

    async def market_cancel_by_prefix(self, user_id: str, prefix: str) -> dict:
        """通过短编号下架（聊天端用）。"""
        listing = await self._data_manager.get_listing_by_id_prefix(prefix)
        if not listing:
            return {"success": False, "message": f"找不到编号为 {prefix} 的商品"}
        return await self.market_cancel(user_id, listing["listing_id"])

    @staticmethod
    def _build_listing_detail(item_id: str) -> dict | None:
        """从注册表构建坊市商品详情 dict，用于前端弹窗展示。"""
        eq = EQUIPMENT_REGISTRY.get(item_id)
        if eq:
            return {
                "kind": "equipment",
                "name": eq.name,
                "tier": eq.tier,
                "tier_name": EQUIPMENT_TIER_NAMES.get(eq.tier, str(eq.tier)),
                "slot": eq.slot,
                "attack": eq.attack,
                "defense": eq.defense,
                "element": eq.element,
                "element_damage": eq.element_damage,
                "description": eq.description,
            }
        item_def = ITEM_REGISTRY.get(item_id)
        if item_def:
            # 心法秘籍
            hm_id = parse_heart_method_manual_id(item_id)
            if hm_id:
                hm = HEART_METHOD_REGISTRY.get(hm_id)
                if hm:
                    bonus = get_heart_method_bonus(hm.method_id, 0)
                    return {
                        "kind": "heart_method",
                        "method_id": hm.method_id,
                        "name": item_def.name,
                        "quality": hm.quality,
                        "quality_name": HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通"),
                        "realm_name": get_realm_name(hm.realm, 0),
                        "mastery": 0,
                        "mastery_name": bonus.get("mastery_name", MASTERY_LEVELS[0]),
                        "mastery_exp": 0,
                        "mastery_exp_max": hm.mastery_exp,
                        "bonus": bonus,
                        "description": hm.description or item_def.description,
                    }
            # 功法卷轴
            gf_id = parse_gongfa_scroll_id(item_id)
            if gf_id:
                gf = GONGFA_REGISTRY.get(gf_id)
                if gf:
                    return {
                        "kind": "gongfa",
                        "gongfa_id": gf.gongfa_id,
                        "name": item_def.name,
                        "tier": gf.tier,
                        "tier_name": GONGFA_TIER_NAMES.get(gf.tier, "未知"),
                        "attack_bonus": gf.attack_bonus,
                        "defense_bonus": gf.defense_bonus,
                        "hp_regen": gf.hp_regen,
                        "lingqi_regen": gf.lingqi_regen,
                        "description": item_def.description,
                    }
            # 普通消耗品
            return {
                "kind": "consumable",
                "name": item_def.name,
                "description": item_def.description,
            }
        return None

    async def market_get_listings(
        self,
        page: int = 1,
        page_size: int = 20,
        cleanup_expired: bool = True,
    ) -> dict:
        """浏览坊市商品（含过期清理 + 卖家名称补充）。"""
        if cleanup_expired:
            await self._process_market_cleanup()

        data = await market_mod.get_listings(self._data_manager, page, page_size)

        # 补充物品名称、卖家名称和物品详情
        for listing in data["listings"]:
            listing["item_name"] = market_mod.get_item_name(listing["item_id"])
            seller = self._players.get(listing["seller_id"])
            listing["seller_name"] = seller.name if seller else "未知"
            listing["item_detail"] = self._build_listing_detail(listing["item_id"])
        return data

    async def market_get_my_listings(self, user_id: str, cleanup_expired: bool = True) -> list[dict]:
        """获取我的上架记录。"""
        player = self._players.get(user_id)
        if not player:
            return []

        if cleanup_expired:
            await self._process_market_cleanup()

        listings = await market_mod.get_my_listings(player, self._data_manager)
        for listing in listings:
            listing["item_name"] = market_mod.get_item_name(listing["item_id"])
        return listings

    async def market_clear_my_history(self, user_id: str, include_expired: bool = False) -> dict:
        """清理我的历史上架记录（已售/已下架，可选含已过期）。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}
        result = await market_mod.clear_my_history(
            player,
            self._data_manager,
            include_expired=bool(include_expired),
        )
        if result.get("success"):
            await self._notify_market_changed("clear_history", int(result.get("deleted", 0)))
        return result

    async def market_fee_preview(
        self, item_id: str, quantity: int, unit_price: int,
    ) -> dict:
        """手续费预览。"""
        stats = await self._data_manager.get_market_stats(item_id)
        fee, rate = market_mod.calculate_listing_fee(unit_price, quantity, stats)
        total_price = unit_price * quantity
        return {
            "fee": fee,
            "rate": rate,
            "total_price": total_price,
            "stats": stats,
        }

    # ── 天机阁（商店） ──────────────────────────────────────

    async def shop_get_items(self, user_id: str) -> dict:
        """获取今日天机阁商品列表。"""
        from datetime import date as _date
        today = _date.today()
        items = shop_mod.generate_daily_items(today)
        today_str = today.isoformat()
        for it in items:
            limit = it.get("daily_limit", 0)
            if limit > 0:
                sold = await self._data_manager.get_shop_sold_today(it["item_id"], today_str)
                it["sold_today"] = sold
        return {"items": items, "date": today_str}

    async def shop_buy(self, user_id: str, item_id: str, quantity: int = 1) -> dict:
        """从天机阁购买物品。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}
        async with self._get_player_lock(user_id):
            snapshot = self._snapshot_player(player)
            result = await shop_mod.buy_from_shop(snapshot, item_id, quantity)
            if not result["success"]:
                return result

            purchase_meta = result.pop("_purchase_meta", None)
            if not purchase_meta:
                return {"success": False, "message": "购买失败，订单信息缺失"}

            try:
                committed = await self._data_manager.commit_shop_purchase_atomic(
                    snapshot,
                    purchase_meta["item_id"],
                    purchase_meta["quantity"],
                    purchase_meta["unit_price"],
                    purchase_meta["purchased_at"],
                    purchase_meta.get("daily_limit", 0),
                )
            except Exception:
                return {"success": False, "message": "购买失败，数据保存异常，请稍后再试"}

            if not committed["success"]:
                daily_limit = int(purchase_meta.get("daily_limit", 0) or 0)
                remaining = int(committed.get("remaining", 0) or 0)
                return {
                    "success": False,
                    "message": f"【{purchase_meta['item_name']}】今日全服限购{daily_limit}个，剩余{remaining}个",
                }

            self._apply_player_snapshot(player, snapshot)
            await self._notify_player_update(player)
            return result

    # ── 宗门系统 ─────────────────────────────────────────

    async def sect_create(self, user_id: str, name: str, description: str = "") -> dict:
        """创建宗门。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}
        reviewer = self._sect_name_reviewer or self._name_reviewer
        result = await sect_mod.create_sect(
            player, name, description, self._data_manager,
            name_reviewer=reviewer,
        )
        if result["success"]:
            await self._save_player(player)
        return result

    async def sect_disband(self, user_id: str) -> dict:
        """解散宗门。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}
        return await sect_mod.disband_sect(player, self._data_manager)

    async def sect_list(self, page: int = 1, page_size: int = 10) -> dict:
        """获取宗门列表。"""
        return await sect_mod.get_sect_list(self._data_manager, page, page_size)

    async def sect_detail(self, sect_id: str) -> dict:
        """获取宗门详情。"""
        return await sect_mod.get_sect_detail(sect_id, self._data_manager)

    async def sect_my(self, user_id: str) -> dict:
        """获取我的宗门信息。"""
        return await sect_mod.get_my_sect(user_id, self._data_manager)

    async def sect_join(self, user_id: str, sect_id: str) -> dict:
        """加入宗门。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}
        return await sect_mod.join_sect(player, sect_id, self._data_manager)

    async def sect_leave(self, user_id: str) -> dict:
        """退出宗门。"""
        return await sect_mod.leave_sect(user_id, self._data_manager)

    async def sect_kick(self, operator_id: str, target_id: str) -> dict:
        """踢出成员。"""
        return await sect_mod.kick_member(operator_id, target_id, self._data_manager)

    async def sect_set_role(self, operator_id: str, target_id: str, role: str) -> dict:
        """设置成员身份。"""
        return await sect_mod.set_member_role(
            operator_id, target_id, role, self._data_manager,
        )

    async def sect_update_info(self, operator_id: str, data: dict) -> dict:
        """修改宗门信息。"""
        return await sect_mod.update_sect_info(operator_id, data, self._data_manager)

    async def sect_transfer(self, leader_id: str, target_id: str) -> dict:
        """转让宗主。"""
        return await sect_mod.transfer_leader(leader_id, target_id, self._data_manager)

    # ── 宗门仓库 ──────────────────────────────────────────

    async def sect_warehouse_deposit(self, user_id: str, item_id: str, count: int = 1) -> dict:
        """上交物品到宗门仓库。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        def _normalize(p: Player) -> None:
            self._auto_unequip_invalid_heart_method(p, convert_ratio=0.6, force=False)
            p.heart_method_value = max(0, int(getattr(p, "heart_method_value", 0)))
            self._clamp_player_hp(p)

        result = await sect_mod.warehouse_deposit(
            player, item_id, count, self._data_manager,
            pre_commit=_normalize,
        )
        if result["success"]:
            try:
                await self._notify_player_update(player)
            except Exception:
                logger.exception("修仙世界：玩家状态推送失败 user_id=%s", user_id)
        return result

    async def sect_warehouse_exchange(self, user_id: str, item_id: str, count: int = 1) -> dict:
        """从宗门仓库兑换物品。"""
        player = self._players.get(user_id)
        if not player:
            return {"success": False, "message": "你还没有角色，请先创建"}

        def _normalize(p: Player) -> None:
            self._auto_unequip_invalid_heart_method(p, convert_ratio=0.6, force=False)
            p.heart_method_value = max(0, int(getattr(p, "heart_method_value", 0)))
            self._clamp_player_hp(p)

        result = await sect_mod.warehouse_exchange(
            player, item_id, count, self._data_manager,
            pre_commit=_normalize,
        )
        if result["success"]:
            try:
                await self._notify_player_update(player)
            except Exception:
                logger.exception("修仙世界：玩家状态推送失败 user_id=%s", user_id)
        return result

    async def sect_warehouse_list(self, user_id: str) -> dict:
        """查看宗门仓库。"""
        return await sect_mod.warehouse_list(user_id, self._data_manager)

    async def sect_set_submit_rule(self, user_id: str, quality_key: str, points: int) -> dict:
        """设置上交贡献点规则。"""
        return await sect_mod.set_submit_rule(user_id, quality_key, points, self._data_manager)

    async def sect_set_exchange_rule(
        self, user_id: str, target_key: str, points: int, *, is_item: bool = False,
    ) -> dict:
        """设置兑换贡献点规则。"""
        return await sect_mod.set_exchange_rule(
            user_id, target_key, points, self._data_manager, is_item=is_item,
        )

    async def sect_get_contribution_rules(self, user_id: str) -> dict:
        """获取宗门贡献点规则。"""
        return await sect_mod.get_contribution_rules(user_id, self._data_manager)
