"""游戏数据模型：Player、序列化/反序列化。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from .constants import (
    REALM_CONFIG, EQUIPMENT_REGISTRY, EQUIPMENT_TIER_NAMES,
    HEART_METHOD_REGISTRY, HEART_METHOD_QUALITY_NAMES, MASTERY_LEVELS,
    GONGFA_REGISTRY, GONGFA_TIER_NAMES, MASTERY_MAX,
    RealmLevel, get_equip_bonus, get_heart_method_bonus,
    get_gongfa_bonus, get_total_gongfa_bonus, get_realm_name,
    get_player_base_max_lingqi,
)


@dataclass
class Player:
    """玩家角色数据。"""
    user_id: str
    name: str
    realm: int = RealmLevel.MORTAL
    sub_realm: int = 0  # 小境界（练气~元婴: 0-9, 化神~大乘: 0-3）
    exp: int = 0
    hp: int = 100
    max_hp: int = 100
    attack: int = 10
    defense: int = 5
    spirit_stones: int = 0
    lingqi: int = 50          # 灵气
    permanent_max_hp_bonus: int = 0    # 丹药提供的永久生命上限加成
    permanent_attack_bonus: int = 0    # 丹药提供的永久攻击加成
    permanent_defense_bonus: int = 0   # 丹药提供的永久防御加成
    permanent_lingqi_bonus: int = 0    # 丹药提供的永久灵气上限加成
    heart_method: str = "无"  # 心法ID（或 "无"）
    weapon: str = "无"       # 武器
    gongfa_1: str = "无"     # 功法一
    gongfa_2: str = "无"     # 功法二
    gongfa_3: str = "无"     # 功法三
    armor: str = "无"        # 护甲
    dao_yun: int = 0         # 道韵
    breakthrough_bonus: float = 0.0  # 突破失败累积加成（最高0.2）
    breakthrough_pill_count: int = 0  # 已使用的突破丹数量
    heart_method_mastery: int = 0  # 心法修炼阶段 0=入门,1=小成,2=大成,3=圆满
    heart_method_exp: int = 0      # 当前阶段已积累的心法经验
    heart_method_value: int = 0    # 预存心法值（用于降境后转化）
    gongfa_1_mastery: int = 0      # 功法一修炼阶段
    gongfa_1_exp: int = 0          # 功法一当前阶段经验
    gongfa_2_mastery: int = 0      # 功法二修炼阶段
    gongfa_2_exp: int = 0          # 功法二当前阶段经验
    gongfa_3_mastery: int = 0      # 功法三修炼阶段
    gongfa_3_exp: int = 0          # 功法三当前阶段经验
    inventory: dict[str, int] = field(default_factory=dict)  # {item_id: count}
    stored_heart_methods: dict[str, float] = field(default_factory=dict)  # {method_id: expire_timestamp}
    created_at: float = field(default_factory=time.time)
    last_cultivate_time: float = 0.0
    last_checkin_date: Optional[str] = None  # 上次签到日期 "YYYY-MM-DD"
    afk_cultivate_start: float = 0.0  # 挂机修炼开始时间戳
    afk_cultivate_end: float = 0.0    # 挂机修炼预定结束时间戳
    last_adventure_time: float = 0.0  # 上次历练时间戳
    death_count: int = 0              # 死亡次数
    active_buffs: list[dict] = field(default_factory=list)  # 丹药临时buff列表
    unified_msg_origin: Optional[str] = None
    password_hash: Optional[str] = None  # Web 登录密码哈希

    def to_dict(self, include_sensitive: bool = False) -> dict:
        """序列化为字典。include_sensitive=True 时包含密码哈希（仅用于存储）。"""
        realm_cfg = REALM_CONFIG.get(self.realm, {})

        # 装备加成
        bonus = get_equip_bonus(self.weapon, self.armor)

        # 装备详情辅助
        def _equip_info(eid: str) -> dict | None:
            eq = EQUIPMENT_REGISTRY.get(eid)
            if not eq:
                return None
            return {
                "equip_id": eq.equip_id,
                "name": eq.name,
                "tier": eq.tier,
                "tier_name": EQUIPMENT_TIER_NAMES.get(eq.tier, "未知"),
                "slot": eq.slot,
                "attack": eq.attack,
                "defense": eq.defense,
                "element": eq.element,
                "element_damage": eq.element_damage,
                "description": eq.description,
            }

        weapon_eq = EQUIPMENT_REGISTRY.get(self.weapon)
        armor_eq = EQUIPMENT_REGISTRY.get(self.armor)

        # 心法信息
        hm = HEART_METHOD_REGISTRY.get(self.heart_method)
        hm_bonus = get_heart_method_bonus(self.heart_method, self.heart_method_mastery)
        if hm:
            hm_info = {
                "method_id": hm.method_id,
                "name": hm.name,
                "quality": hm.quality,
                "quality_name": HEART_METHOD_QUALITY_NAMES.get(hm.quality, ""),
                "mastery": self.heart_method_mastery,
                "mastery_name": MASTERY_LEVELS[min(self.heart_method_mastery, len(MASTERY_LEVELS) - 1)],
                "mastery_exp": self.heart_method_exp,
                "mastery_exp_max": hm.mastery_exp,
                "description": hm.description,
                "bonus": hm_bonus,
            }
            hm_display = hm.name
        else:
            hm_info = None
            hm_display = "无"

        # 功法信息
        def _gongfa_info(slot: str) -> dict | None:
            gongfa_id = getattr(self, slot, "无")
            if not gongfa_id or gongfa_id == "无":
                return None
            gf = GONGFA_REGISTRY.get(gongfa_id)
            if not gf:
                return None
            mastery = getattr(self, f"{slot}_mastery", 0)
            exp = getattr(self, f"{slot}_exp", 0)
            gf_bonus = get_gongfa_bonus(gongfa_id, mastery, self.realm)
            return {
                "gongfa_id": gf.gongfa_id,
                "name": gf.name,
                "tier": gf.tier,
                "tier_name": GONGFA_TIER_NAMES.get(gf.tier, "未知"),
                "lingqi_cost": gf.lingqi_cost,
                "mastery": mastery,
                "mastery_name": MASTERY_LEVELS[min(mastery, MASTERY_MAX)],
                "mastery_exp": exp,
                "mastery_exp_max": gf.mastery_exp,
                "attack_bonus": gf.attack_bonus,
                "defense_bonus": gf.defense_bonus,
                "hp_regen": gf.hp_regen,
                "lingqi_regen": gf.lingqi_regen,
                "description": gf.description,
                "bonus": gf_bonus,
            }

        gongfa_total = get_total_gongfa_bonus(self)

        # 丹药buff加成
        from .pills import get_buff_totals, get_active_buffs_display
        buff_totals = get_buff_totals(self)

        d = {
            "user_id": self.user_id,
            "name": self.name,
            "realm": self.realm,
            "sub_realm": self.sub_realm,
            "realm_name": get_realm_name(self.realm, self.sub_realm),
            "exp": self.exp,
            "exp_to_next": realm_cfg.get("exp_to_next", 0),
            "hp": self.hp,
            "max_hp": self.max_hp,
            "attack": self.attack,
            "defense": self.defense,
            "total_attack": max(0, self.attack + bonus["attack"] + hm_bonus["attack_bonus"] + gongfa_total["attack_bonus"] + buff_totals["attack_boost"] - buff_totals["attack_reduction"]),
            "total_defense": max(0, self.defense + bonus["defense"] + hm_bonus["defense_bonus"] + gongfa_total["defense_bonus"] + buff_totals["defense_boost"] - buff_totals["defense_reduction"]),
            "buff_totals": buff_totals,
            "active_buffs": get_active_buffs_display(self),
            "equip_bonus": bonus,
            "gongfa_bonus": gongfa_total,
            "spirit_stones": self.spirit_stones,
            "lingqi": self.lingqi,
            "max_lingqi": get_player_base_max_lingqi(self),
            "heart_method": self.heart_method,
            "heart_method_name": hm_display,
            "heart_method_info": hm_info,
            "heart_method_mastery": self.heart_method_mastery,
            "heart_method_exp": self.heart_method_exp,
            "heart_method_value": self.heart_method_value,
            "weapon": self.weapon,
            "weapon_name": weapon_eq.name if weapon_eq else "无",
            "weapon_info": _equip_info(self.weapon),
            "gongfa_1": self.gongfa_1,
            "gongfa_2": self.gongfa_2,
            "gongfa_3": self.gongfa_3,
            "gongfa_1_info": _gongfa_info("gongfa_1"),
            "gongfa_2_info": _gongfa_info("gongfa_2"),
            "gongfa_3_info": _gongfa_info("gongfa_3"),
            "gongfa_1_mastery": self.gongfa_1_mastery,
            "gongfa_1_exp": self.gongfa_1_exp,
            "gongfa_2_mastery": self.gongfa_2_mastery,
            "gongfa_2_exp": self.gongfa_2_exp,
            "gongfa_3_mastery": self.gongfa_3_mastery,
            "gongfa_3_exp": self.gongfa_3_exp,
            "armor": self.armor,
            "armor_name": armor_eq.name if armor_eq else "无",
            "armor_info": _equip_info(self.armor),
            "dao_yun": self.dao_yun,
            "permanent_max_hp_bonus": self.permanent_max_hp_bonus,
            "permanent_attack_bonus": self.permanent_attack_bonus,
            "permanent_defense_bonus": self.permanent_defense_bonus,
            "permanent_lingqi_bonus": self.permanent_lingqi_bonus,
            "breakthrough_bonus": self.breakthrough_bonus,
            "breakthrough_pill_count": self.breakthrough_pill_count,
            "inventory": dict(self.inventory),
            "stored_heart_methods": dict(self.stored_heart_methods),
            "created_at": self.created_at,
            "last_cultivate_time": self.last_cultivate_time,
            "last_checkin_date": self.last_checkin_date,
            "afk_cultivate_start": self.afk_cultivate_start,
            "afk_cultivate_end": self.afk_cultivate_end,
            "last_adventure_time": self.last_adventure_time,
            "death_count": self.death_count,
            "has_password": self.password_hash is not None,
        }
        if include_sensitive:
            d["password_hash"] = self.password_hash
            d["unified_msg_origin"] = self.unified_msg_origin
            d["active_buffs_raw"] = list(self.active_buffs)
        return d

    @classmethod
    def from_dict(cls, data: dict) -> Player:
        """从字典反序列化。"""
        return cls(
            user_id=data["user_id"],
            name=data["name"],
            realm=data.get("realm", RealmLevel.MORTAL),
            sub_realm=data.get("sub_realm", 0),
            exp=data.get("exp", 0),
            hp=data.get("hp", 100),
            max_hp=data.get("max_hp", 100),
            attack=data.get("attack", 10),
            defense=data.get("defense", 5),
            spirit_stones=data.get("spirit_stones", 0),
            lingqi=data.get("lingqi", 50),
            permanent_max_hp_bonus=data.get("permanent_max_hp_bonus", 0),
            permanent_attack_bonus=data.get("permanent_attack_bonus", 0),
            permanent_defense_bonus=data.get("permanent_defense_bonus", 0),
            permanent_lingqi_bonus=data.get("permanent_lingqi_bonus", 0),
            heart_method=data.get("heart_method", "无"),
            weapon=data.get("weapon", "无"),
            gongfa_1=data.get("gongfa_1", "无"),
            gongfa_2=data.get("gongfa_2", "无"),
            gongfa_3=data.get("gongfa_3", "无"),
            armor=data.get("armor", "无"),
            dao_yun=data.get("dao_yun", 0),
            breakthrough_bonus=data.get("breakthrough_bonus", 0.0),
            breakthrough_pill_count=data.get("breakthrough_pill_count", 0),
            heart_method_mastery=data.get("heart_method_mastery", 0),
            heart_method_exp=data.get("heart_method_exp", 0),
            heart_method_value=data.get("heart_method_value", 0),
            gongfa_1_mastery=data.get("gongfa_1_mastery", 0),
            gongfa_1_exp=data.get("gongfa_1_exp", 0),
            gongfa_2_mastery=data.get("gongfa_2_mastery", 0),
            gongfa_2_exp=data.get("gongfa_2_exp", 0),
            gongfa_3_mastery=data.get("gongfa_3_mastery", 0),
            gongfa_3_exp=data.get("gongfa_3_exp", 0),
            inventory=data.get("inventory", {}),
            stored_heart_methods=data.get("stored_heart_methods", {}),
            created_at=data.get("created_at", time.time()),
            last_cultivate_time=data.get("last_cultivate_time", 0.0),
            last_checkin_date=data.get("last_checkin_date"),
            afk_cultivate_start=data.get("afk_cultivate_start", 0.0),
            afk_cultivate_end=data.get("afk_cultivate_end", 0.0),
            last_adventure_time=data.get("last_adventure_time", 0.0),
            death_count=data.get("death_count", 0),
            active_buffs=data.get("active_buffs_raw", data.get("active_buffs", [])),
            unified_msg_origin=data.get("unified_msg_origin"),
            password_hash=data.get("password_hash"),
        )
