"""丹药系统 —— 200 种丹药定义、品阶品级体系与 buff 管理。

品阶: 凡阶 / 黄阶 / 玄阶 / 地阶 / 天阶
品级: 下品 / 上品 / 无垢

丹药分为 **永久型** 和 **限时型** 两大类：
* 永久型 —— 立即生效，效果永久
* 限时型 —— 产生持续一段时间的 buff；下品/上品会附带副作用，无垢无副作用
"""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Player


# ═══════════════════════ 枚举与常量 ═══════════════════════

class PillTier(IntEnum):
    FAN = 0     # 凡阶
    HUANG = 1   # 黄阶
    XUAN = 2    # 玄阶
    DI = 3      # 地阶
    TIAN = 4    # 天阶


class PillGrade(IntEnum):
    LOW = 0     # 下品
    HIGH = 1    # 上品
    PURE = 2    # 无垢


PILL_TIER_NAMES: dict[int, str] = {
    0: "凡阶", 1: "黄阶", 2: "玄阶", 3: "地阶", 4: "天阶",
}

PILL_GRADE_NAMES: dict[int, str] = {
    0: "下品", 1: "上品", 2: "无垢",
}


# ═══════════════════════ 丹药定义 ═══════════════════════

@dataclass
class PillDef:
    pill_id: str
    name: str            # 含品级前缀的显示名
    tier: int            # PillTier
    grade: int           # PillGrade
    category: str        # 丹药类别 key
    description: str
    price: int           # 灵石售价
    effects: dict = field(default_factory=dict)
    is_temp: bool = False
    duration: int = 0    # 秒，0 = 永久/瞬发
    side_effects: dict = field(default_factory=dict)
    side_effect_desc: str = ""


# ─── 命名表 (category → {tier: base_name}) ───────────────

_NAMES: dict[str, dict[int, str]] = {
    # 永久型
    "healing":        {0: "回春丹",   1: "续命丹",   2: "还魂丹",   3: "九转还魂丹", 4: "太上回春丹"},
    "attack":         {0: "增力丹",   1: "虎力丹",   2: "龙力丹",   3: "天罡丹",     4: "混元力丹"},
    "defense":        {0: "铁皮丹",   1: "玄铁丹",   2: "金刚丹",   3: "不灭金身丹", 4: "混元护体丹"},
    "lingqi":         {0: "聚气丹",   1: "灵泉丹",   2: "天灵丹",   3: "九霄灵丹",   4: "太清聚灵丹"},
    "dao_yun":        {0: "悟道丹",   1: "明心丹",   2: "通玄丹",   3: "天道丹",     4: "混元道韵丹"},
    "exp":            {0: "聚灵散",   1: "明悟丹",   2: "天悟丹",   3: "造化丹",     4: "太上造化丹"},
    "max_hp":         {0: "固本丹",   1: "培元丹",   2: "天元丹",   3: "造化培元丹", 4: "混元固本丹"},
    "breakthrough":   {0: "破障丹",   1: "筑基丹",   2: "天机丹",   3: "造化破境丹", 4: "混元破境丹"},
    # 限时型
    "temp_attack":    {0: "狂暴丹",   1: "嗜血丹",   2: "魔化丹",   3: "天魔狂暴丹", 4: "太上狂暴丹"},
    "temp_defense":   {0: "铁壁丹",   1: "龟甲丹",   2: "玄武丹",   3: "磐石丹",     4: "混元铁壁丹"},
    "temp_lingqi":    {0: "灵涌丹",   1: "灵泉散",   2: "天灵散",   3: "灵脉丹",     4: "太清灵涌丹"},
    "temp_cultivate": {0: "静心丹",   1: "空明丹",   2: "禅定丹",   3: "大定丹",     4: "太上禅定丹"},
    "temp_all":       {0: "全灵丹",   1: "万灵丹",   2: "天地丹",   3: "造化万灵丹", 4: "太极丹"},
}

# ─── 效果模板 ─────────────────────────────────────────────
# values 下标对应品阶 0-4；限时类额外带 temp/side_key/side_values

_TEMPLATES: dict[str, dict] = {
    "healing": {
        "key": "heal_hp",
        "values": [50, 150, 400, 1000, 2500],
        "desc": "恢复{v}点生命值",
    },
    "attack": {
        "key": "attack_boost",
        "values": [5, 15, 40, 100, 250],
        "desc": "永久增加{v}点攻击力",
    },
    "defense": {
        "key": "defense_boost",
        "values": [3, 10, 25, 65, 160],
        "desc": "永久增加{v}点防御力",
    },
    "lingqi": {
        "key": "lingqi_boost",
        "values": [20, 60, 150, 400, 1000],
        "desc": "永久增加{v}点灵气上限",
    },
    "dao_yun": {
        "key": "dao_yun_boost",
        "values": [50, 150, 400, 1000, 2500],
        "desc": "永久增加{v}点道韵",
    },
    "exp": {
        "key": "exp_bonus",
        "values": [100, 300, 800, 2000, 5000],
        "desc": "获得{v}点修炼经验",
    },
    "max_hp": {
        "key": "max_hp_boost",
        "values": [20, 60, 150, 400, 1000],
        "desc": "永久增加{v}点生命上限",
    },
    "breakthrough": {
        "key": "breakthrough_bonus",
        "values": [0.05, 0.10, 0.15, 0.20, 0.30],
        "desc_fmt": True,
    },
    # ── 限时型 ──
    "temp_attack": {
        "key": "attack_boost", "values": [15, 45, 120, 300, 750],
        "temp": True,
        "side_key": "defense_reduction", "side_values": [5, 15, 40, 100, 250],
        "desc": "限时增加{v}点攻击力", "side_desc": "防御力降低{sv}点",
    },
    "temp_defense": {
        "key": "defense_boost", "values": [10, 30, 80, 200, 500],
        "temp": True,
        "side_key": "attack_reduction", "side_values": [4, 12, 32, 80, 200],
        "desc": "限时增加{v}点防御力", "side_desc": "攻击力降低{sv}点",
    },
    "temp_lingqi": {
        "key": "lingqi_boost", "values": [30, 90, 240, 600, 1500],
        "temp": True,
        "side_key": "max_hp_reduction", "side_values": [10, 30, 75, 200, 500],
        "desc": "限时增加{v}点灵气", "side_desc": "生命上限降低{sv}点",
    },
    "temp_cultivate": {
        "key": "cultivate_speed", "values": [1.2, 1.5, 2.0, 3.0, 5.0],
        "temp": True,
        "side_key": "attack_reduction", "side_values": [3, 8, 20, 50, 125],
        "desc": "修炼速度×{v}", "side_desc": "攻击力降低{sv}点",
    },
    "temp_all": {
        "key": "all_boost", "values": [3, 8, 20, 50, 125],
        "temp": True,
        "side_key": "lingqi_reduction", "side_values": [10, 30, 80, 200, 500],
        "desc": "限时增加攻防各{v}点", "side_desc": "灵气上限降低{sv}点",
    },
}

# ─── 数值缩放表 ──────────────────────────────────────────

_GRADE_MULT: dict[int, float] = {0: 1.0, 1: 1.5, 2: 2.0}
_GRADE_PRICE_MULT: dict[int, float] = {0: 1.0, 1: 2.0, 2: 5.0}
_BASE_PRICE: dict[int, int] = {0: 100, 1: 500, 2: 2000, 3: 10000, 4: 50000}
_DURATION: dict[int, int] = {0: 1800, 1: 3600, 2: 7200, 3: 14400, 4: 28800}
_SIDE_GRADE_MULT: dict[int, float] = {0: 1.0, 1: 0.5, 2: 0.0}


# ═══════════════════════ 生成函数 ═══════════════════════

def _generate_pills() -> dict[str, PillDef]:
    """程序化生成 195 + 5 = 200 种丹药。"""
    registry: dict[str, PillDef] = {}

    for cat, tmpl in _TEMPLATES.items():
        names = _NAMES[cat]
        is_temp = tmpl.get("temp", False)

        for tier in range(5):
            base_name = names[tier]
            base_val = tmpl["values"][tier]

            for grade in range(3):
                pill_id = f"pill_{cat}_{tier}_{grade}"
                grade_name = PILL_GRADE_NAMES[grade]
                full_name = f"{grade_name}{base_name}"

                mult = _GRADE_MULT[grade]
                val = round(base_val * mult, 2) if isinstance(base_val, float) else int(base_val * mult)

                price = int(_BASE_PRICE[tier] * _GRADE_PRICE_MULT[grade])

                # 构造效果字典
                if tmpl["key"] == "all_boost":
                    effects = {"attack_boost": val, "defense_boost": val, "max_hp_boost": val * 2}
                else:
                    effects = {tmpl["key"]: val}

                # 描述
                if tmpl.get("desc_fmt"):
                    desc = f"突破成功率+{int(val * 100)}%"
                elif tmpl["key"] == "all_boost":
                    desc = f"限时增加攻防各{val}点，生命+{val * 2}"
                else:
                    desc = tmpl["desc"].format(v=val)

                duration = _DURATION[tier] if is_temp else 0

                # 副作用（下品全额、上品半额、无垢无）
                side_effects: dict = {}
                side_desc = ""
                if is_temp and grade < 2:
                    s_mult = _SIDE_GRADE_MULT[grade]
                    sv_base = tmpl["side_values"][tier]
                    sv = int(sv_base * s_mult) if isinstance(sv_base, int) else round(sv_base * s_mult, 2)
                    if sv > 0:
                        side_effects[tmpl["side_key"]] = sv
                        side_desc = tmpl["side_desc"].format(sv=sv)

                # 拼接完整描述
                if is_temp:
                    h = duration // 3600
                    m = (duration % 3600) // 60
                    t_str = f"{h}小时" if h else f"{m}分钟"
                    desc += f"（持续{t_str}）"
                    if side_desc:
                        desc += f"，副作用：{side_desc}"

                tier_name = PILL_TIER_NAMES[tier]
                full_desc = f"【{tier_name}·{grade_name}】{desc}"

                registry[pill_id] = PillDef(
                    pill_id=pill_id,
                    name=full_name,
                    tier=tier,
                    grade=grade,
                    category=cat,
                    description=full_desc,
                    price=price,
                    effects=effects,
                    is_temp=is_temp,
                    duration=duration,
                    side_effects=side_effects,
                    side_effect_desc=side_desc,
                )

    # ── 5 颗特殊丹药 ──
    _specials = [
        PillDef(
            "pill_special_nirvana", "涅槃天丹", 4, 2, "special",
            "【天阶·无垢】传说中的涅槃天丹，永久大幅提升全属性",
            200000,
            {"attack_boost": 500, "defense_boost": 300, "max_hp_boost": 2000,
             "lingqi_boost": 2000, "dao_yun_boost": 5000},
        ),
        PillDef(
            "pill_special_reborn", "脱胎换骨丹", 4, 2, "special",
            "【天阶·无垢】清除所有丹药副作用并恢复全部生命",
            150000,
            {"clear_debuffs": True, "heal_full": True},
        ),
        PillDef(
            "pill_special_wuxiang", "无相丹", 4, 2, "special",
            "【天阶·无垢】永久增加攻击300、防御200",
            180000,
            {"attack_boost": 300, "defense_boost": 200},
        ),
        PillDef(
            "pill_special_longevity", "万寿丹", 4, 2, "special",
            "【天阶·无垢】恢复全部生命并增加1500生命上限",
            160000,
            {"heal_full": True, "max_hp_boost": 1500},
        ),
        PillDef(
            "pill_special_insight", "顿悟丹", 4, 2, "special",
            "【天阶·无垢】获得10000经验和5000道韵",
            170000,
            {"exp_bonus": 10000, "dao_yun_boost": 5000},
        ),
    ]
    for sp in _specials:
        registry[sp.pill_id] = sp

    return registry


PILL_REGISTRY: dict[str, PillDef] = _generate_pills()


# ═══════════════════════ ItemDef 桥接 ═══════════════════════

def get_pill_item_defs() -> dict:
    """将 PILL_REGISTRY 转为 {pill_id: ItemDef} 供 ITEM_REGISTRY 注册。"""
    from .constants import ItemDef

    items: dict = {}
    for pid, pill in PILL_REGISTRY.items():
        effect = dict(pill.effects)
        if pill.is_temp:
            effect["_temp_buff"] = True
            effect["_duration"] = pill.duration
            if pill.side_effects:
                effect["_side_effects"] = dict(pill.side_effects)
        items[pid] = ItemDef(
            item_id=pid,
            name=pill.name,
            item_type="consumable",
            description=pill.description,
            effect=effect,
        )
    return items


# ═══════════════════════ Buff 管理 ═══════════════════════

def clean_expired_buffs(player: Player) -> list[dict]:
    """清除过期 buff，返回被移除的列表。"""
    now = time.time()
    expired, active = [], []
    for b in getattr(player, "active_buffs", []) or []:
        if 0 < b.get("expire_time", 0) <= now:
            expired.append(b)
        else:
            active.append(b)
    player.active_buffs = active
    return expired


def get_buff_totals(player: Player) -> dict:
    """计算当前所有活跃 buff 的净效果（含副作用）。"""
    clean_expired_buffs(player)
    totals: dict[str, int | float] = {
        "attack_boost": 0,
        "defense_boost": 0,
        "max_hp_boost": 0,
        "lingqi_boost": 0,
        "cultivate_speed": 0.0,
        # 副作用
        "attack_reduction": 0,
        "defense_reduction": 0,
        "max_hp_reduction": 0,
        "lingqi_reduction": 0,
    }
    for b in getattr(player, "active_buffs", []) or []:
        for k, v in b.get("effects", {}).items():
            if k in totals:
                totals[k] = totals[k] + v
        for k, v in b.get("side_effects", {}).items():
            if k in totals:
                totals[k] = totals[k] + v
    return totals


def get_effective_combat_stats(player: Player) -> dict:
    """计算应用丹药 buff 后的即时战斗属性。"""
    from .constants import get_player_base_max_lingqi

    totals = get_buff_totals(player)
    hp_delta = int(totals["max_hp_boost"] - totals["max_hp_reduction"])
    lingqi_delta = int(totals["lingqi_boost"] - totals["lingqi_reduction"])

    effective_max_hp = max(1, int(player.max_hp + hp_delta))
    effective_hp = max(0, min(effective_max_hp, int(player.hp + hp_delta)))

    base_max_lingqi = get_player_base_max_lingqi(player)
    effective_max_lingqi = max(0, int(base_max_lingqi + lingqi_delta))
    effective_lingqi = max(0, min(effective_max_lingqi, int(player.lingqi + lingqi_delta)))

    return {
        "attack": max(1, int(player.attack + totals["attack_boost"] - totals["attack_reduction"])),
        "defense": max(1, int(player.defense + totals["defense_boost"] - totals["defense_reduction"])),
        "hp": effective_hp,
        "max_hp": effective_max_hp,
        "lingqi": effective_lingqi,
        "max_lingqi": effective_max_lingqi,
        "hp_delta": hp_delta,
        "lingqi_delta": lingqi_delta,
    }


def apply_pill_buff(player: Player, pill_id: str) -> str:
    """将限时丹药效果写入 player.active_buffs，返回描述文本。"""
    pill = PILL_REGISTRY.get(pill_id)
    if not pill or not pill.is_temp:
        return ""

    buff: dict = {
        "buff_id": f"buff_{uuid.uuid4().hex[:8]}",
        "pill_id": pill_id,
        "pill_name": pill.name,
        "effects": dict(pill.effects),
        "side_effects": dict(pill.side_effects),
        "expire_time": time.time() + pill.duration,
        "duration": pill.duration,
    }

    if not hasattr(player, "active_buffs") or player.active_buffs is None:
        player.active_buffs = []
    player.active_buffs.append(buff)

    # 构造描述
    parts: list[str] = []
    _EFFECT_LABELS = {
        "attack_boost": "攻击+{v}",
        "defense_boost": "防御+{v}",
        "max_hp_boost": "生命上限+{v}",
        "lingqi_boost": "灵气+{v}",
        "cultivate_speed": "修炼速度×{v}",
    }
    for k, v in pill.effects.items():
        fmt = _EFFECT_LABELS.get(k)
        if fmt:
            parts.append(fmt.format(v=v))

    h = pill.duration // 3600
    m = (pill.duration % 3600) // 60
    t_str = f"{h}小时" if h else f"{m}分钟"
    msg = f"{'，'.join(parts)}（持续{t_str}）"

    if pill.side_effects:
        _SIDE_LABELS = {
            "attack_reduction": "攻击-{v}",
            "defense_reduction": "防御-{v}",
            "max_hp_reduction": "生命上限-{v}",
            "lingqi_reduction": "灵气上限-{v}",
        }
        sp = [_SIDE_LABELS[k].format(v=v) for k, v in pill.side_effects.items() if k in _SIDE_LABELS]
        if sp:
            msg += f"，副作用：{'，'.join(sp)}"

    return msg


def get_active_buffs_display(player: Player) -> list[dict]:
    """返回适合前端展示的活跃 buff 列表。"""
    clean_expired_buffs(player)
    now = time.time()
    result = []
    for b in getattr(player, "active_buffs", []) or []:
        remaining = max(0, int(b.get("expire_time", 0) - now))
        result.append({
            "buff_id": b.get("buff_id", ""),
            "pill_name": b.get("pill_name", ""),
            "effects": b.get("effects", {}),
            "side_effects": b.get("side_effects", {}),
            "remaining_seconds": remaining,
        })
    return result


# ═══════════════════════ 商店/掉落权重 ═══════════════════════

# 天机阁品阶出现权重（总 10000）
SHOP_PILL_TIER_WEIGHTS: dict[int, int] = {
    0: 5000,   # 凡阶 50%
    1: 3000,   # 黄阶 30%
    2: 1400,   # 玄阶 14%
    3: 500,    # 地阶 5%
    4: 100,    # 天阶 1%
}

# 天机阁品级权重（各品阶总 10000）
SHOP_PILL_GRADE_WEIGHTS: dict[int, dict[int, int]] = {
    0: {0: 7000, 1: 2500, 2: 500},
    1: {0: 7000, 1: 2600, 2: 400},
    2: {0: 7500, 1: 2200, 2: 300},
    3: {0: 8000, 1: 1800, 2: 200},
    4: {0: 9000, 1: 900,  2: 100},   # 天阶无垢 ≈ 1%×1% = 万分之一
}

# 秘境掉落权重 —— 仅玄阶及以下
DUNGEON_PILL_TIER_WEIGHTS: dict[int, int] = {0: 5000, 1: 3500, 2: 1500}
DUNGEON_PILL_GRADE_WEIGHTS: dict[int, dict[int, int]] = {
    0: {0: 7000, 1: 2990, 2: 10},   # 无垢 ≈ 千分之一
    1: {0: 7000, 1: 2990, 2: 10},
    2: {0: 7000, 1: 2990, 2: 10},
}

# 不参与随机掉落的类别
_NON_DROP_CATEGORIES = {"special"}


def pick_random_pill(
    rng: random.Random,
    tier_weights: dict[int, int],
    grade_weights: dict[int, dict[int, int]],
    category_filter: set[str] | None = None,
) -> PillDef | None:
    """根据权重随机选取一颗丹药。"""
    tiers = list(tier_weights.keys())
    t_w = [tier_weights[t] for t in tiers]
    tier = rng.choices(tiers, weights=t_w, k=1)[0]

    gw = grade_weights.get(tier, {0: 7000, 1: 2500, 2: 500})
    grades = list(gw.keys())
    g_w = [gw[g] for g in grades]
    grade = rng.choices(grades, weights=g_w, k=1)[0]

    candidates = [
        p for p in PILL_REGISTRY.values()
        if p.tier == tier and p.grade == grade and p.category not in _NON_DROP_CATEGORIES
    ]
    if category_filter:
        candidates = [p for p in candidates if p.category in category_filter]
    if not candidates:
        return None
    return rng.choice(candidates)
