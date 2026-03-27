"""游戏常量：境界配置、物品注册表、装备注册表。"""

import hashlib
import threading
from dataclasses import dataclass, field
from datetime import date
from enum import IntEnum

# 保护注册表热更新（clear+update+refresh）的原子性
_registry_lock = threading.Lock()


class RealmLevel(IntEnum):
    """修仙大境界等级。"""
    MORTAL = 0           # 凡人
    QI_REFINING = 1      # 练气期
    FOUNDATION = 2       # 筑基期
    GOLDEN_CORE = 3      # 金丹期
    NASCENT_SOUL = 4     # 元婴期
    DEITY_TRANSFORM = 5  # 化神期
    VOID_MERGE = 6       # 合虚期
    TRIBULATION = 7      # 渡劫期
    MAHAYANA = 8         # 大乘期


# 有小境界的大境界范围
SUB_REALM_MIN = RealmLevel.QI_REFINING
SUB_REALM_MAX = RealmLevel.MAHAYANA
MAX_SUB_REALM = 9  # 练气~元婴: 0=一层, 9=十层(圆满)
MAX_HIGH_SUB_REALM = 3  # 化神~大乘: 0=初期, 3=圆满

SUB_REALM_NAMES = ["一层", "二层", "三层", "四层", "五层", "六层", "七层", "八层", "九层", "圆满"]
HIGH_SUB_REALM_NAMES = ["初期", "中期", "后期", "圆满"]

# 元婴期开始，突破大境界有死亡概率
DEATH_REALM_START = RealmLevel.NASCENT_SOUL


REALM_CONFIG: dict[int, dict] = {
    RealmLevel.MORTAL: {
        "name": "凡人",
        "has_sub_realm": False,
        "exp_to_next": 100,        # 突破到练气
        "sub_exp_to_next": 0,      # 无小境界
        "base_hp": 100,
        "base_attack": 10,
        "base_defense": 5,
        "base_lingqi": 50,
        "breakthrough_rate": 1.0,
        "death_rate": 0.0,
    },
    RealmLevel.QI_REFINING: {
        "name": "练气期",
        "has_sub_realm": True,
        "exp_to_next": 500,         # 小境界升级所需经验
        "sub_exp_to_next": 50,      # 每层小境界需要的经验
        "base_hp": 300,
        "base_attack": 30,
        "base_defense": 15,
        "base_lingqi": 120,
        "breakthrough_rate": 0.85,
        "death_rate": 0.0,
    },
    RealmLevel.FOUNDATION: {
        "name": "筑基期",
        "has_sub_realm": True,
        "exp_to_next": 1500,
        "sub_exp_to_next": 150,
        "base_hp": 800,
        "base_attack": 80,
        "base_defense": 40,
        "base_lingqi": 300,
        "breakthrough_rate": 0.7,
        "death_rate": 0.0,
    },
    RealmLevel.GOLDEN_CORE: {
        "name": "金丹期",
        "has_sub_realm": True,
        "exp_to_next": 5000,
        "sub_exp_to_next": 500,
        "base_hp": 2000,
        "base_attack": 200,
        "base_defense": 100,
        "base_lingqi": 700,
        "breakthrough_rate": 0.5,
        "death_rate": 0.0,
    },
    RealmLevel.NASCENT_SOUL: {
        "name": "元婴期",
        "has_sub_realm": True,
        "exp_to_next": 15000,
        "sub_exp_to_next": 1500,
        "base_hp": 5000,
        "base_attack": 500,
        "base_defense": 250,
        "base_lingqi": 1600,
        "breakthrough_rate": 0.35,
        "death_rate": 0.05,         # 5% 死亡
    },
    RealmLevel.DEITY_TRANSFORM: {
        "name": "化神期",
        "has_sub_realm": True,
        "high_realm": True,
        "exp_to_next": 50000,
        "sub_exp_to_next": 12500,
        "base_hp": 12000,
        "base_attack": 1200,
        "base_defense": 600,
        "base_lingqi": 3600,
        "breakthrough_rate": 0.25,
        "death_rate": 0.10,
        "sub_dao_yun_costs": [50, 80, 100, 120],
        "breakthrough_dao_yun_cost": 120,
        "dao_yun_base_rate": 0.10,      # 化神初期基础道韵产出概率
        "dao_yun_per_sub_realm": 0.05,  # 每个小境界 +5%
    },
    RealmLevel.VOID_MERGE: {
        "name": "合虚期",
        "has_sub_realm": True,
        "high_realm": True,
        "exp_to_next": 150000,
        "sub_exp_to_next": 37500,
        "base_hp": 30000,
        "base_attack": 3000,
        "base_defense": 1500,
        "base_lingqi": 8000,
        "breakthrough_rate": 0.18,
        "death_rate": 0.15,
        "sub_dao_yun_costs": [240, 260, 280, 300],
        "breakthrough_dao_yun_cost": 300,
        "dao_yun_base_rate": 0.10,
        "dao_yun_per_sub_realm": 0.05,
    },
    RealmLevel.TRIBULATION: {
        "name": "渡劫期",
        "has_sub_realm": True,
        "high_realm": True,
        "exp_to_next": 500000,
        "sub_exp_to_next": 125000,
        "base_hp": 80000,
        "base_attack": 8000,
        "base_defense": 4000,
        "base_lingqi": 18000,
        "breakthrough_rate": 0.12,
        "death_rate": 0.20,
        "sub_dao_yun_costs": [600, 640, 680, 720],
        "breakthrough_dao_yun_cost": 720,
        "dao_yun_base_rate": 0.10,
        "dao_yun_per_sub_realm": 0.05,
    },
    RealmLevel.MAHAYANA: {
        "name": "大乘期",
        "has_sub_realm": True,
        "high_realm": True,
        "exp_to_next": 999999999,
        "sub_exp_to_next": 250000,
        "base_hp": 200000,
        "base_attack": 20000,
        "base_defense": 10000,
        "base_lingqi": 40000,
        "breakthrough_rate": 0.0,
        "death_rate": 0.30,
        "sub_dao_yun_costs": [1440, 1520, 1600, 1680],
        "breakthrough_dao_yun_cost": 1680,
        "dao_yun_base_rate": 0.10,
        "dao_yun_per_sub_realm": 0.05,
    },
}


@dataclass
class ItemDef:
    """物品定义。"""
    item_id: str
    name: str
    item_type: str  # "consumable" | "material" | "equipment" | "heart_method" | "gongfa"
    description: str
    effect: dict = field(default_factory=dict)


# 初始物品注册表
ITEM_REGISTRY: dict[str, ItemDef] = {
    "healing_pill": ItemDef(
        item_id="healing_pill",
        name="回血丹",
        item_type="consumable",
        description="恢复50点生命值",
        effect={"heal_hp": 50},
    ),
    "exp_pill": ItemDef(
        item_id="exp_pill",
        name="聚灵丹",
        item_type="consumable",
        description="获得100点修炼经验",
        effect={"exp_bonus": 100},
    ),
    "spirit_stone": ItemDef(
        item_id="spirit_stone",
        name="灵石",
        item_type="material",
        description="修仙界通用货币",
    ),
    "breakthrough_pill": ItemDef(
        item_id="breakthrough_pill",
        name="破境丹",
        item_type="consumable",
        description="突破时额外增加20%成功率",
        effect={"breakthrough_bonus": 0.2},
    ),
    "body_tempering_pill": ItemDef(
        item_id="body_tempering_pill",
        name="淬体丹",
        item_type="consumable",
        description="永久增加10点攻击力",
        effect={"attack_boost": 10},
    ),
    "life_talisman": ItemDef(
        item_id="life_talisman",
        name="保命符",
        item_type="consumable",
        description="突破失败时免除死亡（元婴期以上）",
        effect={"prevent_death": True},
    ),
}

# ── 注册 200 种新丹药到 ITEM_REGISTRY ──
from .pills import PILL_REGISTRY, PILL_TIER_NAMES, PillDef, set_pill_registry as _set_runtime_pill_registry, get_pill_item_defs as _get_pill_item_defs  # noqa: E402
ITEM_REGISTRY.update(_get_pill_item_defs())


def _refresh_pill_items():
    """根据当前 PILL_REGISTRY 同步刷新丹药物品定义。"""
    new_items = _get_pill_item_defs()
    ITEM_REGISTRY.update(new_items)
    stale = [
        item_id for item_id in list(ITEM_REGISTRY.keys())
        if item_id.startswith("pill_") and item_id not in new_items
    ]
    for item_id in stale:
        ITEM_REGISTRY.pop(item_id, None)


def set_pill_registry(pills: dict[str, PillDef]):
    """替换丹药注册表（供数据库加载后同步到运行时）。"""
    with _registry_lock:
        _set_runtime_pill_registry(pills)
        _refresh_pill_items()

# 签到丹药权重表：(item_id, weight)
CHECKIN_PILL_WEIGHTS: list[tuple[str, int]] = [
    ("body_tempering_pill", 40),
    ("healing_pill", 30),
    ("exp_pill", 20),
    ("breakthrough_pill", 10),
]


# ──────────────────── 装备系统 ────────────────────


class EquipmentTier(IntEnum):
    """装备品阶。"""
    MORTAL = 0       # 凡器
    SPIRIT = 1       # 灵器
    DAO = 2          # 道器
    INNATE_DAO = 3   # 先天道器


EQUIPMENT_TIER_NAMES: dict[int, str] = {
    EquipmentTier.MORTAL: "凡器",
    EquipmentTier.SPIRIT: "灵器",
    EquipmentTier.DAO: "道器",
    EquipmentTier.INNATE_DAO: "先天道器",
}

# 装备品阶 → 可装备的境界范围 (min_realm, max_realm)
TIER_REALM_REQUIREMENTS: dict[int, tuple[int, int]] = {
    EquipmentTier.MORTAL: (RealmLevel.MORTAL, RealmLevel.DEITY_TRANSFORM),
    EquipmentTier.SPIRIT: (RealmLevel.QI_REFINING, RealmLevel.DEITY_TRANSFORM),
    EquipmentTier.DAO: (RealmLevel.VOID_MERGE, RealmLevel.MAHAYANA),
    EquipmentTier.INNATE_DAO: (RealmLevel.MAHAYANA, RealmLevel.MAHAYANA),
}


@dataclass
class EquipmentDef:
    """装备定义。"""
    equip_id: str
    name: str
    tier: int          # EquipmentTier
    slot: str          # "weapon" | "armor"
    attack: int = 0
    defense: int = 0
    element: str = "无"
    element_damage: int = 0
    description: str = ""


# ── 凡器 ──────────────────────────────────────────

_MORTAL_WEAPONS: list[EquipmentDef] = [
    EquipmentDef("mortal_iron_sword", "铁剑", EquipmentTier.MORTAL, "weapon",
                 attack=5, description="最基本的铁质长剑"),
    EquipmentDef("mortal_bronze_blade", "青铜刀", EquipmentTier.MORTAL, "weapon",
                 attack=8, description="铸造精良的青铜刀"),
    EquipmentDef("mortal_willow_bow", "柳木弓", EquipmentTier.MORTAL, "weapon",
                 attack=6, defense=2, description="柳木制成的轻巧长弓"),
    EquipmentDef("mortal_steel_spear", "精铁枪", EquipmentTier.MORTAL, "weapon",
                 attack=10, description="战场上常见的精铁长枪"),
    EquipmentDef("mortal_shield_blade", "玄铁盾刀", EquipmentTier.MORTAL, "weapon",
                 attack=7, defense=5, description="可攻可守的盾刀一体兵器"),
    EquipmentDef("mortal_cold_dagger", "寒铁匕首", EquipmentTier.MORTAL, "weapon",
                 attack=12, element="冰", element_damage=3,
                 description="寒铁打造，附带冰寒之气"),
    EquipmentDef("mortal_flame_staff", "烈焰棍", EquipmentTier.MORTAL, "weapon",
                 attack=9, element="火", element_damage=5,
                 description="内嵌火晶石的铁棍"),
    EquipmentDef("mortal_stone_hammer", "碎石锤", EquipmentTier.MORTAL, "weapon",
                 attack=11, element="土", element_damage=4,
                 description="沉重有力的石锤"),
]

_MORTAL_ARMORS: list[EquipmentDef] = [
    EquipmentDef("mortal_cloth", "布衣", EquipmentTier.MORTAL, "armor",
                 defense=3, description="普通棉布衣物"),
    EquipmentDef("mortal_leather", "皮甲", EquipmentTier.MORTAL, "armor",
                 defense=6, description="兽皮缝制的轻甲"),
    EquipmentDef("mortal_iron_armor", "铁甲", EquipmentTier.MORTAL, "armor",
                 defense=10, description="厚重的铁质甲胄"),
    EquipmentDef("mortal_bronze_scale", "铜鳞甲", EquipmentTier.MORTAL, "armor",
                 defense=8, attack=2, description="鳞片拼接的铜甲，兼具攻防"),
]

# ── 灵器 ──────────────────────────────────────────

_SPIRIT_WEAPONS: list[EquipmentDef] = [
    EquipmentDef("spirit_purple_sword", "紫电剑", EquipmentTier.SPIRIT, "weapon",
                 attack=50, element="雷", element_damage=15,
                 description="蕴含紫色雷电之力的灵剑"),
    EquipmentDef("spirit_jade_fire_blade", "碧火刀", EquipmentTier.SPIRIT, "weapon",
                 attack=55, element="火", element_damage=12,
                 description="碧色火焰缠绕的长刀"),
    EquipmentDef("spirit_ice_spear", "冰魄枪", EquipmentTier.SPIRIT, "weapon",
                 attack=45, element="冰", element_damage=18,
                 description="以千年冰魄为枪尖的灵枪"),
    EquipmentDef("spirit_wind_bow", "风灵弓", EquipmentTier.SPIRIT, "weapon",
                 attack=40, element="风", element_damage=20,
                 description="驾驭风之力的灵弓"),
    EquipmentDef("spirit_scarlet_sword", "赤焰剑", EquipmentTier.SPIRIT, "weapon",
                 attack=65, element="火", element_damage=10,
                 description="以赤焰淬炼百年的灵剑"),
    EquipmentDef("spirit_water_whip", "玄水鞭", EquipmentTier.SPIRIT, "weapon",
                 attack=35, element="水", element_damage=25,
                 description="以玄水精华凝聚的软鞭"),
]

_SPIRIT_ARMORS: list[EquipmentDef] = [
    EquipmentDef("spirit_rune_armor", "灵纹甲", EquipmentTier.SPIRIT, "armor",
                 defense=35, attack=5, description="刻有灵纹的护体甲"),
    EquipmentDef("spirit_fire_robe", "火鸦袍", EquipmentTier.SPIRIT, "armor",
                 defense=28, element="火", element_damage=10,
                 description="火鸦羽毛织成的法袍"),
    EquipmentDef("spirit_ice_silk", "冰蚕丝甲", EquipmentTier.SPIRIT, "armor",
                 defense=45, element="冰", element_damage=8,
                 description="冰蚕吐丝编织的轻甲"),
    EquipmentDef("spirit_thunder_robe", "雷盾法衣", EquipmentTier.SPIRIT, "armor",
                 defense=32, element="雷", element_damage=12,
                 description="可引雷护体的法衣"),
]

# ── 道器 ──────────────────────────────────────────

_DAO_WEAPONS: list[EquipmentDef] = [
    EquipmentDef("dao_heavenly_sword", "天罡剑", EquipmentTier.DAO, "weapon",
                 attack=800, defense=50, element="雷", element_damage=150,
                 description="蕴含天罡之气的仙剑"),
    EquipmentDef("dao_earth_blade", "地煞刀", EquipmentTier.DAO, "weapon",
                 attack=1000, element="土", element_damage=100,
                 description="汇聚地煞之力的重刀"),
    EquipmentDef("dao_abyss_halberd", "九幽戟", EquipmentTier.DAO, "weapon",
                 attack=1200, element="水", element_damage=80,
                 description="九幽深渊锻造的长戟"),
    EquipmentDef("dao_sky_spear", "焚天枪", EquipmentTier.DAO, "weapon",
                 attack=900, element="火", element_damage=200,
                 description="可焚天灭地的神枪"),
]

_DAO_ARMORS: list[EquipmentDef] = [
    EquipmentDef("dao_chaos_robe", "混元袍", EquipmentTier.DAO, "armor",
                 defense=600, attack=100, description="混元一气织就的道袍"),
    EquipmentDef("dao_vajra_armor", "金刚法体甲", EquipmentTier.DAO, "armor",
                 defense=800, element="土", element_damage=80,
                 description="金刚不坏之体的护甲"),
    EquipmentDef("dao_purple_sky", "紫霄仙衣", EquipmentTier.DAO, "armor",
                 defense=500, element="雷", element_damage=120,
                 description="紫霄雷罚凝聚的仙衣"),
]

# ── 先天道器（仅四件） ────────────────────────────

_INNATE_DAO: list[EquipmentDef] = [
    EquipmentDef("innate_hongmeng", "鸿蒙紫气", EquipmentTier.INNATE_DAO, "weapon",
                 attack=5000, defense=500,
                 description="天地开辟前的混沌紫气凝聚而成，万器之祖"),
    EquipmentDef("innate_nirvana_fire", "涅槃火", EquipmentTier.INNATE_DAO, "weapon",
                 attack=6000, element="火", element_damage=3000,
                 description="可焚天煮海的太古神火，浴火重生"),
    EquipmentDef("innate_primordial_cauldron", "原始鼎", EquipmentTier.INNATE_DAO, "armor",
                 defense=4000, attack=1000, element="土", element_damage=500,
                 description="镇压万界的原始之鼎，不可撼动"),
    EquipmentDef("innate_time_disc", "时间盘", EquipmentTier.INNATE_DAO, "armor",
                 defense=3500, attack=500, element="风", element_damage=2000,
                 description="掌控时间法则的神秘圆盘，逆转因果"),
]

# 装备注册表
EQUIPMENT_REGISTRY: dict[str, EquipmentDef] = {}
for _eq in (
    _MORTAL_WEAPONS + _MORTAL_ARMORS
    + _SPIRIT_WEAPONS + _SPIRIT_ARMORS
    + _DAO_WEAPONS + _DAO_ARMORS
    + _INNATE_DAO
):
    EQUIPMENT_REGISTRY[_eq.equip_id] = _eq


def _refresh_equipment_items():
    """根据当前 EQUIPMENT_REGISTRY 同步刷新装备物品定义。

    采用先增后删策略：先写入新条目，再删除过时条目，
    避免出现字典中装备条目全部缺失的中间状态。
    """
    # 构建新条目
    new_items = {}
    for eq in EQUIPMENT_REGISTRY.values():
        new_items[eq.equip_id] = ItemDef(
            item_id=eq.equip_id,
            name=eq.name,
            item_type="equipment",
            description=eq.description,
            effect={"equip_id": eq.equip_id},
        )
    # 先写入新条目（覆盖同名旧条目）
    ITEM_REGISTRY.update(new_items)
    # 再删除已不存在的旧装备条目
    stale = [
        item_id for item_id, item in ITEM_REGISTRY.items()
        if getattr(item, "item_type", "") == "equipment" and item_id not in new_items
    ]
    for item_id in stale:
        ITEM_REGISTRY.pop(item_id, None)


def set_equipment_registry(equipments: dict[str, EquipmentDef]):
    """替换装备注册表（供数据库加载后同步到运行时）。

    采用先增后删 + 写锁保护，避免读者看到空/半更新状态。
    在 asyncio 单线程中同步代码不会被协程打断，锁主要防御多线程场景。
    """
    new_data = dict(equipments)
    with _registry_lock:
        # 先写入所有新条目（覆盖同名旧条目）
        EQUIPMENT_REGISTRY.update(new_data)
        # 再删除已不存在的旧条目
        stale = [k for k in EQUIPMENT_REGISTRY if k not in new_data]
        for k in stale:
            del EQUIPMENT_REGISTRY[k]
        _refresh_equipment_items()


_refresh_equipment_items()


# ── 回收定价系统 ──────────────────────────────────────────────

RECYCLE_PRICE_CONSUMABLE: dict[str, int] = {
    "healing_pill": 8,
    "spirit_pill": 15,
    "breakthrough_pill": 50,
    "body_pill": 35,
    "life_talisman": 100,
}

NON_RECYCLABLE_ITEMS: set[str] = {"spirit_stone"}

_TIER_RECYCLE_CONFIG: dict[int, tuple[float, int, int]] = {
    EquipmentTier.MORTAL: (1.2, 8, 30),
    EquipmentTier.SPIRIT: (1.8, 80, 250),
    EquipmentTier.DAO: (1.0, 500, 2500),
    EquipmentTier.INNATE_DAO: (1.0, 5000, 15000),
}


def get_recycle_base_price(item_id: str) -> int | None:
    """获取物品的基础回收价格。"""
    if item_id in NON_RECYCLABLE_ITEMS:
        return None

    # 消耗品固定价格表
    if item_id in RECYCLE_PRICE_CONSUMABLE:
        return RECYCLE_PRICE_CONSUMABLE[item_id]

    # 装备按品阶公式
    eq = EQUIPMENT_REGISTRY.get(item_id)
    if eq:
        cfg = _TIER_RECYCLE_CONFIG.get(eq.tier)
        if cfg:
            multiplier, lo, hi = cfg
            return max(lo, min(hi, int((eq.attack + eq.defense) * multiplier)))
        return 5

    # 临时心法道具不可回收
    stored_hm_id = parse_stored_heart_method_item_id(item_id)
    if stored_hm_id:
        return None

    # 心法秘籍
    hm_id = parse_heart_method_manual_id(item_id)
    if hm_id:
        hm = HEART_METHOD_REGISTRY.get(hm_id)
        if hm:
            return int(20 * (1 + hm.realm * 0.8))
        return 20

    # 功法卷轴
    gf_id = parse_gongfa_scroll_id(item_id)
    if gf_id:
        gf = GONGFA_REGISTRY.get(gf_id)
        if gf:
            return gf.recycle_price
        return 1000

    # 注册表中存在的其他物品兜底
    if item_id in ITEM_REGISTRY:
        return 5

    return None


def get_daily_recycle_price(item_id: str, target_date: date | None = None) -> int | None:
    """获取每日浮动回收价格（±5%）。"""
    base = get_recycle_base_price(item_id)
    if base is None:
        return None
    d = target_date or date.today()
    seed_str = f"{item_id}_{d.isoformat()}"
    h = hashlib.md5(seed_str.encode()).hexdigest()
    ratio = int(h[:8], 16) / 0xFFFFFFFF  # 0~1
    fluctuation = 1 + (ratio * 0.1 - 0.05)  # 0.95~1.05
    return max(1, int(base * fluctuation))


def can_equip(realm: int, tier: int) -> bool:
    """检查指定境界能否装备指定品阶的装备。超出预设上限的境界视为满足最高品阶要求。"""
    req = TIER_REALM_REQUIREMENTS.get(tier)
    if not req:
        return False
    min_r, max_r = req
    # 境界超出预设范围时，只检查下限
    if realm > max(v[1] for v in TIER_REALM_REQUIREMENTS.values()):
        return realm >= min_r
    return min_r <= realm <= max_r


def get_equip_bonus(player_weapon: str, player_armor: str) -> dict:
    """计算装备总加成。

    说明：
    - 元素伤害（如「雷+15」）统一并入攻击力；
    - element_damages 仅保留为展示用途。
    """
    total_atk = 0
    total_def = 0
    element_damages: dict[str, int] = {}
    element_attack = 0
    for eid in (player_weapon, player_armor):
        eq = EQUIPMENT_REGISTRY.get(eid)
        if not eq:
            continue
        total_atk += eq.attack
        total_def += eq.defense
        if eq.element != "无" and eq.element_damage > 0:
            element_attack += eq.element_damage
            element_damages[eq.element] = element_damages.get(eq.element, 0) + eq.element_damage
    total_atk += element_attack
    return {
        "attack": total_atk,
        "defense": total_def,
        "element_attack": element_attack,
        "element_damages": element_damages,
    }


# ──────────────────── 心法系统 ────────────────────


class HeartMethodQuality(IntEnum):
    """心法品质。"""
    NORMAL = 0     # 普通
    EPIC = 1       # 史诗
    LEGENDARY = 2  # 传说


HEART_METHOD_QUALITY_NAMES: dict[int, str] = {
    HeartMethodQuality.NORMAL: "普通",
    HeartMethodQuality.EPIC: "史诗",
    HeartMethodQuality.LEGENDARY: "传说",
}

# 心法修炼阶段
MASTERY_LEVELS = ["入门", "小成", "大成", "圆满"]
MASTERY_MAX = len(MASTERY_LEVELS) - 1  # 3 = 圆满


@dataclass
class HeartMethodDef:
    """心法定义。"""
    method_id: str
    name: str
    realm: int            # 对应的大境界 (RealmLevel)
    quality: int          # HeartMethodQuality
    exp_multiplier: float  # 修炼经验倍率加成（如 0.1 = +10%）
    attack_bonus: int      # 攻击加成
    defense_bonus: int     # 防御加成
    dao_yun_rate: float    # 道韵获取速率（每次修炼额外获得道韵的概率）
    description: str = ""
    mastery_exp: int = 100  # 每阶段需要的心法修炼经验


# 心法修炼阶段加成倍率（入门=1.0，小成=1.5，大成=2.0，圆满=3.0）
MASTERY_MULTIPLIERS = [1.0, 1.5, 2.0, 3.0]


def _hm(method_id: str, name: str, realm: int, quality: int,
         exp_mult: float, atk: int, dfn: int, dao_rate: float,
         desc: str = "", mastery_exp: int = 100) -> HeartMethodDef:
    """心法定义快捷构造。"""
    return HeartMethodDef(
        method_id=method_id, name=name, realm=realm, quality=quality,
        exp_multiplier=exp_mult, attack_bonus=atk, defense_bonus=dfn,
        dao_yun_rate=dao_rate, description=desc, mastery_exp=mastery_exp,
    )


# ── 凡人心法（realm=0）─────────────────────────────
_HM_MORTAL = [
    _hm("hm_mortal_01", "吐纳术", 0, 0, 0.05, 1, 1, 0.02, "最基础的呼吸吐纳之法", 50),
    _hm("hm_mortal_02", "养气诀", 0, 0, 0.06, 2, 1, 0.02, "调养气息的入门功法", 50),
    _hm("hm_mortal_03", "壮体功", 0, 0, 0.04, 1, 2, 0.03, "强身健体的基础功法", 50),
    _hm("hm_mortal_04", "静心咒", 0, 0, 0.07, 1, 1, 0.04, "安定心神的冥想之术", 50),
    _hm("hm_mortal_05", "采气法", 0, 0, 0.08, 2, 0, 0.02, "采集天地灵气的方法", 50),
    _hm("hm_mortal_06", "五行基础功", 0, 0, 0.05, 2, 2, 0.03, "修习五行之力的根基功法", 55),
    _hm("hm_mortal_07", "铁骨功", 0, 0, 0.04, 1, 3, 0.02, "锤炼筋骨的刚猛功法", 55),
    _hm("hm_mortal_08", "混元桩", 0, 1, 0.10, 3, 2, 0.05, "混元一气的站桩心法", 80),
    _hm("hm_mortal_09", "先天功", 0, 1, 0.12, 2, 3, 0.06, "返璞归真的先天心法", 80),
    _hm("hm_mortal_10", "太初引气决", 0, 2, 0.18, 4, 3, 0.10, "传说中太初年间流传的引气秘法", 120),
]

# ── 练气期心法（realm=1）─────────────────────────────
_HM_QI_REFINING = [
    _hm("hm_qi_01", "清风诀", 1, 0, 0.06, 4, 2, 0.03, "如清风般轻灵的练气心法", 100),
    _hm("hm_qi_02", "紫阳功", 1, 0, 0.07, 5, 3, 0.03, "吸收阳气精华的功法", 100),
    _hm("hm_qi_03", "冰心诀", 1, 0, 0.05, 3, 5, 0.04, "以冰寒之力凝练真气", 100),
    _hm("hm_qi_04", "灵泉心法", 1, 0, 0.08, 4, 3, 0.03, "引灵泉之力入体修炼", 100),
    _hm("hm_qi_05", "青木功", 1, 0, 0.06, 5, 4, 0.03, "木属性基础修炼功法", 100),
    _hm("hm_qi_06", "烈阳真诀", 1, 0, 0.07, 6, 2, 0.04, "烈阳之力锻体炼气", 110),
    _hm("hm_qi_07", "玄冥功", 1, 0, 0.06, 3, 6, 0.04, "阴寒玄冥之力护体", 110),
    _hm("hm_qi_08", "天罡气", 1, 1, 0.12, 7, 5, 0.06, "修炼天罡正气的上乘心法", 160),
    _hm("hm_qi_09", "九转玄功", 1, 1, 0.14, 6, 6, 0.07, "九转归一的精妙心法", 160),
    _hm("hm_qi_10", "太乙真经", 1, 2, 0.20, 10, 8, 0.12, "太乙门传承的至高心法", 240),
]

# ── 筑基期心法（realm=2）─────────────────────────────
_HM_FOUNDATION = [
    _hm("hm_found_01", "聚灵诀", 2, 0, 0.06, 10, 5, 0.03, "聚拢灵气筑基的常用心法", 200),
    _hm("hm_found_02", "地煞功", 2, 0, 0.07, 12, 8, 0.03, "引地煞之力夯实根基", 200),
    _hm("hm_found_03", "碧落心经", 2, 0, 0.05, 8, 12, 0.04, "碧落仙人传下的心经", 200),
    _hm("hm_found_04", "凝元决", 2, 0, 0.08, 10, 8, 0.04, "凝聚元力巩固根基", 200),
    _hm("hm_found_05", "八荒炼体术", 2, 0, 0.06, 14, 6, 0.03, "八荒之力炼体铸基", 210),
    _hm("hm_found_06", "玉清功", 2, 0, 0.07, 8, 10, 0.04, "玉清道人传授的功法", 210),
    _hm("hm_found_07", "万象归元功", 2, 0, 0.07, 11, 9, 0.04, "万象归一巩固道基", 220),
    _hm("hm_found_08", "乾坤大法", 2, 1, 0.13, 16, 12, 0.07, "乾坤二气交融的大法", 320),
    _hm("hm_found_09", "紫微星辰诀", 2, 1, 0.15, 14, 14, 0.08, "紫微星辰之力筑基", 320),
    _hm("hm_found_10", "混沌筑基经", 2, 2, 0.22, 20, 18, 0.14, "以混沌之力铸就无上道基", 480),
]

# ── 金丹期心法（realm=3）─────────────────────────────
_HM_GOLDEN_CORE = [
    _hm("hm_gold_01", "金丹九转功", 3, 0, 0.06, 25, 15, 0.03, "凝练金丹的基础功法", 400),
    _hm("hm_gold_02", "太清炼丹术", 3, 0, 0.07, 30, 18, 0.04, "太清道人的炼丹心法", 400),
    _hm("hm_gold_03", "玄天宝鉴", 3, 0, 0.06, 20, 28, 0.04, "玄天宗的镇派心法", 400),
    _hm("hm_gold_04", "龙虎丹经", 3, 0, 0.08, 28, 20, 0.03, "龙虎交汇铸就金丹", 410),
    _hm("hm_gold_05", "五雷正法", 3, 0, 0.06, 32, 16, 0.04, "以雷法淬炼金丹", 410),
    _hm("hm_gold_06", "天心诀", 3, 0, 0.07, 22, 25, 0.04, "天心宗的护道心法", 420),
    _hm("hm_gold_07", "三花聚顶功", 3, 0, 0.07, 26, 22, 0.05, "三花聚顶凝练丹力", 420),
    _hm("hm_gold_08", "大日如来功", 3, 1, 0.14, 38, 30, 0.08, "大日如来传承的佛门心法", 640),
    _hm("hm_gold_09", "先天五行诀", 3, 1, 0.16, 35, 32, 0.09, "五行合一的先天心法", 640),
    _hm("hm_gold_10", "造化金丹经", 3, 2, 0.24, 50, 40, 0.16, "传说中造化仙人的金丹秘法", 960),
]

# ── 元婴期心法（realm=4）─────────────────────────────
_HM_NASCENT_SOUL = [
    _hm("hm_soul_01", "元婴化形诀", 4, 0, 0.06, 60, 35, 0.04, "元婴化形的基础修炼法", 800),
    _hm("hm_soul_02", "太阴炼魂功", 4, 0, 0.07, 70, 40, 0.04, "太阴之力淬炼元神", 800),
    _hm("hm_soul_03", "护神大法", 4, 0, 0.05, 50, 65, 0.05, "守护元神的上乘功法", 800),
    _hm("hm_soul_04", "星辰炼魂诀", 4, 0, 0.08, 65, 45, 0.04, "星辰之力炼化元神", 810),
    _hm("hm_soul_05", "天魂归元功", 4, 0, 0.07, 72, 38, 0.05, "天魂归元，凝聚真灵", 810),
    _hm("hm_soul_06", "幽冥鬼仙诀", 4, 0, 0.06, 55, 60, 0.05, "幽冥道的鬼仙修炼法", 820),
    _hm("hm_soul_07", "万灵归宗功", 4, 0, 0.07, 62, 52, 0.05, "万灵归宗的宗门心法", 820),
    _hm("hm_soul_08", "天元造化功", 4, 1, 0.15, 90, 70, 0.09, "天元宗的至高造化功法", 1280),
    _hm("hm_soul_09", "大衍神功", 4, 1, 0.17, 85, 75, 0.10, "推演天机的大衍心法", 1280),
    _hm("hm_soul_10", "太古元神经", 4, 2, 0.26, 120, 95, 0.18, "太古传承的元神至高秘法", 1920),
]

# ── 化神期心法（realm=5）─────────────────────────────
_HM_DEITY_TRANSFORM = [
    _hm("hm_deity_01", "化神大法", 5, 0, 0.06, 150, 80, 0.04, "化神期基础功法", 1600),
    _hm("hm_deity_02", "天人合一诀", 5, 0, 0.07, 170, 95, 0.05, "天人合一的化神心法", 1600),
    _hm("hm_deity_03", "六道轮回功", 5, 0, 0.06, 130, 150, 0.05, "六道轮回参悟生死", 1600),
    _hm("hm_deity_04", "万法归一", 5, 0, 0.08, 165, 100, 0.05, "万法归一，直指大道", 1620),
    _hm("hm_deity_05", "天火焚神诀", 5, 0, 0.07, 180, 85, 0.04, "天火淬炼神魂", 1620),
    _hm("hm_deity_06", "太虚化形功", 5, 0, 0.06, 140, 135, 0.05, "太虚之力化形神通", 1640),
    _hm("hm_deity_07", "九天玄功", 5, 0, 0.07, 155, 120, 0.05, "九重天传承的玄奥功法", 1640),
    _hm("hm_deity_08", "大罗神功", 5, 1, 0.16, 220, 170, 0.10, "大罗仙人传下的神功", 2560),
    _hm("hm_deity_09", "无极化神经", 5, 1, 0.18, 200, 180, 0.11, "无极大道化神秘经", 2560),
    _hm("hm_deity_10", "洪荒神诀", 5, 2, 0.28, 300, 240, 0.20, "洪荒时代神族的至高心法", 3840),
]

# ── 合虚期心法（realm=6）─────────────────────────────
_HM_VOID_MERGE = [
    _hm("hm_void_01", "合虚大法", 6, 0, 0.06, 380, 200, 0.05, "合虚期基础功法", 3200),
    _hm("hm_void_02", "虚空破碎诀", 6, 0, 0.07, 420, 230, 0.05, "虚空破碎的霸道心法", 3200),
    _hm("hm_void_03", "太虚归一功", 6, 0, 0.06, 350, 380, 0.06, "太虚归一护道心法", 3200),
    _hm("hm_void_04", "天道感悟诀", 6, 0, 0.08, 400, 260, 0.06, "感悟天道的修行法门", 3250),
    _hm("hm_void_05", "星河碎空功", 6, 0, 0.07, 440, 210, 0.05, "星河之力碎裂虚空", 3250),
    _hm("hm_void_06", "混沌虚无诀", 6, 0, 0.06, 360, 350, 0.06, "混沌虚无的玄奥修行", 3300),
    _hm("hm_void_07", "万道归虚功", 6, 0, 0.07, 390, 310, 0.06, "万道归虚的宗门秘法", 3300),
    _hm("hm_void_08", "太上虚无经", 6, 1, 0.17, 550, 420, 0.11, "太上老君的虚无真经", 5120),
    _hm("hm_void_09", "鸿蒙合虚功", 6, 1, 0.19, 520, 450, 0.12, "鸿蒙之力合虚归真", 5120),
    _hm("hm_void_10", "天地造化经", 6, 2, 0.30, 750, 600, 0.22, "天地造化的至高秘典", 7680),
]

# ── 渡劫期心法（realm=7）─────────────────────────────
_HM_TRIBULATION = [
    _hm("hm_trib_01", "渡劫心法", 7, 0, 0.06, 1000, 550, 0.05, "渡劫期基础功法", 6400),
    _hm("hm_trib_02", "九天雷劫功", 7, 0, 0.07, 1100, 600, 0.06, "以雷劫淬炼己身", 6400),
    _hm("hm_trib_03", "天劫护体诀", 7, 0, 0.06, 900, 1000, 0.06, "抵御天劫的防护心法", 6400),
    _hm("hm_trib_04", "灭世雷诀", 7, 0, 0.08, 1200, 500, 0.05, "灭世雷劫的霸道功法", 6500),
    _hm("hm_trib_05", "逆天改命功", 7, 0, 0.07, 1050, 650, 0.06, "逆天改命的修行法门", 6500),
    _hm("hm_trib_06", "天罚神功", 7, 0, 0.07, 950, 900, 0.06, "天罚之力锤炼道心", 6600),
    _hm("hm_trib_07", "万劫不灭功", 7, 0, 0.07, 1000, 800, 0.06, "万劫不灭的护道功法", 6600),
    _hm("hm_trib_08", "至尊雷帝诀", 7, 1, 0.18, 1500, 1100, 0.12, "雷帝传承的至尊功法", 10240),
    _hm("hm_trib_09", "天道劫经", 7, 1, 0.20, 1400, 1200, 0.13, "感悟天道劫数的秘经", 10240),
    _hm("hm_trib_10", "鸿蒙雷帝经", 7, 2, 0.32, 2000, 1600, 0.24, "鸿蒙时代雷帝的最强心法", 15360),
]

# ── 大乘期心法（realm=8）─────────────────────────────
_HM_MAHAYANA = [
    _hm("hm_maha_01", "大乘心经", 8, 0, 0.06, 2500, 1400, 0.06, "大乘期基础功法", 12800),
    _hm("hm_maha_02", "天尊大法", 8, 0, 0.07, 2800, 1600, 0.06, "天尊传承的大乘功法", 12800),
    _hm("hm_maha_03", "万物归元经", 8, 0, 0.06, 2200, 2500, 0.07, "万物归元护道法门", 12800),
    _hm("hm_maha_04", "太极阴阳诀", 8, 0, 0.08, 2600, 1800, 0.06, "阴阳合一大乘心法", 13000),
    _hm("hm_maha_05", "诸天万界功", 8, 0, 0.07, 3000, 1500, 0.06, "参悟诸天万界之力", 13000),
    _hm("hm_maha_06", "无上道经", 8, 0, 0.07, 2400, 2200, 0.07, "无上大道的心法真经", 13200),
    _hm("hm_maha_07", "亘古长生诀", 8, 0, 0.07, 2700, 2000, 0.07, "亘古长生的修行法门", 13200),
    _hm("hm_maha_08", "混沌天尊功", 8, 1, 0.19, 3800, 2800, 0.13, "混沌天尊的传世神功", 20480),
    _hm("hm_maha_09", "太上大道经", 8, 1, 0.22, 3500, 3000, 0.14, "太上大道的最终经典", 20480),
    _hm("hm_maha_10", "鸿蒙大道经", 8, 2, 0.35, 5000, 4000, 0.28, "鸿蒙大道的终极心法，世间罕有", 30720),
]

# 心法注册表
HEART_METHOD_REGISTRY: dict[str, HeartMethodDef] = {}
for _hm_list in (
    _HM_MORTAL, _HM_QI_REFINING, _HM_FOUNDATION, _HM_GOLDEN_CORE,
    _HM_NASCENT_SOUL, _HM_DEITY_TRANSFORM, _HM_VOID_MERGE,
    _HM_TRIBULATION, _HM_MAHAYANA,
):
    for _h in _hm_list:
        HEART_METHOD_REGISTRY[_h.method_id] = _h


HEART_METHOD_MANUAL_PREFIX = "heart_manual_"
STORED_HEART_METHOD_PREFIX = "stored_heart_manual_"


def get_heart_method_manual_id(method_id: str) -> str:
    """将心法ID转换为秘籍物品ID。"""
    return f"{HEART_METHOD_MANUAL_PREFIX}{method_id}"


def get_stored_heart_method_item_id(method_id: str) -> str:
    """将临时保留心法ID转换为道具物品ID。"""
    return f"{STORED_HEART_METHOD_PREFIX}{method_id}"


def parse_heart_method_manual_id(item_id: str) -> str | None:
    """从秘籍物品ID解析心法ID。"""
    if not item_id.startswith(HEART_METHOD_MANUAL_PREFIX):
        return None
    return item_id[len(HEART_METHOD_MANUAL_PREFIX):] or None


def parse_stored_heart_method_item_id(item_id: str) -> str | None:
    """从临时心法道具ID解析心法ID。"""
    if not item_id.startswith(STORED_HEART_METHOD_PREFIX):
        return None
    return item_id[len(STORED_HEART_METHOD_PREFIX):] or None


def _refresh_heart_method_manual_items():
    """根据当前 HEART_METHOD_REGISTRY 重新生成心法秘籍定义。

    采用先增后删策略，避免出现秘籍条目全部缺失的中间状态。
    """
    new_items = {}
    for hm in HEART_METHOD_REGISTRY.values():
        manual_id = get_heart_method_manual_id(hm.method_id)
        stored_manual_id = get_stored_heart_method_item_id(hm.method_id)
        realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界")
        quality_name = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通")
        new_items[manual_id] = ItemDef(
            item_id=manual_id,
            name=f"{hm.name}秘籍",
            item_type="heart_method",
            description=f"可领悟{quality_name}心法【{hm.name}】（{realm_name}）",
            effect={"learn_heart_method": hm.method_id},
        )
        new_items[stored_manual_id] = ItemDef(
            item_id=stored_manual_id,
            name=f"{hm.name}秘籍（临时）",
            item_type="heart_method",
            description=f"保留的{quality_name}心法【{hm.name}】（{realm_name}），三日内有效，不可回收",
            effect={"learn_heart_method": hm.method_id},
        )
    # 先写入新条目
    ITEM_REGISTRY.update(new_items)
    # 再删除已不存在的旧心法秘籍条目
    stale = [
        item_id for item_id in ITEM_REGISTRY
        if (item_id.startswith(HEART_METHOD_MANUAL_PREFIX) or item_id.startswith(STORED_HEART_METHOD_PREFIX))
        and item_id not in new_items
    ]
    for item_id in stale:
        ITEM_REGISTRY.pop(item_id, None)


def set_heart_method_registry(methods: dict[str, HeartMethodDef]):
    """替换心法注册表（供数据库加载后同步到运行时）。

    采用先增后删 + 写锁保护，避免读者看到空/半更新状态。
    """
    new_data = dict(methods)
    with _registry_lock:
        HEART_METHOD_REGISTRY.update(new_data)
        stale = [k for k in HEART_METHOD_REGISTRY if k not in new_data]
        for k in stale:
            del HEART_METHOD_REGISTRY[k]
        _refresh_heart_method_manual_items()


def get_heart_method_bonus(method_id: str, mastery: int) -> dict:
    """计算心法加成（含修炼阶段倍率）。

    Returns:
        {exp_multiplier, attack_bonus, defense_bonus, dao_yun_rate, mastery_name}
    """
    hm = HEART_METHOD_REGISTRY.get(method_id)
    if not hm:
        return {
            "exp_multiplier": 0.0, "attack_bonus": 0,
            "defense_bonus": 0, "dao_yun_rate": 0.0,
            "mastery_name": "",
        }
    mult = MASTERY_MULTIPLIERS[min(mastery, MASTERY_MAX)]
    return {
        "exp_multiplier": hm.exp_multiplier * mult,
        "attack_bonus": int(hm.attack_bonus * mult),
        "defense_bonus": int(hm.defense_bonus * mult),
        "dao_yun_rate": hm.dao_yun_rate * mult,
        "mastery_name": MASTERY_LEVELS[min(mastery, MASTERY_MAX)],
    }


def get_realm_heart_methods(realm: int) -> list[HeartMethodDef]:
    """获取指定境界的所有心法。"""
    return [hm for hm in HEART_METHOD_REGISTRY.values() if hm.realm == realm]


_refresh_heart_method_manual_items()


# ──────────────────── 功法系统 ────────────────────


class GongfaTier(IntEnum):
    """功法品阶。"""
    HUANG = 0   # 黄阶
    XUAN = 1    # 玄阶
    DI = 2      # 地阶
    TIAN = 3    # 天阶


GONGFA_TIER_NAMES: dict[int, str] = {
    GongfaTier.HUANG: "黄阶",
    GongfaTier.XUAN: "玄阶",
    GongfaTier.DI: "地阶",
    GongfaTier.TIAN: "天阶",
}

# 修炼熟练度所需最低境界（装备无限制）
GONGFA_TIER_REALM_REQ: dict[int, int] = {
    GongfaTier.HUANG: 0,   # 凡人即可修炼
    GongfaTier.XUAN: 1,    # 练气期
    GongfaTier.DI: 3,      # 金丹期
    GongfaTier.TIAN: 6,    # 合虚期
}


@dataclass
class GongfaDef:
    """功法定义。"""
    gongfa_id: str
    name: str
    tier: int             # GongfaTier
    attack_bonus: int
    defense_bonus: int
    hp_regen: int
    lingqi_regen: int
    description: str = ""
    mastery_exp: int = 200
    dao_yun_cost: int = 0
    recycle_price: int = 1000
    lingqi_cost: int = 0  # 战斗中施展功法消耗的灵气


def calc_gongfa_lingqi_cost(tier: int, atk: int, dfn: int, hp_r: int, lq_r: int) -> int:
    """根据功法品阶和属性计算施法耗灵。"""
    tier_base = {0: 10, 1: 25, 2: 50, 3: 100}
    max_stat = max(int(atk), int(dfn), int(hp_r), int(lq_r))
    return int(tier_base.get(int(tier), 10) + max_stat * 0.5)


def _gf(gongfa_id: str, name: str, tier: int,
         atk: int, dfn: int, hp_r: int, lq_r: int,
         desc: str = "", mastery_exp: int = 200,
         dao_yun_cost: int = 0, recycle_price: int = 1000) -> GongfaDef:
    """功法定义快捷构造。lingqi_cost 根据品阶和属性自动计算。"""
    lingqi_cost = calc_gongfa_lingqi_cost(tier, atk, dfn, hp_r, lq_r)
    return GongfaDef(
        gongfa_id=gongfa_id, name=name, tier=tier,
        attack_bonus=atk, defense_bonus=dfn,
        hp_regen=hp_r, lingqi_regen=lq_r,
        description=desc, mastery_exp=mastery_exp,
        dao_yun_cost=dao_yun_cost, recycle_price=recycle_price,
        lingqi_cost=lingqi_cost,
    )


# 15 种属性组合模板：
# 单属性(4): atk, def, hp, lq
# 双属性(6): atk+def, atk+hp, atk+lq, def+hp, def+lq, hp+lq
# 三属性(4): atk+def+hp, atk+def+lq, atk+hp+lq, def+hp+lq
# 四属性(1): atk+def+hp+lq

# ── 黄阶功法（60 本）── 15 组合 × 4 变体 ──────────────────
_GF_HUANG: list[GongfaDef] = [
    # 单攻 ×4
    _gf("gf_h_a01", "烈阳拳", 0, 25, 0, 0, 0, "以烈阳之力催动拳法", 200, 0, 1000),
    _gf("gf_h_a02", "劈风掌", 0, 22, 0, 0, 0, "掌风如刃劈开空气", 200, 0, 1100),
    _gf("gf_h_a03", "碎石指", 0, 20, 0, 0, 0, "指力惊人可碎顽石", 200, 0, 1000),
    _gf("gf_h_a04", "奔雷腿", 0, 18, 0, 0, 0, "腿法迅捷如奔雷", 200, 0, 1200),
    # 单防 ×4
    _gf("gf_h_d01", "铁壁功", 0, 0, 25, 0, 0, "修炼铜皮铁骨之法", 200, 0, 1000),
    _gf("gf_h_d02", "金钟罩", 0, 0, 22, 0, 0, "体表凝聚金钟护体", 200, 0, 1100),
    _gf("gf_h_d03", "磐石诀", 0, 0, 20, 0, 0, "身坚如磐石不动", 200, 0, 1000),
    _gf("gf_h_d04", "龟甲术", 0, 0, 18, 0, 0, "效法玄龟防御之术", 200, 0, 1200),
    # 单血 ×4
    _gf("gf_h_h01", "长春功", 0, 0, 0, 25, 0, "温养身体恢复气血", 200, 0, 1000),
    _gf("gf_h_h02", "续命诀", 0, 0, 0, 22, 0, "续命回春之法", 200, 0, 1100),
    _gf("gf_h_h03", "活血散", 0, 0, 0, 20, 0, "活血化瘀调理内伤", 200, 0, 1000),
    _gf("gf_h_h04", "回春术", 0, 0, 0, 18, 0, "春风化雨修复肉身", 200, 0, 1200),
    # 单灵 ×4
    _gf("gf_h_l01", "聚灵功", 0, 0, 0, 0, 25, "汇聚灵气充盈经脉", 200, 0, 1000),
    _gf("gf_h_l02", "引气诀", 0, 0, 0, 0, 22, "引导灵气归于丹田", 200, 0, 1100),
    _gf("gf_h_l03", "蓄灵术", 0, 0, 0, 0, 20, "蓄积灵气以备不时", 200, 0, 1000),
    _gf("gf_h_l04", "养灵法", 0, 0, 0, 0, 18, "温养灵气缓缓恢复", 200, 0, 1200),
    # 攻防 ×4
    _gf("gf_h_ad01", "刚柔并济", 0, 18, 15, 0, 0, "刚猛与柔韧交替", 200, 0, 1300),
    _gf("gf_h_ad02", "攻守兼备", 0, 15, 18, 0, 0, "攻中带守守中有攻", 200, 0, 1300),
    _gf("gf_h_ad03", "虎鹤双形", 0, 20, 15, 0, 0, "虎形攻鹤形守", 200, 0, 1400),
    _gf("gf_h_ad04", "龙蛇功", 0, 16, 16, 0, 0, "龙蛇之力兼攻兼守", 200, 0, 1200),
    # 攻血 ×4
    _gf("gf_h_ah01", "战意凝血", 0, 18, 0, 15, 0, "战意愈强气血愈旺", 200, 0, 1300),
    _gf("gf_h_ah02", "血战之法", 0, 15, 0, 18, 0, "越战越勇气血翻涌", 200, 0, 1300),
    _gf("gf_h_ah03", "猛虎吞天", 0, 20, 0, 15, 0, "虎啸生风气血激荡", 200, 0, 1400),
    _gf("gf_h_ah04", "赤焰拳", 0, 16, 0, 16, 0, "赤焰燃身回复气血", 200, 0, 1200),
    # 攻灵 ×4
    _gf("gf_h_al01", "灵攻诀", 0, 18, 0, 0, 15, "以灵气催动攻击", 200, 0, 1300),
    _gf("gf_h_al02", "破灵拳", 0, 15, 0, 0, 18, "拳势中蕴含灵力", 200, 0, 1300),
    _gf("gf_h_al03", "灵刃术", 0, 20, 0, 0, 15, "凝聚灵气为刃出击", 200, 0, 1400),
    _gf("gf_h_al04", "御灵攻", 0, 16, 0, 0, 16, "驾御灵气进攻敌手", 200, 0, 1200),
    # 防血 ×4
    _gf("gf_h_dh01", "铁甲回春", 0, 0, 18, 15, 0, "铁甲护身回春养血", 200, 0, 1300),
    _gf("gf_h_dh02", "金身续命", 0, 0, 15, 18, 0, "金身不坏续命回春", 200, 0, 1300),
    _gf("gf_h_dh03", "玄武功", 0, 0, 20, 15, 0, "玄武镇守回复气血", 200, 0, 1400),
    _gf("gf_h_dh04", "坚甲养身", 0, 0, 16, 16, 0, "坚甲护体养身固本", 200, 0, 1200),
    # 防灵 ×4
    _gf("gf_h_dl01", "灵盾术", 0, 0, 18, 0, 15, "灵气凝盾抵御攻击", 200, 0, 1300),
    _gf("gf_h_dl02", "护灵功", 0, 0, 15, 0, 18, "护体之余滋养灵气", 200, 0, 1300),
    _gf("gf_h_dl03", "灵甲诀", 0, 0, 20, 0, 15, "灵甲凝聚防御极高", 200, 0, 1400),
    _gf("gf_h_dl04", "蓄灵防", 0, 0, 16, 0, 16, "蓄灵护体两不相误", 200, 0, 1200),
    # 血灵 ×4
    _gf("gf_h_hl01", "生生不息", 0, 0, 0, 18, 15, "气血灵力相生不息", 200, 0, 1300),
    _gf("gf_h_hl02", "灵血双修", 0, 0, 0, 15, 18, "灵血同修相辅相成", 200, 0, 1300),
    _gf("gf_h_hl03", "回灵养血", 0, 0, 0, 20, 15, "回灵养血双管齐下", 200, 0, 1400),
    _gf("gf_h_hl04", "灵血诀", 0, 0, 0, 16, 16, "灵气与气血相互转化", 200, 0, 1200),
    # 攻防血 ×4
    _gf("gf_h_adh01", "三才功", 0, 15, 15, 15, 0, "天地人三才合一", 200, 0, 1500),
    _gf("gf_h_adh02", "战场生存", 0, 18, 15, 15, 0, "战场上攻守兼顾", 200, 0, 1600),
    _gf("gf_h_adh03", "铁血战法", 0, 15, 18, 15, 0, "铁血交融战意高涨", 200, 0, 1600),
    _gf("gf_h_adh04", "龙虎回春", 0, 15, 15, 18, 0, "龙虎之力滋养气血", 200, 0, 1600),
    # 攻防灵 ×4
    _gf("gf_h_adl01", "灵战功", 0, 15, 15, 0, 15, "灵气强化攻防", 200, 0, 1500),
    _gf("gf_h_adl02", "灵武合一", 0, 18, 15, 0, 15, "灵力与武技合一", 200, 0, 1600),
    _gf("gf_h_adl03", "御灵护体", 0, 15, 18, 0, 15, "御灵之力强化防御", 200, 0, 1600),
    _gf("gf_h_adl04", "灵攻灵守", 0, 15, 15, 0, 18, "灵力攻守一体", 200, 0, 1600),
    # 攻血灵 ×4
    _gf("gf_h_ahl01", "血灵攻", 0, 15, 0, 15, 15, "气血灵力催动攻击", 200, 0, 1500),
    _gf("gf_h_ahl02", "战灵养血", 0, 18, 0, 15, 15, "以战养灵以灵养血", 200, 0, 1600),
    _gf("gf_h_ahl03", "烈焰涅槃", 0, 15, 0, 18, 15, "烈焰焚身涅槃重生", 200, 0, 1600),
    _gf("gf_h_ahl04", "攻灵双修", 0, 15, 0, 15, 18, "攻击与灵力同修", 200, 0, 1600),
    # 防血灵 ×4
    _gf("gf_h_dhl01", "万防之体", 0, 0, 15, 15, 15, "全面防御恢复之法", 200, 0, 1500),
    _gf("gf_h_dhl02", "铁壁回灵", 0, 0, 18, 15, 15, "铁壁防御灵力回转", 200, 0, 1600),
    _gf("gf_h_dhl03", "续命养灵", 0, 0, 15, 18, 15, "续命回春灵力充盈", 200, 0, 1600),
    _gf("gf_h_dhl04", "护体蓄灵", 0, 0, 15, 15, 18, "护体之余蓄积灵气", 200, 0, 1600),
    # 四属性 ×4
    _gf("gf_h_all01", "太极功", 0, 15, 15, 15, 15, "阴阳调和四象归一", 200, 0, 2000),
    _gf("gf_h_all02", "混元初功", 0, 18, 16, 16, 16, "混元之气初入门径", 200, 0, 2000),
    _gf("gf_h_all03", "五行基功", 0, 16, 18, 16, 16, "五行之力均衡修炼", 200, 0, 2000),
    _gf("gf_h_all04", "均衡之道", 0, 16, 16, 18, 16, "均衡发展面面俱到", 200, 0, 2000),
]

# ── 玄阶功法（45 本）── 15 组合 × 3 变体 ──────────────────
_GF_XUAN: list[GongfaDef] = [
    # 单攻 ×3
    _gf("gf_x_a01", "落日斩", 1, 70, 0, 0, 0, "日落西山斩杀敌手", 500, 0, 3000),
    _gf("gf_x_a02", "穿云掌", 1, 60, 0, 0, 0, "掌力穿云破雾", 500, 0, 3500),
    _gf("gf_x_a03", "惊雷指", 1, 55, 0, 0, 0, "指尖蕴含雷电之力", 500, 0, 4000),
    # 单防 ×3
    _gf("gf_x_d01", "玄铁甲", 1, 0, 70, 0, 0, "玄铁凝聚不可破", 500, 0, 3000),
    _gf("gf_x_d02", "天罡护体", 1, 0, 60, 0, 0, "天罡正气护体周全", 500, 0, 3500),
    _gf("gf_x_d03", "金身诀", 1, 0, 55, 0, 0, "金身不坏坚如磐石", 500, 0, 4000),
    # 单血 ×3
    _gf("gf_x_h01", "九转还阳", 1, 0, 0, 70, 0, "九转还阳恢复气血", 500, 0, 3000),
    _gf("gf_x_h02", "大还丹法", 1, 0, 0, 60, 0, "修炼大还丹之法", 500, 0, 3500),
    _gf("gf_x_h03", "造化回春", 1, 0, 0, 55, 0, "造化之力回春养血", 500, 0, 4000),
    # 单灵 ×3
    _gf("gf_x_l01", "天灵诀", 1, 0, 0, 0, 70, "天地灵气汇聚一身", 500, 0, 3000),
    _gf("gf_x_l02", "灵脉贯通", 1, 0, 0, 0, 60, "灵脉贯通灵力奔涌", 500, 0, 3500),
    _gf("gf_x_l03", "蓄灵大法", 1, 0, 0, 0, 55, "蓄积天地灵气之大法", 500, 0, 4000),
    # 攻防 ×3
    _gf("gf_x_ad01", "阴阳双剑", 1, 50, 40, 0, 0, "阴阳剑气攻守一体", 500, 0, 4500),
    _gf("gf_x_ad02", "天地双极", 1, 45, 45, 0, 0, "天地两极力量平衡", 500, 0, 4500),
    _gf("gf_x_ad03", "矛盾合一", 1, 55, 40, 0, 0, "矛与盾合而为一", 500, 0, 5000),
    # 攻血 ×3
    _gf("gf_x_ah01", "血战八方", 1, 50, 0, 40, 0, "越战气血越旺盛", 500, 0, 4500),
    _gf("gf_x_ah02", "不灭战魂", 1, 45, 0, 45, 0, "战魂不灭气血长存", 500, 0, 4500),
    _gf("gf_x_ah03", "浴血奋战", 1, 55, 0, 40, 0, "浴血之中愈战愈强", 500, 0, 5000),
    # 攻灵 ×3
    _gf("gf_x_al01", "灵刃破空", 1, 50, 0, 0, 40, "灵气凝刃破空而出", 500, 0, 4500),
    _gf("gf_x_al02", "御灵之击", 1, 45, 0, 0, 45, "灵力催动毁灭一击", 500, 0, 4500),
    _gf("gf_x_al03", "风雷灵攻", 1, 55, 0, 0, 40, "风雷灵力融合攻击", 500, 0, 5000),
    # 防血 ×3
    _gf("gf_x_dh01", "不动如山", 1, 0, 50, 40, 0, "稳如泰山气血充盈", 500, 0, 4500),
    _gf("gf_x_dh02", "铁血金刚", 1, 0, 45, 45, 0, "金刚护体铁血不屈", 500, 0, 4500),
    _gf("gf_x_dh03", "神龟吐纳", 1, 0, 55, 40, 0, "神龟吐纳养身固本", 500, 0, 5000),
    # 防灵 ×3
    _gf("gf_x_dl01", "灵盾天成", 1, 0, 50, 0, 40, "天成灵盾坚不可摧", 500, 0, 4500),
    _gf("gf_x_dl02", "护灵大法", 1, 0, 45, 0, 45, "大法护灵防御无双", 500, 0, 4500),
    _gf("gf_x_dl03", "灵壁术", 1, 0, 55, 0, 40, "灵力凝壁抵挡万法", 500, 0, 5000),
    # 血灵 ×3
    _gf("gf_x_hl01", "双修大法", 1, 0, 0, 50, 40, "气血灵力双修大法", 500, 0, 4500),
    _gf("gf_x_hl02", "灵血交融", 1, 0, 0, 45, 45, "灵力与气血互相滋养", 500, 0, 4500),
    _gf("gf_x_hl03", "凤凰涅槃", 1, 0, 0, 55, 40, "凤凰浴火重生之法", 500, 0, 5000),
    # 攻防血 ×3
    _gf("gf_x_adh01", "天人三合", 1, 40, 40, 40, 0, "天人合一三才归位", 500, 0, 5500),
    _gf("gf_x_adh02", "无畏战法", 1, 50, 40, 40, 0, "无畏勇士攻守兼顾", 500, 0, 5500),
    _gf("gf_x_adh03", "百战不殆", 1, 40, 50, 40, 0, "百战老兵从容不迫", 500, 0, 6000),
    # 攻防灵 ×3
    _gf("gf_x_adl01", "灵武双全", 1, 40, 40, 0, 40, "灵力与武技双修", 500, 0, 5500),
    _gf("gf_x_adl02", "灵攻灵守", 1, 50, 40, 0, 40, "灵力催动攻守之道", 500, 0, 5500),
    _gf("gf_x_adl03", "御灵之道", 1, 40, 50, 0, 40, "驾御灵力精通战法", 500, 0, 6000),
    # 攻血灵 ×3
    _gf("gf_x_ahl01", "战灵回血", 1, 40, 0, 40, 40, "战中灵气回血", 500, 0, 5500),
    _gf("gf_x_ahl02", "猛攻续灵", 1, 50, 0, 40, 40, "猛攻之余灵力续命", 500, 0, 5500),
    _gf("gf_x_ahl03", "灵血攻势", 1, 40, 0, 50, 40, "灵血涌动攻势如潮", 500, 0, 6000),
    # 防血灵 ×3
    _gf("gf_x_dhl01", "万全之策", 1, 0, 40, 40, 40, "防御回复面面俱到", 500, 0, 5500),
    _gf("gf_x_dhl02", "固若金汤", 1, 0, 50, 40, 40, "固若金汤守护周全", 500, 0, 5500),
    _gf("gf_x_dhl03", "灵龟之盾", 1, 0, 40, 50, 40, "灵龟护身气血灵力充盈", 500, 0, 6000),
    # 四属性 ×3
    _gf("gf_x_all01", "玄门正功", 1, 40, 40, 40, 40, "玄门正宗四象归位", 500, 0, 6000),
    _gf("gf_x_all02", "万象归宗", 1, 45, 45, 40, 40, "万象归宗均衡发展", 500, 0, 6000),
    _gf("gf_x_all03", "太清真功", 1, 40, 40, 45, 45, "太清道人所传真功", 500, 0, 6000),
]

# ── 地阶功法（30 本）── 15 组合 × 2 变体 ──────────────────
_GF_DI: list[GongfaDef] = [
    # 单攻 ×2
    _gf("gf_d_a01", "天崩地裂斩", 2, 180, 0, 0, 0, "一斩之下天崩地裂", 1500, 50, 10000),
    _gf("gf_d_a02", "灭世拳", 2, 160, 0, 0, 0, "拳势足以灭世", 1500, 60, 12000),
    # 单防 ×2
    _gf("gf_d_d01", "不灭金身", 2, 0, 180, 0, 0, "修成不灭金身", 1500, 50, 10000),
    _gf("gf_d_d02", "万劫护体", 2, 0, 160, 0, 0, "万劫之下护体不破", 1500, 60, 12000),
    # 单血 ×2
    _gf("gf_d_h01", "不死天功", 2, 0, 0, 180, 0, "不死之身天赐神功", 1500, 50, 10000),
    _gf("gf_d_h02", "造化回生", 2, 0, 0, 160, 0, "造化之力起死回生", 1500, 60, 12000),
    # 单灵 ×2
    _gf("gf_d_l01", "天地灵引", 2, 0, 0, 0, 180, "引天地灵气入体", 1500, 50, 10000),
    _gf("gf_d_l02", "灵脉至尊", 2, 0, 0, 0, 160, "灵脉之力汇于至尊", 1500, 60, 12000),
    # 攻防 ×2
    _gf("gf_d_ad01", "乾坤双极", 2, 130, 100, 0, 0, "乾坤两极力量交汇", 1500, 70, 14000),
    _gf("gf_d_ad02", "太极阴阳", 2, 110, 120, 0, 0, "阴阳交替攻守自如", 1500, 70, 14000),
    # 攻血 ×2
    _gf("gf_d_ah01", "血战苍穹", 2, 130, 0, 100, 0, "血战苍穹意志不屈", 1500, 70, 14000),
    _gf("gf_d_ah02", "战魂不灭", 2, 110, 0, 120, 0, "战魂不灭生生不息", 1500, 70, 14000),
    # 攻灵 ×2
    _gf("gf_d_al01", "灵破万法", 2, 130, 0, 0, 100, "灵力催动破万法", 1500, 80, 15000),
    _gf("gf_d_al02", "天灵攻势", 2, 110, 0, 0, 120, "天灵之力攻无不克", 1500, 80, 15000),
    # 防血 ×2
    _gf("gf_d_dh01", "金刚不坏", 2, 0, 130, 100, 0, "金刚之体坚不可摧", 1500, 70, 14000),
    _gf("gf_d_dh02", "万古长青", 2, 0, 110, 120, 0, "万古长青不朽之身", 1500, 70, 14000),
    # 防灵 ×2
    _gf("gf_d_dl01", "灵壁万钧", 2, 0, 130, 0, 100, "灵力凝壁重若万钧", 1500, 80, 15000),
    _gf("gf_d_dl02", "玄灵护法", 2, 0, 110, 0, 120, "玄灵护法抵挡万邪", 1500, 80, 15000),
    # 血灵 ×2
    _gf("gf_d_hl01", "灵血同源", 2, 0, 0, 130, 100, "灵血同源滋养肉身", 1500, 80, 15000),
    _gf("gf_d_hl02", "双修至尊", 2, 0, 0, 110, 120, "气血灵力双修至尊", 1500, 80, 15000),
    # 攻防血 ×2
    _gf("gf_d_adh01", "三才大阵", 2, 110, 100, 100, 0, "天地人三才大阵", 1500, 90, 17000),
    _gf("gf_d_adh02", "无极战法", 2, 120, 110, 100, 0, "无极之力战法通天", 1500, 90, 17000),
    # 攻防灵 ×2
    _gf("gf_d_adl01", "灵武至尊", 2, 110, 100, 0, 100, "灵武双修至尊之境", 1500, 90, 17000),
    _gf("gf_d_adl02", "御灵战神", 2, 120, 110, 0, 100, "御灵战神攻守无双", 1500, 90, 17000),
    # 攻血灵 ×2
    _gf("gf_d_ahl01", "天战回灵", 2, 110, 0, 100, 100, "天战之中灵力回转", 1500, 90, 18000),
    _gf("gf_d_ahl02", "破军灵血", 2, 120, 0, 110, 100, "破军之势灵血交融", 1500, 90, 18000),
    # 防血灵 ×2
    _gf("gf_d_dhl01", "万法归宗", 2, 0, 110, 100, 100, "万法归宗全面防御", 1500, 100, 18000),
    _gf("gf_d_dhl02", "天盾灵壁", 2, 0, 120, 110, 100, "天盾灵壁坚不可摧", 1500, 100, 18000),
    # 四属性 ×2
    _gf("gf_d_all01", "天地造化功", 2, 110, 100, 100, 100, "天地造化均衡之道", 1500, 100, 20000),
    _gf("gf_d_all02", "混沌真功", 2, 120, 110, 110, 100, "混沌之力真功大成", 1500, 100, 20000),
]

# ── 天阶功法（15 本）── 15 组合 × 1 变体 ──────────────────
_GF_TIAN: list[GongfaDef] = [
    _gf("gf_t_a01", "鸿蒙灭世", 3, 400, 0, 0, 0, "鸿蒙之力灭世无敌", 4000, 200, 30000),
    _gf("gf_t_d01", "太古神甲", 3, 0, 400, 0, 0, "太古神甲万法不侵", 4000, 200, 30000),
    _gf("gf_t_h01", "不死仙体", 3, 0, 0, 400, 0, "修成不死仙体长生不灭", 4000, 200, 30000),
    _gf("gf_t_l01", "万灵归宗", 3, 0, 0, 0, 400, "万灵之气归于一身", 4000, 200, 30000),
    _gf("gf_t_ad01", "乾坤大挪移", 3, 300, 250, 0, 0, "乾坤逆转攻守无敌", 4000, 250, 40000),
    _gf("gf_t_ah01", "血神战法", 3, 300, 0, 250, 0, "血神降世战力滔天", 4000, 250, 40000),
    _gf("gf_t_al01", "灵主天诀", 3, 300, 0, 0, 250, "灵力之主天诀无上", 4000, 300, 45000),
    _gf("gf_t_dh01", "万劫不灭体", 3, 0, 300, 250, 0, "万劫不灭铁血长存", 4000, 250, 40000),
    _gf("gf_t_dl01", "天灵护法", 3, 0, 300, 0, 250, "天灵护法抵挡万邪", 4000, 300, 45000),
    _gf("gf_t_hl01", "造化之源", 3, 0, 0, 300, 250, "造化之源气血灵力无穷", 4000, 300, 45000),
    _gf("gf_t_adh01", "太上三才", 3, 280, 260, 260, 0, "太上三才天地人合一", 4000, 350, 50000),
    _gf("gf_t_adl01", "天元灵战", 3, 280, 260, 0, 260, "天元之力灵战无双", 4000, 350, 50000),
    _gf("gf_t_ahl01", "灵血战神", 3, 280, 0, 260, 260, "灵血交融战神降临", 4000, 400, 55000),
    _gf("gf_t_dhl01", "万法归真", 3, 0, 280, 260, 260, "万法归真不灭之体", 4000, 400, 55000),
    _gf("gf_t_all01", "鸿蒙大道", 3, 280, 260, 260, 260, "鸿蒙大道万法归一", 4000, 500, 60000),
]

# 功法注册表
GONGFA_REGISTRY: dict[str, GongfaDef] = {}
for _gf_list in (_GF_HUANG, _GF_XUAN, _GF_DI, _GF_TIAN):
    for _g in _gf_list:
        GONGFA_REGISTRY[_g.gongfa_id] = _g


GONGFA_SCROLL_PREFIX = "gongfa_scroll_"


def get_gongfa_scroll_id(gongfa_id: str) -> str:
    """将功法ID转换为卷轴物品ID。"""
    return f"{GONGFA_SCROLL_PREFIX}{gongfa_id}"


def parse_gongfa_scroll_id(item_id: str) -> str | None:
    """从卷轴物品ID解析功法ID。"""
    if not item_id.startswith(GONGFA_SCROLL_PREFIX):
        return None
    return item_id[len(GONGFA_SCROLL_PREFIX):] or None


def _refresh_gongfa_scroll_items():
    """根据当前 GONGFA_REGISTRY 重新生成功法卷轴定义。

    采用先增后删策略，避免出现卷轴条目全部缺失的中间状态。
    """
    new_items = {}
    for gf in GONGFA_REGISTRY.values():
        scroll_id = get_gongfa_scroll_id(gf.gongfa_id)
        tier_name = GONGFA_TIER_NAMES.get(gf.tier, "未知")
        parts = []
        if gf.attack_bonus:
            parts.append(f"攻+{gf.attack_bonus}")
        if gf.defense_bonus:
            parts.append(f"防+{gf.defense_bonus}")
        if gf.hp_regen:
            parts.append(f"血+{gf.hp_regen}")
        if gf.lingqi_regen:
            parts.append(f"灵+{gf.lingqi_regen}")
        stat_str = "/".join(parts) if parts else "无加成"
        new_items[scroll_id] = ItemDef(
            item_id=scroll_id,
            name=f"{gf.name}卷轴",
            item_type="gongfa",
            description=f"{tier_name}功法【{gf.name}】卷轴（{stat_str}）",
            effect={"learn_gongfa": gf.gongfa_id},
        )
    # 先写入新条目
    ITEM_REGISTRY.update(new_items)
    # 再删除已不存在的旧功法卷轴条目
    stale = [
        item_id for item_id in ITEM_REGISTRY
        if item_id.startswith(GONGFA_SCROLL_PREFIX) and item_id not in new_items
    ]
    for item_id in stale:
        ITEM_REGISTRY.pop(item_id, None)


def set_gongfa_registry(gongfas: dict[str, GongfaDef]):
    """替换功法注册表（供数据库加载后同步到运行时）。

    采用先增后删 + 写锁保护，避免读者看到空/半更新状态。
    """
    new_data = dict(gongfas)
    with _registry_lock:
        GONGFA_REGISTRY.update(new_data)
        stale = [k for k in GONGFA_REGISTRY if k not in new_data]
        for k in stale:
            del GONGFA_REGISTRY[k]
        _refresh_gongfa_scroll_items()


def can_learn_gongfa() -> bool:
    """装备无限制，任何境界都能装备任何品阶的功法。"""
    return True


def can_cultivate_gongfa(realm: int, tier: int) -> bool:
    """检查境界是否满足修炼熟练度的要求。"""
    min_realm = GONGFA_TIER_REALM_REQ.get(tier, 0)
    return realm >= min_realm


def get_gongfa_bonus(gongfa_id: str, mastery: int, realm: int) -> dict:
    """计算功法加成（含境界缩放和精通倍率）。

    公式：effective = base * (1 + 0.1 * realm) * MASTERY_MULTIPLIERS[mastery]

    Returns:
        {attack_bonus, defense_bonus, hp_regen, lingqi_regen, mastery_name}
    """
    gf = GONGFA_REGISTRY.get(gongfa_id)
    if not gf:
        return {
            "attack_bonus": 0, "defense_bonus": 0,
            "hp_regen": 0, "lingqi_regen": 0,
            "mastery_name": "",
        }
    realm_scale = 1.0 + 0.1 * realm
    mastery_mult = MASTERY_MULTIPLIERS[min(mastery, MASTERY_MAX)]
    factor = realm_scale * mastery_mult
    return {
        "attack_bonus": int(gf.attack_bonus * factor),
        "defense_bonus": int(gf.defense_bonus * factor),
        "hp_regen": int(gf.hp_regen * factor),
        "lingqi_regen": int(gf.lingqi_regen * factor),
        "mastery_name": MASTERY_LEVELS[min(mastery, MASTERY_MAX)],
    }


def get_total_gongfa_bonus(player) -> dict:
    """汇总 3 个槽位的功法效果。"""
    total = {"attack_bonus": 0, "defense_bonus": 0, "hp_regen": 0, "lingqi_regen": 0}
    for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
        gongfa_id = getattr(player, slot, "无")
        if not gongfa_id or gongfa_id == "无":
            continue
        mastery = getattr(player, f"{slot}_mastery", 0)
        bonus = get_gongfa_bonus(gongfa_id, mastery, player.realm)
        total["attack_bonus"] += bonus["attack_bonus"]
        total["defense_bonus"] += bonus["defense_bonus"]
        total["hp_regen"] += bonus["hp_regen"]
        total["lingqi_regen"] += bonus["lingqi_regen"]
    return total


_refresh_gongfa_scroll_items()


# ═══════════════════════════════════════════════════════════════════
#   材料系统
# ═══════════════════════════════════════════════════════════════════


class MaterialRarity(IntEnum):
    """材料稀有度。"""
    COMMON = 0     # 普通
    RARE = 1       # 稀有
    PRECIOUS = 2   # 珍稀
    EPIC = 3       # 史诗
    LEGENDARY = 4  # 传说
    MYTHIC = 5     # 神话


MATERIAL_RARITY_NAMES: dict[int, str] = {
    MaterialRarity.COMMON: "普通",
    MaterialRarity.RARE: "稀有",
    MaterialRarity.PRECIOUS: "珍稀",
    MaterialRarity.EPIC: "史诗",
    MaterialRarity.LEGENDARY: "传说",
    MaterialRarity.MYTHIC: "神话",
}

MATERIAL_CATEGORIES: dict[str, str] = {
    "herb": "草药类",
    "mineral": "矿石类",
    "beast": "妖兽材料",
    "spirit_fluid": "灵液类",
    "special": "特殊材料",
}


@dataclass
class MaterialDef:
    """材料定义。"""
    item_id: str
    name: str
    rarity: int           # MaterialRarity
    category: str          # MATERIAL_CATEGORIES key
    source: str = ""       # 来源描述
    description: str = ""
    recycle_price: int = 0  # 灵石回收价


MATERIAL_REGISTRY: dict[str, MaterialDef] = {}


def set_material_registry(materials: dict[str, MaterialDef]):
    """替换材料注册表（供数据库加载后同步到运行时）。"""
    new_data = {k: MaterialDef(**v) if not isinstance(v, MaterialDef) else v
                 for k, v in materials.items()}
    with _registry_lock:
        stale = [k for k in MATERIAL_REGISTRY if k not in new_data]
        for k in stale:
            del MATERIAL_REGISTRY[k]
        MATERIAL_REGISTRY.update(new_data)
        # 同步到 ITEM_REGISTRY（type="material"）
        new_items = {}
        for mat in MATERIAL_REGISTRY.values():
            new_items[mat.item_id] = ItemDef(
                item_id=mat.item_id,
                name=mat.name,
                item_type="material",
                description=mat.description,
                effect={"material_id": mat.item_id},
            )
        stale_items = [
            i for i, it in ITEM_REGISTRY.items()
            if getattr(it, "item_type", "") == "material" and i not in new_items
        ]
        for i in stale_items:
            ITEM_REGISTRY.pop(i, None)
        ITEM_REGISTRY.update(new_items)


# ═══════════════════════════════════════════════════════════════════
#   丹方 / 炼丹配方系统
# ═══════════════════════════════════════════════════════════════════


class PillGrade(IntEnum):
    """丹药品级（对应炼丹产出的品质）。"""
    LOW = 0    # 下品
    HIGH = 1   # 上品
    PURE = 2   # 无垢


PILL_GRADE_NAMES: dict[int, str] = {
    PillGrade.LOW: "下品",
    PillGrade.HIGH: "上品",
    PillGrade.PURE: "无垢",
}


@dataclass
class PillRecipeMaterial:
    """配方中的单一材料需求。"""
    item_id: str
    qty: int


@dataclass
class PillRecipeDef:
    """丹方定义。"""
    recipe_id: str
    pill_id: str          # 关联 PILL_REGISTRY
    grade: int            # PillGrade
    main_material: PillRecipeMaterial
    auxiliary_material: PillRecipeMaterial
    catalyst: PillRecipeMaterial
    forming_material: PillRecipeMaterial


PILL_RECIPE_REGISTRY: dict[str, PillRecipeDef] = {}


def set_pill_recipe_registry(recipes: dict[str, PillRecipeDef]):
    """替换丹方注册表（供数据库加载后同步到运行时）。"""
    def _make(v):
        if isinstance(v, PillRecipeDef):
            return v
        def _mat(m):
            if isinstance(m, PillRecipeMaterial):
                return m
            return PillRecipeMaterial(item_id=m.get("item_id", ""), qty=m.get("qty", 1))
        return PillRecipeDef(
            recipe_id=v["recipe_id"],
            pill_id=v["pill_id"],
            grade=int(v.get("grade", 0)),
            main_material=_mat(v.get("main_material", {})),
            auxiliary_material=_mat(v.get("auxiliary_material", {})),
            catalyst=_mat(v.get("catalyst", {})),
            forming_material=_mat(v.get("forming_material", {})),
        )
    new_data = {k: _make(v) for k, v in recipes.items()}
    with _registry_lock:
        stale = [k for k in PILL_RECIPE_REGISTRY if k not in new_data]
        for k in stale:
            del PILL_RECIPE_REGISTRY[k]
        PILL_RECIPE_REGISTRY.update(new_data)


_PILL_FORMING_MATERIAL_BY_GRADE: dict[int, tuple[str, int]] = {
    # 现有模型只有一个成丹辅材槽位，这里将文档中的“双辅材”打包为一个展示材料
    PillGrade.LOW: ("成丹辅材_下品", 1),   # 凝丹砂×1、地火粉×1
    PillGrade.HIGH: ("成丹辅材_上品", 1),  # 凝丹砂×1、玉髓露×1
    PillGrade.PURE: ("成丹辅材_无垢", 1),  # 净灵花×1、无垢泉×1
}

_PILL_RECIPE_TEMPLATES: dict[str, dict[int, tuple[tuple[str, int], tuple[str, int], tuple[str, int]]]] = {
    "healing": {
        0: (("回春草", 3), ("甘露叶", 2), ("止血藤", 1)),
        1: (("续命藤", 3), ("血灵芝", 2), ("灵泉水", 1)),
        2: (("还魂花", 2), ("生魂草", 2), ("玉髓液", 1)),
        3: (("九转魂果", 1), ("还阳参", 2), ("地心乳", 1)),
        4: (("太上生机莲", 1), ("仙露芝", 2), ("长生泉", 1)),
    },
    "attack": {
        0: (("赤阳果", 2), ("牛筋草", 2), ("烈火砂", 1)),
        1: (("虎骨草", 2), ("猛血藤", 2), ("金刚果", 1)),
        2: (("龙血藤", 2), ("苍角芝", 2), ("炎晶髓", 1)),
        3: (("天罡晶", 1), ("战皇骨粉", 2), ("烈阳髓", 1)),
        4: (("混元战髓", 1), ("真龙精血", 1), ("神力果", 2)),
    },
    "defense": {
        0: (("铁皮藤", 3), ("石甲草", 2), ("黑土精", 1)),
        1: (("玄铁砂", 2), ("厚甲芝", 2), ("山岳根", 1)),
        2: (("金刚骨粉", 2), ("镇岳花", 2), ("岩心液", 1)),
        3: (("不灭石髓", 1), ("金身果", 2), ("地脉精华", 1)),
        4: (("混元护心石", 1), ("玄武甲髓", 1), ("太清玄液", 1)),
    },
    "lingqi": {
        0: (("聚气草", 3), ("清灵叶", 2), ("微光露", 1)),
        1: (("灵泉晶", 2), ("凝气花", 2), ("山泉髓", 1)),
        2: (("天灵叶", 2), ("云华露", 2), ("青冥砂", 1)),
        3: (("九霄云露", 1), ("星辉果", 2), ("灵脉髓", 1)),
        4: (("太清灵液", 1), ("先天灵核", 1), ("仙云花", 2)),
    },
    "dao_yun": {
        0: (("悟道叶", 3), ("静心花", 2), ("灵墨砂", 1)),
        1: (("明心花", 2), ("澄神露", 2), ("灵台木", 1)),
        2: (("通玄藤", 2), ("玄思果", 2), ("清虚液", 1)),
        3: (("天道碎片", 1), ("云纹道果", 1), ("星河沙", 1)),
        4: (("混元道果", 1), ("先天道纹玉", 1), ("太初清气", 1)),
    },
    "exp": {
        0: (("聚灵草", 3), ("明气叶", 2), ("晨露", 1)),
        1: (("明悟果", 2), ("清心花", 2), ("灵砂", 1)),
        2: (("天悟花", 2), ("星思叶", 2), ("灵慧液", 1)),
        3: (("造化果", 1), ("天机露", 2), ("乾坤砂", 1)),
        4: (("太上悟道露", 1), ("仙机果", 1), ("太初灵壤", 1)),
    },
    "max_hp": {
        0: (("固本根", 3), ("参须", 2), ("黄精粉", 1)),
        1: (("培元芝", 2), ("生机草", 2), ("地脉露", 1)),
        2: (("天元果", 2), ("玉骨花", 2), ("元气液", 1)),
        3: (("造化血参", 1), ("长青藤", 2), ("地心玉髓", 1)),
        4: (("混元本源果", 1), ("长生玉芝", 1), ("太始元液", 1)),
    },
    "breakthrough": {
        0: (("破障草", 3), ("清窍叶", 2), ("炼心砂", 1)),
        1: (("筑基灵液", 2), ("凝骨花", 2), ("地灵乳", 1)),
        2: (("天机石乳", 1), ("通脉藤", 2), ("玄窍果", 1)),
        3: (("造化破境花", 1), ("劫火石", 1), ("乾元露", 1)),
        4: (("混元劫晶", 1), ("天道灵胎", 1), ("太初劫液", 1)),
    },
    "temp_attack": {
        0: (("狂血草", 2), ("赤炎果", 2), ("燥火粉", 1)),
        1: (("嗜血花", 2), ("凶兽血", 1), ("烈骨草", 2)),
        2: (("魔炎核", 1), ("黑煞藤", 2), ("血魄花", 2)),
        3: (("天魔心瓣", 1), ("焚杀号角粉", 1), ("噬炎露", 1)),
        4: (("太上狂意晶", 1), ("神魔血髓", 1), ("无相火种", 1)),
    },
    "temp_defense": {
        0: (("铁壁藤", 2), ("厚岩草", 2), ("凝甲粉", 1)),
        1: (("龟甲片", 2), ("玄壳芝", 2), ("山海盐晶", 1)),
        2: (("玄武甲粉", 1), ("镇水莲", 2), ("黑曜砂", 1)),
        3: (("磐石心", 1), ("地脉花", 2), ("山神髓", 1)),
        4: (("混元壁晶", 1), ("玄武真甲片", 1), ("太岳灵液", 1)),
    },
    "temp_lingqi": {
        0: (("灵涌草", 2), ("清泉花", 2), ("润脉露", 1)),
        1: (("灵泉露", 2), ("通脉草", 2), ("月华粉", 1)),
        2: (("天灵髓", 1), ("云灵花", 2), ("星露砂", 1)),
        3: (("灵脉晶", 1), ("地灵藤", 2), ("九曲泉", 1)),
        4: (("太清灵潮液", 1), ("仙脉核心", 1), ("上清云露", 1)),
    },
    "temp_cultivate": {
        0: (("静心叶", 2), ("宁神花", 2), ("清苦茶末", 1)),
        1: (("空明花", 2), ("定神木", 1), ("晨曦露", 2)),
        2: (("禅定木", 1), ("明台莲", 2), ("清魂砂", 1)),
        3: (("大定心香", 1), ("天游花", 2), ("归息液", 1)),
        4: (("太上定神露", 1), ("无念菩提子", 1), ("上清神木汁", 1)),
    },
    "temp_all": {
        0: (("全灵草", 2), ("和合花", 2), ("调元露", 1)),
        1: (("万灵花", 2), ("百草晶", 1), ("平衡叶", 2)),
        2: (("天地髓", 1), ("阴阳果", 1), ("合灵砂", 1)),
        3: (("造化合灵果", 1), ("四象花", 2), ("乾坤露", 1)),
        4: (("太极阴阳液", 1), ("两仪道莲", 1), ("无极灵砂", 1)),
    },
}


def _pill_recipe_mat(item_id: str, qty: int = 1) -> PillRecipeMaterial:
    return PillRecipeMaterial(item_id=item_id, qty=qty)


# ── 默认种子数据 ────────────────────────────────────────────────

def _build_default_materials() -> dict[str, MaterialDef]:
    """构建内置默认材料（来自 丹药炼丹材料清单.md）。"""
    mats = {}

    def _add(item_id, name, rarity, category, source="", description="", recycle_price=0):
        mats[item_id] = MaterialDef(
            item_id=item_id, name=name, rarity=rarity,
            category=category, source=source, description=description,
            recycle_price=recycle_price,
        )

    # 草药类 - 普通
    _add("zhixuecao", "止血草", MaterialRarity.COMMON, "herb", "新手药园", "基础止血药材", 2)
    _add("juanqiicao", "聚气草", MaterialRarity.COMMON, "herb", "新手药园", "聚集灵气的草本", 2)
    _add("puzhangcao", "破障草", MaterialRarity.RARE, "herb", "灵田/黄阶秘境", "辅助突破的灵草", 10)
    _add("huichuncao", "回春草", MaterialRarity.COMMON, "herb", "新手药园", "疗伤用草药", 2)
    _add("xuemingcao", "续命藤", MaterialRarity.RARE, "herb", "灵田", "续命灵藤", 15)
    _add("huanhunhua", "还魂花", MaterialRarity.PRECIOUS, "herb", "玄阶秘境", "起死回生之花", 80)
    _add("tianji_shilu", "天道碎片", MaterialRarity.EPIC, "herb", "地阶秘境", "蕴含天道法则的碎片", 500)
    _add("wudao_ye", "悟道叶", MaterialRarity.COMMON, "herb", "新手药园", "辅助悟道的灵叶", 3)
    _add("mingxin_hua", "明心花", MaterialRarity.RARE, "herb", "灵田", "明心见性的灵花", 20)
    _add("tongxuan_teng", "通玄藤", MaterialRarity.PRECIOUS, "herb", "玄阶秘境", "通达玄妙的灵藤", 100)
    _add("zaohua_guo", "造化果", MaterialRarity.EPIC, "herb", "地阶秘境", "蕴含造化之力的果实", 600)
    _add("guben_gen", "固本根", MaterialRarity.COMMON, "herb", "新手药园", "稳固本元的灵根", 2)
    _add("peiyuan_zhi", "培元芝", MaterialRarity.RARE, "herb", "灵田", "培补元气的灵芝", 18)
    _add("tianyuan_guo", "天元果", MaterialRarity.PRECIOUS, "herb", "玄阶秘境", "天元凝丹之果", 120)
    _add("gutaoyao", "固元草", MaterialRarity.COMMON, "herb", "新手药园", "巩固修为的草药", 3)
    _add("tianji_shieru", "天机石乳", MaterialRarity.PRECIOUS, "herb", "玄阶秘境", "天机石所化灵乳", 150)

    # 草药类 - 传说/神话
    _add("taishang_shenglian", "太上生机莲", MaterialRarity.LEGENDARY, "herb", "天阶秘境", "太上道祖遗留的生机莲花", 2000)
    _add("hunyuandaguo", "混元道果", MaterialRarity.MYTHIC, "herb", "顶级秘境", "混元大道凝结的道果", 10000)
    _add("tuotai_yusui", "脱胎玉髓", MaterialRarity.MYTHIC, "herb", "终局副本", "脱胎换骨的玉髓", 15000)
    _add("wuxiang_shitai", "无相石胎", MaterialRarity.MYTHIC, "herb", "顶级秘境", "无相天成的石胎", 20000)
    _add("wanshou_pantaoh", "万寿蟠桃核", MaterialRarity.LEGENDARY, "herb", "天阶秘境/活动", "蟠桃仙果之核", 3000)
    _add("duwu_puti", "顿悟菩提子", MaterialRarity.LEGENDARY, "herb", "顶级秘境", "触发顿悟的菩提种子", 5000)
    _add("taiqing_lu", "太上悟道露", MaterialRarity.LEGENDARY, "herb", "天阶秘境", "太上道祖的悟道甘露", 4000)

    # 矿石类 - 普通
    _add("dihuofen", "地火粉", MaterialRarity.COMMON, "mineral", "新手药园/商店", "炼丹基础辅材", 5)
    _add("heli_sha", "烈火砂", MaterialRarity.COMMON, "mineral", "新手药园", "火属性基础灵材", 3)
    _add("xuantie_sha", "玄铁砂", MaterialRarity.RARE, "mineral", "灵田", "玄铁研磨所得", 20)
    _add("jingang_gufen", "金刚骨粉", MaterialRarity.PRECIOUS, "mineral", "玄阶秘境", "金刚锻造残余骨粉", 150)
    _add("bubie_shisui", "不灭石髓", MaterialRarity.EPIC, "mineral", "地阶秘境", "不灭金身的石髓", 800)
    _add("tiangang_jing", "天罡晶", MaterialRarity.EPIC, "mineral", "地阶秘境", "天罡正气凝结的晶石", 600)
    _add("yuqie_lou", "玉切露", MaterialRarity.RARE, "mineral", "灵田", "玉矿渗出的灵露", 25)

    # 矿石类 - 传说/神话
    _add("hunyuan_zhan", "混元战髓", MaterialRarity.LEGENDARY, "mineral", "天阶秘境", "混元战意凝结的战髓", 3000)
    _add("wanxiang_shay", "万相火种", MaterialRarity.LEGENDARY, "mineral", "天阶秘境", "万般火相本源", 4000)
    _add("xuanji_bei", "玄武甲片", MaterialRarity.LEGENDARY, "mineral", "天阶秘境", "玄武神兽甲片", 3500)
    _add("taiqing_xuan", "太清玄液", MaterialRarity.LEGENDARY, "mineral", "天阶秘境", "太清上乘玄液", 5000)
    _add("taichu_lu", "太初劫液", MaterialRarity.MYTHIC, "mineral", "终局副本", "太初大劫残留灵液", 20000)
    _add("wuxiang_huozhong", "无相火种", MaterialRarity.LEGENDARY, "mineral", "天阶秘境", "无相天成的火种本源", 6000)

    # 妖兽材料
    _add("tongmai_teng", "通脉藤", MaterialRarity.RARE, "beast", "黄阶秘境/坊市", "妖兽体内的灵藤", 15)
    _add("man_niujin", "蛮牛筋", MaterialRarity.RARE, "beast", "黄阶秘境", "蛮牛的筋骨", 20)
    _add("xiongshou_xue", "凶兽血", MaterialRarity.RARE, "beast", "黄阶秘境", "凶兽精血", 30)
    _add("baigui_po", "百骸破", MaterialRarity.PRECIOUS, "beast", "玄阶秘境", "妖兽骸骨残片", 120)
    _add("longxue_teng", "龙血藤", MaterialRarity.PRECIOUS, "beast", "玄阶秘境", "真龙血脉浸润的灵藤", 200)
    _add("mo_yanhe", "魔炎核", MaterialRarity.PRECIOUS, "beast", "玄阶秘境", "魔兽魔炎核心", 180)
    _add("zhanshi_guf", "战皇骨粉", MaterialRarity.EPIC, "beast", "地阶秘境", "战皇遗骨研磨的粉末", 700)
    _add("zhenlong_jingxue", "真龙精血", MaterialRarity.LEGENDARY, "beast", "天阶秘境", "真龙族精血", 5000)
    _add("shenshi_xuesui", "神魔血髓", MaterialRarity.LEGENDARY, "beast", "天阶秘境", "神魔级血髓", 8000)
    _add("tianmo_xinban", "天魔心瓣", MaterialRarity.LEGENDARY, "beast", "天阶秘境", "天魔心脉瓣膜", 4000)
    _add("tianmo_shaohao", "焚杀号角粉", MaterialRarity.EPIC, "beast", "地阶秘境", "天魔战号残粉", 900)

    # 灵液类
    _add("ganlu", "甘露", MaterialRarity.COMMON, "spirit_fluid", "新手药园", "基础灵露", 2)
    _add("lingquanshui", "灵泉水", MaterialRarity.RARE, "spirit_fluid", "灵田", "灵泉所出泉水", 15)
    _add("yusuiye", "玉髓液", MaterialRarity.RARE, "spirit_fluid", "灵田", "玉矿灵液", 25)
    _add("qingxu_ye", "清虚液", MaterialRarity.PRECIOUS, "spirit_fluid", "玄阶秘境", "清虚凝神的灵液", 120)
    _add("lingmai_sui", "灵脉髓", MaterialRarity.EPIC, "spirit_fluid", "地阶秘境", "灵脉核心的髓液", 600)
    _add("yuanqi_ye", "元气液", MaterialRarity.PRECIOUS, "spirit_fluid", "玄阶秘境", "纯净元气的灵液", 100)
    _add("guixi_ye", "贵息液", MaterialRarity.PRECIOUS, "spirit_fluid", "玄阶秘境", "归息凝神的灵液", 110)
    _add("jiuxiangquan", "九曲泉", MaterialRarity.EPIC, "spirit_fluid", "地阶秘境", "九曲连环的灵泉", 700)
    _add("xianmai_heixin", "仙脉核心", MaterialRarity.LEGENDARY, "spirit_fluid", "天阶秘境", "仙脉凝结的核心", 6000)
    _add("taiqing_lingchao", "太清灵潮液", MaterialRarity.LEGENDARY, "spirit_fluid", "天阶秘境", "太清灵潮凝聚的灵液", 5000)

    # 通用成丹辅材
    _add("ningdan_sha", "凝丹砂", MaterialRarity.COMMON, "special", "新手药园/商店", "炼丹通用辅材，凝聚丹形", 5)
    _add("yusuilo", "玉髓露", MaterialRarity.RARE, "special", "灵田/坊市", "炼丹上品辅材，提升丹药品质", 30)
    _add("jingling_hua", "净灵花", MaterialRarity.LEGENDARY, "special", "顶级秘境", "无垢炼丹传说辅材，净化杂质", 500)
    _add("wugou_quan", "无垢泉", MaterialRarity.LEGENDARY, "special", "顶级秘境", "无垢炼丹传说辅材，去除所有副作用", 800)

    # 特殊/唯一材料
    _add("naihuan_huoyu", "涅槃火羽", MaterialRarity.MYTHIC, "special", "顶级秘境", "凤凰涅槃遗留火羽", 20000)
    _add("zhenhuang_jingxue", "真凰精血", MaterialRarity.MYTHIC, "special", "顶级秘境", "真凤一族的纯净精血", 25000)
    _add("changsheng_quan", "长生泉", MaterialRarity.LEGENDARY, "special", "天阶秘境/活动", "长生不朽的泉水", 3000)
    _add("xianlu_zhi", "仙露芝", MaterialRarity.LEGENDARY, "special", "天阶秘境", "蕴含仙气的灵芝", 2000)
    _add("taishi_yeyuan", "太始元液", MaterialRarity.MYTHIC, "special", "终局副本", "太始时代遗留的元液", 15000)
    _add("wudaowen_yu", "先天道纹玉", MaterialRarity.LEGENDARY, "special", "顶级秘境", "先天道纹凝结的宝玉", 8000)
    _add("yunwen_daoguo", "云纹道果", MaterialRarity.LEGENDARY, "special", "天阶秘境", "道韵凝结的道果", 6000)
    _add("taichu_qingqi", "太初清气", MaterialRarity.LEGENDARY, "special", "天阶秘境", "太初清气，炼丹至宝", 7000)
    _add("tianji_xianru", "天机仙乳", MaterialRarity.LEGENDARY, "special", "天阶秘境", "高阶天机石乳凝练而成的仙乳", 4000)

    # 文档中的成丹辅材组合（现有模型单槽位，使用组合材料承载）
    _add("成丹辅材_下品", "凝丹砂×1、地火粉×1", MaterialRarity.COMMON, "special", "配方规则", "下品固定成丹辅材组合", 6)
    _add("成丹辅材_上品", "凝丹砂×1、玉髓露×1", MaterialRarity.RARE, "special", "配方规则", "上品固定成丹辅材组合", 35)
    _add("成丹辅材_无垢", "净灵花×1、无垢泉×1", MaterialRarity.LEGENDARY, "special", "配方规则", "无垢固定成丹辅材组合", 1000)

    # 特殊丹药组合辅材包
    _add("特丹辅材_涅槃", "长生玉芝×1、无垢泉×2", MaterialRarity.MYTHIC, "special", "特殊丹方", "涅槃天丹辅材组合", 20000)
    _add("特丹辅材_脱胎", "太清玄液×1、无垢泉×2", MaterialRarity.MYTHIC, "special", "特殊丹方", "脱胎换骨丹辅材组合", 18000)
    _add("特丹辅材_无相", "先天道纹玉×1、玉髓露×2", MaterialRarity.LEGENDARY, "special", "特殊丹方", "无相丹辅材组合", 10000)
    _add("特丹辅材_万寿", "太始元液×1、无垢泉×2", MaterialRarity.MYTHIC, "special", "特殊丹方", "万寿丹辅材组合", 22000)
    _add("特丹辅材_顿悟", "云纹道果×1、净灵花×2", MaterialRarity.LEGENDARY, "special", "特殊丹方", "顿悟丹辅材组合", 12000)

    # 按文档丹方自动补齐缺失材料，避免配方引用到“未知材料”
    def _ensure_material(item_id: str, *, rarity: int = MaterialRarity.COMMON, category: str = "special"):
        if not item_id or item_id in mats:
            return
        _add(
            item_id=item_id,
            name=item_id,
            rarity=rarity,
            category=category,
            source="丹方文档",
            description="按丹方文档自动补齐的材料",
            recycle_price=10,
        )

    required_material_ids: set[str] = set()
    for tier_map in _PILL_RECIPE_TEMPLATES.values():
        for recipe_tuple in tier_map.values():
            for item_id, _qty in recipe_tuple:
                required_material_ids.add(str(item_id))
    for item_id, _qty in _PILL_FORMING_MATERIAL_BY_GRADE.values():
        required_material_ids.add(str(item_id))

    # 特殊丹药与旧版兼容丹药的额外材料
    required_material_ids.update({
        "涅槃火羽", "真凰精血", "混元道果", "脱胎玉髓", "还阳参",
        "净灵花", "无相石胎", "玄武甲髓", "真龙精血", "万寿蟠桃核",
        "长生泉", "仙露芝", "顿悟菩提子", "天机仙乳", "太上悟道露",
        "止血草", "回气叶", "甘露花", "聚气草", "明灵叶", "晨露",
        "破障草", "通脉藤", "定心果", "淬骨草", "蛮牛筋", "赤土精",
        "凝丹砂",
    })

    for material_id in sorted(required_material_ids):
        _ensure_material(material_id)

    return mats


def _build_default_pill_recipes() -> dict[str, PillRecipeDef]:
    """构建内置默认丹方（程序化丹药 + 特殊丹药 + 旧版兼容丹药）。"""
    recipes: dict[str, PillRecipeDef] = {}

    for pill in PILL_REGISTRY.values():
        if pill.category == "special":
            continue
        template = _PILL_RECIPE_TEMPLATES.get(pill.category, {})
        tier_template = template.get(int(pill.tier))
        if not tier_template:
            continue
        forming_id, forming_qty = _PILL_FORMING_MATERIAL_BY_GRADE.get(int(pill.grade), ("成丹辅材_下品", 1))
        main_material, auxiliary_material, catalyst = tier_template
        recipes[f"recipe_{pill.pill_id}"] = PillRecipeDef(
            recipe_id=f"recipe_{pill.pill_id}",
            pill_id=pill.pill_id,
            grade=int(pill.grade),
            main_material=_pill_recipe_mat(*main_material),
            auxiliary_material=_pill_recipe_mat(*auxiliary_material),
            catalyst=_pill_recipe_mat(*catalyst),
            forming_material=_pill_recipe_mat(forming_id, forming_qty),
        )

    recipes.update({
        # 特殊丹药：沿用 4 槽位模型，最后两味材料以“辅材包”形式落在 forming_material
        "recipe_nirvana": PillRecipeDef(
            recipe_id="recipe_nirvana",
            pill_id="pill_special_nirvana",
            grade=PillGrade.PURE,
            main_material=PillRecipeMaterial("涅槃火羽", 1),
            auxiliary_material=PillRecipeMaterial("真凰精血", 1),
            catalyst=PillRecipeMaterial("混元道果", 1),
            forming_material=PillRecipeMaterial("特丹辅材_涅槃", 1),  # 长生玉芝×1、无垢泉×2
        ),
        "recipe_rebirth": PillRecipeDef(
            recipe_id="recipe_rebirth",
            pill_id="pill_special_reborn",
            grade=PillGrade.PURE,
            main_material=PillRecipeMaterial("脱胎玉髓", 1),
            auxiliary_material=PillRecipeMaterial("还阳参", 1),
            catalyst=PillRecipeMaterial("净灵花", 2),
            forming_material=PillRecipeMaterial("特丹辅材_脱胎", 1),  # 太清玄液×1、无垢泉×2
        ),
        "recipe_formless": PillRecipeDef(
            recipe_id="recipe_formless",
            pill_id="pill_special_wuxiang",
            grade=PillGrade.PURE,
            main_material=PillRecipeMaterial("无相石胎", 1),
            auxiliary_material=PillRecipeMaterial("玄武甲髓", 1),
            catalyst=PillRecipeMaterial("真龙精血", 1),
            forming_material=PillRecipeMaterial("特丹辅材_无相", 1),  # 先天道纹玉×1、玉髓露×2
        ),
        "recipe_longevity": PillRecipeDef(
            recipe_id="recipe_longevity",
            pill_id="pill_special_longevity",
            grade=PillGrade.PURE,
            main_material=PillRecipeMaterial("万寿蟠桃核", 1),
            auxiliary_material=PillRecipeMaterial("长生泉", 1),
            catalyst=PillRecipeMaterial("仙露芝", 2),
            forming_material=PillRecipeMaterial("特丹辅材_万寿", 1),  # 太始元液×1、无垢泉×2
        ),
        "recipe_insight": PillRecipeDef(
            recipe_id="recipe_insight",
            pill_id="pill_special_insight",
            grade=PillGrade.PURE,
            main_material=PillRecipeMaterial("顿悟菩提子", 1),
            auxiliary_material=PillRecipeMaterial("天机仙乳", 1),
            catalyst=PillRecipeMaterial("太上悟道露", 1),
            forming_material=PillRecipeMaterial("特丹辅材_顿悟", 1),  # 云纹道果×1、净灵花×2
        ),
    })

    # 旧版兼容丹药（文档保留条目）
    recipes.update({
        "recipe_legacy_healing_pill": PillRecipeDef(
            recipe_id="recipe_legacy_healing_pill",
            pill_id="healing_pill",
            grade=PillGrade.LOW,
            main_material=PillRecipeMaterial("止血草", 3),
            auxiliary_material=PillRecipeMaterial("回气叶", 2),
            catalyst=PillRecipeMaterial("甘露花", 1),
            forming_material=PillRecipeMaterial("凝丹砂", 1),
        ),
        "recipe_legacy_exp_pill": PillRecipeDef(
            recipe_id="recipe_legacy_exp_pill",
            pill_id="exp_pill",
            grade=PillGrade.LOW,
            main_material=PillRecipeMaterial("聚气草", 3),
            auxiliary_material=PillRecipeMaterial("明灵叶", 2),
            catalyst=PillRecipeMaterial("晨露", 1),
            forming_material=PillRecipeMaterial("凝丹砂", 1),
        ),
        "recipe_legacy_breakthrough_pill": PillRecipeDef(
            recipe_id="recipe_legacy_breakthrough_pill",
            pill_id="breakthrough_pill",
            grade=PillGrade.LOW,
            main_material=PillRecipeMaterial("破障草", 2),
            auxiliary_material=PillRecipeMaterial("通脉藤", 2),
            catalyst=PillRecipeMaterial("定心果", 1),
            forming_material=PillRecipeMaterial("凝丹砂", 1),
        ),
        "recipe_legacy_body_tempering_pill": PillRecipeDef(
            recipe_id="recipe_legacy_body_tempering_pill",
            pill_id="body_tempering_pill",
            grade=PillGrade.LOW,
            main_material=PillRecipeMaterial("淬骨草", 3),
            auxiliary_material=PillRecipeMaterial("蛮牛筋", 1),
            catalyst=PillRecipeMaterial("赤土精", 1),
            forming_material=PillRecipeMaterial("凝丹砂", 1),
        ),
    })
    return recipes


# 加载种子数据到注册表（启动时即有默认值，数据库后续可覆盖）
_DEFAULT_MATERIALS = _build_default_materials()
MATERIAL_REGISTRY.update(_DEFAULT_MATERIALS)

_DEFAULT_PILL_RECIPES = _build_default_pill_recipes()
PILL_RECIPE_REGISTRY.update(_DEFAULT_PILL_RECIPES)


def set_realm_config(realms: dict[int, dict]):
    """替换境界配置（供数据库加载后同步到运行时）。"""
    REALM_CONFIG.clear()
    REALM_CONFIG.update(realms)


def get_sorted_realm_levels() -> list[int]:
    """按从低到高返回当前已配置的境界等级。"""
    return sorted(int(level) for level in REALM_CONFIG.keys())


def get_max_realm_level() -> int:
    """获取当前配置的最大境界等级。"""
    levels = get_sorted_realm_levels()
    return levels[-1] if levels else 0


def get_next_realm_level(realm: int) -> int | None:
    """获取比当前境界更高的下一个已配置境界。"""
    current = int(realm)
    for level in get_sorted_realm_levels():
        if level > current:
            return level
    return None


def get_previous_realm_level(realm: int) -> int | None:
    """获取比当前境界更低的上一个已配置境界。"""
    current = int(realm)
    prev = None
    for level in get_sorted_realm_levels():
        if level >= current:
            break
        prev = level
    return prev


def get_nearest_realm_level(realm: int) -> int:
    """获取与给定等级最接近的已配置境界，优先回退到更低境界。"""
    current = int(realm)
    if current in REALM_CONFIG:
        return current
    levels = get_sorted_realm_levels()
    if not levels:
        return 0
    prev = get_previous_realm_level(current)
    nxt = get_next_realm_level(current)
    if prev is None:
        return nxt if nxt is not None else levels[0]
    if nxt is None:
        return prev
    if abs(current - prev) <= abs(nxt - current):
        return prev
    return nxt


def has_sub_realm(realm: int) -> bool:
    """该大境界是否有小境界。"""
    cfg = REALM_CONFIG.get(realm)
    return bool(cfg and cfg.get("has_sub_realm"))


def is_high_realm(realm: int) -> bool:
    """是否为高阶大境界（化神~大乘，4层小境界）。"""
    cfg = REALM_CONFIG.get(realm)
    return bool(cfg and cfg.get("high_realm"))


def get_max_sub_realm(realm: int) -> int:
    """获取该大境界的最大小境界索引。"""
    if is_high_realm(realm):
        return MAX_HIGH_SUB_REALM
    if has_sub_realm(realm):
        return MAX_SUB_REALM
    return 0


def get_sub_realm_dao_yun_cost(realm: int, sub_realm: int) -> int:
    """获取从 sub_realm 升到 sub_realm+1 所需道韵（从 REALM_CONFIG 动态读取）。"""
    cfg = REALM_CONFIG.get(realm, {})
    costs = cfg.get("sub_dao_yun_costs", [])
    if not costs:
        return 0
    return costs[sub_realm] if sub_realm < len(costs) else 0


def get_breakthrough_dao_yun_cost(realm: int) -> int:
    """获取从 realm 突破到 realm+1 所需道韵（从 REALM_CONFIG 动态读取）。"""
    cfg = REALM_CONFIG.get(realm, {})
    return int(cfg.get("breakthrough_dao_yun_cost", 0))


def get_dao_yun_rate(realm: int, sub_realm: int = 0) -> float:
    """计算当前境界/小境界的道韵产出概率。

    化神期及以上境界基础10%，每个小境界+5%，无上限。
    可与心法的道韵概率叠加，独立触发。

    Returns:
        0.0 表示该境界不触发道韵产出。
    """
    cfg = REALM_CONFIG.get(realm, {})
    base_rate = cfg.get("dao_yun_base_rate", 0.0)
    if base_rate <= 0:
        return 0.0
    per_sub = cfg.get("dao_yun_per_sub_realm", 0.05)
    return base_rate + per_sub * sub_realm


def get_realm_name(realm: int, sub_realm: int = 0) -> str:
    """获取完整境界名称，如 '练气期·三层' 或 '化神期·中期'。"""
    cfg = REALM_CONFIG.get(realm)
    if not cfg:
        return "未知"
    name = cfg["name"]
    if cfg.get("has_sub_realm"):
        if cfg.get("high_realm"):
            if 0 <= sub_realm <= MAX_HIGH_SUB_REALM:
                name += "·" + HIGH_SUB_REALM_NAMES[sub_realm]
        elif 0 <= sub_realm <= MAX_SUB_REALM:
            name += "·" + SUB_REALM_NAMES[sub_realm]
    return name


def get_max_lingqi_by_realm(realm: int, sub_realm: int = 0) -> int:
    """根据境界与小境界计算灵气上限。"""
    cfg = REALM_CONFIG.get(realm, {})
    base_lingqi = int(cfg.get("base_lingqi", 50))
    if not has_sub_realm(realm):
        return base_lingqi
    max_sr = get_max_sub_realm(realm)
    sub_realm = max(0, min(max_sr, int(sub_realm)))
    lingqi_step = max(1, int(base_lingqi * 0.08))
    return base_lingqi + lingqi_step * sub_realm


def get_realm_base_stats(realm: int, sub_realm: int = 0) -> dict[str, int]:
    """根据境界与小境界计算基础属性。"""
    cfg = REALM_CONFIG.get(realm, {})
    base_hp = int(cfg.get("base_hp", 100))
    base_attack = int(cfg.get("base_attack", 10))
    base_defense = int(cfg.get("base_defense", 5))
    base_lingqi = int(cfg.get("base_lingqi", 50))
    if not has_sub_realm(realm):
        return {
            "max_hp": base_hp,
            "attack": base_attack,
            "defense": base_defense,
            "max_lingqi": base_lingqi,
        }
    max_sr = get_max_sub_realm(realm)
    sub_realm = max(0, min(max_sr, int(sub_realm)))
    hp_step = int(base_hp * 0.08)
    atk_step = int(base_attack * 0.06)
    def_step = int(base_defense * 0.06)
    lingqi_step = max(1, int(base_lingqi * 0.08))
    return {
        "max_hp": base_hp + hp_step * sub_realm,
        "attack": base_attack + atk_step * sub_realm,
        "defense": base_defense + def_step * sub_realm,
        "max_lingqi": base_lingqi + lingqi_step * sub_realm,
    }


def get_player_base_stats(player) -> dict[str, int]:
    """根据玩家当前境界与永久丹药加成计算基础属性。"""
    stats = get_realm_base_stats(player.realm, player.sub_realm)
    stats["max_hp"] += max(0, int(getattr(player, "permanent_max_hp_bonus", 0)))
    stats["attack"] += max(0, int(getattr(player, "permanent_attack_bonus", 0)))
    stats["defense"] += max(0, int(getattr(player, "permanent_defense_bonus", 0)))
    stats["max_lingqi"] += max(0, int(getattr(player, "permanent_lingqi_bonus", 0)))
    return stats


def get_player_base_max_lingqi(player) -> int:
    """根据玩家当前境界与永久灵气加成计算灵气上限。"""
    return get_player_base_stats(player)["max_lingqi"]


# ── 副本（历练）常量 ──────────────────────────────────────
LAYER_PASS_RATES = [0.80, 0.72, 0.64, 0.56, 0.50]
LAYER_REWARD_TYPES = ["spirit_stones", "equipment", "pills", "heart_method", "gongfa"]
LAYER_NAMES = ["灵石秘境", "兵器洞府", "丹药福地", "心法秘阁", "功法圣殿"]
DANGER_WEIGHTS = {"disaster": 80, "monster": 15, "enemy": 5}
DISASTER_OUTCOMES = {"hp_damage": 90, "realm_drop": 7, "catastrophe": 3}
FLEE_BASE_RATES = [0.70, 0.60, 0.50, 0.40, 0.30]
DUNGEON_RISK_SCORE_CAP = 100.0
DUNGEON_LOW_HP_LINE = 0.20
DUNGEON_FAILURE_WEIGHTS = {
    "hp_loss": 0.38,
    "threat_gap": 0.24,
    "layer": 0.16,
    "risk_stack": 0.14,
    "low_hp": 0.08,
}
DUNGEON_FAILURE_THRESHOLDS = {
    "minor": 35.0,
    "serious": 55.0,
    "critical": 75.0,
}
DUNGEON_DEATH_MODEL = {
    "guard_layers": 2,
    "base": 0.003,
    "per_point": 0.0006,
    "max": 0.018,
}
DUNGEON_RISK_ADJUSTMENTS = {
    "safe_pass": -4.0,
    "combat_win": -6.0,
    "disaster_damage_base": 8.0,
    "disaster_damage_ratio": 28.0,
    "low_hp_bonus": 12.0,
    "failed_flee": 10.0,
    "catastrophe": 18.0,
    "realm_drop": 16.0,
}
# (概率, 属性比) — "realm_up" 表示高1大境界
ENEMY_TIERS: list[tuple] = [(0.90, 0.70), (0.09, 1.50), (0.01, "realm_up")]
COMBAT_MAX_ROUNDS = 30
PVP_ROUND_TIMEOUT = 30  # 秒
