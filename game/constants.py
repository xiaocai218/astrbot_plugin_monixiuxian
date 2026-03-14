"""游戏常量：境界配置、物品注册表、装备注册表。"""

import hashlib
from dataclasses import dataclass, field
from datetime import date
from enum import IntEnum


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


# 有小境界的大境界范围（练气~元婴，每个10层）
SUB_REALM_MIN = RealmLevel.QI_REFINING
SUB_REALM_MAX = RealmLevel.NASCENT_SOUL
MAX_SUB_REALM = 9  # 0=一层, 9=十层(圆满)

SUB_REALM_NAMES = ["一层", "二层", "三层", "四层", "五层", "六层", "七层", "八层", "九层", "圆满"]

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
        "has_sub_realm": False,
        "exp_to_next": 50000,
        "sub_exp_to_next": 0,
        "base_hp": 12000,
        "base_attack": 1200,
        "base_defense": 600,
        "base_lingqi": 3600,
        "breakthrough_rate": 0.25,
        "death_rate": 0.10,         # 10% 死亡
    },
    RealmLevel.VOID_MERGE: {
        "name": "合虚期",
        "has_sub_realm": False,
        "exp_to_next": 150000,
        "sub_exp_to_next": 0,
        "base_hp": 30000,
        "base_attack": 3000,
        "base_defense": 1500,
        "base_lingqi": 8000,
        "breakthrough_rate": 0.18,
        "death_rate": 0.15,         # 15% 死亡
    },
    RealmLevel.TRIBULATION: {
        "name": "渡劫期",
        "has_sub_realm": False,
        "exp_to_next": 500000,
        "sub_exp_to_next": 0,
        "base_hp": 80000,
        "base_attack": 8000,
        "base_defense": 4000,
        "base_lingqi": 18000,
        "breakthrough_rate": 0.12,
        "death_rate": 0.20,         # 20% 死亡
    },
    RealmLevel.MAHAYANA: {
        "name": "大乘期",
        "has_sub_realm": False,
        "exp_to_next": 999999999,
        "sub_exp_to_next": 0,
        "base_hp": 200000,
        "base_attack": 20000,
        "base_defense": 10000,
        "base_lingqi": 40000,
        "breakthrough_rate": 0.0,
        "death_rate": 0.30,         # 30% 死亡（若有更高境界）
    },
}


@dataclass
class ItemDef:
    """物品定义。"""
    item_id: str
    name: str
    item_type: str  # "consumable" | "material" | "equipment"
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
    """根据当前 EQUIPMENT_REGISTRY 同步刷新装备物品定义。"""
    to_remove = [
        item_id for item_id, item in ITEM_REGISTRY.items()
        if getattr(item, "item_type", "") == "equipment"
    ]
    for item_id in to_remove:
        ITEM_REGISTRY.pop(item_id, None)

    for eq in EQUIPMENT_REGISTRY.values():
        ITEM_REGISTRY[eq.equip_id] = ItemDef(
            item_id=eq.equip_id,
            name=eq.name,
            item_type="equipment",
            description=eq.description,
            effect={"equip_id": eq.equip_id},
        )


def set_equipment_registry(equipments: dict[str, EquipmentDef]):
    """替换装备注册表（供数据库加载后同步到运行时）。"""
    # 原地更新，避免其他模块通过 from-import 持有旧 dict 引用。
    EQUIPMENT_REGISTRY.clear()
    EQUIPMENT_REGISTRY.update(dict(equipments))
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

    # 心法秘籍
    hm_id = parse_heart_method_manual_id(item_id)
    if hm_id:
        hm = HEART_METHOD_REGISTRY.get(hm_id)
        if hm:
            return int(20 * (1 + hm.realm * 0.8))
        return 20

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
    """检查指定境界能否装备指定品阶的装备。"""
    req = TIER_REALM_REQUIREMENTS.get(tier)
    if not req:
        return False
    return req[0] <= realm <= req[1]


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


def get_heart_method_manual_id(method_id: str) -> str:
    """将心法ID转换为秘籍物品ID。"""
    return f"{HEART_METHOD_MANUAL_PREFIX}{method_id}"


def parse_heart_method_manual_id(item_id: str) -> str | None:
    """从秘籍物品ID解析心法ID。"""
    if not item_id.startswith(HEART_METHOD_MANUAL_PREFIX):
        return None
    return item_id[len(HEART_METHOD_MANUAL_PREFIX):] or None


def _refresh_heart_method_manual_items():
    """根据当前 HEART_METHOD_REGISTRY 重新生成心法秘籍定义。"""
    to_remove = [item_id for item_id in ITEM_REGISTRY if item_id.startswith(HEART_METHOD_MANUAL_PREFIX)]
    for item_id in to_remove:
        ITEM_REGISTRY.pop(item_id, None)

    for hm in HEART_METHOD_REGISTRY.values():
        manual_id = get_heart_method_manual_id(hm.method_id)
        realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界")
        quality_name = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通")
        ITEM_REGISTRY[manual_id] = ItemDef(
            item_id=manual_id,
            name=f"{hm.name}秘籍",
            item_type="consumable",
            description=f"可领悟{quality_name}心法【{hm.name}】（{realm_name}）",
            effect={"learn_heart_method": hm.method_id},
        )


def set_heart_method_registry(methods: dict[str, HeartMethodDef]):
    """替换心法注册表（供数据库加载后同步到运行时）。"""
    # 原地更新，避免其他模块通过 from-import 持有旧 dict 引用。
    HEART_METHOD_REGISTRY.clear()
    HEART_METHOD_REGISTRY.update(dict(methods))
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


def has_sub_realm(realm: int) -> bool:
    """该大境界是否有小境界。"""
    cfg = REALM_CONFIG.get(realm)
    return bool(cfg and cfg.get("has_sub_realm"))


def get_realm_name(realm: int, sub_realm: int = 0) -> str:
    """获取完整境界名称，如 '练气期·三层'。"""
    cfg = REALM_CONFIG.get(realm)
    if not cfg:
        return "未知"
    name = cfg["name"]
    if cfg.get("has_sub_realm") and 0 <= sub_realm <= MAX_SUB_REALM:
        name += "·" + SUB_REALM_NAMES[sub_realm]
    return name
