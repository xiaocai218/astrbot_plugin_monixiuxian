"""宗门系统核心逻辑（纯函数 / 异步函数）。"""
from __future__ import annotations

import copy
import secrets
import time
from typing import TYPE_CHECKING

import aiosqlite

from .constants import (
    get_realm_name,
    ITEM_REGISTRY,
    EQUIPMENT_REGISTRY,
    EQUIPMENT_TIER_NAMES,
    HEART_METHOD_REGISTRY,
    HEART_METHOD_QUALITY_NAMES,
    GONGFA_REGISTRY,
    GONGFA_TIER_NAMES,
    MATERIAL_REGISTRY,
    MATERIAL_RARITY_NAMES,
    PILL_RECIPE_REGISTRY,
    PILL_GRADE_NAMES,
    REALM_CONFIG,
    SEED_REGISTRY,
    parse_heart_method_manual_id,
    parse_stored_heart_method_item_id,
    parse_gongfa_scroll_id,
    parse_pill_recipe_item_id,
    get_pill_recipe_item_id,
    get_recycle_base_price,
)

if TYPE_CHECKING:
    from .data_manager import DataManager
    from .models import Player

# ── 常量 ────────────────────────────────────────────────
CREATE_COST = 5000          # 创建宗门所需灵石
NAME_MIN_LEN = 2
NAME_MAX_LEN = 8
DESC_MAX_LEN = 100
MAX_VICE_LEADERS = 2
MAX_ELDERS = 10

ROLE_RANK = {"leader": 0, "vice_leader": 1, "elder": 2, "disciple": 3}
ROLE_NAMES = {
    "leader": "宗主",
    "vice_leader": "副宗主",
    "elder": "长老",
    "disciple": "弟子",
}

# ── 宗门仓库常量 ──────────────────────────────────────────
WAREHOUSE_DEFAULT_CAPACITY = 200  # 默认仓库容量（格）

# 物品品质分类键 → 默认上交贡献点 / 默认兑换贡献点
# 上交点数 < 兑换点数，形成差价
DEFAULT_SUBMIT_POINTS: dict[str, int] = {
    "consumable": 5,
    "material": 2,
    "equipment_0": 20,    # 凡器
    "equipment_1": 50,    # 灵器
    "equipment_2": 120,   # 道器
    "equipment_3": 250,   # 先天道器
    "heart_method_0": 30,   # 普通心法
    "heart_method_1": 80,   # 史诗心法
    "heart_method_2": 180,  # 传说心法
    "gongfa_0": 20,   # 黄阶功法
    "gongfa_1": 50,   # 玄阶功法
    "gongfa_2": 120,  # 地阶功法
    "gongfa_3": 250,  # 天阶功法
}
DEFAULT_EXCHANGE_POINTS: dict[str, int] = {
    "consumable": 10,
    "material": 5,
    "equipment_0": 40,
    "equipment_1": 100,
    "equipment_2": 240,
    "equipment_3": 500,
    "heart_method_0": 60,
    "heart_method_1": 160,
    "heart_method_2": 360,
    "gongfa_0": 40,
    "gongfa_1": 100,
    "gongfa_2": 240,
    "gongfa_3": 500,
}

QUALITY_CATEGORY_NAMES: dict[str, str] = {
    "consumable": "消耗品",
    "material": "材料",
    "equipment_0": "凡器",
    "equipment_1": "灵器",
    "equipment_2": "道器",
    "equipment_3": "先天道器",
    "heart_method_0": "普通心法",
    "heart_method_1": "史诗心法",
    "heart_method_2": "传说心法",
    "gongfa_0": "黄阶功法",
    "gongfa_1": "玄阶功法",
    "gongfa_2": "地阶功法",
    "gongfa_3": "天阶功法",
}

# 角色兑换折扣：宗主免费，副宗主三折，长老五折，弟子全价
ROLE_EXCHANGE_DISCOUNT: dict[str, float] = {
    "leader": 0.0,
    "vice_leader": 0.3,
    "elder": 0.5,
    "disciple": 1.0,
}

# ── 宗门商店常量 ─────────────────────────────────────────
SECT_SHOP_ITEM_COUNT = 40          # 每日刷新件数
SECT_SHOP_DAILY_LIMIT = 100        # 每人每件商品每日限购数量
SECT_SHOP_DISCOUNT_VS_TIANJI = 0.3  # 宗门商店贡献点 = 天机阁灵石价格 ÷ 除数
SECT_SHOP_SPIRIT_TO_CONTRIB_RATE = 50  # 天机阁价格 / 此数 = 宗门商店贡献点

# 宗门商店类型权重（含丹方，权重与天机阁类似）
SECT_SHOP_TYPE_WEIGHTS: dict[str, int] = {
    "pill": 220,
    "equipment": 200,
    "heart_method": 160,
    "gongfa": 120,
    "material": 150,
    "pill_recipe": 150,   # 宗门商店丹方概率高于天机阁
    "seed": 80,           # 种子出现率与丹方相近
}

# 种子稀有度贡献点倍率（高稀有度贡献点更高）
SECT_SEED_RARITY_CONTRIB_MULTIPLIER: dict[int, int] = {
    0: 1,     # 普通
    1: 5,     # 稀有
    2: 20,    # 珍稀
    3: 80,    # 史诗
    4: 400,   # 传说
    5: 2000,  # 神话
}

# 丹方贡献点价格（按品级）
SECT_RECIPE_GRADE_CONTRIB: dict[int, int] = {
    0: 200,   # 下品丹方
    1: 600,   # 上品丹方
    2: 2000,  # 无垢丹方
}

# 材料稀有度权重（宗门商店，与天机阁一致）
SECT_MATERIAL_RARITY_WEIGHTS: dict[int, int] = {
    0: 6000, 1: 2500, 2: 800, 3: 150, 4: 15, 5: 1,
}

# 宗门商店质量分类（补充丹方）
DEFAULT_SUBMIT_POINTS["pill_recipe_0"] = 80    # 下品丹方
DEFAULT_SUBMIT_POINTS["pill_recipe_1"] = 250   # 上品丹方
DEFAULT_SUBMIT_POINTS["pill_recipe_2"] = 800   # 无垢丹方

DEFAULT_EXCHANGE_POINTS["pill_recipe_0"] = 200
DEFAULT_EXCHANGE_POINTS["pill_recipe_1"] = 600
DEFAULT_EXCHANGE_POINTS["pill_recipe_2"] = 2000

QUALITY_CATEGORY_NAMES["pill_recipe_0"] = "下品丹方"
QUALITY_CATEGORY_NAMES["pill_recipe_1"] = "上品丹方"
QUALITY_CATEGORY_NAMES["pill_recipe_2"] = "无垢丹方"

SEED_WAREHOUSE_POINT_MULTIPLIER: dict[int, int] = {
    0: 1,
    1: 5,
    2: 20,
    3: 80,
    4: 400,
    5: 2000,
}

for rarity, multiplier in SEED_WAREHOUSE_POINT_MULTIPLIER.items():
    quality_key = f"seed_{rarity}"
    quality_name = f"{MATERIAL_RARITY_NAMES.get(rarity, '普通')}种子"
    DEFAULT_SUBMIT_POINTS[quality_key] = 2 * multiplier
    DEFAULT_EXCHANGE_POINTS[quality_key] = 5 * multiplier
    QUALITY_CATEGORY_NAMES[quality_key] = quality_name


def _gen_sect_id() -> str:
    return "sect_" + secrets.token_hex(8)


def _parse_review_result(review) -> tuple[bool, str]:
    """兼容 dict / tuple / bool 三种审核返回格式。"""
    if isinstance(review, dict):
        return bool(review.get("allow", True)), str(review.get("reason", "")).strip()
    if isinstance(review, tuple) and review:
        allow = bool(review[0])
        reason = str(review[1]).strip() if len(review) > 1 else ""
        return allow, reason
    if isinstance(review, bool):
        return review, ""
    return True, ""


# ── 创建 / 解散 ─────────────────────────────────────────

async def create_sect(
    player: "Player",
    name: str,
    description: str,
    dm: "DataManager",
    *,
    name_reviewer=None,
) -> dict:
    """创建宗门。"""
    name = name.strip()
    description = description.strip()[:DESC_MAX_LEN]

    if len(name) < NAME_MIN_LEN or len(name) > NAME_MAX_LEN:
        return {"success": False, "message": f"宗门名称需 {NAME_MIN_LEN}-{NAME_MAX_LEN} 个字"}

    if player.spirit_stones < CREATE_COST:
        return {"success": False, "message": f"灵石不足，创建宗门需要 {CREATE_COST} 灵石"}

    existing = await dm.load_player_sect(player.user_id)
    if existing:
        return {"success": False, "message": "你已加入宗门，无法创建新宗门"}

    dup = await dm.load_sect_by_name(name)
    if dup:
        return {"success": False, "message": f"宗门名「{name}」已被使用"}

    if name_reviewer:
        review = await name_reviewer(name)
        allow, reason = _parse_review_result(review)
        if not allow:
            reason = reason or "名称不合规"
            return {"success": False, "message": f"宗门名审核未通过：{reason}"}

    sect_id = _gen_sect_id()
    now = time.time()
    sect = {
        "sect_id": sect_id,
        "name": name,
        "leader_id": player.user_id,
        "description": description,
        "level": 1,
        "spirit_stones": 0,
        "max_members": 30,
        "join_policy": "open",
        "min_realm": 0,
        "created_at": now,
        "announcement": "",
    }
    try:
        create_with_leader = getattr(dm, "create_sect_with_leader", None)
        if callable(create_with_leader):
            await create_with_leader(sect, player.user_id)
        else:
            await dm.save_sect(sect)
            await dm.save_sect_member(player.user_id, sect_id, role="leader")
    except aiosqlite.IntegrityError:
        dup = await dm.load_sect_by_name(name)
        if dup:
            return {"success": False, "message": f"宗门名「{name}」已被使用"}
        existing = await dm.load_player_sect(player.user_id)
        if existing:
            return {"success": False, "message": "你已加入宗门，无法创建新宗门"}
        return {"success": False, "message": "宗门创建失败，请稍后重试"}

    player.spirit_stones -= CREATE_COST

    return {
        "success": True,
        "message": f"宗门「{name}」创建成功！消耗 {CREATE_COST} 灵石",
        "sect_id": sect_id,
    }


async def disband_sect(player: "Player", dm: "DataManager") -> dict:
    """解散宗门（仅宗主，且仅剩自己）。"""
    membership = await dm.load_player_sect(player.user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}
    if membership["role"] != "leader":
        return {"success": False, "message": "只有宗主才能解散宗门"}

    sect_id = membership["sect_id"]
    count = await dm.count_sect_members(sect_id)
    if count > 1:
        return {"success": False, "message": "宗门内还有其他成员，请先移除所有成员或转让宗主后再解散"}

    sect = await dm.load_sect(sect_id)
    sect_name = sect["name"] if sect else "未知"
    # 清理仓库和贡献规则
    await dm.delete_sect_warehouse(sect_id)
    await dm.delete_all_contribution_config(sect_id)
    await dm.delete_sect(sect_id)

    return {"success": True, "message": f"宗门「{sect_name}」已解散"}


# ── 列表 / 详情 ─────────────────────────────────────────

async def get_sect_list(dm: "DataManager", page: int = 1, page_size: int = 10) -> dict:
    """分页获取宗门列表。"""
    data = await dm.load_sects_page(page, page_size)
    for s in data["sects"]:
        s["leader_name"] = ""
    # 批量查宗主名
    leader_ids = [s["leader_id"] for s in data["sects"]]
    if leader_ids:
        placeholders = ",".join(["?"] * len(leader_ids))
        cur = await dm.db.execute(
            f"SELECT user_id, name FROM players WHERE user_id IN ({placeholders})",
            leader_ids,
        )
        rows = await cur.fetchall()
        id_name = {r[0]: r[1] for r in rows}
        for s in data["sects"]:
            s["leader_name"] = id_name.get(s["leader_id"], "未知")
    return data


async def get_sect_detail(sect_id: str, dm: "DataManager") -> dict:
    """宗门详情（含成员列表）。"""
    sect = await dm.load_sect(sect_id)
    if not sect:
        return {"success": False, "message": "宗门不存在"}
    members = await dm.load_sect_members(sect_id)
    for m in members:
        m["realm_name"] = get_realm_name(m.get("realm", 0), m.get("sub_realm", 0))
        m["role_name"] = ROLE_NAMES.get(m["role"], "弟子")
    sect["members"] = members
    sect["member_count"] = len(members)
    return {"success": True, "sect": sect}


async def get_my_sect(user_id: str, dm: "DataManager") -> dict:
    """我的宗门信息 + 成员列表。"""
    membership = await dm.load_player_sect(user_id)
    if not membership:
        return {"success": True, "in_sect": False}
    sect_id = membership["sect_id"]
    sect = await dm.load_sect(sect_id)
    if not sect:
        await dm.delete_sect_member(user_id)
        return {"success": True, "in_sect": False}
    members = await dm.load_sect_members(sect_id)
    for m in members:
        m["realm_name"] = get_realm_name(m.get("realm", 0), m.get("sub_realm", 0))
        m["role_name"] = ROLE_NAMES.get(m["role"], "弟子")
        m["contribution_points"] = m.get("contribution_points", 0)
    contribution = await dm.get_member_contribution(user_id)
    warehouse_slots = await dm.get_sect_warehouse_slot_count(sect_id)
    capacity = sect.get("warehouse_capacity", WAREHOUSE_DEFAULT_CAPACITY)
    return {
        "success": True,
        "in_sect": True,
        "my_role": membership["role"],
        "my_role_name": ROLE_NAMES.get(membership["role"], "弟子"),
        "my_contribution": contribution,
        "sect": {
            **sect,
            "members": members,
            "member_count": len(members),
            "warehouse_slots_used": warehouse_slots,
            "warehouse_capacity": capacity,
        },
    }


# ── 加入 / 退出 ─────────────────────────────────────────

async def join_sect(
    player: "Player",
    sect_id: str,
    dm: "DataManager",
) -> dict:
    """加入宗门。"""
    existing = await dm.load_player_sect(player.user_id)
    if existing:
        return {"success": False, "message": "你已加入宗门，请先退出当前宗门"}

    sect = await dm.load_sect(sect_id)
    if not sect:
        return {"success": False, "message": "宗门不存在"}

    if sect["join_policy"] == "closed":
        return {"success": False, "message": "该宗门暂不接受新成员"}

    if player.realm < sect["min_realm"]:
        min_name = get_realm_name(sect["min_realm"], 0)
        return {"success": False, "message": f"加入该宗门需要至少达到「{min_name}」境界"}

    count = await dm.count_sect_members(sect_id)
    if count >= sect["max_members"]:
        return {"success": False, "message": "宗门成员已满"}

    await dm.save_sect_member(player.user_id, sect_id, role="disciple")
    return {"success": True, "message": f"成功加入「{sect['name']}」"}


async def leave_sect(user_id: str, dm: "DataManager") -> dict:
    """退出宗门。"""
    membership = await dm.load_player_sect(user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}
    if membership["role"] == "leader":
        return {"success": False, "message": "宗主不可直接退出，请先转让宗主或解散宗门"}

    sect = await dm.load_sect(membership["sect_id"])
    sect_name = sect["name"] if sect else "未知"
    await dm.delete_sect_member(user_id)
    return {"success": True, "message": f"已退出「{sect_name}」"}


# ── 管理操作 ─────────────────────────────────────────────

async def kick_member(
    operator_id: str,
    target_id: str,
    dm: "DataManager",
) -> dict:
    """踢出成员。"""
    op_mem = await dm.load_player_sect(operator_id)
    if not op_mem:
        return {"success": False, "message": "你尚未加入任何宗门"}

    tgt_mem = await dm.load_player_sect(target_id)
    if not tgt_mem or tgt_mem["sect_id"] != op_mem["sect_id"]:
        return {"success": False, "message": "目标不在你的宗门中"}

    op_rank = ROLE_RANK.get(op_mem["role"], 3)
    tgt_rank = ROLE_RANK.get(tgt_mem["role"], 3)

    if op_mem["role"] == "leader":
        if tgt_mem["role"] == "leader":
            return {"success": False, "message": "无法踢出自己"}
    elif op_mem["role"] == "vice_leader":
        if tgt_rank <= 1:
            return {"success": False, "message": "副宗主只能踢出长老和弟子以下成员"}
        if tgt_rank <= 2:
            return {"success": False, "message": "副宗主无法踢出长老"}
    else:
        return {"success": False, "message": "你没有踢人权限"}

    # 获取被踢者名字
    cur = await dm.db.execute("SELECT name FROM players WHERE user_id = ?", (target_id,))
    row = await cur.fetchone()
    tgt_name = row[0] if row else "未知"

    await dm.delete_sect_member(target_id)
    return {"success": True, "message": f"已将「{tgt_name}」踢出宗门"}


async def set_member_role(
    operator_id: str,
    target_id: str,
    role: str,
    dm: "DataManager",
) -> dict:
    """设置成员身份。"""
    if role not in ("vice_leader", "elder", "disciple"):
        return {"success": False, "message": "无效的身份"}

    op_mem = await dm.load_player_sect(operator_id)
    if not op_mem:
        return {"success": False, "message": "你尚未加入任何宗门"}

    tgt_mem = await dm.load_player_sect(target_id)
    if not tgt_mem or tgt_mem["sect_id"] != op_mem["sect_id"]:
        return {"success": False, "message": "目标不在你的宗门中"}

    if target_id == operator_id:
        return {"success": False, "message": "不能修改自己的身份"}

    sect_id = op_mem["sect_id"]

    if role == "vice_leader":
        if op_mem["role"] != "leader":
            return {"success": False, "message": "只有宗主才能设置副宗主"}
        count = await dm.count_members_by_role(sect_id, "vice_leader")
        if count >= MAX_VICE_LEADERS:
            return {"success": False, "message": f"副宗主数量已达上限（{MAX_VICE_LEADERS}人）"}
    elif role == "elder":
        if op_mem["role"] not in ("leader", "vice_leader"):
            return {"success": False, "message": "只有宗主或副宗主才能设置长老"}
        count = await dm.count_members_by_role(sect_id, "elder")
        if count >= MAX_ELDERS:
            return {"success": False, "message": f"长老数量已达上限（{MAX_ELDERS}人）"}
    elif role == "disciple":
        if op_mem["role"] not in ("leader", "vice_leader"):
            return {"success": False, "message": "只有宗主或副宗主才能调整身份"}

    # 获取目标名字
    cur = await dm.db.execute("SELECT name FROM players WHERE user_id = ?", (target_id,))
    row = await cur.fetchone()
    tgt_name = row[0] if row else "未知"

    await dm.update_sect_member_role(target_id, role)
    role_name = ROLE_NAMES.get(role, role)
    return {"success": True, "message": f"已将「{tgt_name}」设为{role_name}"}


async def update_sect_info(
    operator_id: str,
    data: dict,
    dm: "DataManager",
) -> dict:
    """修改宗门信息（描述、加入策略、最低境界）。"""
    op_mem = await dm.load_player_sect(operator_id)
    if not op_mem:
        return {"success": False, "message": "你尚未加入任何宗门"}
    if op_mem["role"] not in ("leader", "vice_leader"):
        return {"success": False, "message": "只有宗主或副宗主才能修改宗门信息"}

    update_data = {}
    if "description" in data:
        desc = str(data["description"]).strip()[:DESC_MAX_LEN]
        update_data["description"] = desc
    if "join_policy" in data:
        if data["join_policy"] in ("open", "closed"):
            update_data["join_policy"] = data["join_policy"]
    if "min_realm" in data:
        try:
            mr = int(data["min_realm"])
            if 0 <= mr <= 8:
                update_data["min_realm"] = mr
        except (TypeError, ValueError):
            pass

    if not update_data:
        return {"success": False, "message": "没有有效的修改内容"}

    await dm.update_sect_info(op_mem["sect_id"], update_data)
    return {"success": True, "message": "宗门信息已更新"}


async def transfer_leader(
    leader_id: str,
    target_id: str,
    dm: "DataManager",
) -> dict:
    """转让宗主。"""
    op_mem = await dm.load_player_sect(leader_id)
    if not op_mem:
        return {"success": False, "message": "你尚未加入任何宗门"}
    if op_mem["role"] != "leader":
        return {"success": False, "message": "只有宗主才能转让宗主"}
    if leader_id == target_id:
        return {"success": False, "message": "不能转让给自己"}

    tgt_mem = await dm.load_player_sect(target_id)
    if not tgt_mem or tgt_mem["sect_id"] != op_mem["sect_id"]:
        return {"success": False, "message": "目标不在你的宗门中"}

    sect_id = op_mem["sect_id"]
    if tgt_mem["role"] != "elder":
        elder_count = await dm.count_members_by_role(sect_id, "elder")
        if elder_count >= MAX_ELDERS:
            return {
                "success": False,
                "message": f"当前长老名额已满（{MAX_ELDERS}人），请先调整长老人数后再转让宗主",
            }

    # 获取目标名字
    cur = await dm.db.execute("SELECT name FROM players WHERE user_id = ?", (target_id,))
    row = await cur.fetchone()
    tgt_name = row[0] if row else "未知"

    await dm.update_sect_leader(sect_id, target_id)
    await dm.update_sect_member_role(target_id, "leader")
    await dm.update_sect_member_role(leader_id, "elder")

    return {"success": True, "message": f"已将宗主转让给「{tgt_name}」，你已变为长老"}


# ── 物品品质分类 ─────────────────────────────────────────

def get_item_quality_category(item_id: str) -> str:
    """将物品ID映射到品质分类键（用于匹配贡献点规则）。"""
    # 装备
    eq = EQUIPMENT_REGISTRY.get(item_id)
    if eq:
        return f"equipment_{eq.tier}"

    # 心法秘籍
    hm_id = parse_heart_method_manual_id(item_id)
    if hm_id:
        hm = HEART_METHOD_REGISTRY.get(hm_id)
        if hm:
            return f"heart_method_{hm.quality}"
        return "heart_method_0"

    # 临时心法道具 → 与秘籍同级
    stored_hm_id = parse_stored_heart_method_item_id(item_id)
    if stored_hm_id:
        hm = HEART_METHOD_REGISTRY.get(stored_hm_id)
        if hm:
            return f"heart_method_{hm.quality}"
        return "heart_method_0"

    # 功法卷轴
    gf_id = parse_gongfa_scroll_id(item_id)
    if gf_id:
        gf = GONGFA_REGISTRY.get(gf_id)
        if gf:
            return f"gongfa_{gf.tier}"
        return "gongfa_0"

    # 丹方道具
    recipe_id = parse_pill_recipe_item_id(item_id)
    if recipe_id:
        recipe = PILL_RECIPE_REGISTRY.get(recipe_id)
        if recipe:
            return f"pill_recipe_{recipe.grade}"
        return "pill_recipe_0"

    # 普通物品
    item_def = ITEM_REGISTRY.get(item_id)
    if item_def:
        if item_def.item_type == "material":
            return "material"
        if item_def.item_type == "seed":
            seed = SEED_REGISTRY.get(item_id)
            rarity = seed.rarity if seed else 0
            return f"seed_{rarity}"
        return "consumable"

    return "consumable"


def _get_item_display_name(item_id: str) -> str:
    """获取物品展示名称。"""
    item_def = ITEM_REGISTRY.get(item_id)
    if item_def:
        return item_def.name
    eq = EQUIPMENT_REGISTRY.get(item_id)
    if eq:
        return eq.name
    return item_id


async def get_submit_points(
    sect_id: str, item_id: str, dm: "DataManager",
) -> int:
    """获取上交某物品可获得的贡献点数。
    优先使用宗主设定的品质规则，否则用默认值。
    """
    category = get_item_quality_category(item_id)
    # 查宗门对该品质的上交规则
    configured = await dm.get_contribution_config_by_key(sect_id, "submit", category)
    if configured is not None:
        return configured
    # 默认值
    return DEFAULT_SUBMIT_POINTS.get(category, 5)


async def get_exchange_points(
    sect_id: str, item_id: str, dm: "DataManager",
) -> int:
    """获取兑换某物品所需的贡献点数。
    优先使用宗主为该物品设定的专属规则，再查品质规则，最后用默认值。
    """
    # 1) 物品专属规则
    configured = await dm.get_contribution_config_by_key(sect_id, "exchange_item", item_id)
    if configured is not None:
        return configured
    # 2) 品质通用规则
    category = get_item_quality_category(item_id)
    cat_configured = await dm.get_contribution_config_by_key(sect_id, "exchange", category)
    if cat_configured is not None:
        return cat_configured
    # 3) 默认值
    return DEFAULT_EXCHANGE_POINTS.get(category, 10)


# ── 宗门仓库操作 ─────────────────────────────────────────

async def warehouse_deposit(
    player: "Player",
    item_id: str,
    count: int,
    dm: "DataManager",
    *,
    pre_commit=None,
) -> dict:
    """上交物品到宗门仓库，获取贡献点。"""
    if count <= 0:
        return {"success": False, "message": "数量必须大于0"}

    # 临时心法道具绑定过期时间，禁止流转
    if parse_stored_heart_method_item_id(item_id):
        return {"success": False, "message": "临时心法道具无法上交到仓库"}

    membership = await dm.load_player_sect(player.user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}

    is_leader = membership["role"] == "leader"

    sect_id = membership["sect_id"]

    # 检查玩家是否有足够物品
    held = player.inventory.get(item_id, 0)
    if held < count:
        name = _get_item_display_name(item_id)
        return {"success": False, "message": f"你的「{name}」数量不足（拥有 {held}）"}

    # 计算贡献点（宗主存放不获得贡献点）
    if is_leader:
        total_points = 0
    else:
        points_per = await get_submit_points(sect_id, item_id, dm)
        total_points = points_per * count

    # 完整快照（覆盖 pre_commit 可能修改的所有字段）
    snapshot = copy.deepcopy(player.__dict__)

    # 修改内存
    player.inventory[item_id] = held - count
    if player.inventory[item_id] <= 0:
        del player.inventory[item_id]

    if pre_commit:
        pre_commit(player)

    # 原子事务；异常和失败都回滚内存
    try:
        result = await dm.warehouse_deposit_atomic(
            player, sect_id, item_id, count, total_points,
        )
    except Exception:
        player.__dict__.update(snapshot)
        raise

    if not result["success"]:
        player.__dict__.update(snapshot)
        reason = result.get("reason", "")
        if reason == "warehouse_full":
            return {"success": False, "message": "宗门仓库已满"}
        if reason == "sect_not_found":
            return {"success": False, "message": "宗门不存在"}
        if reason == "member_not_found":
            return {"success": False, "message": "你已不在该宗门中"}
        return {"success": False, "message": "上交失败，请重试"}

    name = _get_item_display_name(item_id)
    if is_leader:
        return {
            "success": True,
            "message": f"成功存入「{name}」x{count}到宗门仓库",
            "item_name": name,
            "count": count,
            "points_earned": 0,
            "contribution": result["contribution"],
        }
    return {
        "success": True,
        "message": f"成功上交「{name}」x{count}，获得 {total_points} 贡献点",
        "item_name": name,
        "count": count,
        "points_earned": total_points,
        "contribution": result["contribution"],
    }


async def warehouse_exchange(
    player: "Player",
    item_id: str,
    count: int,
    dm: "DataManager",
    *,
    pre_commit=None,
) -> dict:
    """从宗门仓库兑换物品，消耗贡献点。
    宗主免费，副宗主三折，长老五折，弟子全价。
    """
    if count <= 0:
        return {"success": False, "message": "数量必须大于0"}

    # 临时心法道具绑定过期时间，禁止流转
    if parse_stored_heart_method_item_id(item_id):
        return {"success": False, "message": "临时心法道具无法从仓库兑换"}

    membership = await dm.load_player_sect(player.user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}

    sect_id = membership["sect_id"]
    role = membership["role"]

    # 计算所需贡献点（含角色折扣）
    points_per = await get_exchange_points(sect_id, item_id, dm)
    discount = ROLE_EXCHANGE_DISCOUNT.get(role, 1.0)
    total_cost = max(0, int(points_per * count * discount))

    # 完整快照（覆盖 pre_commit 可能修改的所有字段）
    snapshot = copy.deepcopy(player.__dict__)

    # 修改内存
    player.inventory[item_id] = player.inventory.get(item_id, 0) + count

    if pre_commit:
        pre_commit(player)

    # 原子事务；异常和失败都回滚内存
    try:
        result = await dm.warehouse_exchange_atomic(
            player, sect_id, item_id, count, total_cost,
        )
    except Exception:
        player.__dict__.update(snapshot)
        raise

    if not result["success"]:
        player.__dict__.update(snapshot)
        reason = result.get("reason", "")
        if reason == "insufficient_contribution":
            current = await dm.get_member_contribution(player.user_id)
            return {
                "success": False,
                "message": f"贡献点不足（需要 {total_cost}，当前 {current}）",
            }
        if reason == "insufficient_stock":
            name = _get_item_display_name(item_id)
            return {"success": False, "message": f"仓库中「{name}」库存不足"}
        return {"success": False, "message": "兑换失败，请重试"}

    name = _get_item_display_name(item_id)
    if total_cost == 0:
        msg = f"成功取出「{name}」x{count}"
    else:
        msg = f"成功兑换「{name}」x{count}，消耗 {total_cost} 贡献点"
    return {
        "success": True,
        "message": msg,
        "item_name": name,
        "count": count,
        "points_spent": total_cost,
        "contribution": result["contribution"],
    }


def _get_item_detail(item_id: str) -> dict:
    """获取物品的完整展示信息（供仓库物品详情弹窗使用）。"""
    from .pills import PILL_REGISTRY, PILL_TIER_NAMES, PILL_GRADE_NAMES

    detail: dict = {"name": _get_item_display_name(item_id)}
    item_def = ITEM_REGISTRY.get(item_id)
    if item_def:
        detail["type"] = item_def.item_type
        detail["description"] = item_def.description
        if item_def.item_type == "seed":
            seed = SEED_REGISTRY.get(item_id)
            if seed:
                harvest_material = MATERIAL_REGISTRY.get(seed.material_id)
                detail["rarity"] = seed.rarity
                detail["rarity_name"] = MATERIAL_RARITY_NAMES.get(seed.rarity, "普通")
                detail["material_id"] = seed.material_id
                detail["material_name"] = harvest_material.name if harvest_material else seed.material_id
                detail["grow_time"] = seed.grow_time

    eq = EQUIPMENT_REGISTRY.get(item_id)
    if eq:
        detail["type"] = "equipment"
        detail["tier"] = eq.tier
        detail["tier_name"] = EQUIPMENT_TIER_NAMES.get(eq.tier, "未知")
        detail["slot"] = eq.slot
        detail["attack"] = eq.attack
        detail["defense"] = eq.defense
        detail["element"] = eq.element
        detail["element_damage"] = eq.element_damage
        return detail

    pill = PILL_REGISTRY.get(item_id)
    if pill:
        detail["type"] = "pill"
        detail["description"] = pill.description
        detail["pill_tier"] = pill.tier
        detail["pill_tier_name"] = PILL_TIER_NAMES.get(pill.tier, "")
        detail["pill_grade"] = pill.grade
        detail["pill_grade_name"] = PILL_GRADE_NAMES.get(pill.grade, "")
        detail["is_temp"] = pill.is_temp
        if pill.is_temp:
            detail["duration"] = pill.duration
        if pill.side_effect_desc:
            detail["side_effect_desc"] = pill.side_effect_desc
        return detail

    hm_id = parse_heart_method_manual_id(item_id)
    if not hm_id:
        hm_id = parse_stored_heart_method_item_id(item_id)
    if hm_id:
        hm = HEART_METHOD_REGISTRY.get(hm_id)
        if hm:
            detail["type"] = "heart_method"
            detail["description"] = hm.description or detail.get("description", "")
            detail["heart_method_quality"] = hm.quality
            detail["hm_quality_name"] = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通")
            detail["realm_name"] = REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界")
            detail["attack_bonus"] = hm.attack_bonus
            detail["defense_bonus"] = hm.defense_bonus
            detail["exp_multiplier"] = hm.exp_multiplier
            detail["dao_yun_rate"] = hm.dao_yun_rate
        return detail

    gf_id = parse_gongfa_scroll_id(item_id)
    if gf_id:
        gf = GONGFA_REGISTRY.get(gf_id)
        if gf:
            detail["type"] = "gongfa"
            detail["description"] = gf.description or detail.get("description", "")
            detail["gongfa_tier"] = gf.tier
            detail["gf_tier_name"] = GONGFA_TIER_NAMES.get(gf.tier, "未知")
            detail["attack_bonus"] = gf.attack_bonus
            detail["defense_bonus"] = gf.defense_bonus
            detail["hp_regen"] = gf.hp_regen
            detail["lingqi_regen"] = gf.lingqi_regen
            detail["lingqi_cost"] = gf.lingqi_cost
        return detail

    recipe_id = parse_pill_recipe_item_id(item_id)
    if recipe_id:
        recipe = PILL_RECIPE_REGISTRY.get(recipe_id)
        if recipe:
            detail["type"] = "pill_recipe"
            pill = PILL_REGISTRY.get(recipe.pill_id)
            pill_name = pill.name if pill else recipe.pill_id
            pill_tier = pill.tier if pill else 0
            detail["recipe_id"] = recipe_id
            detail["pill_id"] = recipe.pill_id
            detail["pill_name"] = pill_name
            detail["pill_tier"] = pill_tier
            detail["pill_tier_name"] = PILL_TIER_NAMES.get(pill_tier, "")
            detail["grade"] = recipe.grade
            detail["grade_name"] = PILL_GRADE_NAMES.get(recipe.grade, "")
            detail["description"] = detail.get("description") or f"炼制{detail['grade_name']}{pill_name}所需配方"
            detail["main_material"] = recipe.main_material.item_id
            detail["main_qty"] = recipe.main_material.qty
            detail["auxiliary_material"] = recipe.auxiliary_material.item_id
            detail["aux_qty"] = recipe.auxiliary_material.qty
        return detail

    return detail


async def warehouse_list(user_id: str, dm: "DataManager") -> dict:
    """查看宗门仓库物品列表及个人贡献信息。"""
    membership = await dm.load_player_sect(user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}

    sect_id = membership["sect_id"]
    role = membership["role"]
    sect = await dm.load_sect(sect_id)
    if not sect:
        return {"success": False, "message": "宗门不存在"}

    items = await dm.get_sect_warehouse(sect_id)
    slots_used = len(items)
    capacity = sect.get("warehouse_capacity", WAREHOUSE_DEFAULT_CAPACITY)
    contribution = await dm.get_member_contribution(user_id)
    discount = ROLE_EXCHANGE_DISCOUNT.get(role, 1.0)

    # 下发原价 + 折扣率，由前端用 floor(base * count * discount) 计算总价
    display_items = []
    for entry in items:
        iid = entry["item_id"]
        base_pts = await get_exchange_points(sect_id, iid, dm)
        detail = _get_item_detail(iid)
        detail.update({
            "item_id": iid,
            "quantity": entry["quantity"],
            "quality_category": get_item_quality_category(iid),
            "quality_name": QUALITY_CATEGORY_NAMES.get(get_item_quality_category(iid), "未知"),
            "exchange_points_base": base_pts,
        })
        display_items.append(detail)

    return {
        "success": True,
        "sect_name": sect["name"],
        "items": display_items,
        "slots_used": slots_used,
        "capacity": capacity,
        "my_contribution": contribution,
        "my_role": role,
        "my_role_name": ROLE_NAMES.get(role, "弟子"),
        "my_discount": discount,
    }


# ── 贡献点规则管理 ───────────────────────────────────────

async def set_submit_rule(
    operator_id: str,
    quality_key: str,
    points: int,
    dm: "DataManager",
) -> dict:
    """宗主设置某品质物品的上交贡献点规则。"""
    if quality_key not in QUALITY_CATEGORY_NAMES:
        return {"success": False, "message": f"无效的品质分类：{quality_key}"}
    if points < 0:
        return {"success": False, "message": "贡献点数不能为负"}

    membership = await dm.load_player_sect(operator_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}
    if membership["role"] != "leader":
        return {"success": False, "message": "只有宗主才能设置贡献点规则"}

    sect_id = membership["sect_id"]
    await dm.set_contribution_config(sect_id, "submit", quality_key, points)

    cat_name = QUALITY_CATEGORY_NAMES[quality_key]
    return {"success": True, "message": f"已设置「{cat_name}」上交可获得 {points} 贡献点"}


async def set_exchange_rule(
    operator_id: str,
    target_key: str,
    points: int,
    dm: "DataManager",
    *,
    is_item: bool = False,
) -> dict:
    """宗主设置兑换规则。
    is_item=False → 按品质设定通用兑换价格
    is_item=True  → 为具体物品设定专属兑换价格
    """
    if points < 0:
        return {"success": False, "message": "贡献点数不能为负"}

    membership = await dm.load_player_sect(operator_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}
    if membership["role"] != "leader":
        return {"success": False, "message": "只有宗主才能设置兑换规则"}

    sect_id = membership["sect_id"]

    if is_item:
        # 验证物品是否存在
        name = _get_item_display_name(target_key)
        if name == target_key and target_key not in ITEM_REGISTRY:
            return {"success": False, "message": f"物品不存在：{target_key}"}
        await dm.set_contribution_config(sect_id, "exchange_item", target_key, points)
        return {"success": True, "message": f"已设置「{name}」兑换需要 {points} 贡献点"}
    else:
        if target_key not in QUALITY_CATEGORY_NAMES:
            return {"success": False, "message": f"无效的品质分类：{target_key}"}
        await dm.set_contribution_config(sect_id, "exchange", target_key, points)
        cat_name = QUALITY_CATEGORY_NAMES[target_key]
        return {"success": True, "message": f"已设置「{cat_name}」兑换需要 {points} 贡献点"}


async def get_contribution_rules(user_id: str, dm: "DataManager") -> dict:
    """获取宗门贡献点规则列表。"""
    membership = await dm.load_player_sect(user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}

    sect_id = membership["sect_id"]
    rules = await dm.get_contribution_config(sect_id)

    # 分类展示
    submit_rules = []
    exchange_quality_rules = []
    exchange_item_rules = []
    for r in rules:
        if r["rule_type"] == "submit":
            submit_rules.append({
                "quality_key": r["target_key"],
                "quality_name": QUALITY_CATEGORY_NAMES.get(r["target_key"], r["target_key"]),
                "points": r["points"],
            })
        elif r["rule_type"] == "exchange":
            exchange_quality_rules.append({
                "quality_key": r["target_key"],
                "quality_name": QUALITY_CATEGORY_NAMES.get(r["target_key"], r["target_key"]),
                "points": r["points"],
            })
        elif r["rule_type"] == "exchange_item":
            exchange_item_rules.append({
                "item_id": r["target_key"],
                "item_name": _get_item_display_name(r["target_key"]),
                "points": r["points"],
            })

    # 合并默认规则展示
    all_submit = []
    for key, name in QUALITY_CATEGORY_NAMES.items():
        custom = next((s for s in submit_rules if s["quality_key"] == key), None)
        pts = custom["points"] if custom else DEFAULT_SUBMIT_POINTS.get(key, 5)
        all_submit.append({
            "quality_key": key,
            "quality_name": name,
            "points": pts,
            "is_custom": custom is not None,
        })

    all_exchange_quality = []
    for key, name in QUALITY_CATEGORY_NAMES.items():
        custom = next((e for e in exchange_quality_rules if e["quality_key"] == key), None)
        pts = custom["points"] if custom else DEFAULT_EXCHANGE_POINTS.get(key, 10)
        all_exchange_quality.append({
            "quality_key": key,
            "quality_name": name,
            "points": pts,
            "is_custom": custom is not None,
        })

    return {
        "success": True,
        "submit_rules": all_submit,
        "exchange_quality_rules": all_exchange_quality,
        "exchange_item_rules": exchange_item_rules,
        "is_leader": membership["role"] == "leader",
    }


# ── 宗门商店 ─────────────────────────────────────────────────────────


import random as _random
from datetime import date as _date


def _sect_shop_weighted_choice(rng, weights: dict):
    keys = list(weights.keys())
    vals = list(weights.values())
    return rng.choices(keys, weights=vals, k=1)[0]


def _sect_shop_build_item(item_id: str, name: str, item_type: str,
                           contrib_cost: int, description: str,
                           extra: dict | None = None) -> dict:
    d = {
        "item_id": item_id,
        "name": name,
        "type": item_type,
        "contrib_cost": contrib_cost,
        "description": description,
    }
    if extra:
        d.update(extra)
    return d


def generate_sect_shop_items(sect_id: str, target_date: _date | None = None) -> list[dict]:
    """生成宗门商店今日商品（每宗门独立种子，贡献点定价）。"""
    from .pills import (
        PILL_REGISTRY, PILL_TIER_NAMES,
        pick_random_pill, SHOP_PILL_TIER_WEIGHTS, SHOP_PILL_GRADE_WEIGHTS,
    )
    d = target_date or _date.today()
    rng = _random.Random(f"sect_shop_{sect_id}_{d.isoformat()}")

    items: list[dict] = []
    seen_ids: set[str] = set()
    max_attempts = SECT_SHOP_ITEM_COUNT * 15
    attempts = 0

    while len(items) < SECT_SHOP_ITEM_COUNT and attempts < max_attempts:
        attempts += 1
        cat = _sect_shop_weighted_choice(rng, SECT_SHOP_TYPE_WEIGHTS)

        if cat == "pill":
            pill = pick_random_pill(rng, SHOP_PILL_TIER_WEIGHTS, SHOP_PILL_GRADE_WEIGHTS)
            if not pill:
                continue
            iid = pill.pill_id
            if iid in seen_ids:
                continue
            contrib = max(10, pill.price // SECT_SHOP_SPIRIT_TO_CONTRIB_RATE)
            tier_name = PILL_TIER_NAMES.get(pill.tier, "")
            grade_name = PILL_GRADE_NAMES.get(pill.grade, "")
            items.append(_sect_shop_build_item(
                item_id=iid, name=pill.name, item_type="pill",
                contrib_cost=contrib, description=pill.description,
                extra={
                    "pill_tier": pill.tier, "pill_tier_name": tier_name,
                    "pill_grade": pill.grade, "pill_grade_name": grade_name,
                    "is_temp": pill.is_temp, "duration": pill.duration,
                    "side_effect_desc": pill.side_effect_desc,
                },
            ))

        elif cat == "equipment":
            from .constants import TIER_REALM_REQUIREMENTS
            tier_weights = {0: 500, 1: 300, 2: 100, 3: 1}
            tier = _sect_shop_weighted_choice(rng, tier_weights)
            candidates = [eq for eq in EQUIPMENT_REGISTRY.values() if eq.tier == tier]
            if not candidates:
                continue
            eq = rng.choice(candidates)
            iid = eq.equip_id
            if iid in seen_ids:
                continue
            base = max(eq.attack + eq.defense, 5)
            contrib = max(20, base * (tier + 1) * 2)
            tier_name = EQUIPMENT_TIER_NAMES.get(eq.tier, "未知")
            items.append(_sect_shop_build_item(
                item_id=iid, name=eq.name, item_type="equipment",
                contrib_cost=contrib, description=eq.description,
                extra={
                    "tier": eq.tier, "tier_name": tier_name,
                    "slot": eq.slot, "attack": eq.attack, "defense": eq.defense,
                    "element": eq.element, "element_damage": eq.element_damage,
                },
            ))

        elif cat == "heart_method":
            quality_weights = {0: 700, 1: 200, 2: 1}
            quality = _sect_shop_weighted_choice(rng, quality_weights)
            from .constants import get_heart_method_manual_id, HEART_METHOD_MANUAL_PREFIX
            candidates = [
                hm for hm in HEART_METHOD_REGISTRY.values()
                if hm.quality == quality
                and get_heart_method_manual_id(hm.method_id) in ITEM_REGISTRY
            ]
            if not candidates:
                continue
            hm = rng.choice(candidates)
            iid = get_heart_method_manual_id(hm.method_id)
            if iid in seen_ids:
                continue
            quality_contrib = {0: 60, 1: 200, 2: 800}
            contrib = quality_contrib.get(quality, 60)
            realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知")
            quality_name = HEART_METHOD_QUALITY_NAMES.get(quality, "普通")
            items.append(_sect_shop_build_item(
                item_id=iid, name=f"{hm.name}秘籍", item_type="heart_method",
                contrib_cost=contrib, description=hm.description,
                extra={
                    "quality": quality, "quality_name": quality_name,
                    "realm_name": realm_name,
                    "attack_bonus": hm.attack_bonus, "defense_bonus": hm.defense_bonus,
                    "exp_multiplier": hm.exp_multiplier,
                },
            ))

        elif cat == "gongfa":
            from .constants import get_gongfa_scroll_id, GONGFA_SCROLL_PREFIX
            gf_tier_weights = {1: 500, 2: 350, 3: 1}
            gf_tier = _sect_shop_weighted_choice(rng, gf_tier_weights)
            candidates = [gf for gf in GONGFA_REGISTRY.values() if gf.tier == gf_tier]
            if not candidates:
                continue
            gf = rng.choice(candidates)
            iid = get_gongfa_scroll_id(gf.gongfa_id)
            if iid in seen_ids:
                continue
            tier_contrib = {1: 50, 2: 120, 3: 400}
            contrib = tier_contrib.get(gf_tier, 50)
            tier_name = GONGFA_TIER_NAMES.get(gf.tier, "未知")
            items.append(_sect_shop_build_item(
                item_id=iid, name=f"{gf.name}卷轴", item_type="gongfa",
                contrib_cost=contrib, description=gf.description,
                extra={
                    "tier": gf.tier, "tier_name": tier_name,
                    "attack_bonus": gf.attack_bonus, "defense_bonus": gf.defense_bonus,
                    "hp_regen": gf.hp_regen, "lingqi_regen": gf.lingqi_regen,
                },
            ))

        elif cat == "material":
            if not MATERIAL_REGISTRY:
                continue
            rarity_weights = SECT_MATERIAL_RARITY_WEIGHTS
            rarity = _sect_shop_weighted_choice(rng, rarity_weights)
            candidates = [m for m in MATERIAL_REGISTRY.values() if m.rarity == rarity]
            if not candidates:
                candidates = list(MATERIAL_REGISTRY.values())
            if not candidates:
                continue
            mat = rng.choice(candidates)
            iid = mat.item_id
            if iid in seen_ids:
                continue
            contrib = max(5, mat.recycle_price // 3)
            rarity_name = MATERIAL_RARITY_NAMES.get(mat.rarity, "未知")
            items.append(_sect_shop_build_item(
                item_id=iid, name=mat.name, item_type="material",
                contrib_cost=contrib, description=mat.description,
                extra={"rarity": mat.rarity, "rarity_name": rarity_name, "category": mat.category},
            ))

        elif cat == "pill_recipe":
            if not PILL_RECIPE_REGISTRY:
                continue
            recipe = rng.choice(list(PILL_RECIPE_REGISTRY.values()))
            iid = get_pill_recipe_item_id(recipe.recipe_id)
            if iid in seen_ids:
                continue
            contrib = SECT_RECIPE_GRADE_CONTRIB.get(recipe.grade, 200)
            pill = PILL_REGISTRY.get(recipe.pill_id)
            pill_name = pill.name if pill else recipe.pill_id
            pill_tier = pill.tier if pill else 0
            grade_name = PILL_GRADE_NAMES.get(recipe.grade, "")
            tier_name = PILL_TIER_NAMES.get(pill_tier, "")
            items.append(_sect_shop_build_item(
                item_id=iid, name=f"{pill_name}丹方", item_type="pill_recipe",
                contrib_cost=contrib,
                description=f"炼制{grade_name}{pill_name}所需配方，学会后可在炼丹阁使用。",
                extra={
                    "recipe_id": recipe.recipe_id, "pill_id": recipe.pill_id,
                    "pill_name": pill_name, "pill_tier": pill_tier,
                    "pill_tier_name": tier_name,
                    "grade": recipe.grade, "grade_name": grade_name,
                },
            ))

        elif cat == "seed":
            if not SEED_REGISTRY:
                continue
            seed_rarity = _sect_shop_weighted_choice(rng, SECT_MATERIAL_RARITY_WEIGHTS)
            candidates = [s for s in SEED_REGISTRY.values() if s.rarity == seed_rarity]
            if not candidates:
                continue
            seed = rng.choice(candidates)
            iid = seed.seed_id
            if iid in seen_ids:
                continue
            rarity_mult = SECT_SEED_RARITY_CONTRIB_MULTIPLIER.get(seed.rarity, 1)
            base_mat = MATERIAL_REGISTRY.get(seed.material_id)
            base_contrib = max(5, (base_mat.recycle_price if base_mat else 10) // 3)
            contrib = base_contrib * rarity_mult
            rarity_name = MATERIAL_RARITY_NAMES.get(seed.rarity, "未知")
            items.append(_sect_shop_build_item(
                item_id=iid, name=seed.name, item_type="seed",
                contrib_cost=contrib, description=seed.description,
                extra={
                    "rarity": seed.rarity, "rarity_name": rarity_name,
                    "category": seed.category, "material_id": seed.material_id,
                    "grow_time": seed.grow_time,
                },
            ))

        if items:
            seen_ids.add(items[-1]["item_id"])

    return items


async def sect_shop_get_items(user_id: str, dm: "DataManager") -> dict:
    """获取宗门商店今日商品列表。"""
    membership = await dm.load_player_sect(user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}
    sect_id = membership["sect_id"]
    contribution = await dm.get_member_contribution(user_id)
    role = membership["role"]
    discount = ROLE_EXCHANGE_DISCOUNT.get(role, 1.0)
    today = _date.today()
    items = generate_sect_shop_items(sect_id, today)
    bought_map = await dm.get_sect_shop_purchases_today(user_id, today.isoformat())
    for it in items:
        it["daily_limit"] = SECT_SHOP_DAILY_LIMIT
        it["bought_today"] = bought_map.get(it["item_id"], 0)
    return {
        "success": True,
        "items": items,
        "date": today.isoformat(),
        "my_contribution": contribution,
        "my_role": role,
        "my_role_name": ROLE_NAMES.get(role, "弟子"),
        "my_discount": discount,
    }


async def sect_shop_buy(
    player: "Player",
    item_id: str,
    quantity: int,
    dm: "DataManager",
) -> dict:
    """从宗门商店购买物品，消耗贡献点。"""
    import copy as _copy
    if quantity <= 0:
        return {"success": False, "message": "数量必须大于0"}

    membership = await dm.load_player_sect(player.user_id)
    if not membership:
        return {"success": False, "message": "你尚未加入任何宗门"}

    sect_id = membership["sect_id"]
    role = membership["role"]
    discount = ROLE_EXCHANGE_DISCOUNT.get(role, 1.0)

    # 找商品
    today = _date.today()
    items = generate_sect_shop_items(sect_id, today)
    shop_item = next((it for it in items if it["item_id"] == item_id), None)
    if not shop_item:
        return {"success": False, "message": "该商品今日不在售"}

    base_cost = shop_item["contrib_cost"]
    total_cost = max(0, int(base_cost * quantity * discount))

    # 检查每日限购
    today_str = today.isoformat()
    bought_map = await dm.get_sect_shop_purchases_today(player.user_id, today_str)
    already_bought = bought_map.get(item_id, 0)
    if already_bought + quantity > SECT_SHOP_DAILY_LIMIT:
        remain = max(0, SECT_SHOP_DAILY_LIMIT - already_bought)
        if remain == 0:
            return {"success": False, "message": f"该商品今日限购已达上限（{SECT_SHOP_DAILY_LIMIT}个）"}
        return {"success": False, "message": f"超出今日限购，最多还可购买 {remain} 个"}

    # 检查贡献点
    current_contrib = await dm.get_member_contribution(player.user_id)
    if total_cost > 0 and current_contrib < total_cost:
        return {"success": False, "message": f"贡献点不足（需 {total_cost}，当前 {current_contrib}）"}

    # 给物品 + 扣贡献 + 记录限购
    snapshot = _copy.deepcopy(player.__dict__)
    player.inventory[item_id] = player.inventory.get(item_id, 0) + quantity
    try:
        result = await dm.sect_shop_buy_atomic(player, total_cost,
                                               item_id=item_id, date_str=today_str,
                                               quantity=quantity)
    except Exception:
        player.__dict__.update(snapshot)
        raise

    if not result["success"]:
        player.__dict__.update(snapshot)
        return {"success": False, "message": result.get("message", "购买失败，请重试")}

    name = shop_item["name"]
    if total_cost == 0:
        msg = f"成功购得「{name}」×{quantity}"
    else:
        msg = f"成功购得「{name}」×{quantity}，消耗 {total_cost} 贡献点"
    return {
        "success": True,
        "message": msg,
        "item_name": name,
        "count": quantity,
        "points_spent": total_cost,
        "contribution": result["contribution"],
    }
