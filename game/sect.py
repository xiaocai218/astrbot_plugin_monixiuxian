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
    HEART_METHOD_REGISTRY,
    GONGFA_REGISTRY,
    EQUIPMENT_TIER_NAMES,
    HEART_METHOD_QUALITY_NAMES,
    GONGFA_TIER_NAMES,
    parse_heart_method_manual_id,
    parse_stored_heart_method_item_id,
    parse_gongfa_scroll_id,
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

    # 普通物品
    item_def = ITEM_REGISTRY.get(item_id)
    if item_def:
        if item_def.item_type == "material":
            return "material"
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

    if membership["role"] == "leader":
        return {"success": False, "message": "宗主无需上交物品"}

    sect_id = membership["sect_id"]

    # 检查玩家是否有足够物品
    held = player.inventory.get(item_id, 0)
    if held < count:
        name = _get_item_display_name(item_id)
        return {"success": False, "message": f"你的「{name}」数量不足（拥有 {held}）"}

    # 计算贡献点
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
        display_items.append({
            "item_id": iid,
            "name": _get_item_display_name(iid),
            "quantity": entry["quantity"],
            "quality_category": get_item_quality_category(iid),
            "quality_name": QUALITY_CATEGORY_NAMES.get(get_item_quality_category(iid), "未知"),
            "exchange_points_base": base_pts,
        })

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
