"""宗门系统核心逻辑（纯函数 / 异步函数）。"""
from __future__ import annotations

import secrets
import time
from typing import TYPE_CHECKING

import aiosqlite

from .constants import get_realm_name

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
    return {
        "success": True,
        "in_sect": True,
        "my_role": membership["role"],
        "my_role_name": ROLE_NAMES.get(membership["role"], "弟子"),
        "sect": {**sect, "members": members, "member_count": len(members)},
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
