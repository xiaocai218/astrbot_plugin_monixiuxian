"""灵田系统：种植、收获材料。"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

from .constants import (
    MATERIAL_REGISTRY,
    MATERIAL_RARITY_NAMES,
    MaterialRarity,
    SEED_REGISTRY,
)

if TYPE_CHECKING:
    from .data_manager import DataManager
    from .models import Player


FIELD_PLOTS_BASIC = 6

# 灵田等级 -> 允许的最大材料稀有度
FIELD_LEVEL_MAX_RARITY = {
    1: MaterialRarity.COMMON,
    2: MaterialRarity.RARE,
    3: MaterialRarity.PRECIOUS,
}


async def get_field_status(user_id: str, dm: "DataManager") -> dict:
    """获取玩家灵田状态（含所有格子）。"""
    field = await dm.get_spirit_field(user_id)
    if not field:
        return {"success": True, "has_field": False}

    plots = await dm.get_spirit_field_plots(field["field_id"])
    now = time.time()
    plot_list = []
    for p in plots:
        info: dict = {
            "plot_index": p["plot_index"],
            "state": p["state"],  # empty / growing / ready
        }
        if p["state"] != "empty":
            mat = MATERIAL_REGISTRY.get(p["material_id"])
            info["material_id"] = p["material_id"]
            info["material_name"] = mat.name if mat else p["material_id"]
            info["planted_at"] = p["planted_at"]
            info["grow_time"] = p["grow_time"]
            elapsed = now - p["planted_at"]
            remaining = max(0, p["grow_time"] - elapsed)
            progress = min(1.0, elapsed / p["grow_time"]) if p["grow_time"] > 0 else 1.0
            if remaining <= 0:
                info["state"] = "ready"
            info["remaining"] = round(remaining)
            info["progress"] = round(progress, 4)
        plot_list.append(info)

    return {
        "success": True,
        "has_field": True,
        "field_id": field["field_id"],
        "field_level": field["field_level"],
        "plots": plot_list,
    }


async def claim_field(user_id: str, dm: "DataManager") -> dict:
    """玩家领取灵田。"""
    existing = await dm.get_spirit_field(user_id)
    if existing:
        return {"success": False, "message": "你已经拥有灵田了"}

    member = await dm.load_player_sect(user_id)
    if not member:
        return {"success": False, "message": "你还没有加入宗门，无法领取灵田"}

    import aiosqlite

    try:
        field_id = await dm.create_spirit_field(user_id, level=1, plots=FIELD_PLOTS_BASIC)
    except aiosqlite.IntegrityError:
        return {"success": False, "message": "你已经拥有灵田了"}
    return {
        "success": True,
        "message": "成功领取初级灵田！",
        "field_id": field_id,
    }


async def get_plantable_seeds(player: "Player", field_level: int, dm: "DataManager") -> dict:
    """获取玩家当前背包中可种植的种子列表。"""
    max_rarity = FIELD_LEVEL_MAX_RARITY.get(field_level, MaterialRarity.COMMON)

    inventory = _get_inventory(player)
    plantable = []
    for item_id, count in inventory.items():
        if not isinstance(count, int) or count <= 0:
            continue
        seed = SEED_REGISTRY.get(item_id)
        if not seed:
            continue
        if seed.rarity > max_rarity:
            continue
        material = MATERIAL_REGISTRY.get(seed.material_id)
        if not material:
            continue
        plantable.append({
            "seed_id": seed.seed_id,
            "material_id": seed.material_id,
            "name": seed.name,
            "material_name": material.name,
            "rarity": seed.rarity,
            "rarity_name": MATERIAL_RARITY_NAMES.get(seed.rarity, "未知"),
            "category": seed.category,
            "count": count,
            "grow_time": seed.grow_time,
            "description": seed.description,
        })

    plantable.sort(key=lambda item: (item["rarity"], item["category"], item["name"]))
    return {"success": True, "seeds": plantable}


async def plant_seed(
    player: "Player", plot_index: int, seed_id: str, dm: "DataManager"
) -> dict:
    """在指定格子种植种子，消耗背包中 1 个对应种子。"""
    field = await dm.get_spirit_field(player.user_id)
    if not field:
        return {"success": False, "message": "你还没有灵田，请先领取"}

    seed = SEED_REGISTRY.get(seed_id)
    if not seed:
        return {"success": False, "message": "无效的种子"}

    material = MATERIAL_REGISTRY.get(seed.material_id)
    if not material:
        return {"success": False, "message": "该种子未绑定有效产物"}

    max_rarity = FIELD_LEVEL_MAX_RARITY.get(field["field_level"], MaterialRarity.COMMON)
    if material.rarity > max_rarity:
        rarity_name = MATERIAL_RARITY_NAMES.get(max_rarity, "")
        return {"success": False, "message": f"当前灵田等级只能种植{rarity_name}及以下品阶的种子"}

    plots = await dm.get_spirit_field_plots(field["field_id"])
    plot = None
    for p in plots:
        if p["plot_index"] == plot_index:
            plot = p
            break
    if plot is None:
        return {"success": False, "message": "无效的格子编号"}
    if plot["state"] != "empty":
        return {"success": False, "message": "该格子已经有作物了"}

    inventory = _get_inventory(player)
    current = inventory.get(seed_id, 0)
    if not isinstance(current, int) or current <= 0:
        return {"success": False, "message": f"背包中没有「{seed.name}」"}

    inventory[seed_id] = current - 1
    if inventory[seed_id] <= 0:
        del inventory[seed_id]
    player.inventory = inventory

    now = time.time()
    await dm.plant_spirit_field(field["field_id"], plot_index, seed.material_id, now, seed.grow_time)
    await dm.save_player(player)

    return {
        "success": True,
        "message": f"成功种下「{seed.name}」，预计 {_format_time(seed.grow_time)} 后可收获「{material.name}」",
        "plot_index": plot_index,
        "seed_id": seed.seed_id,
        "material_id": seed.material_id,
        "material_name": material.name,
        "planted_at": now,
        "grow_time": seed.grow_time,
    }


async def harvest_plot(
    player: "Player", plot_index: int, dm: "DataManager"
) -> dict:
    """收获指定格子的成熟作物，存入灵田仓库。"""
    field = await dm.get_spirit_field(player.user_id)
    if not field:
        return {"success": False, "message": "你还没有灵田"}

    plots = await dm.get_spirit_field_plots(field["field_id"])
    plot = None
    for p in plots:
        if p["plot_index"] == plot_index:
            plot = p
            break
    if plot is None:
        return {"success": False, "message": "无效的格子编号"}
    if plot["state"] == "empty":
        return {"success": False, "message": "该格子是空的"}

    now = time.time()
    elapsed = now - plot["planted_at"]
    if elapsed < plot["grow_time"]:
        remaining = plot["grow_time"] - elapsed
        return {"success": False, "message": f"还需 {_format_time(remaining)} 才能收获"}

    material = MATERIAL_REGISTRY.get(plot["material_id"])
    material_name = material.name if material else plot["material_id"]

    yield_count = 1
    await dm.harvest_spirit_field_plot(field["field_id"], plot_index)
    await dm.add_to_field_warehouse(player.user_id, field["field_id"], plot["material_id"], yield_count)

    return {
        "success": True,
        "message": f"收获了 {yield_count} 个「{material_name}」，已存入灵田仓库",
        "material_id": plot["material_id"],
        "material_name": material_name,
        "count": yield_count,
    }


async def get_field_warehouse(
    user_id: str, dm: "DataManager",
    filter_rarity: int = -1, search: str = ""
) -> dict:
    """获取灵田仓库内容。"""
    field = await dm.get_spirit_field(user_id)
    if not field:
        return {"success": False, "message": "你还没有灵田"}

    items = await dm.get_field_warehouse_items(user_id)
    result = []
    for row in items:
        material = MATERIAL_REGISTRY.get(row["material_id"])
        if not material:
            continue
        if filter_rarity >= 0 and material.rarity != filter_rarity:
            continue
        if search and search not in material.name:
            continue
        result.append({
            "material_id": row["material_id"],
            "name": material.name,
            "rarity": material.rarity,
            "rarity_name": MATERIAL_RARITY_NAMES.get(material.rarity, "未知"),
            "category": material.category,
            "count": row["count"],
            "description": material.description,
        })

    return {"success": True, "items": result}


async def warehouse_withdraw(
    player: "Player", material_id: str, count: int, dm: "DataManager"
) -> dict:
    """从灵田仓库取出材料到背包。"""
    field = await dm.get_spirit_field(player.user_id)
    if not field:
        return {"success": False, "message": "你还没有灵田"}

    material = MATERIAL_REGISTRY.get(material_id)
    if not material:
        return {"success": False, "message": "无效的材料"}

    if count < 1:
        return {"success": False, "message": "数量必须大于0"}

    warehouse_count = await dm.get_field_warehouse_count(player.user_id, material_id)
    if warehouse_count < count:
        return {"success": False, "message": f"仓库中「{material.name}」不足，当前 {warehouse_count} 个"}

    await dm.remove_from_field_warehouse(player.user_id, material_id, count)

    inventory = _get_inventory(player)
    inventory[material_id] = inventory.get(material_id, 0) + count
    player.inventory = inventory
    await dm.save_player(player)

    return {
        "success": True,
        "message": f"从灵田仓库取出 {count} 个「{material.name}」",
        "material_id": material_id,
        "material_name": material.name,
        "count": count,
    }


def _get_inventory(player: "Player") -> dict:
    """读取玩家背包字典。"""
    import json

    inventory = player.inventory if isinstance(player.inventory, dict) else json.loads(player.inventory or "{}")
    return inventory if isinstance(inventory, dict) else {}


def _format_time(seconds: float) -> str:
    """格式化秒数为可读时间。"""
    total = int(seconds)
    if total < 60:
        return f"{total}秒"
    if total < 3600:
        return f"{total // 60}分{total % 60}秒"
    hours = total // 3600
    minutes = (total % 3600) // 60
    return f"{hours}小时{minutes}分"
