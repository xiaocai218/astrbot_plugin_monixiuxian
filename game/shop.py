"""天机阁 —— 每日刷新 NPC 商店。

用确定性种子 `random.Random(f"shop_{date}")` 每天生成同一组商品，无需缓存。
"""

from __future__ import annotations

import random
from datetime import date
from typing import TYPE_CHECKING

from .constants import (
    EQUIPMENT_REGISTRY,
    EQUIPMENT_TIER_NAMES,
    HEART_METHOD_MANUAL_PREFIX,
    HEART_METHOD_QUALITY_NAMES,
    HEART_METHOD_REGISTRY,
    GONGFA_REGISTRY,
    GONGFA_TIER_NAMES,
    GONGFA_SCROLL_PREFIX,
    ITEM_REGISTRY,
    REALM_CONFIG,
    get_daily_recycle_price,
    get_heart_method_manual_id,
    get_gongfa_scroll_id,
)
from .inventory import add_item

if TYPE_CHECKING:
    from .models import Player

# ── 常量 ──────────────────────────────────────────────────────

SHOP_ITEM_COUNT = 12
SHOP_ITEM_DAILY_LIMIT = 20
SHOP_PRICE_MULTIPLIER = 50

TYPE_WEIGHTS: dict[str, int] = {
    "consumable": 150,
    "pill": 250,
    "equipment": 250,
    "heart_method": 200,
    "gongfa": 150,
}

TIER_WEIGHTS: dict[int, int] = {
    0: 500,   # 凡器
    1: 300,   # 灵器
    2: 100,   # 道器
    3: 1,     # 先天道器 — 千分之一
}

HM_QUALITY_WEIGHTS: dict[int, int] = {
    0: 700,   # 普通
    1: 200,   # 史诗
    2: 1,     # 传说 — 千分之一
}

# 心法按品质分档定价（在回收价×倍率基础上再乘）
HM_QUALITY_PRICE_MULTIPLIER: dict[int, int] = {
    0: 1,     # 普通 — 原价
    1: 5,     # 史诗 — 5倍
    2: 25,    # 传说 — 25倍
}

GONGFA_TIER_WEIGHTS: dict[int, int] = {
    1: 500,   # 玄阶 — 多
    2: 350,   # 地阶
    3: 1,     # 天阶 — 极稀
}

# ── 辅助 ──────────────────────────────────────────────────────


def _weighted_choice(rng: random.Random, weights: dict) -> object:
    """从 {key: weight} 字典中按权重随机选择一个 key。"""
    keys = list(weights.keys())
    vals = [weights[k] for k in keys]
    return rng.choices(keys, weights=vals, k=1)[0]


def _build_item_dict(
    item_id: str,
    name: str,
    item_type: str,
    price: int,
    description: str,
    *,
    extra: dict | None = None,
    daily_limit: int = 0,
) -> dict:
    d = {
        "item_id": item_id,
        "name": name,
        "type": item_type,
        "price": price,
        "description": description,
    }
    if daily_limit:
        d["daily_limit"] = daily_limit
    if extra:
        d.update(extra)
    return d


# ── 每日商品生成 ──────────────────────────────────────────────


def generate_daily_items(target_date: date | None = None) -> list[dict]:
    """生成当天商店商品列表（确定性）。"""
    d = target_date or date.today()
    rng = random.Random(f"shop_{d.isoformat()}")

    items: list[dict] = []
    seen_ids: set[str] = set()

    consumables = [
        it for it in ITEM_REGISTRY.values()
        if it.item_type == "consumable"
        and not it.item_id.startswith(HEART_METHOD_MANUAL_PREFIX)
        and not it.item_id.startswith(GONGFA_SCROLL_PREFIX)
        and not it.item_id.startswith("pill_")
    ]
    heart_manual_ids = {
        get_heart_method_manual_id(hm.method_id)
        for hm in HEART_METHOD_REGISTRY.values()
        if get_heart_method_manual_id(hm.method_id) in ITEM_REGISTRY
    }

    max_attempts = SHOP_ITEM_COUNT * 12
    attempts = 0
    while len(items) < SHOP_ITEM_COUNT and attempts < max_attempts:
        attempts += 1
        cat = _weighted_choice(rng, TYPE_WEIGHTS)

        if cat == "consumable":
            if not consumables:
                continue
            chosen = rng.choice(consumables)
            iid = chosen.item_id
            if iid in seen_ids:
                continue
            price = (get_daily_recycle_price(iid, d) or 5) * SHOP_PRICE_MULTIPLIER
            items.append(_build_item_dict(
                item_id=iid,
                name=chosen.name,
                item_type="consumable",
                price=price,
                description=chosen.description,
                daily_limit=SHOP_ITEM_DAILY_LIMIT,
            ))

        elif cat == "pill":
            from .pills import (
                pick_random_pill, SHOP_PILL_TIER_WEIGHTS, SHOP_PILL_GRADE_WEIGHTS,
                PILL_TIER_NAMES, PILL_GRADE_NAMES,
            )
            pill = pick_random_pill(rng, SHOP_PILL_TIER_WEIGHTS, SHOP_PILL_GRADE_WEIGHTS)
            if not pill:
                continue
            iid = pill.pill_id
            if iid in seen_ids:
                continue
            tier_name = PILL_TIER_NAMES.get(pill.tier, "")
            grade_name = PILL_GRADE_NAMES.get(pill.grade, "")
            items.append(_build_item_dict(
                item_id=iid,
                name=pill.name,
                item_type="pill",
                price=pill.price,
                description=pill.description,
                daily_limit=SHOP_ITEM_DAILY_LIMIT,
                extra={
                    "pill_tier": pill.tier,
                    "pill_tier_name": tier_name,
                    "pill_grade": pill.grade,
                    "pill_grade_name": grade_name,
                    "is_temp": pill.is_temp,
                    "duration": pill.duration,
                    "side_effect_desc": pill.side_effect_desc,
                },
            ))

        elif cat == "equipment":
            tier = _weighted_choice(rng, TIER_WEIGHTS)
            candidates = [eq for eq in EQUIPMENT_REGISTRY.values() if eq.tier == tier]
            if not candidates:
                continue
            eq = rng.choice(candidates)
            iid = eq.equip_id
            if iid in seen_ids:
                continue
            price = (get_daily_recycle_price(iid, d) or 5) * SHOP_PRICE_MULTIPLIER
            tier_name = EQUIPMENT_TIER_NAMES.get(eq.tier, "未知")
            items.append(_build_item_dict(
                item_id=iid,
                name=eq.name,
                item_type="equipment",
                price=price,
                description=eq.description,
                daily_limit=SHOP_ITEM_DAILY_LIMIT,
                extra={
                    "tier": eq.tier,
                    "tier_name": tier_name,
                    "slot": eq.slot,
                    "attack": eq.attack,
                    "defense": eq.defense,
                    "element": eq.element,
                    "element_damage": eq.element_damage,
                },
            ))

        elif cat == "heart_method":
            quality = _weighted_choice(rng, HM_QUALITY_WEIGHTS)
            candidates = [
                hm for hm in HEART_METHOD_REGISTRY.values()
                if hm.quality == quality and get_heart_method_manual_id(hm.method_id) in heart_manual_ids
            ]
            if not candidates:
                continue
            hm = rng.choice(candidates)
            iid = get_heart_method_manual_id(hm.method_id)
            if iid in seen_ids:
                continue
            price = (get_daily_recycle_price(iid, d) or 20) * SHOP_PRICE_MULTIPLIER * HM_QUALITY_PRICE_MULTIPLIER.get(hm.quality, 1)
            quality_name = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通")
            realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知")
            items.append(_build_item_dict(
                item_id=iid,
                name=f"{hm.name}秘籍",
                item_type="heart_method",
                price=price,
                description=hm.description,
                daily_limit=SHOP_ITEM_DAILY_LIMIT,
                extra={
                    "quality": hm.quality,
                    "quality_name": quality_name,
                    "realm": hm.realm,
                    "realm_name": realm_name,
                    "attack_bonus": hm.attack_bonus,
                    "defense_bonus": hm.defense_bonus,
                    "exp_multiplier": hm.exp_multiplier,
                },
            ))

        elif cat == "gongfa":
            gf_tier = _weighted_choice(rng, GONGFA_TIER_WEIGHTS)
            candidates = [gf for gf in GONGFA_REGISTRY.values() if gf.tier == gf_tier]
            if not candidates:
                continue
            gf = rng.choice(candidates)
            iid = get_gongfa_scroll_id(gf.gongfa_id)
            if iid in seen_ids:
                continue
            price = gf.recycle_price * SHOP_PRICE_MULTIPLIER
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
            stat_str = "/".join(parts) if parts else ""
            items.append(_build_item_dict(
                item_id=iid,
                name=f"{gf.name}卷轴",
                item_type="gongfa",
                price=price,
                description=gf.description,
                daily_limit=SHOP_ITEM_DAILY_LIMIT,
                extra={
                    "tier": gf.tier,
                    "tier_name": tier_name,
                    "attack_bonus": gf.attack_bonus,
                    "defense_bonus": gf.defense_bonus,
                    "hp_regen": gf.hp_regen,
                    "lingqi_regen": gf.lingqi_regen,
                    "stat_str": stat_str,
                },
            ))

        seen_ids.add(items[-1]["item_id"] if len(items) > len(seen_ids) else "")

    return items


# ── 购买逻辑 ──────────────────────────────────────────────────


async def buy_from_shop(
    player: Player,
    item_id: str,
    quantity: int,
) -> dict:
    """玩家从天机阁购买商品。"""
    if quantity < 1:
        return {"success": False, "message": "购买数量至少为1"}

    today = date.today().isoformat()
    daily_items = generate_daily_items()
    target = None
    for it in daily_items:
        if it["item_id"] == item_id:
            target = it
            break

    if target is None:
        return {"success": False, "message": "该商品不在今日商店中"}

    total_cost = target["price"] * quantity
    if player.spirit_stones < total_cost:
        return {
            "success": False,
            "message": f"灵石不足，需要{total_cost}灵石（持有{player.spirit_stones}）",
        }

    if item_id not in ITEM_REGISTRY:
        return {"success": False, "message": "该商品尚未完成注册，请稍后再试"}

    daily_limit = target.get("daily_limit", 0)
    player.spirit_stones -= total_cost

    result = await add_item(player, item_id, quantity)
    if not result["success"]:
        player.spirit_stones += total_cost
        return result

    return {
        "success": True,
        "message": f"成功购买{quantity}个【{target['name']}】，花费{total_cost}灵石",
        "item_name": target["name"],
        "quantity": quantity,
        "total_cost": total_cost,
        "_purchase_meta": {
            "item_id": item_id,
            "item_name": target["name"],
            "quantity": quantity,
            "unit_price": target["price"],
            "total_cost": total_cost,
            "purchased_at": today,
            "daily_limit": daily_limit,
        },
    }
