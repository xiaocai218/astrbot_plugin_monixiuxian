"""背包/物品管理逻辑。"""

from __future__ import annotations

from .constants import (
    ITEM_REGISTRY, EQUIPMENT_REGISTRY, EQUIPMENT_TIER_NAMES,
    HEART_METHOD_REGISTRY, HEART_METHOD_QUALITY_NAMES, MASTERY_LEVELS, REALM_CONFIG,
    GONGFA_REGISTRY, GONGFA_TIER_NAMES,
    NON_RECYCLABLE_ITEMS,
    can_equip, get_daily_recycle_price, parse_heart_method_manual_id,
    parse_stored_heart_method_item_id, parse_gongfa_scroll_id, get_player_base_max_lingqi,
)
from .models import Player


async def add_item(player: Player, item_id: str, count: int = 1) -> dict:
    """给玩家添加物品。"""
    if item_id not in ITEM_REGISTRY:
        return {"success": False, "message": "物品不存在"}
    player.inventory[item_id] = player.inventory.get(item_id, 0) + count
    item = ITEM_REGISTRY[item_id]
    return {"success": True, "message": f"获得 {item.name} x{count}"}


async def use_item(player: Player, item_id: str, count: int = 1) -> dict:
    """使用消耗品。

    Returns:
        {"success": bool, "message": str, "effect": dict | None}
    """
    if count < 1:
        return {"success": False, "message": "使用数量至少为1", "effect": None}

    if player.inventory.get(item_id, 0) < count:
        return {"success": False, "message": "物品不足", "effect": None}

    item = ITEM_REGISTRY.get(item_id)
    if not item:
        return {"success": False, "message": "物品不存在", "effect": None}

    if item.item_type == "equipment":
        return {"success": False, "message": "装备请使用【装备】功能", "effect": None}

    if item.item_type not in ("consumable", "heart_method", "gongfa"):
        return {"success": False, "message": "该物品不可使用", "effect": None}

    # 突破丹特殊检查：突破率100%时无法使用（支持新旧丹药）
    if "breakthrough_bonus" in item.effect and "_temp_buff" not in item.effect:
        realm_cfg = REALM_CONFIG.get(player.realm, {})
        base_rate = realm_cfg.get("breakthrough_rate", 0.0)
        accumulated_bonus = getattr(player, 'breakthrough_bonus', 0.0)
        pill_bonus = item.effect.get("breakthrough_bonus", 0.0)
        prepared_pill_bonus = pill_bonus if getattr(player, 'breakthrough_pill_count', 0) > 0 else 0.0
        current_rate = base_rate + accumulated_bonus + prepared_pill_bonus
        if current_rate >= 1.0:
            return {"success": False, "message": "当前突破成功率已达100%，无需服用破境丹", "effect": None}

    # 心法秘籍：使用前先做境界/重复修炼校验，避免误消耗
    if "learn_heart_method" in item.effect:
        method_id = str(item.effect.get("learn_heart_method", ""))
        hm = HEART_METHOD_REGISTRY.get(method_id)
        if not hm:
            return {"success": False, "message": "该心法秘籍数据异常", "effect": None}
        if hm.realm > player.realm:
            realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界")
            return {"success": False, "message": f"【{hm.name}】需达到{realm_name}方可修炼，当前境界不足", "effect": None}
        if player.heart_method == method_id:
            return {"success": False, "message": f"你已在修炼【{hm.name}】", "effect": None}
        old_hm = HEART_METHOD_REGISTRY.get(player.heart_method)
        if old_hm and old_hm.method_id != method_id and player.heart_method_mastery >= 1:
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
                "source_item_id": item_id,
                "message": f"你当前修炼的【{old_hm.name}】已达{mastery_name}，是否转换为心法值？",
                "effect": None,
            }

    # 功法卷轴：使用前校验空槽位和重复装备
    if "learn_gongfa" in item.effect:
        gongfa_id = str(item.effect.get("learn_gongfa", ""))
        gf = GONGFA_REGISTRY.get(gongfa_id)
        if not gf:
            return {"success": False, "message": "该功法卷轴数据异常", "effect": None}
        # 检查是否已装备
        for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
            if getattr(player, slot, "无") == gongfa_id:
                return {"success": False, "message": f"你已装备了【{gf.name}】", "effect": None}
        # 检查空槽位
        has_empty = False
        for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
            slot_value = getattr(player, slot, "无")
            if not slot_value or slot_value == "无":
                has_empty = True
                break
        if not has_empty:
            return {"success": False, "message": "功法槽位已满，请先遗忘一个功法", "effect": None}

    stored_method_id = parse_stored_heart_method_item_id(item_id)
    if stored_method_id:
        player.stored_heart_methods.pop(stored_method_id, None)

    # 批量使用仅支持突破丹类（含新旧）
    is_breakthrough = "breakthrough_bonus" in item.effect and "_temp_buff" not in item.effect
    actual_count = count if is_breakthrough else 1
    player.inventory[item_id] -= actual_count
    if player.inventory[item_id] <= 0:
        del player.inventory[item_id]

    # 临时buff类丹药走buff系统
    if item.effect.get("_temp_buff"):
        from .pills import apply_pill_buff
        effect_msg = apply_pill_buff(player, item_id)
    elif is_breakthrough and actual_count > 1:
        effect_msg = _apply_effect_batch(player, item.effect, actual_count)
    else:
        effect_msg = _apply_effect(player, item.effect)

    return {
        "success": True,
        "message": f"使用了 {actual_count}个{item.name}。{effect_msg}" if actual_count > 1 else f"使用了 {item.name}。{effect_msg}",
        "effect": item.effect,
    }


async def equip_item(player: Player, equip_id: str) -> dict:
    """装备一件物品。从背包移到装备槽，若已有同槽装备则自动卸下。"""
    if player.inventory.get(equip_id, 0) <= 0:
        return {"success": False, "message": "背包中没有该装备"}

    eq = EQUIPMENT_REGISTRY.get(equip_id)
    if not eq:
        return {"success": False, "message": "无效的装备"}

    # 境界限制
    if not can_equip(player.realm, eq.tier):
        tier_name = EQUIPMENT_TIER_NAMES.get(eq.tier, "未知")
        return {"success": False, "message": f"当前境界无法装备{tier_name}·{eq.name}"}

    slot = eq.slot  # "weapon" | "armor"
    current = getattr(player, slot, "无")

    # 从背包移除
    player.inventory[equip_id] -= 1
    if player.inventory[equip_id] <= 0:
        del player.inventory[equip_id]

    # 卸下旧装备到背包
    if current != "无" and current in EQUIPMENT_REGISTRY:
        player.inventory[current] = player.inventory.get(current, 0) + 1
        old_name = EQUIPMENT_REGISTRY[current].name
    else:
        old_name = None

    # 装备新物品
    setattr(player, slot, equip_id)
    tier_name = EQUIPMENT_TIER_NAMES.get(eq.tier, "")
    slot_label = "武器" if slot == "weapon" else "护甲"
    msg = f"成功装备{tier_name}【{eq.name}】为{slot_label}"
    if old_name:
        msg += f"（卸下了{old_name}）"

    # 构建装备详情
    details = {
        "attack": eq.attack,
        "defense": eq.defense,
    }
    if eq.element != "无" and eq.element_damage > 0:
        details["element"] = eq.element
        details["element_damage"] = eq.element_damage

    return {"success": True, "message": msg, "slot": slot, "details": details}


async def unequip_item(player: Player, slot: str) -> dict:
    """卸下指定槽位的装备，放回背包。"""
    if slot not in ("weapon", "armor"):
        return {"success": False, "message": "无效的装备槽位"}

    current = getattr(player, slot, "无")
    if current == "无" or current not in EQUIPMENT_REGISTRY:
        slot_label = "武器" if slot == "weapon" else "护甲"
        return {"success": False, "message": f"当前没有装备{slot_label}"}

    eq = EQUIPMENT_REGISTRY[current]
    player.inventory[current] = player.inventory.get(current, 0) + 1
    setattr(player, slot, "无")
    slot_label = "武器" if slot == "weapon" else "护甲"
    return {"success": True, "message": f"已卸下{slot_label}【{eq.name}】"}


async def recycle_item(player: Player, item_id: str, count: int = 1) -> dict:
    """回收物品获取灵石。"""
    if count < 1:
        return {"success": False, "message": "回收数量至少为1"}

    if item_id in NON_RECYCLABLE_ITEMS:
        return {"success": False, "message": "该物品不可回收"}

    item = ITEM_REGISTRY.get(item_id)
    if not item:
        return {"success": False, "message": "物品不存在"}

    owned = player.inventory.get(item_id, 0)
    if owned < count:
        return {"success": False, "message": f"背包中只有{owned}个【{item.name}】，不足{count}个"}

    unit_price = get_daily_recycle_price(item_id)
    if unit_price is None:
        return {"success": False, "message": "该物品无法回收"}

    earned = unit_price * count
    player.inventory[item_id] -= count
    if player.inventory[item_id] <= 0:
        del player.inventory[item_id]
    player.spirit_stones = player.spirit_stones + earned

    return {
        "success": True,
        "message": f"成功回收{count}个【{item.name}】，获得{earned}灵石（单价{unit_price}灵石）",
        "earned": earned,
        "unit_price": unit_price,
        "item_name": item.name,
        "count": count,
    }


def _apply_effect(player: Player, effect: dict) -> str:
    """应用物品效果到玩家，返回描述文本。"""
    messages = []
    if "heal_full" in effect:
        player.hp = player.max_hp
        messages.append("生命值完全恢复")
    if "heal_hp" in effect:
        heal = effect["heal_hp"]
        player.hp = min(player.hp + heal, player.max_hp)
        messages.append(f"恢复{heal}点生命")
    if "exp_bonus" in effect:
        bonus = effect["exp_bonus"]
        player.exp += bonus
        messages.append(f"获得{bonus}点经验")
    if "attack_boost" in effect and "_temp_buff" not in effect:
        boost = effect["attack_boost"]
        player.permanent_attack_bonus = getattr(player, "permanent_attack_bonus", 0) + boost
        player.attack += boost
        messages.append(f"攻击力永久增加{boost}")
    if "defense_boost" in effect and "_temp_buff" not in effect:
        boost = effect["defense_boost"]
        player.permanent_defense_bonus = getattr(player, "permanent_defense_bonus", 0) + boost
        player.defense += boost
        messages.append(f"防御力永久增加{boost}")
    if "lingqi_boost" in effect and "_temp_buff" not in effect:
        boost = effect["lingqi_boost"]
        player.permanent_lingqi_bonus = getattr(player, "permanent_lingqi_bonus", 0) + boost
        player.lingqi = min(get_player_base_max_lingqi(player), player.lingqi + boost)
        messages.append(f"灵气上限永久增加{boost}")
    if "dao_yun_boost" in effect:
        boost = effect["dao_yun_boost"]
        player.dao_yun += boost
        messages.append(f"道韵永久增加{boost}")
    if "max_hp_boost" in effect and "_temp_buff" not in effect:
        boost = effect["max_hp_boost"]
        player.permanent_max_hp_bonus = getattr(player, "permanent_max_hp_bonus", 0) + boost
        player.max_hp += boost
        player.hp = min(player.hp + boost, player.max_hp)
        messages.append(f"生命上限永久增加{boost}")
    if "clear_debuffs" in effect:
        active_buffs = getattr(player, 'active_buffs', []) or []
        cleared = sum(1 for buff in active_buffs if buff.get("side_effects"))
        for buff in active_buffs:
            if buff.get("side_effects"):
                buff["side_effects"] = {}
        if cleared:
            messages.append(f"清除了{cleared}个丹药副作用")
        else:
            messages.append("当前无可清除的丹药副作用")
    if "learn_heart_method" in effect:
        method_id = str(effect["learn_heart_method"])
        hm = HEART_METHOD_REGISTRY.get(method_id)
        if hm:
            old_hm = HEART_METHOD_REGISTRY.get(player.heart_method)
            old_name = old_hm.name if old_hm else ""
            old_mastery = player.heart_method_mastery
            old_exp = player.heart_method_exp

            convert_points = 0
            if old_hm and old_hm.method_id != hm.method_id:
                convert_points = _calc_heart_method_convert_points(old_hm, old_mastery, old_exp, hm)

            player.heart_method = method_id
            player.heart_method_mastery = 0
            player.heart_method_exp = convert_points
            player.heart_method_value = max(0, int(getattr(player, "heart_method_value", 0)))

            absorbed_value = 0
            if hm.realm == player.realm and player.heart_method_value > 0:
                absorb_cap = max(1, int(hm.mastery_exp * 0.6))
                absorbed_value = min(player.heart_method_value, absorb_cap)
                player.heart_method_exp = min(hm.mastery_exp - 1, player.heart_method_exp + absorbed_value)
                player.heart_method_value -= absorbed_value

            quality = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "")
            if old_name and old_name != hm.name:
                messages.append(f"领悟{quality}心法【{hm.name}】（重置并替换【{old_name}】进度）")
                if convert_points > 0:
                    cap = max(1, int(hm.mastery_exp * 0.4))
                    messages.append(
                        f"化功成功：转化{convert_points}心法值（上限{cap}），"
                        f"当前进度{convert_points}/{hm.mastery_exp}"
                    )
                elif old_mastery >= 2:
                    messages.append("化功后未获得可用心法值")
                else:
                    old_mastery_name = MASTERY_LEVELS[min(old_mastery, len(MASTERY_LEVELS) - 1)]
                    messages.append(f"原心法仅{old_mastery_name}，未达大成，无法转化心法值")
            else:
                messages.append(f"领悟{quality}心法【{hm.name}】（入门）")
            if absorbed_value > 0:
                messages.append(
                    f"吸收预存心法值{absorbed_value}，当前进度{player.heart_method_exp}/{hm.mastery_exp}"
                    f"（剩余心法值{player.heart_method_value}）"
                )
    if "breakthrough_bonus" in effect:
        bonus = effect["breakthrough_bonus"]
        player.breakthrough_pill_count = getattr(player, 'breakthrough_pill_count', 0) + 1
        messages.append(f"服用破境丹，下次突破成功率+{int(bonus * 100)}%（已备{player.breakthrough_pill_count}颗）")
    if "learn_gongfa" in effect:
        gongfa_id = str(effect["learn_gongfa"])
        gf = GONGFA_REGISTRY.get(gongfa_id)
        if gf:
            # 找到第一个空槽位装入
            placed = False
            for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
                slot_value = getattr(player, slot, "无")
                if not slot_value or slot_value == "无":
                    setattr(player, slot, gongfa_id)
                    setattr(player, f"{slot}_mastery", 0)
                    setattr(player, f"{slot}_exp", 0)
                    tier_name = GONGFA_TIER_NAMES.get(gf.tier, "未知")
                    messages.append(f"习得{tier_name}功法【{gf.name}】（入门）")
                    placed = True
                    break
            if not placed:
                messages.append("功法槽位已满")
    return "，".join(messages) if messages else ""


def _apply_effect_batch(player: Player, effect: dict, count: int) -> str:
    """批量应用物品效果（仅支持突破丹）。"""
    messages = []
    if "breakthrough_bonus" in effect:
        bonus = effect["breakthrough_bonus"]
        player.breakthrough_pill_count = getattr(player, 'breakthrough_pill_count', 0) + count
        messages.append(f"服用{count}颗破境丹，下次突破成功率+{int(bonus * 100)}%（已备{player.breakthrough_pill_count}颗）")
    return "，".join(messages) if messages else ""


def _calc_heart_method_convert_points(old_hm, old_mastery: int, old_exp: int, new_hm) -> int:
    """将旧心法转为心法值，用于新心法进度。仅大成(2)及以上可转。"""
    if old_mastery < 2:
        return 0

    # 品质越高转化越多；圆满高于大成
    quality_factor = {0: 1.0, 1: 1.35, 2: 1.75}.get(int(getattr(old_hm, "quality", 0)), 1.0)
    mastery_factor = 1.0 if old_mastery >= 3 else 0.75
    progress_ratio = min(1.0, max(0.0, old_exp / max(1, int(getattr(old_hm, "mastery_exp", 1)))))

    base_points = int(getattr(old_hm, "mastery_exp", 0) * quality_factor * mastery_factor)
    progress_bonus = int(getattr(old_hm, "mastery_exp", 0) * progress_ratio * (0.15 + 0.1 * int(getattr(old_hm, "quality", 0))))
    converted = max(0, base_points + progress_bonus)

    # 转化上限：新心法首阶段 40%
    cap = max(1, int(getattr(new_hm, "mastery_exp", 1) * 0.4))
    return min(converted, cap)


async def get_inventory_display(player: Player) -> list[dict]:
    """获取背包展示数据（含回收价格信息）。"""
    items = get_inventory_display_sync(player)
    equipped = {player.weapon, player.armor}
    for entry in items:
        iid = entry["item_id"]
        price = get_daily_recycle_price(iid)
        entry["recycle_price"] = price
        entry["recyclable"] = price is not None and iid not in NON_RECYCLABLE_ITEMS
        entry["is_equipped"] = iid in equipped
    return items


def get_inventory_display_sync(player: Player) -> list[dict]:
    """获取背包展示数据（同步版，供管理员详情使用）。"""
    from .pills import PILL_REGISTRY, PILL_TIER_NAMES, PILL_GRADE_NAMES
    result = []
    for item_id, count in player.inventory.items():
        item = ITEM_REGISTRY.get(item_id)
        if not item:
            continue
        entry = {
            "item_id": item_id,
            "name": item.name,
            "count": count,
            "type": item.item_type,
            "description": item.description,
        }
        # 装备类物品附加详情
        eq = EQUIPMENT_REGISTRY.get(item_id)
        if eq:
            entry["tier"] = eq.tier
            entry["tier_name"] = EQUIPMENT_TIER_NAMES.get(eq.tier, "未知")
            entry["slot"] = eq.slot
            entry["attack"] = eq.attack
            entry["defense"] = eq.defense
            entry["element"] = eq.element
            entry["element_damage"] = eq.element_damage
        # 丹药品阶详情
        pill = PILL_REGISTRY.get(item_id)
        if pill:
            entry["pill_tier"] = pill.tier
            entry["pill_tier_name"] = PILL_TIER_NAMES.get(pill.tier, "")
            entry["pill_grade"] = pill.grade
            entry["pill_grade_name"] = PILL_GRADE_NAMES.get(pill.grade, "")
            entry["is_temp"] = pill.is_temp
            if pill.is_temp:
                entry["duration"] = pill.duration
            if pill.side_effect_desc:
                entry["side_effect_desc"] = pill.side_effect_desc
        # 心法秘籍详情
        hm_id = parse_heart_method_manual_id(item_id)
        if not hm_id:
            hm_id = parse_stored_heart_method_item_id(item_id)
        if hm_id:
            hm = HEART_METHOD_REGISTRY.get(hm_id)
            if hm:
                entry["heart_method_quality"] = hm.quality
                entry["quality_name"] = HEART_METHOD_QUALITY_NAMES.get(hm.quality, "普通")
                realm_name = REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界")
                entry["realm_name"] = realm_name
                entry["attack_bonus"] = hm.attack_bonus
                entry["defense_bonus"] = hm.defense_bonus
                entry["exp_multiplier"] = hm.exp_multiplier
                entry["dao_yun_rate"] = hm.dao_yun_rate
        # 功法卷轴详情
        gf_id = parse_gongfa_scroll_id(item_id)
        if gf_id:
            gf = GONGFA_REGISTRY.get(gf_id)
            if gf:
                entry["gongfa_tier"] = gf.tier
                entry["tier_name"] = GONGFA_TIER_NAMES.get(gf.tier, "未知")
                entry["attack_bonus"] = gf.attack_bonus
                entry["defense_bonus"] = gf.defense_bonus
                entry["hp_regen"] = gf.hp_regen
                entry["lingqi_regen"] = gf.lingqi_regen
        result.append(entry)
    return result


def find_item_id_by_name(name: str) -> str | None:
    """根据物品名查找 item_id（含装备）。"""
    matches = find_item_ids_by_name(name)
    return matches[0] if matches else None


def find_item_ids_by_name(name: str, query_type: str | None = None) -> list[str]:
    """根据物品名查找所有匹配 item_id，可选按类型过滤。"""
    target = str(name or "").strip()
    qtype = str(query_type or "").strip()
    if not target:
        return []

    def _type_match(item_id: str, item_type: str) -> bool:
        if not qtype:
            return True
        if qtype == "equipment":
            return item_type == "equipment" or item_id in EQUIPMENT_REGISTRY
        if qtype == "heart_method":
            return (
                item_type == "heart_method"
                or parse_heart_method_manual_id(item_id) is not None
                or parse_stored_heart_method_item_id(item_id) is not None
            )
        if qtype == "gongfa":
            return (
                item_type == "gongfa"
                or item_id.startswith("gongfa_scroll_")
            )
        if qtype == "item":
            return (
                item_type not in ("equipment", "heart_method", "gongfa")
                and parse_heart_method_manual_id(item_id) is None
                and parse_stored_heart_method_item_id(item_id) is None
                and not item_id.startswith("gongfa_scroll_")
            )
        return False

    result: list[str] = []
    for item_id, item in ITEM_REGISTRY.items():
        if item.name != target:
            continue
        if not _type_match(item_id, str(getattr(item, "item_type", ""))):
            continue
        result.append(item_id)

    # 兼容：若装备注册表存在但物品注册表未刷新，仍可命中装备
    if not qtype or qtype == "equipment":
        for equip_id, eq in EQUIPMENT_REGISTRY.items():
            if eq.name == target and equip_id not in result:
                result.append(equip_id)

    return result
