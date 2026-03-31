"""历练系统：场景匹配、攻防对抗、奖惩逻辑。"""

from __future__ import annotations

import random

from .constants import (
    REALM_CONFIG, ITEM_REGISTRY, CHECKIN_PILL_WEIGHTS,
    HEART_METHOD_REGISTRY, HeartMethodQuality, get_heart_method_manual_id,
    EQUIPMENT_REGISTRY, EQUIPMENT_TIER_NAMES, EquipmentTier,
    GONGFA_REGISTRY, GONGFA_TIER_NAMES, GongfaTier, MASTERY_LEVELS, MASTERY_MAX,
    MATERIAL_REGISTRY, MATERIAL_RARITY_NAMES,
    get_gongfa_scroll_id, get_total_gongfa_bonus, can_cultivate_gongfa,
    get_player_base_max_lingqi, get_realm_base_stats,
    RealmLevel, has_sub_realm,
    get_realm_name, get_equip_bonus, get_heart_method_bonus,
    get_max_sub_realm, get_nearest_realm_level, get_next_realm_level,
    get_previous_realm_level,
)
from .inventory import add_item
from .models import Player

_REWARD_TYPES = ("stones", "exp", "pill", "equip", "material")
_COMBO_SIZES = (4, 3, 2, 1)  # 1+1+1+1, 1+1+1, 1+1, 1
_HEART_METHOD_DROP_BASE_RATE = 0.18



def _resolve_enemy_realm(player_realm: int, difficulty: str) -> int:
    """根据难度匹配敌方境界。"""
    current = get_nearest_realm_level(player_realm)
    min_enemy_realm = get_next_realm_level(RealmLevel.MORTAL)
    if min_enemy_realm is not None and current < min_enemy_realm:
        current = min_enemy_realm
    if difficulty == "easy":
        prev_realm = get_previous_realm_level(current)
        if prev_realm is not None and (min_enemy_realm is None or prev_realm >= min_enemy_realm):
            return prev_realm
        return current
    if difficulty == "hard":
        return get_next_realm_level(current) or current
    return current


def _build_battle_context(player: Player, enemy_realm: int, difficulty: str) -> dict:
    """构建战斗上下文，输出胜率和敌方属性。"""
    from .pills import get_effective_combat_stats

    effective_stats = get_effective_combat_stats(player)
    equip_bonus = get_equip_bonus(player.weapon, player.armor)
    hm_bonus = get_heart_method_bonus(player.heart_method, player.heart_method_mastery)
    gf_bonus = get_total_gongfa_bonus(player)
    player_atk = max(1, effective_stats["attack"] + equip_bonus["attack"] + hm_bonus["attack_bonus"] + gf_bonus["attack_bonus"])
    player_def = max(1, effective_stats["defense"] + equip_bonus["defense"] + hm_bonus["defense_bonus"] + gf_bonus["defense_bonus"])

    fallback_realm = get_nearest_realm_level(RealmLevel.QI_REFINING)
    enemy_cfg = REALM_CONFIG.get(enemy_realm) or REALM_CONFIG.get(fallback_realm, {})
    diff_scale = {"easy": 0.9, "normal": 1.0, "hard": 1.12}.get(difficulty, 1.0)
    jitter = random.uniform(0.92, 1.08)
    enemy_scale = diff_scale * jitter

    enemy_attack = max(1, int(enemy_cfg["base_attack"] * enemy_scale))
    enemy_defense = max(1, int(enemy_cfg["base_defense"] * enemy_scale))
    enemy_hp = max(1, int(enemy_cfg["base_hp"] * (0.9 + (enemy_scale - 1.0) * 0.6)))

    player_power = player_atk * 1.25 + player_def * 0.95 + effective_stats["hp"] * 0.08
    enemy_power = enemy_attack * 1.2 + enemy_defense * 1.0 + enemy_hp * 0.08
    ratio = player_power / max(1.0, enemy_power)

    # 将战力比映射到 [8%, 92%] 的胜率区间
    win_prob = 0.5 + (ratio - 1.0) * 0.7
    win_prob = max(0.08, min(0.92, win_prob))

    return {
        "player_attack": player_atk,
        "player_defense": player_def,
        "enemy_attack": enemy_attack,
        "enemy_defense": enemy_defense,
        "enemy_hp": enemy_hp,
        "enemy_scale": enemy_scale,
        "player_power": int(player_power),
        "enemy_power": int(enemy_power),
        "power_ratio": ratio,
        "win_prob": win_prob,
    }


async def _apply_victory_rewards(player: Player, result: dict, enemy_scale: float):
    """胜利奖励：按组合规则发放 1+1+1+1 / 1+1+1 / 1+1 / 1。"""
    ratio = result["player_power"] / max(1, result["enemy_power"])
    if ratio >= 1.3:
        combo_weights = [35, 35, 20, 10]
    elif ratio >= 1.1:
        combo_weights = [22, 35, 28, 15]
    elif ratio >= 0.95:
        combo_weights = [12, 25, 35, 28]
    else:
        combo_weights = [6, 16, 35, 43]

    combo_size = random.choices(_COMBO_SIZES, weights=combo_weights, k=1)[0]
    reward_types = random.sample(list(_REWARD_TYPES), k=combo_size)

    reward_lines: list[str] = []
    if "stones" in reward_types:
        stones = _apply_stones(player, enemy_scale)
        result["stones_gained"] = stones
        reward_lines.append(f"灵石 +{stones}")
    if "exp" in reward_types:
        exp = _apply_exp(player, enemy_scale)
        result["exp_gained"] = exp
        reward_lines.append(f"修为 +{exp}")
    if "pill" in reward_types:
        pill_name = await _apply_pill(player)
        result["pill_name"] = pill_name
        reward_lines.append(f"丹药 +{pill_name}")
    if "equip" in reward_types:
        equip = await _apply_equip_drop(player)
        if equip:
            result["equip_name"] = equip["name"]
            result["equip_tier"] = equip["tier_name"]
            reward_lines.append(f"装备 +{equip['tier_name']}【{equip['name']}】")
        else:
            fallback_exp = _apply_exp(player, enemy_scale * 0.8)
            result["exp_gained"] = result.get("exp_gained", 0) + fallback_exp
            reward_lines.append(f"装备池空，改为修为 +{fallback_exp}")
    if "material" in reward_types:
        mat = await _apply_material_drop(player)
        if mat:
            result["material_name"] = mat["name"]
            result["material_rarity"] = mat["rarity_name"]
            reward_lines.append(f"材料 +{mat['rarity_name']}【{mat['name']}】")
        else:
            fallback_stones = random.randint(50, 200)
            player.spirit_stones += fallback_stones
            result["stones_gained"] = result.get("stones_gained", 0) + fallback_stones
            reward_lines.append(f"材料池空，改为灵石 +{fallback_stones}")

    hm_drop = await _apply_heart_method_drop(player, enemy_scale)
    if hm_drop:
        result["heart_method_drop"] = hm_drop
        reward_lines.append(
            f"心法秘籍 +【{hm_drop['manual_name']}】（{hm_drop['realm_name']}·普通）"
        )

    gf_drop = await _apply_gongfa_drop(player)
    if gf_drop:
        result["gongfa_drop"] = gf_drop
        reward_lines.append(
            f"功法卷轴 +{gf_drop['tier_name']}【{gf_drop['name']}】"
        )

    # 功法熟练度 + HP/灵力战后回复
    gf_mastery_msgs = _apply_gongfa_mastery(player)
    gf_regen_msgs = _apply_gongfa_regen(player)
    reward_lines.extend(gf_mastery_msgs)
    reward_lines.extend(gf_regen_msgs)

    combo_desc = "+".join(["1"] * combo_size)
    result["outcome"] = "reward_combo"
    result["combo_size"] = combo_size
    result["reward_types"] = reward_types
    result["reward_lines"] = reward_lines
    result["message"] = (
        f"战胜了{result['enemy_realm_name']}敌手，掉落组合：{combo_desc}\n"
        + "\n".join(reward_lines)
    )


def _apply_stones(player: Player, enemy_scale: float) -> int:
    """获得灵石。"""
    realm_mult = 1.0 + player.realm * 0.28
    scale = max(0.8, min(1.4, enemy_scale))
    stones = random.randint(int(50 * realm_mult * scale), int(220 * realm_mult * scale))
    player.spirit_stones += stones
    return stones


def _apply_exp(player: Player, enemy_scale: float) -> int:
    """获得修为。"""
    base = 120 + player.realm * 90 + player.sub_realm * 25
    scale = max(0.8, min(1.45, enemy_scale))
    exp = random.randint(int(base * 0.9 * scale), int(base * 1.8 * scale))
    player.exp += exp
    return exp


async def _apply_pill(player: Player) -> str:
    """获得丹药（使用新丹药系统，历练掉落凡阶/黄阶）。"""
    from .pills import (
        pick_random_pill,
    )
    # 历练掉落：凡阶/黄阶，无垢概率极低
    adventure_tier_weights = {0: 7000, 1: 3000}
    adventure_grade_weights = {
        0: {0: 7000, 1: 2900, 2: 100},
        1: {0: 7000, 1: 2900, 2: 100},
    }
    pill = pick_random_pill(random.Random(), adventure_tier_weights, adventure_grade_weights)
    if pill:
        await add_item(player, pill.pill_id)
        return pill.name
    # fallback：旧丹药
    weighted_items = []
    for item_id, weight in CHECKIN_PILL_WEIGHTS:
        if item_id in ITEM_REGISTRY and int(weight) > 0:
            weighted_items.append((item_id, int(weight)))
    if not weighted_items:
        weighted_items = [("healing_pill", 1)]
    pill_ids = [w[0] for w in weighted_items]
    pill_weights = [w[1] for w in weighted_items]
    pill_id = random.choices(pill_ids, weights=pill_weights, k=1)[0]
    await add_item(player, pill_id)
    return ITEM_REGISTRY[pill_id].name


# ── 材料掉落 ──────────────────────────────────────
_ADVENTURE_MATERIAL_RARITY_WEIGHTS: dict[int, int] = {
    0: 6000,  # 普通
    1: 2500,  # 稀有
    2: 800,   # 珍稀
    3: 150,   # 史诗
    4: 15,    # 传说
    5: 1,     # 神话（万分之一）
}


async def _apply_material_drop(player: Player) -> dict | None:
    """随机获得一份材料，按稀有度加权。"""
    if not MATERIAL_REGISTRY:
        return None
    rarity_pool: list[tuple[int, int]] = [
        (r, w) for r, w in _ADVENTURE_MATERIAL_RARITY_WEIGHTS.items()
    ]
    rarities = [r for r, _ in rarity_pool]
    weights = [w for _, w in rarity_pool]
    chosen_rarity = random.choices(rarities, weights=weights, k=1)[0]
    candidates = [m for m in MATERIAL_REGISTRY.values() if m.rarity == chosen_rarity]
    if not candidates:
        candidates = list(MATERIAL_REGISTRY.values())
    if not candidates:
        return None
    mat = random.choice(candidates)
    await add_item(player, mat.item_id)
    rarity_name = MATERIAL_RARITY_NAMES.get(mat.rarity, "未知")
    return {"name": mat.name, "rarity_name": rarity_name, "item_id": mat.item_id}


async def _apply_equip_drop(player: Player) -> dict | None:
    """获得装备：仅掉落凡器或灵器。"""
    # 仅允许凡器/灵器
    tiers = [EquipmentTier.MORTAL, EquipmentTier.SPIRIT]
    if player.realm < RealmLevel.QI_REFINING:
        tier = random.choices(tiers, weights=[90, 10], k=1)[0]
    else:
        tier = random.choices(tiers, weights=[45, 55], k=1)[0]

    candidates = [eq for eq in EQUIPMENT_REGISTRY.values() if eq.tier == tier]
    if not candidates:
        return None
    eq = random.choice(candidates)
    await add_item(player, eq.equip_id)
    return {
        "name": eq.name,
        "tier": eq.tier,
        "tier_name": EQUIPMENT_TIER_NAMES.get(eq.tier, ""),
    }


async def _apply_heart_method_drop(player: Player, enemy_scale: float) -> dict | None:
    """掉落普通心法秘籍（掉落池不受玩家境界限制）。"""
    drop_rate = min(0.35, _HEART_METHOD_DROP_BASE_RATE + max(0.0, enemy_scale - 1.0) * 0.12)
    if random.random() >= drop_rate:
        return None

    candidates = [
        hm for hm in HEART_METHOD_REGISTRY.values()
        if hm.quality == HeartMethodQuality.NORMAL and hm.realm <= RealmLevel.NASCENT_SOUL
    ]
    if not candidates:
        return None
    hm = random.choice(candidates)
    manual_id = get_heart_method_manual_id(hm.method_id)
    manual_item = ITEM_REGISTRY.get(manual_id)
    if not manual_item:
        return None
    await add_item(player, manual_id)
    return {
        "method_id": hm.method_id,
        "manual_id": manual_id,
        "manual_name": manual_item.name,
        "name": hm.name,
        "realm": hm.realm,
        "realm_name": REALM_CONFIG.get(hm.realm, {}).get("name", "未知境界"),
        "quality": hm.quality,
    }


def _apply_injured(player: Player, result: dict, enemy_attack: int):
    """战败受伤，伤害受敌方攻击和玩家防御影响。"""
    from .pills import get_effective_combat_stats

    effective_stats = get_effective_combat_stats(player)
    mitigation = 120 / (120 + max(1, effective_stats["defense"]))
    damage = max(1, int(enemy_attack * random.uniform(0.85, 1.25) * mitigation))
    damage = min(damage, max(1, int(effective_stats["max_hp"] * 0.65)))
    new_effective_hp = max(1, effective_stats["hp"] - damage)
    player.hp = max(1, min(player.max_hp, new_effective_hp - effective_stats["hp_delta"]))
    scene = result["scene_name"]
    cat = result["category"]
    result["damage"] = damage
    result["message"] = (
        f"在【{cat}·{scene}】中不敌强敌，受伤{damage}点，"
        f"当前HP：{new_effective_hp}/{effective_stats['max_hp']}"
    )


def _drop_realm_steps(player: Player, steps: int) -> int:
    """按层数连续跌境，跨越边界时自动跌落到上一大境界。"""
    dropped = 0
    target = max(1, int(steps))
    while dropped < target:
        player.realm = get_nearest_realm_level(player.realm)
        if has_sub_realm(player.realm):
            player.sub_realm = max(0, min(int(player.sub_realm), get_max_sub_realm(player.realm)))
        else:
            player.sub_realm = 0
        if has_sub_realm(player.realm):
            if player.sub_realm > 0:
                player.sub_realm -= 1
            else:
                prev_realm = get_previous_realm_level(player.realm)
                if prev_realm is None:
                    break
                player.realm = prev_realm
                player.sub_realm = get_max_sub_realm(player.realm) if has_sub_realm(player.realm) else 0
        else:
            prev_realm = get_previous_realm_level(player.realm)
            if prev_realm is None:
                break
            player.realm = prev_realm
            player.sub_realm = get_max_sub_realm(player.realm) if has_sub_realm(player.realm) else 0
        dropped += 1
    return dropped


def _rebuild_stats_by_realm(player: Player):
    """根据当前(大境界/小境界)重建基础属性。"""
    if has_sub_realm(player.realm):
        player.sub_realm = max(0, min(get_max_sub_realm(player.realm), int(player.sub_realm)))
    else:
        player.sub_realm = 0
    base_stats = get_realm_base_stats(player.realm, player.sub_realm)
    player.max_hp = base_stats["max_hp"] + getattr(player, "permanent_max_hp_bonus", 0)
    player.attack = base_stats["attack"] + getattr(player, "permanent_attack_bonus", 0)
    player.defense = base_stats["defense"] + getattr(player, "permanent_defense_bonus", 0)
    player.lingqi = base_stats["max_lingqi"] + getattr(player, "permanent_lingqi_bonus", 0)
    player.hp = min(player.hp, player.max_hp)


def _apply_injured_realm_down(player: Player, result: dict, enemy_attack: int):
    """受伤 + 必定跌境（触发则一定掉境界）。"""
    from .pills import get_effective_combat_stats

    effective_stats = get_effective_combat_stats(player)
    mitigation = 100 / (100 + max(1, effective_stats["defense"]))
    damage = max(1, int(enemy_attack * random.uniform(1.0, 1.4) * mitigation))
    damage = min(damage, max(1, int(effective_stats["max_hp"] * 0.85)))
    new_effective_hp = max(1, effective_stats["hp"] - damage)
    player.hp = max(1, min(player.max_hp, new_effective_hp - effective_stats["hp_delta"]))
    result["damage"] = damage

    scene = result["scene_name"]
    cat = result["category"]
    old_name = get_realm_name(player.realm, player.sub_realm)

    drop_levels = random.randint(1, 5)
    actual_drop = _drop_realm_steps(player, drop_levels)
    player.exp = 0
    _rebuild_stats_by_realm(player)

    new_name = get_realm_name(player.realm, player.sub_realm)
    result["realm_changed"] = actual_drop > 0
    result["old_realm"] = old_name
    result["new_realm"] = new_name
    result["message"] = (
        f"在【{cat}·{scene}】中血战逃生，受伤{damage}点，"
        f"修为跌落{max(1, actual_drop)}层：{old_name} → {new_name}！"
    )


_GONGFA_DROP_BASE_RATE = 0.12


async def _apply_gongfa_drop(player: Player) -> dict | None:
    """12% 基础概率掉落黄阶或玄阶功法卷轴。"""
    if random.random() >= _GONGFA_DROP_BASE_RATE:
        return None

    candidates = [gf for gf in GONGFA_REGISTRY.values() if gf.tier <= GongfaTier.XUAN]
    if not candidates:
        return None
    gf = random.choice(candidates)
    scroll_id = get_gongfa_scroll_id(gf.gongfa_id)
    scroll_item = ITEM_REGISTRY.get(scroll_id)
    if not scroll_item:
        return None
    await add_item(player, scroll_id)
    return {
        "gongfa_id": gf.gongfa_id,
        "scroll_id": scroll_id,
        "name": gf.name,
        "tier": gf.tier,
        "tier_name": GONGFA_TIER_NAMES.get(gf.tier, "黄阶"),
    }


def _apply_gongfa_mastery(player: Player) -> list[str]:
    """战后功法熟练度增长 random(3,8)/功法。"""
    msgs: list[str] = []
    for slot in ("gongfa_1", "gongfa_2", "gongfa_3"):
        gongfa_id = getattr(player, slot, "无")
        if not gongfa_id or gongfa_id == "无":
            continue
        gf = GONGFA_REGISTRY.get(gongfa_id)
        if not gf:
            continue
        mastery_attr = f"{slot}_mastery"
        exp_attr = f"{slot}_exp"
        mastery = getattr(player, mastery_attr, 0)
        if mastery >= MASTERY_MAX:
            continue
        if not can_cultivate_gongfa(player.realm, gf.tier):
            continue

        gf_exp_gain = random.randint(3, 8)
        cur_exp = getattr(player, exp_attr, 0) + gf_exp_gain
        if cur_exp >= gf.mastery_exp:
            # 地阶+功法的大成→圆满需消耗道韵（不够则暂停累积）
            if gf.tier >= 2 and mastery == 2 and gf.dao_yun_cost > 0:
                if player.dao_yun < gf.dao_yun_cost:
                    cur_exp = gf.mastery_exp - 1
                    setattr(player, exp_attr, cur_exp)
                    continue
                player.dao_yun -= gf.dao_yun_cost
                msgs.append(f"消耗道韵{gf.dao_yun_cost}，助功法【{gf.name}】突破")
            cur_exp -= gf.mastery_exp
            mastery += 1
            setattr(player, mastery_attr, mastery)
            new_level = MASTERY_LEVELS[mastery]
            msgs.append(f"功法【{gf.name}】修炼至{new_level}！")
        setattr(player, exp_attr, cur_exp)
    return msgs


def _apply_gongfa_regen(player: Player) -> list[str]:
    """战后功法HP/灵力回复。"""
    msgs: list[str] = []
    gf_total = get_total_gongfa_bonus(player)
    if gf_total["hp_regen"] > 0 and player.hp < player.max_hp:
        heal = min(gf_total["hp_regen"], player.max_hp - player.hp)
        player.hp += heal
        msgs.append(f"功法回血+{heal}")
    if gf_total["lingqi_regen"] > 0:
        max_lq = get_player_base_max_lingqi(player)
        actual = min(gf_total["lingqi_regen"], max(0, max_lq - player.lingqi))
        if actual > 0:
            player.lingqi += actual
            msgs.append(f"功法回灵+{actual}")
    return msgs
