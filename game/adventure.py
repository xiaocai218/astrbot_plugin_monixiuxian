"""历练系统：场景匹配、攻防对抗、奖惩逻辑。"""

from __future__ import annotations

import random

from .constants import (
    REALM_CONFIG, ITEM_REGISTRY, CHECKIN_PILL_WEIGHTS,
    HEART_METHOD_REGISTRY, HeartMethodQuality, get_heart_method_manual_id,
    EQUIPMENT_REGISTRY, EQUIPMENT_TIER_NAMES, EquipmentTier,
    MAX_SUB_REALM, RealmLevel, has_sub_realm,
    get_realm_name, get_equip_bonus, get_heart_method_bonus,
)
from .inventory import add_item
from .models import Player

_REWARD_TYPES = ("stones", "exp", "pill", "equip")
_COMBO_SIZES = (4, 3, 2, 1)  # 1+1+1+1, 1+1+1, 1+1, 1
_HEART_METHOD_DROP_BASE_RATE = 0.18


async def attempt_adventure(player: Player, scene: dict, difficulty: str) -> dict:
    """执行一次历练。

    历练先进行一次攻防对抗判定（攻击/防御都会参与），胜利后根据组合规则发放奖励。
    """
    diff_labels = {"easy": "轻松", "normal": "正常", "hard": "困难"}
    diff_label = diff_labels.get(difficulty, "正常")

    enemy_realm = _resolve_enemy_realm(player.realm, difficulty)
    battle = _build_battle_context(player, enemy_realm, difficulty)
    win = random.random() <= battle["win_prob"]

    result = {
        "success": True,
        "outcome": "battle",
        "scene_name": scene["name"],
        "category": scene["category"],
        "description": scene["description"],
        "difficulty_label": diff_label,
        "message": "",
        "died": False,
        "realm_changed": False,
        "player_power": battle["player_power"],
        "enemy_power": battle["enemy_power"],
        "win_prob": round(battle["win_prob"], 3),
        "enemy_realm_name": get_realm_name(enemy_realm, 0),
    }

    if win:
        await _apply_victory_rewards(player, result, battle["enemy_scale"])
        return result

    # 战败：依据战力差距决定受伤/跌境/死亡概率
    if battle["power_ratio"] < 0.7:
        lose_weights = [50, 49, 1]  # 受伤, 跌境, 死亡(1%)
    elif battle["power_ratio"] < 0.9:
        lose_weights = [60, 39, 1]
    else:
        lose_weights = [73, 26, 1]
    lose_outcome = random.choices(
        ["injured", "injured_realm_down", "death"],
        weights=lose_weights,
        k=1,
    )[0]
    result["outcome"] = lose_outcome
    if lose_outcome == "injured":
        _apply_injured(player, result, battle["enemy_attack"])
    elif lose_outcome == "injured_realm_down":
        _apply_injured_realm_down(player, result, battle["enemy_attack"])
    else:
        result["died"] = True
        result["message"] = (
            f"在【{scene['category']}·{scene['name']}】中遭遇{diff_label}强敌（{result['enemy_realm_name']}），"
            f"力战不支，不幸陨落……"
        )
    return result


def _resolve_enemy_realm(player_realm: int, difficulty: str) -> int:
    """根据难度匹配敌方境界。"""
    delta = {"easy": -1, "normal": 0, "hard": 1}.get(difficulty, 0)
    realm = player_realm + delta
    if realm < RealmLevel.QI_REFINING:
        realm = RealmLevel.QI_REFINING
    if realm > RealmLevel.MAHAYANA:
        realm = RealmLevel.MAHAYANA
    return realm


def _build_battle_context(player: Player, enemy_realm: int, difficulty: str) -> dict:
    """构建战斗上下文，输出胜率和敌方属性。"""
    equip_bonus = get_equip_bonus(player.weapon, player.armor)
    hm_bonus = get_heart_method_bonus(player.heart_method, player.heart_method_mastery)
    player_atk = max(1, player.attack + equip_bonus["attack"] + hm_bonus["attack_bonus"])
    player_def = max(1, player.defense + equip_bonus["defense"] + hm_bonus["defense_bonus"])

    enemy_cfg = REALM_CONFIG.get(enemy_realm, REALM_CONFIG[RealmLevel.QI_REFINING])
    diff_scale = {"easy": 0.9, "normal": 1.0, "hard": 1.12}.get(difficulty, 1.0)
    jitter = random.uniform(0.92, 1.08)
    enemy_scale = diff_scale * jitter

    enemy_attack = max(1, int(enemy_cfg["base_attack"] * enemy_scale))
    enemy_defense = max(1, int(enemy_cfg["base_defense"] * enemy_scale))
    enemy_hp = max(1, int(enemy_cfg["base_hp"] * (0.9 + (enemy_scale - 1.0) * 0.6)))

    player_power = player_atk * 1.25 + player_def * 0.95 + player.hp * 0.08
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

    hm_drop = await _apply_heart_method_drop(player, enemy_scale)
    if hm_drop:
        result["heart_method_drop"] = hm_drop
        reward_lines.append(
            f"心法秘籍 +【{hm_drop['manual_name']}】（{hm_drop['realm_name']}·普通）"
        )

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
    """获得丹药。"""
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
    mitigation = 120 / (120 + max(1, player.defense))
    damage = max(1, int(enemy_attack * random.uniform(0.85, 1.25) * mitigation))
    damage = min(damage, max(1, int(player.max_hp * 0.65)))
    player.hp = max(1, player.hp - damage)
    scene = result["scene_name"]
    cat = result["category"]
    result["damage"] = damage
    result["message"] = (
        f"在【{cat}·{scene}】中不敌强敌，受伤{damage}点，"
        f"当前HP：{player.hp}/{player.max_hp}"
    )


def _drop_realm_steps(player: Player, steps: int) -> int:
    """按层数连续跌境，跨越边界时自动跌落到上一大境界。"""
    dropped = 0
    target = max(1, int(steps))
    while dropped < target:
        if has_sub_realm(player.realm):
            if player.sub_realm > 0:
                player.sub_realm -= 1
            elif player.realm > RealmLevel.MORTAL:
                player.realm -= 1
                player.sub_realm = MAX_SUB_REALM if has_sub_realm(player.realm) else 0
            else:
                break
        else:
            if player.realm > RealmLevel.MORTAL:
                player.realm -= 1
                player.sub_realm = MAX_SUB_REALM if has_sub_realm(player.realm) else 0
            else:
                break
        dropped += 1
    return dropped


def _rebuild_stats_by_realm(player: Player):
    """根据当前(大境界/小境界)重建基础属性。"""
    cfg = REALM_CONFIG.get(player.realm, {})
    base_hp = int(cfg.get("base_hp", player.max_hp))
    base_atk = int(cfg.get("base_attack", player.attack))
    base_def = int(cfg.get("base_defense", player.defense))
    base_lingqi = int(cfg.get("base_lingqi", player.lingqi))

    if has_sub_realm(player.realm):
        player.sub_realm = max(0, min(MAX_SUB_REALM, int(player.sub_realm)))
        hp_step = int(base_hp * 0.08)
        atk_step = int(base_atk * 0.06)
        def_step = int(base_def * 0.06)
        lingqi_step = max(1, int(base_lingqi * 0.08))
        player.max_hp = base_hp + hp_step * player.sub_realm
        player.attack = base_atk + atk_step * player.sub_realm
        player.defense = base_def + def_step * player.sub_realm
        player.lingqi = base_lingqi + lingqi_step * player.sub_realm
    else:
        player.sub_realm = 0
        player.max_hp = base_hp
        player.attack = base_atk
        player.defense = base_def
        player.lingqi = base_lingqi

    player.hp = min(player.hp, player.max_hp)


def _apply_injured_realm_down(player: Player, result: dict, enemy_attack: int):
    """受伤 + 必定跌境（触发则一定掉境界）。"""
    mitigation = 100 / (100 + max(1, player.defense))
    damage = max(1, int(enemy_attack * random.uniform(1.0, 1.4) * mitigation))
    damage = min(damage, max(1, int(player.max_hp * 0.85)))
    player.hp = max(1, player.hp - damage)
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
