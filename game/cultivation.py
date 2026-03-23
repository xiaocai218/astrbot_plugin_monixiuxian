"""修炼与突破逻辑。"""

from __future__ import annotations

import random
import time

from .constants import (
    REALM_CONFIG, RealmLevel,
    HEART_METHOD_REGISTRY, MASTERY_LEVELS, MASTERY_MAX,
    GONGFA_REGISTRY, GONGFA_TIER_REALM_REQ,
    has_sub_realm, get_realm_name, get_heart_method_bonus,
    get_total_gongfa_bonus, can_cultivate_gongfa,
    get_max_sub_realm, get_player_base_max_lingqi, get_player_base_stats,
    get_sub_realm_dao_yun_cost, get_breakthrough_dao_yun_cost,
    get_nearest_realm_level, get_next_realm_level,
    DEATH_REALM_START,
)
from .models import Player


async def perform_cultivate(player: Player, cooldown: int = 60) -> dict:
    """执行修炼操作。

    修炼获得经验（心法加成），同时积累心法修炼经验。
    如果有小境界体系，经验满后自动升小境界。

    Returns:
        {"success": bool, "exp_gained": int, "message": str, "sub_level_up": bool}
    """
    now = time.time()
    remaining = cooldown - (now - player.last_cultivate_time)
    if remaining > 0:
        return {
            "success": False,
            "exp_gained": 0,
            "message": f"修炼冷却中，还需等待{int(remaining)}秒",
            "sub_level_up": False,
        }

    normalized_realm = get_nearest_realm_level(player.realm)
    if normalized_realm != player.realm:
        player.realm = normalized_realm
    if has_sub_realm(player.realm):
        player.sub_realm = max(0, min(int(player.sub_realm), get_max_sub_realm(player.realm)))
    else:
        player.sub_realm = 0

    realm_cfg = REALM_CONFIG.get(player.realm)
    if not realm_cfg:
        return {
            "success": False,
            "exp_gained": 0,
            "message": "当前境界配置无效，无法修炼",
            "sub_level_up": False,
        }
    # 基础经验
    base_min = 10 + player.realm * 5 + player.sub_realm * 2
    base_max = 30 + player.realm * 10 + player.sub_realm * 4
    if player.realm >= RealmLevel.GOLDEN_CORE:
        base_min *= 10
        base_max *= 10
    exp_gained = random.randint(base_min, base_max)

    # 心法加成
    hm_bonus = get_heart_method_bonus(player.heart_method, player.heart_method_mastery)
    if hm_bonus["exp_multiplier"] > 0:
        exp_gained = int(exp_gained * (1.0 + hm_bonus["exp_multiplier"]))

    # 丹药修炼速度buff加成
    from .pills import get_buff_totals
    buff_totals = get_buff_totals(player)
    if buff_totals["cultivate_speed"] > 0:
        exp_gained = int(exp_gained * buff_totals["cultivate_speed"])

    player.exp += exp_gained
    player.last_cultivate_time = now

    extra_msgs: list[str] = []

    # 心法修炼经验积累
    hm_mastery_msg = _accumulate_heart_method_exp(player)
    if hm_mastery_msg:
        extra_msgs.append(hm_mastery_msg)

    # 功法熟练度积累
    gf_msgs = _accumulate_gongfa_exp(player)
    extra_msgs.extend(gf_msgs)

    # 功法HP/灵力回复
    gf_total = get_total_gongfa_bonus(player)
    if gf_total["hp_regen"] > 0 and player.hp < player.max_hp:
        heal = min(gf_total["hp_regen"], player.max_hp - player.hp)
        player.hp += heal
        extra_msgs.append(f"功法回血+{heal}")
    if gf_total["lingqi_regen"] > 0:
        max_lq = get_player_base_max_lingqi(player)
        actual = min(gf_total["lingqi_regen"], max(0, max_lq - player.lingqi))
        if actual > 0:
            player.lingqi += actual
            extra_msgs.append(f"功法回灵+{actual}")

    # 道韵获取（仅化神期及以上心法生效）
    hm = HEART_METHOD_REGISTRY.get(player.heart_method)
    if (
        hm
        and hm.realm >= RealmLevel.DEITY_TRANSFORM
        and hm_bonus["dao_yun_rate"] > 0
        and random.random() < hm_bonus["dao_yun_rate"]
    ):
        dao_gain = random.randint(1, 3 + player.realm)
        player.dao_yun += dao_gain
        extra_msgs.append(f"感悟道韵+{dao_gain}")

    sub_level_up = False

    # 小境界自动升级
    if has_sub_realm(player.realm) and player.sub_realm < get_max_sub_realm(player.realm):
        sub_exp = realm_cfg.get("sub_exp_to_next", 0)
        if sub_exp > 0 and player.exp >= sub_exp:
            # 高阶境界小境界升级需要道韵
            dao_cost = get_sub_realm_dao_yun_cost(player.realm, player.sub_realm)
            if dao_cost > 0 and player.dao_yun < dao_cost:
                extra_msgs.append(f"道韵不足，需{dao_cost}道韵突破小境界")
            else:
                if dao_cost > 0:
                    player.dao_yun -= dao_cost
                    extra_msgs.append(f"消耗道韵{dao_cost}")
                player.exp -= sub_exp
                player.sub_realm += 1
                sub_level_up = True
                hp_bonus = int(realm_cfg["base_hp"] * 0.08)
                atk_bonus = int(realm_cfg["base_attack"] * 0.06)
                def_bonus = int(realm_cfg["base_defense"] * 0.06)
                lingqi_bonus = max(1, int(realm_cfg.get("base_lingqi", 0) * 0.08))
                player.max_hp += hp_bonus
                player.hp = player.max_hp
                player.attack += atk_bonus
                player.defense += def_bonus
                player.lingqi += lingqi_bonus
                sub_name = get_realm_name(player.realm, player.sub_realm)
                extra_msgs.append(f"境界提升！当前：{sub_name}")

    realm_name = get_realm_name(player.realm, player.sub_realm)
    if has_sub_realm(player.realm) and player.sub_realm < get_max_sub_realm(player.realm):
        exp_target = realm_cfg.get("sub_exp_to_next", 0)
    else:
        exp_target = realm_cfg["exp_to_next"]

    msg = f"修炼获得{exp_gained}点经验，当前经验: {player.exp}/{exp_target}"
    if extra_msgs:
        msg += "\n" + "，".join(extra_msgs)

    return {
        "success": True,
        "exp_gained": exp_gained,
        "message": msg,
        "sub_level_up": sub_level_up,
    }


def _accumulate_heart_method_exp(player: Player) -> str:
    """积累心法修炼经验，检查是否升阶。返回提示消息或空字符串。"""
    hm = HEART_METHOD_REGISTRY.get(player.heart_method)
    if not hm:
        return ""
    if player.heart_method_mastery >= MASTERY_MAX:
        return ""  # 已圆满

    # 每次修炼获得心法经验（基础 1~3，品质越高越快）
    hm_exp_gain = random.randint(1, 3) + hm.quality
    player.heart_method_exp += hm_exp_gain

    if player.heart_method_exp >= hm.mastery_exp:
        player.heart_method_exp -= hm.mastery_exp
        player.heart_method_mastery += 1
        new_level = MASTERY_LEVELS[player.heart_method_mastery]
        return f"心法【{hm.name}】修炼至{new_level}！"
    return ""


def _accumulate_gongfa_exp(player: Player) -> list[str]:
    """积累功法修炼经验，检查是否升阶。返回提示消息列表。"""
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

        # 境界不够则不涨熟练度
        if not can_cultivate_gongfa(player.realm, gf.tier):
            msgs.append(f"功法【{gf.name}】需更高境界方可继续修炼")
            continue

        gf_exp_gain = random.randint(1, 3)
        cur_exp = getattr(player, exp_attr, 0) + gf_exp_gain
        if cur_exp >= gf.mastery_exp:
            # 大成→圆满道韵校验
            if gf.tier >= 2 and mastery == 2 and gf.dao_yun_cost > 0:
                if player.dao_yun < gf.dao_yun_cost:
                    # 道韵不足，停止在阈值附近（不升级）
                    cur_exp = gf.mastery_exp - 1
                    setattr(player, exp_attr, cur_exp)
                    continue
                # 道韵扣除和升级绑定执行
                player.dao_yun -= gf.dao_yun_cost
                msgs.append(f"消耗道韵{gf.dao_yun_cost}，助功法【{gf.name}】突破")
            cur_exp -= gf.mastery_exp
            mastery += 1
            setattr(player, mastery_attr, mastery)
            new_level = MASTERY_LEVELS[mastery]
            msgs.append(f"功法【{gf.name}】修炼至{new_level}！")
        setattr(player, exp_attr, cur_exp)
    return msgs


async def attempt_breakthrough(player: Player, bonus_rate: float = 0.0,
                                prevent_death: bool = False) -> dict:
    """尝试突破大境界。

    前提：有小境界的大境界需要达到圆满（sub_realm=9）才能突破。
    元婴期开始突破有死亡概率。

    Returns:
        {"success": bool, "message": str, "new_realm": str | None, "died": bool}
    """
    normalized_realm = get_nearest_realm_level(player.realm)
    if normalized_realm != player.realm:
        player.realm = normalized_realm
    if has_sub_realm(player.realm):
        player.sub_realm = max(0, min(int(player.sub_realm), get_max_sub_realm(player.realm)))
    else:
        player.sub_realm = 0

    realm_cfg = REALM_CONFIG.get(player.realm)
    if not realm_cfg:
        return {"success": False, "message": "当前境界配置无效，无法突破",
                "new_realm": None, "died": False}

    next_realm = get_next_realm_level(player.realm)
    if next_realm is None:
        max_name = REALM_CONFIG.get(player.realm, {}).get("name", "最高境界")
        return {"success": False, "message": f"已达{max_name}，无法继续突破",
                "new_realm": None, "died": False}

    # 有小境界的大境界，必须到圆满才能突破
    if has_sub_realm(player.realm) and player.sub_realm < get_max_sub_realm(player.realm):
        current_name = get_realm_name(player.realm, player.sub_realm)
        return {
            "success": False,
            "message": f"当前{current_name}，需修炼至圆满方可突破大境界",
            "new_realm": None,
            "died": False,
        }

    # 检查经验
    if player.exp < realm_cfg["exp_to_next"]:
        return {
            "success": False,
            "message": f"经验不足，需要{realm_cfg['exp_to_next']}，当前{player.exp}",
            "new_realm": None,
            "died": False,
        }

    # 高阶境界突破需要道韵
    dao_cost = get_breakthrough_dao_yun_cost(player.realm)
    if dao_cost > 0 and player.dao_yun < dao_cost:
        return {
            "success": False,
            "message": f"道韵不足，突破需要{dao_cost}道韵，当前{player.dao_yun}",
            "new_realm": None,
            "died": False,
        }
    if dao_cost > 0:
        player.dao_yun -= dao_cost

    # 死亡判定（元婴期开始）
    death_rate = realm_cfg.get("death_rate", 0.0)
    if death_rate > 0 and not prevent_death:
        if random.random() < death_rate:
            cost_msg = f"消耗道韵{dao_cost}。\n" if dao_cost > 0 else ""
            return {
                "success": False,
                "message": (
                    f"突破失败！天劫降临，道消身殒...\n"
                    f"{cost_msg}"
                    f"（{int(death_rate * 100)}%概率陨落，可使用保命符规避）"
                ),
                "new_realm": None,
                "died": True,
            }

    # 突破成功率判定
    accumulated_bonus = getattr(player, 'breakthrough_bonus', 0.0)
    rate = min(realm_cfg["breakthrough_rate"] + bonus_rate + accumulated_bonus, 1.0)
    rate_percent = int(rate * 100)

    if random.random() <= rate:
        player.realm = next_realm
        player.sub_realm = 0
        player.exp = 0
        player.breakthrough_bonus = 0.0  # 成功后清零累积加成
        base_stats = get_player_base_stats(player)
        player.max_hp = base_stats["max_hp"]
        player.hp = player.max_hp
        player.attack = base_stats["attack"]
        player.defense = base_stats["defense"]
        player.lingqi = base_stats["max_lingqi"]
        new_name = get_realm_name(player.realm, player.sub_realm)
        cost_msg = f"消耗道韵{dao_cost}，" if dao_cost > 0 else ""
        return {
            "success": True,
            "message": f"突破成功！{cost_msg}当前境界：{new_name}，气血恢复满血（突破概率{rate_percent}%）",
            "new_realm": new_name,
            "died": False,
        }
    else:
        penalty = player.exp // 4
        player.exp -= penalty
        # 失败后增加累积加成，每次+5%，最高20%
        if accumulated_bonus < 0.2:
            player.breakthrough_bonus = min(accumulated_bonus + 0.05, 0.2)
            new_bonus_percent = int(player.breakthrough_bonus * 100)
            bonus_msg = f"，下次突破成功率+{new_bonus_percent}%"
        else:
            bonus_msg = "，累积加成已达上限20%"
        cost_msg = f"消耗道韵{dao_cost}，" if dao_cost > 0 else ""
        return {
            "success": False,
            "message": f"突破失败！{cost_msg}损失{penalty}点经验（突破概率{rate_percent}%{bonus_msg}）",
            "new_realm": None,
            "died": False,
        }
