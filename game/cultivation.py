"""修炼与突破逻辑。"""

from __future__ import annotations

import random
import time

from .constants import (
    REALM_CONFIG, RealmLevel, MAX_SUB_REALM,
    HEART_METHOD_REGISTRY, MASTERY_LEVELS, MASTERY_MAX,
    has_sub_realm, get_realm_name, get_heart_method_bonus,
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

    realm_cfg = REALM_CONFIG[player.realm]
    # 基础经验
    base_min = 10 + player.realm * 5 + player.sub_realm * 2
    base_max = 30 + player.realm * 10 + player.sub_realm * 4
    exp_gained = random.randint(base_min, base_max)

    # 心法加成
    hm_bonus = get_heart_method_bonus(player.heart_method, player.heart_method_mastery)
    if hm_bonus["exp_multiplier"] > 0:
        exp_gained = int(exp_gained * (1.0 + hm_bonus["exp_multiplier"]))

    player.exp += exp_gained
    player.last_cultivate_time = now

    extra_msgs: list[str] = []

    # 心法修炼经验积累
    hm_mastery_msg = _accumulate_heart_method_exp(player)
    if hm_mastery_msg:
        extra_msgs.append(hm_mastery_msg)

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
    if has_sub_realm(player.realm) and player.sub_realm < MAX_SUB_REALM:
        sub_exp = realm_cfg.get("sub_exp_to_next", 0)
        if sub_exp > 0 and player.exp >= sub_exp:
            player.exp -= sub_exp
            player.sub_realm += 1
            sub_level_up = True
            hp_bonus = int(realm_cfg["base_hp"] * 0.08)
            atk_bonus = int(realm_cfg["base_attack"] * 0.06)
            def_bonus = int(realm_cfg["base_defense"] * 0.06)
            lingqi_bonus = max(1, int(realm_cfg.get("base_lingqi", 0) * 0.08))
            player.max_hp += hp_bonus
            player.hp = min(player.hp + hp_bonus, player.max_hp)
            player.attack += atk_bonus
            player.defense += def_bonus
            player.lingqi += lingqi_bonus
            sub_name = get_realm_name(player.realm, player.sub_realm)
            extra_msgs.append(f"境界提升！当前：{sub_name}")

    realm_name = get_realm_name(player.realm, player.sub_realm)
    if has_sub_realm(player.realm) and player.sub_realm < MAX_SUB_REALM:
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


async def attempt_breakthrough(player: Player, bonus_rate: float = 0.0,
                                prevent_death: bool = False) -> dict:
    """尝试突破大境界。

    前提：有小境界的大境界需要达到圆满（sub_realm=9）才能突破。
    元婴期开始突破有死亡概率。

    Returns:
        {"success": bool, "message": str, "new_realm": str | None, "died": bool}
    """
    realm_cfg = REALM_CONFIG[player.realm]

    if player.realm >= RealmLevel.MAHAYANA:
        return {"success": False, "message": "已达大乘期，无法继续突破",
                "new_realm": None, "died": False}

    # 有小境界的大境界，必须到圆满才能突破
    if has_sub_realm(player.realm) and player.sub_realm < MAX_SUB_REALM:
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

    # 死亡判定（元婴期开始）
    death_rate = realm_cfg.get("death_rate", 0.0)
    if death_rate > 0 and not prevent_death:
        if random.random() < death_rate:
            return {
                "success": False,
                "message": (
                    f"突破失败！天劫降临，道消身殒...\n"
                    f"（{int(death_rate * 100)}%概率陨落，可使用保命符规避）"
                ),
                "new_realm": None,
                "died": True,
            }

    # 突破成功率判定
    rate = min(realm_cfg["breakthrough_rate"] + bonus_rate, 1.0)

    if random.random() <= rate:
        player.realm += 1
        player.sub_realm = 0
        player.exp = 0
        new_cfg = REALM_CONFIG[player.realm]
        old_hp = player.hp
        player.max_hp = new_cfg["base_hp"]
        recover_hp = max(1, int(player.max_hp * 0.25))
        player.hp = min(player.max_hp, old_hp + recover_hp)
        player.attack = new_cfg["base_attack"]
        player.defense = new_cfg["base_defense"]
        player.lingqi = new_cfg.get("base_lingqi", player.lingqi)
        actual_recover = max(0, player.hp - old_hp)
        new_name = get_realm_name(player.realm, player.sub_realm)
        return {
            "success": True,
            "message": f"突破成功！当前境界：{new_name}，回复{actual_recover}点气血",
            "new_realm": new_name,
            "died": False,
        }
    else:
        penalty = player.exp // 4
        player.exp -= penalty
        return {
            "success": False,
            "message": f"突破失败！损失{penalty}点经验",
            "new_realm": None,
            "died": False,
        }
