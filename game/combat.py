"""回合制战斗引擎。"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .constants import (
    GONGFA_REGISTRY, ITEM_REGISTRY, COMBAT_MAX_ROUNDS,
    get_gongfa_bonus, get_equip_bonus, get_heart_method_bonus,
    get_total_gongfa_bonus,
)
from .inventory import add_item


@dataclass
class CombatState:
    """战斗状态快照。"""
    player_hp: int
    player_max_hp: int
    player_attack: int
    player_defense: int
    player_lingqi: int
    player_max_lingqi: int
    player_defending: bool = False
    enemy_name: str = ""
    enemy_type: str = "monster"  # monster | enemy | player
    enemy_hp: int = 0
    enemy_max_hp: int = 0
    enemy_attack: int = 0
    enemy_defense: int = 0
    enemy_realm_name: str = ""
    round_number: int = 0
    max_rounds: int = COMBAT_MAX_ROUNDS
    combat_log: list[str] = field(default_factory=list)
    status: str = "player_turn"  # player_turn | enemy_turn | combat_end

    def to_dict(self) -> dict:
        return {
            "player_hp": self.player_hp,
            "player_max_hp": self.player_max_hp,
            "player_attack": self.player_attack,
            "player_defense": self.player_defense,
            "player_lingqi": self.player_lingqi,
            "player_max_lingqi": self.player_max_lingqi,
            "player_defending": self.player_defending,
            "enemy_name": self.enemy_name,
            "enemy_type": self.enemy_type,
            "enemy_hp": self.enemy_hp,
            "enemy_max_hp": self.enemy_max_hp,
            "enemy_attack": self.enemy_attack,
            "enemy_defense": self.enemy_defense,
            "enemy_realm_name": self.enemy_realm_name,
            "round_number": self.round_number,
            "max_rounds": self.max_rounds,
            "combat_log": list(self.combat_log[-20:]),
            "status": self.status,
        }


class CombatEngine:
    """回合制战斗引擎，处理玩家动作和敌人AI。"""

    @staticmethod
    def _calc_damage(atk: int, dfn: int, defending: bool) -> int:
        """基础伤害公式。"""
        raw = max(1, int(atk * random.uniform(0.85, 1.15) - dfn * 0.6))
        if defending:
            raw = max(1, raw // 2)
        return raw

    @staticmethod
    def resolve_player_action(
        state: CombatState, action: str, player, data: dict | None = None
    ) -> dict:
        """处理玩家回合动作。

        action: attack | defend | gongfa | item | flee
        返回 {"success": bool, "message": str, "combat_end": bool, ...}
        """
        if state.status != "player_turn":
            return {"success": False, "message": "当前不是你的回合"}

        data = data or {}
        if action == "skill":
            action = "gongfa"
        state.player_defending = False
        result: dict = {"success": True, "combat_end": False, "message": ""}

        if action == "attack":
            dmg = CombatEngine._calc_damage(
                state.player_attack, state.enemy_defense, False
            )
            state.enemy_hp = max(0, state.enemy_hp - dmg)
            msg = f"你发动攻击，造成{dmg}点伤害"
            state.combat_log.append(msg)
            result["message"] = msg
            result["damage"] = dmg

        elif action == "defend":
            state.player_defending = True
            msg = "你摆出防御姿态，本回合受到伤害减半"
            state.combat_log.append(msg)
            result["message"] = msg

        elif action == "gongfa":
            gf_result = CombatEngine._apply_gongfa_effect(state, player, data)
            result.update(gf_result)
            if not gf_result["success"]:
                return result

        elif action == "item":
            item_result = CombatEngine._apply_item(state, player, data)
            result.update(item_result)
            if not item_result["success"]:
                return result

        elif action == "flee":
            flee_result = CombatEngine._try_flee(state, data.get("layer", 0))
            result.update(flee_result)
            if flee_result.get("fled"):
                state.status = "combat_end"
                result["combat_end"] = True
                result["outcome"] = "flee"
                return result

        else:
            return {"success": False, "message": f"未知动作: {action}"}

        # 检查敌人是否死亡
        if state.enemy_hp <= 0:
            state.status = "combat_end"
            msg = f"你击败了{state.enemy_name}！"
            state.combat_log.append(msg)
            result["combat_end"] = True
            result["outcome"] = "win"
            result["message"] += f"\n{msg}" if result["message"] else msg
            return result

        # 切换到敌人回合
        state.status = "enemy_turn"
        return result

    @staticmethod
    def resolve_enemy_turn(state: CombatState) -> dict:
        """处理敌人回合。"""
        if state.status != "enemy_turn":
            return {"success": False, "message": "当前不是敌人回合"}

        state.round_number += 1
        result: dict = {"success": True, "combat_end": False}

        # 敌人AI: 80%攻击, 20%防御（简单AI）
        if random.random() < 0.8:
            dmg = CombatEngine._calc_damage(
                state.enemy_attack, state.player_defense, state.player_defending
            )
            state.player_hp = max(0, state.player_hp - dmg)
            msg = f"{state.enemy_name}发动攻击，造成{dmg}点伤害"
            state.combat_log.append(msg)
            result["message"] = msg
            result["damage"] = dmg
        else:
            msg = f"{state.enemy_name}摆出防御姿态"
            state.combat_log.append(msg)
            result["message"] = msg

        # 重置玩家防御状态
        state.player_defending = False

        # 检查玩家是否死亡
        if state.player_hp <= 0:
            state.status = "combat_end"
            msg = f"你被{state.enemy_name}击败了……"
            state.combat_log.append(msg)
            result["combat_end"] = True
            result["outcome"] = "lose"
            result["message"] += f"\n{msg}"
            return result

        # 检查回合上限
        if state.round_number >= state.max_rounds:
            state.status = "combat_end"
            msg = "战斗超时，双方脱离战斗"
            state.combat_log.append(msg)
            result["combat_end"] = True
            result["outcome"] = "timeout"
            result["message"] += f"\n{msg}"
            return result

        state.status = "player_turn"
        return result

    @staticmethod
    def _apply_gongfa_effect(
        state: CombatState, player, data: dict
    ) -> dict:
        """施展功法效果。"""
        gongfa_slot = data.get("gongfa_slot", "")
        if gongfa_slot not in ("gongfa_1", "gongfa_2", "gongfa_3"):
            return {"success": False, "message": "无效的功法槽位"}

        gongfa_id = getattr(player, gongfa_slot, "无")
        if not gongfa_id or gongfa_id == "无":
            return {"success": False, "message": "该槽位没有装备功法"}

        gf = GONGFA_REGISTRY.get(gongfa_id)
        if not gf:
            return {"success": False, "message": "功法数据异常"}

        # 灵气消耗检查（回灵类功法免费）
        is_regen = gf.attack_bonus == 0 and gf.defense_bonus == 0 and gf.hp_regen == 0
        if not is_regen and state.player_lingqi < gf.lingqi_cost:
            return {
                "success": False,
                "message": f"灵气不足，需要{gf.lingqi_cost}，当前{state.player_lingqi}",
            }

        if not is_regen:
            state.player_lingqi -= gf.lingqi_cost

        mastery = getattr(player, f"{gongfa_slot}_mastery", 0)
        bonus = get_gongfa_bonus(gongfa_id, mastery, player.realm)
        msgs: list[str] = []

        # 攻击效果
        if bonus["attack_bonus"] > 0:
            dmg = CombatEngine._calc_damage(
                state.player_attack + int(bonus["attack_bonus"] * 1.5),
                state.enemy_defense, False,
            )
            state.enemy_hp = max(0, state.enemy_hp - dmg)
            msgs.append(f"功法攻击造成{dmg}点伤害")

        # 防御效果
        if bonus["defense_bonus"] > 0:
            shield = int(bonus["defense_bonus"] * 2)
            state.player_defending = True
            msgs.append(f"获得{shield}点护盾（本回合减伤）")

        # 回血效果
        if bonus["hp_regen"] > 0:
            heal = int(bonus["hp_regen"] * 3)
            old_hp = state.player_hp
            state.player_hp = min(state.player_max_hp, state.player_hp + heal)
            actual = state.player_hp - old_hp
            msgs.append(f"恢复{actual}点HP")

        # 回灵效果
        if bonus["lingqi_regen"] > 0:
            regen = int(bonus["lingqi_regen"] * 2)
            state.player_lingqi = min(
                state.player_max_lingqi, state.player_lingqi + regen
            )
            msgs.append(f"恢复{regen}点灵气")

        if not msgs:
            msgs.append("功法施展完毕")

        cost_msg = f"（消耗{gf.lingqi_cost}灵气）" if not is_regen else "（免费）"
        full_msg = f"施展【{gf.name}】{cost_msg}：{'，'.join(msgs)}"
        state.combat_log.append(full_msg)
        return {"success": True, "message": full_msg}

    @staticmethod
    def _apply_item(state: CombatState, player, data: dict) -> dict:
        """战斗中使用物品（丹药）。"""
        item_id = data.get("item_id", "")
        if not item_id:
            return {"success": False, "message": "未指定物品"}

        item_def = ITEM_REGISTRY.get(item_id)
        if not item_def:
            return {"success": False, "message": "物品不存在"}

        count = player.inventory.get(item_id, 0)
        if count <= 0:
            return {"success": False, "message": "背包中没有该物品"}

        # 消耗物品
        player.inventory[item_id] = count - 1
        if player.inventory[item_id] <= 0:
            del player.inventory[item_id]

        # 简单处理：回血丹药
        heal = item_def.effect_value if hasattr(item_def, "effect_value") else 0
        if heal <= 0:
            # 默认回复 20% max_hp
            heal = max(1, state.player_max_hp // 5)

        old_hp = state.player_hp
        state.player_hp = min(state.player_max_hp, state.player_hp + heal)
        actual = state.player_hp - old_hp
        msg = f"使用【{item_def.name}】，恢复{actual}点HP"
        state.combat_log.append(msg)
        return {"success": True, "message": msg}

    @staticmethod
    def _try_flee(state: CombatState, layer: int) -> dict:
        """尝试逃跑。"""
        from .constants import FLEE_BASE_RATES
        layer = max(0, min(layer, len(FLEE_BASE_RATES) - 1))
        flee_rate = FLEE_BASE_RATES[layer]
        if random.random() < flee_rate:
            msg = "你成功逃离了战斗！"
            state.combat_log.append(msg)
            return {"success": True, "fled": True, "message": msg}
        else:
            msg = "逃跑失败！"
            state.combat_log.append(msg)
            return {"success": True, "fled": False, "message": msg}
