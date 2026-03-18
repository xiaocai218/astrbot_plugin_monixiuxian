"""副本（历练）管理系统：5层探索 + 危险事件 + 战斗触发。"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Optional

from .combat import CombatState, CombatEngine
from .constants import (
    REALM_CONFIG, RealmLevel,
    LAYER_PASS_RATES, LAYER_REWARD_TYPES, LAYER_NAMES,
    DANGER_WEIGHTS, DISASTER_OUTCOMES,
    ENEMY_TIERS, COMBAT_MAX_ROUNDS,
    get_realm_name, get_equip_bonus, get_heart_method_bonus,
    get_total_gongfa_bonus, get_player_base_max_lingqi,
)
from .models import Player

LOW_HP_WARNING_TEXT = "你已濒死，仅剩1点生命，建议立刻退出秘境；若再受伤，将会当场陨落。"


@dataclass
class DungeonSession:
    """副本会话。"""
    user_id: str
    current_layer: int = 0
    status: str = "exploring"  # exploring|combat|pvp|layer_complete|failed|dead|exited
    accumulated_rewards: list[dict] = field(default_factory=list)
    combat: Optional[CombatState] = None
    pvp_session_id: Optional[str] = None
    message: str = ""
    started_at: float = field(default_factory=time.time)
    fatal_on_next_damage: bool = False
    low_hp_warning_nonce: int = 0

    def to_dict(self) -> dict:
        return {
            "active": True,
            "current_layer": self.current_layer,
            "total_layers": len(LAYER_NAMES),
            "layer_name": LAYER_NAMES[min(self.current_layer, len(LAYER_NAMES) - 1)],
            "status": self.status,
            "pass_rate": LAYER_PASS_RATES[min(self.current_layer, len(LAYER_PASS_RATES) - 1)],
            "rewards": list(self.accumulated_rewards),
            "combat": self.combat.to_dict() if self.combat else None,
            "pvp_session_id": self.pvp_session_id,
            "message": self.message,
            "started_at": self.started_at,
            "fatal_on_next_damage": self.fatal_on_next_damage,
            "low_hp_warning_nonce": self.low_hp_warning_nonce,
        }


class DungeonManager:
    """副本管理器。"""

    def __init__(self, engine):
        self._engine = engine
        self._sessions: dict[str, DungeonSession] = {}

    def get_session(self, user_id: str) -> Optional[DungeonSession]:
        return self._sessions.get(user_id)

    def has_active_session(self, user_id: str) -> bool:
        s = self._sessions.get(user_id)
        return s is not None and s.status not in ("failed", "dead", "exited")

    @staticmethod
    def _sync_low_hp_warning(session: DungeonSession, hp: int) -> bool:
        hp = max(0, int(hp))
        if hp == 1:
            if not session.fatal_on_next_damage:
                session.fatal_on_next_damage = True
                session.low_hp_warning_nonce += 1
                return True
            return False
        session.fatal_on_next_damage = False
        return False

    @staticmethod
    def _append_low_hp_warning(message: str) -> str:
        if not message:
            return LOW_HP_WARNING_TEXT
        return f"{message}\n{LOW_HP_WARNING_TEXT}"

    @staticmethod
    def _consume_life_talisman(player: Player) -> bool:
        """消耗一张保命符，返回是否成功保命。"""
        count = int(player.inventory.get("life_talisman", 0) or 0)
        if count <= 0:
            return False
        next_count = count - 1
        if next_count > 0:
            player.inventory["life_talisman"] = next_count
        else:
            player.inventory.pop("life_talisman", None)
        player.hp = 1
        return True

    async def _resolve_dungeon_death(
        self,
        player: Player,
        session: DungeonSession,
        *,
        death_message: str,
        talisman_message: str,
        extra_result: Optional[dict] = None,
    ) -> dict:
        """统一处理副本中的致死事件，并接入保命符。"""
        result_base = dict(extra_result or {})
        if self._consume_life_talisman(player):
            session.status = "failed"
            session.combat = None
            session.pvp_session_id = None
            session.fatal_on_next_damage = False
            session.message = talisman_message
            await self._engine._save_player(player)
            result = {
                "success": True,
                "death_prevented": True,
                "life_talisman_used": True,
                "message": session.message,
                "dungeon_state": session.to_dict(),
            }
            result.update(result_base)
            self._cleanup_session(player.user_id)
            return result

        player.hp = 0
        session.status = "dead"
        session.combat = None
        session.pvp_session_id = None
        session.fatal_on_next_damage = False
        session.message = death_message
        death_items = await self._engine.prepare_death(player.user_id)
        result = {
            "success": True,
            "died": True,
            "death_items": death_items,
            "message": session.message,
            "dungeon_state": session.to_dict(),
        }
        result.update(result_base)
        self._cleanup_session(player.user_id)
        return result

    async def start(self, player: Player) -> dict:
        """开始副本探索。"""
        if self._engine.pvp.get_session_for_player(player.user_id):
            return {"success": False, "message": "对战中无法进入副本"}
        if self.has_active_session(player.user_id):
            s = self._sessions[player.user_id]
            return {"success": False, "message": "你已在副本中",
                    "dungeon_state": s.to_dict()}

        session = DungeonSession(user_id=player.user_id, current_layer=0,
                                 status="exploring")
        session.message = f"你踏入了{LAYER_NAMES[0]}，准备开始探索……"
        self._sessions[player.user_id] = session
        return {"success": True, "message": session.message,
                "dungeon_state": session.to_dict()}

    async def advance(self, player: Player) -> dict:
        """前进到下一层：安全通过或触发危险。"""
        session = self._sessions.get(player.user_id)
        if not session or session.status not in ("exploring", "layer_complete"):
            return {"success": False, "message": "你不在副本中或无法前进"}

        layer = session.current_layer

        if self._roll_passage(layer):
            reward = await self._generate_layer_reward(player, layer)
            session.accumulated_rewards.append(reward)
            session.message = (
                f"安全通过{LAYER_NAMES[layer]}！获得奖励：{reward.get('desc', '')}"
            )
            await self._engine._save_player(player)
            if layer >= len(LAYER_NAMES) - 1:
                session.status = "exited"
                session.message += "\n恭喜！你通过了全部五层！"
                result = {
                    "success": True, "passed": True, "danger": "safe",
                    "message": session.message,
                    "dungeon_state": session.to_dict(),
                    "dungeon_complete": True,
                }
                self._cleanup_session(player.user_id)
                return result
            session.current_layer = layer + 1
            session.status = "layer_complete"
            return {
                "success": True, "passed": True, "danger": "safe",
                "message": session.message,
                "dungeon_state": session.to_dict(),
            }

        danger = self._roll_danger()

        if danger == "disaster":
            disaster_result = await self._resolve_disaster(player, session)
            return disaster_result

        if danger == "enemy":
            opponent_id = self._engine.pvp.find_online_opponent(
                player,
                list(self._engine.get_online_user_ids()),
            )
            if opponent_id and random.random() < 0.01:
                opponent = await self._engine.get_player(opponent_id)
                if opponent:
                    pvp_session = self._engine.pvp.create_match(
                        player,
                        opponent,
                        source="dungeon",
                        dungeon_owner_id=player.user_id,
                        dungeon_layer=layer,
                    )
                    session.combat = None
                    session.pvp_session_id = pvp_session.session_id
                    session.status = "pvp"
                    session.message = (
                        f"你在{LAYER_NAMES[layer]}遭遇在线玩家{opponent.name}，"
                        "对方需在10秒内决定是否应战，若避战则你直接夺得本层机缘！"
                    )
                    return {
                        "success": True,
                        "passed": True,
                        "danger": "enemy_player",
                        "message": session.message,
                        "dungeon_state": session.to_dict(),
                        "pvp_session_id": pvp_session.session_id,
                        "pvp_opponent_id": opponent.user_id,
                        "pvp_notice": {
                            "session_id": pvp_session.session_id,
                            "countdown_deadline": pvp_session.countdown_deadline,
                            "challenger_name": player.name,
                            "layer_name": LAYER_NAMES[layer],
                        },
                    }

        enemy_type = "monster" if danger == "monster" else "enemy"
        combat = self._generate_enemy(player, layer, enemy_type)
        session.combat = combat
        session.status = "combat"
        session.message = f"遭遇{combat.enemy_name}（{combat.enemy_realm_name}）！"
        return {
            "success": True, "passed": True, "danger": danger,
            "message": session.message,
            "dungeon_state": session.to_dict(),
        }

    async def combat_action(self, player: Player, action: str,
                            data: dict | None = None) -> dict:
        """处理战斗动作。"""
        session = self._sessions.get(player.user_id)
        if not session or session.status != "combat" or not session.combat:
            return {"success": False, "message": "你不在战斗中"}

        data = data or {}
        data["layer"] = session.current_layer
        combat = session.combat

        # 玩家回合
        p_result = CombatEngine.resolve_player_action(combat, action, player, data)
        if not p_result["success"]:
            return {**p_result, "dungeon_state": session.to_dict()}

        if p_result.get("combat_end"):
            return await self._finish_combat(player, session, p_result)

        # 敌人回合
        e_result = CombatEngine.resolve_enemy_turn(combat)
        combined_msg = p_result.get("message", "")
        if e_result.get("message"):
            combined_msg += "\n" + e_result["message"]

        if e_result.get("combat_end"):
            e_result["message"] = combined_msg
            return await self._finish_combat(player, session, e_result)

        self._sync_player_from_combat(player, combat)
        low_hp_warning = self._sync_low_hp_warning(session, player.hp)
        await self._engine._save_player(player)
        session.message = combined_msg
        if low_hp_warning:
            session.message = self._append_low_hp_warning(session.message)
        result = {
            "success": True, "message": combined_msg,
            "dungeon_state": session.to_dict(),
        }
        if low_hp_warning:
            result["message"] = session.message
            result["low_hp_warning"] = True
        return result

    async def exit_dungeon(self, player: Player) -> dict:
        """见好就收，退出副本。"""
        session = self._sessions.get(player.user_id)
        if not session:
            return {"success": False, "message": "你不在副本中"}
        if session.status in ("combat", "pvp"):
            return {"success": False, "message": "战斗中无法退出，请先结束战斗"}

        session.status = "exited"
        session.message = "你选择见好就收，带着战利品离开了副本"
        result = {
            "success": True, "message": session.message,
            "dungeon_state": session.to_dict(),
            "dungeon_complete": True,
        }
        self._cleanup_session(player.user_id)
        return result

    # ── 内部方法 ──────────────────────────────────────────

    def _roll_passage(self, layer: int) -> bool:
        rate = LAYER_PASS_RATES[min(layer, len(LAYER_PASS_RATES) - 1)]
        return random.random() < rate

    def _roll_danger(self) -> str:
        """掷骰决定危险类型。"""
        roll = random.randint(1, 100)
        if roll <= DANGER_WEIGHTS["disaster"]:
            return "disaster"
        if roll <= DANGER_WEIGHTS["disaster"] + DANGER_WEIGHTS["monster"]:
            return "monster"
        return "enemy"

    async def _resolve_disaster(
        self, player: Player, session: DungeonSession
    ) -> dict:
        """天灾结算。"""
        layer = session.current_layer
        roll = random.randint(1, 100)

        if roll <= DISASTER_OUTCOMES["hp_damage"]:
            # 基于境界基础防御计算天灾伤害减免
            dmg_ratio = random.uniform(0.15, 0.35)
            min_ratio = random.uniform(0.01, 0.03)
            realm_cfg = REALM_CONFIG.get(player.realm,
                                          REALM_CONFIG[RealmLevel.MORTAL])
            base_def = realm_cfg["base_defense"]
            equip_bonus = get_equip_bonus(player.weapon, player.armor)
            hm_bonus = get_heart_method_bonus(player.heart_method,
                                              player.heart_method_mastery)
            gf_bonus = get_total_gongfa_bonus(player)
            from .pills import get_effective_combat_stats
            effective_stats = get_effective_combat_stats(player)
            total_def = max(1, effective_stats["defense"] + equip_bonus["defense"]
                           + hm_bonus["defense_bonus"]
                           + gf_bonus["defense_bonus"])
            mitigated_to_floor = False
            if total_def > base_def > 0:
                excess_ratio = (total_def - base_def) / base_def
                final_ratio = dmg_ratio - excess_ratio
                if final_ratio < min_ratio:
                    final_ratio = min_ratio
                    mitigated_to_floor = True
                dmg = max(1, int(effective_stats["max_hp"] * final_ratio))
            else:
                dmg = max(1, int(effective_stats["max_hp"] * dmg_ratio))

            # 有实际伤害且濒死 → 判定死亡
            if dmg > 0 and (effective_stats["hp"] <= 1 or session.fatal_on_next_damage):
                return await self._resolve_dungeon_death(
                    player,
                    session,
                    death_message="你本已命悬一线，又遭天灾重创，当场陨落……",
                    talisman_message="天灾本该夺你性命，所幸保命符替你挡下一劫。你拖着残躯逃出秘境，历练结束。",
                    extra_result={
                        "passed": True,
                        "danger": "disaster",
                    },
                )

            new_effective_hp = max(1, effective_stats["hp"] - dmg)
            player.hp = max(1, min(player.max_hp, new_effective_hp - effective_stats["hp_delta"]))
            low_hp_warning = self._sync_low_hp_warning(session, player.hp)
            reward = await self._generate_layer_reward(player, layer)
            session.accumulated_rewards.append(reward)
            if mitigated_to_floor:
                session.message = (
                    f"遭遇天灾！凭借深厚的防御，你将伤害压制到最低，"
                    f"仍受到{dmg}点伤害，HP: {new_effective_hp}/{effective_stats['max_hp']}。"
                    f"你强撑着穿过险境，获得奖励：{reward.get('desc', '')}"
                )
            else:
                session.message = (
                    f"遭遇天灾！受到{dmg}点伤害，HP: {new_effective_hp}/{effective_stats['max_hp']}。"
                    f"你强撑着穿过险境，获得奖励：{reward.get('desc', '')}"
                )
            if low_hp_warning:
                session.message = self._append_low_hp_warning(session.message)
            await self._engine._save_player(player)
            if layer >= len(LAYER_NAMES) - 1:
                session.status = "exited"
                session.message += "\n通过了全部五层！"
                result = {
                    "success": True, "passed": True, "danger": "disaster",
                    "message": session.message,
                    "dungeon_state": session.to_dict(),
                    "dungeon_complete": True,
                }
                if low_hp_warning:
                    result["low_hp_warning"] = True
                self._cleanup_session(player.user_id)
                return result
            session.current_layer = layer + 1
            session.status = "layer_complete"
            result = {
                "success": True, "passed": True, "danger": "disaster",
                "message": session.message,
                "dungeon_state": session.to_dict(),
            }
            if low_hp_warning:
                result["low_hp_warning"] = True
            return result

        if roll <= DISASTER_OUTCOMES["hp_damage"] + DISASTER_OUTCOMES["realm_drop"]:
            from .adventure import _drop_realm_steps, _rebuild_stats_by_realm

            old_name = get_realm_name(player.realm, player.sub_realm)
            actual = _drop_realm_steps(player, random.randint(1, 3))
            _rebuild_stats_by_realm(player)
            self._engine._auto_unequip_invalid_equipment(player)
            self._engine._auto_unequip_invalid_heart_method(
                player, convert_ratio=0.6, force=False
            )
            new_name = get_realm_name(player.realm, player.sub_realm)
            session.message = f"天灾降临！修为跌落{actual}层：{old_name} → {new_name}"
            session.status = "failed"
            await self._engine._save_player(player)
            result = {
                "success": True, "passed": True, "danger": "disaster",
                "realm_changed": True,
                "message": session.message,
                "dungeon_state": session.to_dict(),
            }
            self._cleanup_session(player.user_id)
            return result

        return await self._resolve_dungeon_death(
            player,
            session,
            death_message="天灾降临，不幸陨落……",
            talisman_message="天灾降临之际，保命符替你扛下死劫。你侥幸逃出生天，历练结束。",
            extra_result={
                "passed": True,
                "danger": "disaster",
            },
        )

    def _generate_enemy(self, player: Player, layer: int,
                        enemy_type: str) -> CombatState:
        """生成敌人战斗状态。"""
        equip_bonus = get_equip_bonus(player.weapon, player.armor)
        hm_bonus = get_heart_method_bonus(player.heart_method,
                                          player.heart_method_mastery)
        gf_bonus = get_total_gongfa_bonus(player)
        from .pills import get_effective_combat_stats
        effective_stats = get_effective_combat_stats(player)
        p_atk = max(1, effective_stats["attack"] + equip_bonus["attack"]
                    + hm_bonus["attack_bonus"] + gf_bonus["attack_bonus"])
        p_def = max(1, effective_stats["defense"] + equip_bonus["defense"]
                    + hm_bonus["defense_bonus"] + gf_bonus["defense_bonus"])

        # 选择敌人难度
        roll = random.random()
        cumulative = 0.0
        scale = 0.7
        for prob, tier_scale in ENEMY_TIERS:
            cumulative += prob
            if roll < cumulative:
                if tier_scale == "realm_up":
                    # 高1大境界
                    enemy_realm = min(player.realm + 1, RealmLevel.MAHAYANA)
                    cfg = REALM_CONFIG.get(enemy_realm,
                                           REALM_CONFIG[RealmLevel.QI_REFINING])
                    e_atk = int(cfg["base_attack"] * random.uniform(0.9, 1.1))
                    e_def = int(cfg["base_defense"] * random.uniform(0.9, 1.1))
                    e_hp = int(cfg["base_hp"] * random.uniform(0.9, 1.1))
                    realm_name = get_realm_name(enemy_realm, 0)
                else:
                    e_atk = max(1, int(p_atk * tier_scale
                                       * random.uniform(0.9, 1.1)))
                    e_def = max(1, int(p_def * tier_scale
                                       * random.uniform(0.9, 1.1)))
                    e_hp = max(1, int(player.max_hp * tier_scale
                                      * random.uniform(0.9, 1.1)))
                    realm_name = get_realm_name(player.realm, player.sub_realm)
                scale = tier_scale
                break
        else:
            e_atk = max(1, int(p_atk * 0.7))
            e_def = max(1, int(p_def * 0.7))
            e_hp = max(1, int(player.max_hp * 0.7))
            realm_name = get_realm_name(player.realm, player.sub_realm)

        # 敌人名称
        monster_names = ["妖兽", "魔物", "凶兽", "邪灵", "鬼修"]
        enemy_names = ["散修", "魔修", "邪道修士", "独行侠", "赏金猎人"]
        if enemy_type == "monster":
            name = random.choice(monster_names)
        else:
            name = random.choice(enemy_names)

        return CombatState(
            player_hp=effective_stats["hp"],
            player_max_hp=effective_stats["max_hp"],
            player_attack=p_atk,
            player_defense=p_def,
            player_lingqi=effective_stats["lingqi"],
            player_max_lingqi=effective_stats["max_lingqi"],
            enemy_name=name,
            enemy_type=enemy_type,
            enemy_hp=e_hp,
            enemy_max_hp=e_hp,
            enemy_attack=e_atk,
            enemy_defense=e_def,
            enemy_realm_name=realm_name,
            round_number=0,
            max_rounds=COMBAT_MAX_ROUNDS,
        )

    async def _generate_layer_reward(self, player: Player,
                                     layer: int) -> dict:
        """生成层级奖励。"""
        reward_type = LAYER_REWARD_TYPES[min(layer,
                                             len(LAYER_REWARD_TYPES) - 1)]
        reward: dict = {"type": reward_type, "layer": layer}

        if reward_type == "spirit_stones":
            amount = random.randint(100, 300) * (layer + 1)
            player.spirit_stones += amount
            reward["value"] = amount
            reward["desc"] = f"灵石 +{amount}"

        elif reward_type == "equipment":
            from .adventure import _apply_equip_drop
            equip = await _apply_equip_drop(player)
            if equip:
                reward["desc"] = f"装备 {equip['tier_name']}【{equip['name']}】"
            else:
                amount = random.randint(150, 400) * (layer + 1)
                player.spirit_stones += amount
                reward["value"] = amount
                reward["desc"] = f"灵石 +{amount}（装备池空）"

        elif reward_type == "pills":
            from .pills import (
                pick_random_pill, DUNGEON_PILL_TIER_WEIGHTS, DUNGEON_PILL_GRADE_WEIGHTS,
            )
            from .inventory import add_item as _add_item
            pill = pick_random_pill(random.Random(), DUNGEON_PILL_TIER_WEIGHTS, DUNGEON_PILL_GRADE_WEIGHTS)
            if pill:
                await _add_item(player, pill.pill_id)
                from .pills import PILL_TIER_NAMES, PILL_GRADE_NAMES
                tier_name = PILL_TIER_NAMES.get(pill.tier, "")
                grade_name = PILL_GRADE_NAMES.get(pill.grade, "")
                reward["desc"] = f"丹药 +{tier_name}{grade_name}【{pill.name}】"
            else:
                from .adventure import _apply_pill
                pill_name = await _apply_pill(player)
                reward["desc"] = f"丹药 +{pill_name}"

        elif reward_type == "heart_method":
            from .adventure import _apply_heart_method_drop
            hm = await _apply_heart_method_drop(player, 1.2)
            if hm:
                reward["desc"] = f"心法秘籍 +【{hm['manual_name']}】"
            else:
                amount = random.randint(200, 500) * (layer + 1)
                player.spirit_stones += amount
                reward["value"] = amount
                reward["desc"] = f"灵石 +{amount}（未掉落心法）"

        elif reward_type == "gongfa":
            from .adventure import _apply_gongfa_drop
            gf = await _apply_gongfa_drop(player)
            if gf:
                reward["desc"] = f"功法卷轴 +{gf['tier_name']}【{gf['name']}】"
            else:
                amount = random.randint(300, 600) * (layer + 1)
                player.spirit_stones += amount
                reward["value"] = amount
                reward["desc"] = f"灵石 +{amount}（未掉落功法）"

        return reward

    async def _finish_combat(self, player: Player,
                             session: DungeonSession,
                             combat_result: dict) -> dict:
        """战斗结束处理。"""
        outcome = combat_result.get("outcome", "")
        layer = session.current_layer
        combat = session.combat

        if outcome == "win":
            low_hp_warning = False
            if combat:
                self._sync_player_from_combat(player, combat)
                low_hp_warning = self._sync_low_hp_warning(session, player.hp)
            result = await self._complete_layer_victory(
                player,
                session,
                combat.enemy_name if combat else "敌人",
            )
            if low_hp_warning:
                session.message = self._append_low_hp_warning(session.message)
                result["message"] = session.message
                result["dungeon_state"] = session.to_dict()
                result["low_hp_warning"] = True
            return result

        elif outcome == "lose":
            if combat:
                self._sync_player_from_combat(player, combat)
            enemy_name = combat.enemy_name if combat else "敌人"
            return await self._resolve_dungeon_death(
                player,
                session,
                death_message=f"被{enemy_name}击败，不幸陨落……",
                talisman_message=f"被{enemy_name}击败之际，保命符替你扛下死劫。你重伤退出秘境，历练结束。",
            )

        elif outcome == "flee":
            if combat:
                self._sync_player_from_combat(player, combat)
            session.combat = None
            session.status = "failed"
            session.message = "逃离战斗，历练结束，保留已获奖励"
            await self._engine._save_player(player)
            result = {
                "success": True,
                "message": session.message,
                "dungeon_state": session.to_dict(),
            }
            self._cleanup_session(player.user_id)
            return result

        else:  # timeout
            if combat:
                self._sync_player_from_combat(player, combat)
            session.combat = None
            session.status = "failed"
            session.message = "战斗超时，双方脱离，历练结束"
            await self._engine._save_player(player)
            result = {
                "success": True,
                "message": session.message,
                "dungeon_state": session.to_dict(),
            }
            self._cleanup_session(player.user_id)
            return result

    async def resolve_pvp_result(self, pvp_session) -> dict | None:
        """把副本中的在线玩家遭遇结算回副本流程。"""
        dungeon_user_id = getattr(pvp_session, "dungeon_owner_id", None)
        if getattr(pvp_session, "source", "") != "dungeon" or not dungeon_user_id:
            return None

        session = self._sessions.get(dungeon_user_id)
        if not session or session.pvp_session_id != pvp_session.session_id:
            return None

        player = self._engine._players.get(dungeon_user_id)
        if not player:
            self._cleanup_session(dungeon_user_id)
            return None

        enemy_name = (
            pvp_session.state_a.enemy_name
            if pvp_session.player_a_id == dungeon_user_id
            else pvp_session.state_b.enemy_name
        )
        session.pvp_session_id = None

        if pvp_session.end_reason in {"challenge_timeout", "challenge_rejected"}:
            result = await self._complete_layer_victory(
                player,
                session,
                f"避战的在线玩家{enemy_name}",
            )
            reward_desc = ""
            if session.accumulated_rewards:
                reward_desc = str(session.accumulated_rewards[-1].get("desc", ""))
            if pvp_session.end_reason == "challenge_timeout":
                reason_text = "10秒内未应战"
            else:
                reason_text = "拒绝应战"
            session.message = f"在线玩家{enemy_name}{reason_text}，你顺势夺得：{reward_desc}"
            if result.get("dungeon_complete"):
                session.message += "\n通过了全部五层！"
            result["message"] = session.message
            if result.get("dungeon_state"):
                result["dungeon_state"]["message"] = session.message
            return result

        if pvp_session.winner_id == dungeon_user_id:
            return await self._complete_layer_victory(
                player,
                session,
                f"在线玩家{enemy_name}",
            )

        if player.hp <= 0:
            session.status = "dead"
            session.message = "你在秘境遭遇战中陨落……"
        elif pvp_session.winner_id is None:
            session.status = "failed"
            session.message = "你与在线对手鏖战许久仍未分出胜负，历练被迫中止"
        elif pvp_session.end_reason == "flee":
            session.status = "failed"
            session.message = "你在秘境遭遇战中献上物品脱身，历练结束"
        else:
            session.status = "failed"
            session.message = f"你被在线玩家{enemy_name}击败，历练结束"

        snapshot = session.to_dict()
        result = {
            "success": True,
            "message": session.message,
            "dungeon_state": snapshot,
        }
        pending = self._engine._pending_deaths.get(player.user_id)
        if player.hp <= 0 and pending:
            result["died"] = True
            result["death_items"] = list(pending.get("items", []))
        self._cleanup_session(dungeon_user_id)
        return result

    async def _complete_layer_victory(
        self,
        player: Player,
        session: DungeonSession,
        enemy_name: str,
    ) -> dict:
        """统一处理战斗 / PvP 胜利后的副本推进。"""
        layer = session.current_layer
        reward = await self._generate_layer_reward(player, layer)
        session.accumulated_rewards.append(reward)
        session.combat = None
        session.pvp_session_id = None
        await self._engine._save_player(player)

        if layer >= len(LAYER_NAMES) - 1:
            session.status = "exited"
            session.message = (
                f"击败{enemy_name}！获得：{reward.get('desc', '')}\n通过了全部五层！"
            )
            result = {
                "success": True,
                "message": session.message,
                "dungeon_state": session.to_dict(),
                "dungeon_complete": True,
            }
            self._cleanup_session(player.user_id)
            return result

        session.current_layer = layer + 1
        session.status = "layer_complete"
        session.message = f"击败{enemy_name}！获得：{reward.get('desc', '')}"
        return {
            "success": True,
            "message": session.message,
            "dungeon_state": session.to_dict(),
        }

    def _cleanup_session(self, user_id: str):
        """清理已结束的会话。"""
        self._sessions.pop(user_id, None)

    def remove_session(self, user_id: str):
        """外部调用：彻底移除会话。"""
        self._sessions.pop(user_id, None)

    @staticmethod
    def _sync_player_from_combat(player: Player, combat: CombatState):
        """把战斗中的实时生命/灵气同步回玩家。"""
        base_max_lingqi = get_player_base_max_lingqi(player)
        hp_delta = combat.player_max_hp - player.max_hp
        lingqi_delta = combat.player_max_lingqi - base_max_lingqi
        player.hp = max(0, min(player.max_hp, combat.player_hp - hp_delta))
        player.lingqi = max(0, min(base_max_lingqi, combat.player_lingqi - lingqi_delta))
