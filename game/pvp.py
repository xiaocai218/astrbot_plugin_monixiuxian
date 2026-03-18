"""实时 PvP 对战系统。"""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from .combat import CombatEngine, CombatState
from .constants import (
    COMBAT_MAX_ROUNDS,
    GONGFA_REGISTRY,
    ITEM_REGISTRY,
    PVP_ROUND_TIMEOUT,
    get_daily_recycle_price,
    get_equip_bonus,
    get_gongfa_bonus,
    get_heart_method_bonus,
    get_player_base_max_lingqi,
    get_realm_name,
    get_total_gongfa_bonus,
)
from .inventory import add_item
from .models import Player

PVP_CHALLENGE_DELAY = 10
PVP_FLEE_DIRECT_VALUE = 1000


@dataclass
class PvPSession:
    """PvP 对战会话。"""

    session_id: str
    player_a_id: str
    player_b_id: str
    state_a: CombatState
    state_b: CombatState
    action_a: Optional[dict] = None
    action_b: Optional[dict] = None
    round_number: int = 0
    round_deadline: float = 0.0
    status: str = "pending"  # pending | waiting | resolving | ended
    winner_id: Optional[str] = None
    combat_log: list[str] = field(default_factory=list)
    source: str = "dungeon"
    dungeon_owner_id: Optional[str] = None
    dungeon_layer: Optional[int] = None
    countdown_deadline: float = 0.0
    flee_offer: Optional[dict] = None
    end_reason: str = ""
    result_message: str = ""

    def to_dict(self, viewer_id: str) -> dict:
        """返回某一方视角的状态。"""
        is_a = viewer_id == self.player_a_id
        my_state = self.state_a if is_a else self.state_b
        opp_state = self.state_b if is_a else self.state_a
        player_a_name = self.state_b.enemy_name
        player_a_realm = self.state_b.enemy_realm_name
        player_b_name = self.state_a.enemy_name
        player_b_realm = self.state_a.enemy_realm_name
        my_name = player_a_name if is_a else player_b_name
        my_realm = player_a_realm if is_a else player_b_realm
        opponent_name = my_state.enemy_name
        opponent_realm = my_state.enemy_realm_name
        offer = None
        if self.flee_offer:
            requester_id = str(self.flee_offer.get("requester_id", ""))
            offer = {
                "requester_id": requester_id,
                "requester_name": str(self.flee_offer.get("requester_name", "")),
                "items": [dict(item) for item in self.flee_offer.get("items", [])],
                "total_value": int(self.flee_offer.get("total_value", 0) or 0),
                "requires_approval": bool(self.flee_offer.get("requires_approval", False)),
                "awaiting_my_response": bool(requester_id and viewer_id != requester_id),
                "awaiting_opponent_response": bool(requester_id and viewer_id == requester_id),
            }
        return {
            "session_id": self.session_id,
            "round_number": self.round_number,
            "round_deadline": self.round_deadline,
            "countdown_deadline": self.countdown_deadline,
            "status": self.status,
            "winner_id": self.winner_id,
            "source": self.source,
            "dungeon_layer": self.dungeon_layer,
            "viewer_id": viewer_id,
            "viewer_side": "a" if is_a else "b",
            "challenge_role": "challenger" if is_a else "defender",
            "is_dungeon_owner": viewer_id == self.dungeon_owner_id,
            "player_id": self.player_a_id if is_a else self.player_b_id,
            "player_name": my_name,
            "player_realm": my_realm,
            "my_action_submitted": (self.action_a is not None) if is_a else (self.action_b is not None),
            "opponent_action_submitted": (self.action_b is not None) if is_a else (self.action_a is not None),
            "player_hp": my_state.player_hp,
            "player_max_hp": my_state.player_max_hp,
            "player_attack": my_state.player_attack,
            "player_defense": my_state.player_defense,
            "player_lingqi": my_state.player_lingqi,
            "player_max_lingqi": my_state.player_max_lingqi,
            "opponent_id": self.player_b_id if is_a else self.player_a_id,
            "opponent_hp": opp_state.player_hp,
            "opponent_max_hp": opp_state.player_max_hp,
            "opponent_name": opponent_name,
            "opponent_realm": opponent_realm,
            "player_a_id": self.player_a_id,
            "player_a_name": player_a_name,
            "player_a_realm": player_a_realm,
            "player_a_hp": self.state_a.player_hp,
            "player_a_max_hp": self.state_a.player_max_hp,
            "player_a_attack": self.state_a.player_attack,
            "player_a_defense": self.state_a.player_defense,
            "player_a_lingqi": self.state_a.player_lingqi,
            "player_a_max_lingqi": self.state_a.player_max_lingqi,
            "player_b_id": self.player_b_id,
            "player_b_name": player_b_name,
            "player_b_realm": player_b_realm,
            "player_b_hp": self.state_b.player_hp,
            "player_b_max_hp": self.state_b.player_max_hp,
            "player_b_attack": self.state_b.player_attack,
            "player_b_defense": self.state_b.player_defense,
            "player_b_lingqi": self.state_b.player_lingqi,
            "player_b_max_lingqi": self.state_b.player_max_lingqi,
            "combat_log": list(self.combat_log[-20:]),
            "flee_direct_value": PVP_FLEE_DIRECT_VALUE,
            "flee_offer": offer,
            "end_reason": self.end_reason,
            "result_message": self.result_message,
        }


class PvPManager:
    """PvP 对战管理器。"""

    def __init__(self, engine):
        self._engine = engine
        self._sessions: dict[str, PvPSession] = {}
        self._player_sessions: dict[str, str] = {}

    def get_session_for_player(self, user_id: str) -> Optional[PvPSession]:
        sid = self._player_sessions.get(user_id)
        if not sid:
            return None
        session = self._sessions.get(sid)
        return self._activate_if_due(session)

    def create_match(
        self,
        player_a: Player,
        player_b: Player,
        *,
        source: str = "dungeon",
        dungeon_owner_id: Optional[str] = None,
        dungeon_layer: Optional[int] = None,
        countdown_seconds: int = PVP_CHALLENGE_DELAY,
    ) -> PvPSession:
        """创建 PvP 对战。"""
        session_id = str(uuid.uuid4())[:8]

        def _build_state(player: Player, opponent: Player) -> CombatState:
            equip_bonus = get_equip_bonus(player.weapon, player.armor)
            heart_bonus = get_heart_method_bonus(player.heart_method, player.heart_method_mastery)
            gongfa_bonus = get_total_gongfa_bonus(player)
            max_lingqi = get_player_base_max_lingqi(player)
            return CombatState(
                player_hp=player.hp,
                player_max_hp=player.max_hp,
                player_attack=max(
                    1,
                    player.attack
                    + equip_bonus["attack"]
                    + heart_bonus["attack_bonus"]
                    + gongfa_bonus["attack_bonus"],
                ),
                player_defense=max(
                    1,
                    player.defense
                    + equip_bonus["defense"]
                    + heart_bonus["defense_bonus"]
                    + gongfa_bonus["defense_bonus"],
                ),
                player_lingqi=min(player.lingqi, max_lingqi),
                player_max_lingqi=max_lingqi,
                enemy_name=opponent.name,
                enemy_type="player",
                enemy_hp=0,
                enemy_max_hp=0,
                enemy_attack=0,
                enemy_defense=0,
                enemy_realm_name=get_realm_name(opponent.realm, opponent.sub_realm),
            )

        now = time.time()
        status = "pending" if countdown_seconds > 0 else "waiting"
        session = PvPSession(
            session_id=session_id,
            player_a_id=player_a.user_id,
            player_b_id=player_b.user_id,
            state_a=_build_state(player_a, player_b),
            state_b=_build_state(player_b, player_a),
            round_deadline=0.0 if status == "pending" else now + PVP_ROUND_TIMEOUT,
            status=status,
            source=source,
            dungeon_owner_id=dungeon_owner_id,
            dungeon_layer=dungeon_layer,
            countdown_deadline=now + max(0, countdown_seconds),
        )
        if status == "pending":
            session.combat_log.append(
                f"杀机已现，对手需在{countdown_seconds}秒内应战，逾期视为放弃"
            )
        self._sessions[session_id] = session
        self._player_sessions[player_a.user_id] = session_id
        self._player_sessions[player_b.user_id] = session_id
        return session

    def activate_session(self, session_id: str) -> dict | None:
        """显式激活待应战的对战。"""
        session = self._sessions.get(session_id)
        if not session or session.status == "ended":
            return None
        if session.status == "pending":
            session.status = "waiting"
            session.countdown_deadline = 0.0
            session.round_deadline = time.time() + PVP_ROUND_TIMEOUT
            if not session.combat_log or session.combat_log[-1] != "生死对局开始！":
                session.combat_log.append("生死对局开始！")
        if session.status != "waiting":
            return None
        return {
            "success": True,
            "pvp_state_a": session.to_dict(session.player_a_id),
            "pvp_state_b": session.to_dict(session.player_b_id),
        }

    def _finish_pending_challenge(
        self,
        session: PvPSession,
        *,
        end_reason: str,
        result_message: str,
    ) -> dict:
        """结束尚未开打的遭遇战。"""
        session.status = "ended"
        session.winner_id = None
        session.end_reason = end_reason
        session.result_message = result_message
        session.countdown_deadline = 0.0
        session.round_deadline = 0.0
        session.action_a = None
        session.action_b = None
        session.flee_offer = None
        session.combat_log.append(result_message)
        return self._build_resolve_payload(session, message=result_message)

    def respond_challenge(
        self,
        session_id: str,
        user_id: str,
        accept: bool,
        player: Optional[Player] = None,
    ) -> dict:
        """回应秘境遭遇战邀请。"""
        session = self._sessions.get(session_id)
        if not session or session.status == "ended":
            return {"success": False, "message": "对战已结束或不存在"}
        if not player:
            return {"success": False, "message": "角色不存在"}
        if user_id != session.player_b_id:
            return {"success": False, "message": "只有被遭遇的在线玩家才能决定是否应战"}
        if session.status != "pending":
            return {"success": False, "message": "当前遭遇战已开始或已结束"}

        challenger_name = session.state_b.enemy_name or "秘境修士"
        if session.countdown_deadline and time.time() >= session.countdown_deadline:
            return self._finish_pending_challenge(
                session,
                end_reason="challenge_timeout",
                result_message=f"{player.name}未在10秒内应战，{challenger_name}顺势夺得了秘境机缘。",
            )

        if not accept:
            return self._finish_pending_challenge(
                session,
                end_reason="challenge_rejected",
                result_message=f"{player.name}选择避战，{challenger_name}顺势夺得了秘境机缘。",
            )

        payload = self.activate_session(session_id)
        if not payload:
            return {"success": False, "message": "对战启动失败，请稍后重试"}
        payload["started"] = True
        payload["message"] = "已接受遭遇战，生死对局开始！"
        payload["session_id"] = session_id
        return payload

    def expire_challenge(self, session_id: str) -> dict | None:
        """挑战超时后自动结束。"""
        session = self._sessions.get(session_id)
        if not session or session.status != "pending":
            return None
        if session.countdown_deadline and time.time() < session.countdown_deadline:
            return None
        challenger_name = session.state_b.enemy_name or "秘境修士"
        defender_name = session.state_a.enemy_name or "在线玩家"
        return self._finish_pending_challenge(
            session,
            end_reason="challenge_timeout",
            result_message=f"{defender_name}未在10秒内应战，{challenger_name}顺势夺得了秘境机缘。",
        )

    async def submit_action(
        self, session_id: str, user_id: str, action: dict, player: Optional[Player] = None
    ) -> dict:
        """提交 PvP 动作。"""
        session = self._activate_if_due(self._sessions.get(session_id))
        if not session or session.status == "ended":
            return {"success": False, "message": "对战已结束或不存在"}
        if not player:
            return {"success": False, "message": "角色不存在"}
        if session.status == "pending":
            return {"success": False, "message": "对战尚未开始，请等待倒计时结束"}
        if session.status != "waiting":
            return {"success": False, "message": "当前回合正在结算，请稍后"}
        if session.flee_offer:
            return {"success": False, "message": "当前有逃跑请求待处理"}

        normalized = self._normalize_action(session, user_id, player, action)
        if not normalized["success"]:
            return normalized
        action_payload = normalized["action"]

        if user_id == session.player_a_id:
            if session.action_a is not None:
                return {"success": False, "message": "你已提交本回合动作"}
            session.action_a = action_payload
        elif user_id == session.player_b_id:
            if session.action_b is not None:
                return {"success": False, "message": "你已提交本回合动作"}
            session.action_b = action_payload
        else:
            return {"success": False, "message": "你不在这场对战中"}

        if session.action_a is not None and session.action_b is not None:
            return await self._resolve_round(session)

        return {
            "success": True,
            "message": "动作已提交，等待对手",
            "pvp_state": session.to_dict(user_id),
        }

    async def submit_flee_request(
        self,
        session_id: str,
        user_id: str,
        offer_items: list[dict],
        player: Optional[Player] = None,
    ) -> dict:
        """提交逃跑献祭。"""
        session = self._activate_if_due(self._sessions.get(session_id))
        if not session or session.status == "ended":
            return {"success": False, "message": "对战已结束或不存在"}
        if not player:
            return {"success": False, "message": "角色不存在"}
        if session.status == "pending":
            return {"success": False, "message": "对战尚未开始，不能提前逃跑"}
        if session.status != "waiting":
            return {"success": False, "message": "当前回合正在结算，请稍后"}
        if session.flee_offer:
            return {"success": False, "message": "已有逃跑请求待处理"}
        if user_id not in (session.player_a_id, session.player_b_id):
            return {"success": False, "message": "你不在这场对战中"}

        if user_id == session.player_a_id and session.action_a is not None:
            return {"success": False, "message": "本回合已出招，无法再申请逃跑"}
        if user_id == session.player_b_id and session.action_b is not None:
            return {"success": False, "message": "本回合已出招，无法再申请逃跑"}

        normalized = self._normalize_flee_items(player, offer_items)
        if not normalized["success"]:
            return normalized

        offer = normalized["offer"]
        offer["requester_id"] = user_id
        offer["requester_name"] = player.name
        if offer["total_value"] >= PVP_FLEE_DIRECT_VALUE:
            return await self._resolve_flee(session, user_id, offer, auto_approved=True)

        session.flee_offer = offer
        session.combat_log.append(
            f"{player.name}申请逃跑，愿献上{self._format_offer_items(offer['items'])}"
            f"（合计{offer['total_value']}灵石）"
        )
        return {
            "success": True,
            "message": (
                f"逃跑请求已发出，当前献祭价值{offer['total_value']}，"
                "尚未达到1000，需对方同意"
            ),
            "pvp_state_a": session.to_dict(session.player_a_id),
            "pvp_state_b": session.to_dict(session.player_b_id),
            "ended": False,
            "state_changed": True,
        }

    async def respond_flee_request(
        self, session_id: str, user_id: str, accept: bool, player: Optional[Player] = None
    ) -> dict:
        """回应逃跑请求。"""
        session = self._activate_if_due(self._sessions.get(session_id))
        if not session or session.status == "ended":
            return {"success": False, "message": "对战已结束或不存在"}
        if not player:
            return {"success": False, "message": "角色不存在"}
        if session.status == "pending":
            return {"success": False, "message": "对战尚未开始"}
        if session.status != "waiting":
            return {"success": False, "message": "当前回合正在结算，请稍后"}
        if not session.flee_offer:
            return {"success": False, "message": "当前没有待处理的逃跑请求"}

        requester_id = str(session.flee_offer.get("requester_id", ""))
        if user_id == requester_id:
            return {"success": False, "message": "不能回应自己发出的逃跑请求"}
        if user_id not in (session.player_a_id, session.player_b_id):
            return {"success": False, "message": "你不在这场对战中"}

        if accept:
            return await self._resolve_flee(session, requester_id, session.flee_offer, auto_approved=False)

        requester_name = str(session.flee_offer.get("requester_name", "对手"))
        session.flee_offer = None
        session.combat_log.append(f"{player.name}拒绝了{requester_name}的逃跑请求")
        return {
            "success": True,
            "message": "你拒绝了对方的逃跑请求，对局继续",
            "pvp_state_a": session.to_dict(session.player_a_id),
            "pvp_state_b": session.to_dict(session.player_b_id),
            "ended": False,
            "state_changed": True,
        }

    async def _resolve_round(self, session: PvPSession) -> dict:
        """双方同时结算。"""
        player_a = self._engine._players.get(session.player_a_id)
        player_b = self._engine._players.get(session.player_b_id)
        if not player_a or not player_b:
            session.status = "ended"
            session.winner_id = None
            session.end_reason = "invalid"
            session.result_message = "对战角色不存在，已自动结束"
            return {
                "success": False,
                "resolved": False,
                "message": session.result_message,
            }

        session.round_number += 1
        session.status = "resolving"

        action_a = session.action_a or {"action": "defend"}
        action_b = session.action_b or {"action": "defend"}
        plan_a = self._build_action_plan(session.state_a, player_a, action_a)
        plan_b = self._build_action_plan(session.state_b, player_b, action_b)

        if plan_a["heal"]:
            session.state_a.player_hp = min(
                session.state_a.player_max_hp,
                session.state_a.player_hp + plan_a["heal"],
            )
        if plan_b["heal"]:
            session.state_b.player_hp = min(
                session.state_b.player_max_hp,
                session.state_b.player_hp + plan_b["heal"],
            )

        if plan_a["lingqi_regen"]:
            session.state_a.player_lingqi = min(
                session.state_a.player_max_lingqi,
                session.state_a.player_lingqi + plan_a["lingqi_regen"],
            )
        if plan_b["lingqi_regen"]:
            session.state_b.player_lingqi = min(
                session.state_b.player_max_lingqi,
                session.state_b.player_lingqi + plan_b["lingqi_regen"],
            )

        damage_a = self._calc_pvp_damage(
            plan_a["attack_power"],
            session.state_b.player_defense,
            plan_b["defending"],
        )
        damage_b = self._calc_pvp_damage(
            plan_b["attack_power"],
            session.state_a.player_defense,
            plan_a["defending"],
        )

        session.state_b.player_hp = max(0, session.state_b.player_hp - damage_a)
        session.state_a.player_hp = max(0, session.state_a.player_hp - damage_b)

        session.combat_log.extend(
            [
                f"第{session.round_number}回合",
                f"{player_a.name}：{plan_a['summary']}，造成{damage_a}点伤害",
                f"{player_b.name}：{plan_b['summary']}，造成{damage_b}点伤害",
            ]
        )

        session.action_a = None
        session.action_b = None

        extras = {
            session.player_a_id: {},
            session.player_b_id: {},
        }
        skip_save: set[str] = set()

        knocked_out_a = session.state_a.player_hp <= 0
        knocked_out_b = session.state_b.player_hp <= 0
        if knocked_out_a or knocked_out_b:
            session.status = "ended"
            session.end_reason = "knockout"
            if knocked_out_a and knocked_out_b:
                session.winner_id = None
                session.result_message = "双方同归于尽！"
            elif knocked_out_a:
                session.winner_id = session.player_b_id
                session.result_message = f"{player_b.name}获胜！"
            else:
                session.winner_id = session.player_a_id
                session.result_message = f"{player_a.name}获胜！"

            if knocked_out_a:
                extra_a = await self._apply_player_defeat(session.state_a, player_a)
                extras[session.player_a_id].update(extra_a)
                if extra_a.get("death_prevented"):
                    session.combat_log.append(f"{player_a.name}触发保命符，保住了一命！")
                elif extra_a.get("died"):
                    session.combat_log.append(f"{player_a.name}当场陨落！")
            if knocked_out_b:
                extra_b = await self._apply_player_defeat(session.state_b, player_b)
                extras[session.player_b_id].update(extra_b)
                if extra_b.get("death_prevented"):
                    session.combat_log.append(f"{player_b.name}触发保命符，保住了一命！")
                elif extra_b.get("died"):
                    session.combat_log.append(f"{player_b.name}当场陨落！")

            session.combat_log.append(session.result_message)
            skip_save = {
                uid
                for uid, extra in extras.items()
                if extra.get("died") and not extra.get("death_prevented")
            }
            await self._sync_players(player_a, player_b, session, skip_save_user_ids=skip_save)

        elif session.round_number >= COMBAT_MAX_ROUNDS:
            session.status = "ended"
            session.end_reason = "timeout"
            if session.state_a.player_hp > session.state_b.player_hp:
                session.winner_id = session.player_a_id
                session.result_message = f"回合耗尽，{player_a.name}以血量优势获胜！"
            elif session.state_b.player_hp > session.state_a.player_hp:
                session.winner_id = session.player_b_id
                session.result_message = f"回合耗尽，{player_b.name}以血量优势获胜！"
            else:
                session.winner_id = None
                session.result_message = "回合耗尽，双方平局！"
            session.combat_log.append(session.result_message)
            await self._sync_players(player_a, player_b, session)

        else:
            session.status = "waiting"
            session.round_deadline = time.time() + PVP_ROUND_TIMEOUT
            await self._sync_players(player_a, player_b, session)

        return self._build_resolve_payload(
            session,
            message=session.result_message or "回合结算完成",
            extras=extras,
        )

    def _calc_pvp_damage(self, attack_power: int, defender_defense: int, defending: bool) -> int:
        """计算 PvP 伤害。"""
        if attack_power <= 0:
            return 0
        return CombatEngine._calc_damage(attack_power, defender_defense, defending)

    async def handle_timeout(self, session_id: str) -> dict | None:
        """处理超时：未提交动作的一方自动防御。"""
        session = self._activate_if_due(self._sessions.get(session_id))
        if not session or session.status != "waiting":
            return None
        if time.time() < session.round_deadline:
            return None

        if session.action_a is None:
            session.action_a = {"action": "defend"}
        if session.action_b is None:
            session.action_b = {"action": "defend"}
        return await self._resolve_round(session)

    def find_online_opponent(self, player: Player, online_ids: list[str]) -> Optional[str]:
        """寻找在线对手。"""
        candidates = [
            uid
            for uid in online_ids
            if uid != player.user_id
            and uid not in self._player_sessions
            and not self._engine.dungeon.has_active_session(uid)
        ]
        if not candidates:
            return None
        return random.choice(candidates)

    def cleanup_session(self, session_id: str):
        """清理已结束的会话。"""
        session = self._sessions.pop(session_id, None)
        if session:
            self._player_sessions.pop(session.player_a_id, None)
            self._player_sessions.pop(session.player_b_id, None)

    def _normalize_action(
        self, session: PvPSession, user_id: str, player: Player, action: dict | None
    ) -> dict:
        """校验 PvP 动作并标准化。"""
        action = action or {}
        raw_action = str(action.get("action", "attack")).strip().lower() or "attack"
        if raw_action == "skill":
            raw_action = "gongfa"
        if raw_action not in {"attack", "defend", "gongfa"}:
            return {"success": False, "message": f"未知动作: {raw_action}"}
        if raw_action != "gongfa":
            return {"success": True, "action": {"action": raw_action}}

        gongfa_slot = str(action.get("gongfa_slot", "")).strip()
        if gongfa_slot not in {"gongfa_1", "gongfa_2", "gongfa_3"}:
            return {"success": False, "message": "请选择要施展的功法"}

        gongfa_id = getattr(player, gongfa_slot, "无")
        if not gongfa_id or gongfa_id == "无":
            return {"success": False, "message": "该槽位没有装备功法"}

        gongfa = GONGFA_REGISTRY.get(gongfa_id)
        if not gongfa:
            return {"success": False, "message": "功法数据异常"}

        my_state = session.state_a if user_id == session.player_a_id else session.state_b
        is_regen = gongfa.attack_bonus == 0 and gongfa.defense_bonus == 0 and gongfa.hp_regen == 0
        if not is_regen and my_state.player_lingqi < gongfa.lingqi_cost:
            return {
                "success": False,
                "message": f"灵气不足，需要{gongfa.lingqi_cost}，当前{my_state.player_lingqi}",
            }
        return {
            "success": True,
            "action": {"action": "gongfa", "gongfa_slot": gongfa_slot},
        }

    def _build_action_plan(self, state: CombatState, player: Player, action: dict) -> dict:
        """构造单个玩家的本回合行动结果。"""
        raw_action = action.get("action", "attack")
        if raw_action == "defend":
            return {
                "attack_power": 0,
                "defending": True,
                "heal": 0,
                "lingqi_regen": 0,
                "summary": "防御架势",
            }
        if raw_action != "gongfa":
            return {
                "attack_power": state.player_attack,
                "defending": False,
                "heal": 0,
                "lingqi_regen": 0,
                "summary": "普通攻击",
            }

        gongfa_slot = action.get("gongfa_slot", "")
        gongfa_id = getattr(player, gongfa_slot, "无")
        gongfa = GONGFA_REGISTRY.get(gongfa_id)
        if not gongfa:
            return {
                "attack_power": state.player_attack,
                "defending": False,
                "heal": 0,
                "lingqi_regen": 0,
                "summary": "功法失效，改为普通攻击",
            }

        mastery = getattr(player, f"{gongfa_slot}_mastery", 0)
        bonus = get_gongfa_bonus(gongfa_id, mastery, player.realm)
        is_regen = gongfa.attack_bonus == 0 and gongfa.defense_bonus == 0 and gongfa.hp_regen == 0
        if not is_regen:
            state.player_lingqi = max(0, state.player_lingqi - gongfa.lingqi_cost)

        attack_power = 0
        if bonus["attack_bonus"] > 0:
            attack_power = state.player_attack + int(bonus["attack_bonus"] * 1.5)
        heal = int(bonus["hp_regen"] * 3) if bonus["hp_regen"] > 0 else 0
        lingqi_regen = int(bonus["lingqi_regen"] * 2) if bonus["lingqi_regen"] > 0 else 0
        defending = bonus["defense_bonus"] > 0
        cost_msg = "免费" if is_regen else f"耗灵{gongfa.lingqi_cost}"
        return {
            "attack_power": attack_power,
            "defending": defending,
            "heal": heal,
            "lingqi_regen": lingqi_regen,
            "summary": f"施展【{gongfa.name}】({cost_msg})",
        }

    def _normalize_flee_items(self, player: Player, offer_items: list[dict] | None) -> dict:
        """校验逃跑献祭物品。"""
        if not isinstance(offer_items, list) or not offer_items:
            return {"success": False, "message": "逃跑时必须上交物品"}

        merged: dict[str, int] = {}
        for entry in offer_items:
            if not isinstance(entry, dict):
                return {"success": False, "message": "逃跑物品格式错误"}
            item_id = str(entry.get("item_id", "")).strip()
            if not item_id:
                return {"success": False, "message": "存在未指定 ID 的物品"}
            try:
                count = int(entry.get("count", 0))
            except (TypeError, ValueError):
                return {"success": False, "message": f"物品 {item_id} 的数量无效"}
            if count <= 0:
                return {"success": False, "message": f"物品 {item_id} 的数量必须大于 0"}
            merged[item_id] = merged.get(item_id, 0) + count

        normalized_items: list[dict] = []
        total_value = 0
        for item_id, count in merged.items():
            own_count = int(player.inventory.get(item_id, 0) or 0)
            if own_count < count:
                return {"success": False, "message": f"【{item_id}】数量不足"}
            price = get_daily_recycle_price(item_id)
            if price is None:
                return {"success": False, "message": f"【{item_id}】不可作为逃跑献祭"}
            item_def = ITEM_REGISTRY.get(item_id)
            if not item_def:
                return {"success": False, "message": f"【{item_id}】数据异常"}
            item_total = price * count
            total_value += item_total
            normalized_items.append(
                {
                    "item_id": item_id,
                    "name": item_def.name,
                    "count": count,
                    "recycle_price": price,
                    "total_value": item_total,
                }
            )

        if total_value <= 0:
            return {"success": False, "message": "逃跑献祭价值必须大于 0"}
        return {
            "success": True,
            "offer": {
                "items": normalized_items,
                "total_value": total_value,
                "requires_approval": total_value < PVP_FLEE_DIRECT_VALUE,
            },
        }

    def _activate_if_due(self, session: Optional[PvPSession]) -> Optional[PvPSession]:
        """刷新会话；待应战状态仅由手动接受或超时任务推进。"""
        return session

    async def _resolve_flee(
        self,
        session: PvPSession,
        requester_id: str,
        offer: dict,
        *,
        auto_approved: bool,
    ) -> dict:
        """结算逃跑成功。"""
        player_a = self._engine._players.get(session.player_a_id)
        player_b = self._engine._players.get(session.player_b_id)
        if not player_a or not player_b:
            session.status = "ended"
            session.winner_id = None
            session.end_reason = "invalid"
            session.result_message = "对战角色不存在，已自动结束"
            return {
                "success": False,
                "resolved": False,
                "message": session.result_message,
            }

        requester = player_a if requester_id == session.player_a_id else player_b
        opponent = player_b if requester is player_a else player_a

        await self._transfer_offer_items(requester, opponent, offer.get("items", []))
        session.action_a = None
        session.action_b = None
        session.flee_offer = None
        session.status = "ended"
        session.winner_id = opponent.user_id
        session.end_reason = "flee"
        offer_summary = self._format_offer_items(offer.get("items", []))
        if auto_approved:
            session.result_message = (
                f"{requester.name}献上{offer_summary}后逃离战场，"
                f"{opponent.name}获胜！"
            )
        else:
            session.result_message = (
                f"{opponent.name}同意{requester.name}逃离，"
                f"{requester.name}献上{offer_summary}后脱身。"
            )
        session.combat_log.append(session.result_message)
        await self._sync_players(player_a, player_b, session)
        return self._build_resolve_payload(
            session,
            message=session.result_message,
            extras={
                session.player_a_id: {},
                session.player_b_id: {},
            },
        )

    async def _transfer_offer_items(self, requester: Player, opponent: Player, items: list[dict]):
        """把逃跑献祭物品转移给对手。"""
        for item in items:
            item_id = str(item.get("item_id", "")).strip()
            count = int(item.get("count", 0) or 0)
            if not item_id or count <= 0:
                continue
            own_count = int(requester.inventory.get(item_id, 0) or 0)
            if own_count < count:
                continue
            remain = own_count - count
            if remain > 0:
                requester.inventory[item_id] = remain
            else:
                requester.inventory.pop(item_id, None)
            await add_item(opponent, item_id, count)

    async def _apply_player_defeat(self, state: CombatState, player: Player) -> dict:
        """处理玩家被打至 0 血后的结算。"""
        if player.inventory.get("life_talisman", 0) > 0:
            player.inventory["life_talisman"] -= 1
            if player.inventory["life_talisman"] <= 0:
                del player.inventory["life_talisman"]
            state.player_hp = 1
            player.hp = 1
            player.lingqi = max(0, min(state.player_max_lingqi, state.player_lingqi))
            return {
                "died": False,
                "death_prevented": True,
            }

        player.hp = 0
        player.lingqi = max(0, min(state.player_max_lingqi, state.player_lingqi))
        death_items = await self._engine.prepare_death(player.user_id)
        return {
            "died": True,
            "death_items": death_items,
        }

    async def _sync_players(
        self,
        player_a: Player,
        player_b: Player,
        session: PvPSession,
        *,
        skip_save_user_ids: Optional[set[str]] = None,
    ):
        """同步双方血量 / 灵气到玩家对象，并按需持久化。"""
        skip_save_user_ids = skip_save_user_ids or set()
        self._sync_player_state(player_a, session.state_a)
        self._sync_player_state(player_b, session.state_b)
        if player_a.user_id not in skip_save_user_ids:
            await self._engine._save_player(player_a)
        if player_b.user_id not in skip_save_user_ids:
            await self._engine._save_player(player_b)

    @staticmethod
    def _sync_player_state(player: Player, state: CombatState):
        """把战斗内状态同步回玩家。"""
        player.hp = max(0, min(player.max_hp, state.player_hp))
        player.lingqi = max(0, min(state.player_max_lingqi, state.player_lingqi))

    def _build_resolve_payload(
        self,
        session: PvPSession,
        *,
        message: str,
        extras: Optional[dict[str, dict]] = None,
    ) -> dict:
        """构造统一的结算返回体。"""
        extras = extras or {}
        state_a = session.to_dict(session.player_a_id)
        state_b = session.to_dict(session.player_b_id)
        state_a.update(extras.get(session.player_a_id, {}))
        state_b.update(extras.get(session.player_b_id, {}))
        return {
            "success": True,
            "resolved": True,
            "message": message,
            "pvp_state_a": state_a,
            "pvp_state_b": state_b,
            "ended": session.status == "ended",
            "winner_id": session.winner_id,
            "session_id": session.session_id,
        }

    @staticmethod
    def _format_offer_items(items: list[dict]) -> str:
        """格式化献祭物品列表。"""
        if not items:
            return "空物品"
        return "、".join(
            f"{item.get('name', item.get('item_id', '未知'))}x{item.get('count', 0)}"
            for item in items
        )
