"""数据持久化管理：SQLite 数据库存储。"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime
from typing import Optional, Any

import aiosqlite

from .models import Player

logger = logging.getLogger(__name__)

# 用于 _alter_add_column 的标识符和类型安全校验
_SAFE_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
# DEFAULT 值仅允许：数字/小数、单引号字符串（内部无引号）、NULL
_SAFE_COL_TYPE = re.compile(
    r"^[A-Z ]+(?:\(\d+\))?"
    r"(?:\s+DEFAULT\s+(?:-?\d+(?:\.\d+)?|'[^']*'|NULL))?$",
    re.I,
)

# 玩家表所有列（与 Player 字段一一对应）
_PLAYER_COLUMNS = [
    "user_id", "name", "realm", "sub_realm", "exp",
    "hp", "max_hp", "attack", "defense", "spirit_stones",
    "lingqi", "permanent_max_hp_bonus", "permanent_attack_bonus",
    "permanent_defense_bonus", "permanent_lingqi_bonus",
    "heart_method", "weapon", "gongfa_1", "gongfa_2", "gongfa_3", "armor", "dao_yun",
    "breakthrough_bonus", "breakthrough_pill_count",
    "heart_method_mastery", "heart_method_exp", "heart_method_value", "stored_heart_methods",
    "gongfa_1_mastery", "gongfa_1_exp",
    "gongfa_2_mastery", "gongfa_2_exp",
    "gongfa_3_mastery", "gongfa_3_exp",
    "inventory", "active_buffs", "created_at", "last_cultivate_time",
    "last_checkin_date", "afk_cultivate_start", "afk_cultivate_end",
    "last_adventure_time", "death_count", "unified_msg_origin", "password_hash",
]


class DataManager:
    """管理玩家数据的加载和保存（SQLite）。"""

    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._db_path = os.path.join(data_dir, "xiuxian.db")
        self.db: Optional[aiosqlite.Connection] = None
        self._shop_purchase_lock = asyncio.Lock()
        self._sect_schema_checked = False

    class TransactionAbort(Exception):
        """在事务上下文中主动中止，携带用户提示消息。"""
        pass

    @contextlib.asynccontextmanager
    async def transaction(self):
        """提供一个使用独立连接的数据库事务，异常时自动 rollback。

        独立连接确保事务内的操作与 self.db 上的普通写操作完全隔离，
        不会出现其他协程的写入被意外卷入事务的问题。

        用法::

            async with dm.transaction() as tx:
                await tx.execute("UPDATE ...", (...))
                await tx.execute("INSERT ...", (...))
            # 离开 with 块自动 commit；异常则自动 rollback
        """
        conn = await aiosqlite.connect(self._db_path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def initialize(self):
        """初始化数据目录、打开数据库、建表、迁移旧数据。"""
        os.makedirs(self._data_dir, exist_ok=True)
        self.db = await aiosqlite.connect(self._db_path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self._create_tables()
        await self._migrate_json_data()

    async def _create_tables(self):
        """创建数据库表。"""
        realms_table_exists = False
        async with self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'realms' LIMIT 1"
        ) as cur:
            realms_table_exists = (await cur.fetchone()) is not None
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS players (
                user_id             TEXT PRIMARY KEY,
                name                TEXT NOT NULL,
                realm               INTEGER DEFAULT 0,
                sub_realm           INTEGER DEFAULT 0,
                exp                 INTEGER DEFAULT 0,
                hp                  INTEGER DEFAULT 100,
                max_hp              INTEGER DEFAULT 100,
                attack              INTEGER DEFAULT 10,
                defense             INTEGER DEFAULT 5,
                spirit_stones       INTEGER DEFAULT 0,
                lingqi              INTEGER DEFAULT 50,
                permanent_max_hp_bonus INTEGER DEFAULT 0,
                permanent_attack_bonus INTEGER DEFAULT 0,
                permanent_defense_bonus INTEGER DEFAULT 0,
                permanent_lingqi_bonus INTEGER DEFAULT 0,
                heart_method        TEXT DEFAULT '无',
                weapon              TEXT DEFAULT '无',
                gongfa_1            TEXT DEFAULT '无',
                gongfa_2            TEXT DEFAULT '无',
                gongfa_3            TEXT DEFAULT '无',
                armor               TEXT DEFAULT '无',
                dao_yun             INTEGER DEFAULT 0,
                breakthrough_bonus  REAL DEFAULT 0.0,
                breakthrough_pill_count INTEGER DEFAULT 0,
                heart_method_mastery INTEGER DEFAULT 0,
                heart_method_exp    INTEGER DEFAULT 0,
                heart_method_value  INTEGER DEFAULT 0,
                stored_heart_methods TEXT DEFAULT '{}',
                inventory           TEXT DEFAULT '{}',
                active_buffs        TEXT DEFAULT '[]',
                created_at          REAL,
                last_cultivate_time REAL DEFAULT 0.0,
                last_checkin_date   TEXT,
                afk_cultivate_start REAL DEFAULT 0.0,
                afk_cultivate_end   REAL DEFAULT 0.0,
                last_adventure_time REAL DEFAULT 0.0,
                death_count         INTEGER DEFAULT 0,
                unified_msg_origin  TEXT,
                password_hash       TEXT
            )
        """)
        # 境界配置表（管理员可维护）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS realms (
                level             INTEGER PRIMARY KEY,
                name              TEXT NOT NULL,
                has_sub_realm     INTEGER DEFAULT 0,
                high_realm        INTEGER DEFAULT 0,
                exp_to_next       INTEGER DEFAULT 100,
                sub_exp_to_next   INTEGER DEFAULT 0,
                base_hp           INTEGER DEFAULT 100,
                base_attack       INTEGER DEFAULT 10,
                base_defense      INTEGER DEFAULT 5,
                base_lingqi       INTEGER DEFAULT 50,
                breakthrough_rate REAL DEFAULT 1.0,
                death_rate        REAL DEFAULT 0.0,
                sub_dao_yun_costs TEXT DEFAULT '',
                breakthrough_dao_yun_cost INTEGER DEFAULT 0,
                dao_yun_base_rate REAL DEFAULT 0.0,
                dao_yun_per_sub_realm REAL DEFAULT 0.0,
                dao_yun_rate_initialized INTEGER DEFAULT 0
            )
        """)
        # 迁移：新版道韵产出字段（已存在的表可能没有这两列）
        try:
            await self.db.execute(
                "ALTER TABLE realms ADD COLUMN dao_yun_base_rate REAL DEFAULT 0.0"
            )
        except Exception:
            pass
        try:
            await self.db.execute(
                "ALTER TABLE realms ADD COLUMN dao_yun_per_sub_realm REAL DEFAULT 0.0"
            )
        except Exception:
            pass
        # 移除已废弃的历练场景表
        await self.db.execute("DROP TABLE IF EXISTS adventure_scenes")
        # 心法定义独立表（可在数据库中独立维护）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS heart_methods (
                method_id       TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                realm           INTEGER NOT NULL,
                quality         INTEGER NOT NULL DEFAULT 0,
                exp_multiplier  REAL NOT NULL DEFAULT 0.0,
                attack_bonus    INTEGER NOT NULL DEFAULT 0,
                defense_bonus   INTEGER NOT NULL DEFAULT 0,
                dao_yun_rate    REAL NOT NULL DEFAULT 0.0,
                description     TEXT DEFAULT '',
                mastery_exp     INTEGER NOT NULL DEFAULT 100,
                enabled         INTEGER NOT NULL DEFAULT 1
            )
        """)
        # 武器/护甲定义独立表（管理员可维护）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS weapons (
                equip_id         TEXT PRIMARY KEY,
                name             TEXT NOT NULL,
                tier             INTEGER NOT NULL,
                slot             TEXT NOT NULL,
                attack           INTEGER NOT NULL DEFAULT 0,
                defense          INTEGER NOT NULL DEFAULT 0,
                element          TEXT DEFAULT '无',
                element_damage   INTEGER NOT NULL DEFAULT 0,
                description      TEXT DEFAULT '',
                enabled          INTEGER NOT NULL DEFAULT 1
            )
        """)
        # 坊市上架记录
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS market_listings (
                listing_id   TEXT PRIMARY KEY,
                seller_id    TEXT NOT NULL,
                item_id      TEXT NOT NULL,
                quantity     INTEGER NOT NULL,
                unit_price   INTEGER NOT NULL,
                total_price  INTEGER NOT NULL,
                fee          INTEGER NOT NULL,
                listed_at    REAL NOT NULL,
                expires_at   REAL NOT NULL,
                status       TEXT NOT NULL DEFAULT 'active',
                buyer_id     TEXT,
                sold_at      REAL
            )
        """)
        # 坊市成交记录（用于手续费计算）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS market_history (
                history_id   TEXT PRIMARY KEY,
                item_id      TEXT NOT NULL,
                quantity     INTEGER NOT NULL,
                unit_price   INTEGER NOT NULL,
                total_price  INTEGER NOT NULL,
                fee          INTEGER NOT NULL,
                seller_id    TEXT NOT NULL,
                buyer_id     TEXT NOT NULL,
                sold_at      REAL NOT NULL
            )
        """)
        # 坊市索引
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_listings_status
            ON market_listings (status)
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_listings_seller
            ON market_listings (seller_id)
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_listings_expires
            ON market_listings (expires_at)
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_history_item
            ON market_history (item_id, sold_at)
        """)
        # 天机阁购买记录
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS shop_purchases (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      TEXT NOT NULL,
                item_id      TEXT NOT NULL,
                quantity     INTEGER NOT NULL DEFAULT 1,
                unit_price   INTEGER NOT NULL,
                purchased_at TEXT NOT NULL
            )
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_date
            ON shop_purchases (purchased_at, item_id)
        """)
        # 公告表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                title      TEXT NOT NULL,
                content    TEXT NOT NULL,
                enabled    INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        # 功法定义独立表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS gongfas (
                gongfa_id     TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                tier          INTEGER NOT NULL DEFAULT 0,
                attack_bonus  INTEGER DEFAULT 0,
                defense_bonus INTEGER DEFAULT 0,
                hp_regen      INTEGER DEFAULT 0,
                lingqi_regen  INTEGER DEFAULT 0,
                description   TEXT DEFAULT '',
                mastery_exp   INTEGER DEFAULT 200,
                dao_yun_cost  INTEGER DEFAULT 0,
                recycle_price INTEGER DEFAULT 1000,
                lingqi_cost   INTEGER DEFAULT 0,
                enabled       INTEGER DEFAULT 1
            )
        """)
        # 丹药定义独立表（管理员可维护）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS pills (
                pill_id           TEXT PRIMARY KEY,
                name              TEXT NOT NULL,
                tier              INTEGER NOT NULL DEFAULT 0,
                grade             INTEGER NOT NULL DEFAULT 0,
                category          TEXT NOT NULL DEFAULT 'healing',
                description       TEXT DEFAULT '',
                price             INTEGER NOT NULL DEFAULT 0,
                effects           TEXT NOT NULL DEFAULT '{}',
                is_temp           INTEGER NOT NULL DEFAULT 0,
                duration          INTEGER NOT NULL DEFAULT 0,
                side_effects      TEXT NOT NULL DEFAULT '{}',
                side_effect_desc  TEXT DEFAULT '',
                enabled           INTEGER NOT NULL DEFAULT 1
            )
        """)
        # 材料定义独立表（管理员可维护）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS materials (
                item_id         TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                rarity          INTEGER NOT NULL DEFAULT 0,
                category        TEXT NOT NULL DEFAULT 'herb',
                source          TEXT DEFAULT '',
                description     TEXT DEFAULT '',
                recycle_price   INTEGER NOT NULL DEFAULT 0
            )
        """)
        # 丹方/炼丹配方表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS pill_recipes (
                recipe_id           TEXT PRIMARY KEY,
                pill_id             TEXT NOT NULL,
                grade               INTEGER NOT NULL DEFAULT 0,
                main_material       TEXT NOT NULL DEFAULT '{}',
                auxiliary_material  TEXT NOT NULL DEFAULT '{}',
                catalyst            TEXT NOT NULL DEFAULT '{}',
                forming_material    TEXT NOT NULL DEFAULT '{}'
            )
        """)
        # 世界频道消息表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS world_chat_messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    TEXT NOT NULL,
                name       TEXT NOT NULL,
                realm      TEXT NOT NULL DEFAULT '',
                sect_name  TEXT NOT NULL DEFAULT '',
                sect_role  TEXT NOT NULL DEFAULT '',
                sect_role_name TEXT NOT NULL DEFAULT '',
                content    TEXT NOT NULL,
                created_at REAL NOT NULL
            )
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_world_chat_created
            ON world_chat_messages (created_at)
        """)
        # 宗门主表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sects (
                sect_id       TEXT PRIMARY KEY,
                name          TEXT NOT NULL UNIQUE,
                leader_id     TEXT NOT NULL,
                description   TEXT DEFAULT '',
                level         INTEGER DEFAULT 1,
                spirit_stones INTEGER DEFAULT 0,
                max_members   INTEGER DEFAULT 30,
                join_policy   TEXT DEFAULT 'open',
                min_realm     INTEGER DEFAULT 0,
                created_at    REAL NOT NULL,
                announcement  TEXT DEFAULT '',
                warehouse_capacity INTEGER DEFAULT 200
            )
        """)
        # 宗门成员关系表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sect_members (
                user_id   TEXT PRIMARY KEY,
                sect_id   TEXT NOT NULL,
                role      TEXT NOT NULL DEFAULT 'disciple',
                joined_at REAL NOT NULL,
                contribution_points INTEGER DEFAULT 0,
                FOREIGN KEY (sect_id) REFERENCES sects(sect_id)
            )
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sect_members_sect
            ON sect_members (sect_id)
        """)
        # 宗门申请表（预留）
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sect_applications (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     TEXT NOT NULL,
                sect_id     TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                applied_at  REAL NOT NULL,
                resolved_at REAL,
                resolved_by TEXT
            )
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sect_applications_sect
            ON sect_applications (sect_id, status)
        """)
        # 宗门仓库表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sect_warehouse (
                sect_id   TEXT NOT NULL,
                item_id   TEXT NOT NULL,
                quantity  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (sect_id, item_id),
                FOREIGN KEY (sect_id) REFERENCES sects(sect_id)
            )
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sect_warehouse_sect
            ON sect_warehouse (sect_id)
        """)
        # 宗门贡献点规则配置表
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sect_contribution_config (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sect_id     TEXT NOT NULL,
                rule_type   TEXT NOT NULL,
                target_key  TEXT NOT NULL,
                points      INTEGER NOT NULL,
                UNIQUE(sect_id, rule_type, target_key),
                FOREIGN KEY (sect_id) REFERENCES sects(sect_id)
            )
        """)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sect_contrib_config_sect
            ON sect_contribution_config (sect_id)
        """)
        await self.db.commit()
        # 数据库升级：为旧表添加新列
        await self._alter_add_column("players", "last_adventure_time", "REAL DEFAULT 0.0")
        await self._alter_add_column("players", "death_count", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "lingqi", "INTEGER DEFAULT 50")
        await self._alter_add_column("players", "permanent_max_hp_bonus", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "permanent_attack_bonus", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "permanent_defense_bonus", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "permanent_lingqi_bonus", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "heart_method", "TEXT DEFAULT '无'")
        await self._alter_add_column("players", "weapon", "TEXT DEFAULT '无'")
        await self._alter_add_column("players", "gongfa_1", "TEXT DEFAULT '无'")
        await self._alter_add_column("players", "gongfa_2", "TEXT DEFAULT '无'")
        await self._alter_add_column("players", "gongfa_3", "TEXT DEFAULT '无'")
        await self._alter_add_column("players", "armor", "TEXT DEFAULT '无'")
        await self._alter_add_column("players", "dao_yun", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "breakthrough_bonus", "REAL DEFAULT 0.0")
        await self._alter_add_column("players", "breakthrough_pill_count", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "heart_method_mastery", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "heart_method_exp", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "heart_method_value", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "stored_heart_methods", "TEXT DEFAULT '{}'")
        await self._alter_add_column("players", "active_buffs", "TEXT DEFAULT '[]'")
        await self._alter_add_column("players", "gongfa_1_mastery", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "gongfa_1_exp", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "gongfa_2_mastery", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "gongfa_2_exp", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "gongfa_3_mastery", "INTEGER DEFAULT 0")
        await self._alter_add_column("players", "gongfa_3_exp", "INTEGER DEFAULT 0")
        await self._alter_add_column("gongfas", "lingqi_cost", "INTEGER DEFAULT 0")
        await self._alter_add_column("world_chat_messages", "sect_name", "TEXT DEFAULT ''")
        await self._alter_add_column("world_chat_messages", "sect_role", "TEXT DEFAULT ''")
        await self._alter_add_column("world_chat_messages", "sect_role_name", "TEXT DEFAULT ''")
        await self._ensure_sect_schema(force=True)
        # 境界表迁移：新增道韵字段
        await self._alter_add_column("realms", "sub_dao_yun_costs", "TEXT DEFAULT ''")
        await self._alter_add_column("realms", "breakthrough_dao_yun_cost", "INTEGER DEFAULT 0")
        await self._alter_add_column("realms", "dao_yun_rate_initialized", "INTEGER DEFAULT 0")
        # 境界表首次创建时写入一份默认数据；之后完全以数据库内容为准
        if not realms_table_exists:
            await self._seed_realms()
        await self._backfill_realm_dao_yun_defaults()
        # 填充心法定义（仅补齐缺失，不覆盖已有配置）
        await self._seed_heart_methods()
        # 填充装备定义（仅补齐缺失，不覆盖已有配置）
        await self._seed_weapons()
        # 填充功法定义（仅补齐缺失，不覆盖已有配置）
        await self._seed_gongfas()
        await self._sync_gongfa_lingqi_costs()
        # 填充丹药定义（仅补齐缺失，不覆盖已有配置）
        await self._seed_pills()
        # 填充材料定义（仅补齐缺失，不覆盖已有配置）
        await self._seed_materials()
        # 填充丹方定义（仅补齐缺失）
        await self._seed_pill_recipes()

    async def _migrate_json_data(self):
        """若存在旧 players.json 且数据库为空，则自动迁移。"""
        json_file = os.path.join(self._data_dir, "players.json")
        if not os.path.exists(json_file):
            return
        # 检查数据库是否已有数据
        async with self.db.execute("SELECT COUNT(*) FROM players") as cur:
            row = await cur.fetchone()
            if row[0] > 0:
                return
        # 读取旧 JSON
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                content = f.read()
            if not content.strip():
                return
            data = json.loads(content)
            for uid, d in data.items():
                d["user_id"] = uid
                player = Player.from_dict(d)
                await self._upsert_player(player)
            await self.db.commit()
            # 迁移完成后重命名旧文件作备份
            backup = json_file + ".bak"
            if not os.path.exists(backup):
                os.rename(json_file, backup)
        except Exception:
            pass

    async def load_all_players(self) -> dict[str, Player]:
        """加载所有玩家数据到内存。"""
        players = {}
        async with self.db.execute("SELECT * FROM players") as cur:
            async for row in cur:
                d = self._row_to_dict(row)
                player = Player.from_dict(d)
                players[player.user_id] = player
        return players

    async def load_heart_methods(self) -> dict:
        """加载启用的心法定义（独立表 -> 运行时）。"""
        from .constants import HEART_METHOD_REGISTRY, HeartMethodDef, HeartMethodQuality

        methods = {}
        try:
            async with self.db.execute(
                """
                SELECT method_id, name, realm, quality, exp_multiplier,
                       attack_bonus, defense_bonus, dao_yun_rate, description, mastery_exp
                FROM heart_methods
                WHERE enabled = 1
                ORDER BY realm ASC, quality ASC, method_id ASC
                """
            ) as cur:
                async for row in cur:
                    method_id = row["method_id"]
                    methods[method_id] = HeartMethodDef(
                        method_id=method_id,
                        name=row["name"],
                        realm=int(row["realm"]),
                        quality=HeartMethodQuality(int(row["quality"])),
                        exp_multiplier=float(row["exp_multiplier"] or 0.0),
                        attack_bonus=int(row["attack_bonus"] or 0),
                        defense_bonus=int(row["defense_bonus"] or 0),
                        dao_yun_rate=float(row["dao_yun_rate"] or 0.0),
                        description=row["description"] or "",
                        mastery_exp=int(row["mastery_exp"] or 100),
                    )
        except Exception:
            # 回退到代码内置，避免启动失败
            return dict(HEART_METHOD_REGISTRY)
        return methods

    async def load_weapons(self) -> dict:
        """加载启用的装备定义（独立表 -> 运行时）。"""
        from .constants import EQUIPMENT_REGISTRY, EquipmentDef

        equips = {}
        try:
            async with self.db.execute(
                """
                SELECT equip_id, name, tier, slot, attack, defense,
                       element, element_damage, description
                FROM weapons
                WHERE enabled = 1
                ORDER BY tier ASC, slot ASC, equip_id ASC
                """
            ) as cur:
                async for row in cur:
                    equip_id = row["equip_id"]
                    equips[equip_id] = EquipmentDef(
                        equip_id=equip_id,
                        name=row["name"],
                        tier=int(row["tier"]),
                        slot=row["slot"],
                        attack=int(row["attack"] or 0),
                        defense=int(row["defense"] or 0),
                        element=row["element"] or "无",
                        element_damage=int(row["element_damage"] or 0),
                        description=row["description"] or "",
                    )
        except Exception:
            return dict(EQUIPMENT_REGISTRY)
        return equips

    async def save_player(self, player: Player):
        """保存单个玩家（INSERT OR REPLACE）。"""
        await self._upsert_player(player)
        await self.db.commit()

    async def save_all_players(self, players: dict[str, Player]):
        """批量保存所有玩家数据（UPSERT模式，分批提交）。"""
        batch_size = 50
        batch: list[Player] = []
        for player in players.values():
            batch.append(player)
            if len(batch) >= batch_size:
                for p in batch:
                    await self._upsert_player(p)
                await self.db.commit()
                batch.clear()
        if batch:
            for p in batch:
                await self._upsert_player(p)
            await self.db.commit()

    async def delete_player(self, user_id: str):
        """删除单个玩家。"""
        await self.db.execute("DELETE FROM players WHERE user_id = ?", (user_id,))
        await self.db.commit()

    async def clear_all_data(self, remove_dir: bool = False):
        """清理插件数据。"""
        if remove_dir:
            await self.close()
            if os.path.isdir(self._data_dir):
                await asyncio.to_thread(shutil.rmtree, self._data_dir, True)
            return
        await self.db.execute("DELETE FROM players")
        await self.db.execute("DELETE FROM web_tokens")
        await self.db.execute("DELETE FROM bind_keys")
        await self.db.execute("DELETE FROM chat_bindings")
        await self.db.commit()

    async def close(self):
        """关闭数据库连接。"""
        if self.db:
            await self.db.close()
            self.db = None

    # ==================== 内部方法 ====================

    async def _upsert_player(self, player: Player, db: aiosqlite.Connection | None = None):
        """INSERT OR REPLACE 单个玩家。"""
        d = player.to_dict(include_sensitive=True)
        # inventory 序列化为 JSON text
        inv = d.get("inventory", {})
        if isinstance(inv, dict):
            inv = json.dumps(inv, ensure_ascii=False)
        stored_heart_methods = d.get("stored_heart_methods", {})
        if isinstance(stored_heart_methods, dict):
            stored_heart_methods = json.dumps(stored_heart_methods, ensure_ascii=False)
        active_buffs = d.get("active_buffs_raw", d.get("active_buffs", []))
        if isinstance(active_buffs, list):
            active_buffs = json.dumps(active_buffs, ensure_ascii=False)
        conn = db or self.db
        if conn is None:
            raise RuntimeError("数据库连接尚未初始化")

        values = (
            player.user_id,
            d.get("name", ""),
            d.get("realm", 0),
            d.get("sub_realm", 0),
            d.get("exp", 0),
            d.get("hp", 100),
            d.get("max_hp", 100),
            d.get("attack", 10),
            d.get("defense", 5),
            d.get("spirit_stones", 0),
            d.get("lingqi", 50),
            d.get("permanent_max_hp_bonus", 0),
            d.get("permanent_attack_bonus", 0),
            d.get("permanent_defense_bonus", 0),
            d.get("permanent_lingqi_bonus", 0),
            d.get("heart_method", "无"),
            d.get("weapon", "无"),
            d.get("gongfa_1", "无"),
            d.get("gongfa_2", "无"),
            d.get("gongfa_3", "无"),
            d.get("armor", "无"),
            d.get("dao_yun", 0),
            d.get("breakthrough_bonus", 0.0),
            d.get("breakthrough_pill_count", 0),
            d.get("heart_method_mastery", 0),
            d.get("heart_method_exp", 0),
            d.get("heart_method_value", 0),
            stored_heart_methods,
            player.gongfa_1_mastery,
            player.gongfa_1_exp,
            player.gongfa_2_mastery,
            player.gongfa_2_exp,
            player.gongfa_3_mastery,
            player.gongfa_3_exp,
            inv,
            active_buffs,
            d.get("created_at", 0),
            d.get("last_cultivate_time", 0.0),
            d.get("last_checkin_date"),
            d.get("afk_cultivate_start", 0.0),
            d.get("afk_cultivate_end", 0.0),
            d.get("last_adventure_time", 0.0),
            d.get("death_count", 0),
            d.get("unified_msg_origin"),
            d.get("password_hash"),
        )
        placeholders = ", ".join(["?"] * len(_PLAYER_COLUMNS))
        cols = ", ".join(_PLAYER_COLUMNS)
        await conn.execute(
            f"INSERT OR REPLACE INTO players ({cols}) VALUES ({placeholders})",
            values,
        )

    @staticmethod
    def _row_to_dict(row: aiosqlite.Row) -> dict:
        """将数据库行转为 Player.from_dict 可用的字典。"""
        d = dict(row)
        # inventory 从 JSON text 反序列化
        inv = d.get("inventory", "{}")
        if isinstance(inv, str):
            try:
                d["inventory"] = json.loads(inv)
            except (json.JSONDecodeError, TypeError):
                d["inventory"] = {}
        stored_heart_methods = d.get("stored_heart_methods", "{}")
        if isinstance(stored_heart_methods, str):
            try:
                loaded = json.loads(stored_heart_methods)
                d["stored_heart_methods"] = loaded if isinstance(loaded, dict) else {}
            except (json.JSONDecodeError, TypeError):
                d["stored_heart_methods"] = {}
        active_buffs = d.get("active_buffs", "[]")
        if isinstance(active_buffs, str):
            try:
                loaded = json.loads(active_buffs)
                d["active_buffs_raw"] = loaded if isinstance(loaded, list) else []
            except (json.JSONDecodeError, TypeError):
                d["active_buffs_raw"] = []
        return d

    async def _alter_add_column(self, table: str, column: str, col_type: str):
        """安全地为表添加新列（已存在则忽略）。"""
        if not _SAFE_IDENT.match(table) or not _SAFE_IDENT.match(column):
            raise ValueError(f"非法标识符: table={table}, column={column}")
        if not _SAFE_COL_TYPE.match(col_type):
            raise ValueError(f"非法列类型: {col_type}")
        try:
            await self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            await self.db.commit()
        except Exception as e:
            if "duplicate column name" not in str(e).lower():
                logger.warning("添加列失败 %s.%s: %s", table, column, e)  # 列已存在

    async def _ensure_sect_schema(self, force: bool = False):
        """确保宗门相关旧表已经自动升级到当前结构。"""
        if self._sect_schema_checked and not force:
            return
        await self._alter_add_column("sects", "description", "TEXT DEFAULT ''")
        await self._alter_add_column("sects", "level", "INTEGER DEFAULT 1")
        await self._alter_add_column("sects", "spirit_stones", "INTEGER DEFAULT 0")
        await self._alter_add_column("sects", "max_members", "INTEGER DEFAULT 30")
        await self._alter_add_column("sects", "join_policy", "TEXT DEFAULT 'open'")
        await self._alter_add_column("sects", "min_realm", "INTEGER DEFAULT 0")
        await self._alter_add_column("sects", "created_at", "REAL DEFAULT 0.0")
        await self._alter_add_column("sects", "announcement", "TEXT DEFAULT ''")
        await self._alter_add_column("sect_members", "role", "TEXT DEFAULT 'disciple'")
        await self._alter_add_column("sect_members", "joined_at", "REAL DEFAULT 0.0")
        await self._alter_add_column("sect_applications", "status", "TEXT DEFAULT 'pending'")
        await self._alter_add_column("sect_applications", "applied_at", "REAL DEFAULT 0.0")
        await self._alter_add_column("sect_applications", "resolved_at", "REAL")
        await self._alter_add_column("sect_applications", "resolved_by", "TEXT")
        await self._alter_add_column("sect_members", "contribution_points", "INTEGER DEFAULT 0")
        await self._alter_add_column("sects", "warehouse_capacity", "INTEGER DEFAULT 200")
        try:
            await self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sects_name_unique ON sects (name)")
            await self.db.commit()
        except Exception:
            pass
        self._sect_schema_checked = True

    async def _seed_realms(self):
        """首次创建境界表时，写入一份内置默认境界。"""
        import json as _json
        from .constants import REALM_CONFIG as DEFAULT_REALM_CONFIG

        rows = []
        for level, cfg in DEFAULT_REALM_CONFIG.items():
            sub_costs = cfg.get("sub_dao_yun_costs", [])
            rows.append((
                int(level),
                cfg["name"],
                1 if cfg.get("has_sub_realm") else 0,
                1 if cfg.get("high_realm") else 0,
                int(cfg.get("exp_to_next", 100)),
                int(cfg.get("sub_exp_to_next", 0)),
                int(cfg.get("base_hp", 100)),
                int(cfg.get("base_attack", 10)),
                int(cfg.get("base_defense", 5)),
                int(cfg.get("base_lingqi", 50)),
                float(cfg.get("breakthrough_rate", 1.0)),
                float(cfg.get("death_rate", 0.0)),
                _json.dumps(sub_costs) if sub_costs else "",
                int(cfg.get("breakthrough_dao_yun_cost", 0)),
                float(cfg.get("dao_yun_base_rate", 0.0)),
                float(cfg.get("dao_yun_per_sub_realm", 0.0)),
                1,
            ))
        await self.db.executemany(
            """
            INSERT OR IGNORE INTO realms (
                level, name, has_sub_realm, high_realm,
                exp_to_next, sub_exp_to_next,
                base_hp, base_attack, base_defense, base_lingqi,
                breakthrough_rate, death_rate,
                sub_dao_yun_costs, breakthrough_dao_yun_cost,
                dao_yun_base_rate, dao_yun_per_sub_realm,
                dao_yun_rate_initialized
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self.db.commit()

    async def _backfill_realm_dao_yun_defaults(self):
        """为旧 realms 表补齐兼容状态，不覆盖已编辑数据。"""
        import json as _json
        from .constants import REALM_CONFIG as DEFAULT_REALM_CONFIG

        changed = False
        for level, cfg in DEFAULT_REALM_CONFIG.items():
            default_sub_costs = cfg.get("sub_dao_yun_costs", [])
            default_bt_cost = int(cfg.get("breakthrough_dao_yun_cost", 0))
            if not default_sub_costs and default_bt_cost <= 0:
                continue
            sub_costs_text = _json.dumps(default_sub_costs) if default_sub_costs else ""
            cur = await self.db.execute(
                """
                UPDATE realms
                SET sub_dao_yun_costs = ?, breakthrough_dao_yun_cost = ?
                WHERE level = ?
                  AND COALESCE(sub_dao_yun_costs, '') = ''
                """,
                (sub_costs_text, default_bt_cost, int(level)),
            )
            if (cur.rowcount or 0) > 0:
                changed = True
            cur = await self.db.execute(
                """
                UPDATE realms
                SET dao_yun_rate_initialized = 1
                WHERE level = ?
                  AND COALESCE(dao_yun_rate_initialized, 0) = 0
                  AND (
                    COALESCE(dao_yun_base_rate, 0.0) > 0
                    OR COALESCE(dao_yun_per_sub_realm, 0.0) > 0
                  )
                """,
                (int(level),),
            )
            if (cur.rowcount or 0) > 0:
                changed = True
        if changed:
            await self.db.commit()

    @staticmethod
    def _resolve_realm_dao_yun_rates(row: Any, default_cfg: dict[str, Any]) -> tuple[float, float, bool]:
        stored_base = float(row["dao_yun_base_rate"] or 0.0)
        stored_per_sub = float(row["dao_yun_per_sub_realm"] or 0.0)
        initialized = bool(int(row["dao_yun_rate_initialized"] or 0))
        if initialized or stored_base > 0 or stored_per_sub > 0:
            return stored_base, stored_per_sub, initialized
        return (
            float(default_cfg.get("dao_yun_base_rate", 0.0)),
            float(default_cfg.get("dao_yun_per_sub_realm", 0.0)),
            initialized,
        )

    async def load_realms(self) -> dict[int, dict]:
        """加载境界配置（独立表 -> 运行时 REALM_CONFIG）。"""
        import json as _json
        from .constants import REALM_CONFIG as DEFAULT_REALM_CONFIG

        realms = {}
        try:
            async with self.db.execute(
                """
                SELECT level, name, has_sub_realm, high_realm,
                       exp_to_next, sub_exp_to_next,
                       base_hp, base_attack, base_defense, base_lingqi,
                       breakthrough_rate, death_rate,
                       sub_dao_yun_costs, breakthrough_dao_yun_cost,
                       dao_yun_base_rate, dao_yun_per_sub_realm,
                       dao_yun_rate_initialized
                FROM realms
                ORDER BY level ASC
                """
            ) as cur:
                async for row in cur:
                    level = int(row["level"])
                    cfg: dict[str, Any] = {
                        "name": row["name"],
                        "has_sub_realm": bool(int(row["has_sub_realm"])),
                        "exp_to_next": int(row["exp_to_next"]),
                        "sub_exp_to_next": int(row["sub_exp_to_next"]),
                        "base_hp": int(row["base_hp"]),
                        "base_attack": int(row["base_attack"]),
                        "base_defense": int(row["base_defense"]),
                        "base_lingqi": int(row["base_lingqi"]),
                        "breakthrough_rate": float(row["breakthrough_rate"]),
                        "death_rate": float(row["death_rate"]),
                    }
                    if int(row["high_realm"]):
                        cfg["high_realm"] = True
                    raw_costs = str(row["sub_dao_yun_costs"] or "").strip()
                    if raw_costs:
                        try:
                            cfg["sub_dao_yun_costs"] = _json.loads(raw_costs)
                        except (ValueError, TypeError):
                            pass
                    bt_cost = int(row["breakthrough_dao_yun_cost"] or 0)
                    if bt_cost > 0:
                        cfg["breakthrough_dao_yun_cost"] = bt_cost
                    default_cfg = DEFAULT_REALM_CONFIG.get(level, {})
                    base_rate, per_sub, dao_initialized = self._resolve_realm_dao_yun_rates(row, default_cfg)
                    if (
                        dao_initialized
                        or base_rate > 0
                        or per_sub > 0
                        or "dao_yun_base_rate" in default_cfg
                        or "dao_yun_per_sub_realm" in default_cfg
                    ):
                        cfg["dao_yun_base_rate"] = base_rate
                        cfg["dao_yun_per_sub_realm"] = per_sub
                    realms[level] = cfg
        except Exception:
            return dict(DEFAULT_REALM_CONFIG)
        return realms if realms else dict(DEFAULT_REALM_CONFIG)

    async def admin_list_realms(self) -> list[dict[str, Any]]:
        from .constants import REALM_CONFIG as DEFAULT_REALM_CONFIG

        result = []
        async with self.db.execute(
            """
            SELECT level, name, has_sub_realm, high_realm,
                   exp_to_next, sub_exp_to_next,
                   base_hp, base_attack, base_defense, base_lingqi,
                   breakthrough_rate, death_rate,
                   sub_dao_yun_costs, breakthrough_dao_yun_cost,
                   dao_yun_base_rate, dao_yun_per_sub_realm,
                   dao_yun_rate_initialized
            FROM realms
            ORDER BY level ASC
            """
        ) as cur:
            async for row in cur:
                item = dict(row)
                default_cfg = DEFAULT_REALM_CONFIG.get(int(item["level"]), {})
                base_rate, per_sub, _ = self._resolve_realm_dao_yun_rates(item, default_cfg)
                item["dao_yun_base_rate"] = base_rate
                item["dao_yun_per_sub_realm"] = per_sub
                item.pop("dao_yun_rate_initialized", None)
                result.append(item)
        return result

    async def admin_has_realm_name(self, name: str, exclude_level: int | None = None) -> bool:
        if exclude_level is not None:
            async with self.db.execute(
                "SELECT 1 FROM realms WHERE name = ? AND level != ? LIMIT 1",
                (name, exclude_level),
            ) as cur:
                return (await cur.fetchone()) is not None
        async with self.db.execute(
            "SELECT 1 FROM realms WHERE name = ? LIMIT 1", (name,),
        ) as cur:
            return (await cur.fetchone()) is not None

    async def admin_create_realm(self, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO realms (
                level, name, has_sub_realm, high_realm,
                exp_to_next, sub_exp_to_next,
                base_hp, base_attack, base_defense, base_lingqi,
                breakthrough_rate, death_rate,
                sub_dao_yun_costs, breakthrough_dao_yun_cost,
                dao_yun_base_rate, dao_yun_per_sub_realm,
                dao_yun_rate_initialized
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(data["level"]),
                data["name"],
                int(data.get("has_sub_realm", 0)),
                int(data.get("high_realm", 0)),
                int(data.get("exp_to_next", 100)),
                int(data.get("sub_exp_to_next", 0)),
                int(data.get("base_hp", 100)),
                int(data.get("base_attack", 10)),
                int(data.get("base_defense", 5)),
                int(data.get("base_lingqi", 50)),
                float(data.get("breakthrough_rate", 1.0)),
                float(data.get("death_rate", 0.0)),
                str(data.get("sub_dao_yun_costs", "")),
                int(data.get("breakthrough_dao_yun_cost", 0)),
                float(data.get("dao_yun_base_rate", 0.0)),
                float(data.get("dao_yun_per_sub_realm", 0.0)),
                1,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_realm(self, level: int, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            UPDATE realms
            SET name = ?, has_sub_realm = ?, high_realm = ?,
                exp_to_next = ?, sub_exp_to_next = ?,
                base_hp = ?, base_attack = ?, base_defense = ?, base_lingqi = ?,
                breakthrough_rate = ?, death_rate = ?,
                sub_dao_yun_costs = ?, breakthrough_dao_yun_cost = ?,
                dao_yun_base_rate = ?, dao_yun_per_sub_realm = ?,
                dao_yun_rate_initialized = 1
            WHERE level = ?
            """,
            (
                data["name"],
                int(data.get("has_sub_realm", 0)),
                int(data.get("high_realm", 0)),
                int(data.get("exp_to_next", 100)),
                int(data.get("sub_exp_to_next", 0)),
                int(data.get("base_hp", 100)),
                int(data.get("base_attack", 10)),
                int(data.get("base_defense", 5)),
                int(data.get("base_lingqi", 50)),
                float(data.get("breakthrough_rate", 1.0)),
                float(data.get("death_rate", 0.0)),
                str(data.get("sub_dao_yun_costs", "")),
                int(data.get("breakthrough_dao_yun_cost", 0)),
                float(data.get("dao_yun_base_rate", 0.0)),
                float(data.get("dao_yun_per_sub_realm", 0.0)),
                level,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_realm(self, level: int) -> bool:
        cur = await self.db.execute(
            "DELETE FROM realms WHERE level = ?",
            (level,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def get_realm_names(self) -> dict[int, str]:
        """获取境界等级->名称映射（公开API用）。"""
        result = {}
        async with self.db.execute(
            "SELECT level, name FROM realms ORDER BY level ASC"
        ) as cur:
            async for row in cur:
                result[int(row["level"])] = row["name"]
        return result

    async def _seed_heart_methods(self):
        """若心法表为空或有缺失，按代码内置定义补齐。"""
        from .constants import HEART_METHOD_REGISTRY

        existing = set()
        async with self.db.execute("SELECT method_id FROM heart_methods") as cur:
            async for row in cur:
                existing.add(row[0])

        rows = []
        for hm in HEART_METHOD_REGISTRY.values():
            if hm.method_id in existing:
                continue
            rows.append(
                (
                    hm.method_id,
                    hm.name,
                    int(hm.realm),
                    int(hm.quality),
                    float(hm.exp_multiplier),
                    int(hm.attack_bonus),
                    int(hm.defense_bonus),
                    float(hm.dao_yun_rate),
                    hm.description,
                    int(hm.mastery_exp),
                    1,
                )
            )

        if not rows:
            return

        await self.db.executemany(
            """
            INSERT OR IGNORE INTO heart_methods (
                method_id, name, realm, quality, exp_multiplier,
                attack_bonus, defense_bonus, dao_yun_rate, description, mastery_exp, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self.db.commit()

    async def _seed_weapons(self):
        """若武器表为空或有缺失，按代码内置定义补齐。"""
        from .constants import EQUIPMENT_REGISTRY

        existing = set()
        async with self.db.execute("SELECT equip_id FROM weapons") as cur:
            async for row in cur:
                existing.add(row[0])

        rows = []
        for eq in EQUIPMENT_REGISTRY.values():
            if eq.equip_id in existing:
                continue
            rows.append(
                (
                    eq.equip_id,
                    eq.name,
                    int(eq.tier),
                    eq.slot,
                    int(eq.attack),
                    int(eq.defense),
                    eq.element,
                    int(eq.element_damage),
                    eq.description,
                    1,
                )
            )

        if not rows:
            return

        await self.db.executemany(
            """
            INSERT OR IGNORE INTO weapons (
                equip_id, name, tier, slot, attack, defense,
                element, element_damage, description, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self.db.commit()

    async def _seed_gongfas(self):
        """若功法表为空或有缺失，按代码内置定义补齐。"""
        from .constants import GONGFA_REGISTRY

        existing = set()
        async with self.db.execute("SELECT gongfa_id FROM gongfas") as cur:
            async for row in cur:
                existing.add(row[0])

        rows = []
        for gf in GONGFA_REGISTRY.values():
            if gf.gongfa_id in existing:
                continue
            rows.append(
                (
                    gf.gongfa_id,
                    gf.name,
                    int(gf.tier),
                    int(gf.attack_bonus),
                    int(gf.defense_bonus),
                    int(gf.hp_regen),
                    int(gf.lingqi_regen),
                    gf.description,
                    int(gf.mastery_exp),
                    int(gf.dao_yun_cost),
                    int(gf.recycle_price),
                    int(gf.lingqi_cost),
                    1,
                )
            )

        if not rows:
            return

        await self.db.executemany(
            """
            INSERT OR IGNORE INTO gongfas (
                gongfa_id, name, tier, attack_bonus, defense_bonus,
                hp_regen, lingqi_regen, description, mastery_exp,
                dao_yun_cost, recycle_price, lingqi_cost, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self.db.commit()

    async def _sync_gongfa_lingqi_costs(self):
        """为旧数据补齐耗灵字段，避免重载后功法耗灵归零。"""
        from .constants import calc_gongfa_lingqi_cost

        rows = []
        async with self.db.execute(
            """
            SELECT gongfa_id, tier, attack_bonus, defense_bonus, hp_regen, lingqi_regen, lingqi_cost
            FROM gongfas
            """
        ) as cur:
            async for row in cur:
                current_cost = int(row["lingqi_cost"] or 0)
                if current_cost > 0:
                    continue
                rows.append((
                    calc_gongfa_lingqi_cost(
                        int(row["tier"] or 0),
                        int(row["attack_bonus"] or 0),
                        int(row["defense_bonus"] or 0),
                        int(row["hp_regen"] or 0),
                        int(row["lingqi_regen"] or 0),
                    ),
                    row["gongfa_id"],
                ))

        if not rows:
            return

        await self.db.executemany(
            "UPDATE gongfas SET lingqi_cost = ? WHERE gongfa_id = ?",
            rows,
        )
        await self.db.commit()

    async def _seed_pills(self):
        """若丹药表为空或有缺失，按代码内置定义补齐。"""
        import json as _json
        from .pills import PILL_REGISTRY as DEFAULT_PILL_REGISTRY

        existing = set()
        async with self.db.execute("SELECT pill_id FROM pills") as cur:
            async for row in cur:
                existing.add(row[0])

        rows = []
        for pill in DEFAULT_PILL_REGISTRY.values():
            if pill.pill_id in existing:
                continue
            rows.append((
                pill.pill_id,
                pill.name,
                int(pill.tier),
                int(pill.grade),
                pill.category,
                pill.description,
                int(pill.price),
                _json.dumps(dict(pill.effects), ensure_ascii=False),
                1 if pill.is_temp else 0,
                int(pill.duration or 0),
                _json.dumps(dict(pill.side_effects), ensure_ascii=False),
                pill.side_effect_desc or "",
                1,
            ))

        if not rows:
            return

        await self.db.executemany(
            """
            INSERT OR IGNORE INTO pills (
                pill_id, name, tier, grade, category, description, price,
                effects, is_temp, duration, side_effects, side_effect_desc, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self.db.commit()

    # ── 种子数据：材料 ──────────────────────────────────────

    async def _seed_materials(self):
        """若材料表为空或有缺失，按代码内置默认数据补齐。"""
        from .constants import _DEFAULT_MATERIALS

        existing = set()
        async with self.db.execute("SELECT item_id FROM materials") as cur:
            async for row in cur:
                existing.add(row[0])

        rows = []
        for mat in _DEFAULT_MATERIALS.values():
            if mat.item_id in existing:
                continue
            rows.append((
                mat.item_id,
                mat.name,
                int(mat.rarity),
                mat.category,
                mat.source,
                mat.description,
                int(mat.recycle_price),
            ))

        if not rows:
            return

        await self.db.executemany(
            """
            INSERT OR IGNORE INTO materials (
                item_id, name, rarity, category, source, description, recycle_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self.db.commit()

    # ── 种子数据：丹方 ──────────────────────────────────────

    async def _seed_pill_recipes(self):
        """若丹方表为空或有缺失，按代码内置默认数据补齐（程序化+特殊+旧版兼容）；同时修正已知的错误 pill_id。"""
        from .constants import _DEFAULT_PILL_RECIPES
        import json as _json

        # 修正已知的错误 pill_id（数据库中可能存了旧数据）
        known_fixes = {
            "pill_special_rebirth": "pill_special_reborn",
            "pill_special_formless": "pill_special_wuxiang",
        }
        for old_id, new_id in known_fixes.items():
            await self.db.execute(
                "UPDATE pill_recipes SET pill_id = ? WHERE pill_id = ?",
                (new_id, old_id),
            )
        await self.db.commit()

        rows = []
        for recipe in _DEFAULT_PILL_RECIPES.values():
            rows.append((
                recipe.recipe_id,
                recipe.pill_id,
                int(recipe.grade),
                _json.dumps({"item_id": recipe.main_material.item_id, "qty": recipe.main_material.qty}),
                _json.dumps({"item_id": recipe.auxiliary_material.item_id, "qty": recipe.auxiliary_material.qty}),
                _json.dumps({"item_id": recipe.catalyst.item_id, "qty": recipe.catalyst.qty}),
                _json.dumps({"item_id": recipe.forming_material.item_id, "qty": recipe.forming_material.qty}),
            ))

        if not rows:
            return

        await self.db.executemany(
            """
            INSERT INTO pill_recipes (
                recipe_id, pill_id, grade,
                main_material, auxiliary_material, catalyst, forming_material
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(recipe_id) DO UPDATE SET
                pill_id = excluded.pill_id,
                grade = excluded.grade,
                main_material = excluded.main_material,
                auxiliary_material = excluded.auxiliary_material,
                catalyst = excluded.catalyst,
                forming_material = excluded.forming_material
            """,
            rows,
        )
        await self.db.commit()

    async def load_gongfas(self) -> dict:
        """加载启用的功法定义（独立表 -> 运行时）。"""
        from .constants import GONGFA_REGISTRY, GongfaDef, calc_gongfa_lingqi_cost

        gongfas = {}
        try:
            async with self.db.execute(
                """
                SELECT gongfa_id, name, tier, attack_bonus, defense_bonus,
                       hp_regen, lingqi_regen, description, mastery_exp,
                       dao_yun_cost, recycle_price, lingqi_cost
                FROM gongfas
                WHERE enabled = 1
                ORDER BY tier ASC, gongfa_id ASC
                """
            ) as cur:
                async for row in cur:
                    gongfa_id = row["gongfa_id"]
                    tier = int(row["tier"] or 0)
                    attack_bonus = int(row["attack_bonus"] or 0)
                    defense_bonus = int(row["defense_bonus"] or 0)
                    hp_regen = int(row["hp_regen"] or 0)
                    lingqi_regen = int(row["lingqi_regen"] or 0)
                    gongfas[gongfa_id] = GongfaDef(
                        gongfa_id=gongfa_id,
                        name=row["name"],
                        tier=tier,
                        attack_bonus=attack_bonus,
                        defense_bonus=defense_bonus,
                        hp_regen=hp_regen,
                        lingqi_regen=lingqi_regen,
                        description=row["description"] or "",
                        mastery_exp=int(row["mastery_exp"] or 200),
                        dao_yun_cost=int(row["dao_yun_cost"] or 0),
                        recycle_price=int(row["recycle_price"] or 1000),
                        lingqi_cost=int(row["lingqi_cost"] or 0) or calc_gongfa_lingqi_cost(
                            tier,
                            attack_bonus,
                            defense_bonus,
                            hp_regen,
                            lingqi_regen,
                        ),
                    )
        except Exception:
            return dict(GONGFA_REGISTRY)
        return gongfas

    async def load_pills(self) -> dict:
        """加载启用的丹药定义（独立表 -> 运行时）。"""
        import json as _json
        from .pills import PILL_REGISTRY as DEFAULT_PILL_REGISTRY, PillDef

        pills = {}
        try:
            async with self.db.execute(
                """
                SELECT pill_id, name, tier, grade, category, description, price,
                       effects, is_temp, duration, side_effects, side_effect_desc
                FROM pills
                WHERE enabled = 1
                ORDER BY tier ASC, grade ASC, pill_id ASC
                """
            ) as cur:
                async for row in cur:
                    pill_id = row["pill_id"]
                    effects = _json.loads(row["effects"] or "{}")
                    side_effects = _json.loads(row["side_effects"] or "{}")
                    pills[pill_id] = PillDef(
                        pill_id=pill_id,
                        name=row["name"],
                        tier=int(row["tier"] or 0),
                        grade=int(row["grade"] or 0),
                        category=row["category"] or "healing",
                        description=row["description"] or "",
                        price=int(row["price"] or 0),
                        effects=effects if isinstance(effects, dict) else {},
                        is_temp=bool(int(row["is_temp"] or 0)),
                        duration=int(row["duration"] or 0),
                        side_effects=side_effects if isinstance(side_effects, dict) else {},
                        side_effect_desc=row["side_effect_desc"] or "",
                    )
        except Exception:
            return dict(DEFAULT_PILL_REGISTRY)
        return pills if pills else dict(DEFAULT_PILL_REGISTRY)

    async def admin_list_gongfas(self) -> list[dict[str, Any]]:
        result = []
        async with self.db.execute(
            """
            SELECT gongfa_id, name, tier, attack_bonus, defense_bonus,
                   hp_regen, lingqi_regen, description, mastery_exp,
                   dao_yun_cost, recycle_price, lingqi_cost, enabled
            FROM gongfas
            ORDER BY tier ASC, gongfa_id ASC
            """
        ) as cur:
            async for row in cur:
                result.append(dict(row))
        return result

    async def admin_has_gongfa_name(self, name: str) -> bool:
        """检查功法名称是否已存在（不区分大小写，忽略首尾空白）。"""
        async with self.db.execute(
            """
            SELECT 1
            FROM gongfas
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (name,),
        ) as cur:
            row = await cur.fetchone()
            return row is not None

    async def admin_create_gongfa(self, data: dict[str, Any]) -> bool:
        from .constants import calc_gongfa_lingqi_cost

        lingqi_cost = data.get("lingqi_cost")
        if lingqi_cost is None:
            lingqi_cost = calc_gongfa_lingqi_cost(
                int(data["tier"]),
                int(data.get("attack_bonus", 0)),
                int(data.get("defense_bonus", 0)),
                int(data.get("hp_regen", 0)),
                int(data.get("lingqi_regen", 0)),
            )
        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO gongfas (
                gongfa_id, name, tier, attack_bonus, defense_bonus,
                hp_regen, lingqi_regen, description, mastery_exp,
                dao_yun_cost, recycle_price, lingqi_cost, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["gongfa_id"],
                data["name"],
                int(data["tier"]),
                int(data.get("attack_bonus", 0)),
                int(data.get("defense_bonus", 0)),
                int(data.get("hp_regen", 0)),
                int(data.get("lingqi_regen", 0)),
                str(data.get("description", "")),
                int(data.get("mastery_exp", 200)),
                int(data.get("dao_yun_cost", 0)),
                int(data.get("recycle_price", 1000)),
                int(lingqi_cost),
                int(data.get("enabled", 1)),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_gongfa(self, gongfa_id: str, data: dict[str, Any]) -> bool:
        from .constants import calc_gongfa_lingqi_cost

        lingqi_cost = data.get("lingqi_cost")
        if lingqi_cost is None:
            lingqi_cost = calc_gongfa_lingqi_cost(
                int(data["tier"]),
                int(data.get("attack_bonus", 0)),
                int(data.get("defense_bonus", 0)),
                int(data.get("hp_regen", 0)),
                int(data.get("lingqi_regen", 0)),
            )
        cur = await self.db.execute(
            """
            UPDATE gongfas
            SET name = ?, tier = ?, attack_bonus = ?, defense_bonus = ?,
                hp_regen = ?, lingqi_regen = ?, description = ?, mastery_exp = ?,
                dao_yun_cost = ?, recycle_price = ?, lingqi_cost = ?, enabled = ?
            WHERE gongfa_id = ?
            """,
            (
                data["name"],
                int(data["tier"]),
                int(data.get("attack_bonus", 0)),
                int(data.get("defense_bonus", 0)),
                int(data.get("hp_regen", 0)),
                int(data.get("lingqi_regen", 0)),
                str(data.get("description", "")),
                int(data.get("mastery_exp", 200)),
                int(data.get("dao_yun_cost", 0)),
                int(data.get("recycle_price", 1000)),
                int(lingqi_cost),
                int(data.get("enabled", 1)),
                gongfa_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_gongfa(self, gongfa_id: str) -> bool:
        cur = await self.db.execute(
            "DELETE FROM gongfas WHERE gongfa_id = ?",
            (gongfa_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_list_pills(self) -> list[dict[str, Any]]:
        result = []
        async with self.db.execute(
            """
            SELECT pill_id, name, tier, grade, category, description, price,
                   effects, is_temp, duration, side_effects, side_effect_desc, enabled
            FROM pills
            ORDER BY tier ASC, grade ASC, pill_id ASC
            """
        ) as cur:
            async for row in cur:
                result.append(dict(row))
        return result

    async def admin_has_pill_name(self, name: str) -> bool:
        async with self.db.execute(
            """
            SELECT 1
            FROM pills
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (name,),
        ) as cur:
            return (await cur.fetchone()) is not None

    async def admin_create_pill(self, data: dict[str, Any]) -> bool:
        import json as _json

        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO pills (
                pill_id, name, tier, grade, category, description, price,
                effects, is_temp, duration, side_effects, side_effect_desc, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["pill_id"],
                data["name"],
                int(data["tier"]),
                int(data["grade"]),
                str(data.get("category", "healing")),
                str(data.get("description", "")),
                int(data.get("price", 0)),
                _json.dumps(data.get("effects", {}), ensure_ascii=False),
                int(data.get("is_temp", 0)),
                int(data.get("duration", 0)),
                _json.dumps(data.get("side_effects", {}), ensure_ascii=False),
                str(data.get("side_effect_desc", "")),
                int(data.get("enabled", 1)),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_pill(self, pill_id: str, data: dict[str, Any]) -> bool:
        import json as _json

        cur = await self.db.execute(
            """
            UPDATE pills
            SET name = ?, tier = ?, grade = ?, category = ?, description = ?, price = ?,
                effects = ?, is_temp = ?, duration = ?, side_effects = ?, side_effect_desc = ?, enabled = ?
            WHERE pill_id = ?
            """,
            (
                data["name"],
                int(data["tier"]),
                int(data["grade"]),
                str(data.get("category", "healing")),
                str(data.get("description", "")),
                int(data.get("price", 0)),
                _json.dumps(data.get("effects", {}), ensure_ascii=False),
                int(data.get("is_temp", 0)),
                int(data.get("duration", 0)),
                _json.dumps(data.get("side_effects", {}), ensure_ascii=False),
                str(data.get("side_effect_desc", "")),
                int(data.get("enabled", 1)),
                pill_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_pill(self, pill_id: str) -> bool:
        cur = await self.db.execute(
            "DELETE FROM pills WHERE pill_id = ?",
            (pill_id,),
        )
        if (cur.rowcount or 0) > 0:
            await self.db.execute(
                "DELETE FROM pill_recipes WHERE pill_id = ?",
                (pill_id,),
            )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ── 管理员 CRUD：材料 ──────────────────────────────────

    async def load_materials(self) -> dict:
        """加载材料定义（独立表 -> 运行时 MATERIAL_REGISTRY）。"""
        from .constants import MaterialDef, _DEFAULT_MATERIALS

        materials = {}
        try:
            async with self.db.execute(
                """
                SELECT item_id, name, rarity, category, source, description, recycle_price
                FROM materials
                ORDER BY rarity ASC, category ASC, item_id ASC
                """
            ) as cur:
                async for row in cur:
                    materials[row["item_id"]] = MaterialDef(
                        item_id=row["item_id"],
                        name=row["name"],
                        rarity=int(row["rarity"] or 0),
                        category=row["category"] or "herb",
                        source=row["source"] or "",
                        description=row["description"] or "",
                        recycle_price=int(row["recycle_price"] or 0),
                    )
        except Exception:
            return dict(_DEFAULT_MATERIALS)
        return materials

    async def admin_list_materials(self) -> list[dict[str, Any]]:
        result = []
        async with self.db.execute(
            """
            SELECT item_id, name, rarity, category, source, description, recycle_price
            FROM materials
            ORDER BY rarity ASC, category ASC, item_id ASC
            """
        ) as cur:
            async for row in cur:
                result.append(dict(row))
        return result

    async def admin_has_material_name(self, name: str) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM materials WHERE LOWER(TRIM(name)) = LOWER(TRIM(?)) LIMIT 1",
            (name,),
        ) as cur:
            return (await cur.fetchone()) is not None

    async def admin_create_material(self, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO materials (
                item_id, name, rarity, category, source, description, recycle_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["item_id"],
                data["name"],
                int(data.get("rarity", 0)),
                str(data.get("category", "herb")),
                str(data.get("source", "")),
                str(data.get("description", "")),
                int(data.get("recycle_price", 0)),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_material(self, item_id: str, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            UPDATE materials
            SET name = ?, rarity = ?, category = ?, source = ?, description = ?, recycle_price = ?
            WHERE item_id = ?
            """,
            (
                data["name"],
                int(data.get("rarity", 0)),
                str(data.get("category", "herb")),
                str(data.get("source", "")),
                str(data.get("description", "")),
                int(data.get("recycle_price", 0)),
                item_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_material(self, item_id: str) -> bool:
        cur = await self.db.execute(
            "DELETE FROM materials WHERE item_id = ?",
            (item_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ── 管理员 CRUD：丹方 ──────────────────────────────────

    async def load_pill_recipes(self) -> dict:
        """加载丹方定义（独立表 -> 运行时 PILL_RECIPE_REGISTRY）。"""
        import json as _json
        from .constants import PillRecipeDef, PillRecipeMaterial, _DEFAULT_PILL_RECIPES

        recipes = {}
        try:
            async with self.db.execute(
                """
                SELECT recipe_id, pill_id, grade,
                       main_material, auxiliary_material, catalyst, forming_material
                FROM pill_recipes
                ORDER BY pill_id ASC, grade ASC
                """
            ) as cur:
                async for row in cur:
                    def _mat(j: dict) -> PillRecipeMaterial:
                        d = _json.loads(j) if isinstance(j, str) else (j or {})
                        return PillRecipeMaterial(item_id=d.get("item_id", ""), qty=d.get("qty", 1))
                    recipes[row["recipe_id"]] = PillRecipeDef(
                        recipe_id=row["recipe_id"],
                        pill_id=row["pill_id"],
                        grade=int(row["grade"] or 0),
                        main_material=_mat(row["main_material"]),
                        auxiliary_material=_mat(row["auxiliary_material"]),
                        catalyst=_mat(row["catalyst"]),
                        forming_material=_mat(row["forming_material"]),
                    )
        except Exception:
            return dict(_DEFAULT_PILL_RECIPES)
        return recipes

    async def admin_list_pill_recipes(self) -> list[dict[str, Any]]:
        import json as _json
        result = []
        async with self.db.execute(
            """
            SELECT recipe_id, pill_id, grade,
                   main_material, auxiliary_material, catalyst, forming_material
            FROM pill_recipes
            ORDER BY pill_id ASC, grade ASC
            """
        ) as cur:
            async for row in cur:
                item = dict(row)
                item["main_material"] = _json.loads(row["main_material"] or "{}")
                item["auxiliary_material"] = _json.loads(row["auxiliary_material"] or "{}")
                item["catalyst"] = _json.loads(row["catalyst"] or "{}")
                item["forming_material"] = _json.loads(row["forming_material"] or "{}")
                result.append(item)
        return result

    async def admin_has_pill_recipe(self, pill_id: str, grade: int) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM pill_recipes WHERE pill_id = ? AND grade = ? LIMIT 1",
            (pill_id, grade),
        ) as cur:
            return (await cur.fetchone()) is not None

    async def admin_create_pill_recipe(self, data: dict[str, Any]) -> bool:
        import json as _json
        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO pill_recipes (
                recipe_id, pill_id, grade,
                main_material, auxiliary_material, catalyst, forming_material
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["recipe_id"],
                data["pill_id"],
                int(data.get("grade", 0)),
                _json.dumps(data.get("main_material", {})),
                _json.dumps(data.get("auxiliary_material", {})),
                _json.dumps(data.get("catalyst", {})),
                _json.dumps(data.get("forming_material", {})),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_pill_recipe(self, recipe_id: str, data: dict[str, Any]) -> bool:
        import json as _json
        cur = await self.db.execute(
            """
            UPDATE pill_recipes
            SET pill_id = ?, grade = ?,
                main_material = ?, auxiliary_material = ?,
                catalyst = ?, forming_material = ?
            WHERE recipe_id = ?
            """,
            (
                data["pill_id"],
                int(data.get("grade", 0)),
                _json.dumps(data.get("main_material", {})),
                _json.dumps(data.get("auxiliary_material", {})),
                _json.dumps(data.get("catalyst", {})),
                _json.dumps(data.get("forming_material", {})),
                recipe_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_pill_recipe(self, recipe_id: str) -> bool:
        cur = await self.db.execute(
            "DELETE FROM pill_recipes WHERE recipe_id = ?",
            (recipe_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ==================== 管理员 CRUD：心法 ====================

    async def admin_list_heart_methods(self) -> list[dict[str, Any]]:
        result = []
        async with self.db.execute(
            """
            SELECT method_id, name, realm, quality, exp_multiplier,
                   attack_bonus, defense_bonus, dao_yun_rate, description, mastery_exp, enabled
            FROM heart_methods
            ORDER BY realm ASC, quality ASC, method_id ASC
            """
        ) as cur:
            async for row in cur:
                result.append(dict(row))
        return result

    async def admin_has_heart_method_name(self, name: str) -> bool:
        """检查心法名称是否已存在（不区分大小写，忽略首尾空白）。"""
        async with self.db.execute(
            """
            SELECT 1
            FROM heart_methods
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (name,),
        ) as cur:
            row = await cur.fetchone()
            return row is not None

    async def admin_create_heart_method(self, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO heart_methods (
                method_id, name, realm, quality, exp_multiplier,
                attack_bonus, defense_bonus, dao_yun_rate, description, mastery_exp, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["method_id"],
                data["name"],
                int(data["realm"]),
                int(data["quality"]),
                float(data.get("exp_multiplier", 0.0)),
                int(data.get("attack_bonus", 0)),
                int(data.get("defense_bonus", 0)),
                float(data.get("dao_yun_rate", 0.0)),
                str(data.get("description", "")),
                int(data.get("mastery_exp", 100)),
                int(data.get("enabled", 1)),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_heart_method(self, method_id: str, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            UPDATE heart_methods
            SET name = ?, realm = ?, quality = ?, exp_multiplier = ?,
                attack_bonus = ?, defense_bonus = ?, dao_yun_rate = ?,
                description = ?, mastery_exp = ?, enabled = ?
            WHERE method_id = ?
            """,
            (
                data["name"],
                int(data["realm"]),
                int(data["quality"]),
                float(data.get("exp_multiplier", 0.0)),
                int(data.get("attack_bonus", 0)),
                int(data.get("defense_bonus", 0)),
                float(data.get("dao_yun_rate", 0.0)),
                str(data.get("description", "")),
                int(data.get("mastery_exp", 100)),
                int(data.get("enabled", 1)),
                method_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_heart_method(self, method_id: str) -> bool:
        cur = await self.db.execute(
            "DELETE FROM heart_methods WHERE method_id = ?",
            (method_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ==================== 管理员 CRUD：武器/护甲 ====================

    async def admin_list_weapons(self) -> list[dict[str, Any]]:
        result = []
        async with self.db.execute(
            """
            SELECT equip_id, name, tier, slot, attack, defense,
                   element, element_damage, description, enabled
            FROM weapons
            ORDER BY tier ASC, slot ASC, equip_id ASC
            """
        ) as cur:
            async for row in cur:
                result.append(dict(row))
        return result

    async def admin_has_weapon_name(self, name: str) -> bool:
        """检查装备名称是否已存在（不区分大小写，忽略首尾空白）。"""
        async with self.db.execute(
            """
            SELECT 1
            FROM weapons
            WHERE LOWER(TRIM(name)) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (name,),
        ) as cur:
            row = await cur.fetchone()
            return row is not None

    async def admin_create_weapon(self, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            INSERT OR IGNORE INTO weapons (
                equip_id, name, tier, slot, attack, defense,
                element, element_damage, description, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["equip_id"],
                data["name"],
                int(data["tier"]),
                data["slot"],
                int(data.get("attack", 0)),
                int(data.get("defense", 0)),
                str(data.get("element", "无") or "无"),
                int(data.get("element_damage", 0)),
                str(data.get("description", "")),
                int(data.get("enabled", 1)),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_weapon(self, equip_id: str, data: dict[str, Any]) -> bool:
        cur = await self.db.execute(
            """
            UPDATE weapons
            SET name = ?, tier = ?, slot = ?, attack = ?, defense = ?,
                element = ?, element_damage = ?, description = ?, enabled = ?
            WHERE equip_id = ?
            """,
            (
                data["name"],
                int(data["tier"]),
                data["slot"],
                int(data.get("attack", 0)),
                int(data.get("defense", 0)),
                str(data.get("element", "无") or "无"),
                int(data.get("element_damage", 0)),
                str(data.get("description", "")),
                int(data.get("enabled", 1)),
                equip_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_weapon(self, equip_id: str) -> bool:
        cur = await self.db.execute(
            "DELETE FROM weapons WHERE equip_id = ?",
            (equip_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ── 坊市 (Market) CRUD ──────────────────────────────────

    async def insert_market_listing(self, listing: dict) -> None:
        """插入一条上架记录。"""
        await self.db.execute(
            """INSERT INTO market_listings
               (listing_id, seller_id, item_id, quantity, unit_price,
                total_price, fee, listed_at, expires_at, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                listing["listing_id"],
                listing["seller_id"],
                listing["item_id"],
                listing["quantity"],
                listing["unit_price"],
                listing["total_price"],
                listing["fee"],
                listing["listed_at"],
                listing["expires_at"],
                listing.get("status", "active"),
            ),
        )
        await self.db.commit()

    async def get_active_listings(
        self,
        page: int = 1,
        page_size: int = 20,
        item_id: str | None = None,
        seller_id: str | None = None,
    ) -> dict:
        """分页查询活跃商品。"""
        conditions = ["status = 'active'"]
        params: list = []
        if item_id:
            conditions.append("item_id = ?")
            params.append(item_id)
        if seller_id:
            conditions.append("seller_id = ?")
            params.append(seller_id)
        where = " AND ".join(conditions)

        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        try:
            page_size = int(page_size)
        except (TypeError, ValueError):
            page_size = 20
        page = max(1, page)
        page_size = max(1, page_size)

        row = await self.db.execute(
            f"SELECT COUNT(*) FROM market_listings WHERE {where}", params,
        )
        total = (await row.fetchone())[0]
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = min(page, total_pages)

        offset = (page - 1) * page_size
        params_page = list(params) + [page_size, offset]
        cur = await self.db.execute(
            f"""SELECT * FROM market_listings
                WHERE {where}
                ORDER BY listed_at DESC
                LIMIT ? OFFSET ?""",
            params_page,
        )
        rows = await cur.fetchall()
        columns = [d[0] for d in cur.description]
        listings = [dict(zip(columns, r)) for r in rows]
        return {
            "listings": listings,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    async def get_listing_by_id(self, listing_id: str) -> dict | None:
        """单条查询上架记录。"""
        cur = await self.db.execute(
            "SELECT * FROM market_listings WHERE listing_id = ?",
            (listing_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, row))

    async def get_listing_by_id_prefix(self, prefix: str) -> dict | None:
        """通过 listing_id 前缀模糊查询（聊天端短编号）。"""
        cur = await self.db.execute(
            "SELECT * FROM market_listings WHERE listing_id LIKE ? AND status = 'active'",
            (prefix + "%",),
        )
        rows = await cur.fetchall()
        if len(rows) != 1:
            return None
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, rows[0]))

    async def update_listing_status(
        self,
        listing_id: str,
        status: str,
        buyer_id: str | None = None,
        sold_at: float | None = None,
        expected_status: str | None = None,
    ) -> int:
        """更新上架状态，返回受影响行数。"""
        if expected_status:
            cur = await self.db.execute(
                """UPDATE market_listings
                   SET status = ?, buyer_id = ?, sold_at = ?
                   WHERE listing_id = ? AND status = ?""",
                (status, buyer_id, sold_at, listing_id, expected_status),
            )
        else:
            cur = await self.db.execute(
                """UPDATE market_listings
                   SET status = ?, buyer_id = ?, sold_at = ?
                   WHERE listing_id = ?""",
                (status, buyer_id, sold_at, listing_id),
            )
        await self.db.commit()
        return cur.rowcount

    async def insert_market_history(self, record: dict) -> None:
        """插入成交记录。"""
        await self.db.execute(
            """INSERT INTO market_history
               (history_id, item_id, quantity, unit_price, total_price,
                fee, seller_id, buyer_id, sold_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record["history_id"],
                record["item_id"],
                record["quantity"],
                record["unit_price"],
                record["total_price"],
                record["fee"],
                record["seller_id"],
                record["buyer_id"],
                record["sold_at"],
            ),
        )
        await self.db.commit()

    async def get_market_stats(self, item_id: str, days: int = 7) -> dict:
        """获取指定物品近 N 天的均价和成交量。"""
        import time as _time
        cutoff = _time.time() - days * 86400
        cur = await self.db.execute(
            """SELECT COUNT(*) as cnt,
                      COALESCE(AVG(unit_price), 0) as avg_price,
                      COALESCE(SUM(quantity), 0) as total_qty
               FROM market_history
               WHERE item_id = ? AND sold_at >= ?""",
            (item_id, cutoff),
        )
        row = await cur.fetchone()
        return {
            "count": row[0],
            "avg_price": row[1],
            "total_quantity": row[2],
        }

    async def get_expired_active_listings(self, now: float) -> list[dict]:
        """查询已过期但仍为 active 的上架记录。"""
        cur = await self.db.execute(
            "SELECT * FROM market_listings WHERE status = 'active' AND expires_at <= ?",
            (now,),
        )
        rows = await cur.fetchall()
        if not rows:
            return []
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, r)) for r in rows]

    async def get_my_listings(self, seller_id: str) -> list[dict]:
        """查询某玩家的所有上架记录（按时间倒序，最多50条）。"""
        cur = await self.db.execute(
            """SELECT * FROM market_listings
               WHERE seller_id = ?
               ORDER BY listed_at DESC
               LIMIT 50""",
            (seller_id,),
        )
        rows = await cur.fetchall()
        if not rows:
            return []
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, r)) for r in rows]

    async def clear_my_listing_history(self, seller_id: str, include_expired: bool = False) -> int:
        """清理某玩家历史上架记录（已售/已下架，可选含已过期），返回删除条数。"""
        statuses = ["sold", "cancelled"]
        if include_expired:
            statuses.append("expired")
        placeholders = ",".join("?" for _ in statuses)
        params = [seller_id] + statuses
        cur = await self.db.execute(
            f"""DELETE FROM market_listings
               WHERE seller_id = ? AND status IN ({placeholders})""",
            params,
        )
        await self.db.commit()
        return int(cur.rowcount or 0)

    async def admin_list_market_listings(
        self,
        page: int = 1,
        page_size: int = 20,
        status: str = "",
        keyword: str = "",
    ) -> dict:
        """管理员分页查询坊市记录（支持状态/关键词过滤）。"""
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        try:
            page_size = int(page_size)
        except (TypeError, ValueError):
            page_size = 20
        page = max(1, page)
        page_size = min(100, max(1, page_size))

        conditions = ["1=1"]
        params: list[Any] = []

        if status:
            conditions.append("status = ?")
            params.append(status)

        keyword = str(keyword or "").strip()
        if keyword:
            like_kw = f"%{keyword}%"
            conditions.append(
                "(listing_id LIKE ? OR seller_id LIKE ? OR item_id LIKE ? OR COALESCE(buyer_id, '') LIKE ?)"
            )
            params.extend([like_kw, like_kw, like_kw, like_kw])

        where = " AND ".join(conditions)

        row = await self.db.execute(
            f"SELECT COUNT(*) FROM market_listings WHERE {where}",
            params,
        )
        total = int((await row.fetchone())[0])
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = min(page, total_pages)

        offset = (page - 1) * page_size
        cur = await self.db.execute(
            f"""SELECT * FROM market_listings
               WHERE {where}
               ORDER BY listed_at DESC
               LIMIT ? OFFSET ?""",
            list(params) + [page_size, offset],
        )
        rows = await cur.fetchall()
        columns = [d[0] for d in cur.description]
        listings = [dict(zip(columns, r)) for r in rows]
        return {
            "listings": listings,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    async def admin_create_market_listing(self, data: dict[str, Any]) -> bool:
        """管理员新增坊市记录。"""
        cur = await self.db.execute(
            """INSERT OR IGNORE INTO market_listings
               (listing_id, seller_id, item_id, quantity, unit_price, total_price,
                fee, listed_at, expires_at, status, buyer_id, sold_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data["listing_id"],
                data["seller_id"],
                data["item_id"],
                int(data["quantity"]),
                int(data["unit_price"]),
                int(data["total_price"]),
                int(data.get("fee", 0)),
                float(data["listed_at"]),
                float(data["expires_at"]),
                str(data.get("status", "active")),
                (str(data.get("buyer_id", "")).strip() or None),
                (float(data["sold_at"]) if data.get("sold_at") is not None else None),
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_update_market_listing(self, listing_id: str, data: dict[str, Any]) -> bool:
        """管理员更新坊市记录。"""
        cur = await self.db.execute(
            """UPDATE market_listings
               SET seller_id = ?, item_id = ?, quantity = ?, unit_price = ?, total_price = ?,
                   fee = ?, listed_at = ?, expires_at = ?, status = ?, buyer_id = ?, sold_at = ?
               WHERE listing_id = ?""",
            (
                data["seller_id"],
                data["item_id"],
                int(data["quantity"]),
                int(data["unit_price"]),
                int(data["total_price"]),
                int(data.get("fee", 0)),
                float(data["listed_at"]),
                float(data["expires_at"]),
                str(data.get("status", "active")),
                (str(data.get("buyer_id", "")).strip() or None),
                (float(data["sold_at"]) if data.get("sold_at") is not None else None),
                listing_id,
            ),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_market_listing(self, listing_id: str) -> bool:
        """管理员删除坊市记录。"""
        cur = await self.db.execute(
            "DELETE FROM market_listings WHERE listing_id = ?",
            (listing_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ── 天机阁（商店） ──────────────────────────────────────

    async def get_shop_sold_today(self, item_id: str, date_str: str) -> int:
        """获取某商品今日全服已购总数。"""
        cur = await self.db.execute(
            "SELECT COALESCE(SUM(quantity), 0) FROM shop_purchases WHERE item_id = ? AND purchased_at = ?",
            (item_id, date_str),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def get_player_shop_today(self, user_id: str, date_str: str) -> list[dict]:
        """获取某玩家今日购买记录。"""
        cur = await self.db.execute(
            "SELECT item_id, SUM(quantity) as qty, unit_price FROM shop_purchases WHERE user_id = ? AND purchased_at = ? GROUP BY item_id",
            (user_id, date_str),
        )
        rows = await cur.fetchall()
        return [{"item_id": r[0], "quantity": r[1], "unit_price": r[2]} for r in rows]

    async def record_shop_purchase(self, user_id: str, item_id: str, quantity: int, unit_price: int, date_str: str):
        """记录一笔天机阁购买。"""
        await self.db.execute(
            "INSERT INTO shop_purchases (user_id, item_id, quantity, unit_price, purchased_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, item_id, quantity, unit_price, date_str),
        )
        await self.db.commit()

    async def reserve_shop_purchase(
        self,
        user_id: str,
        item_id: str,
        quantity: int,
        unit_price: int,
        date_str: str,
        daily_limit: int,
    ) -> dict[str, int | bool]:
        """串行预占商店库存，避免并发下超卖。"""
        async with self._shop_purchase_lock:
            cur = await self.db.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM shop_purchases WHERE item_id = ? AND purchased_at = ?",
                (item_id, date_str),
            )
            row = await cur.fetchone()
            sold_today = int(row[0] or 0) if row else 0
            remaining = max(0, int(daily_limit) - sold_today)
            if quantity > remaining:
                return {"success": False, "remaining": remaining, "sold_today": sold_today}

            await self.db.execute(
                "INSERT INTO shop_purchases (user_id, item_id, quantity, unit_price, purchased_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, item_id, quantity, unit_price, date_str),
            )
            await self.db.commit()
            return {
                "success": True,
                "remaining": remaining - quantity,
                "sold_today": sold_today + quantity,
            }

    async def commit_shop_purchase_atomic(
        self,
        player: Player,
        item_id: str,
        quantity: int,
        unit_price: int,
        date_str: str,
        daily_limit: int = 0,
    ) -> dict[str, int | bool]:
        """以单个事务提交商店购买记录和玩家状态。"""
        conn: aiosqlite.Connection | None = None
        try:
            conn = await aiosqlite.connect(self._db_path)
            await conn.execute("PRAGMA busy_timeout = 30000")
            await conn.execute("BEGIN IMMEDIATE")

            sold_today = 0
            remaining = 0
            if daily_limit > 0:
                cur = await conn.execute(
                    "SELECT COALESCE(SUM(quantity), 0) FROM shop_purchases WHERE item_id = ? AND purchased_at = ?",
                    (item_id, date_str),
                )
                row = await cur.fetchone()
                sold_today = int(row[0] or 0) if row else 0
                remaining = max(0, int(daily_limit) - sold_today)
                if quantity > remaining:
                    await conn.rollback()
                    return {"success": False, "remaining": remaining, "sold_today": sold_today}

            await conn.execute(
                "INSERT INTO shop_purchases (user_id, item_id, quantity, unit_price, purchased_at) VALUES (?, ?, ?, ?, ?)",
                (player.user_id, item_id, quantity, unit_price, date_str),
            )
            await self._upsert_player(player, db=conn)
            await conn.commit()
            return {
                "success": True,
                "remaining": max(0, remaining - quantity) if daily_limit > 0 else 0,
                "sold_today": sold_today + quantity,
            }
        except Exception:
            if conn is not None:
                await conn.rollback()
            raise
        finally:
            if conn is not None:
                await conn.close()

    # ── 公告管理 ────────────────────────────────────────────
    async def get_active_announcements(self) -> list[dict]:
        """获取所有启用的公告。"""
        result = []
        async with self.db.execute(
            "SELECT id, title, content, created_at, updated_at FROM announcements WHERE enabled=1 ORDER BY id DESC"
        ) as cur:
            async for row in cur:
                result.append({
                    "id": row[0], "title": row[1], "content": row[2],
                    "created_at": row[3], "updated_at": row[4],
                })
        return result

    async def admin_list_announcements(self) -> list[dict]:
        """管理员获取全量公告列表。"""
        result = []
        async with self.db.execute(
            "SELECT id, title, content, enabled, created_at, updated_at FROM announcements ORDER BY id DESC"
        ) as cur:
            async for row in cur:
                result.append({
                    "id": row[0], "title": row[1], "content": row[2],
                    "enabled": row[3], "created_at": row[4], "updated_at": row[5],
                })
        return result

    async def admin_create_announcement(self, title: str, content: str) -> int:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = await self.db.execute(
            "INSERT INTO announcements (title, content, enabled, created_at, updated_at) VALUES (?, ?, 1, ?, ?)",
            (title, content, now, now),
        )
        await self.db.commit()
        return int(cur.lastrowid or 0)

    async def admin_update_announcement(self, ann_id: int, title: str, content: str, enabled: int) -> bool:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = await self.db.execute(
            """
            UPDATE announcements
            SET title = ?, content = ?, enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (title, content, enabled, now, ann_id),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    async def admin_delete_announcement(self, ann_id: int) -> bool:
        cur = await self.db.execute(
            "DELETE FROM announcements WHERE id = ?",
            (ann_id,),
        )
        await self.db.commit()
        return (cur.rowcount or 0) > 0

    # ── 世界频道消息 ──────────────────────────────────────────

    async def save_chat_message(
        self,
        user_id: str,
        name: str,
        realm: str,
        content: str,
        created_at: float,
        sect_name: str = "",
        sect_role: str = "",
        sect_role_name: str = "",
    ):
        """保存一条世界频道消息到数据库。"""
        await self.db.execute(
            "INSERT INTO world_chat_messages (user_id, name, realm, content, created_at, sect_name, sect_role, sect_role_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, name, realm, content, created_at, sect_name, sect_role, sect_role_name),
        )
        await self.db.commit()

    async def load_chat_history(self, limit: int = 100, max_age_seconds: float | None = None) -> list[dict]:
        """从数据库加载最近的世界频道消息（按时间升序）。"""
        if max_age_seconds is not None:
            cutoff = time.time() - max_age_seconds
            cursor = await self.db.execute(
                "SELECT user_id, name, realm, content, created_at, sect_name, sect_role, sect_role_name "
                "FROM world_chat_messages "
                "WHERE created_at >= ? "
                "ORDER BY created_at DESC LIMIT ?",
                (cutoff, limit),
            )
        else:
            cursor = await self.db.execute(
                "SELECT user_id, name, realm, content, created_at, sect_name, sect_role, sect_role_name FROM world_chat_messages ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        rows = await cursor.fetchall()
        result = []
        for row in reversed(rows):
            result.append({
                "user_id": row[0],
                "name": row[1],
                "realm": row[2],
                "content": row[3],
                "time": row[4],
                "sect_name": row[5] if len(row) > 5 else "",
                "sect_role": row[6] if len(row) > 6 else "",
                "sect_role_name": row[7] if len(row) > 7 else "",
            })
        return result

    async def cleanup_old_chat_messages(self, max_age_seconds: float) -> int:
        """删除超过指定时间的世界频道消息，返回删除条数。"""
        cutoff = time.time() - max_age_seconds
        cur = await self.db.execute(
            "DELETE FROM world_chat_messages WHERE created_at < ?",
            (cutoff,),
        )
        await self.db.commit()
        return cur.rowcount or 0

    # ── 宗门 CRUD ──────────────────────────────────────────

    async def save_sect(self, sect: dict) -> None:
        """新增宗门记录。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            """INSERT INTO sects
               (sect_id, name, leader_id, description, level, spirit_stones,
                max_members, join_policy, min_realm, created_at, announcement)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                sect["sect_id"],
                sect["name"],
                sect["leader_id"],
                sect.get("description", ""),
                sect.get("level", 1),
                sect.get("spirit_stones", 0),
                sect.get("max_members", 30),
                sect.get("join_policy", "open"),
                sect.get("min_realm", 0),
                sect["created_at"],
                sect.get("announcement", ""),
            ),
        )
        await self.db.commit()

    async def create_sect_with_leader(self, sect: dict, leader_user_id: str) -> None:
        """原子创建宗门及宗主成员关系。"""
        await self._ensure_sect_schema()
        try:
            await self.db.execute(
                """INSERT INTO sects
                   (sect_id, name, leader_id, description, level, spirit_stones,
                    max_members, join_policy, min_realm, created_at, announcement)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    sect["sect_id"],
                    sect["name"],
                    sect["leader_id"],
                    sect.get("description", ""),
                    sect.get("level", 1),
                    sect.get("spirit_stones", 0),
                    sect.get("max_members", 30),
                    sect.get("join_policy", "open"),
                    sect.get("min_realm", 0),
                    sect["created_at"],
                    sect.get("announcement", ""),
                ),
            )
            await self.db.execute(
                """INSERT INTO sect_members (user_id, sect_id, role, joined_at)
                   VALUES (?, ?, ?, ?)""",
                (leader_user_id, sect["sect_id"], "leader", time.time()),
            )
            await self.db.commit()
        except Exception:
            await self.db.rollback()
            raise

    async def delete_sect(self, sect_id: str) -> None:
        """删除宗门及其所有成员关系和申请记录。"""
        await self.db.execute("DELETE FROM sect_members WHERE sect_id = ?", (sect_id,))
        await self.db.execute("DELETE FROM sect_applications WHERE sect_id = ?", (sect_id,))
        await self.db.execute("DELETE FROM sects WHERE sect_id = ?", (sect_id,))
        await self.db.commit()

    async def load_sect(self, sect_id: str) -> dict | None:
        """按 sect_id 加载宗门。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute("SELECT * FROM sects WHERE sect_id = ?", (sect_id,))
        row = await cur.fetchone()
        if not row:
            return None
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, row))

    async def load_sect_by_name(self, name: str) -> dict | None:
        """按名称加载宗门。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute("SELECT * FROM sects WHERE name = ?", (name,))
        row = await cur.fetchone()
        if not row:
            return None
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, row))

    async def load_sects_page(self, page: int = 1, page_size: int = 10) -> dict:
        """分页查询宗门列表（含成员计数）。"""
        await self._ensure_sect_schema()
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        page = max(1, page)
        page_size = max(1, min(page_size, 50))

        row = await self.db.execute("SELECT COUNT(*) FROM sects")
        total = (await row.fetchone())[0]
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = min(page, total_pages)
        offset = (page - 1) * page_size

        cur = await self.db.execute(
            """SELECT s.*, COUNT(m.user_id) AS member_count
               FROM sects s
               LEFT JOIN sect_members m ON s.sect_id = m.sect_id
               GROUP BY s.sect_id
               ORDER BY s.created_at DESC
               LIMIT ? OFFSET ?""",
            (page_size, offset),
        )
        rows = await cur.fetchall()
        columns = [d[0] for d in cur.description]
        sects = [dict(zip(columns, r)) for r in rows]
        return {
            "sects": sects,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    async def update_sect_info(self, sect_id: str, data: dict) -> None:
        """更新宗门可变字段（description, join_policy, min_realm, announcement）。"""
        await self._ensure_sect_schema()
        allowed = {"description", "join_policy", "min_realm", "announcement"}
        sets = []
        params = []
        for k, v in data.items():
            if k in allowed:
                sets.append(f"{k} = ?")
                params.append(v)
        if not sets:
            return
        params.append(sect_id)
        await self.db.execute(
            f"UPDATE sects SET {', '.join(sets)} WHERE sect_id = ?",
            params,
        )
        await self.db.commit()

    async def update_sect_leader(self, sect_id: str, new_leader_id: str) -> None:
        """更新宗门宗主。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            "UPDATE sects SET leader_id = ? WHERE sect_id = ?",
            (new_leader_id, sect_id),
        )
        await self.db.commit()

    # ── 宗门成员 CRUD ──────────────────────────────────────

    async def save_sect_member(self, user_id: str, sect_id: str, role: str = "disciple") -> None:
        """添加宗门成员。"""
        await self._ensure_sect_schema()
        import time as _time
        await self.db.execute(
            """INSERT OR REPLACE INTO sect_members (user_id, sect_id, role, joined_at)
               VALUES (?, ?, ?, ?)""",
            (user_id, sect_id, role, _time.time()),
        )
        await self.db.commit()

    async def delete_sect_member(self, user_id: str) -> None:
        """移除宗门成员。"""
        await self.db.execute("DELETE FROM sect_members WHERE user_id = ?", (user_id,))
        await self.db.commit()

    async def load_sect_members(self, sect_id: str) -> list[dict]:
        """加载宗门所有成员（含玩家名、境界和贡献点）。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            """SELECT m.user_id, m.role, m.joined_at, m.contribution_points,
                      p.name AS player_name, p.realm, p.sub_realm
               FROM sect_members m
               LEFT JOIN players p ON m.user_id = p.user_id
               WHERE m.sect_id = ?
               ORDER BY
                   CASE m.role
                       WHEN 'leader' THEN 0
                       WHEN 'vice_leader' THEN 1
                       WHEN 'elder' THEN 2
                       ELSE 3
                   END,
                   m.joined_at ASC""",
            (sect_id,),
        )
        rows = await cur.fetchall()
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, r)) for r in rows]

    async def load_player_sect(self, user_id: str) -> dict | None:
        """查询玩家所在宗门（返回成员记录）。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT * FROM sect_members WHERE user_id = ?", (user_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        columns = [d[0] for d in cur.description]
        return dict(zip(columns, row))

    async def count_sect_members(self, sect_id: str) -> int:
        """统计宗门成员数。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT COUNT(*) FROM sect_members WHERE sect_id = ?", (sect_id,),
        )
        return (await cur.fetchone())[0]

    async def count_members_by_role(self, sect_id: str, role: str) -> int:
        """统计宗门某身份的成员数。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT COUNT(*) FROM sect_members WHERE sect_id = ? AND role = ?",
            (sect_id, role),
        )
        return (await cur.fetchone())[0]

    async def update_sect_member_role(self, user_id: str, role: str) -> None:
        """更新成员身份。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            "UPDATE sect_members SET role = ? WHERE user_id = ?",
            (role, user_id),
        )
        await self.db.commit()

    # ── 宗门仓库 ──────────────────────────────────────────

    async def get_sect_warehouse(self, sect_id: str) -> list[dict]:
        """获取宗门仓库所有物品。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT item_id, quantity FROM sect_warehouse WHERE sect_id = ? AND quantity > 0",
            (sect_id,),
        )
        rows = await cur.fetchall()
        return [{"item_id": r[0], "quantity": r[1]} for r in rows]

    async def get_sect_warehouse_item(self, sect_id: str, item_id: str) -> int:
        """获取宗门仓库中某物品数量。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT quantity FROM sect_warehouse WHERE sect_id = ? AND item_id = ?",
            (sect_id, item_id),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def get_sect_warehouse_slot_count(self, sect_id: str) -> int:
        """获取宗门仓库已使用格数（不同物品种类数）。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT COUNT(*) FROM sect_warehouse WHERE sect_id = ? AND quantity > 0",
            (sect_id,),
        )
        return (await cur.fetchone())[0]

    async def add_sect_warehouse_item(self, sect_id: str, item_id: str, quantity: int) -> None:
        """向宗门仓库添加物品。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            """INSERT INTO sect_warehouse (sect_id, item_id, quantity)
               VALUES (?, ?, ?)
               ON CONFLICT(sect_id, item_id) DO UPDATE SET quantity = quantity + ?""",
            (sect_id, item_id, quantity, quantity),
        )
        await self.db.commit()

    async def remove_sect_warehouse_item(self, sect_id: str, item_id: str, quantity: int) -> bool:
        """从宗门仓库移除物品，返回是否成功。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT quantity FROM sect_warehouse WHERE sect_id = ? AND item_id = ?",
            (sect_id, item_id),
        )
        row = await cur.fetchone()
        if not row or row[0] < quantity:
            return False
        new_qty = row[0] - quantity
        if new_qty <= 0:
            await self.db.execute(
                "DELETE FROM sect_warehouse WHERE sect_id = ? AND item_id = ?",
                (sect_id, item_id),
            )
        else:
            await self.db.execute(
                "UPDATE sect_warehouse SET quantity = ? WHERE sect_id = ? AND item_id = ?",
                (new_qty, sect_id, item_id),
            )
        await self.db.commit()
        return True

    async def delete_sect_warehouse(self, sect_id: str) -> None:
        """删除宗门仓库所有物品（解散宗门时调用）。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            "DELETE FROM sect_warehouse WHERE sect_id = ?", (sect_id,),
        )
        await self.db.commit()

    async def warehouse_deposit_atomic(
        self, player: "Player", sect_id: str, item_id: str, quantity: int,
        contribution_delta: int,
    ) -> dict:
        """独立连接事务：仓库入库 + 贡献点增加 + 玩家落库。

        返回 {"success": True, "contribution": int}
        或 {"success": False, "reason": ...}。
        """
        conn: aiosqlite.Connection | None = None
        try:
            conn = await aiosqlite.connect(self._db_path)
            await conn.execute("PRAGMA busy_timeout = 30000")
            await conn.execute("BEGIN IMMEDIATE")

            # 复检仓库容量
            cur = await conn.execute(
                "SELECT warehouse_capacity FROM sects WHERE sect_id = ?", (sect_id,),
            )
            sect_row = await cur.fetchone()
            if not sect_row:
                await conn.rollback()
                return {"success": False, "reason": "sect_not_found"}
            capacity = sect_row[0] if sect_row[0] is not None else 200

            cur2 = await conn.execute(
                "SELECT quantity FROM sect_warehouse WHERE sect_id = ? AND item_id = ?",
                (sect_id, item_id),
            )
            existing = await cur2.fetchone()
            if not existing or existing[0] == 0:
                cur3 = await conn.execute(
                    "SELECT COUNT(*) FROM sect_warehouse WHERE sect_id = ? AND quantity > 0",
                    (sect_id,),
                )
                slots_used = (await cur3.fetchone())[0]
                if slots_used >= capacity:
                    await conn.rollback()
                    return {"success": False, "reason": "warehouse_full"}

            # 写仓库
            await conn.execute(
                """INSERT INTO sect_warehouse (sect_id, item_id, quantity)
                   VALUES (?, ?, ?)
                   ON CONFLICT(sect_id, item_id) DO UPDATE SET quantity = quantity + ?""",
                (sect_id, item_id, quantity, quantity),
            )

            # 加贡献点（校验成员仍在该宗门）
            cur_contrib = await conn.execute(
                "UPDATE sect_members SET contribution_points = contribution_points + ? "
                "WHERE user_id = ? AND sect_id = ?",
                (contribution_delta, player.user_id, sect_id),
            )
            if cur_contrib.rowcount == 0:
                await conn.rollback()
                return {"success": False, "reason": "member_not_found"}

            # 玩家落库
            await self._upsert_player(player, db=conn)
            await conn.commit()

            # 事务完成后读贡献点（用主连接）
            new_contribution = await self.get_member_contribution(player.user_id)
            return {"success": True, "contribution": new_contribution}
        except Exception:
            if conn is not None:
                await conn.rollback()
            raise
        finally:
            if conn is not None:
                await conn.close()

    async def warehouse_exchange_atomic(
        self, player: "Player", sect_id: str, item_id: str, quantity: int,
        contribution_cost: int,
    ) -> dict:
        """独立连接事务：条件扣贡献点 + 条件扣仓库 + 玩家落库。

        返回 {"success": True, "contribution": int}
        或 {"success": False, "reason": "insufficient_stock"|"insufficient_contribution"}。
        """
        conn: aiosqlite.Connection | None = None
        try:
            conn = await aiosqlite.connect(self._db_path)
            await conn.execute("PRAGMA busy_timeout = 30000")
            await conn.execute("BEGIN IMMEDIATE")

            # 条件扣贡献点
            cur = await conn.execute(
                "UPDATE sect_members "
                "SET contribution_points = contribution_points - ? "
                "WHERE user_id = ? AND sect_id = ? AND contribution_points >= ?",
                (contribution_cost, player.user_id, sect_id, contribution_cost),
            )
            if cur.rowcount == 0:
                await conn.rollback()
                return {"success": False, "reason": "insufficient_contribution"}

            # 条件扣仓库
            cur2 = await conn.execute(
                "UPDATE sect_warehouse "
                "SET quantity = quantity - ? "
                "WHERE sect_id = ? AND item_id = ? AND quantity >= ?",
                (quantity, sect_id, item_id, quantity),
            )
            if cur2.rowcount == 0:
                await conn.rollback()
                return {"success": False, "reason": "insufficient_stock"}

            # 清理零库存行
            await conn.execute(
                "DELETE FROM sect_warehouse WHERE sect_id = ? AND item_id = ? AND quantity <= 0",
                (sect_id, item_id),
            )

            # 玩家落库
            await self._upsert_player(player, db=conn)
            await conn.commit()

            # 事务完成后读贡献点（用主连接）
            new_contribution = await self.get_member_contribution(player.user_id)
            return {"success": True, "contribution": new_contribution}
        except Exception:
            if conn is not None:
                await conn.rollback()
            raise
        finally:
            if conn is not None:
                await conn.close()

    # ── 宗门贡献点规则 ────────────────────────────────────

    async def get_contribution_config(self, sect_id: str) -> list[dict]:
        """获取宗门全部贡献点规则。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT rule_type, target_key, points FROM sect_contribution_config WHERE sect_id = ?",
            (sect_id,),
        )
        rows = await cur.fetchall()
        return [{"rule_type": r[0], "target_key": r[1], "points": r[2]} for r in rows]

    async def get_contribution_config_by_key(
        self, sect_id: str, rule_type: str, target_key: str,
    ) -> int | None:
        """获取某条具体规则的贡献点数。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT points FROM sect_contribution_config WHERE sect_id = ? AND rule_type = ? AND target_key = ?",
            (sect_id, rule_type, target_key),
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def set_contribution_config(
        self, sect_id: str, rule_type: str, target_key: str, points: int,
    ) -> None:
        """设置/更新一条贡献点规则。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            """INSERT INTO sect_contribution_config (sect_id, rule_type, target_key, points)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(sect_id, rule_type, target_key) DO UPDATE SET points = ?""",
            (sect_id, rule_type, target_key, points, points),
        )
        await self.db.commit()

    async def delete_contribution_config(
        self, sect_id: str, rule_type: str, target_key: str,
    ) -> None:
        """删除一条贡献点规则。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            "DELETE FROM sect_contribution_config WHERE sect_id = ? AND rule_type = ? AND target_key = ?",
            (sect_id, rule_type, target_key),
        )
        await self.db.commit()

    async def delete_all_contribution_config(self, sect_id: str) -> None:
        """删除宗门所有贡献点规则（解散宗门时调用）。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            "DELETE FROM sect_contribution_config WHERE sect_id = ?", (sect_id,),
        )
        await self.db.commit()

    # ── 成员贡献点 ────────────────────────────────────────

    async def get_member_contribution(self, user_id: str) -> int:
        """获取成员贡献点。"""
        await self._ensure_sect_schema()
        cur = await self.db.execute(
            "SELECT contribution_points FROM sect_members WHERE user_id = ?",
            (user_id,),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def update_member_contribution(self, user_id: str, delta: int) -> int:
        """增减成员贡献点，返回新值。"""
        await self._ensure_sect_schema()
        await self.db.execute(
            "UPDATE sect_members SET contribution_points = MAX(0, contribution_points + ?) WHERE user_id = ?",
            (delta, user_id),
        )
        await self.db.commit()
        return await self.get_member_contribution(user_id)
